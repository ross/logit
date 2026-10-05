//! The pieces every sink that writes over `reqwest` shares: building the client, bucketing a
//! response status for telemetry, and turning a status or a transport error into a
//! [`logit_pipeline::Fault`].
//!
//! Shared, not copied: [`classify_status`] is the one status-only table, the class of every
//! response a sink's own table doesn't name, and only a connect failure is clean. Two copies of
//! one table are two things to drift.
//!
//! [`Outcomes`] is the verdict rule every sink that sends one batch as several requests applies,
//! whatever carries the request (`otlp_out`'s gRPC and `datadog_trace_out`'s Unix socket go over
//! `hyper`, not `reqwest`). Each request's verdict stands on its own: a `Fault::Rejected` that
//! names one request is counted by the sink and the send goes on to the next request; a
//! `Fault::Refused` (the destination refuses every request), a `Fault::Clean`, or a
//! `Fault::Ambiguous` stops the send. A sink that resends the whole batch on a retry
//! ([`Outcomes::new`]) stops it through [`after_delivery`]; one that remembers the requests the
//! destination settled and resends only the rest ([`Outcomes::resuming`], `otlp_out`) stops it with
//! the failed request's own fault. The send succeeds when any request was accepted, and fails with
//! the first rejection when none was. See `docs/adr/delivery-semantics.md`'s "Amendment:
//! per-request verdicts (2026-10-04)" and `docs/adr/sink-fault-classes.md`'s "Amendment:
//! `otlp_out` retries per signal (2026-10-05)".
//!
//! **How a sink overrides the status table.** [`classify_status`] reads the status alone, and is
//! the class of every response a sink's own table doesn't name. A sink whose destination says
//! more, in a documented body code, a header, or the protocol's own retry rules, reads the body
//! with [`read_body_prefix`] (bounded; [`redacted_snippet`] when the body may echo a secret),
//! matches the rows its destination documents, and falls through to [`classify_status`] for the
//! rest. Each sink records its rows in a `Response | Class | Why | Evidence` table in its module
//! doc, one test per row:
//!
//! - `influxdb_out`: [`crate::influxdb`], "Response classes"
//! - `otlp_out`: [`crate::otlp`], "Response classes"
//! - `prometheus_out`: [`crate::prometheus`], "Faults, retries and duplicate safety (sender mode)"
//! - `datadog_out`: [`crate::datadog`], "Faults, retries, and duplicate safety"
//! - `datadog_trace_out`: [`crate::datadog_trace`], "Faults, retries, and duplicate safety"
//! - `splunk_hec_out`: [`crate::splunk`], "Faults, retries, and duplicate safety"
//!
//! [`split_encode`] is the request splitter `datadog_out` and `datadog_trace_out` share: both cut a
//! batch into requests under a per-route entry count and body size.

use bytes::Bytes;
use logit_core::CountGate;
use logit_pipeline::Fault;
use std::time::Duration;

/// Builds an HTTP sink's client.
///
/// `tls` is `None` when no `tls:` block is set: `reqwest`'s default already trusts the bundled
/// Mozilla roots for an `https://` endpoint. `Some` carries a config built from operator settings
/// by [`crate::tls::build_client_config`].
///
/// `timeout` is the client-wide default; both callers also set a per-request `.timeout(..)`,
/// which is what bounds each request.
///
/// **Redirects are off**, overriding `reqwest`'s `limited(10)` default, for `otlp_out` and
/// `prometheus_out` alike: both are write paths whose fault classification assumes *one* request
/// went to the *configured* URL:
///
/// - a `301`/`302`/`303` is replayed as a body-less `GET`, so whatever the target answers becomes
///   the sink's verdict on a batch that was never written -- a `2xx` acks it with every sample
///   counted, a `405` (what Prometheus and Mimir answer for `GET` on a write path) refuses the
///   sink as `Fault::Refused`;
/// - a `307`/`308` replays the body *and* the operator's `headers:` at the `Location` host.
///   `reqwest` strips only `Authorization`/`Cookie`, and only when the host or port changes, so a
///   tenant header always travels and a same-host `https://` → `http://` downgrade carries
///   credentials in the clear -- past a config-time scheme check that has no say at runtime.
///
/// With the policy off a `3xx` is a non-2xx: [`status_class`] buckets it `3xx`,
/// [`classify_status`] reads it as `Fault::Rejected`, and the operator sees the misconfiguration
/// reported against the URL they configured. Nothing legitimate is lost: neither remote-write nor
/// OTLP defines a redirect, and a moved endpoint is a config change, not something to follow.
pub(crate) fn build_client(
    timeout: Duration,
    tls: Option<&rustls::ClientConfig>,
) -> reqwest::Client {
    let mut builder =
        reqwest::Client::builder().timeout(timeout).redirect(reqwest::redirect::Policy::none());
    if let Some(cfg) = tls {
        builder = builder.use_preconfigured_tls(cfg.clone());
    }
    builder.build().expect("reqwest client should build with the configured TLS settings")
}

/// How much of a rejection body to read and quote in an error message and a diagnostic. Enough
/// for Prometheus's `400` text, which names the offending series and why; short enough that an
/// HTML error page doesn't fill a log line.
pub(crate) const ERROR_BODY_SNIPPET_BYTES: usize = 256;

/// At most `max` (plus a little slack) bytes of `response`'s body, decoded lossily.
///
/// **A bounded read, not only a bounded message.** `Response::text()` buffers the whole body,
/// bounded in time by the per-request timeout but not in bytes, so a receiver answering `500`
/// with an arbitrarily long body costs that much allocation per attempt, on the sink most likely
/// to keep retrying. Stopping the read makes the cost constant.
///
/// The slack past `max` lets [`body_snippet`] tell a body that ended at `max` from one that
/// carried on, so only the latter gets an ellipsis. A chunk boundary mid-character is
/// `from_utf8_lossy`'s to handle; `body_snippet` cuts back to a character boundary anyway. A read
/// error isn't propagated: the status is the primary signal, and a partial body beats none.
pub(crate) async fn read_body_prefix(mut response: reqwest::Response, max: usize) -> String {
    let limit = max.saturating_add(4);
    let mut buf: Vec<u8> = Vec::new();
    while buf.len() < limit {
        match response.chunk().await {
            Ok(Some(chunk)) => buf.extend_from_slice(&chunk),
            Ok(None) | Err(_) => break,
        }
    }
    buf.truncate(limit);
    String::from_utf8_lossy(&buf).into_owned()
}

/// The first `max` bytes of `body`, trimmed, on a character boundary, with an ellipsis when
/// anything was cut. [`read_body_prefix`] has already bounded what reaches here.
pub(crate) fn body_snippet(body: &str, max: usize) -> String {
    let trimmed = body.trim();
    if trimmed.len() <= max {
        return trimmed.to_string();
    }
    let mut end = max;
    while end > 0 && !trimmed.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &trimmed[..end])
}

/// How many bytes to read from a rejection body before cutting it to [`ERROR_BODY_SNIPPET_BYTES`]:
/// enough past the snippet size that a secret echoed within the kept snippet is read whole,
/// rather than cut mid-secret by the read limit itself, so [`redacted_snippet`]'s whole-secret
/// match can still catch it before the cut.
pub(crate) fn error_read_bytes(secret: &str) -> usize {
    ERROR_BODY_SNIPPET_BYTES + secret.len()
}

/// A rejection body read with [`error_read_bytes`], as a snippet safe to quote: `secret` replaced
/// with `<redacted>`, cut to [`ERROR_BODY_SNIPPET_BYTES`], and any trailing remnant of a secret the
/// read limit split stripped ([`strip_secret_remnant`]). `datadog_out`'s API key and
/// `splunk_hec_out`'s token are the secrets.
pub(crate) fn redacted_snippet(body: &str, secret: &str) -> String {
    let redacted =
        if secret.is_empty() { body.to_string() } else { body.replace(secret, "<redacted>") };
    strip_secret_remnant(body_snippet(&redacted, ERROR_BODY_SNIPPET_BYTES), secret)
}

/// After `snippet` is cut to size, strips a trailing run of 4 or more bytes that is itself a
/// prefix of `secret`. That run is what's left of a secret split by [`error_read_bytes`]'s own
/// read limit, which [`redacted_snippet`]'s whole-secret match can't catch because the read never
/// captured the whole secret.
fn strip_secret_remnant(snippet: String, secret: &str) -> String {
    let (content, ellipsis) = match snippet.strip_suffix("...") {
        Some(rest) => (rest, "..."),
        None => (snippet.as_str(), ""),
    };
    let max_run = content.len().min(secret.len());
    for len in (4..=max_run).rev() {
        let cut = content.len() - len;
        if !content.is_char_boundary(cut) {
            continue;
        }
        if secret.as_bytes().starts_with(&content.as_bytes()[cut..]) {
            return format!("{}{ellipsis}", &content[..cut]);
        }
    }
    snippet
}

/// A coarse HTTP response-status bucket: the `class` tag on `logit.output.requests`.
///
/// Six values and no more. A `429` stays a `4xx`; [`classify_status`] is what reads it
/// for the [`Fault`]. A timeout isn't a status, so each caller counts it as `"network_error"` on
/// its own transport-error arm.
pub(crate) fn status_class(status: reqwest::StatusCode) -> &'static str {
    match status.as_u16() / 100 {
        1 => "1xx",
        2 => "2xx",
        3 => "3xx",
        4 => "4xx",
        5 => "5xx",
        _ => "other",
    }
}

/// The [`Fault`] of a non-success HTTP response, from the status alone. This is the one copy of
/// the status-only default in
/// [ADR `sink-fault-classes`](../../../docs/adr/sink-fault-classes.md), "Each sink attributes from
/// everything its destination gives it"; a sink that reads the body points here and overrides only
/// the rows its destination's documentation changes.
///
/// | Status | Fault | Why |
/// |---|---|---|
/// | 401, 403, 404, 405, 407, 501 | [`Fault::Refused`] | credentials, a missing endpoint or tenant, a wrong method, or an unsupported feature: the same answer for every batch |
/// | 429, any 5xx | [`Fault::Ambiguous`] | the request reached the server and may have been applied |
/// | any other 4xx, any 3xx | [`Fault::Rejected`] | about this request; redirects are off ([`build_client`]), so a 3xx is a misconfigured URL the sink won't follow |
///
/// A 1xx or 2xx isn't a failure and no caller passes one; it reads as [`Fault::Rejected`], the
/// default for a response the sink can't attribute.
pub(crate) fn classify_status(status: reqwest::StatusCode) -> Fault {
    match status.as_u16() {
        401 | 403 | 404 | 405 | 407 | 501 => Fault::Refused,
        429 | 500..=599 => Fault::Ambiguous,
        _ => Fault::Rejected,
    }
}

/// A `reqwest` transport failure's [`Fault`]. [`Fault::Clean`] is reserved for a *connect*
/// failure, the one case where the destination provably never saw the request; everything else --
/// a timeout, a connection reset mid-body, a TLS failure after the handshake started -- is
/// [`Fault::Ambiguous`], since the request may have been received and the response lost.
pub(crate) fn classify_reqwest_error(err: &reqwest::Error) -> Fault {
    if err.is_connect() {
        Fault::Clean
    } else {
        Fault::Ambiguous
    }
}

/// The fault of a failed request in a `send` of several requests. `sent_any` says whether an
/// earlier request of the same `send` was accepted; once one was, a [`Fault::Clean`] or
/// [`Fault::Refused`] failure becomes [`Fault::Ambiguous`]. Both mean the destination holds
/// nothing of the batch, and `write_loop` retries them under every posture, `at_most_once`
/// included, which would resend the accepted requests (`docs/adr/delivery-semantics.md`, item 9).
/// Every other fault passes through unchanged.
///
/// The added context is the outermost, and [`logit_pipeline::classify`] reads the outermost
/// `Fault`, so it overrides the request's own.
pub(crate) fn after_delivery(err: anyhow::Error, sent_any: bool) -> anyhow::Error {
    if sent_any && matches!(logit_pipeline::classify(&err), Fault::Clean | Fault::Refused) {
        err.context(Fault::Ambiguous)
    } else {
        err
    }
}

/// The verdicts of the requests one `send` issued, folded into the send's result.
///
/// | A request's result | [`Outcomes::note`] |
/// |---|---|
/// | `Ok` | notes it accepted; the send goes on |
/// | [`Fault::Rejected`] | calls `on_rejected` (the sink counts the request's records there), keeps the first such error; the send goes on |
/// | [`Fault::Refused`] | stops the send through [`after_delivery`]: every later request would get the same answer |
/// | [`Fault::Clean`] or [`Fault::Ambiguous`] | stops the send through [`after_delivery`] |
/// | no `Fault` attached | stops the send with the error unchanged |
///
/// An error with no `Fault` is not a destination's verdict on the request, so it isn't counted as
/// a rejection; [`logit_pipeline::classify`] still reads it as `Rejected`.
///
/// [`Outcomes::finish`] ends a send that wasn't stopped: `Ok` when any request was accepted, even
/// with rejections beside it; the first rejection when none was, still explicitly `Rejected`, so
/// `write_loop` drops the wholly rejected batch; `Ok` when there were no requests.
///
/// Built with [`Outcomes::resuming`], a stop keeps the request's own fault instead of passing
/// through [`after_delivery`]: the sink resends none of the requests already accepted, so a retry
/// risks no duplicate of them.
#[derive(Debug, Default)]
pub(crate) struct Outcomes {
    accepted: bool,
    first_rejected: Option<anyhow::Error>,
    /// Set by [`Outcomes::resuming`]: a stop keeps the request's own fault.
    resuming: bool,
}

impl Outcomes {
    /// For a sink whose retry resends every request of the batch.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// For a sink whose retry resends only the requests the destination hasn't settled.
    /// `accepted_before` says whether an earlier attempt at the same batch had a request
    /// accepted, which makes the send `Ok` however this attempt's requests are answered short of
    /// a stop.
    pub(crate) fn resuming(accepted_before: bool) -> Self {
        Self { accepted: accepted_before, first_rejected: None, resuming: true }
    }

    /// Folds one request's `result` in. `Ok(())` means the send goes on; `Err` is the send's
    /// result, to return at once. `on_rejected` runs only for a rejection the send continues past.
    pub(crate) fn note(
        &mut self,
        result: anyhow::Result<()>,
        on_rejected: impl FnOnce(&anyhow::Error),
    ) -> anyhow::Result<()> {
        let err = match result {
            Ok(()) => {
                self.accepted = true;
                return Ok(());
            }
            Err(err) => err,
        };
        match err.downcast_ref::<Fault>() {
            Some(Fault::Rejected) => {
                on_rejected(&err);
                if self.first_rejected.is_none() {
                    self.first_rejected = Some(err);
                }
                Ok(())
            }
            None => Err(err),
            Some(Fault::Clean | Fault::Ambiguous | Fault::Refused) if self.resuming => Err(err),
            Some(Fault::Clean | Fault::Ambiguous | Fault::Refused) => {
                Err(after_delivery(err, self.accepted))
            }
        }
    }

    /// The send's result once every request was noted without a stop.
    pub(crate) fn finish(self) -> anyhow::Result<()> {
        match self.first_rejected {
            Some(err) if !self.accepted => Err(err),
            _ => Ok(()),
        }
    }
}

/// One route's request limits; `usize::MAX` is no limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Caps {
    /// Entries per request, by [`split_encode`]'s weight.
    pub entries: usize,
    /// The body before compression.
    pub raw_bytes: usize,
    /// The body as sent.
    pub wire_bytes: usize,
}

impl Caps {
    pub const UNBOUNDED: Caps =
        Caps { entries: usize::MAX, raw_bytes: usize::MAX, wire_bytes: usize::MAX };
}

/// One encoded request body, with whatever else the encoder reported about it (`meta`).
#[derive(Debug)]
pub(crate) struct Encoded<M = ()> {
    /// As sent: compressed, when the route compresses.
    pub body: Bytes,
    /// Before compression.
    pub raw_len: usize,
    pub meta: M,
}

/// [`split_encode`]'s result: the requests to send, in order, and the items too big to send alone.
#[derive(Debug)]
pub(crate) struct Split<T, M = ()> {
    pub requests: Vec<(Vec<T>, Encoded<M>)>,
    /// Each with its encoded size (uncompressed, as sent).
    pub oversize: Vec<(T, usize, usize)>,
}

/// Cuts `items` into requests under `caps`: by entry count first (`weight` per item; an item
/// heavier than the cap goes alone), then, for a chunk whose encoded body is over a byte cap, by
/// bisection and re-encoding down to one item, which is reported oversize if it still doesn't
/// fit. A chunk `encode` returns `None` for sends nothing. Order is kept.
///
/// The count-capped chunks partition `items`, so their encodes see each item once, and they run
/// with `gate` as the caller left it. Every bisection re-encode sees items a chunk encode already
/// saw, so it runs inside [`CountGate::muted`]: a codec counting through handles gated by `gate`
/// counts each item once per `split_encode`, however deep the bisection goes. A counter a codec
/// emits once per body, not per item, counts once per count-capped chunk.
pub(crate) fn split_encode<T: Copy, M>(
    items: &[T],
    caps: Caps,
    gate: &CountGate,
    weight: impl Fn(&T) -> usize,
    mut encode: impl FnMut(&[T]) -> Option<Encoded<M>>,
) -> Split<T, M> {
    let mut split = Split { requests: Vec::new(), oversize: Vec::new() };
    let mut start = 0;
    let mut entries = 0usize;
    for (i, item) in items.iter().enumerate() {
        let w = weight(item);
        if i > start && entries.saturating_add(w) > caps.entries {
            fit(&items[start..i], caps, gate, 0, &mut encode, &mut split);
            start = i;
            entries = 0;
        }
        entries = entries.saturating_add(w);
    }
    if start < items.len() {
        fit(&items[start..], caps, gate, 0, &mut encode, &mut split);
    }
    split
}

/// [`split_encode`]'s byte-cap half, for one count-capped chunk (`depth` 0) or one half of a
/// bisection (`depth` above 0), whose encode runs muted.
fn fit<T: Copy, M, F: FnMut(&[T]) -> Option<Encoded<M>>>(
    items: &[T],
    caps: Caps,
    gate: &CountGate,
    depth: u32,
    encode: &mut F,
    split: &mut Split<T, M>,
) {
    let encoded = if depth == 0 { encode(items) } else { gate.muted(|| encode(items)) };
    let Some(encoded) = encoded else { return };
    if encoded.raw_len <= caps.raw_bytes && encoded.body.len() <= caps.wire_bytes {
        split.requests.push((items.to_vec(), encoded));
    } else if let [item] = items {
        split.oversize.push((*item, encoded.raw_len, encoded.body.len()));
    } else {
        let mid = items.len() / 2;
        fit(&items[..mid], caps, gate, depth + 1, encode, split);
        fit(&items[mid..], caps, gate, depth + 1, encode, split);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_short_body_is_trimmed_and_not_truncated() {
        assert_eq!(body_snippet("  short  ", ERROR_BODY_SNIPPET_BYTES), "short");
        assert_eq!(body_snippet("", ERROR_BODY_SNIPPET_BYTES), "");
    }

    /// A body at the bound keeps every byte; a longer one is cut on a character boundary.
    #[test]
    fn a_long_body_is_truncated_on_a_character_boundary() {
        let exactly = "x".repeat(ERROR_BODY_SNIPPET_BYTES);
        assert_eq!(body_snippet(&exactly, ERROR_BODY_SNIPPET_BYTES), exactly);

        let over = "é".repeat(400);
        let snippet = body_snippet(&over, ERROR_BODY_SNIPPET_BYTES);
        assert!(snippet.ends_with("..."));
        assert!(snippet.len() <= ERROR_BODY_SNIPPET_BYTES + 3, "got {} bytes", snippet.len());
        assert!(
            snippet.trim_end_matches('.').chars().all(|c| c == 'é'),
            "no partial character survives the cut: {snippet}"
        );
    }

    /// A `3xx` is its own class and is rejected, so a redirect surfaces as a misconfiguration.
    #[test]
    fn a_3xx_is_its_own_class_and_is_rejected() {
        assert_eq!(status_class(reqwest::StatusCode::FOUND), "3xx");
        assert_eq!(classify_status(reqwest::StatusCode::FOUND), Fault::Rejected);
    }

    /// The status-only table: each row of [`classify_status`]'s doc.
    #[test]
    fn classify_status_follows_the_table() {
        for code in [401, 403, 404, 405, 407, 501] {
            let status = reqwest::StatusCode::from_u16(code).unwrap();
            assert_eq!(classify_status(status), Fault::Refused, "{code}");
        }
        for code in [429, 500, 502, 503, 504, 599] {
            let status = reqwest::StatusCode::from_u16(code).unwrap();
            assert_eq!(classify_status(status), Fault::Ambiguous, "{code}");
        }
        for code in [300, 301, 307, 400, 402, 406, 408, 413, 415, 422] {
            let status = reqwest::StatusCode::from_u16(code).unwrap();
            assert_eq!(classify_status(status), Fault::Rejected, "{code}");
        }
    }

    fn failed(fault: Fault) -> anyhow::Result<()> {
        Err(anyhow::anyhow!("request failed").context(fault))
    }

    fn refused() -> anyhow::Result<()> {
        failed(Fault::Refused)
    }

    /// Notes `result`, returning the send's next step and whether `on_rejected` ran.
    fn note(outcomes: &mut Outcomes, result: anyhow::Result<()>) -> (anyhow::Result<()>, bool) {
        let mut rejected = false;
        let next = outcomes.note(result, |_| rejected = true);
        (next, rejected)
    }

    #[test]
    fn a_send_with_no_requests_is_ok() {
        assert!(Outcomes::new().finish().is_ok());
    }

    #[test]
    fn accepted_requests_finish_ok() {
        let mut outcomes = Outcomes::new();
        let (next, rejected) = note(&mut outcomes, Ok(()));
        assert!(next.is_ok() && !rejected);
        assert!(outcomes.finish().is_ok());
    }

    /// A rejection beside an accepted request is counted, and the send still succeeds.
    #[test]
    fn a_rejection_continues_and_an_accepted_request_makes_the_send_ok() {
        for accepted_first in [true, false] {
            let mut outcomes = Outcomes::new();
            if accepted_first {
                assert!(note(&mut outcomes, Ok(())).0.is_ok());
            }
            let (next, rejected) = note(&mut outcomes, failed(Fault::Rejected));
            assert!(next.is_ok(), "a rejection does not stop the send");
            assert!(rejected, "on_rejected runs for a rejection");
            if !accepted_first {
                assert!(note(&mut outcomes, Ok(())).0.is_ok());
            }
            assert!(outcomes.finish().is_ok(), "accepted_first: {accepted_first}");
        }
    }

    /// With nothing accepted, the send fails with the first rejection, explicitly `Rejected`.
    #[test]
    fn rejections_alone_finish_with_the_first_rejection() {
        let mut outcomes = Outcomes::new();
        let first = Err(anyhow::anyhow!("first").context(Fault::Rejected));
        assert!(note(&mut outcomes, first).0.is_ok());
        assert!(note(&mut outcomes, failed(Fault::Rejected)).0.is_ok());
        let err = outcomes.finish().unwrap_err();
        assert_eq!(err.downcast_ref::<Fault>(), Some(&Fault::Rejected));
        assert!(format!("{err:#}").contains("first"), "the first rejection is kept: {err:#}");
    }

    /// A refusal stops the send: `Refused` with nothing accepted, so the runtime holds the batch,
    /// and `Ambiguous` once a request was accepted, so an at-most-once sink doesn't resend it.
    #[test]
    fn a_refusal_stops_the_send_and_is_ambiguous_once_a_request_was_accepted() {
        for (accepted_first, want) in [(false, Fault::Refused), (true, Fault::Ambiguous)] {
            let mut outcomes = Outcomes::new();
            if accepted_first {
                assert!(note(&mut outcomes, Ok(())).0.is_ok());
            }
            let (next, rejected) = note(&mut outcomes, refused());
            let err = next.unwrap_err();
            assert!(!rejected, "a refusal is not counted as a rejection");
            assert_eq!(logit_pipeline::classify(&err), want, "accepted first: {accepted_first}");
        }
    }

    /// `Clean` stops the send, and becomes `Ambiguous` once a request was accepted; `Ambiguous`
    /// stops it unchanged. A rejection alone doesn't make `Clean` ambiguous: nothing was taken.
    #[test]
    fn clean_and_ambiguous_stop_the_send_through_after_delivery() {
        let cases = [
            (false, Fault::Clean, Fault::Clean),
            (true, Fault::Clean, Fault::Ambiguous),
            (false, Fault::Ambiguous, Fault::Ambiguous),
            (true, Fault::Ambiguous, Fault::Ambiguous),
        ];
        for (accepted_first, fault, want) in cases {
            let mut outcomes = Outcomes::new();
            assert!(note(&mut outcomes, failed(Fault::Rejected)).0.is_ok());
            if accepted_first {
                assert!(note(&mut outcomes, Ok(())).0.is_ok());
            }
            let (next, rejected) = note(&mut outcomes, failed(fault));
            assert!(!rejected);
            assert_eq!(
                logit_pipeline::classify(&next.unwrap_err()),
                want,
                "accepted_first: {accepted_first}, fault: {fault}"
            );
        }
    }

    /// Resuming, a stop keeps the request's own fault even after an accepted request, and an
    /// earlier attempt's acceptance makes a send of rejections alone `Ok`.
    #[test]
    fn resuming_stops_with_the_requests_own_fault_and_carries_an_earlier_acceptance() {
        for fault in [Fault::Clean, Fault::Refused, Fault::Ambiguous] {
            let mut outcomes = Outcomes::resuming(false);
            assert!(note(&mut outcomes, Ok(())).0.is_ok());
            let err = note(&mut outcomes, failed(fault)).0.unwrap_err();
            assert_eq!(logit_pipeline::classify(&err), fault);
        }
        let mut outcomes = Outcomes::resuming(true);
        assert!(note(&mut outcomes, failed(Fault::Rejected)).0.is_ok());
        assert!(outcomes.finish().is_ok(), "an earlier attempt's acceptance makes the send Ok");
        let mut outcomes = Outcomes::resuming(false);
        assert!(note(&mut outcomes, failed(Fault::Rejected)).0.is_ok());
        assert_eq!(logit_pipeline::classify(&outcomes.finish().unwrap_err()), Fault::Rejected);
    }

    /// An error with no `Fault` is no destination verdict: it stops the send unchanged.
    #[test]
    fn an_unclassified_error_stops_the_send_unchanged() {
        let mut outcomes = Outcomes::new();
        let (next, rejected) = note(&mut outcomes, Err(anyhow::anyhow!("encode failed")));
        let err = next.unwrap_err();
        assert!(!rejected);
        assert!(err.downcast_ref::<Fault>().is_none());
    }

    fn fake_encode(items: &[u32]) -> Option<Encoded> {
        let raw_len: usize = items.iter().map(|&i| i as usize).sum();
        Some(Encoded { body: Bytes::from(vec![0; raw_len / 2]), raw_len, meta: () })
    }

    fn chunks(split: &Split<u32>) -> Vec<Vec<u32>> {
        split.requests.iter().map(|(items, _)| items.clone()).collect()
    }

    /// Entries fill a request up to the cap by weight; an item heavier than the cap goes alone.
    #[test]
    fn the_splitter_cuts_by_entry_count_first() {
        let caps = Caps { entries: 5, ..Caps::UNBOUNDED };
        let gate = CountGate::new();
        let split = split_encode(&[2, 2, 2, 9, 1], caps, &gate, |&w| w as usize, fake_encode);
        assert_eq!(chunks(&split), [vec![2, 2], vec![2], vec![9], vec![1]]);
        assert!(split.oversize.is_empty());
    }

    /// Over a byte cap the chunk bisects, and one item still over it is reported, not sent.
    #[test]
    fn the_splitter_bisects_over_a_byte_cap_and_reports_a_lone_oversize_item() {
        let gate = CountGate::new();
        let caps = Caps { raw_bytes: 10, ..Caps::UNBOUNDED };
        let split = split_encode(&[4, 4, 4, 30, 1], caps, &gate, |_| 1, fake_encode);
        assert_eq!(chunks(&split), [vec![4, 4], vec![4], vec![1]]);
        assert_eq!(split.oversize, [(30, 30, 15)]);

        let caps = Caps { wire_bytes: 3, ..Caps::UNBOUNDED };
        let split = split_encode(&[4, 4, 8], caps, &gate, |_| 1, fake_encode);
        assert_eq!(chunks(&split), [vec![4], vec![4]]);
        assert_eq!(split.oversize, [(8, 8, 4)]);
    }

    /// A chunk the encoder has nothing for sends nothing.
    #[test]
    fn the_splitter_skips_a_chunk_that_encodes_to_nothing() {
        let gate = CountGate::new();
        let split = split_encode(&[1, 2], Caps::UNBOUNDED, &gate, |_| 1, |_| None::<Encoded>);
        assert!(split.requests.is_empty() && split.oversize.is_empty());
    }

    /// Every encode `split_encode` makes over `items`, in order: the items and whether `gate` was
    /// muted during it.
    fn encodes(items: &[u32], caps: Caps, gate: &CountGate) -> Vec<(Vec<u32>, bool)> {
        let mut seen = Vec::new();
        split_encode(
            items,
            caps,
            gate,
            |_| 1,
            |chunk| {
                seen.push((chunk.to_vec(), gate.is_muted()));
                fake_encode(chunk)
            },
        );
        seen
    }

    /// The count-capped chunks run with the gate as the caller left it and partition the items;
    /// every bisection re-encode runs muted, and the gate is as it was afterwards.
    #[test]
    fn bisection_re_encodes_run_muted_and_count_capped_chunks_as_the_caller_left_the_gate() {
        // Two count-capped chunks of three; the first is over the byte cap and bisects to one
        // oversize item, the second fits.
        let caps = Caps { entries: 3, raw_bytes: 10, ..Caps::UNBOUNDED };
        let items = [4, 4, 30, 1, 2, 3];
        for outer in [false, true] {
            let gate = CountGate::new();
            gate.set_muted(outer);
            let seen = encodes(&items, caps, &gate);
            assert_eq!(
                seen,
                [
                    (vec![4, 4, 30], outer),
                    (vec![4], true),
                    (vec![4, 30], true),
                    (vec![4], true),
                    (vec![30], true),
                    (vec![1, 2, 3], outer),
                ],
                "outer muted: {outer}"
            );
            let depth_0 = [&seen[0].0[..], &seen[5].0[..]].concat();
            assert_eq!(depth_0, items, "the count-capped chunks partition the items");
            assert_eq!(gate.is_muted(), outer, "the gate is as the caller left it");
        }
    }
}
