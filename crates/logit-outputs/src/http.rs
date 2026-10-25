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
//! `Fault::Ambiguous` stops the send with the failed request's own fault. Each such sink remembers
//! the requests the destination settled ([`crate::accounting::BatchAccounting::settle`]) and a
//! retry resends only the rest, so a stop after an accepted request risks no duplicate of it. The
//! send succeeds when any request was accepted, on this attempt or an earlier one, and fails with
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
//!
//! **Concurrency.** A sink whose `concurrency:` is above 1 keeps that many batches in flight
//! through [`Slots`] (`slots.rs`'s module doc): one slot per batch, each encoded on the loop task
//! and sent from its own spawned task, answered to `write_loop` in batch order, and each fault
//! settled on its own batch (`docs/adr/sink-request-concurrency.md`). A sink that sends one
//! request per batch builds the request on the loop task and hands it to [`exchange`], which
//! gathers the answer for the sink to classify back on the loop task; its `send` does the same
//! with no task. `influxdb_out` and `prometheus_out`'s remote-write sender use it.
//!
//! A sink that sends a batch as several requests (`otlp_out`, `datadog_out`,
//! `datadog_trace_out`, `splunk_hec_out`) encodes them all on the loop task, then sends them in
//! order in the task, which counts and diagnoses each answer into a [`Journal`] the loop task
//! replays when the slot is acknowledged. Its `send` runs the same task code inline and replays
//! the journal before it returns, so both paths count the same points in the same order.

#[cfg(test)]
pub(crate) mod held;
mod journal;
mod slots;

pub(crate) use journal::Journal;
pub(crate) use slots::Slots;

use bytes::Bytes;
use logit_core::CountGate;
use logit_pipeline::Fault;
use std::time::Duration;

/// One request's answer, gathered without touching the sink, so it can run in a spawned task.
#[derive(Debug)]
pub(crate) struct Exchange {
    /// From the request's send to its response head or its transport error.
    pub took: Duration,
    pub reply: Reply,
}

#[derive(Debug)]
pub(crate) enum Reply {
    Success(reqwest::StatusCode),
    /// A non-2xx answer, with its headers and its body read bounded ([`read_body_prefix`], to
    /// [`ERROR_BODY_SNIPPET_BYTES`]).
    Failure {
        status: reqwest::StatusCode,
        headers: reqwest::header::HeaderMap,
        body: String,
    },
    Transport(reqwest::Error),
}

impl Reply {
    /// The `class` tag on `logit.output.requests`: [`status_class`], or [`transport_class`].
    pub(crate) fn class(&self) -> &'static str {
        match self {
            Reply::Success(status) | Reply::Failure { status, .. } => status_class(*status),
            Reply::Transport(err) => transport_class(classify_reqwest_error(err)),
        }
    }
}

/// Sends `request` and gathers its answer ([`Exchange`]). The request's own timeout bounds it.
pub(crate) async fn exchange(request: reqwest::RequestBuilder) -> Exchange {
    let started = std::time::Instant::now();
    let result = request.send().await;
    let took = started.elapsed();
    let reply = match result {
        Ok(response) if response.status().is_success() => Reply::Success(response.status()),
        Ok(response) => {
            let status = response.status();
            let headers = response.headers().clone();
            // A bounded read, not `text()`: see `read_body_prefix`.
            let body = read_body_prefix(response, ERROR_BODY_SNIPPET_BYTES).await;
            Reply::Failure { status, headers, body }
        }
        Err(err) => Reply::Transport(err),
    };
    Exchange { took, reply }
}

/// Builds an HTTP sink's client.
///
/// `tls` is `None` when no `tls:` block is set: `reqwest`'s default already trusts the bundled
/// Mozilla roots for an `https://` endpoint, and offers `h2` and `http/1.1` through ALPN. `Some`
/// carries a config built from operator settings by `logit_pipeline::tls::build_client_config`,
/// which leaves ALPN empty for `otlp_out`'s gRPC connector; [`client_builder`] sets the same two
/// protocols on its own copy (`logit_pipeline::tls::with_http_alpn`).
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
///
/// `idle_timeout` is the sink's `idle_timeout:`: how long a pooled connection may sit idle
/// before the pool closes it instead of reusing it, so the sink, not the destination, closes an
/// idle connection first. A write the destination receives after closing the connection it came
/// on is reset, and the sink can't tell whether it was applied: an `Ambiguous` fault no fault at
/// the destination caused. `Duration::ZERO` keeps no idle connection, so every request opens its
/// own. See `docs/adr/sink-request-concurrency.md`, decision 7.
pub(crate) fn build_client(
    timeout: Duration,
    idle_timeout: Duration,
    tls: Option<&rustls::ClientConfig>,
) -> reqwest::Client {
    client_builder(timeout, idle_timeout, tls)
        .build()
        .expect("reqwest client should build with the configured TLS settings")
}

/// [`build_client`]'s settings, unbuilt, for a caller that changes one, such as the proxy.
pub(crate) fn client_builder(
    timeout: Duration,
    idle_timeout: Duration,
    tls: Option<&rustls::ClientConfig>,
) -> reqwest::ClientBuilder {
    let mut builder =
        reqwest::Client::builder().timeout(timeout).redirect(reqwest::redirect::Policy::none());
    builder = if idle_timeout.is_zero() {
        builder.pool_max_idle_per_host(0)
    } else {
        builder.pool_idle_timeout(idle_timeout)
    };
    if let Some(cfg) = tls {
        builder = builder.use_preconfigured_tls(logit_pipeline::tls::with_http_alpn(cfg));
    }
    builder
}

/// The pool settings of [`client_builder`] on a `hyper_util` client: `otlp_out`'s gRPC client
/// and `datadog_trace_out`'s Unix-socket client. The timer is what closes an idle connection once
/// it expires; without one, the pool only skips it at the next checkout and the socket stays
/// open.
pub(crate) fn hyper_client_builder(idle_timeout: Duration) -> hyper_util::client::legacy::Builder {
    let mut builder =
        hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new());
    builder.pool_timer(hyper_util::rt::TokioTimer::new());
    if idle_timeout.is_zero() {
        builder.pool_max_idle_per_host(0);
    } else {
        builder.pool_idle_timeout(idle_timeout);
    }
    builder
}

/// The `idle_timeout:` default of every HTTP sink: below the keep-alive idle timeouts of the
/// common destinations that close at 60 s and longer, and above a 10 s or 15 s batch cadence, so
/// such a sink reuses its connection (`docs/adr/sink-request-concurrency.md`, decision 7, has the
/// table).
/// `logit_config`'s `default_sink_idle_timeout` mirrors it.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

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
pub(crate) async fn read_body_prefix(response: reqwest::Response, max: usize) -> String {
    let buf = read_body_bytes(response, max.saturating_add(4)).await;
    String::from_utf8_lossy(&buf).into_owned()
}

/// At most `limit` bytes of `response`'s body, as bytes: [`read_body_prefix`]'s bounded read, for
/// a body that isn't text. A read error ends the body where it stopped.
pub(crate) async fn read_body_bytes(mut response: reqwest::Response, limit: usize) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::new();
    while buf.len() < limit {
        match response.chunk().await {
            Ok(Some(chunk)) => buf.extend_from_slice(&chunk),
            Ok(None) | Err(_) => break,
        }
    }
    buf.truncate(limit);
    buf
}

/// [`read_body_bytes`], but a read error, such as the request's timeout or a connection closed
/// mid-body, is returned rather than ending the body, for a caller that parses the body and
/// must not read a cut-off one as complete.
pub(crate) async fn try_read_body_bytes(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, reqwest::Error> {
    let mut buf: Vec<u8> = Vec::new();
    while buf.len() < limit {
        match response.chunk().await? {
            Some(chunk) => buf.extend_from_slice(&chunk),
            None => break,
        }
    }
    buf.truncate(limit);
    Ok(buf)
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
/// for the [`Fault`]. A timeout isn't a status, so each caller counts a transport error with
/// [`transport_class`] on its own transport-error arm.
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

/// The `class` tag on `logit.output.requests` of a request that got no response, from its
/// transport error's [`Fault`]: `connect_error` for a [`Fault::Clean`] connect failure (a refused
/// or failed dial, a DNS failure, a failed TLS handshake), which the destination never saw, and
/// `network_error` for the rest (a timeout, a connection reset or closed before the response),
/// which it may have taken. A soak or an operator reading resends counts only `network_error`.
pub(crate) fn transport_class(fault: Fault) -> &'static str {
    if fault == Fault::Clean {
        "connect_error"
    } else {
        "network_error"
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
/// | 408, 429, any 5xx | [`Fault::Ambiguous`] | the request reached the server and may have been applied; a 408 is the server or a proxy in front of it giving up on the request's arrival (nginx `client_body_timeout`, HAProxy `timeout http-request`), which RFC 9110 lets a client repeat |
/// | any other 4xx, any 3xx | [`Fault::Rejected`] | about this request; redirects are off ([`build_client`]), so a 3xx is a misconfigured URL the sink won't follow |
///
/// A 1xx or 2xx isn't a failure and no caller passes one; it reads as [`Fault::Rejected`], the
/// default for a response the sink can't attribute.
pub(crate) fn classify_status(status: reqwest::StatusCode) -> Fault {
    match status.as_u16() {
        401 | 403 | 404 | 405 | 407 | 501 => Fault::Refused,
        408 | 429 | 500..=599 => Fault::Ambiguous,
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

/// The fault of a failed request in a `send` of several requests, for a sink whose retry resends
/// the whole batch (`splunk_hec_out`). `sent_any` says whether an
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
/// | [`Fault::Refused`] | stops the send with the request's own fault: every later request would get the same answer |
/// | [`Fault::Clean`] or [`Fault::Ambiguous`] | stops the send with the request's own fault |
/// | no `Fault` attached | stops the send with the error unchanged |
///
/// An error with no `Fault` is not a destination's verdict on the request, so it isn't counted as
/// a rejection; [`logit_pipeline::classify`] still reads it as `Rejected`.
///
/// [`Outcomes::finish`] ends a send that wasn't stopped: `Ok` when any request was accepted, even
/// with rejections beside it; the first rejection when none was, still explicitly `Rejected`, so
/// `write_loop` drops the wholly rejected batch; `Ok` when there were no requests.
///
/// The sink resends none of the requests already settled, so a stop keeps the request's own
/// fault: a retry risks no duplicate of an accepted request.
#[derive(Debug, Default)]
pub(crate) struct Outcomes {
    accepted: bool,
    first_rejected: Option<anyhow::Error>,
}

impl Outcomes {
    /// `accepted_before` says whether an earlier attempt at the same batch had a request
    /// accepted, which makes the send `Ok` however this attempt's requests are answered short of
    /// a stop.
    pub(crate) fn new(accepted_before: bool) -> Self {
        Self { accepted: accepted_before, first_rejected: None }
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
            None | Some(Fault::Clean | Fault::Ambiguous | Fault::Refused) => Err(err),
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

/// Reads a protobuf varint at `buf[*pos]`, advancing `*pos`. `None` if truncated or longer than
/// a 64-bit varint can be.
pub(crate) fn read_varint(buf: &[u8], pos: &mut usize) -> Option<u64> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        let byte = *buf.get(*pos)?;
        *pos += 1;
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some(result);
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
}

/// Advances `pos` past one uninteresting field's value. `None` if it runs past the end of `bytes`
/// or is a group (wire types 3/4, deprecated, and emitted by neither OTLP nor Datadog).
pub(crate) fn skip_field(bytes: &[u8], pos: &mut usize, wire_type: u64) -> Option<()> {
    match wire_type {
        0 => read_varint(bytes, pos).map(|_| ()),
        1 => {
            *pos += 8;
            (*pos <= bytes.len()).then_some(())
        }
        2 => {
            let len = read_varint(bytes, pos)? as usize;
            let end = pos.checked_add(len)?;
            if end > bytes.len() {
                return None;
            }
            *pos = end;
            Some(())
        }
        5 => {
            *pos += 4;
            (*pos <= bytes.len()).then_some(())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// With no `tls:` block, `reqwest`'s own config offers h2 through ALPN, so four requests at
    /// once to an h2 server share one connection. The test CA stands in for the bundled roots;
    /// adding it keeps `reqwest` building its own config.
    #[tokio::test]
    async fn the_default_tls_config_offers_h2() {
        let tls = crate::test_support::fixture_server_config("server", None, &[b"h2", b"http/1.1"]);
        let mux = held::Mux::start(Some(tls)).await;
        let ca = std::fs::read(crate::test_support::testdata_dir().join("ca.pem")).unwrap();
        let client = client_builder(Duration::from_secs(60), DEFAULT_IDLE_TIMEOUT, None)
            .add_root_certificate(reqwest::Certificate::from_pem(&ca).unwrap())
            .build()
            .unwrap();
        let requests: Vec<_> =
            (0..4).map(|_| tokio::spawn(client.post(mux.url()).body("x").send())).collect();
        logit_pipeline::test_util::wait_until("four requests open at once", || mux.open() == 4)
            .await;
        mux.release();
        for request in requests {
            assert_eq!(request.await.unwrap().unwrap().status(), 204);
        }
        let carriers = mux.carriers();
        assert_eq!(carriers.len(), 1, "four requests on one connection: {carriers:?}");
        assert_eq!(carriers[0].1.as_deref(), Some(&b"h2"[..]));
    }

    /// The `idle_timeout` the tests below set: short enough to wait out, long enough that two
    /// requests sent back to back never straddle it.
    const SHORT_IDLE: Duration = Duration::from_millis(200);

    /// Sends one request to `mux` and reads its answer to the end, so the connection is idle.
    async fn post(client: &reqwest::Client, mux: &held::Mux) {
        let response = client.post(mux.url()).body("x").send().await.unwrap();
        assert_eq!(response.status(), 204);
        response.bytes().await.unwrap();
    }

    fn connections(mux: &held::Mux) -> Vec<usize> {
        mux.carriers().into_iter().map(|(id, _)| id).collect()
    }

    /// A pooled connection idle past `idle_timeout` is closed by the client, and the next request
    /// opens a new one, so it never lands on a connection the destination may have closed.
    #[tokio::test]
    async fn a_connection_idle_past_idle_timeout_is_closed_and_not_reused() {
        let mux = held::Mux::start(None).await;
        mux.release();
        let client = build_client(Duration::from_secs(60), SHORT_IDLE, None);
        post(&client, &mux).await;
        logit_pipeline::test_util::wait_until("the client closed the idle connection", || {
            mux.closed() == 1
        })
        .await;
        post(&client, &mux).await;
        assert_eq!(connections(&mux), [0, 1], "the second request opened a connection");
    }

    /// Under the default, a connection idle for longer than [`SHORT_IDLE`] stays open and the
    /// next request reuses it.
    #[tokio::test]
    async fn a_connection_idle_under_idle_timeout_is_reused() {
        let mux = held::Mux::start(None).await;
        mux.release();
        let client = build_client(Duration::from_secs(60), DEFAULT_IDLE_TIMEOUT, None);
        post(&client, &mux).await;
        // Sizes a negative window: twice the idle time that closes the connection above.
        tokio::time::sleep(SHORT_IDLE * 2).await;
        assert_eq!(mux.closed(), 0, "the default keeps the connection");
        post(&client, &mux).await;
        assert_eq!(connections(&mux), [0], "both requests on one connection");
    }

    /// An `idle_timeout` of zero keeps no idle connection: each request opens its own, and the
    /// client closes it once answered.
    #[tokio::test]
    async fn an_idle_timeout_of_zero_reuses_no_connection() {
        let mux = held::Mux::start(None).await;
        mux.release();
        let client = build_client(Duration::from_secs(60), Duration::ZERO, None);
        post(&client, &mux).await;
        logit_pipeline::test_util::wait_until("the client closed the connection", || {
            mux.closed() == 1
        })
        .await;
        post(&client, &mux).await;
        assert_eq!(connections(&mux), [0, 1], "each request on its own connection");
    }

    /// [`hyper_client_builder`]'s pool closes a connection idle past `idle_timeout`, as
    /// [`build_client`]'s does: `otlp_out`'s gRPC client and `datadog_trace_out`'s socket client.
    #[tokio::test]
    async fn the_hyper_pool_closes_a_connection_idle_past_idle_timeout() {
        let mux = held::Mux::start(None).await;
        mux.release();
        let client = hyper_client_builder(SHORT_IDLE).build_http::<http_body_util::Full<Bytes>>();
        for expected in [1, 2] {
            let request = http::Request::post(mux.url())
                .body(http_body_util::Full::new(Bytes::from_static(b"x")))
                .unwrap();
            assert_eq!(client.request(request).await.unwrap().status(), 204);
            logit_pipeline::test_util::wait_until("the client closed the idle connection", || {
                mux.closed() == expected
            })
            .await;
        }
        assert_eq!(connections(&mux), [0, 1], "the second request opened a connection");
    }

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

    /// A refused dial took nothing, so its class is `connect_error`, apart from the
    /// `network_error` a request the destination may have taken counts.
    #[tokio::test]
    async fn a_refused_dial_counts_connect_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let client = build_client(Duration::from_secs(5), DEFAULT_IDLE_TIMEOUT, None);
        let exchange = exchange(client.post(format!("http://{addr}/")).body("x")).await;
        assert!(matches!(exchange.reply, Reply::Transport(_)), "{:?}", exchange.reply);
        assert_eq!(exchange.reply.class(), "connect_error");
    }

    /// A request the destination accepted the connection for and never answered may have been
    /// taken, so its timeout counts `network_error`.
    #[tokio::test]
    async fn a_timed_out_request_counts_network_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let held = tokio::spawn(async move {
            // Holds the connection open with no answer until the test ends.
            let _socket = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
        });
        let client = build_client(Duration::from_millis(200), DEFAULT_IDLE_TIMEOUT, None);
        let exchange = exchange(client.post(format!("http://{addr}/")).body("x")).await;
        held.abort();
        match &exchange.reply {
            Reply::Transport(err) => assert!(err.is_timeout(), "{err:?}"),
            other => panic!("expected a transport error, got {other:?}"),
        }
        assert_eq!(exchange.reply.class(), "network_error");
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
        for code in [408, 429, 500, 502, 503, 504, 599] {
            let status = reqwest::StatusCode::from_u16(code).unwrap();
            assert_eq!(classify_status(status), Fault::Ambiguous, "{code}");
        }
        for code in [300, 301, 307, 400, 402, 406, 413, 415, 422] {
            let status = reqwest::StatusCode::from_u16(code).unwrap();
            assert_eq!(classify_status(status), Fault::Rejected, "{code}");
        }
    }

    fn failed(fault: Fault) -> anyhow::Result<()> {
        Err(anyhow::anyhow!("request failed").context(fault))
    }

    /// Notes `result`, returning the send's next step and whether `on_rejected` ran.
    fn note(outcomes: &mut Outcomes, result: anyhow::Result<()>) -> (anyhow::Result<()>, bool) {
        let mut rejected = false;
        let next = outcomes.note(result, |_| rejected = true);
        (next, rejected)
    }

    #[test]
    fn a_send_with_no_requests_is_ok() {
        assert!(Outcomes::new(false).finish().is_ok());
    }

    #[test]
    fn accepted_requests_finish_ok() {
        let mut outcomes = Outcomes::new(false);
        let (next, rejected) = note(&mut outcomes, Ok(()));
        assert!(next.is_ok() && !rejected);
        assert!(outcomes.finish().is_ok());
    }

    /// A rejection beside an accepted request is counted, and the send still succeeds.
    #[test]
    fn a_rejection_continues_and_an_accepted_request_makes_the_send_ok() {
        for accepted_first in [true, false] {
            let mut outcomes = Outcomes::new(false);
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
        let mut outcomes = Outcomes::new(false);
        let first = Err(anyhow::anyhow!("first").context(Fault::Rejected));
        assert!(note(&mut outcomes, first).0.is_ok());
        assert!(note(&mut outcomes, failed(Fault::Rejected)).0.is_ok());
        let err = outcomes.finish().unwrap_err();
        assert_eq!(err.downcast_ref::<Fault>(), Some(&Fault::Rejected));
        assert!(format!("{err:#}").contains("first"), "the first rejection is kept: {err:#}");
    }

    /// A stop keeps the request's own fault whether or not a request was accepted before it, and
    /// isn't counted as a rejection.
    #[test]
    fn a_stop_keeps_the_requests_own_fault_after_an_accepted_request() {
        for fault in [Fault::Clean, Fault::Refused, Fault::Ambiguous] {
            for (accepted_before, accepted_first) in [(false, false), (false, true), (true, false)]
            {
                let mut outcomes = Outcomes::new(accepted_before);
                assert!(note(&mut outcomes, failed(Fault::Rejected)).0.is_ok());
                if accepted_first {
                    assert!(note(&mut outcomes, Ok(())).0.is_ok());
                }
                let (next, rejected) = note(&mut outcomes, failed(fault));
                assert!(!rejected, "a stop is not counted as a rejection");
                assert_eq!(
                    logit_pipeline::classify(&next.unwrap_err()),
                    fault,
                    "accepted before: {accepted_before}, accepted first: {accepted_first}"
                );
            }
        }
    }

    /// An earlier attempt's acceptance makes a send of rejections alone `Ok`; without one, the
    /// send fails `Rejected`.
    #[test]
    fn an_earlier_acceptance_makes_a_send_of_rejections_ok() {
        let mut outcomes = Outcomes::new(true);
        assert!(note(&mut outcomes, failed(Fault::Rejected)).0.is_ok());
        assert!(outcomes.finish().is_ok(), "an earlier attempt's acceptance makes the send Ok");
        let mut outcomes = Outcomes::new(false);
        assert!(note(&mut outcomes, failed(Fault::Rejected)).0.is_ok());
        assert_eq!(logit_pipeline::classify(&outcomes.finish().unwrap_err()), Fault::Rejected);
    }

    /// An error with no `Fault` is no destination verdict: it stops the send unchanged.
    #[test]
    fn an_unclassified_error_stops_the_send_unchanged() {
        let mut outcomes = Outcomes::new(false);
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
