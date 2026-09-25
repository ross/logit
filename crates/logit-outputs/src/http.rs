//! The pieces every sink that writes over `reqwest` shares: building the client, bucketing a
//! response status for telemetry, and turning a status or a transport error into a
//! [`logit_pipeline::Fault`].
//!
//! Shared, not copied, because `prometheus_out`'s remote-write sender's `Fault` table *is*
//! `otlp_out`'s, by name in
//! [ADR `prometheus-remote-write`](../../../docs/adr/prometheus-remote-write.md): 429 and 5xx are
//! ambiguous, every other 4xx is permanent, only a connect failure is clean. Two copies of one
//! table are two things to drift.
//!
//! [`split_encode`] is the request splitter `datadog_out` and `datadog_trace_out` share: both cut a
//! batch into requests under a per-route entry count and body size.
//!
//! `influxdb.rs` keeps its own `status_class`/`classify_transport_error` pair (the same table
//! today) and builds its own client, so [`build_client`]'s redirect policy doesn't reach it, a
//! tracked gap in `docs/known-gaps.md`.

use bytes::Bytes;
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
///   counted, a `405` (what Prometheus and Mimir answer for `GET` on a write path) drops it as
///   `Fault::Permanent`;
/// - a `307`/`308` replays the body *and* the operator's `headers:` at the `Location` host.
///   `reqwest` strips only `Authorization`/`Cookie`, and only when the host or port changes, so a
///   tenant header always travels and a same-host `https://` → `http://` downgrade carries
///   credentials in the clear -- past a config-time scheme check that has no say at runtime.
///
/// With the policy off a `3xx` is a non-2xx: [`status_class`] buckets it `3xx`,
/// [`is_retryable_http_status`] leaves it permanent, and the operator sees the misconfiguration
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

/// A coarse HTTP response-status bucket: the `class` tag on `logit.output.requests`.
///
/// Six values and no more. A `429` stays a `4xx`; [`is_retryable_http_status`] is what reads it
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

/// 429 and any 5xx are transient ([`Fault::Ambiguous`] -- the request reached the server and may
/// have been partly applied); every other 4xx is a configuration error ([`Fault::Permanent`]).
/// See `docs/adr/buffered-sink-delivery.md`'s table and each caller's own module doc.
pub(crate) fn is_retryable_http_status(status: reqwest::StatusCode) -> bool {
    status.is_server_error() || status.as_u16() == 429
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
pub(crate) fn split_encode<T: Copy, M>(
    items: &[T],
    caps: Caps,
    weight: impl Fn(&T) -> usize,
    mut encode: impl FnMut(&[T]) -> Option<Encoded<M>>,
) -> Split<T, M> {
    let mut split = Split { requests: Vec::new(), oversize: Vec::new() };
    let mut start = 0;
    let mut entries = 0usize;
    for (i, item) in items.iter().enumerate() {
        let w = weight(item);
        if i > start && entries.saturating_add(w) > caps.entries {
            fit(&items[start..i], caps, &mut encode, &mut split);
            start = i;
            entries = 0;
        }
        entries = entries.saturating_add(w);
    }
    if start < items.len() {
        fit(&items[start..], caps, &mut encode, &mut split);
    }
    split
}

/// [`split_encode`]'s byte-cap half, for one count-capped chunk.
fn fit<T: Copy, M, F: FnMut(&[T]) -> Option<Encoded<M>>>(
    items: &[T],
    caps: Caps,
    encode: &mut F,
    split: &mut Split<T, M>,
) {
    let Some(encoded) = encode(items) else { return };
    if encoded.raw_len <= caps.raw_bytes && encoded.body.len() <= caps.wire_bytes {
        split.requests.push((items.to_vec(), encoded));
    } else if let [item] = items {
        split.oversize.push((*item, encoded.raw_len, encoded.body.len()));
    } else {
        let mid = items.len() / 2;
        fit(&items[..mid], caps, encode, split);
        fit(&items[mid..], caps, encode, split);
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

    /// A `3xx` is its own class and never retried, so a redirect surfaces as a misconfiguration.
    #[test]
    fn a_3xx_is_its_own_class_and_is_not_retryable() {
        assert_eq!(status_class(reqwest::StatusCode::FOUND), "3xx");
        assert!(!is_retryable_http_status(reqwest::StatusCode::FOUND));
        assert!(is_retryable_http_status(reqwest::StatusCode::TOO_MANY_REQUESTS));
        assert!(is_retryable_http_status(reqwest::StatusCode::SERVICE_UNAVAILABLE));
        assert!(!is_retryable_http_status(reqwest::StatusCode::BAD_REQUEST));
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
        let split = split_encode(&[2, 2, 2, 9, 1], caps, |&w| w as usize, fake_encode);
        assert_eq!(chunks(&split), [vec![2, 2], vec![2], vec![9], vec![1]]);
        assert!(split.oversize.is_empty());
    }

    /// Over a byte cap the chunk bisects, and one item still over it is reported, not sent.
    #[test]
    fn the_splitter_bisects_over_a_byte_cap_and_reports_a_lone_oversize_item() {
        let caps = Caps { raw_bytes: 10, ..Caps::UNBOUNDED };
        let split = split_encode(&[4, 4, 4, 30, 1], caps, |_| 1, fake_encode);
        assert_eq!(chunks(&split), [vec![4, 4], vec![4], vec![1]]);
        assert_eq!(split.oversize, [(30, 30, 15)]);

        let caps = Caps { wire_bytes: 3, ..Caps::UNBOUNDED };
        let split = split_encode(&[4, 4, 8], caps, |_| 1, fake_encode);
        assert_eq!(chunks(&split), [vec![4], vec![4]]);
        assert_eq!(split.oversize, [(8, 8, 4)]);
    }

    /// A chunk the encoder has nothing for sends nothing.
    #[test]
    fn the_splitter_skips_a_chunk_that_encodes_to_nothing() {
        let split = split_encode(&[1, 2], Caps::UNBOUNDED, |_| 1, |_| None::<Encoded>);
        assert!(split.requests.is_empty() && split.oversize.is_empty());
    }
}
