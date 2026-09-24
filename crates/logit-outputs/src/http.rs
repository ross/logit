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
//! `influxdb.rs` keeps its own `status_class`/`classify_transport_error` pair (the same table
//! today) and builds its own client, so [`build_client`]'s redirect policy doesn't reach it, a
//! tracked gap in `docs/known-gaps.md`.

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
}
