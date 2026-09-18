//! The pieces every sink that writes over `reqwest` shares: building the client, bucketing a
//! response status for telemetry, and turning a status or a transport error into a
//! [`logit_pipeline::Fault`].
//!
//! Extracted from `otlp.rs` when `prometheus_out` grew its remote-write sender
//! ([ADR `prometheus-remote-write`](../../../docs/adr/prometheus-remote-write.md)) -- a pure
//! refactor, no behaviour change for `otlp_out`. The point of sharing rather than copying is that
//! the remote-write sender's `Fault` table *is* `otlp_out`'s, deliberately and by name in that
//! ADR: 429 and 5xx are ambiguous, every other 4xx is permanent, only a connect failure is clean.
//! Two copies of one table are two things to drift.
//!
//! `influxdb.rs` keeps its own `status_class`/`classify_transport_error` pair on purpose -- its
//! module doc argues that sink's classification is its own to evolve, and nothing here disturbs
//! that. It builds its own client too, so [`build_client`]'s redirect policy doesn't reach it --
//! a gap worth closing separately rather than in passing here.

use logit_pipeline::Fault;
use std::time::Duration;

/// Builds an HTTP sink's client. `tls` is `None` for the common case (no `tls:` block set) --
/// `reqwest`'s own default TLS configuration already trusts the bundled Mozilla root set for an
/// `https://` endpoint, so there's nothing to override. `Some` only once a sink has built a
/// customized `rustls::ClientConfig` from operator settings ([`crate::tls::build_client_config`]).
///
/// The `timeout` here is the client-wide default; both callers also set a per-request
/// `.timeout(..)`, which is what actually bounds a request they build.
///
/// **Redirects are off**, overriding `reqwest`'s own `limited(10)` default, and that applies to
/// `otlp_out` as much as to `prometheus_out` -- both are write paths whose whole fault
/// classification assumes *one* request went to the *configured* URL:
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
/// With the policy off a `3xx` is simply a non-2xx: [`status_class`] buckets it `3xx`,
/// [`is_retryable_http_status`] leaves it permanent, and the operator sees the misconfiguration
/// reported against the URL they actually configured. Nothing legitimate is lost -- neither
/// remote-write nor OTLP defines a redirect, and an endpoint that has genuinely moved is a config
/// change, not something a sink should follow silently.
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

/// How much of a rejection body is worth reading, and worth carrying into an error message and a
/// diagnostic. Enough for Prometheus's own `400` text -- which names the offending series and why
/// -- and short enough that a receiver answering with an HTML error page doesn't fill a log line.
pub(crate) const ERROR_BODY_SNIPPET_BYTES: usize = 256;

/// At most `max` (plus a little slack) bytes of `response`'s body, decoded lossily -- for an error
/// path that wants to quote what the receiver said without buffering however much it decided to
/// say.
///
/// **A bounded read, not just a bounded message.** `Response::text()` buffers the whole body
/// first; it is bounded in *time* by the per-request timeout but not in bytes, so a receiver
/// answering `500` with an arbitrarily long body costs the sink that much allocation per attempt
/// -- and a sink getting 500s is exactly the one that keeps retrying. Stopping the read is what
/// makes the cost a constant.
///
/// The slack past `max` is what lets [`body_snippet`] tell a body that ended exactly at `max` from
/// one that carried on, so only the latter gets an ellipsis. A chunk boundary landing mid-character
/// is `from_utf8_lossy`'s to handle; `body_snippet` then cuts back to a character boundary anyway.
/// A read error is not propagated: the status is the primary signal and a partial body is still a
/// better hint than none.
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
/// anything was cut. Paired with [`read_body_prefix`], which has already bounded what reaches
/// here; this is the presentation half.
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

/// A coarse HTTP response-status bucket -- the `class` tag on `logit.output.requests`. Six values
/// and no more: a `429` is a `4xx` here, because splitting it out would contradict
/// [`is_retryable_http_status`], which reads the same status to decide the [`Fault`]; and a
/// timeout is not a status at all, so it lands under the literal `"network_error"` each caller
/// counts on its own transport-error arm.
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

    /// A body exactly at the bound keeps every byte; one past it is cut, and the cut lands on a
    /// character boundary rather than inside a multi-byte character.
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

    /// `status_class` and `is_retryable_http_status` must agree about a `3xx`: bucketed as its own
    /// class, and never retried -- which is what makes turning redirects off a *reported*
    /// misconfiguration rather than a silently retried one.
    #[test]
    fn a_3xx_is_its_own_class_and_is_not_retryable() {
        assert_eq!(status_class(reqwest::StatusCode::FOUND), "3xx");
        assert!(!is_retryable_http_status(reqwest::StatusCode::FOUND));
        assert!(is_retryable_http_status(reqwest::StatusCode::TOO_MANY_REQUESTS));
        assert!(is_retryable_http_status(reqwest::StatusCode::SERVICE_UNAVAILABLE));
        assert!(!is_retryable_http_status(reqwest::StatusCode::BAD_REQUEST));
    }
}
