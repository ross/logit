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
//! that.

use logit_pipeline::Fault;
use std::time::Duration;

/// Builds an HTTP sink's client. `tls` is `None` for the common case (no `tls:` block set) --
/// `reqwest`'s own default TLS configuration already trusts the bundled Mozilla root set for an
/// `https://` endpoint, so there's nothing to override. `Some` only once a sink has built a
/// customized `rustls::ClientConfig` from operator settings ([`crate::tls::build_client_config`]).
///
/// The `timeout` here is the client-wide default; both callers also set a per-request
/// `.timeout(..)`, which is what actually bounds a request they build.
pub(crate) fn build_client(
    timeout: Duration,
    tls: Option<&rustls::ClientConfig>,
) -> reqwest::Client {
    let mut builder = reqwest::Client::builder().timeout(timeout);
    if let Some(cfg) = tls {
        builder = builder.use_preconfigured_tls(cfg.clone());
    }
    builder.build().expect("reqwest client should build with the configured TLS settings")
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
