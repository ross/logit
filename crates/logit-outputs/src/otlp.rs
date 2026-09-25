//! OTLP output: logs, metrics, and traces over OTLP/HTTP or OTLP/gRPC, chosen by `protocol`.
//!
//! | Transport | `POST` target | `content-type` |
//! |---|---|---|
//! | HTTP | [`Signal::path`] (`/v1/logs` etc.) or `paths:` | `application/x-protobuf` |
//! | gRPC | [`Signal::grpc_method`] (the service's `Export`) | `application/grpc+proto` |
//!
//! gRPC framing (`grpc_frame`/`grpc_unframe`, status parsing) is hand-rolled over `hyper`, not
//! `tonic` (`docs/adr/hand-rolled-grpc-over-hyper.md`); connection management is a pooled
//! `hyper_util` client over a `hyper-rustls` `HttpsConnector`, the TLS stack `reqwest` gives the
//! HTTP transport (`docs/adr/otlp-tls-and-pooled-grpc-client.md`). TLS is chosen by `endpoint`'s
//! scheme: `https://` means TLS under either `protocol`; `http://`, `grpc://`, or a bare
//! `host:port` means plaintext. `tls:` ([`TlsClientSettings`]) tunes a TLS connection and never
//! turns TLS on by itself.
//!
//! **One `send`, several requests.** An [`EventBatch`] mixes logs, metrics, and spans (ADR
//! `multi-payload-events`), but OTLP is three services, so `send` issues one request per
//! non-empty signal [`logit_proto::SignalEncoder::encode_signals`] returns, sequentially. That's
//! one reason [`OtlpOutput::duplicate_safe`] is `false`.
//!
//! **`Fault` classification.** The HTTP half is [`crate::http`]'s table, shared by name with
//! `prometheus_out`'s remote-write sender; the gRPC half is this module's `grpc_fault`:
//!
//! | Condition | `Fault` |
//! |---|---|
//! | Connect refused, DNS failure (the request never reached anything) | `Clean` |
//! | Request timeout | `Ambiguous` |
//! | HTTP 429 or any 5xx; gRPC `UNAVAILABLE`/`RESOURCE_EXHAUSTED`/`DEADLINE_EXCEEDED`/`ABORTED`/`INTERNAL` | `Ambiguous` |
//! | Any HTTP 3xx (redirects are off; [`crate::http::build_client`] says why) | `Permanent` |
//! | Any other HTTP 4xx; gRPC `INVALID_ARGUMENT`/`UNAUTHENTICATED`/`PERMISSION_DENIED`/`UNIMPLEMENTED` | `Permanent` |
//! | Any other gRPC status (never retry an unrecognized code) | `Permanent` |
//!
//! A non-2xx HTTP response's body is quoted in the error, read bounded to
//! [`crate::http::ERROR_BODY_SNIPPET_BYTES`] ([`crate::http::read_body_prefix`]).
//!
//! **Partial success.** A 2xx/`OK` response can still reject part of a batch through
//! `Export*ServiceResponse.partial_success`, parsed by hand ([`parse_partial_success`]) because
//! `logit-proto` doesn't generate the collector-service types. A partial rejection is still a
//! successful `send` (the accepted part landed; a retry would duplicate it), counted as
//! `logit.output.records.rejected{signal}` with a throttled `otlp_partial_success` warning.

use crate::Output;
use anyhow::Context;
use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Method};
use http_body_util::{BodyExt, Full};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client as GrpcClient;
use hyper_util::rt::TokioExecutor;
use logit_core::{Diagnostics, EventBatch, Telemetry};
use logit_pipeline::Fault;
use logit_proto::otlp::OtlpEncoder;
use logit_proto::{Signal, SignalEncoder};
// For the test module's TLS server (`test_server_tls_config`); client TLS lives in `crate::tls`.
#[cfg(test)]
use rustls_pki_types::pem::PemObject;
#[cfg(test)]
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

use crate::http::{
    body_snippet, build_client, classify_reqwest_error, is_retryable_http_status, read_body_prefix,
    status_class, ERROR_BODY_SNIPPET_BYTES,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// `reqwest` has no default timeout, and a server that accepts but never answers would hang
/// `send`; the same value as `influxdb_out`'s, by coincidence rather than coupling.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Which OTLP wire transport this sink speaks. Mirrors `logit_config::OtlpProtocol`, which
/// `logit-cli::pipeline::build_spec` translates, since this crate doesn't depend on
/// `logit-config` (`docs/design/pipeline-graph.md`'s "Crate layout").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpTransport {
    Http,
    Grpc,
}

/// Per-signal HTTP path overrides (`paths:` in config); `None` means [`Signal::path`]. HTTP-only:
/// gRPC method names are fixed by the `.proto` services, and graph rule 23 rejects a non-empty
/// `paths:` under `protocol: grpc`.
#[derive(Debug, Clone, Default)]
pub struct SignalPaths {
    pub logs: Option<String>,
    pub metrics: Option<String>,
    pub traces: Option<String>,
}

/// Whether request bodies are gzipped. Mirrors `logit_config::OtlpCompression`; see
/// `docs/adr/otlp-compression-and-decompression-bounds.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OtlpCompression {
    #[default]
    None,
    Gzip,
}

/// `crate::tls::TlsClientSettings`, shared with `logit_out`; re-exported here because
/// `logit-cli::pipeline::build_spec` names it as `logit_outputs::otlp::TlsClientSettings`.
pub use crate::tls::TlsClientSettings;

pub struct OtlpOutput {
    endpoint: String,
    transport: OtlpTransport,
    client: reqwest::Client,
    /// The gRPC transport's pooled, TLS-capable client
    /// (`docs/adr/otlp-tls-and-pooled-grpc-client.md`): one HTTP/2 connection reused across
    /// requests instead of a connect and handshake per request, plaintext or TLS. Built against
    /// the default trust store and rebuilt when [`OtlpOutput::with_tls`] sets a non-empty `tls:`.
    grpc_client: GrpcClient<HttpsConnector<HttpConnector>, Full<Bytes>>,
    request_timeout: Duration,
    encoder: OtlpEncoder,
    telemetry: Telemetry,
    diag: Diagnostics,
    /// Extra headers on every export request, both transports. Applied per request, never as
    /// `reqwest` `default_headers`: `with_timeout` rebuilds `client`, which would drop them if
    /// called afterward. `client` stays a function of timeout and TLS settings only.
    headers: HeaderMap,
    paths: SignalPaths,
    compression: OtlpCompression,
    /// `Some` once [`OtlpOutput::with_tls`] set a non-empty `tls:`. `None` means each transport's
    /// default TLS config, not "TLS off": the scheme decides TLS.
    tls: Option<rustls::ClientConfig>,
}

impl OtlpOutput {
    pub fn new(endpoint: String, transport: OtlpTransport) -> anyhow::Result<Self> {
        Ok(Self {
            endpoint,
            transport,
            client: build_client(DEFAULT_TIMEOUT, None),
            grpc_client: build_grpc_client(&default_client_tls_config()),
            request_timeout: DEFAULT_TIMEOUT,
            encoder: OtlpEncoder::new(),
            telemetry: Telemetry::default(),
            diag: Diagnostics::default(),
            headers: HeaderMap::new(),
            paths: SignalPaths::default(),
            compression: OtlpCompression::default(),
            tls: None,
        })
    }

    /// Overrides the default 10s request timeout. Over HTTP it's `reqwest`'s timeout; over gRPC
    /// it bounds the whole round trip (connect, handshake, request, response) with
    /// `tokio::time::timeout`, so `grpc_client` isn't rebuilt.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.client = build_client(timeout, self.tls.as_ref());
        self.request_timeout = timeout;
        self
    }

    /// Sets client-side TLS tuning (`tls:` in config): a private CA, a client certificate for
    /// mutual TLS, or no verification. A no-op if `settings` is empty; both transports already
    /// trust the bundled Mozilla roots for an `https://` endpoint.
    ///
    /// Graph rule 24 rejects a non-empty `tls:` on a non-`https://` endpoint and requires
    /// `cert_file`/`key_file` together; this still loads and validates every file, since
    /// `graph::resolve` never touches the filesystem.
    pub fn with_tls(
        mut self,
        settings: &TlsClientSettings,
        base_dir: &Path,
    ) -> anyhow::Result<Self> {
        if settings.is_empty() {
            return Ok(self);
        }
        if settings.insecure_skip_verify {
            self.diag.warn(
                "tls.insecure_skip_verify is set -- the connection is encrypted, but this \
                 output will accept any certificate the peer presents, self-signed or otherwise",
            );
        }
        let cfg = crate::tls::build_client_config(settings, base_dir)?;
        self.client = build_client(self.request_timeout, Some(&cfg));
        self.grpc_client = build_grpc_client(&cfg);
        self.tls = Some(cfg);
        Ok(self)
    }

    /// Sets the extra headers sent on every export request (`headers:` in config), e.g.
    /// `X-Scope-OrgID` for a multi-tenant Loki or Mimir.
    ///
    /// Graph rule 22 rejects a protocol-owned name (`content-type`, ...) and a case-insensitive
    /// duplicate before construction. This fails on what the graph can't check (illegal bytes,
    /// embedded newlines) and repeats the duplicate check as defense in depth: otherwise
    /// `HeaderMap::insert` would keep whichever of `X-Scope-OrgID`/`x-scope-orgid` the `HashMap`
    /// iterated last.
    pub fn with_headers(mut self, headers: &HashMap<String, String>) -> anyhow::Result<Self> {
        let mut map = HeaderMap::with_capacity(headers.len());
        for (name, value) in headers {
            let header_name = HeaderName::from_bytes(name.as_bytes())
                .with_context(|| format!("otlp_out: {name:?} is not a legal header name"))?;
            let header_value = HeaderValue::from_str(value)
                .with_context(|| format!("otlp_out: header {name:?} has an invalid value"))?;
            if map.insert(header_name, header_value).is_some() {
                anyhow::bail!(
                    "otlp_out: header {name:?} collides with another entry in 'headers' once \
                     case is ignored -- HTTP header names are case-insensitive, so which value \
                     would actually be sent is undefined"
                );
            }
        }
        self.headers = map;
        Ok(self)
    }

    /// Sets per-signal HTTP path overrides (`paths:` in config), for a backend with a non-standard
    /// OTLP mount point. No effect under gRPC, where graph rule 23 rejects it.
    pub fn with_paths(mut self, paths: SignalPaths) -> Self {
        self.paths = paths;
        self
    }

    /// Sets whether request bodies are gzipped (`compression:` in config). Never affects
    /// responses: this output never advertises accepting a compressed one
    /// (`docs/adr/otlp-compression-and-decompression-bounds.md`).
    pub fn with_compression(mut self, compression: OtlpCompression) -> Self {
        self.compression = compression;
        self
    }

    /// The HTTP path for `signal`: the config override, else [`Signal::path`].
    fn path_for(&self, signal: Signal) -> &str {
        let override_path = match signal {
            Signal::Logs => &self.paths.logs,
            Signal::Metrics => &self.paths.metrics,
            Signal::Traces => &self.paths.traces,
        };
        override_path.as_deref().unwrap_or_else(|| signal.path())
    }

    /// Attaches the diagnostics handle, to this output and to its encoder's lossy-metric paths.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag.clone();
        self.encoder = self.encoder.with_diagnostics(diag);
        self
    }

    /// Attaches the layer-3 telemetry handle, also threaded into the encoder for its
    /// lossy-metric-path counters (`logit-proto`'s `otlp::metrics` module doc).
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry.clone();
        self.encoder = self.encoder.with_telemetry(telemetry);
        self
    }

    fn record_partial_success(&mut self, signal: Signal, rejected: i64, error_message: &str) {
        if rejected > 0 {
            self.telemetry.count(
                "logit.output.records.rejected",
                rejected as f64,
                &[("signal", signal.as_str())],
            );
            self.diag.warn_throttled(
                "otlp_partial_success",
                format_args!(
                    "OTLP {} export partially rejected ({rejected} record(s)): {error_message}",
                    signal.as_str()
                ),
            );
        }
    }

    async fn send_http(&mut self, signal: Signal, payload: Bytes) -> anyhow::Result<()> {
        let url = format!("{}{}", self.endpoint.trim_end_matches('/'), self.path_for(signal));
        // Custom headers first, protocol-owned ones inserted after: `HeaderMap::insert` replaces,
        // so the fixed headers win even if rule 22 were bypassed. One `.headers(..)` call, never
        // a further `.header(..)`, which appends and would undo that.
        let mut headers = self.headers.clone();
        headers
            .insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/x-protobuf"));
        let payload = if self.compression == OtlpCompression::Gzip {
            headers.insert(http::header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
            Bytes::from(gzip(&payload))
        } else {
            payload
        };
        let result = self
            .client
            .post(&url)
            .headers(headers)
            .timeout(self.request_timeout)
            .body(payload)
            .send()
            .await;

        match result {
            Ok(resp) if resp.status().is_success() => {
                self.telemetry.count(
                    "logit.output.requests",
                    1.0,
                    &[("signal", signal.as_str()), ("class", status_class(resp.status()))],
                );
                let body = resp.bytes().await.unwrap_or_default();
                let (rejected, message) = parse_partial_success(&body);
                self.record_partial_success(signal, rejected, &message);
                Ok(())
            }
            Ok(resp) => {
                let status = resp.status();
                self.telemetry.count(
                    "logit.output.requests",
                    1.0,
                    &[("signal", signal.as_str()), ("class", status_class(status))],
                );
                // A bounded read, not `text()`: see `crate::http::read_body_prefix`.
                let text = body_snippet(
                    &read_body_prefix(resp, ERROR_BODY_SNIPPET_BYTES).await,
                    ERROR_BODY_SNIPPET_BYTES,
                );
                let fault = if is_retryable_http_status(status) {
                    Fault::Ambiguous
                } else {
                    Fault::Permanent
                };
                Err(anyhow::anyhow!(
                    "OTLP/HTTP {} write failed ({status}): {text}",
                    signal.as_str()
                ))
                .context(fault)
            }
            Err(err) => {
                self.telemetry.count(
                    "logit.output.requests",
                    1.0,
                    &[("signal", signal.as_str()), ("class", "network_error")],
                );
                let fault = classify_reqwest_error(&err);
                Err(anyhow::Error::new(err)).context(fault)
            }
        }
    }

    async fn send_grpc(&mut self, signal: Signal, payload: Bytes) -> anyhow::Result<()> {
        let base = normalize_grpc_endpoint(&self.endpoint);
        let outcome = tokio::time::timeout(
            self.request_timeout,
            grpc_roundtrip(
                &self.grpc_client,
                &base,
                signal,
                payload,
                &self.headers,
                self.compression,
            ),
        )
        .await;

        let (code, message, body) = match outcome {
            Ok(Ok(v)) => v,
            Ok(Err((fault, err))) => {
                self.telemetry.count(
                    "logit.output.requests",
                    1.0,
                    &[("signal", signal.as_str()), ("class", "network_error")],
                );
                return Err(err).context(fault);
            }
            Err(_) => {
                self.telemetry.count(
                    "logit.output.requests",
                    1.0,
                    &[("signal", signal.as_str()), ("class", "network_error")],
                );
                return Err(anyhow::anyhow!(
                    "OTLP/gRPC {} request to {base} timed out",
                    signal.as_str()
                ))
                .context(Fault::Ambiguous);
            }
        };

        self.telemetry.count(
            "logit.output.requests",
            1.0,
            &[("signal", signal.as_str()), ("class", grpc_status_class(code))],
        );

        if code == 0 {
            let (rejected, err_msg) = parse_partial_success(&body);
            self.record_partial_success(signal, rejected, &err_msg);
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "OTLP/gRPC {} write failed (grpc-status {code}): {message}",
                signal.as_str()
            ))
            .context(grpc_fault(code))
        }
    }
}

#[async_trait::async_trait]
impl Output for OtlpOutput {
    /// One attempt per request, no retry in the sink (`docs/adr/buffered-sink-delivery.md`). The
    /// first failing request aborts the rest; `write_loop` then retries the whole batch, which is
    /// why [`OtlpOutput::duplicate_safe`] matters here.
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let payloads = self.encoder.encode_signals(batch)?;
        for (signal, payload) in payloads {
            match self.transport {
                OtlpTransport::Http => self.send_http(signal, payload).await?,
                OtlpTransport::Grpc => self.send_grpc(signal, payload).await?,
            }
        }
        Ok(())
    }

    /// `false`, for two independent reasons, either sufficient:
    ///
    /// 1. **A multi-signal batch is several requests.** If the second of three fails, the retry
    ///    re-sends the whole batch, including the first request, which already succeeded.
    /// 2. **OTLP has no idempotency identity.** A replayed span is a second span; a replayed delta
    ///    `Sum` double-counts. InfluxDB's `(measurement, tag set, timestamp)` identity has no
    ///    equivalent here.
    ///
    /// So the default is at-most-once; `buffer: { delivery: at_least_once }` overrides it.
    fn duplicate_safe(&self) -> bool {
        false
    }
}

/// The gRPC transport's default trust: the bundled Mozilla roots `reqwest` uses for HTTP, via the
/// `ring` provider, never `aws-lc-rs` (`docs/adr/otlp-tls-and-pooled-grpc-client.md`). Lets the
/// pooled client dial `https://` with no `tls:` block. Infallible: `ring` always supports
/// TLS 1.2/1.3.
fn default_client_tls_config() -> rustls::ClientConfig {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .expect("the ring crypto provider always supports TLS 1.2/1.3")
        .with_root_certificates(roots)
        .with_no_client_auth()
}

/// Builds the gRPC transport's pooled client. `enable_http2` means prior-knowledge h2c for
/// `http://` and ALPN `h2` for `https://`; `https_or_http` lets one connector serve both, since
/// the scheme decides TLS. `tls.alpn_protocols` must be empty (`with_tls_config` panics
/// otherwise); neither `default_client_tls_config` nor `crate::tls::build_client_config` sets it.
///
/// `TCP_NODELAY` is on because h2 writes a request's HEADERS and DATA frames separately: with
/// Nagle's algorithm on, the DATA frame waits for the peer's delayed ACK of the HEADERS, which
/// adds about 40 ms to every export on Linux. `enforce_http(false)` is what `build()` sets too,
/// so the one connector still dials `https://`.
fn build_grpc_client(
    tls: &rustls::ClientConfig,
) -> GrpcClient<HttpsConnector<HttpConnector>, Full<Bytes>> {
    let mut http = HttpConnector::new();
    http.enforce_http(false);
    http.set_nodelay(true);
    let connector = HttpsConnectorBuilder::new()
        .with_tls_config(tls.clone())
        .https_or_http()
        .enable_http2()
        .wrap_connector(http);
    let mut builder = GrpcClient::builder(TokioExecutor::new());
    builder.http2_only(true);
    builder.build(connector)
}

/// Normalizes a gRPC `endpoint` into the absolute `http://`/`https://` base URI the pooled client
/// dispatches on. `grpc://` and a bare `host:port` mean plaintext and map to `http://`;
/// `http://`/`https://` are kept as written.
fn normalize_grpc_endpoint(endpoint: &str) -> String {
    let trimmed = endpoint.trim_end_matches('/');
    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("https://") || lower.starts_with("http://") {
        trimmed.to_string()
    } else if lower.starts_with("grpc://") {
        format!("http://{}", &trimmed[7..])
    } else {
        format!("http://{trimmed}")
    }
}

/// A gRPC status code's `class` tag. Only the codes the `Fault` table names get their own; the
/// rest are `"other"`.
fn grpc_status_class(code: u32) -> &'static str {
    match code {
        0 => "ok",
        3 => "invalid_argument",
        4 => "deadline_exceeded",
        7 => "permission_denied",
        8 => "resource_exhausted",
        10 => "aborted",
        12 => "unimplemented",
        13 => "internal",
        14 => "unavailable",
        16 => "unauthenticated",
        _ => "other",
    }
}

/// Maps a gRPC status code to a [`Fault`] per the module doc's table. An unlisted code is
/// `Permanent`, as in `logit_pipeline::classify`: never retry what isn't known to be transient.
fn grpc_fault(code: u32) -> Fault {
    match code {
        14 | 8 | 4 | 10 | 13 => Fault::Ambiguous, // UNAVAILABLE, RESOURCE_EXHAUSTED,
        // DEADLINE_EXCEEDED, ABORTED, INTERNAL
        _ => Fault::Permanent, // INVALID_ARGUMENT, UNAUTHENTICATED, PERMISSION_DENIED,
                               // UNIMPLEMENTED, and every other/unrecognized code
    }
}

/// One gRPC unary round trip: send one framed request, return the response payload and its
/// `grpc-status`/`grpc-message`.
///
/// The status is read from the response headers first (a Trailers-Only response, what a server
/// sends for an immediate failure with no body, e.g. Tempo rejecting an unknown method), then the
/// trailers (a normal unary response). No status at all is `Ambiguous`. Connect and TLS handshake
/// failures surface through `client.request`'s `Err`. `send_grpc` turns an `Err` into telemetry.
async fn grpc_roundtrip(
    client: &GrpcClient<HttpsConnector<HttpConnector>, Full<Bytes>>,
    base: &str,
    signal: Signal,
    payload: Bytes,
    headers: &HeaderMap,
    compression: OtlpCompression,
) -> Result<(u32, String, Bytes), (Fault, anyhow::Error)> {
    // Custom headers first, protocol-owned ones inserted after so they win; assigned wholesale
    // rather than via `.header(..)`, which appends (as in `send_http`).
    let mut req_headers = headers.clone();
    req_headers
        .insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/grpc+proto"));
    req_headers.insert(http::header::TE, HeaderValue::from_static("trailers"));
    // Always `identity`, whatever `compression` is: this is what's accepted back. OTLP
    // responses are tiny `partial_success` messages, so accepting gzip saves nothing and adds an
    // untrusted-decompression path (`docs/adr/otlp-compression-and-decompression-bounds.md`).
    req_headers.insert(
        HeaderName::from_static("grpc-accept-encoding"),
        HeaderValue::from_static("identity"),
    );
    let compressed = compression == OtlpCompression::Gzip;
    if compressed {
        req_headers
            .insert(HeaderName::from_static("grpc-encoding"), HeaderValue::from_static("gzip"));
    }

    let framed_payload = if compressed { gzip(&payload) } else { payload.to_vec() };
    let body = Full::new(Bytes::from(grpc_frame(&framed_payload, compressed)));
    let uri: http::Uri = format!("{base}{}", signal.grpc_method()).parse().map_err(|e| {
        (
            Fault::Permanent,
            anyhow::Error::new(e)
                .context(format!("building the OTLP/gRPC request URI from endpoint {base:?}")),
        )
    })?;
    let mut req = http::Request::builder()
        .method(Method::POST)
        .uri(uri)
        .body(body)
        .expect("a well-formed request always builds");
    *req.headers_mut() = req_headers;

    let res = client.request(req).await.map_err(|e| {
        // `is_connect()` covers refused, DNS failure, and a TLS handshake failure (the connector
        // does connect and handshake as one step): no application byte left, so `Clean`. A slow
        // attempt hits `send_grpc`'s timeout instead, which is `Ambiguous`.
        let fault = if e.is_connect() { Fault::Clean } else { Fault::Ambiguous };
        (fault, anyhow::Error::new(e).context("OTLP/gRPC request failed"))
    })?;

    let header_status = grpc_status_from(res.headers());
    let collected = res.into_body().collect().await.map_err(|e| {
        (Fault::Ambiguous, anyhow::Error::new(e).context("reading the OTLP/gRPC response failed"))
    })?;
    let trailer_status = collected.trailers().and_then(grpc_status_from);
    let Some((code, message)) = header_status.or(trailer_status) else {
        return Err((
            Fault::Ambiguous,
            anyhow::anyhow!("OTLP/gRPC {} response carried no grpc-status", signal.as_str()),
        ));
    };

    let framed = collected.to_bytes();
    let response_payload = grpc_unframe(&framed).unwrap_or(&[]);
    Ok((code, message, Bytes::copy_from_slice(response_payload)))
}

/// Reads `grpc-status`/`grpc-message` from headers or trailers. `None` if `grpc-status` is absent
/// or not an unsigned integer: "no status here", never "status 0".
fn grpc_status_from(headers: &HeaderMap) -> Option<(u32, String)> {
    let status = headers.get("grpc-status")?.to_str().ok()?.parse::<u32>().ok()?;
    let message =
        headers.get("grpc-message").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
    Some((status, message))
}

/// Frames `payload` as one gRPC message: `[compressed:u8][len:u32 BE][payload]`, the shape of
/// every gRPC-over-HTTP/2 message (`docs/adr/hand-rolled-grpc-over-hyper.md`). With `compressed`,
/// `payload` must already be compressed; the 5-byte header never is.
fn grpc_frame(payload: &[u8], compressed: bool) -> Vec<u8> {
    let mut buf = Vec::with_capacity(5 + payload.len());
    buf.push(u8::from(compressed));
    buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(payload);
    buf
}

/// The mirror of [`grpc_frame`]: the payload after the 5-byte header. `None` unless it's one
/// complete, uncompressed frame: a compressed flag is refused because `grpc_roundtrip` advertises
/// `grpc-accept-encoding: identity`, so a peer sending one anyway is broken.
fn grpc_unframe(bytes: &[u8]) -> Option<&[u8]> {
    if bytes.len() < 5 || bytes[0] != 0 {
        return None;
    }
    let len = u32::from_be_bytes(bytes[1..5].try_into().expect("checked len >= 5 above")) as usize;
    bytes.get(5..5 + len)
}

/// Gzips `payload` at the default level. Inline, not `spawn_blocking`: a few MiB of protobuf
/// compresses in well under a millisecond.
fn gzip(payload: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(payload).expect("writing to an in-memory Vec never fails");
    encoder.finish().expect("finishing an in-memory GzEncoder never fails")
}

/// Reads a protobuf varint at `buf[*pos]`, advancing `*pos`. `None` if truncated or longer than
/// a 64-bit varint can be.
fn read_varint(buf: &[u8], pos: &mut usize) -> Option<u64> {
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

/// Reads `partial_success.rejected_<signal>`/`.error_message` from an `Export*ServiceResponse`
/// (`crates/logit-proto/proto/opentelemetry/proto/collector/*/v1/*_service.proto`, not generated;
/// `logit-proto`'s `otlp` module doc says why). All three signals share the shape: field 1 is
/// `partial_success`, whose field 1 is a varint count and field 2 a string. An empty, absent, or
/// malformed one decodes as `(0, "")`, proto3's default, rather than failing a response over a
/// field used only for a warning.
fn parse_partial_success(bytes: &[u8]) -> (i64, String) {
    let mut pos = 0;
    while pos < bytes.len() {
        let Some(tag) = read_varint(bytes, &mut pos) else { break };
        let field = tag >> 3;
        let wire_type = tag & 0x7;
        match (field, wire_type) {
            (1, 2) => {
                let Some(len) = read_varint(bytes, &mut pos) else { break };
                let Some(end) = pos.checked_add(len as usize) else { break };
                let Some(sub) = bytes.get(pos..end) else { break };
                return parse_partial_success_message(sub);
            }
            _ => {
                if skip_field(bytes, &mut pos, wire_type).is_none() {
                    break;
                }
            }
        }
    }
    (0, String::new())
}

fn parse_partial_success_message(bytes: &[u8]) -> (i64, String) {
    let mut pos = 0;
    let mut rejected = 0i64;
    let mut message = String::new();
    while pos < bytes.len() {
        let Some(tag) = read_varint(bytes, &mut pos) else { break };
        let field = tag >> 3;
        let wire_type = tag & 0x7;
        match (field, wire_type) {
            (1, 0) => match read_varint(bytes, &mut pos) {
                Some(v) => rejected = v as i64,
                None => break,
            },
            (2, 2) => {
                let Some(len) = read_varint(bytes, &mut pos) else { break };
                let Some(end) = pos.checked_add(len as usize) else { break };
                let Some(s) = bytes.get(pos..end) else { break };
                pos = end;
                message = String::from_utf8_lossy(s).into_owned();
            }
            _ => {
                if skip_field(bytes, &mut pos, wire_type).is_none() {
                    break;
                }
            }
        }
    }
    (rejected, message)
}

/// Advances `pos` past one uninteresting field's value. `None` if it runs past the end of `bytes`
/// or is a group (wire types 3/4, deprecated and never emitted by an OTLP collector).
fn skip_field(bytes: &[u8], pos: &mut usize, wire_type: u64) -> Option<()> {
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
    use hyper_util::rt::TokioIo;
    use logit_core::{AttrMap, Event, MetricKind, MetricRecord, Registry, Resource};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn metric_batch() -> EventBatch {
        EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![Event::metric(
                1,
                AttrMap::new(),
                MetricRecord::new(logit_core::interner::intern("x"), MetricKind::counter(1.0)),
            )],
        }
    }

    fn all_three_signals_batch() -> EventBatch {
        let log = Event::log(
            1,
            AttrMap::new(),
            logit_core::LogRecord {
                message: logit_core::Value::str("hi"),
                severity: None,
                body_format: logit_core::BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        let metric = Event::metric(
            2,
            AttrMap::new(),
            MetricRecord::new(logit_core::interner::intern("m"), MetricKind::counter(1.0)),
        );
        let span = Event::span(
            3,
            AttrMap::new(),
            logit_core::SpanRecord {
                trace_id: [1; 16],
                span_id: [2; 8],
                parent_span_id: None,
                name: logit_core::Value::str("s"),
                kind: logit_core::SpanKind::Internal,
                status: logit_core::SpanStatus::Ok,
                events: Vec::new(),
                links: Vec::new(),
                end_timestamp: 4,
                flags: 0,
                ext: None,
            },
        );
        EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![log, metric, span],
        }
    }

    // ---- HTTP transport: a bare HTTP/1.1 canned-response peer ----

    async fn canned_http_server(
        responses: Vec<&'static str>,
    ) -> (std::net::SocketAddr, Arc<AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let count_task = count.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else { return };
                let i = count_task.fetch_add(1, Ordering::SeqCst);
                let response = responses.get(i).or(responses.last()).copied().unwrap_or("");
                let mut buf = [0u8; 8192];
                let _ =
                    tokio::time::timeout(Duration::from_millis(500), stream.read(&mut buf)).await;
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });
        (addr, count)
    }

    const RESP_200_EMPTY: &str =
        "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    const RESP_400: &str =
        "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    const RESP_429: &str =
        "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    const RESP_503: &str =
        "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

    fn http_output(addr: std::net::SocketAddr) -> OtlpOutput {
        OtlpOutput::new(format!("http://{addr}"), OtlpTransport::Http).unwrap()
    }

    #[tokio::test]
    async fn a_metrics_only_batch_issues_exactly_one_request_to_v1_metrics() {
        let (addr, count) = canned_http_server(vec![RESP_200_EMPTY]).await;
        let mut output = http_output(addr);
        output.send(&metric_batch()).await.expect("should succeed");
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_path_override_is_used_instead_of_the_otlp_standard_default() {
        let (addr, captured) = canned_http_server_capturing_request().await;
        let mut output = http_output(addr).with_paths(SignalPaths {
            metrics: Some("/otlp/v1/metrics".to_string()),
            ..Default::default()
        });
        output.send(&metric_batch()).await.expect("should succeed");

        let request = String::from_utf8_lossy(&captured.lock().unwrap()).to_string();
        assert!(request.starts_with("POST /otlp/v1/metrics "), "got request: {request}");
    }

    #[tokio::test]
    async fn a_signal_with_no_path_override_still_uses_the_otlp_standard_default() {
        let (addr, captured) = canned_http_server_capturing_request().await;
        let mut output = http_output(addr).with_paths(SignalPaths {
            logs: Some("/otlp/v1/logs".to_string()),
            ..Default::default()
        });
        output.send(&metric_batch()).await.expect("should succeed");

        let request = String::from_utf8_lossy(&captured.lock().unwrap()).to_string();
        assert!(request.starts_with("POST /v1/metrics "), "got request: {request}");
    }

    #[tokio::test]
    async fn a_batch_with_all_three_signals_issues_exactly_three_requests() {
        let (addr, count) =
            canned_http_server(vec![RESP_200_EMPTY, RESP_200_EMPTY, RESP_200_EMPTY]).await;
        let mut output = http_output(addr);
        output.send(&all_three_signals_batch()).await.expect("should succeed");
        assert_eq!(count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn a_gzip_compressed_body_is_sent_with_content_encoding_gzip() {
        let (addr, captured) = canned_http_server_capturing_request().await;
        let mut output = http_output(addr).with_compression(OtlpCompression::Gzip);
        output.send(&metric_batch()).await.expect("should succeed");

        let request = captured.lock().unwrap().clone();
        let text = String::from_utf8_lossy(&request).to_lowercase();
        assert!(text.contains("content-encoding: gzip"), "got request: {text}");

        let marker = b"\r\n\r\n";
        let body_start =
            request.windows(marker.len()).position(|w| w == marker).unwrap() + marker.len();
        let mut decompressed = Vec::new();
        std::io::Read::read_to_end(
            &mut flate2::read::GzDecoder::new(&request[body_start..]),
            &mut decompressed,
        )
        .expect("the body should be valid gzip");
        assert!(!decompressed.is_empty(), "decompressed body should contain the encoded protobuf");
    }

    #[tokio::test]
    async fn no_compression_sends_no_content_encoding_header() {
        let (addr, captured) = canned_http_server_capturing_request().await;
        let mut output = http_output(addr);
        output.send(&metric_batch()).await.expect("should succeed");

        let request = String::from_utf8_lossy(&captured.lock().unwrap()).to_lowercase();
        assert!(!request.contains("content-encoding"), "got request: {request}");
    }

    #[tokio::test]
    async fn a_503_response_is_classified_ambiguous() {
        let (addr, _count) = canned_http_server(vec![RESP_503]).await;
        let mut output = http_output(addr);
        let err = output.send(&metric_batch()).await.expect_err("a 503 should fail send");
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
    }

    #[tokio::test]
    async fn a_429_response_is_classified_ambiguous() {
        let (addr, _count) = canned_http_server(vec![RESP_429]).await;
        let mut output = http_output(addr);
        let err = output.send(&metric_batch()).await.expect_err("a 429 should fail send");
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
    }

    #[tokio::test]
    async fn a_400_response_is_classified_permanent() {
        let (addr, _count) = canned_http_server(vec![RESP_400]).await;
        let mut output = http_output(addr);
        let err = output.send(&metric_batch()).await.expect_err("a 400 should fail send");
        assert_eq!(logit_pipeline::classify(&err), Fault::Permanent);
    }

    #[tokio::test]
    async fn connect_refused_is_classified_clean() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let mut output = http_output(addr);
        let err = output.send(&metric_batch()).await.expect_err("should fail");
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
    }

    #[tokio::test]
    async fn a_request_timeout_is_classified_ambiguous() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { return };
                std::mem::forget(stream);
            }
        });
        let mut output = http_output(addr).with_timeout(Duration::from_millis(50));
        let err = output.send(&metric_batch()).await.expect_err("should time out");
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
    }

    #[tokio::test]
    async fn a_partial_success_response_is_counted_not_failed() {
        // partial_success { rejected_data_points: 2, error_message: "bad" }
        let mut sub = vec![0x08, 2, 0x12, 3]; // rejected = 2 (fits in one varint byte), len 3
        sub.extend_from_slice(b"bad");
        let mut body = vec![0x0a, sub.len() as u8];
        body.extend_from_slice(&sub);

        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let Ok((mut stream, _)) = listener.accept().await else { return };
            let mut buf = [0u8; 8192];
            let _ = tokio::time::timeout(Duration::from_millis(500), stream.read(&mut buf)).await;
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.write_all(&body).await;
            let _ = stream.shutdown().await;
        });

        let registry = Registry::new();
        let telemetry = registry.telemetry_for("out", "otlp_out", "sink");
        let mut output = http_output(addr).with_telemetry(telemetry);
        output.send(&metric_batch()).await.expect("a partial success is still Ok");

        let events = registry.drain(0);
        let rejected = events
            .iter()
            .find_map(|e| {
                e.metrics.iter().find_map(|m| match &m.kind {
                    MetricKind::Sum(s)
                        if logit_core::interner::resolve(m.name)
                            == "logit.output.records.rejected" =>
                    {
                        Some(s.value)
                    }
                    _ => None,
                })
            })
            .unwrap_or(0.0);
        assert_eq!(rejected, 2.0, "the rejected count should be counted, not silently dropped");
    }

    /// A raw TCP peer that captures request bytes verbatim, so a test can assert on the wire.
    async fn canned_http_server_capturing_request(
    ) -> (std::net::SocketAddr, Arc<std::sync::Mutex<Vec<u8>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured_task = captured.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else { return };
                let mut buf = [0u8; 8192];
                if let Ok(Ok(n)) =
                    tokio::time::timeout(Duration::from_millis(500), stream.read(&mut buf)).await
                {
                    captured_task.lock().unwrap().extend_from_slice(&buf[..n]);
                }
                let _ = stream.write_all(RESP_200_EMPTY.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });
        (addr, captured)
    }

    #[tokio::test]
    async fn a_custom_header_is_sent_on_the_http_request() {
        let (addr, captured) = canned_http_server_capturing_request().await;
        let mut output = http_output(addr)
            .with_headers(&HashMap::from([("X-Scope-OrgID".to_string(), "tenant-a".to_string())]))
            .unwrap();
        output.send(&metric_batch()).await.expect("should succeed");

        let request = String::from_utf8_lossy(&captured.lock().unwrap()).to_lowercase();
        assert!(request.contains("x-scope-orgid: tenant-a"), "got request: {request}");
    }

    #[tokio::test]
    async fn a_custom_header_does_not_override_content_type() {
        // Bypasses rule 22 to pin the defense in depth.
        let (addr, captured) = canned_http_server_capturing_request().await;
        let mut output = http_output(addr)
            .with_headers(&HashMap::from([("content-type".to_string(), "text/plain".to_string())]))
            .unwrap();
        output.send(&metric_batch()).await.expect("should succeed");

        let request = String::from_utf8_lossy(&captured.lock().unwrap()).to_lowercase();
        assert!(request.contains("content-type: application/x-protobuf"), "got request: {request}");
        assert!(!request.contains("text/plain"), "got request: {request}");
    }

    #[test]
    fn with_timeout_after_with_headers_keeps_the_headers() {
        let output = OtlpOutput::new("http://localhost:4318".to_string(), OtlpTransport::Http)
            .unwrap()
            .with_headers(&HashMap::from([("X-Scope-OrgID".to_string(), "tenant-a".to_string())]))
            .unwrap()
            .with_timeout(Duration::from_secs(5));
        assert_eq!(
            output.headers.get("x-scope-orgid").map(|v| v.to_str().unwrap()),
            Some("tenant-a")
        );
    }

    #[test]
    fn an_invalid_header_value_fails_construction() {
        // A `match`, not `unwrap_err`: `OtlpOutput` isn't `Debug`.
        let err = match OtlpOutput::new("http://localhost:4318".to_string(), OtlpTransport::Http)
            .unwrap()
            .with_headers(&HashMap::from([("X-Scope-OrgID".to_string(), "bad\nvalue".to_string())]))
        {
            Ok(_) => panic!("expected an invalid header value to fail construction"),
            Err(err) => err,
        };
        assert!(format!("{err:?}").contains("X-Scope-OrgID"), "got: {err:?}");
    }

    #[test]
    fn two_headers_differing_only_in_case_fail_construction_instead_of_silently_colliding() {
        let err = match OtlpOutput::new("http://localhost:4318".to_string(), OtlpTransport::Http)
            .unwrap()
            .with_headers(&HashMap::from([
                ("X-Scope-OrgID".to_string(), "tenant-a".to_string()),
                ("x-scope-orgid".to_string(), "tenant-b".to_string()),
            ])) {
            Ok(_) => panic!("expected case-colliding headers to fail construction"),
            Err(err) => err,
        };
        assert!(format!("{err:?}").contains("case is ignored"), "got: {err:?}");
    }

    #[test]
    fn otlp_output_reports_itself_not_duplicate_safe() {
        let output =
            OtlpOutput::new("http://localhost:4318".to_string(), OtlpTransport::Http).unwrap();
        assert!(
            !output.duplicate_safe(),
            "a multi-signal batch issues several requests (a mid-batch failure would re-send an \
             already-delivered signal on retry) and OTLP itself has no idempotency identity to \
             make a re-sent request a safe overwrite -- see this module's doc comment"
        );
    }

    // ---- gRPC transport: a raw HTTP/2 peer built with `hyper::server::conn::http2`. ----

    /// An HTTP/2 peer answering every unary call with the given status, message, and payload.
    async fn canned_grpc_server(
        status: u32,
        message: &'static str,
        payload: Vec<u8>,
    ) -> std::net::SocketAddr {
        use hyper::service::service_fn;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { return };
                let io = TokioIo::new(stream);
                let payload = payload.clone();
                tokio::spawn(async move {
                    let svc = service_fn(move |_req: http::Request<hyper::body::Incoming>| {
                        let payload = payload.clone();
                        async move {
                            let mut trailers = HeaderMap::new();
                            trailers.insert("grpc-status", status.to_string().parse().unwrap());
                            trailers.insert("grpc-message", message.parse().unwrap());
                            let frame = grpc_frame(&payload, false);
                            let body = TestGrpcBody {
                                data: Some(Bytes::from(frame)),
                                trailers: Some(trailers),
                            };
                            Ok::<_, std::convert::Infallible>(
                                http::Response::builder()
                                    .status(200)
                                    .header("content-type", "application/grpc+proto")
                                    .body(body)
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                        .serve_connection(io, svc)
                        .await;
                });
            }
        });
        addr
    }

    /// A response body yielding one data frame then one trailers frame; a test-only twin of
    /// `logit_inputs::otlp::GrpcBody`, since nothing else in this crate serves gRPC.
    struct TestGrpcBody {
        data: Option<Bytes>,
        trailers: Option<HeaderMap>,
    }

    impl hyper::body::Body for TestGrpcBody {
        type Data = Bytes;
        type Error = std::convert::Infallible;

        fn poll_frame(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<hyper::body::Frame<Bytes>, Self::Error>>> {
            if let Some(data) = self.data.take() {
                return std::task::Poll::Ready(Some(Ok(hyper::body::Frame::data(data))));
            }
            if let Some(trailers) = self.trailers.take() {
                return std::task::Poll::Ready(Some(Ok(hyper::body::Frame::trailers(trailers))));
            }
            std::task::Poll::Ready(None)
        }
    }

    fn grpc_output(addr: std::net::SocketAddr) -> OtlpOutput {
        OtlpOutput::new(addr.to_string(), OtlpTransport::Grpc).unwrap()
    }

    #[tokio::test]
    async fn a_grpc_ok_status_succeeds() {
        let addr = canned_grpc_server(0, "", Vec::new()).await;
        let mut output = grpc_output(addr);
        output.send(&metric_batch()).await.expect("grpc-status 0 should succeed");
    }

    /// `canned_grpc_server` that captures request headers and always answers `grpc-status: 0`.
    async fn canned_grpc_server_capturing_headers(
    ) -> (std::net::SocketAddr, Arc<std::sync::Mutex<Option<HeaderMap>>>) {
        use hyper::service::service_fn;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let captured = Arc::new(std::sync::Mutex::new(None));
        let captured_task = captured.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { return };
                let io = TokioIo::new(stream);
                let captured = captured_task.clone();
                tokio::spawn(async move {
                    let svc = service_fn(move |req: http::Request<hyper::body::Incoming>| {
                        *captured.lock().unwrap() = Some(req.headers().clone());
                        async move {
                            let mut trailers = HeaderMap::new();
                            trailers.insert("grpc-status", "0".parse().unwrap());
                            let body = TestGrpcBody {
                                data: Some(Bytes::from(grpc_frame(&[], false))),
                                trailers: Some(trailers),
                            };
                            Ok::<_, std::convert::Infallible>(
                                http::Response::builder()
                                    .status(200)
                                    .header("content-type", "application/grpc+proto")
                                    .body(body)
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                        .serve_connection(io, svc)
                        .await;
                });
            }
        });
        (addr, captured)
    }

    #[tokio::test]
    async fn a_custom_header_is_sent_on_the_grpc_request() {
        let (addr, captured) = canned_grpc_server_capturing_headers().await;
        let mut output = grpc_output(addr)
            .with_headers(&HashMap::from([("X-Scope-OrgID".to_string(), "tenant-a".to_string())]))
            .unwrap();
        output.send(&metric_batch()).await.expect("should succeed");

        let headers = captured.lock().unwrap().clone().expect("request should have been captured");
        assert_eq!(headers.get("x-scope-orgid").map(|v| v.to_str().unwrap()), Some("tenant-a"));
    }

    #[tokio::test]
    async fn a_custom_header_does_not_override_the_fixed_grpc_content_type() {
        let (addr, captured) = canned_grpc_server_capturing_headers().await;
        let mut output = grpc_output(addr)
            .with_headers(&HashMap::from([("content-type".to_string(), "text/plain".to_string())]))
            .unwrap();
        output.send(&metric_batch()).await.expect("should succeed");

        let headers = captured.lock().unwrap().clone().expect("request should have been captured");
        assert_eq!(
            headers.get("content-type").map(|v| v.to_str().unwrap()),
            Some("application/grpc+proto")
        );
    }

    /// Also captures the framed request body, to inspect the compressed flag and payload.
    async fn canned_grpc_server_capturing_request(
    ) -> (std::net::SocketAddr, Arc<std::sync::Mutex<Option<(HeaderMap, Bytes)>>>) {
        use hyper::service::service_fn;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let captured = Arc::new(std::sync::Mutex::new(None));
        let captured_task = captured.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { return };
                let io = TokioIo::new(stream);
                let captured = captured_task.clone();
                tokio::spawn(async move {
                    let svc = service_fn(move |req: http::Request<hyper::body::Incoming>| {
                        let captured = captured.clone();
                        async move {
                            let headers = req.headers().clone();
                            let body = req.into_body().collect().await.unwrap().to_bytes();
                            *captured.lock().unwrap() = Some((headers, body));
                            let mut trailers = HeaderMap::new();
                            trailers.insert("grpc-status", "0".parse().unwrap());
                            let resp_body = TestGrpcBody {
                                data: Some(Bytes::from(grpc_frame(&[], false))),
                                trailers: Some(trailers),
                            };
                            Ok::<_, std::convert::Infallible>(
                                http::Response::builder()
                                    .status(200)
                                    .header("content-type", "application/grpc+proto")
                                    .body(resp_body)
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                        .serve_connection(io, svc)
                        .await;
                });
            }
        });
        (addr, captured)
    }

    #[tokio::test]
    async fn a_gzip_compressed_grpc_request_sets_the_compressed_flag_and_header() {
        let (addr, captured) = canned_grpc_server_capturing_request().await;
        let mut output = grpc_output(addr).with_compression(OtlpCompression::Gzip);
        output.send(&metric_batch()).await.expect("should succeed");

        let (headers, body) =
            captured.lock().unwrap().clone().expect("request should have been captured");
        assert_eq!(headers.get("grpc-encoding").map(|v| v.to_str().unwrap()), Some("gzip"));
        assert_eq!(body[0], 1, "the frame's compressed flag should be set");

        let len = u32::from_be_bytes(body[1..5].try_into().unwrap()) as usize;
        let mut decompressed = Vec::new();
        std::io::Read::read_to_end(
            &mut flate2::read::GzDecoder::new(&body[5..5 + len]),
            &mut decompressed,
        )
        .expect("the framed payload should be valid gzip");
        assert!(
            !decompressed.is_empty(),
            "decompressed payload should contain the encoded protobuf"
        );
    }

    #[tokio::test]
    async fn no_compression_sends_an_uncompressed_grpc_frame() {
        let (addr, captured) = canned_grpc_server_capturing_request().await;
        let mut output = grpc_output(addr);
        output.send(&metric_batch()).await.expect("should succeed");

        let (headers, body) =
            captured.lock().unwrap().clone().expect("request should have been captured");
        assert!(headers.get("grpc-encoding").is_none());
        assert_eq!(body[0], 0, "the frame's compressed flag should not be set");
    }

    #[test]
    fn gzip_round_trips() {
        let payload = b"hello world, this is a protobuf-shaped payload";
        let compressed = gzip(payload);
        let mut decompressed = Vec::new();
        std::io::Read::read_to_end(
            &mut flate2::read::GzDecoder::new(&compressed[..]),
            &mut decompressed,
        )
        .unwrap();
        assert_eq!(decompressed, payload);
    }

    #[test]
    fn grpc_frame_sets_the_compressed_flag_when_asked() {
        let framed = grpc_frame(b"x", true);
        assert_eq!(framed[0], 1);
    }

    #[tokio::test]
    async fn grpc_unavailable_is_classified_ambiguous() {
        let addr = canned_grpc_server(14, "unavailable", Vec::new()).await;
        let mut output = grpc_output(addr);
        let err = output.send(&metric_batch()).await.expect_err("should fail");
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
    }

    #[tokio::test]
    async fn grpc_resource_exhausted_is_classified_ambiguous() {
        let addr = canned_grpc_server(8, "resource exhausted", Vec::new()).await;
        let mut output = grpc_output(addr);
        let err = output.send(&metric_batch()).await.expect_err("should fail");
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
    }

    #[tokio::test]
    async fn grpc_invalid_argument_is_classified_permanent() {
        let addr = canned_grpc_server(3, "invalid argument", Vec::new()).await;
        let mut output = grpc_output(addr);
        let err = output.send(&metric_batch()).await.expect_err("should fail");
        assert_eq!(logit_pipeline::classify(&err), Fault::Permanent);
    }

    #[tokio::test]
    async fn grpc_unimplemented_is_classified_permanent() {
        let addr = canned_grpc_server(12, "unimplemented", Vec::new()).await;
        let mut output = grpc_output(addr);
        let err = output.send(&metric_batch()).await.expect_err("should fail");
        assert_eq!(logit_pipeline::classify(&err), Fault::Permanent);
    }

    #[tokio::test]
    async fn grpc_partial_success_is_counted_not_failed() {
        let sub = vec![0x08, 1]; // rejected = 1
        let mut body = vec![0x0a, sub.len() as u8];
        body.extend_from_slice(&sub);

        let addr = canned_grpc_server(0, "", body).await;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("out", "otlp_out", "sink");
        let mut output = grpc_output(addr).with_telemetry(telemetry);
        output.send(&metric_batch()).await.expect("partial success is still Ok");

        let events = registry.drain(0);
        let rejected = events
            .iter()
            .find_map(|e| {
                e.metrics.iter().find_map(|m| match &m.kind {
                    MetricKind::Sum(s)
                        if logit_core::interner::resolve(m.name)
                            == "logit.output.records.rejected" =>
                    {
                        Some(s.value)
                    }
                    _ => None,
                })
            })
            .unwrap_or(0.0);
        assert_eq!(rejected, 1.0);
    }

    #[test]
    fn grpc_frame_and_unframe_round_trip() {
        let payload = b"hello world";
        let framed = grpc_frame(payload, false);
        assert_eq!(framed[0], 0, "uncompressed flag");
        assert_eq!(grpc_unframe(&framed), Some(&payload[..]));
    }

    #[test]
    fn grpc_unframe_rejects_a_short_buffer() {
        assert_eq!(grpc_unframe(&[0, 0, 0]), None);
    }

    #[test]
    fn grpc_unframe_rejects_a_set_compressed_flag() {
        let mut framed = grpc_frame(b"x", false);
        framed[0] = 1;
        assert_eq!(grpc_unframe(&framed), None);
    }

    #[test]
    fn parse_partial_success_on_an_empty_body_is_zero_and_empty() {
        assert_eq!(parse_partial_success(&[]), (0, String::new()));
    }

    /// A maximal varint length on `partial_success` decodes as none, not a `usize` overflow panic.
    #[test]
    fn parse_partial_success_does_not_panic_on_an_overflowing_declared_length() {
        // field 1 (partial_success), length-delimited, with the largest encodable varint length.
        let mut bytes = vec![0x0a];
        bytes.extend_from_slice(&[0xff; 9]);
        bytes.push(0x01); // 10-byte varint, decodes to a huge u64
        assert_eq!(parse_partial_success(&bytes), (0, String::new()));
    }

    /// The same overflow one level down, in `error_message`.
    #[test]
    fn parse_partial_success_message_does_not_panic_on_an_overflowing_declared_length() {
        let mut bytes = vec![0x12]; // field 2 (error_message), length-delimited
        bytes.extend_from_slice(&[0xff; 9]);
        bytes.push(0x01);
        assert_eq!(parse_partial_success_message(&bytes), (0, String::new()));
    }

    #[test]
    fn normalize_grpc_endpoint_maps_every_plaintext_spelling_to_http() {
        assert_eq!(normalize_grpc_endpoint("grpc://tempo:4317"), "http://tempo:4317");
        assert_eq!(normalize_grpc_endpoint("http://tempo:4317"), "http://tempo:4317");
        assert_eq!(normalize_grpc_endpoint("tempo:4317"), "http://tempo:4317");
        assert_eq!(normalize_grpc_endpoint("http://tempo:4317/"), "http://tempo:4317");
        assert_eq!(normalize_grpc_endpoint("GRPC://tempo:4317"), "http://tempo:4317");
    }

    /// `https://` (gRPC over TLS) is kept as written, not mapped to plaintext.
    #[test]
    fn normalize_grpc_endpoint_keeps_https_as_written() {
        assert_eq!(normalize_grpc_endpoint("https://tempo:4317"), "https://tempo:4317");
        assert_eq!(normalize_grpc_endpoint("https://tempo:4317/"), "https://tempo:4317");
        assert_eq!(normalize_grpc_endpoint("HTTPS://tempo:4317"), "HTTPS://tempo:4317");
    }

    #[test]
    fn constructing_an_otlp_output_with_https_and_protocol_grpc_now_succeeds() {
        // `an_https_grpc_endpoint_is_reachable_over_tls` proves the handshake itself.
        OtlpOutput::new("https://tempo:4317".to_string(), OtlpTransport::Grpc)
            .expect("https:// under protocol: grpc is now a TLS connection, not a hard error");
    }

    #[test]
    fn constructing_an_otlp_output_with_https_and_protocol_http_succeeds() {
        OtlpOutput::new("https://tempo:4318".to_string(), OtlpTransport::Http)
            .expect("https:// is exactly what protocol: http is for");
    }

    #[test]
    fn constructing_an_otlp_output_with_http_and_protocol_grpc_succeeds() {
        OtlpOutput::new("http://tempo:4317".to_string(), OtlpTransport::Grpc)
            .expect("http:// under protocol: grpc is a plaintext connection, not a downgrade");
    }

    // ---- TLS: canned `tokio-rustls`-wrapped HTTP and gRPC servers. ----

    fn test_tls_settings(overrides: impl FnOnce(&mut TlsClientSettings)) -> TlsClientSettings {
        let mut settings = TlsClientSettings::default();
        overrides(&mut settings);
        settings
    }

    fn testdata_dir() -> std::path::PathBuf {
        // The repo root's `testdata/tls` (`testdata/tls/README.md`).
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    /// A server config presenting `testdata/tls/server.{pem,key}`, optionally requiring a client
    /// certificate chaining to `testdata/tls/ca.pem`.
    fn test_server_tls_config(require_client_auth: bool) -> Arc<rustls::ServerConfig> {
        let dir = testdata_dir();
        let chain: Vec<CertificateDer<'static>> =
            CertificateDer::pem_file_iter(dir.join("server.pem"))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
        let key = PrivateKeyDer::from_pem_file(dir.join("server.key")).unwrap();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap();
        let mut cfg = if require_client_auth {
            let mut roots = rustls::RootCertStore::empty();
            let ca: Vec<CertificateDer<'static>> =
                CertificateDer::pem_file_iter(dir.join("ca.pem"))
                    .unwrap()
                    .collect::<Result<_, _>>()
                    .unwrap();
            roots.add_parsable_certificates(ca);
            let verifier =
                rustls::server::WebPkiClientVerifier::builder(Arc::new(roots)).build().unwrap();
            builder.with_client_cert_verifier(verifier).with_single_cert(chain, key).unwrap()
        } else {
            builder.with_no_client_auth().with_single_cert(chain, key).unwrap()
        };
        cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Arc::new(cfg)
    }

    /// A TLS peer answering `200 OK` to any request.
    async fn canned_tls_http_server(require_client_auth: bool) -> std::net::SocketAddr {
        let acceptor = tokio_rustls::TlsAcceptor::from(test_server_tls_config(require_client_auth));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { return };
                let Ok(tls_stream) = acceptor.accept(stream).await else { continue };
                let mut tls_stream = tls_stream;
                let mut buf = [0u8; 4096];
                let _ = tls_stream.read(&mut buf).await;
                let _ = tls_stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await;
                let _ = tls_stream.shutdown().await;
            }
        });
        addr
    }

    #[tokio::test]
    async fn an_https_http_endpoint_with_a_trusted_ca_file_succeeds() {
        let addr = canned_tls_http_server(false).await;
        let mut output = OtlpOutput::new(format!("https://{addr}"), OtlpTransport::Http)
            .unwrap()
            .with_tls(
                &test_tls_settings(|t| {
                    t.ca_file = Some("ca.pem".to_string());
                }),
                &testdata_dir(),
            )
            .unwrap();
        output.send(&metric_batch()).await.expect("a trusted CA should let the handshake succeed");
    }

    #[tokio::test]
    async fn an_https_http_endpoint_with_an_untrusted_ca_is_rejected_cleanly() {
        let addr = canned_tls_http_server(false).await;
        let mut output = OtlpOutput::new(format!("https://{addr}"), OtlpTransport::Http)
            .unwrap()
            .with_tls(
                &test_tls_settings(|t| {
                    t.ca_file = Some("other-ca.pem".to_string());
                }),
                &testdata_dir(),
            )
            .unwrap();
        let err = output.send(&metric_batch()).await.expect_err("an untrusted CA should fail");
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
    }

    #[tokio::test]
    async fn an_https_http_endpoint_with_insecure_skip_verify_succeeds_against_an_untrusted_ca() {
        let addr = canned_tls_http_server(false).await;
        let output = OtlpOutput::new(format!("https://{addr}"), OtlpTransport::Http)
            .unwrap()
            .with_tls(&test_tls_settings(|t| t.insecure_skip_verify = true), &testdata_dir());
        let mut output = output.unwrap();
        output.send(&metric_batch()).await.expect("insecure_skip_verify should bypass CA trust");
    }

    #[tokio::test]
    async fn an_https_grpc_endpoint_is_reachable_over_tls() {
        // A real handshake with ALPN `h2`, not only a construction check.
        let acceptor = tokio_rustls::TlsAcceptor::from(test_server_tls_config(false));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { return };
                let Ok(tls_stream) = acceptor.accept(stream).await else { continue };
                let io = TokioIo::new(tls_stream);
                let svc = hyper::service::service_fn(
                    move |_req: http::Request<hyper::body::Incoming>| async move {
                        let mut trailers = HeaderMap::new();
                        trailers.insert("grpc-status", "0".parse().unwrap());
                        let body = TestGrpcBody {
                            data: Some(Bytes::from(grpc_frame(&[], false))),
                            trailers: Some(trailers),
                        };
                        Ok::<_, std::convert::Infallible>(
                            http::Response::builder()
                                .status(200)
                                .header("content-type", "application/grpc+proto")
                                .body(body)
                                .unwrap(),
                        )
                    },
                );
                let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(io, svc)
                    .await;
            }
        });
        let mut output = OtlpOutput::new(format!("https://{addr}"), OtlpTransport::Grpc)
            .unwrap()
            .with_tls(
                &test_tls_settings(|t| {
                    t.ca_file = Some("ca.pem".to_string());
                }),
                &testdata_dir(),
            )
            .unwrap();
        output.send(&metric_batch()).await.expect("gRPC over TLS should round-trip");
    }

    #[tokio::test]
    async fn mutual_tls_succeeds_with_a_client_certificate_and_fails_without_one() {
        let addr = canned_tls_http_server(true).await;
        let with_cert = test_tls_settings(|t| {
            t.ca_file = Some("ca.pem".to_string());
            t.cert_file = Some("client.pem".to_string());
            t.key_file = Some("client.key".to_string());
        });
        let mut output = OtlpOutput::new(format!("https://{addr}"), OtlpTransport::Http)
            .unwrap()
            .with_tls(&with_cert, &testdata_dir())
            .unwrap();
        output.send(&metric_batch()).await.expect("a valid client certificate should be accepted");

        let addr = canned_tls_http_server(true).await;
        let without_cert = test_tls_settings(|t| t.ca_file = Some("ca.pem".to_string()));
        let mut output = OtlpOutput::new(format!("https://{addr}"), OtlpTransport::Http)
            .unwrap()
            .with_tls(&without_cert, &testdata_dir())
            .unwrap();
        output.send(&metric_batch()).await.expect_err("no client certificate should be rejected");
    }

    #[test]
    fn with_timeout_after_with_tls_keeps_tls() {
        let output = OtlpOutput::new("https://localhost:4318".to_string(), OtlpTransport::Http)
            .unwrap()
            .with_tls(
                &test_tls_settings(|t| t.ca_file = Some("ca.pem".to_string())),
                &testdata_dir(),
            )
            .unwrap()
            .with_timeout(Duration::from_secs(5));
        assert!(output.tls.is_some(), "with_timeout must not have cleared the TLS config");
    }

    #[test]
    fn with_tls_on_an_empty_settings_value_is_a_no_op() {
        let output = OtlpOutput::new("https://localhost:4318".to_string(), OtlpTransport::Http)
            .unwrap()
            .with_tls(&TlsClientSettings::default(), &testdata_dir())
            .unwrap();
        assert!(output.tls.is_none(), "an empty tls: block should not build a custom config");
    }

    #[test]
    fn with_tls_reports_a_missing_ca_file_with_the_path_in_the_error() {
        let err = match OtlpOutput::new("https://localhost:4318".to_string(), OtlpTransport::Http)
            .unwrap()
            .with_tls(
                &test_tls_settings(|t| t.ca_file = Some("does-not-exist.pem".to_string())),
                &testdata_dir(),
            ) {
            Ok(_) => panic!("a missing ca_file should fail construction"),
            Err(err) => err,
        };
        assert!(format!("{err:?}").contains("does-not-exist.pem"), "got: {err:?}");
    }
}
