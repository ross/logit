//! OTLP output -- exports logs, metrics, and traces to any OTLP-speaking backend, over either
//! OTLP/HTTP (protobuf-over-POST) or OTLP/gRPC, selected by `protocol` in config
//! (`logit_config::OtlpProtocol`). See `docs/adr/hand-rolled-grpc-over-hyper.md` for why the
//! gRPC transport's *framing* (`grpc_frame`/`grpc_unframe`, trailers/status parsing) is
//! hand-rolled against `hyper` rather than `tonic`, and
//! `docs/adr/otlp-tls-and-pooled-grpc-client.md` for why its *connection management* is not: the
//! gRPC transport's connect/handshake step is a pooled `hyper_util::client::legacy::Client` over a
//! `hyper-rustls` `HttpsConnector`, the same TLS stack the HTTP transport already gets from
//! `reqwest`. TLS is selected by `endpoint`'s scheme -- `https://` under either `protocol` means
//! TLS, `http://`/`grpc://` (or a bare `host:port`, which the gRPC transport treats the same as
//! `http://`) means plaintext -- and `tls:` in config (see [`TlsClientSettings`]) tunes an
//! already-TLS connection; it never turns TLS on by itself.
//!
//! **One `send` call, several HTTP/gRPC requests.** `logit`'s [`EventBatch`] mixes logs, metrics,
//! and spans on one `Event` (ADR `multi-payload-events`), but OTLP is three separate services -- so `send` calls
//! [`logit_proto::SignalEncoder::encode_signals`] once and issues one request per non-empty
//! signal it returns, sequentially, in whatever order `encode_signals` produced them. A batch with
//! only metrics issues exactly one request; a mixed batch issues up to three. This is also why
//! [`OtlpOutput::duplicate_safe`] is `false` -- see that method's doc comment.
//!
//! **`Fault` classification is this output's own**, not shared with [`crate::influxdb`]'s
//! `classify_transport_error`/`is_retryable_status` -- OTLP's status vocabulary (gRPC status codes
//! alongside HTTP ones) doesn't fit either helper as-is, and reusing them by coincidence would tie
//! two independently-evolving protocols' retry semantics together for no reason. The table:
//!
//! | Condition | `Fault` |
//! |---|---|
//! | Connect refused, DNS failure (the request never reached anything) | `Clean` |
//! | Request timeout | `Ambiguous` |
//! | HTTP 429 or any 5xx; gRPC `UNAVAILABLE`/`RESOURCE_EXHAUSTED`/`DEADLINE_EXCEEDED`/`ABORTED`/`INTERNAL` | `Ambiguous` |
//! | Any other HTTP 4xx; gRPC `INVALID_ARGUMENT`/`UNAUTHENTICATED`/`PERMISSION_DENIED`/`UNIMPLEMENTED` | `Permanent` |
//! | Any other gRPC status | `Permanent` (never retry a code this sink doesn't positively recognize) |
//!
//! **Partial success.** A 2xx/`OK` response can still say "I only accepted part of this" via OTLP's
//! own `Export*ServiceResponse.partial_success` field (`rejected_<signal>` + `error_message`) --
//! this output parses that by hand ([`parse_partial_success`]) rather than pulling in the generated
//! collector-service types `logit-proto` deliberately doesn't generate (see that crate's `otlp`
//! module doc: the wire shape is identical across all three signals, so one hand-rolled parser
//! covers it). A partial rejection still counts as a successful `send` -- the accepted portion
//! already landed, and retrying would duplicate it -- but is counted
//! (`logit.output.records.rejected{signal}`) and throttle-warned with the server's own message.

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
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// See [`crate::influxdb::DEFAULT_TIMEOUT`]'s doc comment -- same reasoning, a separate constant
/// because these are two unrelated outputs, not because the value differs.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Which OTLP wire transport this sink speaks. Mirrors `logit_config::OtlpProtocol` -- this crate
/// doesn't depend on `logit-config` (`docs/design/pipeline-graph.md`'s crate layout), so
/// `logit-cli::pipeline::build_spec` translates one into the other at construction time, the same
/// way it already turns `StdioTarget` into calls on `StdioOutput`'s own constructors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpTransport {
    Http,
    Grpc,
}

/// Per-signal HTTP path overrides (`paths:` in config) -- `None` means "use
/// [`Signal::path`]'s default." gRPC method names are fixed by the `.proto` service definitions
/// (`Signal::grpc_method`, shared with `otlp_in`'s router), so this is HTTP-only;
/// `logit-pipeline::graph::resolve`'s rule 21 rejects a non-empty `paths:` under `protocol: grpc`
/// rather than silently ignoring it.
#[derive(Debug, Clone, Default)]
pub struct SignalPaths {
    pub logs: Option<String>,
    pub metrics: Option<String>,
    pub traces: Option<String>,
}

/// Whether this output gzips its request bodies -- mirrors `logit_config::OtlpCompression`. See
/// `docs/adr/otlp-compression-and-decompression-bounds.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OtlpCompression {
    #[default]
    None,
    Gzip,
}

/// Mirrors `logit_config::TlsClientConfig` -- this crate doesn't depend on `logit-config`
/// (`docs/design/pipeline-graph.md`'s crate layout), the same reason [`SignalPaths`]/
/// [`OtlpCompression`] exist as local copies rather than re-exports. See that type's own doc
/// comment for what each field means; `logit-cli::pipeline::build_spec` converts one into the
/// other at construction time.
#[derive(Debug, Clone, Default)]
pub struct TlsClientSettings {
    pub ca_file: Option<String>,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
    pub insecure_skip_verify: bool,
}

impl TlsClientSettings {
    /// `true` if every field is at its default -- [`OtlpOutput::with_tls`]'s "was a `tls:` block
    /// actually set" check, mirroring `logit_config::TlsClientConfig::is_empty`.
    pub fn is_empty(&self) -> bool {
        self.ca_file.is_none()
            && self.cert_file.is_none()
            && self.key_file.is_none()
            && !self.insecure_skip_verify
    }
}

pub struct OtlpOutput {
    endpoint: String,
    transport: OtlpTransport,
    client: reqwest::Client,
    /// The gRPC transport's connection manager (`docs/adr/otlp-tls-and-pooled-grpc-client.md`) --
    /// a pooled, TLS-capable client replacing what used to be a raw `TcpStream::connect` and
    /// `hyper::client::conn::http2::handshake` per request. Built once at construction (against a
    /// default trust store) and rebuilt whenever [`OtlpOutput::with_tls`] sets a non-empty
    /// `tls:`; used for every gRPC request regardless of TLS, since the pooling benefit applies
    /// to plaintext gRPC too.
    grpc_client: GrpcClient<HttpsConnector<HttpConnector>, Full<Bytes>>,
    request_timeout: Duration,
    encoder: OtlpEncoder,
    telemetry: Telemetry,
    diag: Diagnostics,
    /// Extra headers sent on every export request, on both transports -- applied per request
    /// (`send_http`/`grpc_roundtrip`), never baked into `client` via `reqwest`'s
    /// `default_headers`. `with_timeout` rebuilds `client` unconditionally, so headers set via
    /// `default_headers` would silently vanish if `with_timeout` were called afterward -- an
    /// order-dependent bug no type would catch. A plain field sidesteps that entirely: `client`
    /// stays a pure function of the timeout (and now TLS settings), exactly the invariant
    /// `build_client` already assumes.
    headers: HeaderMap,
    paths: SignalPaths,
    compression: OtlpCompression,
    /// `Some` only once [`OtlpOutput::with_tls`] has set a non-empty `tls:` -- `None` means "use
    /// each transport's own default" (`reqwest`'s built-in TLS config for HTTP, a fresh
    /// [`default_client_tls_config`] for gRPC), not "TLS is off." TLS itself is always selected by
    /// `endpoint`'s scheme, on both transports; this only ever tunes an already-TLS connection.
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

    /// Overrides the default 10s request timeout (HTTP transport: `reqwest`'s own timeout; gRPC
    /// transport: the whole connect+handshake+request+response round trip, via
    /// `tokio::time::timeout` -- the pooled `grpc_client` itself carries no timeout, so this
    /// doesn't need to rebuild it).
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.client = build_client(timeout, self.tls.as_ref());
        self.request_timeout = timeout;
        self
    }

    /// Sets client-side TLS tuning (`tls:` in config) -- a private CA, a client certificate for
    /// mutual TLS, or disabling verification entirely. A no-op if `settings` is empty: both
    /// transports already default to a working TLS configuration (the bundled Mozilla root set)
    /// for an `https://` endpoint without this ever being called.
    /// `logit-pipeline::graph::resolve`'s rule 22 rejects a non-empty `tls:` on a non-`https://`
    /// endpoint before this ever runs, and requires `cert_file`/`key_file` together -- this method
    /// still loads and validates every file itself, since `graph::resolve` never touches the
    /// filesystem.
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
        let cfg = build_rustls_client_config(settings, base_dir)?;
        self.client = build_client(self.request_timeout, Some(&cfg));
        self.grpc_client = build_grpc_client(&cfg);
        self.tls = Some(cfg);
        Ok(self)
    }

    /// Sets the extra headers sent on every export request (`headers:` in config) -- e.g.
    /// `X-Scope-OrgID` for a multi-tenant Loki/Mimir/Grafana Cloud target. Fails if any name or
    /// value isn't a legal HTTP header (`logit-pipeline::graph::resolve`'s rule 20 rejects a
    /// protocol-owned name like `content-type` before construction ever sees it; this catches the
    /// lexical shape `graph` can't -- illegal bytes, embedded newlines). Also fails if two names
    /// collide once `HeaderName` normalizes their case (`X-Scope-OrgID`/`x-scope-orgid` are the
    /// same header on the wire) -- `HeaderMap::insert` would otherwise silently keep whichever of
    /// the two happened to be iterated last out of `headers`' arbitrary `HashMap` order, and
    /// `graph::resolve`'s own rule 20 check for this is the same defense-in-depth relationship as
    /// the reserved-name check above.
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

    /// Sets per-signal HTTP path overrides (`paths:` in config) -- for a backend using a
    /// non-standard OTLP mount point. gRPC method names are protocol-fixed, so this has no effect
    /// under `protocol: grpc`; `graph::resolve`'s rule 21 rejects a non-empty `paths:` there at
    /// config-validation time rather than silently ignoring it.
    pub fn with_paths(mut self, paths: SignalPaths) -> Self {
        self.paths = paths;
        self
    }

    /// Sets whether request bodies are gzipped (`compression:` in config). Never affects response
    /// handling -- this output never advertises accepting a compressed response on either
    /// transport, so `Gzip` only ever changes what it sends, not what it's willing to receive
    /// back (`docs/adr/otlp-compression-and-decompression-bounds.md`).
    pub fn with_compression(mut self, compression: OtlpCompression) -> Self {
        self.compression = compression;
        self
    }

    /// The HTTP path a request for `signal` is POSTed to -- the config override if one was set,
    /// else [`Signal::path`]'s OTLP-standard default.
    fn path_for(&self, signal: Signal) -> &str {
        let override_path = match signal {
            Signal::Logs => &self.paths.logs,
            Signal::Metrics => &self.paths.metrics,
            Signal::Traces => &self.paths.traces,
        };
        override_path.as_deref().unwrap_or_else(|| signal.path())
    }

    /// Attaches a component id to this output's own diagnostics and, via
    /// [`OtlpEncoder::with_diagnostics`], to its encoder's lossy-metric-path diagnostics.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag.clone();
        self.encoder = self.encoder.with_diagnostics(diag);
        self
    }

    /// Attaches a telemetry handle -- see [`crate::influxdb::InfluxDbOutput`]'s `telemetry` field
    /// for the layer-3 rationale. Also threaded into the encoder, for the lossy-metric-path
    /// counters (`docs/design/wire-protocol.md`... see `logit-proto`'s `otlp::metrics` module doc).
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
        // Built as one `HeaderMap`, custom headers cloned in first and the protocol-owned ones
        // inserted after -- `HeaderMap::insert` unconditionally replaces any prior value for that
        // key (unlike `RequestBuilder::header`, which appends), so the fixed headers always win
        // even if `graph::resolve`'s reserved-name rule (rule 20) were ever bypassed. One
        // `.headers(..)` call rather than mixing it with further `.header(..)` calls, whose
        // append-not-replace semantics would otherwise undo this guarantee.
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
                let text = resp.text().await.unwrap_or_default();
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
    /// Exactly one attempt per request, same "no retry in a sink" contract every other output
    /// follows (`docs/adr/buffered-sink-delivery.md`) -- but here that's per-*request*, not
    /// per-`send` call: a batch producing three signals issues three requests, and the first
    /// failure aborts the rest without attempting them. `write_loop` sees this as one failed
    /// `send` and retries the whole batch, which is exactly what makes duplicate-safety a real
    /// question here (see [`OtlpOutput::duplicate_safe`]).
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

    /// `false`, unlike [`crate::influxdb::InfluxDbOutput`] -- for two independent reasons, either
    /// one sufficient on its own:
    ///
    /// 1. **A batch carrying more than one signal issues more than one request.** If the second of
    ///    three requests fails, `write_loop`'s retry re-sends the *whole batch* -- including the
    ///    first signal's request, which already succeeded. That request is not idempotent (see
    ///    the next point), so the retry duplicates it.
    /// 2. **OTLP has no idempotency identity at all.** A span has no "already delivered" marker a
    ///    backend can dedupe on, so replaying one creates a second, distinct span in the trace. A
    ///    delta `Sum`/`Counter` has no identity either, so replaying one double-counts it at the
    ///    backend -- unlike InfluxDB line protocol's `(measurement, tag set, timestamp)` identity,
    ///    which makes a re-sent InfluxDB point an idempotent overwrite rather than a duplicate.
    ///
    /// `AtMostOnce` (this method's `false`) is therefore the correct default. An operator who
    /// wants at-least-once delivery anyway already has `buffer: { delivery: at_least_once }` --
    /// no new code needed for that override.
    fn duplicate_safe(&self) -> bool {
        false
    }
}

/// Builds the HTTP transport's client. `tls` is `None` for the common case (no `tls:` block set)
/// -- `reqwest`'s own default TLS configuration already trusts the bundled Mozilla root set for
/// an `https://` endpoint, so there's nothing to override. `Some` only once
/// [`OtlpOutput::with_tls`] has built a customized `rustls::ClientConfig`.
fn build_client(timeout: Duration, tls: Option<&rustls::ClientConfig>) -> reqwest::Client {
    let mut builder = reqwest::Client::builder().timeout(timeout);
    if let Some(cfg) = tls {
        builder = builder.use_preconfigured_tls(cfg.clone());
    }
    builder.build().expect("reqwest client should build with the configured TLS settings")
}

/// The gRPC transport's default trust: the same bundled Mozilla root set `reqwest`'s own default
/// TLS configuration uses for the HTTP transport, via the `ring` crypto provider (never
/// `aws-lc-rs` -- `docs/adr/otlp-tls-and-pooled-grpc-client.md`). Built once at [`OtlpOutput::new`]
/// so the gRPC transport's pooled client exists (and can dial an `https://` endpoint) even when no
/// `tls:` block is ever set; superseded by [`build_rustls_client_config`]'s output once one is.
/// Infallible: `with_safe_default_protocol_versions` only fails if the provider supports no usable
/// cipher suite for TLS 1.2/1.3, which `ring`'s bundled suite list never triggers.
fn default_client_tls_config() -> rustls::ClientConfig {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .expect("the ring crypto provider always supports TLS 1.2/1.3")
        .with_root_certificates(roots)
        .with_no_client_auth()
}

/// Builds a `rustls::ClientConfig` from a non-empty [`TlsClientSettings`] -- the customized
/// counterpart to [`default_client_tls_config`]. Every path is resolved against `base_dir` (the
/// config file's own directory) first, exactly as `logit-cli::pipeline::build_spec` resolves
/// `lua_file`/`stdio_out`'s `path`.
fn build_rustls_client_config(
    settings: &TlsClientSettings,
    base_dir: &Path,
) -> anyhow::Result<rustls::ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .expect("the ring crypto provider always supports TLS 1.2/1.3");

    // Both arms land in the same `WantsClientCert` builder state -- `with_root_certificates` and
    // `dangerous().with_custom_certificate_verifier` are just two different ways to supply a
    // verifier -- so client-cert material (below) is layered on identically either way.
    // `graph::resolve`'s rule 22 already rejects `insecure_skip_verify` together with `ca_file`,
    // so this crate doesn't need to re-reject that combination; `insecure_skip_verify` together
    // with a client certificate is legal (mTLS with no server verification) and reaches the
    // `with_client_auth_cert` branch below like any other case.
    let builder = if settings.insecure_skip_verify {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCert((*provider).clone())))
    } else {
        let mut roots = rustls::RootCertStore::empty();
        match &settings.ca_file {
            Some(ca_file) => {
                let path = base_dir.join(ca_file);
                let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&path)
                    .with_context(|| format!("otlp_out: reading tls.ca_file {}", path.display()))?
                    .collect::<Result<_, _>>()
                    .with_context(|| format!("otlp_out: parsing tls.ca_file {}", path.display()))?;
                roots.add_parsable_certificates(certs);
            }
            None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
        }
        builder.with_root_certificates(roots)
    };

    match (&settings.cert_file, &settings.key_file) {
        (Some(cert_file), Some(key_file)) => {
            let cert_path = base_dir.join(cert_file);
            let key_path = base_dir.join(key_file);
            let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&cert_path)
                .with_context(|| {
                    format!("otlp_out: reading tls.cert_file {}", cert_path.display())
                })?
                .collect::<Result<_, _>>()
                .with_context(|| {
                    format!("otlp_out: parsing tls.cert_file {}", cert_path.display())
                })?;
            let key = PrivateKeyDer::from_pem_file(&key_path).with_context(|| {
                format!("otlp_out: reading tls.key_file {}", key_path.display())
            })?;
            Ok(builder.with_client_auth_cert(chain, key)?)
        }
        _ => Ok(builder.with_no_client_auth()),
    }
}

/// A [`rustls::client::danger::ServerCertVerifier`] that accepts any certificate the peer
/// presents -- `tls.insecure_skip_verify`'s implementation. The connection is still encrypted;
/// only the "is this actually who I meant to talk to" check is skipped. Still verifies the
/// handshake *signature* itself via `provider`'s own algorithms (`verify_tls12_signature`/
/// `verify_tls13_signature`) -- only certificate-chain and hostname validation are skipped, not
/// cryptographic signature verification.
#[derive(Debug)]
struct AcceptAnyServerCert(rustls::crypto::CryptoProvider);

impl rustls::client::danger::ServerCertVerifier for AcceptAnyServerCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls_pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls_pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// Builds the gRPC transport's pooled, TLS-capable connection manager -- `enable_http2` selects
/// prior-knowledge h2c for a plaintext `http://` target and ALPN `h2` for an `https://` one;
/// `https_or_http` (not `https_only`) is what lets the same connector serve both, since a single
/// `OtlpOutput` only fixes its transport (HTTP vs. gRPC), not TLS-vs-plaintext, which is decided
/// per-endpoint. `tls`'s `alpn_protocols` must be empty when passed in -- `with_tls_config`
/// panics otherwise -- which both `default_client_tls_config` and `build_rustls_client_config`
/// satisfy by construction (neither ever sets it); `enable_http2` fills it in.
fn build_grpc_client(
    tls: &rustls::ClientConfig,
) -> GrpcClient<HttpsConnector<HttpConnector>, Full<Bytes>> {
    let connector = HttpsConnectorBuilder::new()
        .with_tls_config(tls.clone())
        .https_or_http()
        .enable_http2()
        .build();
    let mut builder = GrpcClient::builder(TokioExecutor::new());
    builder.http2_only(true);
    builder.build(connector)
}

/// Normalizes a gRPC `endpoint` into an absolute `http://`/`https://` base URI -- what the pooled
/// `grpc_client` needs to dispatch a request on scheme, unlike the old per-request
/// `TcpStream::connect`, which only ever needed a bare authority. `grpc://` (this output's own
/// historical spelling for "plaintext gRPC") and a bare `host:port` with no scheme (the demo's own
/// habit) both mean plaintext and map to `http://`; `http://`/`https://` are kept as written --
/// `https://` now means TLS rather than being rejected outright
/// (`docs/adr/otlp-tls-and-pooled-grpc-client.md`).
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

/// A coarse HTTP response-status bucket -- see `crate::influxdb::status_class`'s identical
/// reasoning; duplicated rather than shared because these are two independently-evolving outputs.
fn status_class(status: reqwest::StatusCode) -> &'static str {
    match status.as_u16() / 100 {
        1 => "1xx",
        2 => "2xx",
        3 => "3xx",
        4 => "4xx",
        5 => "5xx",
        _ => "other",
    }
}

/// 429 and any 5xx are transient (`Fault::Ambiguous`); every other 4xx is a configuration error
/// (`Fault::Permanent`) -- see this module's doc comment table.
fn is_retryable_http_status(status: reqwest::StatusCode) -> bool {
    status.is_server_error() || status.as_u16() == 429
}

/// See `crate::influxdb::classify_transport_error`'s doc comment -- same underlying
/// `reqwest::Error::is_connect()` distinction, duplicated (not shared) per this module's own doc
/// comment on why `Fault` classification isn't shared between the two outputs.
fn classify_reqwest_error(err: &reqwest::Error) -> Fault {
    if err.is_connect() {
        Fault::Clean
    } else {
        Fault::Ambiguous
    }
}

/// A telemetry-tag-friendly name for a gRPC status code -- only the codes this module's Fault
/// table cares about get their own name; anything else (a code that's valid gRPC but not one
/// `otlp_out` treats specially) buckets as `"other"`, mirroring `status_class`'s `"other"` arm.
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

/// Maps a gRPC status code to a [`Fault`] -- this module's doc comment table. Anything not
/// explicitly listed defaults to `Fault::Permanent`, the same reasoning
/// `logit_pipeline::classify`'s own unclassified-error default uses: never retry a status this
/// sink doesn't positively recognize as transient.
fn grpc_fault(code: u32) -> Fault {
    match code {
        14 | 8 | 4 | 10 | 13 => Fault::Ambiguous, // UNAVAILABLE, RESOURCE_EXHAUSTED,
        // DEADLINE_EXCEEDED, ABORTED, INTERNAL
        _ => Fault::Permanent, // INVALID_ARGUMENT, UNAUTHENTICATED, PERMISSION_DENIED,
                               // UNIMPLEMENTED, and every other/unrecognized code
    }
}

/// One full gRPC unary round trip over the pooled `client`: send one framed request, and read
/// back the framed response payload plus its `grpc-status`/`grpc-message` -- checked in the
/// response's headers first (a "Trailers-Only" response, the shape a server sends for an
/// immediate failure with no message body -- e.g. Tempo rejecting an unknown method) and its
/// trailers second (the shape a successful, or gracefully-failed-after-a-response, unary call
/// uses). `send_grpc` is the only caller, and owns turning an `Err` here into telemetry plus the
/// final classified `anyhow::Result` -- this only ever returns `(fault, err)` pairs, never
/// classifies on its own. Unlike the old per-request `TcpStream::connect` this replaced
/// (`docs/adr/otlp-tls-and-pooled-grpc-client.md`), connect/handshake failures (including a TLS
/// handshake failure) surface through `client.request`'s own `Err` rather than a separate step.
async fn grpc_roundtrip(
    client: &GrpcClient<HttpsConnector<HttpConnector>, Full<Bytes>>,
    base: &str,
    signal: Signal,
    payload: Bytes,
    headers: &HeaderMap,
    compression: OtlpCompression,
) -> Result<(u32, String, Bytes), (Fault, anyhow::Error)> {
    // Built as one `HeaderMap`, custom headers cloned in first and the protocol-owned ones
    // inserted after -- `HeaderMap::insert` unconditionally replaces any prior value for that
    // key, so these three always win even if `graph::resolve`'s reserved-name rule (rule 20)
    // were ever bypassed. Assigned onto the request wholesale rather than via the builder's own
    // `.header(..)` (which appends, not replaces), for the same reason as `send_http`.
    let mut req_headers = headers.clone();
    req_headers
        .insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/grpc+proto"));
    req_headers.insert(http::header::TE, HeaderValue::from_static("trailers"));
    // Always `identity` here, regardless of `compression` -- this is what this output is willing
    // to *accept back*, a separate question from what it sends. Never advertising gzip means a
    // compliant server never compresses its response, so `grpc_unframe` below never has to
    // inflate one -- OTLP responses are single-digit-byte `partial_success` messages, so there's
    // nothing worth saving by accepting a compressed one, only an inbound untrusted-decompression
    // path to gain (`docs/adr/otlp-compression-and-decompression-bounds.md`).
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
        // `is_connect()` covers connect refused, DNS/name-resolution failure, and (since the
        // `HttpsConnector`'s `call` does connect+TLS-handshake as one step) a TLS handshake
        // failure -- none of those cases got as far as a real destination seeing an application
        // byte, so there's nothing to duplicate on retry. A *slow* (rather than outright failing)
        // attempt is caught by `send_grpc`'s overall per-request timeout instead, which
        // classifies as `Fault::Ambiguous`.
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

/// Reads `grpc-status`/`grpc-message` out of a header map (either a response's own headers, for a
/// Trailers-Only failure, or its real trailers). `None` if `grpc-status` is absent or not a valid
/// unsigned integer -- the caller treats that as "no status here," not "status 0."
fn grpc_status_from(headers: &HeaderMap) -> Option<(u32, String)> {
    let status = headers.get("grpc-status")?.to_str().ok()?.parse::<u32>().ok()?;
    let message =
        headers.get("grpc-message").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
    Some((status, message))
}

/// Frames `payload` as one unary gRPC message: `[compressed:u8][len:u32 BE][payload]` -- the wire
/// shape every gRPC-over-HTTP/2 message body uses, request or response
/// (`docs/adr/hand-rolled-grpc-over-hyper.md`). `payload` must already be compressed if
/// `compressed` is `true` -- only the 5-byte header is ever left uncompressed, per gRPC's own
/// framing (`docs/adr/otlp-compression-and-decompression-bounds.md`).
fn grpc_frame(payload: &[u8], compressed: bool) -> Vec<u8> {
    let mut buf = Vec::with_capacity(5 + payload.len());
    buf.push(u8::from(compressed));
    buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(payload);
    buf
}

/// The mirror of [`grpc_frame`]: strips the 5-byte header and returns the payload slice. `None`
/// for anything short of one complete, uncompressed frame -- a body shorter than 5 bytes, a
/// compressed-flag byte other than `0` (this output never *accepts* a compressed response --
/// `grpc_roundtrip` always advertises `grpc-accept-encoding: identity`, so a peer that sends one
/// anyway is either broken or lying about its own header, `docs/adr/
/// otlp-compression-and-decompression-bounds.md`), or a declared length longer than what's
/// actually present.
fn grpc_unframe(bytes: &[u8]) -> Option<&[u8]> {
    if bytes.len() < 5 || bytes[0] != 0 {
        return None;
    }
    let len = u32::from_be_bytes(bytes[1..5].try_into().expect("checked len >= 5 above")) as usize;
    bytes.get(5..5 + len)
}

/// Gzips `payload` at its default compression level -- used by both transports' compressed path
/// (`send_http`, `grpc_roundtrip`). A few MiB of protobuf compresses in well under a millisecond,
/// so this runs inline rather than via `spawn_blocking`.
fn gzip(payload: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(payload).expect("writing to an in-memory Vec never fails");
    encoder.finish().expect("finishing an in-memory GzEncoder never fails")
}

/// Reads a protobuf varint starting at `buf[*pos]`, advancing `*pos` past it. `None` on a
/// truncated or pathologically long (more than 10 bytes -- the most a 64-bit varint ever needs)
/// varint, either of which means malformed input rather than a real field.
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

/// Parses just enough of an `Export*ServiceResponse` (the OTLP collector wire shape --
/// `crates/logit-proto/proto/opentelemetry/proto/collector/*/v1/*_service.proto`, none of which
/// `logit-proto` generates types for -- see that crate's `otlp` module doc for why) to read back
/// `partial_success.rejected_<signal>`/`.error_message`. All three signals' response messages are
/// wire-identical in the one respect this cares about: field 1 is `partial_success`
/// (length-delimited), itself field 1 a varint count and field 2 a string message -- so one
/// hand-rolled parser covers logs, metrics, and traces alike. An empty, absent, or malformed
/// `partial_success` decodes as `(0, "")` -- "no partial success" -- matching proto3's own
/// "an unset field reads back as its default" semantics rather than failing the whole response
/// over a field this output only ever uses for a warning, never a hard failure.
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

/// Advances `pos` past one field's value, given its wire type, without interpreting it -- for a
/// field number this parser doesn't care about. `None` on a length-delimited field whose declared
/// length runs past the end of `bytes` (malformed), or a group-encoded field (wire types 3/4,
/// deprecated in proto3 and never emitted by anything this parser reads).
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
            events: vec![Event::metric(
                1,
                AttrMap::new(),
                MetricRecord {
                    name: logit_core::interner::intern("x"),
                    kind: MetricKind::Counter(1.0),
                    unit: None,
                },
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
            },
        );
        let metric = Event::metric(
            2,
            AttrMap::new(),
            MetricRecord {
                name: logit_core::interner::intern("m"),
                kind: MetricKind::Counter(1.0),
                unit: None,
            },
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
            },
        );
        EventBatch { resource: Arc::new(Resource::default()), events: vec![log, metric, span] }
    }

    // ---- HTTP transport: a bare HTTP/1.1 canned-response peer, `influxdb.rs`'s pattern ----

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
                    MetricKind::Counter(v)
                        if logit_core::interner::resolve(m.name)
                            == "logit.output.records.rejected" =>
                    {
                        Some(*v)
                    }
                    _ => None,
                })
            })
            .unwrap_or(0.0);
        assert_eq!(rejected, 2.0, "the rejected count should be counted, not silently dropped");
    }

    /// A one-shot raw TCP peer that captures the first request's bytes verbatim rather than
    /// parsing them -- enough to assert a header actually landed on the wire without pulling in
    /// an HTTP request parser just for a test.
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
        // `graph::resolve`'s rule 20 rejects `content-type` at config-validation time -- this
        // proves the defense-in-depth guarantee directly, bypassing that rule via `with_headers`.
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
        // Regression test for the ordering hazard `with_headers`'s own doc comment describes:
        // headers live in their own field, applied per request, never baked into `client` --
        // so calling `with_timeout` after `with_headers` must not lose them.
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
        // `OtlpOutput` isn't `Debug` (it embeds a `reqwest::Client`), so `Result::unwrap_err`
        // -- which needs `Debug` on the `Ok` side to format its panic message -- doesn't work
        // here. Same reason `logit-pipeline::graph`'s tests have their own `expect_err` helper.
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
        // Regression guard: HTTP header names are case-insensitive, so `HeaderName` normalizes
        // "X-Scope-OrgID" and "x-scope-orgid" to the same key -- without this check,
        // `HeaderMap::insert` would silently keep whichever of the two `with_headers` happened to
        // iterate last (`headers`' `HashMap` iteration order is unspecified), sending a different
        // value on different runs with no error either way.
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

    /// Starts a bare gRPC-shaped HTTP/2 peer on an ephemeral port that always replies to any
    /// unary call with the given `grpc-status`/`grpc-message`/response payload -- the gRPC
    /// analogue of `canned_http_server` above.
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

    /// Duplicated, minimal test-only twin of `logit_inputs::otlp::GrpcBody` -- a response body
    /// that yields one data frame then one trailers frame. Kept local rather than shared: this is
    /// the only place in this crate a *server*-shaped body is ever needed at all.
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

    /// The twin of `canned_grpc_server`, capturing the request's headers into `captured` instead
    /// of replying with a configurable status -- always `grpc-status: 0`, since these tests only
    /// need to inspect what was sent, not how a failure is classified.
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

    /// Like `canned_grpc_server_capturing_headers`, but also captures the raw framed request body
    /// -- needed to inspect the frame's own compressed-flag byte and its (possibly gzipped)
    /// payload, not just the headers declaring compression.
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
                    MetricKind::Counter(v)
                        if logit_core::interner::resolve(m.name)
                            == "logit.output.records.rejected" =>
                    {
                        Some(*v)
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

    /// A backend replying with a maximal (10-byte) varint length on the `partial_success` field
    /// must not panic via `pos + len` overflowing `usize` -- `checked_add` should reject it as
    /// malformed (the declared length runs past the end of `bytes` regardless) and fall back to
    /// "no partial success," the same as any other truncated/malformed body.
    #[test]
    fn parse_partial_success_does_not_panic_on_an_overflowing_declared_length() {
        // field 1 (partial_success), length-delimited, with the largest encodable varint length.
        let mut bytes = vec![0x0a];
        bytes.extend_from_slice(&[0xff; 9]);
        bytes.push(0x01); // 10-byte varint, decodes to a huge u64
        assert_eq!(parse_partial_success(&bytes), (0, String::new()));
    }

    /// The same overflow hazard one level down, inside the `partial_success` submessage's own
    /// `error_message` field.
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

    /// `https://` used to be rejected outright under `protocol: grpc` -- it's now the normal way
    /// to ask for gRPC-over-TLS (`docs/adr/otlp-tls-and-pooled-grpc-client.md`), so
    /// `normalize_grpc_endpoint` must keep it exactly as written rather than treating it as one
    /// more plaintext spelling.
    #[test]
    fn normalize_grpc_endpoint_keeps_https_as_written() {
        assert_eq!(normalize_grpc_endpoint("https://tempo:4317"), "https://tempo:4317");
        assert_eq!(normalize_grpc_endpoint("https://tempo:4317/"), "https://tempo:4317");
        assert_eq!(normalize_grpc_endpoint("HTTPS://tempo:4317"), "HTTPS://tempo:4317");
    }

    #[test]
    fn constructing_an_otlp_output_with_https_and_protocol_grpc_now_succeeds() {
        // An `https://` endpoint under `protocol: grpc` used to be a hard construction-time
        // error (the hand-rolled gRPC client had no TLS support at all); it's now the normal way
        // to ask for gRPC-over-TLS, so construction succeeds like any other endpoint --
        // `an_https_grpc_endpoint_is_reachable_over_tls` (below) proves it actually negotiates
        // TLS, not just that construction doesn't fail.
        OtlpOutput::new("https://tempo:4317".to_string(), OtlpTransport::Grpc)
            .expect("https:// under protocol: grpc is now a TLS connection, not a hard error");
    }

    #[test]
    fn constructing_an_otlp_output_with_https_and_protocol_http_succeeds() {
        // `protocol: http` genuinely does support TLS (it's plain `reqwest`) -- unaffected by any
        // of this module's gRPC changes.
        OtlpOutput::new("https://tempo:4318".to_string(), OtlpTransport::Http)
            .expect("https:// is exactly what protocol: http is for");
    }

    #[test]
    fn constructing_an_otlp_output_with_http_and_protocol_grpc_succeeds() {
        // The one legitimate alternate spelling `normalize_grpc_endpoint` still accepts.
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
        // `logit-outputs` lives at `crates/logit-outputs`; the fixtures live at the repo root's
        // `testdata/tls` (`testdata/tls/README.md`) -- two levels up from `CARGO_MANIFEST_DIR`.
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    /// Builds a `rustls::ServerConfig` presenting `testdata/tls/server.{pem,key}`, optionally
    /// requiring a client certificate chaining to `testdata/tls/ca.pem` -- the test-only
    /// counterpart to `logit_inputs::otlp`'s real `build_rustls_server_config`, kept local since
    /// this crate has no server-side TLS code of its own to reuse.
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

    /// The TLS-wrapped twin of `canned_http_server`: replies `200 OK` to any request over a real
    /// TLS handshake against `testdata/tls/server.pem`.
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
        // Reuses `canned_grpc_server`'s framing but wraps its `TcpListener::accept` output in a
        // TLS handshake first -- proves the gRPC transport actually negotiates TLS (ALPN `h2`
        // over a real handshake), not just that construction under `https://` no longer errors.
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
        // The rebuild-hazard regression test `with_headers`'s own doc comment describes, applied
        // to `tls`: `with_timeout` rebuilds `client` unconditionally, so it must rebuild it with
        // the already-set TLS config, not silently drop back to the default one.
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
        // `OtlpOutput` isn't `Debug` (it embeds a `reqwest::Client`), so `Result::expect_err`
        // doesn't work here -- same reasoning every other constructor-failure test in this module
        // already follows.
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
