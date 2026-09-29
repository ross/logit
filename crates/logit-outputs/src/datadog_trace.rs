//! `datadog_trace_out`: sends APM traces and tracer-computed stats to a Datadog Agent's trace API
//! (`:8126`, or its Unix socket), as a dd-trace tracer does. It is the sending half of the
//! `datadog_trace_in -> datadog_trace_out` pair, a like-protocol relay under
//! [ADR `lossless-transit`](../../../docs/adr/lossless-transit.md), and the last hop of the
//! tracer-direct topology (tracer → `datadog_trace_in` → `datadog_trace_out` → a real Agent;
//! [`docs/plans/datadog-relay.md`](../../../docs/plans/datadog-relay.md) §14). The decisions are
//! [ADR `datadog-agent-and-intake-relay`](../../../docs/adr/datadog-agent-and-intake-relay.md)'s
//! 1, 2, and 13. Every body comes from [`DatadogEncoder`], whose module doc holds the mappings;
//! this module owns HTTP: routes, headers, request splitting, the two transports, and what a
//! response means.
//!
//! The Agent does all trace processing (normalization, obfuscation, `_top_level`, sampling, the
//! stats concentrator); this sink relays what it is given and derives nothing. An OpenTelemetry
//! span without `service.name`, `resource.name`, or `span.type` reaches the Agent with those
//! fields empty: `otlp_out` is the path for OTel-origin spans.
//!
//! ## Config
//!
//! ```yaml
//! kind: datadog_trace_out
//! endpoint: http://127.0.0.1:8126   # or socket: /var/run/datadog/apm.socket, not both
//! version: v0.4                     # default; or v0.7
//! compression: none                 # default; or gzip
//! timeout: 10s                      # default; bounds one request
//! headers: {}                       # extra headers; `!env` works on a value
//! tls: {}                           # tunes an https:// endpoint
//! ```
//!
//! Graph rule 67 validates the block.
//!
//! ## Routes
//!
//! | What in the batch | Request | Body |
//! |---|---|---|
//! | span events other than APM stats | `PUT /v0.4/traces` or `/v0.7/traces`, by `version` | [`DatadogEncoder::encode_tracer_api_traces`] |
//! | APM stats events ([`is_datadog_stats`]) | `POST /v0.6/stats` | [`DatadogEncoder::encode_client_stats_v06`] |
//!
//! Every body is `Content-Type: application/msgpack`. `PUT` is what dd-trace-java sends on the
//! trace routes; the Agent and `datadog_trace_in` take `PUT` and `POST` alike. An event that is
//! neither a span nor APM stats has no route here and is ignored, as `prometheus_out` ignores a
//! log. Traces go out before stats, sequentially.
//!
//! **`version`.** `v0.4` (the default) is what most tracers send and every Agent takes. It has no
//! trace chunk and no tracer payload, so the chunk carriers (`datadog.chunk.*`) and the tracer
//! payload carriers no request header carries (`datadog.tracer.runtime_id`, `.env`, `.hostname`,
//! `.app_version`, `.tags`, `.container_debug`) are dropped and counted by the encoder as
//! `logit.output.spans.degraded{reason="no_wire_form"}`. `v0.7` carries all of them in its
//! `TracerPayload`. A v0.4-origin batch (what most tracers send `datadog_trace_in`) relays through
//! either with nothing lost.
//!
//! **What isn't read from a reply.** The Agent answers a trace request with `rate_by_service`,
//! the sampling rates a tracer's priority sampler applies. This sink samples nothing, so it
//! ignores them and never sends `Datadog-Rates-Payload-Version`.
//!
//! ## The wire
//!
//! Headers are the operator's `headers:` with the protocol's `insert`ed over them, and every
//! `datadog-*` or `x-datadog-*` name the operator set removed, so a protocol-owned name is never
//! sent with an operator's value (rule 67 also rejects one at config time):
//!
//! | Header | Value |
//! |---|---|
//! | `Content-Type` | `application/msgpack` |
//! | `Content-Encoding` | `gzip` under `compression: gzip`; absent under `none` |
//! | `User-Agent` | `logit/<version>` |
//! | [`TRACER_STR_HEADERS`] (`Datadog-Meta-Lang`, `-Lang-Version`, `-Lang-Interpreter`, `-Lang-Interpreter-Vendor`, `-Tracer-Version`, `Datadog-Container-ID`, `Datadog-Entity-ID`, `Datadog-External-Env`) | trace requests: the batch resource's `Str` attribute, when present |
//! | [`TRACER_FLAG_HEADERS`] (`Datadog-Client-Computed-Top-Level`, `-Stats`) | trace requests: `true` when the attribute is `Bool(true)`; absent otherwise |
//! | [`TRACER_U64_HEADERS`] (`Datadog-Client-Dropped-P0-Traces`, `-Spans`) | trace requests: the `U64` attribute in decimal, when present |
//! | `X-Datadog-Trace-Count` | trace requests: the number of traces the encoder wrote |
//!
//! These are the headers `datadog_trace_in` reads into the same attributes, so the pair restores
//! them. A `Str` attribute that isn't a legal header value is left out, with a throttled
//! `bad_header` diagnostic. The stats route carries no tracer header: `datadog_trace_in` reads
//! none there, and the `ClientStatsPayload` names its tracer itself.
//!
//! ## Transports
//!
//! `endpoint:` sends over `reqwest`, `http://` or `https://` (tuned by `tls:`), with redirects off
//! ([`crate::http::build_client`] says why). `socket:` sends HTTP/1.1 over the Agent's Unix
//! socket through a pooled `hyper_util` client whose connector dials the path for every new
//! connection; the request URI's host is `localhost`, which the Agent ignores. Both share the
//! request building and the response classification below.
//!
//! ## Size limits
//!
//! A request holds at most [`MAX_TRACES_PER_REQUEST`] traces (the Agent has no count limit; the
//! bound keeps one request's encode and send short) or as many stats groups, and at most
//! [`MAX_REQUEST_BYTES`] on the wire (the Agent's `max_request_bytes`). The routes' events are
//! cut by [`crate::http::split_encode`], by trace on the trace route so a trace is never split
//! across requests. A single trace or stats group over the byte cap is dropped, counted
//! `logit.output.records.dropped{reason="oversize"}` for its spans (or its one group), with a
//! throttled `oversize` diagnostic.
//!
//! ## Faults, retries, and duplicate safety
//!
//! **One `send` is one attempt per request**, traces then stats. The first failing request aborts
//! the rest, and `write_loop` retries the whole batch, re-sending any request that had already
//! succeeded (`otlp_out`'s rule).
//!
//! | Outcome | Result |
//! |---|---|
//! | 2xx | `Ok` |
//! | 408, 429, any 5xx | [`Fault::Ambiguous`] |
//! | 413 | [`Fault::Permanent`], and the request's records counted `records.dropped{reason="oversize"}` |
//! | any other 1xx, 3xx, or 4xx | [`Fault::Permanent`], with a throttled `request_rejected` diagnostic quoting the first 256 bytes of the body |
//! | connect failure (refused, no such socket file) | [`Fault::Clean`] |
//! | any other transport error, timeout included | [`Fault::Ambiguous`] |
//!
//! [`DatadogTraceOutput::duplicate_safe`] is **`false`**: an Agent dedupes nothing, so a resent
//! trace is a second copy of every span in it.
//!
//! ## Telemetry
//!
//! | Point | Meaning |
//! |---|---|
//! | `logit.output.requests{route, class}` | one per request; `route` is `traces` or `stats`, `class` [`crate::http::status_class`]'s or `network_error` |
//! | `logit.output.request.duration{route}` | one timer per request |
//! | `logit.output.request.bytes{route}` | the body as sent, after compression, for a request that got an answer or failed after it may have left; not for a refused connection or a missing socket file |
//! | `logit.output.records{route}` | spans (`traces`) or stats groups (`stats`) in a request the Agent accepted |
//! | `logit.output.records.dropped{route, reason="oversize"}` | as above |
//!
//! Plus everything [`DatadogEncoder`] counts itself (`logit.output.spans.degraded`,
//! `logit.output.stats.*`, `logit.output.tags.dropped`), which this sink doesn't repeat.
//!
//! **Once per batch or per attempt** (ADR `sink-send-path-and-attempt-accounting`, decision 1).
//! Each route is a unit of the sink's `BatchAccounting`: the encoder's counters, the trace or
//! stats group too large alone, and the `bad_header` diagnostic count on the unit's first encode,
//! once per batch. A bisection's re-encodes count nothing ([`crate::http::split_encode`]). The
//! encoder's counters for the batch resource count once per request of up to
//! [`MAX_TRACES_PER_REQUEST`], not once per batch. The transport counters and a `413`'s
//! `oversize` count per attempt.

use crate::accounting::BatchAccounting;
use crate::http::{
    body_snippet, build_client, classify_reqwest_error, read_body_prefix, split_encode,
    status_class, Caps, Encoded, ERROR_BODY_SNIPPET_BYTES,
};
/// `tls:`: the shared `crate::tls` type, re-exported as the other sinks do.
pub use crate::tls::TlsClientSettings;
use anyhow::Context;
use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::{TokioExecutor, TokioIo};
use logit_core::{Diagnostics, EventBatch, Resource, Telemetry, Value};
use logit_pipeline::{BatchContext, Fault, Output};
pub use logit_proto::datadog::traces_msgpack::TracerApiForm;
use logit_proto::datadog::{
    is_datadog_stats, trace_chunks, DatadogEncoder, HEADER_TRACE_COUNT, TRACER_FLAG_HEADERS,
    TRACER_STR_HEADERS, TRACER_U64_HEADERS,
};
use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;
use tokio::net::UnixStream;

/// The default `timeout:` for one request, the 10s `datadog_out` and `otlp_out` use.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// The cap on a request body as sent: the Agent's `max_request_bytes` default, 25 MiB.
pub const MAX_REQUEST_BYTES: usize = 25 * 1024 * 1024;

/// The cap on traces (or stats groups) per request. The Agent sets none; this bound keeps one
/// request's encode and send short.
pub const MAX_TRACES_PER_REQUEST: usize = 1_000;

/// How much of an accepted reply's body is read before it is dropped. A reply is
/// `rate_by_service`, a few bytes per service; reading it whole lets the connection be reused.
const ACCEPTED_BODY_BYTES: usize = 64 * 1024;

/// The `User-Agent` on every request. Reserved in config (rule 67).
const USER_AGENT: &str = concat!("logit/", env!("CARGO_PKG_VERSION"));

const MSGPACK: &str = "application/msgpack";

const REQUESTS: &str = "logit.output.requests";
const REQUEST_DURATION: &str = "logit.output.request.duration";
const REQUEST_BYTES: &str = "logit.output.request.bytes";
const RECORDS: &str = "logit.output.records";
const RECORDS_DROPPED: &str = "logit.output.records.dropped";

/// Whether request bodies are gzipped. Mirrors `logit_config::DatadogTraceCompression`, which
/// `logit-cli::pipeline::build_spec` translates, since this crate doesn't depend on
/// `logit-config`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DatadogTraceCompression {
    #[default]
    None,
    Gzip,
}

impl DatadogTraceCompression {
    fn header(self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Gzip => Some("gzip"),
        }
    }

    /// Inline, not `spawn_blocking`: `datadog_out`'s reasoning, at the same few-MiB scale.
    fn apply(self, raw: Bytes) -> Bytes {
        match self {
            Self::None => raw,
            Self::Gzip => {
                let mut e =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                e.write_all(&raw).expect("writing to an in-memory Vec never fails");
                Bytes::from(e.finish().expect("finishing an in-memory encoder never fails"))
            }
        }
    }
}

/// A request's route: the module doc's "Routes" table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Route {
    Traces,
    Stats,
}

impl Route {
    /// The `route` tag.
    fn name(self) -> &'static str {
        match self {
            Self::Traces => "traces",
            Self::Stats => "stats",
        }
    }

    fn method(self) -> Method {
        match self {
            Self::Traces => Method::PUT,
            Self::Stats => Method::POST,
        }
    }

    fn path(self, version: TracerApiForm) -> &'static str {
        match (self, version) {
            (Self::Traces, TracerApiForm::V04) => "/v0.4/traces",
            (Self::Traces, TracerApiForm::V07) => "/v0.7/traces",
            (Self::Stats, _) => "/v0.6/stats",
        }
    }
}

const CAPS: Caps =
    Caps { entries: MAX_TRACES_PER_REQUEST, raw_bytes: usize::MAX, wire_bytes: MAX_REQUEST_BYTES };

/// What one encoded request carries beyond its body.
#[derive(Debug, Clone, Copy)]
struct RequestMeta {
    /// `X-Datadog-Trace-Count`: the encoder's count; 0 on the stats route.
    traces: usize,
    /// Spans or stats groups, for `logit.output.records`.
    records: usize,
}

/// The Unix-socket connector: dials `path` for every connection the pool opens, whatever the
/// request URI names.
#[derive(Clone)]
struct UnixConnector {
    path: Arc<Path>,
}

impl tower_service::Service<http::Uri> for UnixConnector {
    type Response = TokioIo<UnixStream>;
    type Error = std::io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: http::Uri) -> Self::Future {
        let path = Arc::clone(&self.path);
        Box::pin(async move { Ok(TokioIo::new(UnixStream::connect(&*path).await?)) })
    }
}

/// The two transports (module doc's "Transports").
enum Client {
    Http { client: reqwest::Client, base: String },
    Unix { client: Box<HyperClient<UnixConnector, Full<Bytes>>>, path: Arc<Path> },
}

/// A response's status and a bounded prefix of its body.
struct Reply {
    status: StatusCode,
    body: String,
}

impl Client {
    fn unix(path: PathBuf) -> Self {
        let path: Arc<Path> = Arc::from(path);
        let connector = UnixConnector { path: Arc::clone(&path) };
        Self::Unix {
            client: Box::new(HyperClient::builder(TokioExecutor::new()).build(connector)),
            path,
        }
    }

    /// Where requests go, for messages.
    fn describe(&self, path: &str) -> String {
        match self {
            Self::Http { base, .. } => format!("{base}{path}"),
            Self::Unix { path: socket, .. } => format!("unix:{}{path}", socket.display()),
        }
    }

    /// One request, one attempt, bounded by `timeout` through reading the reply. A transport
    /// failure comes back with its [`Fault`].
    async fn send(
        &self,
        method: Method,
        path: &str,
        headers: HeaderMap,
        body: Bytes,
        timeout: Duration,
    ) -> Result<Reply, (Fault, anyhow::Error)> {
        match self {
            Self::Http { client, base } => {
                let response = client
                    .request(method, format!("{base}{path}"))
                    .headers(headers)
                    .timeout(timeout)
                    .body(body)
                    .send()
                    .await
                    .map_err(|err| (classify_reqwest_error(&err), anyhow::Error::new(err)))?;
                let status = response.status();
                let body = read_body_prefix(response, read_limit(status)).await;
                Ok(Reply { status, body })
            }
            Self::Unix { client, .. } => {
                let mut request = http::Request::builder()
                    .method(method)
                    .uri(format!("http://localhost{path}"))
                    .body(Full::new(body))
                    .expect("a well-formed request always builds");
                *request.headers_mut() = headers;
                let exchange = async {
                    let response = client.request(request).await.map_err(|err| {
                        // A connector failure (no socket file, nobody listening) is a connect
                        // failure: no byte of the request left.
                        let fault = if err.is_connect() { Fault::Clean } else { Fault::Ambiguous };
                        (fault, anyhow::Error::new(err))
                    })?;
                    let status = response.status();
                    let body = read_hyper_prefix(response.into_body(), read_limit(status)).await;
                    Ok(Reply { status, body })
                };
                match tokio::time::timeout(timeout, exchange).await {
                    Ok(result) => result,
                    Err(_elapsed) => Err((
                        Fault::Ambiguous,
                        anyhow::anyhow!("no complete reply within {timeout:?}"),
                    )),
                }
            }
        }
    }
}

/// How much of a reply to read: enough to reuse the connection after an accepted request, a
/// snippet after a rejected one.
fn read_limit(status: StatusCode) -> usize {
    if status.is_success() {
        ACCEPTED_BODY_BYTES
    } else {
        ERROR_BODY_SNIPPET_BYTES
    }
}

/// [`read_body_prefix`] for a `hyper` body: at most `max` (plus a little slack) bytes, decoded
/// lossily, a read error ending the read.
async fn read_hyper_prefix(mut body: hyper::body::Incoming, max: usize) -> String {
    let limit = max.saturating_add(4);
    let mut buf: Vec<u8> = Vec::new();
    while buf.len() < limit {
        match body.frame().await {
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    buf.extend_from_slice(&data);
                }
            }
            Some(Err(_)) | None => break,
        }
    }
    buf.truncate(limit);
    String::from_utf8_lossy(&buf).into_owned()
}

/// The events of `batch` at `indices` (ascending, no repeats): the batch itself when that is
/// every event, else a copy sharing its resource and scope.
fn sub_batch<'a>(batch: &'a EventBatch, indices: &[usize]) -> Cow<'a, EventBatch> {
    if indices.len() == batch.events.len() {
        return Cow::Borrowed(batch);
    }
    Cow::Owned(EventBatch {
        resource: batch.resource.clone(),
        scope: batch.scope.clone(),
        events: indices.iter().map(|&i| batch.events[i].clone()).collect(),
    })
}

/// The Datadog Agent trace-API client (module doc).
pub struct DatadogTraceOutput {
    client: Client,
    version: TracerApiForm,
    compression: DatadogTraceCompression,
    request_timeout: Duration,
    /// The operator's `headers:`, built once; the protocol's are inserted over a clone per
    /// request.
    headers: HeaderMap,
    /// Built by [`DatadogTraceOutput::with_tls`]; `None` keeps `reqwest`'s default trust.
    tls: Option<rustls::ClientConfig>,
    /// Counts through views of `telemetry`/`diag` gated by `accounting`
    /// ([`DatadogTraceOutput::new_encoder`]).
    encoder: DatadogEncoder,
    /// Ungated: the transport counters, the Agent's verdicts, and the oversize drops and
    /// `bad_header` diagnostic, which the sink skips itself on a repeat encode.
    diag: Diagnostics,
    telemetry: Telemetry,
    accounting: BatchAccounting,
    /// Replaces [`CAPS`], so a test can drop a small trace as oversize.
    #[cfg(test)]
    caps_override: Option<Caps>,
}

/// The batch accounting's unit for the trace route's encode and tracer headers.
const TRACES_UNIT: u32 = 0;
/// The batch accounting's unit for the stats route's encode.
const STATS_UNIT: u32 = 1;

impl DatadogTraceOutput {
    fn with_client(client: Client) -> Self {
        let mut output = Self {
            client,
            version: TracerApiForm::V04,
            compression: DatadogTraceCompression::default(),
            request_timeout: DEFAULT_TIMEOUT,
            headers: HeaderMap::new(),
            tls: None,
            encoder: DatadogEncoder::new(),
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
            accounting: BatchAccounting::default(),
            #[cfg(test)]
            caps_override: None,
        };
        output.encoder = output.new_encoder();
        output
    }

    /// Sends to the Agent at `endpoint` (`endpoint:`), an `http://` or `https://` base URL; a
    /// trailing `/` is dropped.
    pub fn http(endpoint: impl Into<String>) -> Self {
        let base = endpoint.into().trim_end_matches('/').to_string();
        Self::with_client(Client::Http { client: build_client(DEFAULT_TIMEOUT, None), base })
    }

    /// Sends to the Agent's Unix socket at `path` (`socket:`).
    pub fn unix(path: impl Into<PathBuf>) -> Self {
        Self::with_client(Client::unix(path.into()))
    }

    /// The trace form (`version:`).
    pub fn with_version(mut self, version: TracerApiForm) -> Self {
        self.version = version;
        self
    }

    pub fn with_compression(mut self, compression: DatadogTraceCompression) -> Self {
        self.compression = compression;
        self
    }

    /// Per-request timeout (`timeout:`). An HTTP client is rebuilt so its default agrees with the
    /// per-request `.timeout(..)` that bounds each request.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        if let Client::Http { client, .. } = &mut self.client {
            *client = build_client(timeout, self.tls.as_ref());
        }
        self
    }

    /// The extra headers on every request (`headers:`). Fails on a name or value that isn't legal
    /// HTTP, and on two names that collide once case is normalized; rule 67 rejects the
    /// protocol's own names.
    pub fn with_headers(mut self, headers: &HashMap<String, String>) -> anyhow::Result<Self> {
        let mut map = HeaderMap::with_capacity(headers.len());
        for (name, value) in headers {
            let header_name = HeaderName::from_bytes(name.as_bytes()).with_context(|| {
                format!("datadog_trace_out: {name:?} is not a legal header name")
            })?;
            let header_value = HeaderValue::from_str(value).with_context(|| {
                format!("datadog_trace_out: header {name:?} has an invalid value")
            })?;
            if map.insert(header_name, header_value).is_some() {
                anyhow::bail!(
                    "datadog_trace_out: header {name:?} collides with another entry in 'headers' \
                     once case is ignored -- HTTP header names are case-insensitive, so which \
                     value would be sent is undefined"
                );
            }
        }
        self.headers = map;
        Ok(self)
    }

    /// Client TLS tuning (`tls:`) for an `https://` endpoint. A no-op when `settings` is empty;
    /// an error on the Unix socket, which is always plaintext.
    pub fn with_tls(
        mut self,
        settings: &TlsClientSettings,
        base_dir: &Path,
    ) -> anyhow::Result<Self> {
        if settings.is_empty() {
            return Ok(self);
        }
        if matches!(self.client, Client::Unix { .. }) {
            anyhow::bail!("datadog_trace_out: 'tls' can't be used with 'socket'");
        }
        if settings.insecure_skip_verify {
            self.diag.warn(
                "tls.insecure_skip_verify is set -- the connection is encrypted, but this output \
                 will accept any certificate the peer presents, self-signed or otherwise",
            );
        }
        let cfg = crate::tls::build_client_config(settings, base_dir)?;
        if let Client::Http { client, .. } = &mut self.client {
            *client = build_client(self.request_timeout, Some(&cfg));
        }
        self.tls = Some(cfg);
        Ok(self)
    }

    /// Reaches the encoder too, which reports its own throttled diagnostics.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self.encoder = self.new_encoder();
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self.encoder = self.new_encoder();
        self
    }

    /// The encoder, on views of this sink's handles gated by its batch accounting, so a retried
    /// batch counts the codec's drops once (`crate::accounting`). `with_client` and every builder
    /// that changes what the encoder holds call this, so no builder order leaves it ungated.
    fn new_encoder(&self) -> DatadogEncoder {
        let gate = self.accounting.gate();
        DatadogEncoder::new()
            .with_telemetry(self.telemetry.gated(gate))
            .with_diagnostics(self.diag.gated(gate))
    }

    /// Both routes' request limits: [`CAPS`], or a test's override.
    fn caps(&self) -> Caps {
        #[cfg(test)]
        if let Some(caps) = self.caps_override {
            return caps;
        }
        CAPS
    }

    fn dropped(&self, route: Route, reason: &'static str, n: usize) {
        if n > 0 {
            self.telemetry.count(
                RECORDS_DROPPED,
                n as f64,
                &[("route", route.name()), ("reason", reason)],
            );
        }
    }

    /// The trace route's requests: the batch's span events other than APM stats, one item per
    /// trace so a trace is never split. The encode and the tracer headers are [`TRACES_UNIT`]:
    /// the oversize drops and the `bad_header` diagnostic count on its first encode only.
    async fn send_traces(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let resource = &batch.resource;
        let chunks: Vec<Vec<usize>> = trace_chunks(batch)
            .into_iter()
            .map(|mut members| {
                members.retain(|&i| !is_datadog_stats(resource, &batch.events[i]));
                members
            })
            .filter(|members| !members.is_empty())
            .collect();
        if chunks.is_empty() {
            return Ok(());
        }
        let items: Vec<usize> = (0..chunks.len()).collect();
        let (version, compression, caps) = (self.version, self.compression, self.caps());
        let gate = self.accounting.gate().clone();
        let encoder = &mut self.encoder;
        let (first, split) = self.accounting.encode(TRACES_UNIT, || {
            split_encode(
                &items,
                caps,
                &gate,
                |_| 1,
                |selected| {
                    let mut indices: Vec<usize> =
                        selected.iter().flat_map(|&c| chunks[c].iter().copied()).collect();
                    indices.sort_unstable();
                    let sub = sub_batch(batch, &indices);
                    let out = encoder.encode_tracer_api_traces(&sub, version)?;
                    Some(Encoded {
                        raw_len: out.body.len(),
                        body: compression.apply(out.body),
                        meta: RequestMeta { traces: out.traces, records: indices.len() },
                    })
                },
            )
        });
        if first {
            for &(chunk, raw_len, wire_len) in &split.oversize {
                let spans = chunks[chunk].len();
                self.dropped(Route::Traces, "oversize", spans);
                self.diag.warn_throttled(
                    "oversize",
                    format_args!(
                        "dropped a trace of {spans} spans: it encodes to {raw_len} bytes \
                         ({wire_len} as sent), over the Agent's {MAX_REQUEST_BYTES}-byte limit"
                    ),
                );
            }
        }
        let headers = self.tracer_headers(resource, first);
        for (_, encoded) in split.requests {
            let mut headers = headers.clone();
            headers.insert(HEADER_TRACE_COUNT, HeaderValue::from(encoded.meta.traces));
            self.post(Route::Traces, headers, encoded).await?;
        }
        Ok(())
    }

    /// The stats route's requests: one item per APM stats event (a stats group). The encode is
    /// [`STATS_UNIT`]: the oversize drops count on its first encode only.
    async fn send_stats(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let items: Vec<usize> = (0..batch.events.len())
            .filter(|&i| is_datadog_stats(&batch.resource, &batch.events[i]))
            .collect();
        if items.is_empty() {
            return Ok(());
        }
        let (compression, caps) = (self.compression, self.caps());
        let gate = self.accounting.gate().clone();
        let encoder = &mut self.encoder;
        let (first, split) = self.accounting.encode(STATS_UNIT, || {
            split_encode(
                &items,
                caps,
                &gate,
                |_| 1,
                |selected| {
                    let raw = encoder.encode_client_stats_v06(&sub_batch(batch, selected))?;
                    Some(Encoded {
                        raw_len: raw.len(),
                        body: compression.apply(raw),
                        meta: RequestMeta { traces: 0, records: selected.len() },
                    })
                },
            )
        });
        if first {
            for &(_, raw_len, wire_len) in &split.oversize {
                self.dropped(Route::Stats, "oversize", 1);
                self.diag.warn_throttled(
                    "oversize",
                    format_args!(
                        "dropped a stats group: it encodes to {raw_len} bytes ({wire_len} as \
                         sent), over the Agent's {MAX_REQUEST_BYTES}-byte limit"
                    ),
                );
            }
        }
        for (_, encoded) in split.requests {
            self.post(Route::Stats, HeaderMap::new(), encoded).await?;
        }
        Ok(())
    }

    /// The tracer headers restored from `resource` (module doc's "The wire"). A carrier that
    /// isn't a legal header value is diagnosed only when `report`: the diagnostic describes the
    /// batch, so a retry doesn't repeat it.
    fn tracer_headers(&mut self, resource: &Resource, report: bool) -> HeaderMap {
        let attrs = &resource.attributes;
        let mut headers = HeaderMap::new();
        for (header, attr) in TRACER_STR_HEADERS {
            let Some(text) = attrs.get(attr).and_then(Value::as_str) else { continue };
            match HeaderValue::from_str(text) {
                Ok(value) => {
                    headers.insert(header, value);
                }
                Err(_) if report => {
                    self.diag.warn_throttled(
                        "bad_header",
                        format_args!(
                            "datadog_trace_out: leaving out {header}: the batch's {attr} isn't a \
                             legal header value"
                        ),
                    );
                }
                Err(_) => {}
            }
        }
        for (header, attr) in TRACER_FLAG_HEADERS {
            if matches!(attrs.get(attr), Some(Value::Bool(true))) {
                headers.insert(header, HeaderValue::from_static("true"));
            }
        }
        for (header, attr) in TRACER_U64_HEADERS {
            if let Some(Value::U64(n)) = attrs.get(attr) {
                headers.insert(header, HeaderValue::from(*n));
            }
        }
        headers
    }

    /// The operator's headers without any protocol-owned name, then `protocol` and the fixed
    /// ones `insert`ed over them. One `.headers(..)` at the call site, never
    /// `RequestBuilder::header`, which appends.
    fn request_headers(&self, protocol: HeaderMap) -> HeaderMap {
        let mut headers = self.headers.clone();
        let owned: Vec<HeaderName> = headers
            .keys()
            .filter(|name| {
                let name = name.as_str();
                name.starts_with("datadog-") || name.starts_with("x-datadog-")
            })
            .cloned()
            .collect();
        for name in owned {
            headers.remove(name);
        }
        for (name, value) in &protocol {
            headers.insert(name.clone(), value.clone());
        }
        headers.insert(http::header::CONTENT_TYPE, HeaderValue::from_static(MSGPACK));
        match self.compression.header() {
            Some(encoding) => {
                headers.insert(http::header::CONTENT_ENCODING, HeaderValue::from_static(encoding));
            }
            None => {
                headers.remove(http::header::CONTENT_ENCODING);
            }
        }
        headers.insert(http::header::USER_AGENT, HeaderValue::from_static(USER_AGENT));
        headers
    }

    /// One attempt: traces, then stats.
    async fn attempt(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        self.send_traces(batch).await?;
        self.send_stats(batch).await
    }

    /// One request, one attempt (module doc's "Faults, retries, and duplicate safety").
    /// `request.bytes` counts a request that may have left: any answer, and any error but a
    /// [`Fault::Clean`] one, which never connected.
    async fn post(
        &mut self,
        route: Route,
        protocol: HeaderMap,
        encoded: Encoded<RequestMeta>,
    ) -> anyhow::Result<()> {
        let path = route.path(self.version);
        let target = self.client.describe(path);
        let tags = [("route", route.name())];
        let wire_len = encoded.body.len();
        let records = encoded.meta.records;
        let headers = self.request_headers(protocol);
        let timer = self.telemetry.timer(REQUEST_DURATION);
        let result = self
            .client
            .send(route.method(), path, headers, encoded.body, self.request_timeout)
            .await;
        timer.stop(&tags);

        let reply = match result {
            Ok(reply) => reply,
            Err((fault, err)) => {
                if fault != Fault::Clean {
                    self.telemetry.count(REQUEST_BYTES, wire_len as f64, &tags);
                }
                self.telemetry.count(
                    REQUESTS,
                    1.0,
                    &[("route", route.name()), ("class", "network_error")],
                );
                return Err(err
                    .context(format!(
                        "datadog_trace_out: {} request to {target} failed",
                        route.name()
                    ))
                    .context(fault));
            }
        };
        self.telemetry.count(REQUEST_BYTES, wire_len as f64, &tags);
        let status = reply.status;
        self.telemetry.count(
            REQUESTS,
            1.0,
            &[("route", route.name()), ("class", status_class(status))],
        );
        if status.is_success() {
            self.telemetry.count(RECORDS, records as f64, &tags);
            return Ok(());
        }
        let snippet = body_snippet(&reply.body, ERROR_BODY_SNIPPET_BYTES);
        let fault = match status.as_u16() {
            408 | 429 | 500..=599 => Fault::Ambiguous,
            413 => {
                self.dropped(route, "oversize", records);
                self.diag.warn_throttled(
                    "request_rejected",
                    format_args!("{target} answered 413, request too large: {snippet}"),
                );
                Fault::Permanent
            }
            _ => {
                self.diag.warn_throttled(
                    "request_rejected",
                    format_args!("{target} answered {status}: {snippet}"),
                );
                Fault::Permanent
            }
        };
        Err(anyhow::anyhow!(
            "datadog_trace_out: {} request to {target} failed ({status}): {snippet}",
            route.name()
        ))
        .context(fault)
    }
}

#[async_trait::async_trait]
impl Output for DatadogTraceOutput {
    /// Arms this sink's batch accounting (`crate::accounting`).
    fn observe_batch(&mut self, _ctx: BatchContext) {
        self.accounting.observe();
    }

    /// Traces, then stats, one request at a time; the first failure aborts the rest (module doc's
    /// "Faults, retries, and duplicate safety"). An `Ok` disarms the batch accounting on every
    /// path, a batch that sent nothing included.
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let result = self.attempt(batch).await;
        self.accounting.finish(result)
    }

    /// `false`: an Agent dedupes nothing, and a batch can be two requests, so a retry after the
    /// second fails re-sends the first. `buffer: { delivery: at_least_once }` accepts the
    /// duplicates instead.
    fn duplicate_safe(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::service::service_fn;
    use logit_core::interner::{intern, resolve};
    use logit_core::{
        AttrMap, Event, LogRecord, MetricKind, MetricRecord, Registry, SpanKind, SpanRecord,
        SpanStatus,
    };
    use logit_proto::datadog::stats::{ATTR_BUCKET_DURATION, ATTR_STATS_NAME, METRIC_HITS};
    use logit_proto::datadog::traces::RESOURCE_ATTR_TRACER_LANGUAGE_VERSION;
    use logit_proto::datadog::{
        DatadogDecoder, ATTR_RESOURCE_NAME, ATTR_SERVICE_NAME,
        RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_STATS, RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_TOP_LEVEL,
        RESOURCE_ATTR_TRACER_CONTAINER_ID, RESOURCE_ATTR_TRACER_DROPPED_P0_SPANS,
        RESOURCE_ATTR_TRACER_DROPPED_P0_TRACES, RESOURCE_ATTR_TRACER_ENTITY_ID,
        RESOURCE_ATTR_TRACER_LANGUAGE_INTERPRETER,
        RESOURCE_ATTR_TRACER_LANGUAGE_INTERPRETER_VENDOR, RESOURCE_ATTR_TRACER_LANGUAGE_NAME,
        RESOURCE_ATTR_TRACER_VERSION,
    };
    use std::io::Read;
    use std::net::SocketAddr;
    use std::sync::Mutex;

    // ---- a local Agent that records every request -------------------------------------------

    #[derive(Debug, Clone)]
    struct Captured {
        method: Method,
        path: String,
        headers: HeaderMap,
        body: Vec<u8>,
    }

    impl Captured {
        fn header(&self, name: &str) -> Option<&str> {
            self.headers.get(name).and_then(|v| v.to_str().ok())
        }

        /// The body, decompressed per its `Content-Encoding`.
        fn decoded(&self) -> Vec<u8> {
            match self.header("content-encoding") {
                None => self.body.clone(),
                Some("gzip") => {
                    let mut out = Vec::new();
                    flate2::read::GzDecoder::new(&self.body[..]).read_to_end(&mut out).unwrap();
                    out
                }
                Some(other) => panic!("unexpected content-encoding {other}"),
            }
        }
    }

    type Log = Arc<Mutex<Vec<Captured>>>;
    type Respond = Arc<dyn Fn(&str) -> (u16, String) + Send + Sync>;

    /// Serves HTTP/1.1 on one accepted connection, recording each request.
    fn serve<IO>(io: IO, log: Log, respond: Respond)
    where
        IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        tokio::spawn(async move {
            let svc = service_fn(move |req: http::Request<hyper::body::Incoming>| {
                let (log, respond) = (log.clone(), respond.clone());
                async move {
                    let method = req.method().clone();
                    let path = req.uri().path().to_string();
                    let headers = req.headers().clone();
                    let body = req.into_body().collect().await.unwrap().to_bytes();
                    log.lock().unwrap().push(Captured {
                        method,
                        path: path.clone(),
                        headers,
                        body: body.to_vec(),
                    });
                    let (status, text) = respond(&path);
                    Ok::<_, std::convert::Infallible>(
                        http::Response::builder()
                            .status(status)
                            .body(Full::new(Bytes::from(text)))
                            .unwrap(),
                    )
                }
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(io), svc)
                .await;
        });
    }

    /// A TCP Agent answering `respond(path)`.
    async fn agent(
        respond: impl Fn(&str) -> (u16, String) + Send + Sync + 'static,
    ) -> (SocketAddr, Log) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let log: Log = Arc::default();
        let respond: Respond = Arc::new(respond);
        let task_log = log.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                serve(stream, task_log.clone(), respond.clone());
            }
        });
        (addr, log)
    }

    const RATES: &str = r#"{"rate_by_service":{"service:,env:":1.0}}"#;

    async fn accepting() -> (SocketAddr, Log) {
        agent(|_| (200, RATES.into())).await
    }

    fn sink(addr: SocketAddr) -> DatadogTraceOutput {
        DatadogTraceOutput::http(format!("http://{addr}/"))
    }

    fn metered(out: DatadogTraceOutput) -> (Arc<Registry>, DatadogTraceOutput) {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("out", "datadog_trace_out", "sink");
        (registry, out.with_telemetry(telemetry))
    }

    /// A counter's total across every point matching `tags`.
    fn total(points: &[Event], metric: &str, tags: &[(&str, &str)]) -> f64 {
        let mut sum = 0.0;
        for event in points {
            if tags.iter().any(|(k, v)| event.attributes.get(k).and_then(Value::as_str) != Some(v))
            {
                continue;
            }
            for record in &event.metrics {
                if let MetricKind::Sum(s) = &record.kind {
                    if resolve(record.name) == metric {
                        sum += s.value;
                    }
                }
            }
        }
        sum
    }

    fn captured(log: &Log) -> Vec<Captured> {
        log.lock().unwrap().clone()
    }

    // ---- events ------------------------------------------------------------------------------

    fn span(trace: u8, id: u8, parent: Option<u8>) -> Event {
        let mut attributes = AttrMap::new();
        attributes.insert(ATTR_SERVICE_NAME, Value::str("checkout"));
        attributes.insert(ATTR_RESOURCE_NAME, Value::str("GET /"));
        let mut trace_id = [0; 16];
        trace_id[15] = trace;
        Event::span(
            1_700_000_000_000_000_000,
            attributes,
            SpanRecord {
                trace_id,
                span_id: [0, 0, 0, 0, 0, 0, 0, id],
                parent_span_id: parent.map(|p| [0, 0, 0, 0, 0, 0, 0, p]),
                name: Value::str("op"),
                kind: SpanKind::Internal,
                status: SpanStatus::Unset,
                events: Vec::new(),
                links: Vec::new(),
                end_timestamp: 1_700_000_000_000_001_000,
                flags: 0,
                ext: None,
            },
        )
    }

    fn stats_event() -> Event {
        let mut attrs = AttrMap::new();
        attrs.insert(ATTR_STATS_NAME, Value::str("http.request"));
        attrs.insert(ATTR_BUCKET_DURATION, Value::U64(10_000_000_000));
        Event::metric(
            1_700_000_000_000_000_000,
            attrs,
            MetricRecord::new(intern(METRIC_HITS), MetricKind::counter(4.0)),
        )
    }

    fn batch_with(resource: Resource, events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(resource), scope: None, events }
    }

    fn batch(events: Vec<Event>) -> EventBatch {
        batch_with(Resource::default(), events)
    }

    /// Two traces, the first of two spans.
    fn two_traces() -> EventBatch {
        batch(vec![span(1, 1, None), span(1, 2, Some(1)), span(2, 3, None)])
    }

    /// Every tracer header's carrier, as `datadog_trace_in` writes them.
    fn tracer_resource() -> Resource {
        let mut resource = Resource::default();
        for (key, value) in [
            (RESOURCE_ATTR_TRACER_LANGUAGE_NAME, Value::str("python")),
            (RESOURCE_ATTR_TRACER_LANGUAGE_VERSION, Value::str("3.12.1")),
            (RESOURCE_ATTR_TRACER_LANGUAGE_INTERPRETER, Value::str("CPython")),
            (RESOURCE_ATTR_TRACER_LANGUAGE_INTERPRETER_VENDOR, Value::str("python.org")),
            (RESOURCE_ATTR_TRACER_VERSION, Value::str("2.14.0")),
            (RESOURCE_ATTR_TRACER_CONTAINER_ID, Value::str("abc123")),
            (RESOURCE_ATTR_TRACER_ENTITY_ID, Value::str("ci-abc123")),
            (RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_TOP_LEVEL, Value::Bool(true)),
            (RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_STATS, Value::Bool(true)),
            (RESOURCE_ATTR_TRACER_DROPPED_P0_TRACES, Value::U64(7)),
            (RESOURCE_ATTR_TRACER_DROPPED_P0_SPANS, Value::U64(21)),
        ] {
            resource.attributes.insert(key, value);
        }
        resource
    }

    // ---- routes ------------------------------------------------------------------------------

    /// v0.4 `PUT`s the plain v0.4 encoder's body; v0.7 its `TracerPayload`. Either way the stats
    /// event goes to `POST /v0.6/stats`, after the traces, and the log goes nowhere.
    #[tokio::test]
    async fn each_version_puts_its_own_route_and_stats_are_posted_after() {
        let mut events = two_traces().events;
        events.push(stats_event());
        events.push(Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::str("not a span"),
                severity: None,
                body_format: logit_core::BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        ));
        let b = batch(events);
        let spans = batch(b.events[..3].to_vec());
        let stats = batch(vec![b.events[3].clone()]);

        for (version, path) in
            [(TracerApiForm::V04, "/v0.4/traces"), (TracerApiForm::V07, "/v0.7/traces")]
        {
            let (addr, log) = accepting().await;
            sink(addr).with_version(version).send(&b).await.expect("the Agent accepts");
            let c = captured(&log);
            let routes: Vec<_> = c.iter().map(|c| (c.method.clone(), c.path.as_str())).collect();
            assert_eq!(routes, [(Method::PUT, path), (Method::POST, "/v0.6/stats")]);
            for request in &c {
                assert_eq!(request.header("content-type"), Some(MSGPACK));
                assert_eq!(request.header("user-agent"), Some(USER_AGENT));
            }
            let mut encoder = DatadogEncoder::new();
            let expected = match version {
                TracerApiForm::V04 => encoder.encode_traces_v04(&spans),
                TracerApiForm::V07 => encoder.encode_tracer_payload_v07(&spans),
            };
            assert_eq!(c[0].body, expected.unwrap().to_vec(), "{path}");
            assert_eq!(c[1].body, encoder.encode_client_stats_v06(&stats).unwrap().to_vec());
            let decoded = DatadogDecoder::new().decode_client_stats_v06(&c[1].body, 0).unwrap();
            assert_eq!(decoded.events.len(), 1);
        }
    }

    /// A batch with no span and no stats makes no request.
    #[tokio::test]
    async fn a_batch_with_nothing_to_send_makes_no_request() {
        let (addr, log) = accepting().await;
        sink(addr).send(&batch(Vec::new())).await.unwrap();
        assert!(captured(&log).is_empty());
    }

    // ---- headers -----------------------------------------------------------------------------

    /// Every tracer header is restored from its carrier, the count is the encoder's, and the
    /// stats request carries none of them.
    #[tokio::test]
    async fn every_tracer_header_is_restored_from_the_resource() {
        let (addr, log) = accepting().await;
        let mut events = two_traces().events;
        events.push(stats_event());
        sink(addr).send(&batch_with(tracer_resource(), events)).await.unwrap();
        let c = captured(&log);
        let expected = [
            ("datadog-meta-lang", "python"),
            ("datadog-meta-lang-version", "3.12.1"),
            ("datadog-meta-lang-interpreter", "CPython"),
            ("datadog-meta-lang-interpreter-vendor", "python.org"),
            ("datadog-meta-tracer-version", "2.14.0"),
            ("datadog-container-id", "abc123"),
            ("datadog-entity-id", "ci-abc123"),
            ("datadog-client-computed-top-level", "true"),
            ("datadog-client-computed-stats", "true"),
            ("datadog-client-dropped-p0-traces", "7"),
            ("datadog-client-dropped-p0-spans", "21"),
            ("x-datadog-trace-count", "2"),
        ];
        for (name, value) in expected {
            assert_eq!(c[0].header(name), Some(value), "{name}");
        }
        for (name, _) in expected {
            assert_eq!(c[1].header(name), None, "stats carry no {name}");
        }
        let spans = DatadogDecoder::new().decode_traces_v04(&c[0].body, 0).unwrap();
        for event in &spans.events {
            assert!(
                event.attributes.iter().all(|(k, _)| !resolve(k).starts_with("datadog.tracer.")),
                "no carrier leaks into a span tag: {:?}",
                event.attributes
            );
        }
    }

    /// With no carrier, no tracer header; a `false` flag is absent, not `false`.
    #[tokio::test]
    async fn an_absent_carrier_sends_no_header() {
        let (addr, log) = accepting().await;
        let mut resource = Resource::default();
        resource.attributes.insert(RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_STATS, Value::Bool(false));
        sink(addr).send(&batch_with(resource, two_traces().events)).await.unwrap();
        let c = captured(&log);
        let tracer: Vec<_> =
            c[0].headers.keys().map(|k| k.as_str()).filter(|k| k.starts_with("datadog-")).collect();
        assert!(tracer.is_empty(), "{tracer:?}");
        assert_eq!(c[0].header("x-datadog-trace-count"), Some("2"));
    }

    /// An operator header goes out; one spelled like a protocol header never does, whether the
    /// protocol sets that header on this request or not.
    #[tokio::test]
    async fn operator_headers_are_sent_under_the_protocols_own() {
        let (addr, log) = accepting().await;
        let mut out = sink(addr)
            .with_headers(&HashMap::from([
                ("X-Proxy-Token".to_string(), "t".to_string()),
                ("Content-Type".to_string(), "text/plain".to_string()),
                ("User-Agent".to_string(), "other".to_string()),
                ("X-Datadog-Trace-Count".to_string(), "99".to_string()),
                ("Datadog-Entity-ID".to_string(), "forged".to_string()),
            ]))
            .unwrap();
        out.send(&two_traces()).await.unwrap();
        let c = &captured(&log)[0];
        assert_eq!(c.header("x-proxy-token"), Some("t"));
        assert_eq!(c.header("content-type"), Some(MSGPACK));
        assert_eq!(c.header("user-agent"), Some(USER_AGENT));
        assert_eq!(c.header("x-datadog-trace-count"), Some("2"));
        assert_eq!(c.header("datadog-entity-id"), None, "no carrier, so no header");
        assert_eq!(c.headers.get_all("content-type").iter().count(), 1);
    }

    // ---- compression -------------------------------------------------------------------------

    #[tokio::test]
    async fn gzip_only_when_asked() {
        let b = two_traces();
        let (addr, log) = accepting().await;
        sink(addr).send(&b).await.unwrap();
        let plain = captured(&log).remove(0);
        assert_eq!(plain.header("content-encoding"), None);

        let (addr, log) = accepting().await;
        sink(addr).with_compression(DatadogTraceCompression::Gzip).send(&b).await.unwrap();
        let gzipped = captured(&log).remove(0);
        assert_eq!(gzipped.header("content-encoding"), Some("gzip"));
        assert_ne!(gzipped.body, plain.body);
        assert_eq!(gzipped.decoded(), plain.body);
    }

    // ---- the splitter ------------------------------------------------------------------------

    /// 1,001 traces are two requests, each with its own trace count, and every span counted.
    #[tokio::test]
    async fn more_traces_than_a_request_holds_are_split_by_trace() {
        let (addr, log) = accepting().await;
        let (registry, mut out) = metered(sink(addr));
        let events: Vec<Event> = (0..1_001u32)
            .map(|i| {
                let mut e = span(0, 1, None);
                let span = e.span.as_mut().unwrap();
                span.trace_id[12..].copy_from_slice(&(i + 1).to_be_bytes());
                e
            })
            .collect();
        out.send(&batch(events)).await.unwrap();
        let counts: Vec<_> = captured(&log)
            .iter()
            .map(|c| c.header("x-datadog-trace-count").unwrap().to_string())
            .collect();
        assert_eq!(counts, ["1000", "1"]);
        let points = registry.drain(0);
        assert_eq!(total(&points, RECORDS, &[("route", "traces")]), 1_001.0);
        assert_eq!(total(&points, REQUESTS, &[("route", "traces"), ("class", "2xx")]), 2.0);
    }

    /// One trace over the 25 MiB cap is dropped and counted per span; the other still goes.
    #[tokio::test]
    async fn a_single_trace_over_the_byte_cap_is_dropped_as_oversize() {
        let (addr, log) = accepting().await;
        let (registry, mut out) = metered(sink(addr));
        let mut huge = span(2, 3, None);
        huge.attributes.insert("blob", Value::str("x".repeat(MAX_REQUEST_BYTES)));
        let mut child = span(2, 4, Some(3));
        child.attributes.insert("n", Value::I64(1));
        out.send(&batch(vec![span(1, 1, None), huge, child])).await.unwrap();
        let c = captured(&log);
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].header("x-datadog-trace-count"), Some("1"));
        let points = registry.drain(0);
        assert_eq!(
            total(&points, RECORDS_DROPPED, &[("route", "traces"), ("reason", "oversize")]),
            2.0
        );
    }

    // ---- responses ---------------------------------------------------------------------------

    async fn fault_for(status: u16) -> Fault {
        let (addr, _log) = agent(move |_| (status, "nope".into())).await;
        let err = sink(addr).send(&two_traces()).await.unwrap_err();
        logit_pipeline::classify(&err)
    }

    #[tokio::test]
    async fn each_response_class_maps_to_its_fault() {
        for status in [408, 429, 500, 503] {
            assert_eq!(fault_for(status).await, Fault::Ambiguous, "{status}");
        }
        for status in [301, 400, 404, 413] {
            assert_eq!(fault_for(status).await, Fault::Permanent, "{status}");
        }
    }

    /// A failing trace request aborts the stats request after it.
    #[tokio::test]
    async fn a_failing_trace_request_aborts_the_stats_request() {
        let (addr, log) =
            agent(|path| (if path == "/v0.4/traces" { 500 } else { 200 }, String::new())).await;
        let mut events = two_traces().events;
        events.push(stats_event());
        sink(addr).send(&batch(events)).await.unwrap_err();
        let paths: Vec<_> = captured(&log).iter().map(|c| c.path.clone()).collect();
        assert_eq!(paths, ["/v0.4/traces"]);
    }

    /// A 413 counts the request's spans oversize, and the request by its class.
    #[tokio::test]
    async fn a_413_counts_the_requests_spans_oversize() {
        let (addr, _log) = agent(|_| (413, String::new())).await;
        let (registry, mut out) = metered(sink(addr));
        out.send(&two_traces()).await.unwrap_err();
        let points = registry.drain(0);
        assert_eq!(
            total(&points, RECORDS_DROPPED, &[("route", "traces"), ("reason", "oversize")]),
            3.0
        );
        assert_eq!(total(&points, REQUESTS, &[("route", "traces"), ("class", "4xx")]), 1.0);
    }

    #[tokio::test]
    async fn connect_refused_is_clean() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let (registry, mut out) = metered(sink(addr));
        let err = out.send(&two_traces()).await.unwrap_err();
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
        let points = registry.drain(0);
        assert_eq!(
            total(&points, REQUESTS, &[("route", "traces"), ("class", "network_error")]),
            1.0
        );
    }

    // ---- the Unix socket ---------------------------------------------------------------------

    /// A fresh directory under the system temp dir, removed on drop. Short, because a Unix socket
    /// path is limited to about 100 bytes.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("ldto-{tag}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// The same request over a Unix socket: one accepted connection serving it.
    #[tokio::test]
    async fn the_unix_client_sends_to_the_socket() {
        let dir = TempDir::new("one");
        let path = dir.0.join("apm.socket");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let log: Log = Arc::default();
        let respond: Respond = Arc::new(|_| (200, RATES.into()));
        let (task_log, task_respond) = (log.clone(), respond.clone());
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            serve(stream, task_log, task_respond);
        });

        let (registry, mut out) = metered(DatadogTraceOutput::unix(&path));
        out.send(&batch_with(tracer_resource(), two_traces().events)).await.unwrap();
        let c = captured(&log);
        assert_eq!(c.len(), 1);
        assert_eq!((c[0].method.clone(), c[0].path.as_str()), (Method::PUT, "/v0.4/traces"));
        assert_eq!(c[0].header("datadog-meta-lang"), Some("python"));
        assert_eq!(c[0].header("x-datadog-trace-count"), Some("2"));
        assert_eq!(c[0].header("host"), Some("localhost"));
        let spans = DatadogDecoder::new().decode_traces_v04(&c[0].body, 0).unwrap();
        assert_eq!(spans.events.len(), 3);
        let points = registry.drain(0);
        assert_eq!(total(&points, RECORDS, &[("route", "traces")]), 3.0);
    }

    /// No socket file, or nobody listening on it, is a connect failure: `Clean`.
    #[tokio::test]
    async fn a_missing_or_dead_socket_is_clean() {
        let dir = TempDir::new("dead");
        let missing = dir.0.join("missing.socket");
        let err = DatadogTraceOutput::unix(&missing).send(&two_traces()).await.unwrap_err();
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean, "{err:#}");
        assert!(format!("{err:#}").contains("missing.socket"), "{err:#}");

        let dead = dir.0.join("dead.socket");
        drop(std::os::unix::net::UnixListener::bind(&dead).unwrap());
        let err = DatadogTraceOutput::unix(&dead).send(&two_traces()).await.unwrap_err();
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean, "{err:#}");
    }

    /// A socket that accepts and never answers times out as `Ambiguous`.
    #[tokio::test]
    async fn a_silent_socket_times_out_ambiguous() {
        let dir = TempDir::new("slow");
        let path = dir.0.join("apm.socket");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        let mut out = DatadogTraceOutput::unix(&path).with_timeout(Duration::from_millis(200));
        let err = out.send(&two_traces()).await.unwrap_err();
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous, "{err:#}");
    }

    #[test]
    fn tls_on_the_socket_is_refused() {
        let settings = TlsClientSettings { insecure_skip_verify: true, ..Default::default() };
        let result =
            DatadogTraceOutput::unix("/run/apm.socket").with_tls(&settings, Path::new("."));
        assert!(result.is_err());
    }

    #[test]
    fn datadog_trace_output_is_not_duplicate_safe() {
        assert!(!DatadogTraceOutput::http("http://127.0.0.1:8126").duplicate_safe());
    }

    // ---- attempt accounting (ADR `sink-send-path-and-attempt-accounting`, decision 2) --------

    use crate::test_support::{
        assert_counted_once_per_batch, assert_direct_sends_count_after_an_empty_batch, fast_retry,
        http_recorder, refused_addr, sum_of, sums_through_write_loop, RecordLog, Recorded,
        Reply as Answer, SumSeries, Sums, HUNG_REQUEST_BUDGET,
    };
    use logit_pipeline::test_util::TelemetryProbe;
    use logit_pipeline::{DeliveryPosture, WriteLoopConfig};
    use logit_proto::datadog::RESOURCE_ATTR_TRACER_RUNTIME_ID;

    const TRACES: &str = "/v0.4/traces";
    const STATS: &str = "/v0.6/stats";

    /// A resource whose runtime id has no v0.4 field (the trace codec counts it `no_wire_form`
    /// once per request body) and whose language isn't a legal header value (the sink leaves the
    /// header out and diagnoses `bad_header`).
    fn lossy_resource() -> Resource {
        let mut resource = Resource::default();
        resource.attributes.insert(RESOURCE_ATTR_TRACER_RUNTIME_ID, Value::str("rt-1"));
        resource.attributes.insert(RESOURCE_ATTR_TRACER_LANGUAGE_NAME, Value::str("py\nthon"));
        resource
    }

    /// A span with a map attribute, which the trace codec writes as JSON text and counts
    /// `json_text`.
    fn span_with_map(trace: u8, id: u8, parent: Option<u8>) -> Event {
        let mut event = span(trace, id, parent);
        let map = AttrMap::from_iter([("k", Value::str("v"))]);
        event.attributes.insert("nested", Value::Map(Box::new(map)));
        event
    }

    /// A stats group with an attribute the stats codec has no field for, counted `no_wire_form`.
    fn lossy_stats_event() -> Event {
        let mut event = stats_event();
        event.attributes.insert("not.a.group.field", Value::str("x"));
        event
    }

    /// Two traces and a stats group, each unit with codec counts, and the `bad_header` diagnostic.
    fn encode_side_batch() -> EventBatch {
        batch_with(
            lossy_resource(),
            vec![
                span_with_map(1, 1, None),
                span_with_map(1, 2, Some(1)),
                span_with_map(2, 3, None),
                lossy_stats_event(),
            ],
        )
    }

    const ENCODE_SIDE: [SumSeries<'static>; 4] = [
        ("logit.output.spans.degraded", &[("reason", "json_text")]),
        ("logit.output.spans.degraded", &[("reason", "no_wire_form")]),
        ("logit.component.diagnostics", &[("key", "bad_header")]),
        ("logit.output.tags.dropped", &[("reason", "no_wire_form")]),
    ];

    /// Beyond `logit.output.requests`, what a retried batch counts once per attempt here.
    const PER_ATTEMPT: [SumSeries<'static>; 2] = [(REQUEST_BYTES, &[]), (RECORDS, &[])];

    fn accepted() -> Answer {
        Answer::Answer(200, RATES.as_bytes().to_vec())
    }

    /// `503` on a path's first request, then [`accepted`].
    fn busy_once(k: usize) -> Answer {
        if k == 0 {
            Answer::Answer(503, Vec::new())
        } else {
            accepted()
        }
    }

    /// An Agent answering the `k`th request (from 0) on each path with `script(path, k)`.
    async fn scripted_agent(
        script: impl Fn(&str, usize) -> Answer + Send + Sync + 'static,
    ) -> (SocketAddr, RecordLog) {
        let per_path: Mutex<HashMap<String, usize>> = Mutex::default();
        http_recorder(move |_, path, _| {
            let k = {
                let mut per_path = per_path.lock().unwrap();
                let seen = per_path.entry(path.to_string()).or_default();
                *seen += 1;
                *seen - 1
            };
            script(path, k)
        })
        .await
    }

    fn at_least_once() -> WriteLoopConfig {
        WriteLoopConfig { delivery_override: Some(DeliveryPosture::AtLeastOnce), ..fast_retry() }
    }

    fn instrumented(addr: SocketAddr, probe: &TelemetryProbe) -> DatadogTraceOutput {
        let telemetry = probe.telemetry("out", "datadog_trace_out", "sink");
        sink(addr)
            .with_diagnostics(Diagnostics::new("out").with_telemetry(telemetry.clone()))
            .with_telemetry(telemetry)
    }

    /// `batches` through the write loop under `config`, over the sink `build` makes, against a
    /// [`scripted_agent`]; and the requests it received.
    async fn run_agent(
        script: impl Fn(&str, usize) -> Answer + Send + Sync + 'static,
        batches: Vec<EventBatch>,
        config: WriteLoopConfig,
        build: impl FnOnce(SocketAddr, &TelemetryProbe) -> DatadogTraceOutput,
    ) -> (Sums, Vec<Recorded>) {
        let (addr, log) = scripted_agent(script).await;
        let mut probe = TelemetryProbe::new();
        let mut output = build(addr, &probe);
        let sums =
            sums_through_write_loop(&mut output, &mut probe, "datadog_trace_out", batches, config)
                .await;
        let log = log.lock().unwrap().clone();
        (sums, log)
    }

    fn recorded_paths(log: &[Recorded]) -> Vec<&str> {
        log.iter().map(|r| r.path.as_str()).collect()
    }

    fn bodies(log: &[Recorded], path: &str) -> Vec<Vec<u8>> {
        log.iter().filter(|r| r.path == path).map(|r| r.body.clone()).collect()
    }

    /// The stats request fails after the traces were sent: the retry re-sends both, and each
    /// unit's codec counts and the `bad_header` diagnostic read as after one attempt.
    #[tokio::test]
    async fn a_stats_failure_after_the_traces_were_sent_counts_each_units_encode_side_once() {
        let batches = || vec![encode_side_batch()];
        let (single, one) =
            run_agent(|_, _| accepted(), batches(), at_least_once(), instrumented).await;
        let script = |p: &str, k| if p == STATS { busy_once(k) } else { accepted() };
        let (retried, log) = run_agent(script, batches(), at_least_once(), instrumented).await;

        assert_eq!(recorded_paths(&one), [TRACES, STATS]);
        assert_eq!(recorded_paths(&log), [TRACES, STATS, TRACES, STATS], "two attempts");
        assert_eq!(sum_of(&retried, REQUESTS, &[("route", "traces"), ("class", "2xx")]), 2.0);
        assert_eq!(sum_of(&retried, REQUESTS, &[("route", "stats"), ("class", "5xx")]), 1.0);
        assert_eq!(sum_of(&retried, REQUESTS, &[("route", "stats"), ("class", "2xx")]), 1.0);
        assert_eq!(sum_of(&retried, RECORDS, &[("route", "traces")]), 6.0, "three spans, twice");
        assert_eq!(sum_of(&retried, RECORDS, &[("route", "stats")]), 1.0);
        let bytes = sum_of(&single, REQUEST_BYTES, &[("route", "traces")]);
        assert_eq!(sum_of(&retried, REQUEST_BYTES, &[("route", "traces")]), 2.0 * bytes);
        for path in [TRACES, STATS] {
            let first = &bodies(&one, path)[0];
            for body in bodies(&log, path) {
                assert_eq!(&body, first, "{path}: a retry sends the first attempt's bytes");
            }
        }
        assert_counted_once_per_batch(&single, &retried, &ENCODE_SIDE, &PER_ATTEMPT);
    }

    /// The traces request fails first, so attempt 1 never encodes the stats: they count on
    /// attempt 2, their first encode, once.
    #[tokio::test]
    async fn stats_first_encoded_on_a_retry_count_their_encode_side_then() {
        let batches = || vec![encode_side_batch()];
        let (single, _) =
            run_agent(|_, _| accepted(), batches(), at_least_once(), instrumented).await;
        let script = |p: &str, k| if p == TRACES { busy_once(k) } else { accepted() };
        let (retried, log) = run_agent(script, batches(), at_least_once(), instrumented).await;
        assert_eq!(recorded_paths(&log), [TRACES, TRACES, STATS]);
        assert_counted_once_per_batch(&single, &retried, &ENCODE_SIDE, &PER_ATTEMPT);
    }

    /// 1,001 traces are two count-capped requests. The runtime id the v0.4 form can't carry is
    /// counted once per request body, so twice, on one attempt and on a retried one alike.
    #[tokio::test]
    async fn a_per_body_counter_counts_once_per_count_capped_chunk_on_every_attempt() {
        let traces = || {
            let events: Vec<Event> = (0..1_001u32)
                .map(|i| {
                    let mut e = span(0, 1, None);
                    let span = e.span.as_mut().unwrap();
                    span.trace_id[12..].copy_from_slice(&(i + 1).to_be_bytes());
                    e
                })
                .collect();
            let mut resource = Resource::default();
            resource.attributes.insert(RESOURCE_ATTR_TRACER_RUNTIME_ID, Value::str("rt-1"));
            vec![batch_with(resource, events)]
        };
        let (single, one) =
            run_agent(|_, _| accepted(), traces(), at_least_once(), instrumented).await;
        let (retried, log) =
            run_agent(|_, k| busy_once(k), traces(), at_least_once(), instrumented).await;
        assert_eq!(one.len(), 2, "two count-capped requests");
        assert_eq!(log.len(), 3, "the first request failed, then both were resent");
        let lost = [("reason", "no_wire_form")];
        assert_eq!(sum_of(&single, "logit.output.spans.degraded", &lost), 2.0);
        assert_eq!(sum_of(&retried, "logit.output.spans.degraded", &lost), 2.0);
        let encode_side: [SumSeries<'_>; 1] = [("logit.output.spans.degraded", &lost)];
        assert_counted_once_per_batch(&single, &retried, &encode_side, &PER_ATTEMPT);
    }

    /// The gate re-arms per batch: a second batch counts as the first did.
    #[tokio::test]
    async fn a_second_trace_batch_counts_its_encode_side_counters() {
        let batches = vec![encode_side_batch(), encode_side_batch()];
        let (sums, _) = run_agent(|_, _| accepted(), batches, fast_retry(), instrumented).await;
        let (one, _) =
            run_agent(|_, _| accepted(), vec![encode_side_batch()], fast_retry(), instrumented)
                .await;
        for (name, tags) in ENCODE_SIDE {
            assert_eq!(
                sum_of(&sums, name, tags),
                2.0 * sum_of(&one, name, tags),
                "{name} {tags:?}"
            );
        }
    }

    /// A batch whose stats request never answers is cut off by the retry budget and dropped, and
    /// the next batch counts its encode-side counters.
    #[tokio::test]
    async fn a_trace_batch_after_one_dropped_at_its_budget_counts_encode_side() {
        let script = |p: &str, k| if p == STATS && k == 0 { Answer::Hang } else { accepted() };
        let mut config = fast_retry();
        config.retry.total_budget = HUNG_REQUEST_BUDGET;
        let batches = vec![encode_side_batch(), encode_side_batch()];
        let (sums, log) = run_agent(script, batches, config, instrumented).await;
        let (one, _) =
            run_agent(|_, _| accepted(), vec![encode_side_batch()], fast_retry(), instrumented)
                .await;
        assert_eq!(sum_of(&sums, "logit.component.batches.dropped", &[]), 1.0);
        assert_eq!(sum_of(&sums, "logit.component.batches.delivered", &[]), 1.0);
        assert_eq!(recorded_paths(&log), [TRACES, STATS, TRACES, STATS]);
        for (name, tags) in ENCODE_SIDE {
            assert_eq!(
                sum_of(&sums, name, tags),
                2.0 * sum_of(&one, name, tags),
                "{name} {tags:?}"
            );
        }
    }

    /// A batch with nothing to send returns `Ok` early and leaves the accounting disarmed, so
    /// later direct sends count.
    #[tokio::test]
    async fn trace_direct_sends_after_a_batch_that_sent_nothing_count_every_time() {
        let (addr, log) = scripted_agent(|_, _| accepted()).await;
        let mut probe = TelemetryProbe::new();
        let mut output = instrumented(addr, &probe);
        let nothing = Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::str("not a span"),
                severity: None,
                body_format: logit_core::BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        assert_direct_sends_count_after_an_empty_batch(
            &mut output,
            &mut probe,
            "datadog_trace_out",
            batch(vec![nothing]),
            encode_side_batch,
            &ENCODE_SIDE,
        )
        .await;
        let log = log.lock().unwrap().clone();
        assert_eq!(recorded_paths(&log), [TRACES, STATS, TRACES, STATS], "the two direct sends");
    }

    /// One of the two builders that rebuild the encoder.
    #[derive(Clone, Copy, Debug)]
    enum Builder {
        Diagnostics,
        Telemetry,
    }

    /// Both orders of the two encoder-building builders, each after a first call with other
    /// handles, leave the encoder counting through gated views of the last handles.
    #[tokio::test]
    async fn every_trace_builder_order_gates_the_encoder_on_the_final_handles() {
        use Builder::{Diagnostics as D, Telemetry as T};
        for order in [[D, T], [T, D]] {
            let decoy = Registry::new();
            let build = |addr: SocketAddr, probe: &TelemetryProbe| -> DatadogTraceOutput {
                let other = decoy.telemetry_for("other", "datadog_trace_out", "sink");
                let mut sink = sink(addr)
                    .with_telemetry(other.clone())
                    .with_diagnostics(Diagnostics::new("other").with_telemetry(other));
                let telemetry = probe.telemetry("out", "datadog_trace_out", "sink");
                for builder in order {
                    sink = match builder {
                        Builder::Diagnostics => sink.with_diagnostics(
                            Diagnostics::new("out").with_telemetry(telemetry.clone()),
                        ),
                        Builder::Telemetry => sink.with_telemetry(telemetry.clone()),
                    };
                }
                sink
            };
            let batches = || vec![encode_side_batch()];
            let (single, _) = run_agent(|_, _| accepted(), batches(), at_least_once(), build).await;
            let script = |p: &str, k| if p == STATS { busy_once(k) } else { accepted() };
            let (retried, _) = run_agent(script, batches(), at_least_once(), build).await;
            assert_counted_once_per_batch(&single, &retried, &ENCODE_SIDE, &PER_ATTEMPT);
            let stale = decoy.drain(0).iter().map(|e| e.metrics.len()).sum::<usize>();
            assert_eq!(stale, 0, "{order:?}: nothing counts through a replaced handle");
        }
    }

    /// With no handle builders, a retried batch reports its `bad_header` diagnostic once, read
    /// from the sink's own throttle.
    #[tokio::test]
    async fn a_trace_sink_with_no_handle_builders_reports_each_encode_side_diagnostic_once() {
        let (addr, log) =
            scripted_agent(|p, k| if p == STATS { busy_once(k) } else { accepted() }).await;
        let mut probe = TelemetryProbe::new();
        let mut output = sink(addr);
        let batches = vec![encode_side_batch()];
        sums_through_write_loop(
            &mut output,
            &mut probe,
            "datadog_trace_out",
            batches,
            at_least_once(),
        )
        .await;
        assert_eq!(recorded_paths(&log.lock().unwrap()), [TRACES, STATS, TRACES, STATS]);
        assert_eq!(output.diag.occurrences("bad_header"), 1);
    }

    /// A trace too large to send alone is dropped and counted once, and diagnosed once, on a
    /// batch whose stats request is retried. Under a 2,000-byte cap the ordinary trace and the
    /// stats group fit, and the trace with a 5,000-byte attribute doesn't.
    #[tokio::test]
    async fn an_oversize_trace_counts_once_on_a_retried_batch() {
        let small = |addr: SocketAddr, probe: &TelemetryProbe| {
            let mut out = instrumented(addr, probe);
            out.caps_override = Some(Caps { wire_bytes: 2_000, ..CAPS });
            out
        };
        let mut big = span(2, 3, None);
        big.attributes.insert("blob", Value::str("x".repeat(5_000)));
        let events = vec![span(1, 1, None), big, stats_event()];
        let b = || vec![batch(events.clone())];
        let (single, _) = run_agent(|_, _| accepted(), b(), at_least_once(), small).await;
        let script = |p: &str, k| if p == STATS { busy_once(k) } else { accepted() };
        let (retried, log) = run_agent(script, b(), at_least_once(), small).await;
        assert_eq!(recorded_paths(&log), [TRACES, STATS, TRACES, STATS]);
        let encode_side: [SumSeries<'static>; 2] = [
            (RECORDS_DROPPED, &[("route", "traces"), ("reason", "oversize")]),
            ("logit.component.diagnostics", &[("key", "oversize")]),
        ];
        for sums in [&single, &retried] {
            assert_eq!(sum_of(sums, RECORDS_DROPPED, encode_side[0].1), 1.0);
        }
        assert_counted_once_per_batch(&single, &retried, &encode_side, &PER_ATTEMPT);
    }

    /// A `413` answered on a retry is the Agent's verdict on that attempt: its spans are counted
    /// oversize and it is diagnosed, through the sink's ungated handles.
    #[tokio::test]
    async fn a_413_answered_on_a_retry_is_counted() {
        let script = |_: &str, k| match k {
            0 => Answer::Answer(503, Vec::new()),
            _ => Answer::Answer(413, b"too large".to_vec()),
        };
        let (sums, log) =
            run_agent(script, vec![two_traces()], at_least_once(), instrumented).await;
        assert_eq!(recorded_paths(&log), [TRACES, TRACES]);
        assert_eq!(sum_of(&sums, "logit.component.batches.dropped", &[]), 1.0);
        let oversize = [("route", "traces"), ("reason", "oversize")];
        assert_eq!(sum_of(&sums, RECORDS_DROPPED, &oversize), 3.0);
        let rejected = [("key", "request_rejected")];
        assert_eq!(sum_of(&sums, "logit.component.diagnostics", &rejected), 1.0);
    }

    // ---- request.bytes -----------------------------------------------------------------------

    /// A refused TCP connection and a missing socket file sent nothing, so they count no
    /// `request.bytes`, only their request.
    #[tokio::test]
    async fn a_refused_connection_or_a_missing_socket_counts_no_request_bytes() {
        let dir = TempDir::new("bytes");
        for out in [sink(refused_addr().await), DatadogTraceOutput::unix(dir.0.join("missing"))] {
            let (registry, mut out) = metered(out);
            let err = out.send(&two_traces()).await.unwrap_err();
            assert_eq!(logit_pipeline::classify(&err), Fault::Clean, "{err:#}");
            let points = registry.drain(0);
            assert_eq!(total(&points, REQUEST_BYTES, &[]), 0.0, "{err:#}");
            let refused = [("route", "traces"), ("class", "network_error")];
            assert_eq!(total(&points, REQUESTS, &refused), 1.0);
        }
    }

    /// A request that got an answer counts the body as sent, and so does one that timed out,
    /// which may have reached the Agent.
    #[tokio::test]
    async fn an_answered_or_timed_out_request_counts_its_bytes() {
        let (addr, log) = accepting().await;
        let (registry, mut out) = metered(sink(addr));
        out.send(&two_traces()).await.unwrap();
        let sent = captured(&log)[0].body.len() as f64;
        assert_eq!(total(&registry.drain(0), REQUEST_BYTES, &[("route", "traces")]), sent);

        let (addr, _log) = http_recorder(|_, _, _| Answer::Hang).await;
        // The recorder never answers, so the timeout ends the request whatever its length: 100 ms
        // bounds only how long the test waits, and a loaded machine can't make it fire early.
        let (registry, mut out) = metered(sink(addr).with_timeout(Duration::from_millis(100)));
        let err = out.send(&two_traces()).await.unwrap_err();
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
        assert!(total(&registry.drain(0), REQUEST_BYTES, &[("route", "traces")]) > 0.0);
    }
}
