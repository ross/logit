//! `splunk_hec_in`: a stand-in for Splunk's HTTP Event Collector (HEC), the receiving end of any
//! HEC client (Docker's `splunk` log driver, Splunk's logging libraries, the OpenTelemetry
//! Collector's `splunk_hec` exporter, Vector)
//! ([ADR `splunk-hec-relay`](../../../../docs/adr/splunk-hec-relay.md),
//! [`docs/plans/splunk-relay.md`](../../../../docs/plans/splunk-relay.md) §2). One TCP listener,
//! optionally TLS, serves HTTP/1.1 and h2c through [`hyper_util::server::conn::auto::Builder`],
//! and every body decodes through [`logit_proto::splunk::SplunkDecoder`]. The payload mappings
//! live in that codec's module doc; this module owns HTTP: routing, authentication, compression,
//! size caps, channels and acknowledgment, and backpressure.
//!
//! The accept loop, connection cap, handshake timeout, and idle timeout are `datadog_in`'s
//! ([`crate::datadog`]), which are `otlp_in`'s; `crate::otlp`'s module doc has the reasoning for
//! each. The request helpers (`Content-Encoding`, bounded decompression, the constant-time token
//! check, deadline-bounded delivery) are shared through [`crate::http`].
//!
//! # Routes
//!
//! One trailing `/` is stripped before matching.
//!
//! | Request | Handling | Response |
//! |---|---|---|
//! | `POST /services/collector`, `/services/collector/event`, `/services/collector/event/1.0` | `decode_events`: concatenated objects or an array, one batch per resource | `200` `{"text":"Success","code":0}`, plus `"ackID":N` with a channel |
//! | `POST /services/collector/raw`, `/services/collector/raw/1.0` | `decode_raw`: one log per line, the envelope from the query string's `host`, `source`, `sourcetype`, and `index` | as `/event` |
//! | `POST /services/collector/ack` | `{"acks":[<id>,…]}` | `200` `{"acks":{"<id>":true,…}}` |
//! | `GET`/`HEAD /services/collector/health`, `/services/collector/health/1.0` | none; no authentication | `200` `{"text":"HEC is healthy","code":17}` |
//! | another method on a path above | none | `405` + `Allow`, `{"text":"Method Not Allowed","code":405}` |
//! | any other path | none | `404` `{"text":"Not Found","code":404}` |
//!
//! Every body is Splunk's own `{"text","code"}` shape ([`logit_proto::splunk::response`]), so a
//! HEC client's error handling reads a `logit` answer as it reads Splunk's. An HTTP-level error
//! Splunk answers without a HEC code (`404`, `405`, `408`, `413`, `415`) carries the HTTP status
//! as its `code`.
//!
//! **Channels and acknowledgment.** A request that names a channel (the `X-Splunk-Request-Channel`
//! header, or `?channel=`) is answered with an `ackID`, drawn from one per-listener counter that
//! starts at 1; a request without one gets no `ackID`, as from a token without `useACK`. `/ack`
//! answers every id it is asked about `true`: a `200` already means the data reached the pipeline,
//! and a pipeline that can't take it answers `503` instead. No channel is ever required, and
//! neither the channel nor the id enters an event.
//!
//! # Request handling
//!
//! In order, after the connection-level steps `otlp_in` also takes:
//!
//! 1. **Route and method**, as the table above. `/health` answers here, before authentication.
//! 2. **Size.** A `Content-Length` over `max_request_bytes` is a `413` before any byte is read, and
//!    the body is read through [`Limited`] at the same cap in case the header lied or was absent.
//! 3. **Authentication.** A `token` query parameter is `400` code 16. With `tokens` configured,
//!    the `Authorization` header must be `Splunk <token>`, or `Basic` with the token as the
//!    password, else `401` or `403` with Splunk's code. The comparison takes the same time for
//!    every token of one length, and no token is ever logged, counted, or kept on an event. The
//!    per-outcome codes, the error bodies, and when an `ackID` is issued are recorded in
//!    [ADR `splunk-hec-relay`](../../../../docs/adr/splunk-hec-relay.md)'s amendment "what the
//!    listener settled"; [`authenticate`] and [`respond`] implement them.
//! 4. **`Content-Encoding`.** `identity` (or none) or `gzip`, else `415`: `deflate` and `zstd`
//!    included, which Splunk doesn't accept either.
//! 5. **Body.** A body that stops arriving mid-upload gets `408` and the connection closes, when
//!    `idle_timeout` is set. The gzip output is capped at `max_request_bytes` too (`413` past it),
//!    and a stream that doesn't decompress is `400` code 6.
//! 6. **Decode.** An empty body is `400` code 5. A body that isn't HEC JSON is the codec's
//!    [`HecError`]: code 6 with `invalid-event-number` naming the first bad object, and nothing is
//!    delivered. A `/raw` body with no non-empty line is code 5. A malformed `/ack` body is code
//!    6.
//! 7. **Delivery**, bounded (below), then the route's `200`. A body whose every object the codec
//!    skipped (no `event`) sends nothing and still answers `200`.
//!
//! # Backpressure: a bounded wait, then `503`
//!
//! As on `datadog_in` ([`crate::datadog`]'s "Backpressure" section): the request's batches are
//! sent in order under one deadline, [`BUSY_AFTER`] from the start of delivery, each reaching every
//! downstream consumer or none. When the deadline passes, the request is answered `503`
//! `{"text":"Server is busy","code":9}` with `Retry-After: 1`, counted
//! `logit.input.requests{class="busy"}`, and the batches not yet delivered are counted
//! `logit.input.batches.dropped{reason="busy"}`. Every HEC client retries a code 9.
//!
//! **A `503` after partial delivery duplicates.** A `/event` body carrying several envelopes
//! decodes to one batch per resource. If the deadline passes after some of them were delivered,
//! the client's retry sends the whole body again, and the batches already delivered are delivered
//! twice. Splunk indexes a resent event twice as well; the timed-out batch itself reaches no
//! consumer.
//!
//! # Telemetry
//!
//! Every name and tag is `&'static`. Per request: `logit.input.requests{route, class}` (class `ok`,
//! `rejected`, or `busy`; route `event`, `raw`, `ack`, `health`, or `unknown`),
//! `logit.input.request.duration` (timing, every exit), and `logit.input.request.bytes` (the body
//! size as sent, once read). Rejections: `logit.input.requests.rejected{reason}`, reason
//! `unknown_route`, `method`, `oversize`, `query_token`, `auth`, `encoding`,
//! `malformed_encoding`, `no_data`, `malformed`, `stalled`, or `body_read` (a body that failed for a
//! reason other than its size, such as a client disconnecting mid-upload).
//! `logit.input.batches.dropped{reason="busy"}` counts the batches a `503` left undelivered. The
//! connection metrics are `otlp_in`'s verbatim. `docs/design/internal-telemetry.md`'s
//! `splunk_hec_in` section is the operator-facing account.

use crate::http::{
    body_read_error_message, collect_with_stall_bound, declared_length, decompress,
    deliver_with_deadline, drive_with_idle, is_length_limit, json_response, matches_any_key,
    now_nanos, Activity, BodyReadError, DecompressError, Encoding,
};
use crate::Input;
use base64::Engine as _;
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use http_body_util::{Full, Limited};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use logit_core::{Diagnostics, EventBatch, Telemetry};
use logit_pipeline::Fanout;
use logit_proto::splunk::response::{
    encode_ack_reply, encode_http_error, encode_status, encode_success, parse_ack_request,
};
use logit_proto::splunk::{Envelope, HecError, HecStatus, SplunkDecoder};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

/// Default for [`SplunkHecInput::with_max_request_bytes`]: 5 MiB, the OpenTelemetry exporter's
/// `max_event_size`, above its 2 MiB default body. Mirrored by hand in
/// `logit_config::default_splunk_max_request_bytes`.
pub const DEFAULT_MAX_REQUEST_BYTES: usize = 5 * 1024 * 1024;

/// Bounds the connections [`Input::run`] serves at once: the same 1024 as every other HTTP
/// listener. A connection past the cap is rejected, not queued.
const MAX_CONCURRENT_CONNECTIONS: usize = 1024;

/// Default for [`SplunkHecInput::with_handshake_timeout`]: the same 5s as every other TCP
/// listener, mirrored by hand in `logit_config::default_handshake_timeout`. Also the grace an idle
/// close gives hyper.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// How long one request's delivery may wait on a full downstream before it is answered `503` code
/// 9 (this module's "Backpressure" section), `datadog_in`'s bound.
const BUSY_AFTER: Duration = Duration::from_secs(5);

/// The header a HEC client names its channel in.
const CHANNEL_HEADER: &str = "x-splunk-request-channel";

/// `crate::tls::TlsServerSettings`, re-exported as `otlp_in`'s is.
pub use crate::tls::TlsServerSettings;

/// The `splunk_hec_in` listener. See this module's doc.
pub struct SplunkHecInput {
    bind: String,
    diag: Diagnostics,
    telemetry: Telemetry,
    tls: Option<Arc<rustls::ServerConfig>>,
    /// Set by [`Input::bind`], taken by [`Input::run`]. `None` after a run, so a second run
    /// rebinds.
    listener: Option<TcpListener>,
    handshake_timeout: Duration,
    /// `None`, the default, means no idle timeout.
    idle_timeout: Option<Duration>,
    /// Empty accepts any request (this module's "Request handling", step 3).
    tokens: Arc<[Box<[u8]>]>,
    max_request_bytes: usize,
    max_connections: usize,
    busy_after: Duration,
    /// The next `ackID`, shared by every connection of this listener.
    next_ack_id: Arc<AtomicU64>,
}

impl SplunkHecInput {
    pub fn new(bind: impl Into<String>) -> Self {
        Self {
            bind: bind.into(),
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
            tls: None,
            listener: None,
            handshake_timeout: HANDSHAKE_TIMEOUT,
            idle_timeout: None,
            tokens: Arc::from(Vec::new()),
            max_request_bytes: DEFAULT_MAX_REQUEST_BYTES,
            max_connections: MAX_CONCURRENT_CONNECTIONS,
            busy_after: BUSY_AFTER,
            next_ack_id: Arc::new(AtomicU64::new(1)),
        }
    }

    /// The address bound, once [`Input::bind`] has run, so a caller can learn the OS-assigned port
    /// without a bind-drop-rebind race.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.listener.as_ref().and_then(|l| l.local_addr().ok())
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Turns on TLS termination (`tls:` in config). Paths in `settings` resolve against
    /// `base_dir`, the config file's directory. Both ALPN protocols the auto builder serves are
    /// advertised.
    pub fn with_tls(
        mut self,
        settings: &TlsServerSettings,
        base_dir: &Path,
    ) -> anyhow::Result<Self> {
        let alpn: &[&[u8]] = &[b"h2", b"http/1.1"];
        self.tls = Some(Arc::new(crate::tls::build_server_config(settings, base_dir, alpn)?));
        Ok(self)
    }

    /// Overrides [`HANDSHAKE_TIMEOUT`] for the TLS accept and the plaintext first-byte peek
    /// (`handshake_timeout:` in config). Graph rule 45 rejects `0s`.
    pub fn with_handshake_timeout(mut self, handshake_timeout: Duration) -> Self {
        self.handshake_timeout = handshake_timeout;
        self
    }

    /// Bounds how long a connection may sit with no request in flight (`idle_timeout:` in config;
    /// off when `None`), with `otlp_in`'s semantics. Graph rule 53 rejects `Some(0s)`.
    pub fn with_idle_timeout(mut self, idle_timeout: Option<Duration>) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    /// The HEC tokens this listener accepts (`tokens:` in config). Empty accepts any request.
    /// Graph rule 69 rejects an empty or whitespace-padded entry.
    pub fn with_tokens(mut self, tokens: Vec<String>) -> Self {
        self.tokens = tokens.into_iter().map(|t| t.into_bytes().into_boxed_slice()).collect();
        self
    }

    /// Overrides [`DEFAULT_MAX_REQUEST_BYTES`], the cap on a request body both as sent and after
    /// gzip decompression (`max_request_bytes:` in config). Graph rule 69 rejects `0`.
    pub fn with_max_request_bytes(mut self, max_request_bytes: usize) -> Self {
        self.max_request_bytes = max_request_bytes;
        self
    }

    /// Test-only override of [`MAX_CONCURRENT_CONNECTIONS`].
    #[cfg(test)]
    fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Overrides [`BUSY_AFTER`]. Not a config field: a test/tuning hook, as on `datadog_in`, so
    /// a round-trip test's busy case doesn't wait out five real seconds.
    pub fn with_busy_after(mut self, d: Duration) -> Self {
        self.busy_after = d;
        self
    }
}

#[async_trait::async_trait]
impl Input for SplunkHecInput {
    async fn bind(&mut self) -> anyhow::Result<()> {
        if self.listener.is_some() {
            return Ok(()); // idempotent, per `Input::bind`'s contract
        }
        let listener = TcpListener::bind(&self.bind).await?;
        self.diag.info("bound", format_args!("listening on {}", self.bind));
        self.listener = Some(listener);
        Ok(())
    }

    /// `datadog_in`'s accept loop: a permit per connection, taken before any TLS accept and
    /// rejected rather than queued at the cap; the handshake, or a plaintext first-byte peek,
    /// bounded inside the spawned task; a clean close before the first byte treated as a health
    /// check.
    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        self.bind().await?;
        let listener = self.listener.take().expect("bind() leaves a listener behind");
        let connection_limit = Arc::new(tokio::sync::Semaphore::new(self.max_connections));
        let tls_acceptor = self.tls.clone().map(TlsAcceptor::from);
        let live_connections = Arc::new(AtomicI64::new(0));
        let handshake_timeout = self.handshake_timeout;
        let idle_timeout = self.idle_timeout;
        let mut accept_queue =
            crate::tcp::AcceptQueueSampler::new(self.telemetry.clone(), self.diag.clone());
        loop {
            let (stream, peer) = accept_queue.accept(&listener).await?;

            let Ok(permit) = connection_limit.clone().try_acquire_owned() else {
                self.telemetry.count(
                    "logit.input.connections.rejected",
                    1.0,
                    &[("reason", "limit")],
                );
                drop(stream);
                continue;
            };

            let shared = Arc::new(Shared {
                sink: sink.clone(),
                telemetry: self.telemetry.clone(),
                diag: self.diag.clone(),
                tokens: Arc::clone(&self.tokens),
                max_request_bytes: self.max_request_bytes,
                busy_after: self.busy_after,
                next_ack_id: Arc::clone(&self.next_ack_id),
                peer,
            });
            let mut diag = self.diag.clone();
            let telemetry = self.telemetry.clone();
            let tls_acceptor = tls_acceptor.clone();
            let live_connections = Arc::clone(&live_connections);
            tokio::spawn(async move {
                let _permit = permit; // held for the connection's lifetime; released on drop

                let live = live_connections.fetch_add(1, Ordering::Relaxed) + 1;
                telemetry.gauge("logit.input.connections", live as f64, &[]);

                let result = match tls_acceptor {
                    Some(acceptor) => {
                        match tokio::time::timeout(handshake_timeout, acceptor.accept(stream)).await
                        {
                            Ok(Ok(tls_stream)) => {
                                serve_connection(
                                    TokioIo::new(tls_stream),
                                    shared,
                                    idle_timeout,
                                    handshake_timeout,
                                )
                                .await
                            }
                            Ok(Err(err)) => Err(format!("TLS handshake failed: {err}")),
                            Err(_elapsed) => Err(format!(
                                "TLS handshake did not complete within {handshake_timeout:?}"
                            )),
                        }
                    }
                    None => {
                        let first_byte =
                            tokio::time::timeout(handshake_timeout, stream.peek(&mut [0u8; 1]))
                                .await;
                        match first_byte {
                            Ok(Ok(0)) => Ok(()), // a health-check probe, not a fault
                            Ok(Ok(_)) => {
                                serve_connection(
                                    TokioIo::new(stream),
                                    shared,
                                    idle_timeout,
                                    handshake_timeout,
                                )
                                .await
                            }
                            Ok(Err(err)) => Err(format!("waiting for a first byte failed: {err}")),
                            Err(_elapsed) => {
                                Err(format!("no first byte received within {handshake_timeout:?}"))
                            }
                        }
                    }
                };

                let live = live_connections.fetch_sub(1, Ordering::Relaxed) - 1;
                telemetry.gauge("logit.input.connections", live as f64, &[]);

                if let Err(err) = result {
                    diag.warn_throttled("connection_error", err);
                }
            });
        }
    }
}

/// What every request on one connection needs, built once per connection.
struct Shared {
    sink: Fanout,
    telemetry: Telemetry,
    diag: Diagnostics,
    tokens: Arc<[Box<[u8]>]>,
    max_request_bytes: usize,
    busy_after: Duration,
    next_ack_id: Arc<AtomicU64>,
    /// For rejection diagnostics' message text only, never a tag.
    peer: SocketAddr,
}

/// Serves one accepted (and, with TLS on, handshaken) connection to completion: `datadog_in`'s,
/// with this listener's handler.
async fn serve_connection<IO>(
    io: IO,
    shared: Arc<Shared>,
    idle_timeout: Option<Duration>,
    grace: Duration,
) -> Result<(), String>
where
    IO: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
{
    let activity = Arc::new(Activity::new());
    let telemetry = shared.telemetry.clone();
    let svc = service_fn({
        let activity = Arc::clone(&activity);
        move |req| {
            // `enter` here, not inside the returned future: hyper calls the service as soon as a
            // request head is parsed, so the in-flight count rises then.
            let in_flight = activity.enter();
            let shared = Arc::clone(&shared);
            let activity = Arc::clone(&activity);
            async move {
                let _in_flight = in_flight;
                handle(req, &shared, &activity, idle_timeout).await
            }
        }
    });
    let builder = auto::Builder::new(TokioExecutor::new());
    let conn = builder.serve_connection(io, svc);
    drive_with_idle(
        conn,
        |conn| conn.graceful_shutdown(),
        &activity,
        idle_timeout,
        grace,
        &telemetry,
    )
    .await
}

/// One request, timed and counted: every exit from [`respond`] contributes one
/// `logit.input.requests{route, class}` count and one `logit.input.request.duration` timing.
async fn handle(
    req: http::Request<Incoming>,
    shared: &Shared,
    activity: &Activity,
    stall: Option<Duration>,
) -> Result<http::Response<Full<Bytes>>, std::convert::Infallible> {
    let started = Instant::now();
    let (route, class, response) = respond(req, shared, activity, stall).await;
    shared.telemetry.count("logit.input.requests", 1.0, &[("route", route), ("class", class)]);
    shared.telemetry.timing("logit.input.request.duration", started.elapsed(), &[]);
    Ok(response)
}

const OK: &str = "ok";
const REJECTED: &str = "rejected";
const BUSY: &str = "busy";

/// One HEC route: a row of this module's routes table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Route {
    Event,
    Raw,
    Ack,
    Health,
}

impl Route {
    fn from_path(path: &str) -> Option<Self> {
        let path = path.strip_suffix('/').unwrap_or(path);
        Some(match path {
            "/services/collector"
            | "/services/collector/event"
            | "/services/collector/event/1.0" => Self::Event,
            "/services/collector/raw" | "/services/collector/raw/1.0" => Self::Raw,
            "/services/collector/ack" => Self::Ack,
            "/services/collector/health" | "/services/collector/health/1.0" => Self::Health,
            _ => return None,
        })
    }

    /// The `route` tag on this listener's request counters.
    fn name(self) -> &'static str {
        match self {
            Self::Event => "event",
            Self::Raw => "raw",
            Self::Ack => "ack",
            Self::Health => "health",
        }
    }

    fn allows(self, method: &Method) -> bool {
        match self {
            Self::Health => method == Method::GET || method == Method::HEAD,
            _ => method == Method::POST,
        }
    }

    fn allow_header(self) -> &'static str {
        match self {
            Self::Health => "GET, HEAD",
            _ => "POST",
        }
    }
}

/// The routes table and "Request handling" steps, in order, returning the counters' `route` and
/// `class` tags alongside the response.
async fn respond(
    req: http::Request<Incoming>,
    shared: &Shared,
    activity: &Activity,
    stall: Option<Duration>,
) -> (&'static str, &'static str, http::Response<Full<Bytes>>) {
    let Some(route) = Route::from_path(req.uri().path()) else {
        let response = http_error(StatusCode::NOT_FOUND, "Not Found");
        return ("unknown", REJECTED, reject(shared, "unknown_route", None, response));
    };
    let name = route.name();
    if !route.allows(req.method()) {
        let mut response = http_error(StatusCode::METHOD_NOT_ALLOWED, "Method Not Allowed");
        response
            .headers_mut()
            .insert(http::header::ALLOW, HeaderValue::from_static(route.allow_header()));
        return (name, REJECTED, reject(shared, "method", None, response));
    }
    if route == Route::Health {
        return (name, OK, hec_response(HecStatus::HEALTHY));
    }
    let cap = shared.max_request_bytes;
    if declared_length(req.headers()).is_some_and(|len| len > cap as u64) {
        let message = format!("request body exceeds the {cap}-byte limit");
        let response = http_error(StatusCode::PAYLOAD_TOO_LARGE, "Request Entity Too Large");
        return (name, REJECTED, reject(shared, "oversize", Some(&message), response));
    }
    let query = Query::parse(req.uri().query());
    if query.token {
        let message = "a token in the query string (query-string authorization is off)";
        let response = hec_response(HecStatus::QUERY_STRING_AUTH_DISABLED);
        return (name, REJECTED, reject(shared, "query_token", Some(message), response));
    }
    if let Err(status) = authenticate(&shared.tokens, req.headers()) {
        let message = format!("HEC code {}: {}", status.code, status.text);
        return (name, REJECTED, reject(shared, "auth", Some(&message), hec_response(status)));
    }
    let encoding = match Encoding::from_headers(req.headers()) {
        Ok(encoding @ (Encoding::Identity | Encoding::Gzip)) => encoding,
        _ => {
            let message = "unsupported Content-Encoding -- this input speaks identity and gzip";
            let response = http_error(StatusCode::UNSUPPORTED_MEDIA_TYPE, "Unsupported Media Type");
            return (name, REJECTED, reject(shared, "encoding", Some(message), response));
        }
    };
    let channel = query.channel || has_channel_header(req.headers());

    let body = match collect_with_stall_bound(Limited::new(req.into_body(), cap), stall).await {
        Ok(body) => body,
        // `otlp_in`'s `408`: the client's clock, not its size, and the connection closes once
        // this response is out.
        Err(BodyReadError::Stalled(stall)) => {
            activity.request_close();
            let message = format!("request body stalled for {stall:?}");
            let response = http_error(StatusCode::REQUEST_TIMEOUT, "Request Timeout");
            return (name, REJECTED, reject(shared, "stalled", Some(&message), response));
        }
        Err(BodyReadError::Failed(err)) => {
            let reason = if is_length_limit(err.as_ref()) { "oversize" } else { "body_read" };
            let message = body_read_error_message(err.as_ref());
            let response = http_error(StatusCode::PAYLOAD_TOO_LARGE, "Request Entity Too Large");
            return (name, REJECTED, reject(shared, reason, Some(&message), response));
        }
    };
    shared.telemetry.count("logit.input.request.bytes", body.len() as f64, &[]);

    let body = match decompress(encoding, body, cap) {
        Ok(body) => body,
        Err(DecompressError::TooLarge) => {
            let message = format!("decompressed request exceeds the {cap}-byte limit");
            let response = http_error(StatusCode::PAYLOAD_TOO_LARGE, "Request Entity Too Large");
            return (name, REJECTED, reject(shared, "oversize", Some(&message), response));
        }
        Err(DecompressError::Malformed(message)) => {
            let response = hec_response(HecStatus::INVALID_DATA_FORMAT);
            return (
                name,
                REJECTED,
                reject(shared, "malformed_encoding", Some(&message), response),
            );
        }
    };
    if body.is_empty() {
        let response = hec_response(HecStatus::NO_DATA);
        return (name, REJECTED, reject(shared, "no_data", Some("an empty body"), response));
    }

    if route == Route::Ack {
        return match parse_ack_request(&body) {
            Some(ids) => {
                let acks: Vec<(u64, bool)> = ids.into_iter().map(|id| (id, true)).collect();
                (name, OK, json_response(StatusCode::OK, Bytes::from(encode_ack_reply(&acks))))
            }
            None => {
                let message = "an /ack body that isn't {\"acks\":[<id>,...]}";
                let response = hec_response(HecStatus::INVALID_DATA_FORMAT);
                (name, REJECTED, reject(shared, "malformed", Some(message), response))
            }
        };
    }

    let received_at = now_nanos();
    // Built per request: requests are served concurrently, and the handles are clones of one
    // registry and one throttled diagnostics sink, as on `datadog_in`.
    let mut decoder = SplunkDecoder::new()
        .with_telemetry(shared.telemetry.clone())
        .with_diagnostics(shared.diag.clone());
    let batches: Vec<EventBatch> = match route {
        Route::Event => match decoder.decode_events(&body, received_at) {
            Ok(batches) => batches.into_iter().filter(|batch| !batch.events.is_empty()).collect(),
            Err(err) => return (name, REJECTED, reject_hec_error(shared, &err)),
        },
        Route::Raw => {
            let batch = decoder.decode_raw(&body, &query.envelope, received_at);
            if batch.events.is_empty() {
                let response = hec_response(HecStatus::NO_DATA);
                let message = "a /raw body with no non-empty line";
                return (name, REJECTED, reject(shared, "no_data", Some(message), response));
            }
            vec![batch]
        }
        Route::Ack | Route::Health => unreachable!("answered above"),
    };

    if !batches.is_empty() {
        if let Err(not_sent) = deliver_with_deadline(&shared.sink, batches, shared.busy_after).await
        {
            shared.telemetry.count(
                "logit.input.batches.dropped",
                not_sent as f64,
                &[("reason", "busy")],
            );
            shared.diag.clone().warn_throttled(
                "busy",
                format_args!(
                    "splunk_hec_in: answered 503 to {}: the pipeline did not accept a batch \
                     within {:?}",
                    shared.peer, shared.busy_after
                ),
            );
            let mut response = hec_response(HecStatus::SERVER_BUSY);
            response.headers_mut().insert(http::header::RETRY_AFTER, HeaderValue::from_static("1"));
            return (name, BUSY, response);
        }
    }
    let ack_id = channel.then(|| shared.next_ack_id.fetch_add(1, Ordering::Relaxed));
    (name, OK, json_response(StatusCode::OK, Bytes::from(encode_success(ack_id))))
}

/// Counts one rejection under `reason` and, with a `message`, reports it through the throttled
/// `request_rejected` diagnostic. Returns `response` unchanged.
fn reject(
    shared: &Shared,
    reason: &'static str,
    message: Option<&str>,
    response: http::Response<Full<Bytes>>,
) -> http::Response<Full<Bytes>> {
    shared.telemetry.count("logit.input.requests.rejected", 1.0, &[("reason", reason)]);
    if let Some(message) = message {
        shared.diag.clone().warn_throttled(
            "request_rejected",
            format_args!("splunk_hec_in: rejecting a request from {}: {message}", shared.peer),
        );
    }
    response
}

/// The codec's whole-body rejection: code 5 is `no_data`, anything else `malformed`.
fn reject_hec_error(shared: &Shared, err: &HecError) -> http::Response<Full<Bytes>> {
    let reason = if err.status == HecStatus::NO_DATA { "no_data" } else { "malformed" };
    let response = json_response(http_status(err.status), Bytes::from(err.body()));
    reject(shared, reason, Some(&err.to_string()), response)
}

/// `{"text":…,"code":N}` under the HTTP status Splunk sends that code with.
fn hec_response(status: HecStatus) -> http::Response<Full<Bytes>> {
    json_response(http_status(status), Bytes::from(encode_status(status)))
}

fn http_status(status: HecStatus) -> StatusCode {
    StatusCode::from_u16(status.http).expect("every HecStatus carries a valid HTTP status")
}

/// An error Splunk answers without a HEC code: the body's `code` is the HTTP status.
fn http_error(status: StatusCode, text: &str) -> http::Response<Full<Bytes>> {
    json_response(status, Bytes::from(encode_http_error(status.as_u16(), text)))
}

/// Step 3 of this module's "Request handling": `Ok` when `tokens` is empty or the
/// `Authorization` header carries one of them, else the status to answer. The token is compared in
/// constant time ([`matches_any_key`]) and never leaves this function.
fn authenticate(tokens: &[Box<[u8]>], headers: &HeaderMap) -> Result<(), HecStatus> {
    if tokens.is_empty() {
        return Ok(());
    }
    let Some(value) = headers.get(http::header::AUTHORIZATION) else {
        return Err(HecStatus::TOKEN_REQUIRED);
    };
    let value = value.as_bytes().trim_ascii();
    if value.is_empty() {
        return Err(HecStatus::TOKEN_REQUIRED);
    }
    let (scheme, credentials) = match value.iter().position(|&b| b == b' ') {
        Some(at) => (&value[..at], value[at + 1..].trim_ascii()),
        None => (value, &b""[..]),
    };
    let token: Vec<u8> = if scheme.eq_ignore_ascii_case(b"splunk") {
        credentials.to_vec()
    } else if scheme.eq_ignore_ascii_case(b"basic") {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(credentials)
            .map_err(|_| HecStatus::INVALID_AUTHORIZATION)?;
        let at = decoded.iter().position(|&b| b == b':').ok_or(HecStatus::INVALID_AUTHORIZATION)?;
        decoded[at + 1..].to_vec()
    } else {
        return Err(HecStatus::INVALID_AUTHORIZATION);
    };
    if token.is_empty() {
        return Err(HecStatus::TOKEN_REQUIRED);
    }
    if matches_any_key(tokens, &token) {
        Ok(())
    } else {
        Err(HecStatus::INVALID_TOKEN)
    }
}

fn has_channel_header(headers: &HeaderMap) -> bool {
    headers.get(CHANNEL_HEADER).is_some_and(|v| !v.as_bytes().trim_ascii().is_empty())
}

/// What this listener reads from a query string: `/raw`'s envelope, whether a channel was named,
/// and whether a token was sent there. The first occurrence of each key wins.
#[derive(Debug, Default, PartialEq)]
struct Query {
    envelope: Envelope,
    channel: bool,
    token: bool,
}

impl Query {
    fn parse(query: Option<&str>) -> Self {
        let mut out = Query::default();
        let Some(query) = query else { return out };
        for (key, value) in form_urlencoded::parse(query.as_bytes()) {
            let slot = match key.as_ref() {
                "host" => &mut out.envelope.host,
                "source" => &mut out.envelope.source,
                "sourcetype" => &mut out.envelope.sourcetype,
                "index" => &mut out.envelope.index,
                "channel" => {
                    out.channel |= !value.is_empty();
                    continue;
                }
                "token" => {
                    out.token = true;
                    continue;
                }
                _ => continue,
            };
            if slot.is_none() {
                *slot = Some(value.into_owned());
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls_pki_types::pem::PemObject;
    use rustls_pki_types::CertificateDer;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::mpsc;

    const TOKEN: &str = "11111111-2222-3333-4444-555555555555";

    /// A bound, running listener with `input`'s settings, delivering into a channel of
    /// `capacity`. Returns the address and the channel's receiving end.
    async fn start(
        input: SplunkHecInput,
        capacity: usize,
    ) -> (String, mpsc::Receiver<logit_pipeline::Delivered>) {
        let mut input = input;
        input.bind().await.expect("binding an ephemeral port");
        let addr = input.local_addr().expect("bind() leaves an address").to_string();
        let (tx, rx) = mpsc::channel(capacity);
        let sink = Fanout::new(vec![tx]);
        tokio::spawn(async move { input.run(sink).await });
        (addr, rx)
    }

    async fn start_default() -> (String, mpsc::Receiver<logit_pipeline::Delivered>) {
        start(SplunkHecInput::new("127.0.0.1:0"), 16).await
    }

    async fn recv_batch(rx: &mut mpsc::Receiver<logit_pipeline::Delivered>) -> EventBatch {
        let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("a batch within 5s")
            .expect("the channel is open");
        logit_pipeline::unwrap_batch(delivered)
    }

    /// One request on a fresh connection, `Connection: close`, returning the raw response.
    async fn request_raw(
        addr: &str,
        method: &str,
        path: &str,
        headers: &str,
        body: &[u8],
    ) -> String {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\n\
             Connection: close\r\n{headers}\r\n",
            body.len()
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        // Ignored: a response sent off the head alone (a `Content-Length` over the cap, a `415`)
        // can close the connection while the body is still being written.
        let _ = stream.write_all(body).await;
        let mut buf = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut buf)).await;
        String::from_utf8_lossy(&buf).into_owned()
    }

    async fn post_raw(addr: &str, path: &str, headers: &str, body: &[u8]) -> String {
        request_raw(addr, "POST", path, headers, body).await
    }

    fn body_of(response: &str) -> &str {
        response.split_once("\r\n\r\n").map_or("", |(_, body)| body)
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut encoder, bytes).unwrap();
        encoder.finish().unwrap()
    }

    fn auth() -> String {
        format!("Authorization: Splunk {TOKEN}\r\n")
    }

    const SUCCESS: &str = r#"{"text":"Success","code":0}"#;
    const ONE_EVENT: &[u8] = br#"{"time":1700000000,"host":"h","event":"hello"}"#;

    #[tokio::test]
    async fn every_event_alias_delivers_a_batch_and_answers_success() {
        let (addr, mut rx) = start_default().await;
        for path in [
            "/services/collector",
            "/services/collector/",
            "/services/collector/event",
            "/services/collector/event/",
            "/services/collector/event/1.0",
        ] {
            let response = post_raw(&addr, path, "", ONE_EVENT).await;
            assert!(response.starts_with("HTTP/1.1 200"), "{path}: {response}");
            assert_eq!(body_of(&response), SUCCESS, "{path}");
            assert_eq!(recv_batch(&mut rx).await.events.len(), 1, "{path}");
        }
    }

    #[tokio::test]
    async fn concatenated_and_array_bodies_decode_alike() {
        let (addr, mut rx) = start_default().await;
        let concatenated = b"{\"event\":\"a\",\"time\":1}\n{\"event\":\"b\",\"time\":2}";
        let array = b"[{\"event\":\"a\",\"time\":1},{\"event\":\"b\",\"time\":2}]";
        let path = "/services/collector/event";
        post_raw(&addr, path, "", concatenated).await;
        let first = recv_batch(&mut rx).await;
        post_raw(&addr, path, "", array).await;
        let second = recv_batch(&mut rx).await;
        assert_eq!(first.events.len(), 2);
        assert_eq!(first, second);
    }

    /// Two envelopes in one body are two batches, delivered in first-appearance order.
    #[tokio::test]
    async fn a_body_with_two_envelopes_delivers_two_ordered_batches() {
        let (addr, mut rx) = start_default().await;
        let body = b"{\"host\":\"a\",\"event\":\"1\",\"time\":1}\
                     {\"host\":\"b\",\"event\":\"2\",\"time\":2}\
                     {\"host\":\"a\",\"event\":\"3\",\"time\":3}";
        let response = post_raw(&addr, "/services/collector/event", "", body).await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        let first = recv_batch(&mut rx).await;
        let second = recv_batch(&mut rx).await;
        let host = |b: &EventBatch| b.resource.attributes.get("host.name").cloned();
        assert_eq!(host(&first), Some(logit_core::Value::str("a")));
        assert_eq!(first.events.len(), 2);
        assert_eq!(host(&second), Some(logit_core::Value::str("b")));
        assert_eq!(second.events.len(), 1);
    }

    #[tokio::test]
    async fn raw_takes_its_envelope_from_the_query_and_splits_crlf_lines() {
        let (addr, mut rx) = start_default().await;
        for path in ["/services/collector/raw", "/services/collector/raw/1.0"] {
            let full =
                format!("{path}?host=web%201&source=%2Fvar%2Flog&sourcetype=app&index=main&x=y");
            let response = post_raw(&addr, &full, "", b"one\r\ntwo\r\n\r\nthree").await;
            assert!(response.starts_with("HTTP/1.1 200"), "{path}: {response}");
            assert_eq!(body_of(&response), SUCCESS);
            let batch = recv_batch(&mut rx).await;
            let attrs = &batch.resource.attributes;
            assert_eq!(attrs.get("host.name"), Some(&logit_core::Value::str("web 1")));
            assert_eq!(attrs.get("com.splunk.source"), Some(&logit_core::Value::str("/var/log")));
            assert_eq!(attrs.get("com.splunk.sourcetype"), Some(&logit_core::Value::str("app")));
            assert_eq!(attrs.get("com.splunk.index"), Some(&logit_core::Value::str("main")));
            assert_eq!(attrs.len(), 4);
            let lines: Vec<_> = batch
                .events
                .iter()
                .map(|e| e.log.as_ref().unwrap().message.as_str().unwrap().to_string())
                .collect();
            assert_eq!(lines, ["one", "two", "three"]);
        }
    }

    #[tokio::test]
    async fn the_ack_id_increments_only_for_a_request_with_a_channel() {
        let (addr, mut rx) = start_default().await;
        let path = "/services/collector/event";
        let channel = "X-Splunk-Request-Channel: 0f3c2a1e-7d4b-4c55-9a1d-3b0e8d6f2c10\r\n";
        let cases = [
            (path.to_string(), channel, r#"{"text":"Success","code":0,"ackID":1}"#),
            (path.to_string(), "", SUCCESS),
            (format!("{path}?channel=abc"), "", r#"{"text":"Success","code":0,"ackID":2}"#),
            (
                "/services/collector/raw?channel=abc".to_string(),
                "",
                r#"{"text":"Success","code":0,"ackID":3}"#,
            ),
            (path.to_string(), channel, r#"{"text":"Success","code":0,"ackID":4}"#),
        ];
        for (path, headers, expected) in cases {
            let response = post_raw(&addr, &path, headers, ONE_EVENT).await;
            assert_eq!(body_of(&response), expected, "{path} {headers:?}");
            recv_batch(&mut rx).await;
        }
    }

    #[tokio::test]
    async fn ack_answers_every_asked_id_true() {
        let (addr, _rx) = start_default().await;
        let response = post_raw(&addr, "/services/collector/ack", "", br#"{"acks":[3,1,2]}"#).await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert_eq!(body_of(&response), r#"{"acks":{"3":true,"1":true,"2":true}}"#);
        let response = post_raw(&addr, "/services/collector/ack", "", br#"{"acks":"x"}"#).await;
        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
        assert_eq!(body_of(&response), r#"{"text":"Invalid data format","code":6}"#);
    }

    /// `/health` answers on `GET` and `HEAD` with no token, even when tokens are configured.
    #[tokio::test]
    async fn health_is_unauthenticated_on_get_and_head() {
        let input = SplunkHecInput::new("127.0.0.1:0").with_tokens(vec![TOKEN.to_string()]);
        let (addr, _rx) = start(input, 16).await;
        for path in ["/services/collector/health", "/services/collector/health/1.0"] {
            let response = request_raw(&addr, "GET", path, "", b"").await;
            assert!(response.starts_with("HTTP/1.1 200"), "{path}: {response}");
            assert_eq!(body_of(&response), r#"{"text":"HEC is healthy","code":17}"#);
            let response = request_raw(&addr, "HEAD", path, "", b"").await;
            assert!(response.starts_with("HTTP/1.1 200"), "{path}: {response}");
            assert_eq!(body_of(&response), "", "a HEAD response has no body");
        }
        let response = request_raw(&addr, "POST", "/services/collector/health", "", b"").await;
        assert!(response.starts_with("HTTP/1.1 405"), "{response}");
        assert!(response.to_ascii_lowercase().contains("allow: get, head\r\n"), "{response}");
    }

    #[tokio::test]
    async fn a_gzip_body_decodes() {
        let (addr, mut rx) = start_default().await;
        let response = post_raw(
            &addr,
            "/services/collector/event",
            "Content-Encoding: gzip\r\n",
            &gzip(ONE_EVENT),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert_eq!(recv_batch(&mut rx).await.events.len(), 1);
    }

    #[tokio::test]
    async fn deflate_zstd_and_unknown_encodings_are_415() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("hec", "splunk_hec_in", "listener");
        let (addr, rx) =
            start(SplunkHecInput::new("127.0.0.1:0").with_telemetry(telemetry), 16).await;
        for encoding in ["deflate", "zstd", "br"] {
            let headers = format!("Content-Encoding: {encoding}\r\n");
            let response = post_raw(&addr, "/services/collector/event", &headers, ONE_EVENT).await;
            assert!(response.starts_with("HTTP/1.1 415"), "{encoding}: {response}");
            assert_eq!(body_of(&response), r#"{"text":"Unsupported Media Type","code":415}"#);
        }
        assert!(rx.is_empty());
        assert_eq!(
            sum_of(&registry.drain(0), "logit.input.requests.rejected", ("reason", "encoding")),
            Some(3.0)
        );
    }

    #[tokio::test]
    async fn a_malformed_gzip_stream_is_400_code_6() {
        let (addr, _rx) = start_default().await;
        let response =
            post_raw(&addr, "/services/collector/event", "Content-Encoding: gzip\r\n", b"not gzip")
                .await;
        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
        assert_eq!(body_of(&response), r#"{"text":"Invalid data format","code":6}"#);
    }

    /// The three `413` paths: a declared length over the cap, a body over it with no
    /// `Content-Length` (chunked), and a gzip body inflating past it.
    #[tokio::test]
    async fn every_oversize_path_is_413() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("hec", "splunk_hec_in", "listener");
        let input = SplunkHecInput::new("127.0.0.1:0")
            .with_telemetry(telemetry)
            .with_max_request_bytes(1024);
        let (addr, rx) = start(input, 16).await;
        let path = "/services/collector/event";
        let big = vec![b' '; 1025];

        let response = post_raw(&addr, path, "", &big).await;
        assert!(response.starts_with("HTTP/1.1 413"), "declared: {response}");
        assert_eq!(body_of(&response), r#"{"text":"Request Entity Too Large","code":413}"#);

        let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let head = format!(
            "POST {path} HTTP/1.1\r\nHost: {addr}\r\nTransfer-Encoding: chunked\r\n\
             Connection: close\r\n\r\n{:x}\r\n",
            big.len()
        );
        stream.write_all(head.as_bytes()).await.unwrap();
        let _ = stream.write_all(&big).await;
        let _ = stream.write_all(b"\r\n0\r\n\r\n").await;
        let mut buf = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut buf)).await;
        let response = String::from_utf8_lossy(&buf);
        assert!(response.starts_with("HTTP/1.1 413"), "streamed: {response}");

        let compressed = gzip(&vec![b' '; 4096]);
        assert!(compressed.len() < 1024);
        let response = post_raw(&addr, path, "Content-Encoding: gzip\r\n", &compressed).await;
        assert!(response.starts_with("HTTP/1.1 413"), "decompressed: {response}");

        assert!(rx.is_empty());
        assert_eq!(
            sum_of(&registry.drain(0), "logit.input.requests.rejected", ("reason", "oversize")),
            Some(3.0)
        );
    }

    /// A body that stops arriving is `408` once `idle_timeout` bounds each frame.
    #[tokio::test]
    async fn a_stalled_body_is_408() {
        let input =
            SplunkHecInput::new("127.0.0.1:0").with_idle_timeout(Some(Duration::from_millis(200)));
        let (addr, _rx) = start(input, 16).await;
        let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let head = format!(
            "POST /services/collector/event HTTP/1.1\r\nHost: {addr}\r\nContent-Length: 100\r\n\r\n"
        );
        stream.write_all(head.as_bytes()).await.unwrap();
        stream.write_all(b"{\"event\":").await.unwrap();
        let mut buf = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut buf)).await;
        let response = String::from_utf8_lossy(&buf);
        assert!(response.starts_with("HTTP/1.1 408"), "{response}");
        assert_eq!(body_of(&response), r#"{"text":"Request Timeout","code":408}"#);
    }

    /// A one-slot channel nothing drains: the first request fills it, the second waits
    /// `busy_after` and is answered `503` code 9 with `Retry-After`, delivering nothing.
    #[tokio::test]
    async fn a_full_downstream_is_503_code_9_with_retry_after() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("hec", "splunk_hec_in", "listener");
        let input = SplunkHecInput::new("127.0.0.1:0")
            .with_telemetry(telemetry)
            .with_busy_after(Duration::from_millis(200));
        let (addr, mut rx) = start(input, 1).await;
        let path = "/services/collector/event";
        let channel = "X-Splunk-Request-Channel: c\r\n";

        let response = post_raw(&addr, path, channel, ONE_EVENT).await;
        assert_eq!(body_of(&response), r#"{"text":"Success","code":0,"ackID":1}"#);
        let started = Instant::now();
        let response = post_raw(&addr, path, channel, ONE_EVENT).await;
        assert!(response.starts_with("HTTP/1.1 503"), "{response}");
        assert!(response.to_ascii_lowercase().contains("retry-after: 1\r\n"), "{response}");
        assert_eq!(body_of(&response), r#"{"text":"Server is busy","code":9}"#);
        assert!(started.elapsed() >= Duration::from_millis(200));

        recv_batch(&mut rx).await;
        assert!(rx.try_recv().is_err(), "the 503'd batch was never delivered");
        let response = post_raw(&addr, path, channel, ONE_EVENT).await;
        assert_eq!(
            body_of(&response),
            r#"{"text":"Success","code":0,"ackID":2}"#,
            "a 503 draws no ackID"
        );
        recv_batch(&mut rx).await;

        let events = registry.drain(0);
        assert_eq!(sum_of(&events, "logit.input.requests", ("class", "busy")), Some(1.0));
        assert_eq!(sum_of(&events, "logit.input.batches.dropped", ("reason", "busy")), Some(1.0));
    }

    #[tokio::test]
    async fn authentication_answers_splunk_s_codes() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("hec", "splunk_hec_in", "listener");
        let input = SplunkHecInput::new("127.0.0.1:0")
            .with_tokens(vec!["other".to_string(), TOKEN.to_string()])
            .with_telemetry(telemetry);
        let (addr, mut rx) = start(input, 16).await;
        let path = "/services/collector/event";
        let basic = |user_pass: &str| {
            let encoded = base64::engine::general_purpose::STANDARD.encode(user_pass);
            format!("Authorization: Basic {encoded}\r\n")
        };
        let rejected = [
            (String::new(), "401", r#"{"text":"Token is required","code":2}"#),
            (
                "Authorization: Splunk\r\n".to_string(),
                "401",
                r#"{"text":"Token is required","code":2}"#,
            ),
            (
                "Authorization: Bearer x\r\n".to_string(),
                "401",
                r#"{"text":"Invalid authorization","code":3}"#,
            ),
            (
                "Authorization: Basic !!!\r\n".to_string(),
                "401",
                r#"{"text":"Invalid authorization","code":3}"#,
            ),
            (
                "Authorization: Splunk nope\r\n".to_string(),
                "403",
                r#"{"text":"Invalid token","code":4}"#,
            ),
            (basic("x:nope"), "403", r#"{"text":"Invalid token","code":4}"#),
        ];
        for (headers, status, body) in &rejected {
            let response = post_raw(&addr, path, headers, ONE_EVENT).await;
            assert!(response.starts_with(&format!("HTTP/1.1 {status}")), "{headers:?}: {response}");
            assert_eq!(body_of(&response), *body, "{headers:?}");
        }
        for headers in
            [auth(), format!("Authorization: splunk {TOKEN}\r\n"), basic(&format!("x:{TOKEN}"))]
        {
            let response = post_raw(&addr, path, &headers, ONE_EVENT).await;
            assert!(response.starts_with("HTTP/1.1 200"), "{headers:?}: {response}");
            recv_batch(&mut rx).await;
        }
        let response = post_raw(&addr, &format!("{path}?token={TOKEN}"), &auth(), ONE_EVENT).await;
        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
        assert_eq!(
            body_of(&response),
            r#"{"text":"Query string authorization is not enabled","code":16}"#
        );

        let events = registry.drain(0);
        assert_eq!(
            sum_of(&events, "logit.input.requests.rejected", ("reason", "auth")),
            Some(rejected.len() as f64)
        );
        assert_eq!(
            sum_of(&events, "logit.input.requests.rejected", ("reason", "query_token")),
            Some(1.0)
        );
    }

    #[tokio::test]
    async fn no_tokens_accepts_any_request() {
        let (addr, mut rx) = start_default().await;
        for headers in ["", "Authorization: Bearer x\r\n", "Authorization: Splunk anything\r\n"] {
            let response = post_raw(&addr, "/services/collector/event", headers, ONE_EVENT).await;
            assert!(response.starts_with("HTTP/1.1 200"), "{headers:?}: {response}");
            recv_batch(&mut rx).await;
        }
    }

    #[tokio::test]
    async fn an_unknown_path_is_404_and_a_wrong_method_is_405_with_allow() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("hec", "splunk_hec_in", "listener");
        let (addr, _rx) =
            start(SplunkHecInput::new("127.0.0.1:0").with_telemetry(telemetry), 16).await;
        for path in ["/services/collector/s2s", "/services/collector/mint", "/"] {
            let response = post_raw(&addr, path, "", ONE_EVENT).await;
            assert!(response.starts_with("HTTP/1.1 404"), "{path}: {response}");
            assert_eq!(body_of(&response), r#"{"text":"Not Found","code":404}"#);
        }
        for path in
            ["/services/collector/event", "/services/collector/raw", "/services/collector/ack"]
        {
            let response = request_raw(&addr, "GET", path, "", b"").await;
            assert!(response.starts_with("HTTP/1.1 405"), "{path}: {response}");
            assert!(response.to_ascii_lowercase().contains("allow: post\r\n"), "{response}");
            assert_eq!(body_of(&response), r#"{"text":"Method Not Allowed","code":405}"#);
        }
        let events = registry.drain(0);
        assert_eq!(
            sum_of(&events, "logit.input.requests.rejected", ("reason", "unknown_route")),
            Some(3.0)
        );
        assert_eq!(sum_of(&events, "logit.input.requests", ("route", "unknown")), Some(3.0));
        assert_eq!(
            sum_of(&events, "logit.input.requests.rejected", ("reason", "method")),
            Some(3.0)
        );
    }

    /// A syntax error in any object rejects the whole body, naming that object's index, and
    /// delivers nothing; so does a number the model can't hold.
    #[tokio::test]
    async fn a_malformed_body_is_400_code_6_with_the_first_bad_index() {
        let (addr, rx) = start_default().await;
        let path = "/services/collector/event";
        let cases: [(&[u8], u64); 3] = [
            (b"{\"event\":\"a\"}{\"event\":\"b\"}{\"event\":", 2),
            (b"[{\"event\":\"a\"},7]", 1),
            (b"{\"event\":\"a\"}{\"event\":{\"n\":1e400}}", 1),
        ];
        for (body, index) in cases {
            let response = post_raw(&addr, path, "", body).await;
            assert!(response.starts_with("HTTP/1.1 400"), "{response}");
            assert_eq!(
                body_of(&response),
                format!(
                    r#"{{"text":"Invalid data format","code":6,"invalid-event-number":{index}}}"#
                )
            );
        }
        assert!(rx.is_empty(), "a rejected body delivers nothing");
    }

    #[tokio::test]
    async fn an_empty_body_is_400_code_5() {
        let (addr, rx) = start_default().await;
        let no_data = r#"{"text":"No data","code":5}"#;
        for (path, body) in [
            ("/services/collector/event", &b""[..]),
            ("/services/collector/event", b"  \n "),
            ("/services/collector/event", b"[]"),
            ("/services/collector/raw", b""),
            ("/services/collector/raw", b"\r\n\n"),
            ("/services/collector/ack", b""),
        ] {
            let response = post_raw(&addr, path, "", body).await;
            assert!(response.starts_with("HTTP/1.1 400"), "{path} {body:?}: {response}");
            assert_eq!(body_of(&response), no_data, "{path} {body:?}");
        }
        assert!(rx.is_empty());
    }

    /// A body whose every object the codec skips sends nothing and still answers success.
    #[tokio::test]
    async fn a_body_with_no_deliverable_event_answers_success_and_sends_nothing() {
        let (addr, rx) = start_default().await;
        let response =
            post_raw(&addr, "/services/collector/event", "", br#"{"host":"h","event":""}"#).await;
        assert_eq!(body_of(&response), SUCCESS, "{response}");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(rx.is_empty());
    }

    #[tokio::test]
    async fn requests_are_counted_by_route_and_bytes_are_summed() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("hec", "splunk_hec_in", "listener");
        let (addr, mut rx) =
            start(SplunkHecInput::new("127.0.0.1:0").with_telemetry(telemetry), 16).await;
        post_raw(&addr, "/services/collector/event", "", ONE_EVENT).await;
        recv_batch(&mut rx).await;
        request_raw(&addr, "GET", "/services/collector/health", "", b"").await;
        let events = registry.drain(0);
        assert_eq!(sum_of(&events, "logit.input.requests", ("route", "event")), Some(1.0));
        assert_eq!(sum_of(&events, "logit.input.requests", ("route", "health")), Some(1.0));
        assert_eq!(
            sum_of(&events, "logit.input.request.bytes", ("", "")),
            Some(ONE_EVENT.len() as f64)
        );
    }

    #[tokio::test]
    async fn a_connection_past_the_cap_is_dropped_and_counted() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("hec", "splunk_hec_in", "listener");
        let input =
            SplunkHecInput::new("127.0.0.1:0").with_telemetry(telemetry).with_max_connections(1);
        let (addr, _rx) = start(input, 16).await;
        let mut first = tokio::net::TcpStream::connect(&addr).await.unwrap();
        first.write_all(b"P").await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut second = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let mut buf = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(2), second.read(&mut buf))
            .await
            .expect("the past-the-cap connection is closed");
        assert!(matches!(read, Ok(0) | Err(_)), "{read:?}");
        assert_eq!(
            sum_of(&registry.drain(0), "logit.input.connections.rejected", ("reason", "limit")),
            Some(1.0)
        );
        drop(first);
    }

    #[tokio::test]
    async fn a_second_bind_is_a_no_op() {
        let mut input = SplunkHecInput::new("127.0.0.1:0");
        assert_eq!(input.local_addr(), None);
        input.bind().await.unwrap();
        let addr = input.local_addr().unwrap();
        input.bind().await.unwrap();
        assert_eq!(input.local_addr(), Some(addr));
    }

    // ---- TLS: server termination against a real `tokio-rustls` client ----------------------

    fn testdata_dir() -> std::path::PathBuf {
        // The repo root's `testdata/tls` (`testdata/tls/README.md`).
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    #[tokio::test]
    async fn an_event_over_tls_reaches_the_fanout() {
        let settings = TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: None,
        };
        let input = SplunkHecInput::new("127.0.0.1:0")
            .with_tokens(vec![TOKEN.to_string()])
            .with_tls(&settings, &testdata_dir())
            .unwrap();
        let (addr, mut rx) = start(input, 16).await;

        let mut roots = rustls::RootCertStore::empty();
        let ca: Vec<CertificateDer<'static>> =
            CertificateDer::pem_file_iter(testdata_dir().join("ca.pem"))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
        roots.add_parsable_certificates(ca);
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
        let stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let server_name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
        let mut tls = connector.connect(server_name, stream).await.unwrap();
        let request = format!(
            "POST /services/collector/event HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\n\
             Connection: close\r\n{}\r\n",
            ONE_EVENT.len(),
            auth()
        );
        tls.write_all(request.as_bytes()).await.unwrap();
        tls.write_all(ONE_EVENT).await.unwrap();
        let mut buf = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), tls.read_to_end(&mut buf)).await;
        let response = String::from_utf8_lossy(&buf);
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert_eq!(body_of(&response), SUCCESS);
        assert_eq!(recv_batch(&mut rx).await.events.len(), 1);
    }

    // ---- units --------------------------------------------------------------------------------

    #[test]
    fn defaults_match_the_documented_constants() {
        let input = SplunkHecInput::new("127.0.0.1:0");
        assert_eq!(input.busy_after, Duration::from_secs(5));
        assert_eq!(input.handshake_timeout, Duration::from_secs(5));
        assert_eq!(input.max_request_bytes, 5 * 1024 * 1024);
        assert_eq!(input.max_connections, 1024);
    }

    #[test]
    fn routes_match_their_aliases_with_one_trailing_slash() {
        assert_eq!(Route::from_path("/services/collector"), Some(Route::Event));
        assert_eq!(Route::from_path("/services/collector/event/1.0/"), Some(Route::Event));
        assert_eq!(Route::from_path("/services/collector/raw/1.0"), Some(Route::Raw));
        assert_eq!(Route::from_path("/services/collector/ack/"), Some(Route::Ack));
        assert_eq!(Route::from_path("/services/collector/health/1.0"), Some(Route::Health));
        assert_eq!(Route::from_path("/services/collector/event//"), None);
        assert_eq!(Route::from_path("/services/collector/ack/1.0"), None);
    }

    #[test]
    fn the_query_string_is_percent_decoded_and_first_wins() {
        let q = Query::parse(Some("host=a%20b&host=c&index=main&channel=&token&x=1"));
        assert_eq!(q.envelope.host.as_deref(), Some("a b"));
        assert_eq!(q.envelope.index.as_deref(), Some("main"));
        assert_eq!(q.envelope.source, None);
        assert!(!q.channel, "an empty channel names none");
        assert!(q.token, "a bare token key is still a query-string token");
        assert_eq!(Query::parse(None), Query::default());
    }

    /// The value of `metric`'s `Sum` in a drained snapshot, restricted to the point carrying
    /// `tag`; an empty tag key matches any point.
    fn sum_of(events: &[logit_core::Event], metric: &str, tag: (&str, &str)) -> Option<f64> {
        let mut total = None;
        for event in events {
            if !tag.0.is_empty()
                && event.attributes.get(tag.0).and_then(|v| v.as_str()) != Some(tag.1)
            {
                continue;
            }
            for record in &event.metrics {
                if logit_core::interner::resolve(record.name) != metric {
                    continue;
                }
                if let logit_core::MetricKind::Sum(sum) = record.kind {
                    *total.get_or_insert(0.0) += sum.value;
                }
            }
        }
        total
    }
}
