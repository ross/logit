//! `datadog_in`: a stand-in for Datadog's intake API, the receiving end of a Datadog Agent's
//! `dd_url`, `logs_config.logs_dd_url`, `apm_config.apm_dd_url`, or `additional_endpoints`
//! ([ADR `datadog-agent-and-intake-relay`](../../../../docs/adr/datadog-agent-and-intake-relay.md),
//! [`docs/plans/datadog-relay.md`](../../../../docs/plans/datadog-relay.md) §2). One TCP listener,
//! optionally TLS, serves HTTP/1.1 and h2c through [`hyper_util::server::conn::auto::Builder`],
//! and every body decodes through [`logit_proto::datadog::DatadogDecoder`]. The payload mappings
//! live in that codec's module doc; this module owns HTTP: routing, authentication, compression,
//! size caps, and backpressure.
//!
//! The accept loop, connection cap, handshake timeout, and idle timeout are `otlp_in`'s, copied
//! rather than shared because the telemetry and dispatch are each listener's own; the idle
//! machinery and bounded body read are shared ([`crate::http`]). `crate::otlp`'s module doc has the
//! reasoning for each, and it applies here unchanged.
//!
//! # Routes
//!
//! | Request | Decoder | Response |
//! |---|---|---|
//! | `POST /api/v2/series` | `decode_series_v2_protobuf` under `Content-Type: application/x-protobuf`, else `decode_series_v2_json` | `202` `{}` |
//! | `POST /api/v1/series` | `decode_series_v1` | `202` `{"status":"ok"}` |
//! | `POST /api/v1/distribution_points` | `decode_distribution_points` | `202` `{"status":"ok"}` |
//! | `POST /api/beta/sketches`, `/api/v1/sketches` | `decode_sketches` | `202`, empty |
//! | `POST /api/v1/check_run`, `/api/v2/service_checks` | `decode_service_checks` | `202` `{"status":"ok"}` |
//! | `POST /api/v2/events`, `/api/v1/events`, `/intake/` | `decode_events` | `202` `{"status":"ok"}` |
//! | `POST /api/v2/logs`, `/v1/input` | `decode_logs` | `202` `{}` |
//! | `POST /api/v0.2/traces` | `decode_agent_payload`, one batch per `TracerPayload` | `200` `{}` |
//! | `POST /api/v0.2/stats` | `decode_stats_payload`, one batch per `ClientStatsPayload` | `200` `{}` |
//! | `GET`/`POST /api/v1/validate` | none | `200` `{"valid":true}` |
//! | `POST /api/v2/host_metadata`, `/api/v1/metadata`, `/api/v1/collector`, `/api/v1/container`, `/api/v2/orch` | none: read, then discarded | `202` `{}` |
//! | another method on a path above | none | `405` + `Allow` |
//! | any other path | none | `404` |
//!
//! **An unknown path is a `404`, never an acknowledgement.** An Agent retries and reports a failed
//! route, so a route this listener doesn't speak shows up on the Agent's side as an error, not as
//! data that silently vanished. The acknowledged routes above are the ones whose payloads are
//! deliberately not relayed (host and inventory metadata, the process and orchestrator
//! collectors): each is counted `logit.input.requests.acknowledged{route}` so it stays visible
//! here.
//!
//! **`/intake/` carries events and host metadata on one route.** A body that is a JSON object
//! with no `events` member and no event title or text is host metadata: it is acknowledged and
//! counted like the routes above rather than decoded, so a metadata flush doesn't count as a
//! skipped event in the codec's telemetry. A body that decodes to no events for another reason is
//! answered the same way, without a send.
//!
//! # Request handling
//!
//! In order, after the connection-level steps `otlp_in` also takes:
//!
//! 1. **Route and method**, as the table above.
//! 2. **Size.** A `Content-Length` over [`MAX_REQUEST_BYTES`] is a `413` before any byte is read,
//!    and the body is read through [`Limited`] at the same cap in case the header lied or was
//!    absent. The cap is on the compressed body, on every route.
//! 3. **Authentication.** With `api_keys` configured, the request's `DD-API-KEY` header (any case;
//!    an Agent sends `DD-Api-Key`) must equal one entry, else `403`
//!    `{"status":"error","code":403,"errors":["Forbidden"]}`. The comparison takes the same time
//!    for every key of one length, and no key is ever logged or counted. With no `api_keys` every
//!    request passes, and `/api/v1/validate` answers `200` to any key.
//! 4. **`Content-Encoding`.** `identity` (or none), `gzip`, `deflate` (zlib-wrapped: what the
//!    Agent's `zlib` compressor kind sends under that name), or `zstd` (the Agent's default), else
//!    `415`. The decompressed size is capped at [`MAX_DECOMPRESSED_BYTES`], or
//!    [`MAX_TRACES_DECOMPRESSED_BYTES`] for traces, by `otlp_in`'s pattern: read through
//!    `Read::take(cap + 1)`, and `413` when that last byte arrives. A stream that doesn't decode is a
//!    `400`. zstd has its own bounds ([`crate::zstd`]).
//! 5. **Decode.** `CodecError::Malformed` is a `400` `{"status":"error","errors":[..]}`; the codec
//!    has already counted any items it dropped while the rest decoded.
//! 6. **Delivery**, bounded (below), then the route's `2xx`.
//!
//! A body that stops arriving mid-upload gets `408` and the connection closes, when `idle_timeout`
//! is set: the per-frame stall bound `otlp_in` derives from it.
//!
//! # Backpressure: a bounded wait, then `503`
//!
//! `otlp_in` and `prometheus_in` let a full downstream block the handler, so the client's request
//! stays open until the pipeline drains. That suits a client that waits patiently. A Datadog Agent
//! doesn't: its forwarder times a request out after 20 seconds and retries it, so a blocked
//! connection costs the Agent a slot for 20 seconds and still ends in a retry.
//!
//! So ([ADR `datadog-agent-and-intake-relay`](../../../../docs/adr/datadog-agent-and-intake-relay.md),
//! decision 5) each batch is sent under one deadline per request, [`BUSY_AFTER`] from the start of
//! delivery, through [`Fanout::send_with_deadline`]: a batch reaches every downstream consumer or
//! none. When the deadline passes, the request is answered `503` with `Retry-After: 1` and
//! `{"status":"error","errors":["busy"]}`, counted `logit.input.requests{class="busy"}`, and the
//! batches not yet delivered are counted `logit.input.batches.dropped{reason="busy"}`, disjoint
//! from `logit.component.batches.sent`. The Agent retries a `503` with backoff, which makes the
//! pair at-least-once end to end:
//!
//! - **The timed-out batch is delivered to no consumer.** `send_with_deadline` reserves room on
//!   every consumer's channel before sending to any of them, and releases what it holds if the
//!   deadline passes first, so the Agent's retry is the only copy, whatever the fan-out.
//! - **A request that decodes to several batches** (traces and stats: one per tracer or client
//!   payload) is sent in order and answered `503` as a whole if the deadline passes partway. The
//!   batches fully delivered before the deadline are delivered again by the retry. This matches
//!   what Datadog's own intake does with a resent request: a resent series point overwrites the
//!   one it repeats, and a resent log or span duplicates.
//!
//! A `503` is also cheaper for the Agent than the block it replaces: it releases the connection at
//! once, and the Agent's retry queue, not this listener, holds the payload while the pipeline
//! catches up.
//!
//! # Telemetry
//!
//! Every name and tag is `&'static`. Per request: `logit.input.requests{route, class}` (class `ok`,
//! `rejected`, or `busy`; route one of [`Route::name`], or `unknown`), `logit.input.request.duration`
//! (timing, every exit), and `logit.input.request.bytes` (the compressed body size, once read).
//! Rejections: `logit.input.requests.rejected{reason}`, reason `unknown_route`, `method`,
//! `oversize`, `auth`, `encoding`, `malformed_encoding`, `malformed`, `stalled`, or `body_read` (a
//! body that failed for a reason other than its size, such as a client disconnecting mid-upload).
//! `logit.input.requests.acknowledged{route}` counts a payload answered without a send, and
//! `logit.input.batches.dropped{reason="busy"}` the batches a `503` left undelivered. The
//! connection metrics are `otlp_in`'s verbatim. `docs/design/internal-telemetry.md`'s `datadog_in`
//! section is the operator-facing account.

use crate::http::{
    body_read_error_message, collect_with_stall_bound, drive_with_idle, is_length_limit, Activity,
    BodyReadError,
};
use crate::zstd::{self, ZstdError};
use crate::Input;
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use http_body_util::{Full, Limited};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use logit_core::{Diagnostics, EventBatch, Telemetry};
use logit_pipeline::Fanout;
use logit_proto::datadog::DatadogDecoder;
use logit_proto::CodecError;
use std::io::Read;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

/// The cap on a request's compressed body, on every route: 5 MiB, the Agent's own decompressed
/// payload limit for series and sketches, so no conforming compressed body comes near it. A
/// denial-of-service bound, not a tuning knob, as on `otlp_in`.
const MAX_REQUEST_BYTES: usize = 5 * 1024 * 1024;

/// The cap on a decompressed body, on every route but traces: 5,242,880 bytes, the Agent's
/// serializer limit for a series or sketch payload, and above its 1 MB log batch.
const MAX_DECOMPRESSED_BYTES: usize = 5_242_880;

/// The traces route's decompressed cap. The trace agent caps a payload at 3.2 MB uncompressed, and
/// this leaves room for a sender that allows more without letting a compression bomb through.
const MAX_TRACES_DECOMPRESSED_BYTES: usize = 16 * 1024 * 1024;

/// Bounds the connections [`Input::run`] serves at once: the same 1024 as `otlp_in`, `logit_in`,
/// and `crate::tcp`'s listeners. A connection past the cap is rejected, not queued.
const MAX_CONCURRENT_CONNECTIONS: usize = 1024;

/// Default for [`DatadogInput::with_handshake_timeout`]: the same 5s as every other TCP listener,
/// mirrored by hand in `logit_config::default_handshake_timeout`. Also the grace an idle close
/// gives hyper.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// How long one request's delivery may wait on a full downstream before it is answered `503`
/// (this module's "Backpressure" section). A quarter of the Agent forwarder's 20-second request
/// timeout, so the Agent hears "busy" well before it would give up on its own.
const BUSY_AFTER: Duration = Duration::from_secs(5);

/// `crate::tls::TlsServerSettings`, re-exported as `otlp_in`'s is.
pub use crate::tls::TlsServerSettings;

/// The `datadog_in` listener. See this module's doc.
pub struct DatadogInput {
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
    /// Empty accepts any key (this module's "Request handling", step 3).
    api_keys: Arc<[Box<[u8]>]>,
    max_connections: usize,
    busy_after: Duration,
}

impl DatadogInput {
    pub fn new(bind: impl Into<String>) -> Self {
        Self {
            bind: bind.into(),
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
            tls: None,
            listener: None,
            handshake_timeout: HANDSHAKE_TIMEOUT,
            idle_timeout: None,
            api_keys: Arc::from(Vec::new()),
            max_connections: MAX_CONCURRENT_CONNECTIONS,
            busy_after: BUSY_AFTER,
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

    /// The `DD-API-KEY` values this listener accepts (`api_keys:` in config). Empty accepts any
    /// request. Graph rule 63 rejects an empty entry.
    pub fn with_api_keys(mut self, api_keys: Vec<String>) -> Self {
        self.api_keys =
            api_keys.into_iter().map(|key| key.into_bytes().into_boxed_slice()).collect();
        self
    }

    /// Test-only override of [`MAX_CONCURRENT_CONNECTIONS`].
    #[cfg(test)]
    fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Overrides [`BUSY_AFTER`]. Not a config field -- the 5s default suits every real
    /// deployment, and shortening the Agent's own busy-retry budget isn't something an operator
    /// should reach for -- but a test/tuning hook: a round-trip test builds a listener with a
    /// short bound so its busy case doesn't wait out five real seconds.
    pub fn with_busy_after(mut self, d: Duration) -> Self {
        self.busy_after = d;
        self
    }
}

#[async_trait::async_trait]
impl Input for DatadogInput {
    async fn bind(&mut self) -> anyhow::Result<()> {
        if self.listener.is_some() {
            return Ok(()); // idempotent, per `Input::bind`'s contract
        }
        let listener = TcpListener::bind(&self.bind).await?;
        self.diag.info("bound", format_args!("listening on {}", self.bind));
        self.listener = Some(listener);
        Ok(())
    }

    /// `otlp_in`'s accept loop: a permit per connection, taken before any TLS accept and rejected
    /// rather than queued at the cap; the handshake, or a plaintext first-byte peek, bounded inside
    /// the spawned task; a clean close before the first byte treated as a health check.
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
                api_keys: Arc::clone(&self.api_keys),
                busy_after: self.busy_after,
                peer,
            });
            let mut diag = self.diag.clone();
            let telemetry = self.telemetry.clone();
            let tls_acceptor = tls_acceptor.clone();
            let live_connections = Arc::clone(&live_connections);
            tokio::spawn(async move {
                let _permit = permit; // held for the connection's lifetime; released on drop

                // From the read-modify-write's return value, as `otlp_in` publishes it.
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
    api_keys: Arc<[Box<[u8]>]>,
    busy_after: Duration,
    /// For rejection diagnostics' message text only, never a tag.
    peer: SocketAddr,
}

/// Serves one accepted (and, with TLS on, handshaken) connection to completion: `otlp_in`'s HTTP
/// arm, with this listener's handler.
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

/// One Datadog intake route: a row of this module's routes table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Route {
    SeriesV2,
    SeriesV1,
    DistributionPoints,
    Sketches,
    ServiceChecks,
    Events,
    Intake,
    Logs,
    Traces,
    Stats,
    Validate,
    /// Answered `202` and discarded; the name is the `route` tag.
    Acknowledged(&'static str),
}

impl Route {
    fn from_path(path: &str) -> Option<Self> {
        Some(match path {
            "/api/v2/series" => Self::SeriesV2,
            "/api/v1/series" => Self::SeriesV1,
            "/api/v1/distribution_points" => Self::DistributionPoints,
            "/api/beta/sketches" | "/api/v1/sketches" => Self::Sketches,
            "/api/v1/check_run" | "/api/v2/service_checks" => Self::ServiceChecks,
            "/api/v2/events" | "/api/v1/events" => Self::Events,
            "/intake/" => Self::Intake,
            "/api/v2/logs" | "/v1/input" => Self::Logs,
            "/api/v0.2/traces" => Self::Traces,
            "/api/v0.2/stats" => Self::Stats,
            "/api/v1/validate" => Self::Validate,
            "/api/v2/host_metadata" => Self::Acknowledged("host_metadata"),
            "/api/v1/metadata" => Self::Acknowledged("metadata"),
            "/api/v1/collector" => Self::Acknowledged("collector"),
            "/api/v1/container" => Self::Acknowledged("container"),
            "/api/v2/orch" => Self::Acknowledged("orch"),
            _ => return None,
        })
    }

    /// The `route` tag on this listener's request counters.
    fn name(self) -> &'static str {
        match self {
            Self::SeriesV2 => "series_v2",
            Self::SeriesV1 => "series_v1",
            Self::DistributionPoints => "distribution_points",
            Self::Sketches => "sketches",
            Self::ServiceChecks => "service_checks",
            Self::Events => "events",
            Self::Intake => "intake",
            Self::Logs => "logs",
            Self::Traces => "traces",
            Self::Stats => "stats",
            Self::Validate => "validate",
            Self::Acknowledged(name) => name,
        }
    }

    fn allows(self, method: &Method) -> bool {
        method == Method::POST || (self == Self::Validate && method == Method::GET)
    }

    fn allow_header(self) -> &'static str {
        if self == Self::Validate {
            "GET, POST"
        } else {
            "POST"
        }
    }

    fn decompressed_cap(self) -> usize {
        if self == Self::Traces {
            MAX_TRACES_DECOMPRESSED_BYTES
        } else {
            MAX_DECOMPRESSED_BYTES
        }
    }

    /// The route's success status and body, as the routes table lists them.
    fn success(self) -> http::Response<Full<Bytes>> {
        let (status, body): (StatusCode, &'static [u8]) = match self {
            Self::SeriesV2 | Self::Logs | Self::Acknowledged(_) => (StatusCode::ACCEPTED, b"{}"),
            Self::SeriesV1
            | Self::DistributionPoints
            | Self::ServiceChecks
            | Self::Events
            | Self::Intake => (StatusCode::ACCEPTED, br#"{"status":"ok"}"#),
            Self::Sketches => (StatusCode::ACCEPTED, b""),
            Self::Traces | Self::Stats => (StatusCode::OK, b"{}"),
            Self::Validate => (StatusCode::OK, br#"{"valid":true}"#),
        };
        json_response(status, Bytes::from_static(body))
    }
}

/// A request's declared `Content-Encoding` (this module's "Request handling", step 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Encoding {
    Identity,
    Gzip,
    Deflate,
    Zstd,
}

impl Encoding {
    /// Matched case-insensitively, since HTTP content codings are. `Err` carries what was sent,
    /// for the `415` message.
    fn from_headers(headers: &HeaderMap) -> Result<Self, String> {
        let Some(value) = headers.get(http::header::CONTENT_ENCODING) else {
            return Ok(Self::Identity);
        };
        let value = value.to_str().unwrap_or("").trim();
        if value.is_empty() || value.eq_ignore_ascii_case("identity") {
            Ok(Self::Identity)
        } else if value.eq_ignore_ascii_case("gzip") {
            Ok(Self::Gzip)
        } else if value.eq_ignore_ascii_case("deflate") {
            Ok(Self::Deflate)
        } else if value.eq_ignore_ascii_case("zstd") {
            Ok(Self::Zstd)
        } else {
            Err(value.to_string())
        }
    }
}

/// Why [`decompress`] failed: `413` versus `400`, as `otlp_in` distinguishes them.
enum DecompressError {
    TooLarge,
    Malformed(String),
}

/// Decompresses `body` under `encoding`, bounded to `cap` bytes of output.
fn decompress(encoding: Encoding, body: Bytes, cap: usize) -> Result<Bytes, DecompressError> {
    match encoding {
        // Already capped at `MAX_REQUEST_BYTES`, which no decompressed cap is below.
        Encoding::Identity => Ok(body),
        Encoding::Gzip => bounded_read(flate2::read::GzDecoder::new(&body[..]), cap, "gzip"),
        Encoding::Deflate => {
            bounded_read(flate2::read::ZlibDecoder::new(&body[..]), cap, "deflate")
        }
        Encoding::Zstd => match zstd::decompress(&body, cap) {
            Ok(out) => Ok(Bytes::from(out)),
            Err(ZstdError::TooLarge) => Err(DecompressError::TooLarge),
            Err(ZstdError::Malformed(message)) => Err(DecompressError::Malformed(message)),
        },
    }
}

/// `otlp_in`'s `inflate`: `Read::take` allows one byte past `cap`, so an input inflating to
/// `cap + 1` is caught rather than truncated to fit.
fn bounded_read(reader: impl Read, cap: usize, name: &str) -> Result<Bytes, DecompressError> {
    let mut out = Vec::new();
    reader
        .take(cap as u64 + 1)
        .read_to_end(&mut out)
        .map_err(|err| DecompressError::Malformed(format!("invalid {name} body: {err}")))?;
    if out.len() > cap {
        return Err(DecompressError::TooLarge);
    }
    Ok(Bytes::from(out))
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
        let response = reject(shared, "unknown_route", StatusCode::NOT_FOUND, "Not found", false);
        return ("unknown", REJECTED, response);
    };
    let name = route.name();
    if !route.allows(req.method()) {
        let mut response =
            reject(shared, "method", StatusCode::METHOD_NOT_ALLOWED, "Method not allowed", false);
        response
            .headers_mut()
            .insert(http::header::ALLOW, HeaderValue::from_static(route.allow_header()));
        return (name, REJECTED, response);
    }
    if declared_length(req.headers()).is_some_and(|len| len > MAX_REQUEST_BYTES as u64) {
        let message = format!("request body exceeds the {MAX_REQUEST_BYTES}-byte limit");
        let response = reject(shared, "oversize", StatusCode::PAYLOAD_TOO_LARGE, &message, true);
        return (name, REJECTED, response);
    }
    if !authorized(&shared.api_keys, req.headers()) {
        shared.telemetry.count("logit.input.requests.rejected", 1.0, &[("reason", "auth")]);
        shared.diag.clone().warn_throttled(
            "request_rejected",
            format_args!(
                "datadog_in: rejecting a request from {}: bad or missing DD-API-KEY",
                shared.peer
            ),
        );
        let body = br#"{"status":"error","code":403,"errors":["Forbidden"]}"#;
        return (name, REJECTED, json_response(StatusCode::FORBIDDEN, Bytes::from_static(body)));
    }
    if route == Route::Validate {
        return (name, OK, route.success());
    }
    let encoding = match Encoding::from_headers(req.headers()) {
        Ok(encoding) => encoding,
        Err(sent) => {
            let message = format!(
                "unsupported Content-Encoding {sent:?} -- this input speaks identity, gzip, \
                 deflate, and zstd"
            );
            let response =
                reject(shared, "encoding", StatusCode::UNSUPPORTED_MEDIA_TYPE, &message, true);
            return (name, REJECTED, response);
        }
    };
    let protobuf = route == Route::SeriesV2 && is_protobuf(req.headers());

    let body =
        match collect_with_stall_bound(Limited::new(req.into_body(), MAX_REQUEST_BYTES), stall)
            .await
        {
            Ok(body) => body,
            // `otlp_in`'s `408`: the client's clock, not its size, and the connection closes once
            // this response is out.
            Err(BodyReadError::Stalled(stall)) => {
                activity.request_close();
                let message = format!("request body stalled for {stall:?}");
                let response =
                    reject(shared, "stalled", StatusCode::REQUEST_TIMEOUT, &message, true);
                return (name, REJECTED, response);
            }
            Err(BodyReadError::Failed(err)) => {
                let reason = if is_length_limit(err.as_ref()) { "oversize" } else { "body_read" };
                let message = body_read_error_message(err.as_ref());
                let response =
                    reject(shared, reason, StatusCode::PAYLOAD_TOO_LARGE, &message, true);
                return (name, REJECTED, response);
            }
        };
    shared.telemetry.count("logit.input.request.bytes", body.len() as f64, &[]);

    if let Route::Acknowledged(_) = route {
        acknowledge(shared, name);
        return (name, OK, route.success());
    }

    let body = match decompress(encoding, body, route.decompressed_cap()) {
        Ok(body) => body,
        Err(DecompressError::TooLarge) => {
            let message =
                format!("decompressed request exceeds the {}-byte limit", route.decompressed_cap());
            let response =
                reject(shared, "oversize", StatusCode::PAYLOAD_TOO_LARGE, &message, true);
            return (name, REJECTED, response);
        }
        Err(DecompressError::Malformed(message)) => {
            let response =
                reject(shared, "malformed_encoding", StatusCode::BAD_REQUEST, &message, true);
            return (name, REJECTED, response);
        }
    };

    if route == Route::Intake && is_host_metadata(&body) {
        acknowledge(shared, name);
        return (name, OK, route.success());
    }

    let received_at = now_nanos();
    // Built per request: requests are served concurrently, and the handles are clones of one
    // registry and one throttled diagnostics sink, as in `prometheus_in`.
    let mut decoder = DatadogDecoder::new()
        .with_telemetry(shared.telemetry.clone())
        .with_diagnostics(shared.diag.clone());
    let decoded: Result<Vec<EventBatch>, CodecError> = match route {
        Route::SeriesV2 if protobuf => {
            decoder.decode_series_v2_protobuf(&body, received_at).map(|b| vec![b])
        }
        Route::SeriesV2 => decoder.decode_series_v2_json(&body, received_at).map(|b| vec![b]),
        Route::SeriesV1 => decoder.decode_series_v1(&body, received_at).map(|b| vec![b]),
        Route::DistributionPoints => {
            decoder.decode_distribution_points(&body, received_at).map(|b| vec![b])
        }
        Route::Sketches => decoder.decode_sketches(&body, received_at).map(|b| vec![b]),
        Route::ServiceChecks => decoder.decode_service_checks(&body, received_at).map(|b| vec![b]),
        Route::Events | Route::Intake => decoder.decode_events(&body, received_at).map(|b| vec![b]),
        Route::Logs => decoder.decode_logs(&body, received_at).map(|b| vec![b]),
        Route::Traces => decoder.decode_agent_payload(&body, received_at),
        Route::Stats => decoder.decode_stats_payload(&body, received_at),
        Route::Validate | Route::Acknowledged(_) => unreachable!("answered above"),
    };
    let batches: Vec<EventBatch> = match decoded {
        Ok(batches) => batches.into_iter().filter(|batch| !batch.events.is_empty()).collect(),
        Err(err) => {
            let response =
                reject(shared, "malformed", StatusCode::BAD_REQUEST, &err.to_string(), true);
            return (name, REJECTED, response);
        }
    };
    if batches.is_empty() {
        if route == Route::Intake {
            acknowledge(shared, name);
        }
        return (name, OK, route.success());
    }

    match deliver(&shared.sink, batches, shared.busy_after).await {
        Ok(()) => (name, OK, route.success()),
        Err(not_sent) => {
            shared.telemetry.count(
                "logit.input.batches.dropped",
                not_sent as f64,
                &[("reason", "busy")],
            );
            shared.diag.clone().warn_throttled(
                "busy",
                format_args!(
                    "datadog_in: answered 503 to {}: the pipeline did not accept a batch within \
                     {:?}",
                    shared.peer, shared.busy_after
                ),
            );
            let mut response = error_response(StatusCode::SERVICE_UNAVAILABLE, "busy");
            response.headers_mut().insert(http::header::RETRY_AFTER, HeaderValue::from_static("1"));
            (name, BUSY, response)
        }
    }
}

/// Sends `batches` in order under one deadline, `busy_after` from now (this module's
/// "Backpressure" section). Each batch reaches every consumer or none
/// ([`Fanout::send_with_deadline`]). `Err` carries how many were not delivered, the timed-out one
/// included.
async fn deliver(
    sink: &Fanout,
    batches: Vec<EventBatch>,
    busy_after: Duration,
) -> Result<(), usize> {
    let deadline = tokio::time::Instant::now() + busy_after;
    let total = batches.len();
    for (sent, batch) in batches.into_iter().enumerate() {
        if sink.send_with_deadline(batch, deadline).await.is_err() {
            return Err(total - sent);
        }
    }
    Ok(())
}

/// Counts one rejection under `reason`, optionally reports it through the throttled
/// `request_rejected` diagnostic, and builds its `{"status":"error"}` response.
fn reject(
    shared: &Shared,
    reason: &'static str,
    status: StatusCode,
    message: &str,
    diagnose: bool,
) -> http::Response<Full<Bytes>> {
    shared.telemetry.count("logit.input.requests.rejected", 1.0, &[("reason", reason)]);
    if diagnose {
        shared.diag.clone().warn_throttled(
            "request_rejected",
            format_args!("datadog_in: rejecting a request from {}: {message}", shared.peer),
        );
    }
    error_response(status, message)
}

fn acknowledge(shared: &Shared, route: &'static str) {
    shared.telemetry.count("logit.input.requests.acknowledged", 1.0, &[("route", route)]);
}

/// Whether the request carries a `DD-API-KEY` equal to one of `api_keys`; always `true` when none
/// are configured. Every configured key is compared, and each comparison runs over the whole key,
/// so the time taken doesn't reveal how much of a guess matched.
fn authorized(api_keys: &[Box<[u8]>], headers: &HeaderMap) -> bool {
    if api_keys.is_empty() {
        return true;
    }
    let Some(sent) = headers.get("dd-api-key") else {
        return false;
    };
    let sent = sent.as_bytes();
    api_keys.iter().fold(false, |matched, key| matched | constant_time_eq(key, sent))
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// `Content-Length`, when present and a number. An unparseable one is left for hyper, which
/// rejects it before the handler runs.
fn declared_length(headers: &HeaderMap) -> Option<u64> {
    headers.get(http::header::CONTENT_LENGTH)?.to_str().ok()?.trim().parse().ok()
}

/// `Content-Type: application/x-protobuf` (or `application/protobuf`), matched case-insensitively
/// and ignoring parameters: the Agent's v2 series protobuf. Anything else, or none, is JSON.
fn is_protobuf(headers: &HeaderMap) -> bool {
    let Some(value) = headers.get(http::header::CONTENT_TYPE) else {
        return false;
    };
    let media = value.to_str().unwrap_or("").split(';').next().unwrap_or("").trim();
    media.eq_ignore_ascii_case("application/x-protobuf")
        || media.eq_ignore_ascii_case("application/protobuf")
}

/// The fields an `/intake/` body is probed for (this module's "`/intake/` carries events and
/// host metadata"). `IgnoredAny` skips each value without building it.
#[derive(serde::Deserialize)]
struct IntakeProbe {
    events: Option<serde::de::IgnoredAny>,
    title: Option<serde::de::IgnoredAny>,
    msg_title: Option<serde::de::IgnoredAny>,
    text: Option<serde::de::IgnoredAny>,
    msg_text: Option<serde::de::IgnoredAny>,
}

/// A JSON object with none of an event's fields: an Agent host-metadata payload. Anything that
/// isn't a JSON object is left for `decode_events` to reject.
fn is_host_metadata(body: &[u8]) -> bool {
    match serde_json::from_slice::<IntakeProbe>(body) {
        Ok(probe) => {
            probe.events.is_none()
                && probe.title.is_none()
                && probe.msg_title.is_none()
                && probe.text.is_none()
                && probe.msg_text.is_none()
        }
        Err(_) => false,
    }
}

fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn json_response(status: StatusCode, body: Bytes) -> http::Response<Full<Bytes>> {
    http::Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(Full::new(body))
        .expect("a well-formed response always builds")
}

/// Datadog's error shape: `{"status":"error","errors":[message]}`.
fn error_response(status: StatusCode, message: &str) -> http::Response<Full<Bytes>> {
    // Formatted rather than built as a `serde_json::Value`, whose map would sort `errors` first.
    let message = serde_json::to_string(message).expect("a string always serializes");
    json_response(status, Bytes::from(format!(r#"{{"status":"error","errors":[{message}]}}"#)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::mpsc;

    const KEY: &str = "0123456789abcdef0123456789abcdef";

    /// A bound, running listener with `input`'s settings, delivering into a channel of
    /// `capacity`. Returns the address and the channel's receiving end.
    async fn start(
        input: DatadogInput,
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
        start(DatadogInput::new("127.0.0.1:0"), 16).await
    }

    async fn recv_batch(rx: &mut mpsc::Receiver<logit_pipeline::Delivered>) -> EventBatch {
        let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("a batch within 5s")
            .expect("the channel is open");
        logit_pipeline::unwrap_batch(delivered)
    }

    /// `otlp_in`'s `post_raw`, with the method as a parameter: one request on a fresh
    /// connection, `Connection: close`, returning the raw response.
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

    fn zlib(bytes: &[u8]) -> Vec<u8> {
        let mut encoder =
            flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut encoder, bytes).unwrap();
        encoder.finish().unwrap()
    }

    fn zstd_frame(bytes: &[u8]) -> Vec<u8> {
        ruzstd::encoding::compress_to_vec(bytes, ruzstd::encoding::CompressionLevel::Fastest)
    }

    const SERIES_V1: &[u8] =
        br#"{"series":[{"metric":"a.b","points":[[1700000000,1.5]],"type":"gauge","host":"h"}]}"#;
    const SERIES_V2_JSON: &[u8] = br#"{"series":[{"metric":"a.b","type":3,"points":[{"timestamp":1700000000,"value":1.5}]}]}"#;
    const DISTRIBUTION: &[u8] = br#"{"series":[{"metric":"d","points":[[1700000000,[1.0,2.0]]]}]}"#;
    const CHECKS: &[u8] =
        br#"[{"check":"app.ok","status":0,"host_name":"h","timestamp":1700000000}]"#;
    const EVENT: &[u8] = br#"{"title":"deploy","text":"v2 is out"}"#;
    const LOGS: &[u8] = br#"[{"message":"hello","service":"web"}]"#;
    const HOST_METADATA: &[u8] =
        br#"{"internalHostname":"h","agentVersion":"7.60.0","meta":{"hostname":"h"}}"#;

    /// Every decoding route's success status and body, with a body that decodes to one batch.
    #[tokio::test]
    async fn every_json_route_answers_its_documented_success_and_delivers_a_batch() {
        let (addr, mut rx) = start_default().await;
        let cases: &[(&str, &[u8], &str, &str)] = &[
            ("/api/v1/series", SERIES_V1, "HTTP/1.1 202", r#"{"status":"ok"}"#),
            ("/api/v2/series", SERIES_V2_JSON, "HTTP/1.1 202", "{}"),
            ("/api/v1/distribution_points", DISTRIBUTION, "HTTP/1.1 202", r#"{"status":"ok"}"#),
            ("/api/v1/check_run", CHECKS, "HTTP/1.1 202", r#"{"status":"ok"}"#),
            ("/api/v2/service_checks", CHECKS, "HTTP/1.1 202", r#"{"status":"ok"}"#),
            ("/api/v1/events", EVENT, "HTTP/1.1 202", r#"{"status":"ok"}"#),
            ("/api/v2/events", EVENT, "HTTP/1.1 202", r#"{"status":"ok"}"#),
            ("/intake/", EVENT, "HTTP/1.1 202", r#"{"status":"ok"}"#),
            ("/api/v2/logs", LOGS, "HTTP/1.1 202", "{}"),
            ("/v1/input", LOGS, "HTTP/1.1 202", "{}"),
        ];
        for (path, body, status, expected) in cases {
            let response = post_raw(&addr, path, "Content-Type: application/json\r\n", body).await;
            assert!(response.starts_with(status), "{path}: {response}");
            assert_eq!(body_of(&response), *expected, "{path}");
            let batch = recv_batch(&mut rx).await;
            assert_eq!(batch.events.len(), 1, "{path}");
        }
    }

    /// The protobuf and msgpack routes, with bodies that decode to nothing: an empty protobuf
    /// message is every field's default, and an empty msgpack map a `StatsPayload` with no
    /// payloads. Each answers its success and sends nothing; the integration test sends real ones.
    #[tokio::test]
    async fn the_binary_routes_answer_their_success_for_an_empty_payload() {
        let (addr, mut rx) = start_default().await;
        let cases: &[(&str, &[u8], &str, &str)] = &[
            ("/api/beta/sketches", b"", "HTTP/1.1 202", ""),
            ("/api/v1/sketches", b"", "HTTP/1.1 202", ""),
            ("/api/v0.2/traces", b"", "HTTP/1.1 200", "{}"),
            ("/api/v0.2/stats", &[0x80], "HTTP/1.1 200", "{}"),
        ];
        for (path, body, status, expected) in cases {
            let response =
                post_raw(&addr, path, "Content-Type: application/x-protobuf\r\n", body).await;
            assert!(response.starts_with(status), "{path}: {response}");
            assert_eq!(body_of(&response), *expected, "{path}");
        }
        assert!(rx.try_recv().is_err(), "an empty payload sends nothing");
    }

    #[tokio::test]
    async fn the_acknowledged_routes_answer_202_and_count_without_sending() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("dd", "datadog_in", "listener");
        let (addr, mut rx) =
            start(DatadogInput::new("127.0.0.1:0").with_telemetry(telemetry), 16).await;
        for path in [
            "/api/v2/host_metadata",
            "/api/v1/metadata",
            "/api/v1/collector",
            "/api/v1/container",
            "/api/v2/orch",
        ] {
            let response = post_raw(&addr, path, "", b"{\"anything\":1}").await;
            assert!(response.starts_with("HTTP/1.1 202"), "{path}: {response}");
            assert_eq!(body_of(&response), "{}", "{path}");
        }
        let response = post_raw(&addr, "/intake/", "", HOST_METADATA).await;
        assert!(response.starts_with("HTTP/1.1 202"), "{response}");
        assert!(rx.try_recv().is_err(), "an acknowledged payload is never sent");

        let events = registry.drain(0);
        for route in ["host_metadata", "metadata", "collector", "container", "orch", "intake"] {
            assert_eq!(
                sum_of(&events, "logit.input.requests.acknowledged", ("route", route)),
                Some(1.0),
                "{route}"
            );
        }
        assert_eq!(
            sum_of(&events, "logit.input.events.skipped", ("reason", "no_title")),
            None,
            "host metadata is not a skipped event"
        );
    }

    #[tokio::test]
    async fn validate_answers_200_with_no_keys_configured_for_either_method() {
        let (addr, _rx) = start_default().await;
        for method in ["GET", "POST"] {
            let response = request_raw(&addr, method, "/api/v1/validate", "", b"").await;
            assert!(response.starts_with("HTTP/1.1 200"), "{method}: {response}");
            assert_eq!(body_of(&response), r#"{"valid":true}"#);
        }
    }

    #[tokio::test]
    async fn validate_checks_the_key_when_keys_are_configured() {
        let input = DatadogInput::new("127.0.0.1:0").with_api_keys(vec![KEY.to_string()]);
        let (addr, _rx) = start(input, 16).await;
        let good = format!("DD-Api-Key: {KEY}\r\n");
        let response = request_raw(&addr, "GET", "/api/v1/validate", &good, b"").await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        let response =
            request_raw(&addr, "GET", "/api/v1/validate", "DD-API-KEY: nope\r\n", b"").await;
        assert!(response.starts_with("HTTP/1.1 403"), "{response}");
    }

    #[tokio::test]
    async fn a_missing_or_wrong_key_is_403_forbidden_and_a_right_one_in_any_case_passes() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("dd", "datadog_in", "listener");
        let input = DatadogInput::new("127.0.0.1:0")
            .with_api_keys(vec!["other-key".to_string(), KEY.to_string()])
            .with_telemetry(telemetry);
        let (addr, mut rx) = start(input, 16).await;

        for headers in ["", "DD-API-KEY: wrong\r\n", "DD-API-KEY: 0123456789abcdef\r\n"] {
            let response = post_raw(&addr, "/api/v1/series", headers, SERIES_V1).await;
            assert!(response.starts_with("HTTP/1.1 403"), "{headers:?}: {response}");
            assert_eq!(
                body_of(&response),
                r#"{"status":"error","code":403,"errors":["Forbidden"]}"#
            );
        }
        for name in ["DD-API-KEY", "dd-api-key", "DD-Api-Key"] {
            let headers = format!("{name}: {KEY}\r\n");
            let response = post_raw(&addr, "/api/v1/series", &headers, SERIES_V1).await;
            assert!(response.starts_with("HTTP/1.1 202"), "{name}: {response}");
            recv_batch(&mut rx).await;
        }
        let events = registry.drain(0);
        assert_eq!(sum_of(&events, "logit.input.requests.rejected", ("reason", "auth")), Some(3.0));
    }

    #[tokio::test]
    async fn a_wrong_method_on_a_known_path_is_405_with_allow() {
        let (addr, _rx) = start_default().await;
        let response = request_raw(&addr, "GET", "/api/v1/series", "", b"").await;
        assert!(response.starts_with("HTTP/1.1 405"), "{response}");
        assert!(response.to_ascii_lowercase().contains("allow: post\r\n"), "{response}");
        let response = request_raw(&addr, "PUT", "/api/v1/validate", "", b"").await;
        assert!(response.starts_with("HTTP/1.1 405"), "{response}");
        assert!(response.to_ascii_lowercase().contains("allow: get, post\r\n"), "{response}");
    }

    #[tokio::test]
    async fn an_unknown_path_is_404_and_counted() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("dd", "datadog_in", "listener");
        let (addr, _rx) =
            start(DatadogInput::new("127.0.0.1:0").with_telemetry(telemetry), 16).await;
        let response = post_raw(&addr, "/api/intake/metrics/v3/series", "", b"{}").await;
        assert!(response.starts_with("HTTP/1.1 404"), "{response}");
        let events = registry.drain(0);
        assert_eq!(
            sum_of(&events, "logit.input.requests.rejected", ("reason", "unknown_route")),
            Some(1.0)
        );
        assert_eq!(sum_of(&events, "logit.input.requests", ("route", "unknown")), Some(1.0));
    }

    #[tokio::test]
    async fn every_supported_encoding_decodes() {
        let (addr, mut rx) = start_default().await;
        let cases: [(&str, Vec<u8>); 5] = [
            ("identity", SERIES_V1.to_vec()),
            ("gzip", gzip(SERIES_V1)),
            ("deflate", zlib(SERIES_V1)),
            ("zstd", zstd_frame(SERIES_V1)),
            ("ZSTD", zstd_frame(SERIES_V1)),
        ];
        for (encoding, body) in cases {
            let headers = format!("Content-Encoding: {encoding}\r\n");
            let response = post_raw(&addr, "/api/v1/series", &headers, &body).await;
            assert!(response.starts_with("HTTP/1.1 202"), "{encoding}: {response}");
            assert_eq!(recv_batch(&mut rx).await.events.len(), 1, "{encoding}");
        }
    }

    #[tokio::test]
    async fn an_unsupported_encoding_is_415() {
        let (addr, _rx) = start_default().await;
        let response =
            post_raw(&addr, "/api/v1/series", "Content-Encoding: br\r\n", SERIES_V1).await;
        assert!(response.starts_with("HTTP/1.1 415"), "{response}");
        assert!(body_of(&response).starts_with(r#"{"status":"error","errors":["#), "{response}");
    }

    #[tokio::test]
    async fn a_malformed_compressed_stream_is_400() {
        let (addr, _rx) = start_default().await;
        for encoding in ["gzip", "deflate", "zstd"] {
            let headers = format!("Content-Encoding: {encoding}\r\n");
            let response = post_raw(&addr, "/api/v1/series", &headers, b"not compressed").await;
            assert!(response.starts_with("HTTP/1.1 400"), "{encoding}: {response}");
        }
    }

    #[tokio::test]
    async fn a_malformed_payload_is_400_with_datadog_s_error_shape() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("dd", "datadog_in", "listener");
        let (addr, _rx) =
            start(DatadogInput::new("127.0.0.1:0").with_telemetry(telemetry), 16).await;
        let response = post_raw(&addr, "/api/v1/series", "", b"[not json").await;
        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
        let body: serde_json::Value = serde_json::from_str(body_of(&response)).unwrap();
        assert_eq!(body["status"], "error");
        assert!(body["errors"][0].as_str().is_some_and(|m| !m.is_empty()), "{body}");
        assert_eq!(
            sum_of(&registry.drain(0), "logit.input.requests.rejected", ("reason", "malformed")),
            Some(1.0)
        );
    }

    #[tokio::test]
    async fn a_compressed_body_over_the_request_cap_is_413() {
        let (addr, _rx) = start_default().await;
        let body = vec![b' '; MAX_REQUEST_BYTES + 1];
        let response = post_raw(&addr, "/api/v1/series", "", &body).await;
        assert!(response.starts_with("HTTP/1.1 413"), "{response}");
    }

    /// Each encoding's decompressed cap, isolated from the compressed one: a few KiB inflating
    /// past 5 MiB on the series route. The traces route's larger cap accepts the same body.
    #[tokio::test]
    async fn a_body_decompressing_past_the_cap_is_413_on_every_encoding() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("dd", "datadog_in", "listener");
        let (addr, _rx) =
            start(DatadogInput::new("127.0.0.1:0").with_telemetry(telemetry), 16).await;
        let zeros = vec![b' '; MAX_DECOMPRESSED_BYTES + 1];
        for (encoding, body) in
            [("gzip", gzip(&zeros)), ("deflate", zlib(&zeros)), ("zstd", zstd_frame(&zeros))]
        {
            assert!(body.len() < MAX_REQUEST_BYTES, "{encoding}: the compressed body fits");
            let headers = format!("Content-Encoding: {encoding}\r\n");
            let response = post_raw(&addr, "/api/v1/series", &headers, &body).await;
            assert!(response.starts_with("HTTP/1.1 413"), "{encoding}: {response}");
        }
        assert_eq!(
            sum_of(&registry.drain(0), "logit.input.requests.rejected", ("reason", "oversize")),
            Some(3.0)
        );

        // 5 MiB + 1 of spaces isn't an `AgentPayload`, so the traces route's answer is a decode
        // failure: the size check passed.
        let response =
            post_raw(&addr, "/api/v0.2/traces", "Content-Encoding: gzip\r\n", &gzip(&zeros)).await;
        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    }

    #[tokio::test]
    async fn a_series_v2_protobuf_content_type_selects_the_protobuf_decoder() {
        let (addr, _rx) = start_default().await;
        // Valid JSON, invalid protobuf: under the protobuf content type it must not decode.
        let response = post_raw(
            &addr,
            "/api/v2/series",
            "Content-Type: application/x-protobuf\r\n",
            SERIES_V2_JSON,
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    }

    /// A one-slot channel nothing drains: the first request's batch fills it, the second's send
    /// waits `busy_after` and is answered `503` with `Retry-After`, delivering nothing. Once the
    /// channel drains, a third request succeeds.
    #[tokio::test]
    async fn a_full_downstream_is_answered_503_after_the_busy_bound() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("dd", "datadog_in", "listener");
        let input = DatadogInput::new("127.0.0.1:0")
            .with_telemetry(telemetry)
            .with_busy_after(Duration::from_millis(200));
        let (addr, mut rx) = start(input, 1).await;

        let response = post_raw(&addr, "/api/v1/series", "", SERIES_V1).await;
        assert!(response.starts_with("HTTP/1.1 202"), "{response}");
        let started = Instant::now();
        let response = post_raw(&addr, "/api/v1/series", "", SERIES_V1).await;
        assert!(response.starts_with("HTTP/1.1 503"), "{response}");
        assert!(response.to_ascii_lowercase().contains("retry-after: 1\r\n"), "{response}");
        assert_eq!(body_of(&response), r#"{"status":"error","errors":["busy"]}"#);
        assert!(started.elapsed() >= Duration::from_millis(200));

        recv_batch(&mut rx).await;
        assert!(rx.try_recv().is_err(), "the 503'd batch was never delivered");
        let response = post_raw(&addr, "/api/v1/series", "", SERIES_V1).await;
        assert!(response.starts_with("HTTP/1.1 202"), "{response}");
        recv_batch(&mut rx).await;

        let events = registry.drain(0);
        assert_eq!(sum_of(&events, "logit.input.requests", ("class", "busy")), Some(1.0));
        assert_eq!(sum_of(&events, "logit.input.batches.dropped", ("reason", "busy")), Some(1.0));
    }

    /// With two consumers and the second full, a `503` leaves the first holding nothing, and the
    /// timed-out batch counts as busy, never as sent (the module doc's "Backpressure" section).
    #[tokio::test]
    async fn a_503_with_one_of_two_consumers_full_delivers_to_neither_and_counts_busy_not_sent() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("dd", "datadog_in", "listener");
        let mut input = DatadogInput::new("127.0.0.1:0")
            .with_telemetry(telemetry.clone())
            .with_busy_after(Duration::from_millis(200));
        input.bind().await.expect("binding an ephemeral port");
        let addr = input.local_addr().expect("bind() leaves an address").to_string();
        let (tx_a, mut rx_a) = mpsc::channel(1);
        let (tx_b, mut rx_b) = mpsc::channel(1);
        let sink = Fanout::new(vec![tx_a, tx_b]).with_telemetry(telemetry);
        tokio::spawn(async move { input.run(sink).await });

        let response = post_raw(&addr, "/api/v1/series", "", SERIES_V1).await;
        assert!(response.starts_with("HTTP/1.1 202"), "{response}");
        recv_batch(&mut rx_a).await; // `a` drains; `b` stays full

        let response = post_raw(&addr, "/api/v1/series", "", SERIES_V1).await;
        assert!(response.starts_with("HTTP/1.1 503"), "{response}");
        assert!(rx_a.try_recv().is_err(), "a must not hold a batch the Agent will resend");
        recv_batch(&mut rx_b).await;
        assert!(rx_b.try_recv().is_err(), "b never had room for the 503'd batch");

        let events = registry.drain(0);
        assert_eq!(sum_of(&events, "logit.component.batches.sent", ("", "")), Some(1.0));
        assert_eq!(sum_of(&events, "logit.input.batches.dropped", ("reason", "busy")), Some(1.0));
    }

    #[tokio::test]
    async fn requests_are_counted_by_route_and_class_and_bytes_are_summed() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("dd", "datadog_in", "listener");
        let (addr, mut rx) =
            start(DatadogInput::new("127.0.0.1:0").with_telemetry(telemetry), 16).await;
        post_raw(&addr, "/api/v2/logs", "", LOGS).await;
        recv_batch(&mut rx).await;
        let events = registry.drain(0);
        assert_eq!(sum_of(&events, "logit.input.requests", ("route", "logs")), Some(1.0));
        assert_eq!(sum_of(&events, "logit.input.request.bytes", ("", "")), Some(LOGS.len() as f64));
    }

    #[tokio::test]
    async fn a_connection_past_the_cap_is_dropped_and_counted() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("dd", "datadog_in", "listener");
        let input =
            DatadogInput::new("127.0.0.1:0").with_telemetry(telemetry).with_max_connections(1);
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
        let mut input = DatadogInput::new("127.0.0.1:0");
        assert_eq!(input.local_addr(), None);
        input.bind().await.unwrap();
        let addr = input.local_addr().unwrap();
        input.bind().await.unwrap();
        assert_eq!(input.local_addr(), Some(addr));
    }

    /// [`DatadogInput::with_busy_after`] is a test/tuning hook, not a config field: a fresh
    /// listener keeps the real 5s default with nothing overriding it. No waiting -- this checks
    /// the constant and the builder's starting value, not the deadline itself.
    #[test]
    fn default_busy_after_is_five_seconds() {
        assert_eq!(BUSY_AFTER, Duration::from_secs(5));
        assert_eq!(DatadogInput::new("127.0.0.1:0").busy_after, BUSY_AFTER);
    }

    #[test]
    fn constant_time_eq_compares_whole_keys() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(authorized(&[], &HeaderMap::new()), "no keys accepts anything");
    }

    #[test]
    fn host_metadata_is_told_apart_from_an_event() {
        assert!(is_host_metadata(HOST_METADATA));
        assert!(!is_host_metadata(EVENT));
        assert!(!is_host_metadata(br#"{"apiKey":"k","events":{},"internalHostname":"h"}"#));
        assert!(!is_host_metadata(b"[1]"), "not an object: left for the decoder to reject");
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
