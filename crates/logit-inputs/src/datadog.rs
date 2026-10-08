//! `datadog_in`: a stand-in for Datadog's intake API, the receiving end of a Datadog Agent's
//! `dd_url`, `logs_config.logs_dd_url`, `apm_config.apm_dd_url`, or `additional_endpoints`
//! ([ADR `datadog-agent-and-intake-relay`](../../../../docs/adr/datadog-agent-and-intake-relay.md),
//! [`docs/plans/datadog-relay.md`](../../../../docs/plans/datadog-relay.md) §2). One TCP listener,
//! optionally TLS, serves HTTP/1.1 and h2c through [`hyper_util::server::conn::auto::Builder`],
//! and every body decodes through [`logit_proto::datadog::DatadogDecoder`]. The payload mappings
//! live in that codec's module doc; this module owns HTTP: routing, authentication, compression,
//! size caps, and backpressure.
//!
//! The accept loop, connection cap, handshake timeout, idle timeout, and shutdown close are
//! `otlp_in`'s, copied rather than shared because the telemetry and dispatch are each listener's
//! own; the connection driver and bounded body read are shared ([`crate::http`]). `crate::otlp`'s
//! module doc has the reasoning for each, and it applies here unchanged. The request helpers (`Content-Encoding`,
//! bounded decompression, the JSON response shapes, deadline-bounded delivery) live in
//! [`crate::http`] too, shared with `datadog_trace_in` ([`crate::datadog_trace`]).
//!
//! **Sender address.** Under `peer:` and `proxy_protocol:` ([`DatadogInput::with_peer`],
//! [`DatadogInput::with_proxy_protocol`]), each connection task builds one [`ConnectionPeer`], and
//! `respond` stamps it on every batch a request decodes into, before delivery. The PROXY header is
//! read right after the permit, before the TLS accept or the first-byte peek, with `otlp_in`'s
//! rejection, counting, and health-check rules (`crate::otlp`'s "Sender address"). Under
//! `forwarded:` ([`DatadogInput::with_forwarded`]), each request's forwarding header replaces the
//! PROXY origin's `client.*` for that request, per ADR `forwarded-header-parsing`.
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
//! | `GET`/`POST /api/v1/validate`, `/api/v2/validate` | none | `200` `{"valid":true}` |
//! | `GET /_health` | none | `200` `{}` |
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
//!    `{"status":"error","code":403,"errors":["Forbidden"]}`. On the validate and `/_health`
//!    routes an `api_key` query parameter counts too: an Agent's own key check is
//!    `GET /api/v1/validate?api_key=<key>` with no header at all
//!    (`testdata/interop/datadog/agent-api-v1-validate-000.headers`). The comparison takes the
//!    same time for every key of one length, and no key is ever logged or counted. With no
//!    `api_keys` every request passes, and the validate routes answer `200` to any key.
//! 4. **`Content-Encoding`.** `identity` (or none), `gzip`, `deflate` (zlib-wrapped: what the
//!    Agent's `zlib` compressor kind sends under that name), or `zstd` (the Agent's default), else
//!    `415`. A header that is present but empty, or not ASCII, is a `415` too, not identity. The decompressed size is capped at [`MAX_DECOMPRESSED_BYTES`], or
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
//! A batch no consumer takes, because every consumer of the listener has closed, gets the same
//! `503` and `Retry-After: 1` with `closed_consumer` in place of `busy`: in the body, in
//! `logit.input.requests{class}`, and in `logit.input.batches.dropped{reason}`, which counts that
//! batch and every later one of the request. The refused batch was offered, so unlike a timed-out
//! one it also counts in `logit.component.batches.sent`.
//!
//! # Telemetry
//!
//! Every name and tag is `&'static`. Per request: `logit.input.requests{route, class}` (class `ok`,
//! `rejected`, `busy`, or `closed_consumer`; route one of [`Route::name`], or `unknown`),
//! `logit.input.request.duration` (timing, every exit), and `logit.input.request.bytes` (the
//! compressed body size, once read). Rejections: `logit.input.requests.rejected{reason}`, reason
//! `unknown_route`, `method`, `oversize`, `auth`, `encoding`, `malformed_encoding`, `malformed`,
//! `stalled`, or `body_read` (a body that failed for a reason other than its size, such as a
//! client disconnecting mid-upload). `logit.input.requests.acknowledged{route}` counts a payload
//! answered without a send, and `logit.input.batches.dropped{reason}` (`busy` or
//! `closed_consumer`) the batches a `503` left undelivered. The
//! connection metrics are `otlp_in`'s verbatim. `docs/design/internal-telemetry.md`'s `datadog_in`
//! section is the operator-facing account.

use crate::http::{
    body_read_error_message, collect_with_stall_bound, declared_length, decompress,
    deliver_with_deadline, drive_connection, error_response, is_length_limit, json_response,
    matches_any_key, media_type, now_nanos, Activity, BodyReadError, DecompressError, Encoding,
    MediaType, Undelivered,
};
use crate::listener::Prelude;
use crate::peer::ConnectionPeer;
use crate::Input;
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use http_body_util::{Full, Limited};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use logit_core::{Diagnostics, EventBatch, Telemetry};
use logit_pipeline::listen::BindOptions;
use logit_pipeline::Fanout;
use logit_proto::datadog::DatadogDecoder;
use logit_proto::forwarded::ForwardedHeader;
use logit_proto::CodecError;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::watch;
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

/// Default for [`DatadogInput::with_handshake_timeout`]: the same 5s as every other TCP listener,
/// mirrored by hand in `logit_config::default_handshake_timeout`. Also the grace an idle close
/// gives hyper.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// How long one request's delivery may wait on a full downstream before it is answered `503`
/// (this module's "Backpressure" section). A quarter of the Agent forwarder's 20-second request
/// timeout, so the Agent hears "busy" well before it would give up on its own.
const BUSY_AFTER: Duration = Duration::from_secs(5);

/// `logit_pipeline::tls::TlsServerSettings`, re-exported as `otlp_in`'s is.
pub use logit_pipeline::tls::TlsServerSettings;

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
    /// Bounds the connections [`Input::run`] serves at once; [`crate::DEFAULT_MAX_CONNECTIONS`]
    /// unless [`Self::with_max_connections`] sets it. A connection past the cap is rejected, not
    /// queued. With a 5 MiB body inflating to 16 MiB on the traces route, this listener's worst
    /// case at the default cap (`max_connections:`, 1024) is 4.1 TiB, a bound rather than a memory
    /// budget ([`crate::http::MAX_CONCURRENT_STREAMS`] has the formula).
    max_connections: usize,
    busy_after: Duration,
    /// `peer:` in config. See [`Self::with_peer`].
    peer: bool,
    /// Socket options set before the bind (`reuse_port:`).
    bind_options: BindOptions,
    /// `proxy_protocol:` in config. See [`Self::with_proxy_protocol`].
    proxy_protocol: bool,
    /// `forwarded:` in config. See [`Self::with_forwarded`].
    forwarded: Option<ForwardedHeader>,
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
            max_connections: crate::DEFAULT_MAX_CONNECTIONS,
            busy_after: BUSY_AFTER,
            peer: false,
            bind_options: BindOptions::default(),
            proxy_protocol: false,
            forwarded: None,
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
    ///
    /// Registers the files with `reloader` under this listener's diagnostics and telemetry as
    /// they are when this runs, so call it after `with_diagnostics` and `with_telemetry`.
    pub fn with_tls(
        mut self,
        settings: &TlsServerSettings,
        base_dir: &Path,
        reloader: &logit_pipeline::tls::TlsReloader,
    ) -> anyhow::Result<Self> {
        let alpn: &[&[u8]] = &[b"h2", b"http/1.1"];
        self.tls = Some(Arc::new(logit_pipeline::tls::build_server_config(
            settings,
            base_dir,
            alpn,
            reloader,
            &self.diag,
            &self.telemetry,
        )?));
        Ok(self)
    }

    /// Overrides [`HANDSHAKE_TIMEOUT`] for the PROXY header, the TLS accept, and the plaintext
    /// first-byte peek, each on its own budget (`handshake_timeout:` in config). Graph rule 45
    /// rejects `0s`.
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

    /// Overrides [`crate::DEFAULT_MAX_CONNECTIONS`]; `max_connections:` in config. Graph rule 74
    /// rejects `0` before it gets here.
    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
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

    /// Stamps every event a request decodes with its connection's socket peer (`peer:` in
    /// config), per [`crate::peer`]. Off by default.
    pub fn with_peer(mut self, peer: bool) -> Self {
        self.peer = peer;
        self
    }

    /// Sets `SO_REUSEPORT` before the bind (`reuse_port:` in config), so another process can bind
    /// the same address at the same time. Off by default.
    pub fn with_reuse_port(mut self, reuse_port: bool) -> Self {
        self.bind_options.reuse_port = reuse_port;
        self
    }

    /// Requires a PROXY protocol header ahead of every connection and stamps the origin it names
    /// (`proxy_protocol:` in config). Off by default. See this module's "Sender address".
    pub fn with_proxy_protocol(mut self, proxy_protocol: bool) -> Self {
        self.proxy_protocol = proxy_protocol;
        self
    }

    /// Reads the client from `header` on every request (`forwarded:` in config), per
    /// [`ConnectionPeer::request`]. Off by default.
    pub fn with_forwarded(mut self, header: Option<ForwardedHeader>) -> Self {
        self.forwarded = header;
        self
    }
}

#[async_trait::async_trait]
impl Input for DatadogInput {
    async fn bind(&mut self) -> anyhow::Result<()> {
        if self.listener.is_some() {
            return Ok(()); // idempotent, per `Input::bind`'s contract
        }
        let listener = logit_pipeline::listen::bind_tcp(&self.bind, self.bind_options).await?;
        self.diag.info("bound", format_args!("listening on {}", self.bind));
        self.listener = Some(listener);
        Ok(())
    }

    /// `otlp_in`'s accept loop: a permit per connection, taken before any TLS accept and rejected
    /// rather than queued at the cap; under `proxy_protocol:` the PROXY header, then the
    /// handshake or a plaintext first-byte peek, each bounded inside the spawned task; a clean
    /// close or a reset before the first byte treated as a health check.
    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        // A never-firing `watch`, so `run` and `run_until_shutdown` share one implementation. The
        // sender lives for this scope: a dropped sender reads as shutdown.
        let (_tx, rx) = watch::channel(false);
        self.run_until_shutdown(sink, rx).await
    }

    /// `otlp_in`'s shutdown close: the accept races the signal, the listening socket closes on
    /// it, and the connection tasks drain (`crate::otlp`'s "Idle timeout").
    async fn run_until_shutdown(
        &mut self,
        sink: Fanout,
        mut shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        self.bind().await?;
        let listener = self.listener.take().expect("bind() leaves a listener behind");
        let connection_limit = Arc::new(tokio::sync::Semaphore::new(self.max_connections));
        let tls_acceptor = self.tls.clone().map(TlsAcceptor::from);
        let live_connections = crate::listener::LiveConnections::new(self.telemetry.clone());
        let handshake_timeout = self.handshake_timeout;
        let idle_timeout = self.idle_timeout;
        let mut accept_queue =
            crate::tcp::AcceptQueueSampler::new(self.telemetry.clone(), self.diag.clone());
        let mut accept_diag = self.diag.clone();
        // A local of this future, so `run_input`'s backstop dropping it aborts every connection
        // still open (`crate::listener::ConnectionTasks`).
        let mut tasks = crate::listener::ConnectionTasks::new();
        loop {
            // Both waits race shutdown: see `docs/design/pipeline-graph.md`'s "Cancellation
            // points".
            let accepted = tokio::select! {
                accepted = accept_queue.accept(&listener) => accepted,
                _ = shutdown.wait_for(|&due| due) => break,
            };
            let (mut stream, peer) = match accepted {
                Ok(accepted) => accepted,
                Err(err) => {
                    tokio::select! {
                        biased;
                        absorbed = crate::listener::absorb_accept_error(
                            err,
                            &self.telemetry,
                            &mut accept_diag,
                        ) => absorbed?,
                        _ = shutdown.wait_for(|&due| due) => break,
                    }
                    continue;
                }
            };

            let Ok(permit) = connection_limit.clone().try_acquire_owned() else {
                self.telemetry.count(
                    "logit.input.connections.rejected",
                    1.0,
                    &[("reason", "limit")],
                );
                drop(stream);
                continue;
            };

            let (sink, telemetry) = (sink.clone(), self.telemetry.clone());
            let api_keys = Arc::clone(&self.api_keys);
            let busy_after = self.busy_after;
            let mut diag = self.diag.clone();
            let tls_acceptor = tls_acceptor.clone();
            let live_connections = live_connections.clone();
            let (record_peer, proxy_protocol) = (self.peer, self.proxy_protocol);
            let forwarded = self.forwarded;
            let mut conn_shutdown = shutdown.clone();
            tasks.spawn(async move {
                let _permit = permit; // held for the connection's lifetime; released on drop

                // Counted out on drop, so a panicking handler brings the gauge back down too.
                let live = live_connections.enter();

                // Races shutdown as a whole (`docs/design/pipeline-graph.md`'s "Cancellation
                // points"): nothing of a request has been read before it ends.
                let prelude = async {
                    // Ahead of the TLS accept and the first-byte peek, both of which would
                    // otherwise read the header's bytes as the request's (`crate::otlp`'s "Sender
                    // address").
                    let origin = if proxy_protocol {
                        match crate::peer::read_proxy_origin(&mut stream, handshake_timeout).await {
                            Ok(origin) => Some(origin),
                            Err(err) => return Prelude::ProxyRejected(err),
                        }
                    } else {
                        None
                    };
                    let connection_peer = ConnectionPeer::tcp(peer, record_peer, origin.as_ref())
                        .with_forwarded(forwarded, &diag);

                    match tls_acceptor {
                        Some(acceptor) => {
                            match tokio::time::timeout(handshake_timeout, acceptor.accept(stream))
                                .await
                            {
                                Ok(Ok(tls_stream)) => {
                                    Prelude::Tls(Box::new(tls_stream), connection_peer)
                                }
                                Ok(Err(err)) => {
                                    Prelude::Failed(format!("TLS handshake failed: {err}"))
                                }
                                Err(_elapsed) => Prelude::Failed(format!(
                                    "TLS handshake did not complete within {handshake_timeout:?}"
                                )),
                            }
                        }
                        None => {
                            let first_byte =
                                tokio::time::timeout(handshake_timeout, stream.peek(&mut [0u8; 1]))
                                    .await;
                            match first_byte {
                                // A clean close or a reset before the first byte is a
                                // health-check probe, not a fault (`crate::otlp`'s "not a fault").
                                Ok(Ok(0)) => Prelude::Probe,
                                Ok(Err(err))
                                    if err.kind() == std::io::ErrorKind::ConnectionReset =>
                                {
                                    Prelude::Probe
                                }
                                Ok(Ok(_)) => Prelude::Plain(stream, connection_peer),
                                Ok(Err(err)) => Prelude::Failed(format!(
                                    "waiting for a first byte failed: {err}"
                                )),
                                Err(_elapsed) => Prelude::Failed(format!(
                                    "no first byte received within {handshake_timeout:?}"
                                )),
                            }
                        }
                    }
                };
                let prelude = tokio::select! {
                    prelude = prelude => prelude,
                    _ = conn_shutdown.wait_for(|&due| due) => return,
                };

                let shared = |peer| {
                    Arc::new(Shared {
                        sink,
                        telemetry: telemetry.clone(),
                        diag: diag.clone(),
                        api_keys,
                        busy_after,
                        peer,
                    })
                };
                let result = match prelude {
                    Prelude::Tls(tls_stream, connection_peer) => {
                        serve_connection(
                            TokioIo::new(*tls_stream),
                            shared(connection_peer),
                            idle_timeout,
                            handshake_timeout,
                            conn_shutdown,
                        )
                        .await
                    }
                    Prelude::Plain(stream, connection_peer) => {
                        serve_connection(
                            TokioIo::new(stream),
                            shared(connection_peer),
                            idle_timeout,
                            handshake_timeout,
                            conn_shutdown,
                        )
                        .await
                    }
                    Prelude::Probe => Ok(()),
                    Prelude::Failed(err) => Err(err),
                    Prelude::ProxyRejected(err) => {
                        drop(live);
                        telemetry.count(
                            "logit.input.connections.rejected",
                            1.0,
                            &[("reason", "proxy_header")],
                        );
                        diag.warn_throttled("proxy_header", err);
                        return;
                    }
                };

                if let Err(err) = result {
                    diag.warn_throttled("connection_error", err);
                }
            });
        }

        // Closed before the drain: under `reuse_port` the kernel keeps hashing new connections to
        // a bound socket that nothing accepts on any more.
        drop(listener);
        tasks.drain().await;
        Ok(())
    }
}

/// What every request on one connection needs, built once per connection.
struct Shared {
    sink: Fanout,
    telemetry: Telemetry,
    diag: Diagnostics,
    api_keys: Arc<[Box<[u8]>]>,
    busy_after: Duration,
    /// Names the sender in diagnostic text, and stamps every batch under `peer:` or
    /// `proxy_protocol:`.
    peer: ConnectionPeer,
}

/// Serves one accepted (and, with TLS on, handshaken) connection to completion: `otlp_in`'s HTTP
/// arm, with this listener's handler.
async fn serve_connection<IO>(
    io: IO,
    shared: Arc<Shared>,
    idle_timeout: Option<Duration>,
    grace: Duration,
    shutdown: watch::Receiver<bool>,
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
    let builder = crate::http::auto_builder();
    let conn = builder.serve_connection(io, svc);
    drive_connection(
        conn,
        |conn| conn.graceful_shutdown(),
        &activity,
        idle_timeout,
        grace,
        shutdown,
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
    Health,
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
            "/api/v1/validate" | "/api/v2/validate" => Self::Validate,
            "/_health" => Self::Health,
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
            Self::Health => "health",
            Self::Acknowledged(name) => name,
        }
    }

    fn allows(self, method: &Method) -> bool {
        match self {
            Self::Validate => method == Method::GET || method == Method::POST,
            Self::Health => method == Method::GET,
            _ => method == Method::POST,
        }
    }

    fn allow_header(self) -> &'static str {
        match self {
            Self::Validate => "GET, POST",
            Self::Health => "GET",
            _ => "POST",
        }
    }

    /// Whether the route answers without a body to decode: the key check is the whole request.
    fn is_probe(self) -> bool {
        matches!(self, Self::Validate | Self::Health)
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
            Self::Health => (StatusCode::OK, b"{}"),
        };
        json_response(status, Bytes::from_static(body))
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
    // An Agent's key check sends its key only in the query string (this module's
    // "Authentication"), so a probe route also reads `?api_key=`.
    let query_key = if route.is_probe() { query_api_key(req.uri().query()) } else { None };
    if !authorized(&shared.api_keys, req.headers(), query_key) {
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
    if route.is_probe() {
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
    let protobuf = route == Route::SeriesV2 && media_type(req.headers()) == MediaType::Protobuf;
    let request_peer = shared.peer.request(req.headers());

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
        Route::Validate | Route::Health | Route::Acknowledged(_) => {
            unreachable!("answered above")
        }
    };
    let mut batches: Vec<EventBatch> = match decoded {
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
    request_peer.stamp_batches(&mut batches);

    match deliver_with_deadline(&shared.sink, batches, shared.busy_after).await {
        Ok(()) => (name, OK, route.success()),
        Err(undelivered) => {
            let reason = undelivered.reason();
            shared.telemetry.count(
                "logit.input.batches.dropped",
                undelivered.count() as f64,
                &[("reason", reason)],
            );
            let mut diag = shared.diag.clone();
            let _ = match undelivered {
                Undelivered::Busy(_) => diag.warn_throttled(
                    "busy",
                    format_args!(
                        "datadog_in: answered 503 to {}: the pipeline did not accept a batch \
                         within {:?}",
                        shared.peer, shared.busy_after
                    ),
                ),
                Undelivered::Closed(_) => diag.warn_throttled(
                    "closed_consumer",
                    format_args!(
                        "datadog_in: answered 503 to {}: no consumer took the batch",
                        shared.peer
                    ),
                ),
            };
            let mut response = error_response(StatusCode::SERVICE_UNAVAILABLE, reason);
            response.headers_mut().insert(http::header::RETRY_AFTER, HeaderValue::from_static("1"));
            (name, reason, response)
        }
    }
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

/// Whether the request carries a `DD-API-KEY` header, or else a `query_key`, equal to one of
/// `api_keys` ([`matches_any_key`]'s constant-time comparison); always `true` when none are
/// configured.
fn authorized(api_keys: &[Box<[u8]>], headers: &HeaderMap, query_key: Option<&[u8]>) -> bool {
    if api_keys.is_empty() {
        return true;
    }
    let Some(sent) = headers.get("dd-api-key").map(HeaderValue::as_bytes).or(query_key) else {
        return false;
    };
    matches_any_key(api_keys, sent)
}

/// The first `api_key=` parameter of a query string, as sent: an API key is hex, so nothing in it
/// is percent-encoded.
fn query_api_key(query: Option<&str>) -> Option<&[u8]> {
    query?.split('&').find_map(|pair| pair.strip_prefix("api_key=")).map(str::as_bytes)
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

#[cfg(test)]
mod tests {
    use super::*;
    use logit_pipeline::test_util::{recv_batch, Totals};
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

        let events = Totals::of(registry.drain(0));
        for route in ["host_metadata", "metadata", "collector", "container", "orch", "intake"] {
            assert_eq!(
                events.sum("logit.input.requests.acknowledged", &[("route", route)]),
                1.0,
                "{route}"
            );
        }
        assert!(
            !events.has("logit.input.events.skipped", &[("reason", "no_title")]),
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

    /// The probe routes a recorded Agent 7.83 sends (`testdata/interop/datadog/`): its key check
    /// carries the key only in the query string, its trace agent asks `/api/v2/validate` with the
    /// header, and something asks `GET /_health`.
    #[tokio::test]
    async fn the_agent_s_probe_routes_answer_200_with_the_key_in_the_query_or_the_header() {
        let input = DatadogInput::new("127.0.0.1:0").with_api_keys(vec![KEY.to_string()]);
        let (addr, _rx) = start(input, 16).await;
        let header = format!("DD-API-KEY: {KEY}\r\n");
        let cases = [
            (format!("/api/v1/validate?api_key={KEY}"), String::new(), "200", r#"{"valid":true}"#),
            ("/api/v1/validate?api_key=nope".to_string(), String::new(), "403", ""),
            ("/api/v2/validate".to_string(), header.clone(), "200", r#"{"valid":true}"#),
            ("/api/v2/validate".to_string(), String::new(), "403", ""),
            ("/_health".to_string(), header.clone(), "200", "{}"),
            (format!("/_health?api_key={KEY}"), String::new(), "200", "{}"),
        ];
        for (path, headers, status, body) in cases {
            let response = request_raw(&addr, "GET", &path, &headers, b"").await;
            assert!(response.starts_with(&format!("HTTP/1.1 {status}")), "{path}: {response}");
            if !body.is_empty() {
                assert_eq!(body_of(&response), body, "{path}");
            }
        }
        // Only a probe route reads the query string: a data route still wants the header.
        let path = format!("/api/v1/series?api_key={KEY}");
        let response = post_raw(&addr, &path, "", SERIES_V1).await;
        assert!(response.starts_with("HTTP/1.1 403"), "{response}");
        let response = request_raw(&addr, "POST", "/_health", &header, b"").await;
        assert!(response.starts_with("HTTP/1.1 405"), "{response}");
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
        let events = Totals::of(registry.drain(0));
        assert_eq!(events.sum("logit.input.requests.rejected", &[("reason", "auth")]), 3.0);
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
        let events = Totals::of(registry.drain(0));
        assert_eq!(
            events.sum("logit.input.requests.rejected", &[("reason", "unknown_route")]),
            1.0
        );
        assert_eq!(events.sum("logit.input.requests", &[("route", "unknown")]), 1.0);
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
            Totals::of(registry.drain(0))
                .sum("logit.input.requests.rejected", &[("reason", "malformed")]),
            1.0
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
            Totals::of(registry.drain(0))
                .sum("logit.input.requests.rejected", &[("reason", "oversize")]),
            3.0
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

        let events = Totals::of(registry.drain(0));
        assert_eq!(events.sum("logit.input.requests", &[("class", "busy")]), 1.0);
        assert_eq!(events.sum("logit.input.batches.dropped", &[("reason", "busy")]), 1.0);
    }

    /// A batch no consumer takes is answered as busy is, but counted `closed_consumer` and
    /// without waiting out `busy_after`.
    #[tokio::test]
    async fn a_request_no_consumer_takes_is_answered_503_and_counted_closed_consumer() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("dd", "datadog_in", "listener");
        let input = DatadogInput::new("127.0.0.1:0").with_telemetry(telemetry);
        let (addr, rx) = start(input, 16).await;
        drop(rx);

        let response = post_raw(&addr, "/api/v1/series", "", SERIES_V1).await;
        assert!(response.starts_with("HTTP/1.1 503"), "{response}");
        assert!(response.to_ascii_lowercase().contains("retry-after: 1\r\n"), "{response}");
        assert!(body_of(&response).contains("closed_consumer"), "{response}");

        let events = Totals::of(registry.drain(0));
        assert_eq!(
            events.sum("logit.input.batches.dropped", &[("reason", "closed_consumer")]),
            1.0
        );
        assert_eq!(events.sum("logit.input.batches.dropped", &[("reason", "busy")]), 0.0);
        assert_eq!(events.sum("logit.input.requests", &[("class", "closed_consumer")]), 1.0);
        assert_eq!(events.sum("logit.input.requests", &[("class", "busy")]), 0.0);
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

        let events = Totals::of(registry.drain(0));
        assert_eq!(events.sum("logit.component.batches.sent", &[]), 1.0);
        assert_eq!(events.sum("logit.input.batches.dropped", &[("reason", "busy")]), 1.0);
    }

    #[tokio::test]
    async fn requests_are_counted_by_route_and_class_and_bytes_are_summed() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("dd", "datadog_in", "listener");
        let (addr, mut rx) =
            start(DatadogInput::new("127.0.0.1:0").with_telemetry(telemetry), 16).await;
        post_raw(&addr, "/api/v2/logs", "", LOGS).await;
        recv_batch(&mut rx).await;
        let events = Totals::of(registry.drain(0));
        assert_eq!(events.sum("logit.input.requests", &[("route", "logs")]), 1.0);
        assert_eq!(events.sum("logit.input.request.bytes", &[]), LOGS.len() as f64);
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
        // The accept loop takes a connection's permit as it accepts it, one accept at a time and in
        // the kernel queue's order, so `first` holds it before `second` is accepted.
        let mut second = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let mut buf = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(2), second.read(&mut buf))
            .await
            .expect("the past-the-cap connection is closed");
        assert!(matches!(read, Ok(0) | Err(_)), "{read:?}");
        assert_eq!(
            Totals::of(registry.drain(0))
                .sum("logit.input.connections.rejected", &[("reason", "limit")]),
            1.0
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
    fn no_api_keys_accepts_anything() {
        assert!(authorized(&[], &HeaderMap::new(), None));
    }

    #[test]
    fn query_api_key_reads_the_first_api_key_parameter() {
        assert_eq!(query_api_key(Some("api_key=abc")), Some(&b"abc"[..]));
        assert_eq!(query_api_key(Some("x=1&api_key=abc&api_key=def")), Some(&b"abc"[..]));
        assert_eq!(query_api_key(Some("my_api_key=abc")), None);
        assert_eq!(query_api_key(None), None);
    }

    #[test]
    fn host_metadata_is_told_apart_from_an_event() {
        assert!(is_host_metadata(HOST_METADATA));
        assert!(!is_host_metadata(EVENT));
        assert!(!is_host_metadata(br#"{"apiKey":"k","events":{},"internalHostname":"h"}"#));
        assert!(!is_host_metadata(b"[1]"), "not an object: left for the decoder to reject");
    }

    // ---- shutdown -----------------------------------------------------------------------------
    //
    // Every test here goes through `spawn_input` and `Running`, so the listener runs
    // `run_until_shutdown` with a real signal. "Returned" means `Running::stop`'s 5s ceiling.

    /// The close grace (`handshake_timeout`, reused) for this section. Short, so a close that
    /// spends the whole grace returns 50x inside `Running::stop`'s 5s ceiling and never ties it.
    const SHUTDOWN_GRACE: Duration = Duration::from_millis(100);

    /// Far past any wait in this section, so a request parked on a full edge stays parked rather
    /// than being answered `503` at the busy bound.
    const PARKED_BUSY_AFTER: Duration = Duration::from_secs(30);

    /// A running listener whose one consumer is a capacity-1 edge carrying its own telemetry, so
    /// a send parked on it counts `logit.component.inbox.full` the moment it parks. Returns the
    /// probe over both the listener's and the consumer's telemetry.
    async fn shutdown_listener(
        input: DatadogInput,
    ) -> (
        String,
        logit_pipeline::test_util::Running,
        mpsc::Receiver<logit_pipeline::Delivered>,
        logit_pipeline::test_util::TelemetryProbe,
    ) {
        let probe = logit_pipeline::test_util::TelemetryProbe::new();
        let mut input = input
            .with_telemetry(probe.telemetry("datadog_in", "datadog_in", "listener"))
            .with_busy_after(PARKED_BUSY_AFTER);
        input.bind().await.expect("binding an ephemeral port");
        let addr = input.local_addr().expect("bind() leaves an address").to_string();
        let (tx, rx) = mpsc::channel(1);
        let edge = logit_pipeline::fanout::Edge::new(tx)
            .with_telemetry(probe.telemetry("sink", "null_out", "sink"));
        let running =
            logit_pipeline::test_util::spawn_input(input, Fanout::from_edges(vec![edge])).await;
        (addr, running, rx, probe)
    }

    fn plaintext_shutdown_input() -> DatadogInput {
        DatadogInput::new("127.0.0.1:0").with_handshake_timeout(SHUTDOWN_GRACE)
    }

    /// One series POST on an open stream with no `Connection: close`, so hyper waits for the next
    /// request rather than closing.
    async fn write_series(stream: &mut tokio::net::TcpStream, addr: &str) {
        let request = format!(
            "POST /api/v1/series HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\n\r\n",
            SERIES_V1.len()
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        stream.write_all(SERIES_V1).await.unwrap();
    }

    /// Two series POSTs on one keep-alive connection: the first fills the edge's one slot and is
    /// answered, the second parks in `send_with_deadline`. Returns once the park is counted.
    async fn park_a_request(
        addr: &str,
        probe: &mut logit_pipeline::test_util::TelemetryProbe,
    ) -> tokio::net::TcpStream {
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        write_series(&mut client, addr).await;
        let response =
            crate::http::read_response(&mut client, "the POST that fills the slot").await;
        assert!(response.starts_with("HTTP/1.1 202"), "got: {response}");
        write_series(&mut client, addr).await;
        probe
            .wait_for("the second POST to park on the full edge", |t| {
                t.sum("logit.component.inbox.full", &[]) >= 1.0
            })
            .await;
        client
    }

    /// Every `Fanout` clone is gone once the listener has returned: the inbox reads closed.
    async fn expect_inbox_closed(rx: &mut mpsc::Receiver<logit_pipeline::Delivered>) {
        let next = tokio::time::timeout(logit_pipeline::test_util::RECV_TIMEOUT, rx.recv())
            .await
            .expect("the inbox should close once the listener returned");
        assert!(next.is_none(), "no batch is left after the ones the test drained");
    }

    /// An h1 keep-alive connection between requests is closed by shutdown, the listener returns,
    /// and no `Fanout` clone outlives it.
    #[tokio::test]
    async fn shutdown_closes_an_idle_keep_alive_connection_and_the_listener_returns() {
        let (addr, running, mut rx, _probe) = shutdown_listener(plaintext_shutdown_input()).await;
        let mut keep_alive = tokio::net::TcpStream::connect(&addr).await.unwrap();
        write_series(&mut keep_alive, &addr).await;
        let response = crate::http::read_response(&mut keep_alive, "the keep-alive POST").await;
        assert!(response.starts_with("HTTP/1.1 202"), "got: {response}");
        recv_batch(&mut rx).await;

        running.stop().await;
        logit_pipeline::test_util::expect_closed(&mut keep_alive, "a keep-alive connection").await;
        expect_inbox_closed(&mut rx).await;
    }

    /// A request parked on a full downstream when shutdown fires is served out: both batches
    /// reach the consumer, the client gets its `202`, then the connection closes and the listener
    /// returns.
    #[tokio::test]
    async fn shutdown_serves_a_parked_request_out_then_closes_its_connection() {
        let (addr, running, mut rx, mut probe) =
            shutdown_listener(plaintext_shutdown_input()).await;
        let mut client = park_a_request(&addr, &mut probe).await;

        running.shutdown.send(true).unwrap();
        recv_batch(&mut rx).await;
        recv_batch(&mut rx).await;
        let response = crate::http::read_response(&mut client, "the parked POST").await;
        assert!(response.starts_with("HTTP/1.1 202"), "got: {response}");
        logit_pipeline::test_util::expect_closed(&mut client, "the parked request's connection")
            .await;
        tokio::time::timeout(logit_pipeline::test_util::RECV_TIMEOUT, running.handle)
            .await
            .expect("the listener should return once its connection closed")
            .expect("the listener task should not panic")
            .expect("the listener should return Ok");
        expect_inbox_closed(&mut rx).await;
    }

    /// Dropping the listener's future (what `run_input`'s grace backstop does) aborts its
    /// connection tasks. The parked batch's reservation drops with its task, so it's never sent,
    /// and the client, never answered, resends it: only the batch that filled the slot arrives.
    #[tokio::test]
    async fn dropping_the_listener_future_aborts_a_parked_request_and_sends_nothing_more() {
        let (addr, running, mut rx, mut probe) =
            shutdown_listener(plaintext_shutdown_input()).await;
        let mut client = park_a_request(&addr, &mut probe).await;

        running.handle.abort();
        logit_pipeline::test_util::expect_closed(&mut client, "an aborted connection").await;
        recv_batch(&mut rx).await;
        logit_pipeline::test_util::assert_no_batch(
            &mut rx,
            // A negative window, ended early by the inbox closing: a send that survived the abort
            // would hold a `Fanout` clone and deliver inside it, the slot being free.
            Duration::from_millis(200),
            "the aborted request's batch",
        )
        .await;
        expect_inbox_closed(&mut rx).await;
        probe
            .wait_for("the connections gauge to read 0", |t| {
                t.gauge("logit.input.connections", &[]) == Some(0.0)
            })
            .await;
    }

    /// A connection still in its TLS accept when shutdown fires ends at once rather than at its
    /// `handshake_timeout`. That timeout is an hour, far past `Running::stop`'s 5s ceiling, so the
    /// stop returns in time only if the prelude races the signal; the gauge reading 0 right after
    /// it shows the connection's task ended before the listener returned.
    #[tokio::test]
    async fn shutdown_ends_a_connection_still_in_its_tls_accept() {
        let settings = TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: None,
        };
        let input = DatadogInput::new("127.0.0.1:0")
            .with_tls(&settings, &testdata_tls_dir(), &logit_pipeline::tls::TlsReloader::new())
            .unwrap()
            .with_handshake_timeout(Duration::from_secs(3600));
        let (addr, running, _rx, mut probe) = shutdown_listener(input).await;

        // Raw TCP, no ClientHello: the task waits in `acceptor.accept`.
        let _silent = tokio::net::TcpStream::connect(&addr).await.unwrap();
        probe
            .wait_for("the connections gauge to read 1", |t| {
                t.gauge("logit.input.connections", &[]) == Some(1.0)
            })
            .await;

        running.stop().await;
        assert_eq!(
            probe.gauge("logit.input.connections", &[]),
            Some(0.0),
            "the connection's task ended before the listener returned"
        );
    }

    // ---- sender address: `peer:` and `proxy_protocol:` ----------------------------------------

    const CONNECTIONS_REJECTED: &str = "logit.input.connections.rejected";

    /// A running listener with `peer:` and `proxy_protocol:` as given, one connection permit, its
    /// diagnostics, and a probe on its telemetry.
    struct Stamping {
        addr: String,
        rx: mpsc::Receiver<logit_pipeline::Delivered>,
        diag: Diagnostics,
        probe: logit_pipeline::test_util::TelemetryProbe,
    }

    async fn sender_input(
        peer: bool,
        proxy_protocol: bool,
        configure: impl FnOnce(DatadogInput) -> DatadogInput,
    ) -> Stamping {
        let probe = logit_pipeline::test_util::TelemetryProbe::new();
        let telemetry = probe.telemetry("dd", "datadog_in", "listener");
        let diag = Diagnostics::new("dd").with_telemetry(telemetry.clone());
        let input = configure(
            DatadogInput::new("127.0.0.1:0")
                .with_telemetry(telemetry)
                .with_diagnostics(diag.clone())
                .with_max_connections(1)
                .with_peer(peer)
                .with_proxy_protocol(proxy_protocol),
        );
        let (addr, rx) = start(input, 16).await;
        Stamping { addr, rx, diag, probe }
    }

    /// A v2 `PROXY` over TCP/IPv4 from 203.0.113.5:41000.
    fn v2_ipv4_header() -> Vec<u8> {
        let mut header = logit_proto::proxy::V2_SIGNATURE.to_vec();
        header.extend_from_slice(&[0x21, 0x11, 0x00, 0x0C]);
        header.extend_from_slice(&[203, 0, 113, 5, 127, 0, 0, 1]);
        header.extend_from_slice(&41000u16.to_be_bytes());
        header.extend_from_slice(&8080u16.to_be_bytes());
        header
    }

    /// A v2 `LOCAL` header, a proxy's own health check, which names no origin.
    fn v2_local_header() -> Vec<u8> {
        let mut header = logit_proto::proxy::V2_SIGNATURE.to_vec();
        header.extend_from_slice(&[0x20, 0x00, 0x00, 0x00]);
        header
    }

    const V1_HEADER: &[u8] = b"PROXY TCP4 198.51.100.7 127.0.0.1 40000 8080\r\n";

    /// An intake `StatsPayload` with two `ClientStatsPayload`s, one group each: two batches.
    fn two_client_stats_payload() -> Vec<u8> {
        let mut w = logit_proto::msgpack::Writer::new();
        w.write_map_len(1);
        w.write_str("Stats");
        w.write_array_len(2);
        for hostname in ["h1", "h2"] {
            w.write_map_len(2);
            w.write_str("Hostname");
            w.write_str(hostname);
            w.write_str("Stats");
            w.write_array_len(1);
            w.write_map_len(3);
            w.write_str("Start");
            w.write_u64(1_700_000_000_000_000_000);
            w.write_str("Duration");
            w.write_u64(10_000_000_000);
            w.write_str("Stats");
            w.write_array_len(1);
            w.write_map_len(2);
            w.write_str("Name");
            w.write_str("http.request");
            w.write_str("Hits");
            w.write_u64(1);
        }
        w.into_inner()
    }

    /// The HTTP/1.1 request carrying [`two_client_stats_payload`], `Connection: close`.
    fn stats_request(host: &str) -> Vec<u8> {
        stats_request_with(host, &[])
    }

    /// [`stats_request`], with `headers` added.
    fn stats_request_with(host: &str, headers: &[(&str, &str)]) -> Vec<u8> {
        let body = two_client_stats_payload();
        let extra: String =
            headers.iter().map(|(name, value)| format!("{name}: {value}\r\n")).collect();
        let mut wire = format!(
            "POST /api/v0.2/stats HTTP/1.1\r\nHost: {host}\r\nContent-Length: {}\r\n\
             Content-Type: application/msgpack\r\nConnection: close\r\n{extra}\r\n",
            body.len()
        )
        .into_bytes();
        wire.extend_from_slice(&body);
        wire
    }

    /// Connects, writes `prefix`, then the two-payload stats request. Returns the response and
    /// the client's local port.
    async fn post_stats_after(addr: &str, prefix: &[u8]) -> (String, u16) {
        post_stats_with(addr, prefix, &[]).await
    }

    /// [`post_stats_after`], with `headers` added to the request.
    async fn post_stats_with(addr: &str, prefix: &[u8], headers: &[(&str, &str)]) -> (String, u16) {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let port = stream.local_addr().unwrap().port();
        let mut wire = prefix.to_vec();
        wire.extend_from_slice(&stats_request_with(addr, headers));
        // Unchecked: a connection refused at the cap may already be closed, and then the
        // response is empty.
        let _ = stream.write_all(&wire).await;
        let mut buf = Vec::new();
        let _ = tokio::time::timeout(
            logit_pipeline::test_util::RECV_TIMEOUT,
            stream.read_to_end(&mut buf),
        )
        .await;
        (String::from_utf8_lossy(&buf).into_owned(), port)
    }

    fn str_attr<'a>(event: &'a logit_core::Event, key: &str) -> Option<&'a str> {
        event.attributes.get(key).and_then(|v| v.as_str())
    }

    /// The two batches the stats body decodes into, with every event checked against the
    /// expected `network.peer.*` and `client.*` values.
    async fn expect_two_stamped_batches(
        rx: &mut mpsc::Receiver<logit_pipeline::Delivered>,
        peer_port: Option<u16>,
        client: Option<(&str, i64)>,
    ) {
        for _ in 0..2 {
            let batch = recv_batch(rx).await;
            assert!(!batch.events.is_empty());
            for event in &batch.events {
                let port = event.attributes.get("network.peer.port");
                match peer_port {
                    Some(expected) => {
                        assert_eq!(str_attr(event, "network.peer.address"), Some("127.0.0.1"));
                        assert_eq!(port, Some(&logit_core::Value::I64(i64::from(expected))));
                    }
                    None => {
                        assert_eq!(event.attributes.get("network.peer.address"), None);
                        assert_eq!(port, None);
                    }
                }
                assert_eq!(str_attr(event, "client.address"), client.map(|(address, _)| address));
                assert_eq!(
                    event.attributes.get("client.port"),
                    client.map(|(_, port)| logit_core::Value::I64(port)).as_ref()
                );
            }
        }
    }

    #[tokio::test]
    async fn peer_stamps_every_batch_of_a_request_with_the_socket_peer() {
        for peer in [true, false] {
            let mut running = sender_input(peer, false, |i| i).await;
            let (response, port) = post_stats_after(&running.addr, b"").await;
            assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
            expect_two_stamped_batches(&mut running.rx, peer.then_some(port), None).await;
        }
    }

    /// The proxy is the socket peer, and the v2 header names the client.
    #[tokio::test]
    async fn a_v2_header_stamps_the_client_beside_the_peer() {
        let mut running = sender_input(true, true, |i| i).await;
        let (response, port) = post_stats_after(&running.addr, &v2_ipv4_header()).await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
        expect_two_stamped_batches(&mut running.rx, Some(port), Some(("203.0.113.5", 41000))).await;
    }

    #[tokio::test]
    async fn a_v1_header_stamps_the_client() {
        let mut running = sender_input(false, true, |i| i).await;
        let (response, _port) = post_stats_after(&running.addr, V1_HEADER).await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
        expect_two_stamped_batches(&mut running.rx, None, Some(("198.51.100.7", 40000))).await;
    }

    fn testdata_tls_dir() -> std::path::PathBuf {
        // The repo root's `testdata/tls` (`testdata/tls/README.md`).
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    /// A client trusting `testdata/tls/ca.pem`, presenting no certificate.
    fn tls_connector() -> tokio_rustls::TlsConnector {
        use rustls_pki_types::pem::PemObject;
        let mut roots = rustls::RootCertStore::empty();
        let ca: Vec<rustls_pki_types::CertificateDer<'static>> =
            rustls_pki_types::CertificateDer::pem_file_iter(testdata_tls_dir().join("ca.pem"))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
        roots.add_parsable_certificates(ca);
        let cfg = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        tokio_rustls::TlsConnector::from(Arc::new(cfg))
    }

    /// The header goes ahead of the ClientHello, in the clear, as a proxy sends it.
    #[tokio::test]
    async fn a_proxy_header_ahead_of_the_tls_handshake_is_read_first() {
        let tls = |input: DatadogInput| {
            let settings = TlsServerSettings {
                cert_file: "server.pem".to_string(),
                key_file: "server.key".to_string(),
                client_ca_file: None,
            };
            input
                .with_tls(&settings, &testdata_tls_dir(), &logit_pipeline::tls::TlsReloader::new())
                .unwrap()
        };
        let mut running = sender_input(false, true, tls).await;
        let mut stream = tokio::net::TcpStream::connect(&running.addr).await.unwrap();
        stream.write_all(&v2_ipv4_header()).await.unwrap();
        let server_name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
        let mut tls_stream = tls_connector().connect(server_name, stream).await.unwrap();
        tls_stream.write_all(&stats_request("localhost")).await.unwrap();
        let mut buf = Vec::new();
        let _ = tokio::time::timeout(
            logit_pipeline::test_util::RECV_TIMEOUT,
            tls_stream.read_to_end(&mut buf),
        )
        .await;
        let response = String::from_utf8_lossy(&buf);
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
        expect_two_stamped_batches(&mut running.rx, None, Some(("203.0.113.5", 41000))).await;
    }

    /// A direct request on a `proxy_protocol: true` port is closed, counted, and never decoded.
    #[tokio::test]
    async fn a_request_without_a_proxy_header_is_rejected_and_counted() {
        let mut running = sender_input(false, true, |i| i).await;
        let mut bare = tokio::net::TcpStream::connect(&running.addr).await.unwrap();
        bare.write_all(b"POST /api/v0.2/stats HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        logit_pipeline::test_util::expect_closed(&mut bare, "a request with no PROXY header").await;
        running
            .probe
            .wait_for("the proxy_header rejection", |totals| {
                totals.sum(CONNECTIONS_REJECTED, &[("reason", "proxy_header")]) == 1.0
            })
            .await;
        assert_eq!(running.diag.occurrences("proxy_header"), 1);
        assert_eq!(running.diag.occurrences("connection_error"), 0);
        assert!(running.rx.try_recv().is_err(), "nothing is delivered");
    }

    /// Sends a request with a `LOCAL` header on new connections until one is answered `200`.
    /// With one permit, that proves every earlier connection's task has finished, diagnostics
    /// included: a connection refused at the cap is closed, and the loop tries again.
    async fn next_connection_is_served(running: &mut Stamping) {
        let deadline = tokio::time::Instant::now() + logit_pipeline::test_util::RECV_TIMEOUT;
        loop {
            assert!(tokio::time::Instant::now() < deadline, "no connection got a permit back");
            let (response, _port) = post_stats_after(&running.addr, &v2_local_header()).await;
            if response.starts_with("HTTP/1.1 200") {
                return;
            }
        }
    }

    /// HAProxy's PROXY-aware health check: a complete header, then an RST before any request
    /// byte. It ends quietly, as a probe with no header does on a plain port.
    #[tokio::test]
    async fn a_reset_after_a_complete_proxy_header_is_not_an_error() {
        for header in [v2_local_header(), V1_HEADER.to_vec()] {
            let mut running = sender_input(false, true, |i| i).await;
            let mut client = tokio::net::TcpStream::connect(&running.addr).await.unwrap();
            client.write_all(&header).await.unwrap();
            // `SO_LINGER` of zero makes the close an RST, not a FIN.
            socket2::SockRef::from(&client).set_linger(Some(Duration::ZERO)).unwrap();
            drop(client);
            next_connection_is_served(&mut running).await;
            assert_eq!(running.diag.occurrences("connection_error"), 0, "after {header:?}");
            assert_eq!(running.diag.occurrences("proxy_header"), 0, "after {header:?}");
            assert_eq!(
                running.probe.sum(CONNECTIONS_REJECTED, &[("reason", "proxy_header")]),
                0.0,
                "after {header:?}"
            );
        }
    }

    /// The same check ended with a FIN: the first-byte peek reads `Ok(0)` after the header.
    #[tokio::test]
    async fn a_close_after_a_complete_proxy_header_is_not_an_error() {
        let mut running = sender_input(false, true, |i| i).await;
        let mut client = tokio::net::TcpStream::connect(&running.addr).await.unwrap();
        client.write_all(&v2_local_header()).await.unwrap();
        drop(client);
        next_connection_is_served(&mut running).await;
        assert_eq!(running.diag.occurrences("connection_error"), 0);
        assert_eq!(running.diag.occurrences("proxy_header"), 0);
        assert_eq!(running.probe.sum(CONNECTIONS_REJECTED, &[("reason", "proxy_header")]), 0.0);
    }

    // ---- sender address: `forwarded:` ---------------------------------------------------------

    /// The two batches the request decodes into, each event carrying `client.address`
    /// `address` and `client.port` `port`, absent where `None`.
    async fn expect_forwarded_client(
        rx: &mut mpsc::Receiver<logit_pipeline::Delivered>,
        address: Option<&str>,
        port: Option<i64>,
        case: &str,
    ) {
        for _ in 0..2 {
            let batch = logit_pipeline::test_util::recv_batch(rx).await;
            assert!(!batch.events.is_empty());
            for event in &batch.events {
                assert_eq!(str_attr(event, "client.address"), address, "{case}");
                let expected = port.map(logit_core::Value::I64);
                assert_eq!(event.attributes.get("client.port"), expected.as_ref(), "{case}");
            }
        }
    }

    /// One request per case, on a fresh listener with `forwarded:` and `proxy_protocol:` as
    /// given: what `client.*` reads, and how many `forwarded` diagnostics it counted. The PROXY
    /// header, when on, names 203.0.113.5:41000.
    #[tokio::test]
    async fn forwarded_reads_the_named_header() {
        use ForwardedHeader::{Forwarded, XForwardedFor, XRealIp};
        type Case = (
            Option<ForwardedHeader>,
            bool,
            &'static [(&'static str, &'static str)],
            Option<&'static str>,
            Option<i64>,
            u64,
        );
        let cases: [Case; 7] = [
            (
                Some(XForwardedFor),
                false,
                &[("X-Forwarded-For", "203.0.113.9, 10.0.0.1")],
                Some("203.0.113.9"),
                None,
                0,
            ),
            (
                Some(Forwarded),
                false,
                &[("Forwarded", r#"for="[2001:db8::1]:443""#)],
                Some("2001:db8::1"),
                Some(443),
                0,
            ),
            (Some(XRealIp), false, &[("X-Real-IP", "203.0.113.9")], Some("203.0.113.9"), None, 0),
            // Absent: the PROXY origin stands, quietly.
            (Some(XForwardedFor), true, &[], Some("203.0.113.5"), Some(41000), 0),
            // Unusable: the PROXY origin stands, diagnosed.
            (
                Some(XForwardedFor),
                true,
                &[("X-Forwarded-For", "unknown")],
                Some("203.0.113.5"),
                Some(41000),
                1,
            ),
            // Only the named header is read.
            (Some(XRealIp), false, &[("X-Forwarded-For", "203.0.113.9")], None, None, 0),
            // Off: nothing is read.
            (None, false, &[("X-Forwarded-For", "203.0.113.9")], None, None, 0),
        ];
        for (forwarded, proxy, headers, address, port, diagnosed) in cases {
            let case = format!("{forwarded:?} proxy {proxy} {headers:?}");
            let mut running = sender_input(false, proxy, |i| i.with_forwarded(forwarded)).await;
            let prefix = if proxy { v2_ipv4_header() } else { Vec::new() };
            let (response, _) = post_stats_with(&running.addr, &prefix, headers).await;
            assert!(response.starts_with("HTTP/1.1 200"), "{case}: {response}");
            expect_forwarded_client(&mut running.rx, address, port, &case).await;
            assert_eq!(running.diag.occurrences("forwarded"), diagnosed, "{case}");
        }
    }

    /// An L4 proxy in front of an L7 one: the PROXY header names the L7 proxy and its ephemeral
    /// port, `X-Forwarded-For` names the client, and the client's address goes out with no port
    /// at all. `network.peer.*` stays the socket.
    #[tokio::test]
    async fn a_forwarding_header_replaces_the_proxy_origin_as_a_pair() {
        let mut running =
            sender_input(true, true, |i| i.with_forwarded(Some(ForwardedHeader::XForwardedFor)))
                .await;
        let headers = [("X-Forwarded-For", "203.0.113.9")];
        let (response, port) = post_stats_with(&running.addr, &v2_ipv4_header(), &headers).await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
        for _ in 0..2 {
            let batch = logit_pipeline::test_util::recv_batch(&mut running.rx).await;
            assert!(!batch.events.is_empty());
            for event in &batch.events {
                assert_eq!(str_attr(event, "client.address"), Some("203.0.113.9"));
                assert_eq!(event.attributes.get("client.port"), None);
                assert_eq!(str_attr(event, "network.peer.address"), Some("127.0.0.1"));
                assert_eq!(
                    event.attributes.get("network.peer.port"),
                    Some(&logit_core::Value::I64(i64::from(port)))
                );
            }
        }
    }
}
