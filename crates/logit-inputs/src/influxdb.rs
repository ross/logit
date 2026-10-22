//! `influxdb_in`: a stand-in for InfluxDB 2.x's write API, the receiving end of Telegraf's
//! `outputs.influxdb_v2`, the InfluxDB client libraries, and `influxdb_out`
//! ([ADR `influxdb-line-protocol-relay`](../../../../docs/adr/influxdb-line-protocol-relay.md)).
//! One TCP listener, optionally TLS, serves HTTP/1.1 and h2c through
//! [`crate::http::auto_builder`], and every write body decodes through
//! [`logit_proto::influxdb::InfluxDecoder`], whose module doc holds the grammar, the mapping table,
//! and the permitted normalizations. This module owns HTTP: routing, authentication, compression,
//! size caps, the query parameters, and backpressure.
//!
//! A point whose fields are all strings decodes to an event carrying no signal, so a
//! `route_signals` downstream discards it unless `signals.none` names the target that feeds
//! `influxdb_out`; the codec's module doc has the rule for every signal selector.
//!
//! The accept loop, connection cap, handshake timeout, idle timeout, shutdown close, and
//! sender-address handling are `splunk_hec_in`'s ([`crate::splunk`]), which takes them from
//! `otlp_in`; `crate::otlp`'s module doc has the reasoning for each. The request helpers are shared
//! through [`crate::http`]. Under `peer:`, `proxy_protocol:`, and `forwarded:`, the request's batch
//! is stamped per [`crate::peer`] before it is delivered.
//!
//! # Responses
//!
//! This table is the canonical response table, with the check order below it. Every error body is
//! influxd's `{"code","message"}` ([`logit_proto::influxdb::response`]), and every response carries
//! `X-Influxdb-Version: logit/<version>`. A route matches only as written: a trailing `/` is a
//! `404`, where influxd answers a `308` redirect.
//!
//! | Request | Response | `influxdb_out` reads it as |
//! |---|---|---|
//! | `POST /api/v2/write`, decoded and in every downstream inbox | `204`, empty | `Ok` |
//! | an empty, blank, or comment-only body | `204`, nothing delivered | `Ok` |
//! | a line that fails to parse | `400` `invalid`, `unable to parse '<line>': <reason>`; nothing delivered | `Rejected` |
//! | `precision` other than `ns`, `us`, `ms`, `s`, or empty | `400` `invalid` | `Rejected` |
//! | no non-empty `bucket` | `404` `not found`, `bucket not found` | `Refused` |
//! | no non-empty `org` or `orgID` | `400` `invalid`, `Please provide either orgID or org` | `Rejected` |
//! | `tokens:` set and no `Authorization: Token <t>` or `Bearer <t>` naming one | `401` `unauthorized`, `unauthorized access` | `Refused` |
//! | a body over `max_request_bytes`, by `Content-Length`, as read, or after gzip | `413` `request too large` | `Rejected` |
//! | a `Content-Encoding` other than lowercase `gzip` or `identity` | `415` `unsupported media type` | `Rejected` |
//! | a body labeled `gzip` that doesn't decompress | `400` `invalid` | `Rejected` |
//! | a body that stops arriving, under `idle_timeout:` | `408` `invalid`; the connection closes | `Rejected` |
//! | busy past [`BUSY_AFTER`], or every downstream consumer closed | `503` `unavailable`, `Retry-After: 1`; nothing taken | `Ambiguous` |
//! | `GET`/`HEAD /ping` | `204`; no token needed | not sent |
//! | `GET`/`HEAD /health` | `200` influxd's health JSON, `version` `logit/<version>`; no token needed | not sent |
//! | `OPTIONS` on a route above | `204` with `Allow`; no token needed | not sent |
//! | another method on a route above | `405` `method not allowed` with `Allow` (`OPTIONS, POST` on the write route) | `Refused` |
//! | any other path, v1 `/write` included | `404` `not found`, `path not found` | `Refused` |
//!
//! The checks run in this order: route and method, `/ping` and `/health`, the declared size, the
//! token, `Content-Encoding`, then `precision`, `bucket`, and `org` (influxd's order), then the
//! body. Nothing here sends `403`, `422`, or `429`: there are no per-bucket permissions, no stored
//! schema, and no rate limit. `Content-Type` is ignored and a chunked body is accepted, as at
//! influxd. A token is compared in constant time ([`crate::http::matches_any_key`]) and never
//! logged, counted, or kept on an event.
//!
//! **One request, one batch, all or nothing.** The decoder parses the whole body before building
//! any event, so a parse error delivers nothing. A decoded body is one batch, sent under one
//! deadline ([`crate::http::deliver_with_deadline`]) that reaches every consumer or none; a `503`
//! therefore always means nothing of the body was taken, and a client may resend it whole.
//!
//! **Resource cache.** `org`, `orgID`, and `bucket`, each when non-empty, become the private
//! resource attributes `_logit.influxdb.org`, `_logit.influxdb.org_id`, and
//! `_logit.influxdb.bucket`. Requests naming the same
//! three strings share one `Arc<Resource>`, so a downstream batch accumulator, which groups on the
//! resource's pointer, merges them. The cache holds at most [`RESOURCE_CACHE_CAP`] entries and is
//! cleared whole when a new one would pass it: an accidental flood of distinct buckets costs only
//! accumulator merges, never unbounded memory.
//!
//! **Receipt time.** Each request reads the wall clock once, after its body is read, and passes it
//! to the decoder, which stamps every timestamp-less point with it truncated to `precision`.
//!
//! # Telemetry
//!
//! Per request: `logit.input.requests{route, class}`, `logit.input.request.duration`, and
//! `logit.input.request.bytes`. Rejections: `logit.input.requests.rejected{reason}`. A decoded
//! batch a `503` left undelivered: `logit.input.batches.deferred` when the pipeline was busy,
//! since the client still holds the body, and
//! `logit.input.batches.dropped{reason="closed_consumer"}` when no consumer is open. The decoder
//! counts `logit.input.tags.omitted{reason="reserved_prefix"}`. The connection metrics are
//! `otlp_in`'s verbatim. The reasons, classes, and meanings are in
//! `docs/design/internal-telemetry.md`'s `influxdb_in` section.

use crate::http::{
    body_read_error_message, collect_with_stall_bound, declared_length, decompress,
    deliver_with_deadline, drive_connection, is_length_limit, json_response, matches_any_key,
    now_nanos, Activity, BodyReadError, DecompressError, Encoding, Undelivered,
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
use logit_core::{Diagnostics, Resource, Telemetry};
use logit_pipeline::listen::BindOptions;
use logit_pipeline::Fanout;
use logit_proto::forwarded::ForwardedHeader;
use logit_proto::influxdb::response::{
    error_body, parse_error_body, CODE_INVALID, CODE_NOT_FOUND, CODE_REQUEST_TOO_LARGE,
    CODE_UNAUTHORIZED, CODE_UNAVAILABLE, CODE_UNSUPPORTED_MEDIA_TYPE,
};
use logit_proto::influxdb::{write_resource, InfluxDecoder, Precision};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;

/// Default for [`InfluxDbInput::with_max_request_bytes`]: 25 MiB. influxd enforces no limit, so
/// this is a `logit` choice, well above Telegraf's default batch of 1000 metrics. Mirrored by hand
/// in `logit_config`'s `influxdb_in` default.
pub const DEFAULT_MAX_REQUEST_BYTES: usize = 25 * 1024 * 1024;

/// Default for [`InfluxDbInput::with_handshake_timeout`]: the same 5s as every other TCP listener,
/// mirrored by hand in `logit_config::default_handshake_timeout`. Also the grace an idle close
/// gives hyper.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// How long one request's delivery may wait on a full downstream before it is answered `503`,
/// `datadog_in`'s and `splunk_hec_in`'s bound.
const BUSY_AFTER: Duration = Duration::from_secs(5);

/// How many distinct `(org, orgID, bucket)` resources the listener keeps (this module's "Resource
/// cache").
pub const RESOURCE_CACHE_CAP: usize = 64;

/// The `X-Influxdb-Version` header on every response, and `/health`'s `version`.
const VERSION: &str = concat!("logit/", env!("CARGO_PKG_VERSION"));

/// influxd's `/health` body, with this build's version.
const HEALTH_BODY: &str = concat!(
    r#"{"name":"influxdb","message":"ready for queries and writes","status":"pass","checks":[],"#,
    r#""version":"logit/"#,
    env!("CARGO_PKG_VERSION"),
    r#"","commit":""}"#
);

const VERSION_HEADER: &str = "x-influxdb-version";

/// `logit_pipeline::tls::TlsServerSettings`, re-exported as `otlp_in`'s is.
pub use logit_pipeline::tls::TlsServerSettings;

/// The `influxdb_in` listener. See this module's doc.
pub struct InfluxDbInput {
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
    /// Empty accepts any request.
    tokens: Arc<[Box<[u8]>]>,
    max_request_bytes: usize,
    /// Bounds the connections [`Input::run`] serves at once; a connection past the cap is
    /// rejected, not queued. With the default 25 MiB request cap, which bounds the compressed and
    /// decompressed body alike, the worst case at the default cap is about 10 TiB, a bound rather
    /// than a memory budget ([`crate::http::MAX_CONCURRENT_STREAMS`] has the formula).
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

impl InfluxDbInput {
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
    /// first-byte peek, each on its own budget (`handshake_timeout:` in config).
    pub fn with_handshake_timeout(mut self, handshake_timeout: Duration) -> Self {
        self.handshake_timeout = handshake_timeout;
        self
    }

    /// Bounds how long a connection may sit with no request in flight, and how long one frame of a
    /// request body may take (`idle_timeout:` in config; off when `None`), with `otlp_in`'s
    /// semantics.
    pub fn with_idle_timeout(mut self, idle_timeout: Option<Duration>) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    /// The tokens this listener accepts (`tokens:` in config). Empty accepts any request.
    pub fn with_tokens(mut self, tokens: Vec<String>) -> Self {
        self.tokens = tokens.into_iter().map(|t| t.into_bytes().into_boxed_slice()).collect();
        self
    }

    /// Overrides [`DEFAULT_MAX_REQUEST_BYTES`], the cap on a request body both as sent and after
    /// gzip decompression (`max_request_bytes:` in config).
    pub fn with_max_request_bytes(mut self, max_request_bytes: usize) -> Self {
        self.max_request_bytes = max_request_bytes;
        self
    }

    /// Overrides [`crate::DEFAULT_MAX_CONNECTIONS`]; `max_connections:` in config.
    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Overrides [`BUSY_AFTER`]. Not a config field: a test hook, as on `splunk_hec_in`, so a
    /// busy case doesn't wait out five real seconds.
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
    /// (`proxy_protocol:` in config), with `otlp_in`'s rules. Off by default.
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
impl Input for InfluxDbInput {
    async fn bind(&mut self) -> anyhow::Result<()> {
        if self.listener.is_some() {
            return Ok(()); // idempotent, per `Input::bind`'s contract
        }
        let listener = logit_pipeline::listen::bind_tcp(&self.bind, self.bind_options).await?;
        let addr = logit_pipeline::listen::bound_display(listener.local_addr(), &self.bind);
        self.diag.info("bound", format_args!("listening on {addr}"));
        self.listener = Some(listener);
        Ok(())
    }

    /// `otlp_in`'s accept loop, as `splunk_hec_in` runs it.
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
        let resources = Arc::new(ResourceCache::new(RESOURCE_CACHE_CAP));
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
            let tokens = Arc::clone(&self.tokens);
            let (max_request_bytes, busy_after) = (self.max_request_bytes, self.busy_after);
            let resources = Arc::clone(&resources);
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
                        tokens,
                        max_request_bytes,
                        busy_after,
                        resources,
                        decoder: Mutex::new(None),
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
    tokens: Arc<[Box<[u8]>]>,
    max_request_bytes: usize,
    busy_after: Duration,
    /// Shared by every connection of one run.
    resources: Arc<ResourceCache>,
    /// The connection's decoder, kept between requests for its warm scratch. A request takes it
    /// and puts it back, so concurrent h2 streams on one connection each decode with their own.
    decoder: Mutex<Option<InfluxDecoder>>,
    /// Names the sender in diagnostic text, and stamps every batch under `peer:`,
    /// `proxy_protocol:`, or `forwarded:`.
    peer: ConnectionPeer,
}

/// `(org, orgID, bucket)`, each as the query string spelled it.
type ResourceKey = (Option<String>, Option<String>, String);

/// This module's "Resource cache": one `Arc<Resource>` per distinct `(org, orgID, bucket)`.
struct ResourceCache {
    entries: Mutex<HashMap<ResourceKey, Arc<Resource>>>,
    cap: usize,
}

impl ResourceCache {
    fn new(cap: usize) -> Self {
        Self { entries: Mutex::new(HashMap::new()), cap: cap.max(1) }
    }

    fn get(&self, key: ResourceKey) -> Arc<Resource> {
        // The map only ever holds complete entries, so a poisoned lock still holds a usable one.
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(resource) = entries.get(&key) {
            return Arc::clone(resource);
        }
        if entries.len() >= self.cap {
            entries.clear();
        }
        let (org, org_id, bucket) = &key;
        let resource = Arc::new(write_resource(org.as_deref(), org_id.as_deref(), bucket));
        entries.insert(key, Arc::clone(&resource));
        resource
    }
}

/// Serves one accepted (and, with TLS on, handshaken) connection to completion: `splunk_hec_in`'s,
/// with this listener's handler.
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
/// `logit.input.requests{route, class}` count and one `logit.input.request.duration` timing, and
/// every response gets `X-Influxdb-Version`.
async fn handle(
    req: http::Request<Incoming>,
    shared: &Shared,
    activity: &Activity,
    stall: Option<Duration>,
) -> Result<http::Response<Full<Bytes>>, std::convert::Infallible> {
    let started = Instant::now();
    let (route, class, mut response) = respond(req, shared, activity, stall).await;
    response.headers_mut().insert(VERSION_HEADER, HeaderValue::from_static(VERSION));
    shared.telemetry.count("logit.input.requests", 1.0, &[("route", route), ("class", class)]);
    shared.telemetry.timing("logit.input.request.duration", started.elapsed(), &[]);
    Ok(response)
}

const OK: &str = "ok";
const REJECTED: &str = "rejected";
const BUSY: &str = "busy";

/// One route of this module's "Responses" table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Route {
    Write,
    Ping,
    Health,
}

impl Route {
    fn from_path(path: &str) -> Option<Self> {
        Some(match path {
            "/api/v2/write" => Self::Write,
            "/ping" => Self::Ping,
            "/health" => Self::Health,
            _ => return None,
        })
    }

    /// The `route` tag on this listener's request counters.
    fn name(self) -> &'static str {
        match self {
            Self::Write => "write",
            Self::Ping => "ping",
            Self::Health => "health",
        }
    }

    fn allows(self, method: &Method) -> bool {
        match self {
            Self::Write => method == Method::POST,
            Self::Ping | Self::Health => method == Method::GET || method == Method::HEAD,
        }
    }

    /// influxd's `Allow` on the write route; the read routes list their own methods.
    fn allow_header(self) -> &'static str {
        match self {
            Self::Write => "OPTIONS, POST",
            Self::Ping | Self::Health => "GET, HEAD, OPTIONS",
        }
    }
}

/// This module's "Responses" table, in the order its checks run, returning the counters' `route`
/// and `class` tags alongside the response.
async fn respond(
    req: http::Request<Incoming>,
    shared: &Shared,
    activity: &Activity,
    stall: Option<Duration>,
) -> (&'static str, &'static str, http::Response<Full<Bytes>>) {
    let Some(route) = Route::from_path(req.uri().path()) else {
        let response = influx_error(StatusCode::NOT_FOUND, CODE_NOT_FOUND, "path not found");
        return ("unknown", REJECTED, reject(shared, "unknown_route", None, response));
    };
    let name = route.name();
    if req.method() == Method::OPTIONS {
        let mut response = empty(StatusCode::NO_CONTENT);
        response
            .headers_mut()
            .insert(http::header::ALLOW, HeaderValue::from_static(route.allow_header()));
        return (name, OK, response);
    }
    if !route.allows(req.method()) {
        let mut response = influx_error(
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed",
            "method not allowed",
        );
        response
            .headers_mut()
            .insert(http::header::ALLOW, HeaderValue::from_static(route.allow_header()));
        return (name, REJECTED, reject(shared, "method", None, response));
    }
    match route {
        Route::Ping => return (name, OK, empty(StatusCode::NO_CONTENT)),
        Route::Health => {
            return (
                name,
                OK,
                json_response(StatusCode::OK, Bytes::from_static(HEALTH_BODY.as_bytes())),
            )
        }
        Route::Write => {}
    }

    let cap = shared.max_request_bytes;
    if declared_length(req.headers()).is_some_and(|len| len > cap as u64) {
        let message = format!("request body exceeds the {cap}-byte limit");
        return (name, REJECTED, too_large(shared, "oversize", &message));
    }
    if !authenticate(&shared.tokens, req.headers()) {
        let response =
            influx_error(StatusCode::UNAUTHORIZED, CODE_UNAUTHORIZED, "unauthorized access");
        let message = "a missing or unknown token";
        return (name, REJECTED, reject(shared, "auth", Some(message), response));
    }
    let Some(encoding) = encoding(req.headers()) else {
        let message = "Content-Encoding must be gzip or identity";
        let response =
            influx_error(StatusCode::UNSUPPORTED_MEDIA_TYPE, CODE_UNSUPPORTED_MEDIA_TYPE, message);
        return (name, REJECTED, reject(shared, "encoding", Some(message), response));
    };
    let query = Query::parse(req.uri().query());
    let Some(precision) = query.precision else {
        let message = "invalid precision; valid precision units are ns, us, ms, and s";
        let response = influx_error(StatusCode::BAD_REQUEST, CODE_INVALID, message);
        return (name, REJECTED, reject(shared, "precision", Some(message), response));
    };
    let Some(bucket) = query.bucket else {
        let response = influx_error(StatusCode::NOT_FOUND, CODE_NOT_FOUND, "bucket not found");
        return (name, REJECTED, reject(shared, "no_bucket", Some("no bucket named"), response));
    };
    if query.org.is_none() && query.org_id.is_none() {
        let message = "Please provide either orgID or org";
        let response = influx_error(StatusCode::BAD_REQUEST, CODE_INVALID, message);
        return (name, REJECTED, reject(shared, "no_org", Some(message), response));
    }
    let resource = shared.resources.get((query.org, query.org_id, bucket));

    let request_peer = shared.peer.request(req.headers());
    let body = match collect_with_stall_bound(Limited::new(req.into_body(), cap), stall).await {
        Ok(body) => body,
        // `otlp_in`'s `408`: the client's clock, not its size, and the connection closes once
        // this response is out.
        Err(BodyReadError::Stalled(stall)) => {
            activity.request_close();
            let message = format!("request body stalled for {stall:?}");
            let response = influx_error(StatusCode::REQUEST_TIMEOUT, CODE_INVALID, &message);
            return (name, REJECTED, reject(shared, "stalled", Some(&message), response));
        }
        Err(BodyReadError::Failed(err)) => {
            let reason = if is_length_limit(err.as_ref()) { "oversize" } else { "body_read" };
            let message = body_read_error_message(err.as_ref());
            return (name, REJECTED, too_large(shared, reason, &message));
        }
    };
    shared.telemetry.count("logit.input.request.bytes", body.len() as f64, &[]);

    let body = match decompress(encoding, body, cap) {
        Ok(body) => body,
        Err(DecompressError::TooLarge) => {
            let message = format!("decompressed request exceeds the {cap}-byte limit");
            return (name, REJECTED, too_large(shared, "oversize", &message));
        }
        Err(DecompressError::Malformed(message)) => {
            let response = influx_error(StatusCode::BAD_REQUEST, CODE_INVALID, &message);
            return (
                name,
                REJECTED,
                reject(shared, "malformed_encoding", Some(&message), response),
            );
        }
    };

    let received_at = now_nanos();
    let mut decoder = shared
        .decoder
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take()
        .unwrap_or_else(|| InfluxDecoder::new().with_telemetry(shared.telemetry.clone()));
    let decoded = decoder.decode_body(body, precision, received_at, resource);
    *shared.decoder.lock().unwrap_or_else(PoisonError::into_inner) = Some(decoder);
    let mut batch = match decoded {
        Ok(batch) => batch,
        Err(err) => {
            let response = json_response(StatusCode::BAD_REQUEST, parse_error_body(&err));
            // The line's text is the sender's data; the diagnostic names its index only.
            let message = format!("line {} does not parse: {}", err.line, err.reason);
            return (name, REJECTED, reject(shared, "malformed", Some(&message), response));
        }
    };
    if batch.events.is_empty() {
        // An empty, blank, or comment-only body: influxd answers `204` and stores nothing.
        return (name, OK, empty(StatusCode::NO_CONTENT));
    }

    request_peer.stamp_batches(std::slice::from_mut(&mut batch));
    match deliver_with_deadline(&shared.sink, vec![batch], shared.busy_after).await {
        Ok(()) => (name, OK, empty(StatusCode::NO_CONTENT)),
        Err(undelivered @ Undelivered::Busy(_)) => {
            undelivered.record(&shared.telemetry);
            shared.diag.clone().warn_throttled(
                "busy",
                format_args!(
                    "influxdb_in: answered 503 to {}: the pipeline did not accept a batch within \
                     {:?}",
                    shared.peer, shared.busy_after
                ),
            );
            (name, undelivered_class(undelivered), unavailable())
        }
        Err(undelivered @ Undelivered::Closed(_)) => {
            undelivered.record(&shared.telemetry);
            shared.diag.clone().warn_throttled(
                "closed_consumer",
                format_args!(
                    "influxdb_in: refused a write from {}: no consumer took its batch",
                    shared.peer
                ),
            );
            (name, undelivered_class(undelivered), unavailable())
        }
    }
}

/// The `class` tag for a write `503`'d undelivered.
fn undelivered_class(undelivered: Undelivered) -> &'static str {
    match undelivered {
        Undelivered::Busy(_) => BUSY,
        Undelivered::Closed(_) => undelivered.reason(),
    }
}

/// `503` `unavailable` with `Retry-After: 1`: nothing of the body was taken, so the client
/// resends it whole.
fn unavailable() -> http::Response<Full<Bytes>> {
    let mut response = influx_error(
        StatusCode::SERVICE_UNAVAILABLE,
        CODE_UNAVAILABLE,
        "the pipeline is busy; retry the write",
    );
    response.headers_mut().insert(http::header::RETRY_AFTER, HeaderValue::from_static("1"));
    response
}

/// A `413` counted under `reason`.
fn too_large(shared: &Shared, reason: &'static str, message: &str) -> http::Response<Full<Bytes>> {
    let response = influx_error(StatusCode::PAYLOAD_TOO_LARGE, CODE_REQUEST_TOO_LARGE, message);
    reject(shared, reason, Some(message), response)
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
            format_args!("influxdb_in: rejecting a request from {}: {message}", shared.peer),
        );
    }
    response
}

/// influxd's `{"code","message"}` under `status`.
fn influx_error(status: StatusCode, code: &str, message: &str) -> http::Response<Full<Bytes>> {
    json_response(status, error_body(code, message))
}

fn empty(status: StatusCode) -> http::Response<Full<Bytes>> {
    http::Response::builder()
        .status(status)
        .body(Full::new(Bytes::new()))
        .expect("a well-formed response always builds")
}

/// The body's coding: none or `identity` (any case) is identity, and only an exact lowercase
/// `gzip` is gzip, as influxd matches it. `None` is a `415`.
fn encoding(headers: &HeaderMap) -> Option<Encoding> {
    let Some(value) = headers.get(http::header::CONTENT_ENCODING) else {
        return Some(Encoding::Identity);
    };
    let value = value.as_bytes().trim_ascii();
    if value == b"gzip" {
        Some(Encoding::Gzip)
    } else if value.eq_ignore_ascii_case(b"identity") {
        Some(Encoding::Identity)
    } else {
        None
    }
}

/// `true` when `tokens` is empty or the `Authorization` header is `Token <t>` or `Bearer <t>`,
/// the scheme in any case, naming one of them. The comparison is constant-time per token length
/// ([`matches_any_key`]), and the token never leaves this function.
fn authenticate(tokens: &[Box<[u8]>], headers: &HeaderMap) -> bool {
    if tokens.is_empty() {
        return true;
    }
    let Some(value) = headers.get(http::header::AUTHORIZATION) else { return false };
    let value = value.as_bytes().trim_ascii();
    let Some(at) = value.iter().position(|&b| b == b' ') else { return false };
    let (scheme, token) = (&value[..at], value[at + 1..].trim_ascii());
    let known_scheme =
        scheme.eq_ignore_ascii_case(b"token") || scheme.eq_ignore_ascii_case(b"bearer");
    known_scheme && !token.is_empty() && matches_any_key(tokens, token)
}

/// What a write reads from its query string. The first occurrence of each key wins, as influxd
/// reads it. An empty `org`, `orgID`, or `bucket` names none; an empty or absent `precision` is
/// `ns`, and an unknown one is `None`.
#[derive(Debug, PartialEq)]
struct Query {
    org: Option<String>,
    org_id: Option<String>,
    bucket: Option<String>,
    precision: Option<Precision>,
}

impl Query {
    fn parse(query: Option<&str>) -> Self {
        let mut org = None;
        let mut org_id = None;
        let mut bucket = None;
        let mut precision: Option<String> = None;
        for (key, value) in form_urlencoded::parse(query.unwrap_or("").as_bytes()) {
            let slot = match key.as_ref() {
                "org" => &mut org,
                "orgID" => &mut org_id,
                "bucket" => &mut bucket,
                "precision" => &mut precision,
                _ => continue,
            };
            if slot.is_none() {
                *slot = Some(value.into_owned());
            }
        }
        let non_empty = |s: Option<String>| s.filter(|s| !s.is_empty());
        let precision = match precision.as_deref() {
            None | Some("") => Some(Precision::Ns),
            Some(unit) => Precision::from_query(unit),
        };
        Self {
            org: non_empty(org),
            org_id: non_empty(org_id),
            bucket: non_empty(bucket),
            precision,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner::resolve;
    use logit_core::{EventBatch, MetricKind, MetricValue, Value};
    use logit_pipeline::test_util::{recv_batch, Totals};
    use rustls_pki_types::pem::PemObject;
    use rustls_pki_types::CertificateDer;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::mpsc;

    const TOKEN: &str = "s3cr3t-token";
    const WRITE: &str = "/api/v2/write?org=acme&bucket=metrics";
    const TWO_POINTS: &[u8] = b"cpu,host=a usage=1.5,n=2i 1700000000000000000\n\
                                mem,host=a used=7u 1700000000000000000\n";

    /// A bound, running listener with `input`'s settings, delivering into a channel of
    /// `capacity`. Returns the address and the channel's receiving end.
    async fn start(
        input: InfluxDbInput,
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
        start(InfluxDbInput::new("127.0.0.1:0"), 16).await
    }

    /// A listener with telemetry, and the registry it counts into.
    async fn start_counted(
        configure: impl FnOnce(InfluxDbInput) -> InfluxDbInput,
        capacity: usize,
    ) -> (String, mpsc::Receiver<logit_pipeline::Delivered>, Arc<logit_core::Registry>) {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("influx", "influxdb_in", "listener");
        let (addr, rx) =
            start(configure(InfluxDbInput::new("127.0.0.1:0").with_telemetry(telemetry)), capacity)
                .await;
        (addr, rx, registry)
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
        let _ = tokio::time::timeout(
            logit_pipeline::test_util::RECV_TIMEOUT,
            stream.read_to_end(&mut buf),
        )
        .await;
        String::from_utf8_lossy(&buf).into_owned()
    }

    async fn post(addr: &str, path: &str, headers: &str, body: &[u8]) -> String {
        request_raw(addr, "POST", path, headers, body).await
    }

    fn body_of(response: &str) -> &str {
        response.split_once("\r\n\r\n").map_or("", |(_, body)| body)
    }

    fn has_header(response: &str, line: &str) -> bool {
        let head = response.split_once("\r\n\r\n").map_or(response, |(head, _)| head);
        head.to_ascii_lowercase().lines().any(|l| l == line.to_ascii_lowercase())
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut encoder, bytes).unwrap();
        encoder.finish().unwrap()
    }

    fn str_attr<'a>(attrs: &'a logit_core::AttrMap, key: &str) -> Option<&'a str> {
        attrs.get(key).and_then(|v| v.as_str())
    }

    /// Each record's name and value, in order.
    fn fields(event: &logit_core::Event) -> Vec<(&'static str, MetricValue)> {
        event
            .metrics
            .iter()
            .map(|m| match &m.kind {
                MetricKind::Gauge(v) => (resolve(m.name), *v),
                other => panic!("every field is a Gauge, got {other:?}"),
            })
            .collect()
    }

    // ---- writes ---------------------------------------------------------------------------------

    #[tokio::test]
    async fn a_write_delivers_one_batch_and_answers_204_with_the_version_header() {
        let (addr, mut rx) = start_default().await;
        let response = post(&addr, WRITE, "", TWO_POINTS).await;
        assert!(response.starts_with("HTTP/1.1 204"), "{response}");
        assert_eq!(body_of(&response), "");
        assert!(has_header(&response, &format!("x-influxdb-version: {VERSION}")), "{response}");

        let batch = recv_batch(&mut rx).await;
        assert_eq!(batch.events.len(), 2);
        let cpu = &batch.events[0];
        assert_eq!(cpu.timestamp, 1_700_000_000_000_000_000);
        assert_eq!(str_attr(&cpu.attributes, "_logit.influxdb.measurement"), Some("cpu"));
        assert_eq!(str_attr(&cpu.attributes, "host"), Some("a"));
        assert_eq!(
            fields(cpu),
            [("cpu.usage", MetricValue::F64(1.5)), ("cpu.n", MetricValue::I64(2))]
        );
        assert_eq!(fields(&batch.events[1]), [("mem.used", MetricValue::U64(7))]);
        let resource = &batch.resource.attributes;
        assert_eq!(str_attr(resource, "_logit.influxdb.org"), Some("acme"));
        assert_eq!(str_attr(resource, "_logit.influxdb.bucket"), Some("metrics"));
        assert_eq!(resource.len(), 2);
    }

    /// Telegraf's `outputs.influxdb_v2` and the client libraries may stream a body chunked.
    #[tokio::test]
    async fn a_chunked_body_is_accepted() {
        let (addr, mut rx) = start_default().await;
        let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let head = format!(
            "POST {WRITE} HTTP/1.1\r\nHost: {addr}\r\nTransfer-Encoding: chunked\r\n\
             Connection: close\r\n\r\n{:x}\r\n",
            TWO_POINTS.len()
        );
        stream.write_all(head.as_bytes()).await.unwrap();
        stream.write_all(TWO_POINTS).await.unwrap();
        stream.write_all(b"\r\n0\r\n\r\n").await.unwrap();
        let mut buf = Vec::new();
        let _ = tokio::time::timeout(
            logit_pipeline::test_util::RECV_TIMEOUT,
            stream.read_to_end(&mut buf),
        )
        .await;
        let response = String::from_utf8_lossy(&buf);
        assert!(response.starts_with("HTTP/1.1 204"), "{response}");
        assert_eq!(recv_batch(&mut rx).await.events.len(), 2);
    }

    /// A parse error on the second line answers influxd's `400` and delivers neither line.
    #[tokio::test]
    async fn a_parse_error_is_400_and_delivers_nothing() {
        let (addr, rx, registry) = start_counted(|i| i, 16).await;
        let body = b"cpu value=1 1\ncpu value= 2\n";
        let response = post(&addr, WRITE, "", body).await;
        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
        assert_eq!(
            body_of(&response),
            r#"{"code":"invalid","message":"unable to parse 'cpu value= 2': missing field value"}"#
        );
        assert!(rx.is_empty(), "all or nothing");
        let totals = Totals::of(registry.drain(0));
        assert_eq!(totals.sum("logit.input.requests.rejected", &[("reason", "malformed")]), 1.0);
        assert_eq!(totals.sum("logit.input.requests", &[("class", "rejected")]), 1.0);
    }

    #[tokio::test]
    async fn duplicate_points_in_one_body_merge() {
        let (addr, mut rx) = start_default().await;
        let body = b"cpu,host=a f=1i,g=1i 5\ncpu,host=a f=2i,h=3i 5\n";
        let response = post(&addr, WRITE, "", body).await;
        assert!(response.starts_with("HTTP/1.1 204"), "{response}");
        let batch = recv_batch(&mut rx).await;
        assert_eq!(batch.events.len(), 1);
        assert_eq!(
            fields(&batch.events[0]),
            [
                ("cpu.f", MetricValue::I64(2)),
                ("cpu.g", MetricValue::I64(1)),
                ("cpu.h", MetricValue::I64(3))
            ]
        );
    }

    #[tokio::test]
    async fn precision_scales_timestamps_and_empty_means_ns() {
        let (addr, mut rx) = start_default().await;
        for (precision, expected) in [
            ("&precision=s", 7_000_000_000),
            ("&precision=ms", 7_000_000),
            ("&precision=us", 7_000),
            ("&precision=ns", 7),
            ("&precision=", 7),
            ("", 7),
        ] {
            let response = post(&addr, &format!("{WRITE}{precision}"), "", b"m f=1 7").await;
            assert!(response.starts_with("HTTP/1.1 204"), "{precision}: {response}");
            assert_eq!(recv_batch(&mut rx).await.events[0].timestamp, expected, "{precision}");
        }
    }

    #[tokio::test]
    async fn an_unknown_precision_is_400() {
        let (addr, rx, registry) = start_counted(|i| i, 16).await;
        for precision in ["h", "NS", "n"] {
            let response =
                post(&addr, &format!("{WRITE}&precision={precision}"), "", b"m f=1 7").await;
            assert!(response.starts_with("HTTP/1.1 400"), "{precision}: {response}");
            assert_eq!(
                body_of(&response),
                r#"{"code":"invalid","message":"invalid precision; valid precision units are ns, us, ms, and s"}"#
            );
        }
        assert!(rx.is_empty());
        assert_eq!(
            Totals::of(registry.drain(0))
                .sum("logit.input.requests.rejected", &[("reason", "precision")]),
            3.0
        );
    }

    /// Every timestamp-less point gets the one receipt instant, truncated to `precision`.
    #[tokio::test]
    async fn a_point_with_no_timestamp_gets_the_receipt_time_truncated_to_precision() {
        let (addr, mut rx) = start_default().await;
        let before = now_nanos();
        let response =
            post(&addr, &format!("{WRITE}&precision=s"), "", b"a f=1\nb f=2\nc f=3 9\n").await;
        let after = now_nanos();
        assert!(response.starts_with("HTTP/1.1 204"), "{response}");
        let batch = recv_batch(&mut rx).await;
        let (a, b) = (batch.events[0].timestamp, batch.events[1].timestamp);
        assert_eq!(a, b, "one instant for the request");
        assert_eq!(a % 1_000_000_000, 0, "truncated to seconds");
        assert!(a >= Precision::S.truncate(before) && a <= after, "{before} <= {a} <= {after}");
        assert_eq!(batch.events[2].timestamp, 9_000_000_000);
    }

    #[tokio::test]
    async fn an_empty_or_comment_only_body_is_204_and_delivers_nothing() {
        let (addr, rx) = start_default().await;
        for body in [&b""[..], b"\n  \n", b"# a comment\n"] {
            let response = post(&addr, WRITE, "", body).await;
            assert!(response.starts_with("HTTP/1.1 204"), "{body:?}: {response}");
        }
        assert!(rx.is_empty());
    }

    // ---- query parameters and the resource ------------------------------------------------------

    #[tokio::test]
    async fn a_missing_or_empty_bucket_is_404() {
        let (addr, rx, registry) = start_counted(|i| i, 16).await;
        for path in ["/api/v2/write?org=acme", "/api/v2/write?org=acme&bucket="] {
            let response = post(&addr, path, "", b"m f=1").await;
            assert!(response.starts_with("HTTP/1.1 404"), "{path}: {response}");
            assert_eq!(body_of(&response), r#"{"code":"not found","message":"bucket not found"}"#);
        }
        assert!(rx.is_empty());
        assert_eq!(
            Totals::of(registry.drain(0))
                .sum("logit.input.requests.rejected", &[("reason", "no_bucket")]),
            2.0
        );
    }

    #[tokio::test]
    async fn a_missing_org_and_org_id_is_400() {
        let (addr, rx) = start_default().await;
        for path in ["/api/v2/write?bucket=b", "/api/v2/write?bucket=b&org=&orgID="] {
            let response = post(&addr, path, "", b"m f=1").await;
            assert!(response.starts_with("HTTP/1.1 400"), "{path}: {response}");
            assert_eq!(
                body_of(&response),
                r#"{"code":"invalid","message":"Please provide either orgID or org"}"#
            );
        }
        assert!(rx.is_empty());
    }

    /// `org`, `orgID`, and `bucket` become resource attributes, each only when given.
    #[tokio::test]
    async fn the_resource_carries_org_org_id_and_bucket() {
        let (addr, mut rx) = start_default().await;
        let cases: [(&str, &[(&str, &str)]); 3] = [
            (
                "org=acme&bucket=b",
                &[("_logit.influxdb.org", "acme"), ("_logit.influxdb.bucket", "b")],
            ),
            (
                "orgID=0123456789abcdef&bucket=b",
                &[("_logit.influxdb.org_id", "0123456789abcdef"), ("_logit.influxdb.bucket", "b")],
            ),
            (
                "org=acme&orgID=01&bucket=b%20x&org=other",
                &[
                    ("_logit.influxdb.org", "acme"),
                    ("_logit.influxdb.org_id", "01"),
                    ("_logit.influxdb.bucket", "b x"),
                ],
            ),
        ];
        for (query, expected) in cases {
            let response = post(&addr, &format!("/api/v2/write?{query}"), "", b"m f=1").await;
            assert!(response.starts_with("HTTP/1.1 204"), "{query}: {response}");
            let batch = recv_batch(&mut rx).await;
            let attrs = &batch.resource.attributes;
            assert_eq!(attrs.len(), expected.len(), "{query}");
            for (key, value) in expected {
                assert_eq!(str_attr(attrs, key), Some(*value), "{query}: {key}");
            }
        }
    }

    /// Requests naming the same org and bucket share one `Arc<Resource>`, so a downstream batch
    /// accumulator merges them; another bucket gets its own.
    #[tokio::test]
    async fn requests_naming_one_org_and_bucket_share_a_resource() {
        let (addr, mut rx) = start_default().await;
        let mut resources = Vec::new();
        for path in [WRITE, WRITE, "/api/v2/write?org=acme&bucket=other"] {
            post(&addr, path, "", b"m f=1").await;
            resources.push(recv_batch(&mut rx).await.resource);
        }
        assert!(Arc::ptr_eq(&resources[0], &resources[1]), "one pair, one resource");
        assert!(!Arc::ptr_eq(&resources[0], &resources[2]), "another bucket, another resource");
    }

    #[test]
    fn the_resource_cache_clears_when_full() {
        let cache = ResourceCache::new(2);
        let key = |bucket: &str| (Some("o".to_string()), None, bucket.to_string());
        let a = cache.get(key("a"));
        let b = cache.get(key("b"));
        assert!(Arc::ptr_eq(&a, &cache.get(key("a"))));
        assert!(Arc::ptr_eq(&b, &cache.get(key("b"))));
        let c = cache.get(key("c"));
        assert_eq!(cache.entries.lock().unwrap().len(), 1, "cleared, then holds the new entry");
        assert!(Arc::ptr_eq(&c, &cache.get(key("c"))));
        assert!(!Arc::ptr_eq(&a, &cache.get(key("a"))), "a was evicted with the clear");
    }

    #[test]
    fn the_query_string_is_percent_decoded_and_first_wins() {
        let q = Query::parse(Some("bucket=a%2Fb&bucket=c&org=o&precision=ms&rp=x"));
        assert_eq!(q.bucket.as_deref(), Some("a/b"));
        assert_eq!(q.org.as_deref(), Some("o"));
        assert_eq!(q.org_id, None);
        assert_eq!(q.precision, Some(Precision::Ms));
        let q = Query::parse(None);
        assert_eq!((q.bucket, q.org, q.precision), (None, None, Some(Precision::Ns)));
        assert_eq!(Query::parse(Some("precision=x")).precision, None);
        assert_eq!(Query::parse(Some("bucket=&bucket=b")).bucket, None, "the first, empty, wins");
    }

    // ---- encoding and size ----------------------------------------------------------------------

    #[tokio::test]
    async fn a_gzip_body_decodes() {
        let (addr, mut rx) = start_default().await;
        let response = post(&addr, WRITE, "Content-Encoding: gzip\r\n", &gzip(TWO_POINTS)).await;
        assert!(response.starts_with("HTTP/1.1 204"), "{response}");
        assert_eq!(recv_batch(&mut rx).await.events.len(), 2);
        let response = post(&addr, WRITE, "Content-Encoding: identity\r\n", TWO_POINTS).await;
        assert!(response.starts_with("HTTP/1.1 204"), "{response}");
        assert_eq!(recv_batch(&mut rx).await.events.len(), 2);
    }

    /// Only lowercase `gzip` is gzip, as influxd matches it; anything else but identity is `415`.
    #[tokio::test]
    async fn any_other_encoding_is_415() {
        let (addr, rx, registry) = start_counted(|i| i, 16).await;
        for encoding in ["GZIP", "Gzip", "deflate", "zstd", "br"] {
            let headers = format!("Content-Encoding: {encoding}\r\n");
            let response = post(&addr, WRITE, &headers, &gzip(TWO_POINTS)).await;
            assert!(response.starts_with("HTTP/1.1 415"), "{encoding}: {response}");
            assert_eq!(
                body_of(&response),
                r#"{"code":"unsupported media type","message":"Content-Encoding must be gzip or identity"}"#
            );
        }
        assert!(rx.is_empty());
        assert_eq!(
            Totals::of(registry.drain(0))
                .sum("logit.input.requests.rejected", &[("reason", "encoding")]),
            5.0
        );
    }

    #[tokio::test]
    async fn a_body_labeled_gzip_that_is_not_is_400() {
        let (addr, rx) = start_default().await;
        let response = post(&addr, WRITE, "Content-Encoding: gzip\r\n", b"m f=1").await;
        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
        assert!(body_of(&response).starts_with(r#"{"code":"invalid","#), "{response}");
        assert!(rx.is_empty());
    }

    /// The three `413` paths: a declared length over the cap, a chunked body over it, and a gzip
    /// body inflating past it.
    #[tokio::test]
    async fn every_oversize_path_is_413() {
        let (addr, rx, registry) = start_counted(|i| i.with_max_request_bytes(1024), 16).await;
        let big = vec![b'\n'; 1025];

        let response = post(&addr, WRITE, "", &big).await;
        assert!(response.starts_with("HTTP/1.1 413"), "declared: {response}");
        assert_eq!(
            body_of(&response),
            r#"{"code":"request too large","message":"request body exceeds the 1024-byte limit"}"#
        );

        let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let head = format!(
            "POST {WRITE} HTTP/1.1\r\nHost: {addr}\r\nTransfer-Encoding: chunked\r\n\
             Connection: close\r\n\r\n{:x}\r\n",
            big.len()
        );
        stream.write_all(head.as_bytes()).await.unwrap();
        let _ = stream.write_all(&big).await;
        let _ = stream.write_all(b"\r\n0\r\n\r\n").await;
        let mut buf = Vec::new();
        let _ = tokio::time::timeout(
            logit_pipeline::test_util::RECV_TIMEOUT,
            stream.read_to_end(&mut buf),
        )
        .await;
        let response = String::from_utf8_lossy(&buf);
        assert!(response.starts_with("HTTP/1.1 413"), "streamed: {response}");

        let compressed = gzip(&vec![b'\n'; 4096]);
        assert!(compressed.len() < 1024);
        let response = post(&addr, WRITE, "Content-Encoding: gzip\r\n", &compressed).await;
        assert!(response.starts_with("HTTP/1.1 413"), "decompressed: {response}");

        assert!(rx.is_empty());
        assert_eq!(
            Totals::of(registry.drain(0))
                .sum("logit.input.requests.rejected", &[("reason", "oversize")]),
            3.0
        );
    }

    /// A body that stops arriving is `408` once `idle_timeout` bounds each frame.
    #[tokio::test]
    async fn a_stalled_body_is_408() {
        let (addr, _rx) = start(
            InfluxDbInput::new("127.0.0.1:0").with_idle_timeout(Some(Duration::from_millis(200))),
            16,
        )
        .await;
        let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let head = format!("POST {WRITE} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: 100\r\n\r\n");
        stream.write_all(head.as_bytes()).await.unwrap();
        stream.write_all(b"cpu f=").await.unwrap();
        let mut buf = Vec::new();
        let _ = tokio::time::timeout(
            logit_pipeline::test_util::RECV_TIMEOUT,
            stream.read_to_end(&mut buf),
        )
        .await;
        let response = String::from_utf8_lossy(&buf);
        assert!(response.starts_with("HTTP/1.1 408"), "{response}");
        assert!(body_of(&response).starts_with(r#"{"code":"invalid","#), "{response}");
    }

    // ---- authentication -------------------------------------------------------------------------

    #[tokio::test]
    async fn tokens_accept_token_and_bearer_and_refuse_anything_else() {
        let (addr, mut rx, registry) =
            start_counted(|i| i.with_tokens(vec!["other".to_string(), TOKEN.to_string()]), 16)
                .await;
        for header in [
            format!("Authorization: Token {TOKEN}\r\n"),
            format!("Authorization: Bearer {TOKEN}\r\n"),
            format!("Authorization: token {TOKEN}\r\n"),
            format!("Authorization: BEARER  {TOKEN} \r\n"),
        ] {
            let response = post(&addr, WRITE, &header, b"m f=1").await;
            assert!(response.starts_with("HTTP/1.1 204"), "{header:?}: {response}");
            recv_batch(&mut rx).await;
        }
        let basic = format!(
            "Authorization: Basic {}\r\n",
            base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                format!("u:{TOKEN}")
            )
        );
        for header in [
            String::new(),
            "Authorization: Token wrong\r\n".to_string(),
            "Authorization: Token \r\n".to_string(),
            format!("Authorization: {TOKEN}\r\n"),
            format!("Authorization: Splunk {TOKEN}\r\n"),
            basic,
        ] {
            let response = post(&addr, WRITE, &header, b"m f=1").await;
            assert!(response.starts_with("HTTP/1.1 401"), "{header:?}: {response}");
            assert_eq!(
                body_of(&response),
                r#"{"code":"unauthorized","message":"unauthorized access"}"#
            );
        }
        let response = post(&addr, &format!("{WRITE}&token={TOKEN}"), "", b"m f=1").await;
        assert!(response.starts_with("HTTP/1.1 401"), "a query token: {response}");
        assert!(rx.is_empty());
        let totals = Totals::of(registry.drain(0));
        assert_eq!(totals.sum("logit.input.requests.rejected", &[("reason", "auth")]), 7.0);
    }

    #[tokio::test]
    async fn no_tokens_accepts_any_request() {
        let (addr, mut rx) = start_default().await;
        let response = post(&addr, WRITE, "Authorization: Token anything\r\n", b"m f=1").await;
        assert!(response.starts_with("HTTP/1.1 204"), "{response}");
        recv_batch(&mut rx).await;
    }

    // ---- routes ---------------------------------------------------------------------------------

    /// `/ping` and `/health` answer on `GET` and `HEAD` with no token, even with tokens set.
    #[tokio::test]
    async fn ping_and_health_need_no_token() {
        let (addr, _rx) =
            start(InfluxDbInput::new("127.0.0.1:0").with_tokens(vec![TOKEN.to_string()]), 16).await;
        for method in ["GET", "HEAD"] {
            let response = request_raw(&addr, method, "/ping", "", b"").await;
            assert!(response.starts_with("HTTP/1.1 204"), "{method} /ping: {response}");
            assert!(has_header(&response, &format!("x-influxdb-version: {VERSION}")), "{response}");
            let response = request_raw(&addr, method, "/health", "", b"").await;
            assert!(response.starts_with("HTTP/1.1 200"), "{method} /health: {response}");
            if method == "GET" {
                let expected = format!(
                    r#"{{"name":"influxdb","message":"ready for queries and writes","status":"pass","checks":[],"version":"logit/{}","commit":""}}"#,
                    env!("CARGO_PKG_VERSION")
                );
                assert_eq!(body_of(&response), expected);
            } else {
                assert_eq!(body_of(&response), "", "a HEAD response has no body");
            }
        }
    }

    #[tokio::test]
    async fn an_unknown_path_is_404_and_a_wrong_method_is_405_with_allow() {
        let (addr, _rx, registry) =
            start_counted(|i| i.with_tokens(vec![TOKEN.to_string()]), 16).await;
        for path in ["/write?db=x", "/api/v2/write/", "/api/v2/query", "/api/v2/delete", "/"] {
            let response = post(&addr, path, "", b"m f=1").await;
            assert!(response.starts_with("HTTP/1.1 404"), "{path}: {response}");
            assert_eq!(body_of(&response), r#"{"code":"not found","message":"path not found"}"#);
            assert!(has_header(&response, &format!("x-influxdb-version: {VERSION}")), "{response}");
        }
        for method in ["GET", "PUT", "DELETE"] {
            let response = request_raw(&addr, method, WRITE, "", b"").await;
            assert!(response.starts_with("HTTP/1.1 405"), "{method}: {response}");
            assert!(has_header(&response, "allow: OPTIONS, POST"), "{response}");
            assert_eq!(
                body_of(&response),
                r#"{"code":"method not allowed","message":"method not allowed"}"#
            );
        }
        let response = request_raw(&addr, "POST", "/ping", "", b"").await;
        assert!(response.starts_with("HTTP/1.1 405"), "{response}");
        assert!(has_header(&response, "allow: GET, HEAD, OPTIONS"), "{response}");
        let response = request_raw(&addr, "OPTIONS", WRITE, "", b"").await;
        assert!(response.starts_with("HTTP/1.1 204"), "OPTIONS needs no token: {response}");
        assert!(has_header(&response, "allow: OPTIONS, POST"), "{response}");

        let totals = Totals::of(registry.drain(0));
        assert_eq!(totals.sum("logit.input.requests", &[("route", "unknown")]), 5.0);
        assert_eq!(totals.sum("logit.input.requests.rejected", &[("reason", "method")]), 4.0);
    }

    // ---- backpressure ---------------------------------------------------------------------------

    /// A one-slot channel nothing drains: the first write fills it, the second waits
    /// `busy_after` and is answered `503` with `Retry-After`, delivering nothing.
    #[tokio::test]
    async fn a_full_downstream_is_503_with_retry_after() {
        let (addr, mut rx, registry) =
            start_counted(|i| i.with_busy_after(Duration::from_millis(200)), 1).await;
        let response = post(&addr, WRITE, "", b"m f=1 1").await;
        assert!(response.starts_with("HTTP/1.1 204"), "{response}");
        let started = Instant::now();
        let response = post(&addr, WRITE, "", b"m f=2 2").await;
        assert!(response.starts_with("HTTP/1.1 503"), "{response}");
        assert!(has_header(&response, "retry-after: 1"), "{response}");
        assert_eq!(
            body_of(&response),
            r#"{"code":"unavailable","message":"the pipeline is busy; retry the write"}"#
        );
        assert!(started.elapsed() >= Duration::from_millis(200));

        assert_eq!(recv_batch(&mut rx).await.events[0].timestamp, 1);
        assert!(rx.try_recv().is_err(), "the 503'd batch was never delivered");
        let totals = Totals::of(registry.drain(0));
        assert_eq!(totals.sum("logit.input.requests", &[("class", "busy")]), 1.0);
        assert_eq!(totals.sum("logit.input.batches.deferred", &[]), 1.0);
        assert!(
            !totals.has("logit.input.batches.dropped", &[]),
            "a busy 503 is deferred, not lost"
        );
    }

    #[tokio::test]
    async fn a_closed_consumer_is_503_and_counted() {
        let (addr, rx, registry) = start_counted(|i| i, 16).await;
        drop(rx);
        let response = post(&addr, WRITE, "", b"m f=1").await;
        assert!(response.starts_with("HTTP/1.1 503"), "{response}");
        assert!(has_header(&response, "retry-after: 1"), "{response}");
        let totals = Totals::of(registry.drain(0));
        assert_eq!(
            totals.sum("logit.input.batches.dropped", &[("reason", "closed_consumer")]),
            1.0
        );
        assert_eq!(totals.sum("logit.input.requests", &[("class", "closed_consumer")]), 1.0);
        assert_eq!(totals.sum("logit.input.requests", &[("class", "busy")]), 0.0);
        assert!(!totals.has("logit.input.batches.deferred", &[]), "a closed consumer is loss");
    }

    // ---- telemetry and connections --------------------------------------------------------------

    #[tokio::test]
    async fn requests_bytes_and_reserved_tags_are_counted() {
        let (addr, mut rx, registry) = start_counted(|i| i, 16).await;
        let body = b"m,_logit.x=y,host=a f=1";
        post(&addr, WRITE, "", body).await;
        let batch = recv_batch(&mut rx).await;
        assert_eq!(batch.events[0].attributes.get("_logit.x"), None);
        request_raw(&addr, "GET", "/ping", "", b"").await;
        request_raw(&addr, "GET", "/health", "", b"").await;
        let totals = Totals::of(registry.drain(0));
        assert_eq!(totals.sum("logit.input.requests", &[("route", "write"), ("class", "ok")]), 1.0);
        assert_eq!(totals.sum("logit.input.requests", &[("route", "ping")]), 1.0);
        assert_eq!(totals.sum("logit.input.requests", &[("route", "health")]), 1.0);
        assert_eq!(totals.sum("logit.input.request.bytes", &[]), body.len() as f64);
        assert_eq!(totals.sum("logit.input.tags.omitted", &[("reason", "reserved_prefix")]), 1.0);
        assert!(!totals.has("logit.input.tags.dropped", &[]), "the point is delivered");
    }

    #[tokio::test]
    async fn a_connection_past_the_cap_is_dropped_and_counted() {
        let (addr, _rx, registry) = start_counted(|i| i.with_max_connections(1), 16).await;
        let mut first = tokio::net::TcpStream::connect(&addr).await.unwrap();
        first.write_all(b"P").await.unwrap();
        // The accept loop takes a connection's permit as it accepts it, one accept at a time and in
        // the kernel queue's order, so `first` holds it before `second` is accepted.
        let mut second = tokio::net::TcpStream::connect(&addr).await.unwrap();
        logit_pipeline::test_util::expect_closed(&mut second, "the past-the-cap connection").await;
        assert_eq!(
            Totals::of(registry.drain(0))
                .sum("logit.input.connections.rejected", &[("reason", "limit")]),
            1.0
        );
        drop(first);
    }

    #[tokio::test]
    async fn a_second_bind_is_a_no_op() {
        let mut input = InfluxDbInput::new("127.0.0.1:0");
        assert_eq!(input.local_addr(), None);
        input.bind().await.unwrap();
        let addr = input.local_addr().unwrap();
        input.bind().await.unwrap();
        assert_eq!(input.local_addr(), Some(addr));
    }

    #[test]
    fn defaults_match_the_documented_constants() {
        let input = InfluxDbInput::new("127.0.0.1:0");
        assert_eq!(input.busy_after, Duration::from_secs(5));
        assert_eq!(input.handshake_timeout, Duration::from_secs(5));
        assert_eq!(input.max_request_bytes, 25 * 1024 * 1024);
        assert_eq!(input.max_connections, crate::DEFAULT_MAX_CONNECTIONS);
        assert_eq!(input.idle_timeout, None);
        assert!(input.tokens.is_empty());
    }

    // ---- TLS ------------------------------------------------------------------------------------

    fn testdata_dir() -> std::path::PathBuf {
        // The repo root's `testdata/tls` (`testdata/tls/README.md`).
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    fn tls_settings() -> TlsServerSettings {
        TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: None,
        }
    }

    fn tls_connector() -> tokio_rustls::TlsConnector {
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
        tokio_rustls::TlsConnector::from(Arc::new(config))
    }

    #[tokio::test]
    async fn a_write_over_tls_reaches_the_fanout() {
        let input = InfluxDbInput::new("127.0.0.1:0")
            .with_tokens(vec![TOKEN.to_string()])
            .with_tls(&tls_settings(), &testdata_dir(), &logit_pipeline::tls::TlsReloader::new())
            .unwrap();
        let (addr, mut rx) = start(input, 16).await;
        let stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let server_name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
        let mut tls = tls_connector().connect(server_name, stream).await.unwrap();
        let request = format!(
            "POST {WRITE} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\n\
             Connection: close\r\nAuthorization: Token {TOKEN}\r\n\r\n",
            TWO_POINTS.len()
        );
        tls.write_all(request.as_bytes()).await.unwrap();
        tls.write_all(TWO_POINTS).await.unwrap();
        let mut buf = Vec::new();
        let _ = tokio::time::timeout(
            logit_pipeline::test_util::RECV_TIMEOUT,
            tls.read_to_end(&mut buf),
        )
        .await;
        let response = String::from_utf8_lossy(&buf);
        assert!(response.starts_with("HTTP/1.1 204"), "{response}");
        assert_eq!(recv_batch(&mut rx).await.events.len(), 2);
    }

    // ---- shutdown -------------------------------------------------------------------------------
    //
    // Every test here goes through `spawn_input` and `Running`, so the listener runs
    // `run_until_shutdown` with a real signal.

    /// The close grace (`handshake_timeout`, reused) for this section, short so a close that
    /// spends the whole grace returns well inside `Running::stop`'s 5s ceiling.
    const SHUTDOWN_GRACE: Duration = Duration::from_millis(100);

    /// Far past any wait in this section, so a batch parked on a full edge stays parked rather
    /// than being answered `503`.
    const PARKED_BUSY_AFTER: Duration = Duration::from_secs(30);

    /// A running listener whose one consumer is a capacity-1 edge carrying its own telemetry, so
    /// a send parked on it counts `logit.component.inbox.full` the moment it parks.
    async fn shutdown_listener(
        input: InfluxDbInput,
    ) -> (
        String,
        logit_pipeline::test_util::Running,
        mpsc::Receiver<logit_pipeline::Delivered>,
        logit_pipeline::test_util::TelemetryProbe,
    ) {
        let probe = logit_pipeline::test_util::TelemetryProbe::new();
        let mut input = input
            .with_telemetry(probe.telemetry("influxdb_in", "influxdb_in", "listener"))
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

    fn plaintext_shutdown_input() -> InfluxDbInput {
        InfluxDbInput::new("127.0.0.1:0").with_handshake_timeout(SHUTDOWN_GRACE)
    }

    /// One write on an open stream with no `Connection: close`, so hyper waits for the next
    /// request rather than closing.
    async fn write_keep_alive(stream: &mut tokio::net::TcpStream, addr: &str, body: &[u8]) {
        let request = format!(
            "POST {WRITE} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
    }

    /// Every `Fanout` clone is gone once the listener has returned: the inbox reads closed.
    async fn expect_inbox_closed(rx: &mut mpsc::Receiver<logit_pipeline::Delivered>) {
        let next = tokio::time::timeout(logit_pipeline::test_util::RECV_TIMEOUT, rx.recv())
            .await
            .expect("the inbox should close once the listener returned");
        assert!(next.is_none(), "no batch is left after the ones the test drained");
    }

    #[tokio::test]
    async fn shutdown_closes_an_idle_keep_alive_connection_and_the_listener_returns() {
        let (addr, running, mut rx, _probe) = shutdown_listener(plaintext_shutdown_input()).await;
        let mut keep_alive = tokio::net::TcpStream::connect(&addr).await.unwrap();
        write_keep_alive(&mut keep_alive, &addr, b"m f=1").await;
        let response = crate::http::read_response(&mut keep_alive, "the keep-alive POST").await;
        assert!(response.starts_with("HTTP/1.1 204"), "got: {response}");
        recv_batch(&mut rx).await;

        running.stop().await;
        logit_pipeline::test_util::expect_closed(&mut keep_alive, "a keep-alive connection").await;
        expect_inbox_closed(&mut rx).await;
    }

    /// A write parked on a full downstream when shutdown fires is served out: the batch reaches
    /// the consumer, the client gets its `204`, then the connection closes and the listener
    /// returns.
    #[tokio::test]
    async fn shutdown_serves_a_parked_write_out_then_closes_its_connection() {
        let (addr, running, mut rx, mut probe) =
            shutdown_listener(plaintext_shutdown_input()).await;
        let mut client = tokio::net::TcpStream::connect(&addr).await.unwrap();
        write_keep_alive(&mut client, &addr, b"m f=1").await;
        let response =
            crate::http::read_response(&mut client, "the POST that fills the slot").await;
        assert!(response.starts_with("HTTP/1.1 204"), "got: {response}");
        write_keep_alive(&mut client, &addr, b"m f=2").await;
        probe
            .wait_for("the second write to park on the full edge", |t| {
                t.sum("logit.component.inbox.full", &[]) >= 1.0
            })
            .await;

        running.shutdown.send(true).unwrap();
        for _ in 0..2 {
            recv_batch(&mut rx).await;
        }
        let response = crate::http::read_response(&mut client, "the parked POST").await;
        assert!(response.starts_with("HTTP/1.1 204"), "got: {response}");
        logit_pipeline::test_util::expect_closed(&mut client, "the parked write's connection")
            .await;
        tokio::time::timeout(logit_pipeline::test_util::RECV_TIMEOUT, running.handle)
            .await
            .expect("the listener should return once its connection closed")
            .expect("the listener task should not panic")
            .expect("the listener should return Ok");
        expect_inbox_closed(&mut rx).await;
    }

    /// A connection still in its TLS accept when shutdown fires ends at once rather than at its
    /// hour-long `handshake_timeout`.
    #[tokio::test]
    async fn shutdown_ends_a_connection_still_in_its_tls_accept() {
        let (addr, running, _rx, mut probe) = shutdown_listener(
            InfluxDbInput::new("127.0.0.1:0")
                .with_tls(
                    &tls_settings(),
                    &testdata_dir(),
                    &logit_pipeline::tls::TlsReloader::new(),
                )
                .unwrap()
                .with_handshake_timeout(Duration::from_secs(3600)),
        )
        .await;
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

    // ---- sender address -------------------------------------------------------------------------

    /// A v2 `PROXY` over TCP/IPv4 from 203.0.113.5:41000.
    fn v2_ipv4_header() -> Vec<u8> {
        let mut header = logit_proto::proxy::V2_SIGNATURE.to_vec();
        header.extend_from_slice(&[0x21, 0x11, 0x00, 0x0C]);
        header.extend_from_slice(&[203, 0, 113, 5, 127, 0, 0, 1]);
        header.extend_from_slice(&41000u16.to_be_bytes());
        header.extend_from_slice(&8086u16.to_be_bytes());
        header
    }

    /// Connects, writes `prefix`, then one write carrying `headers`. Returns the response and the
    /// client's local port.
    async fn write_after(addr: &str, prefix: &[u8], headers: &str) -> (String, u16) {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let port = stream.local_addr().unwrap().port();
        let body = b"m f=1\nn f=2\n";
        let mut wire = prefix.to_vec();
        wire.extend_from_slice(
            format!(
                "POST {WRITE} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\n\
                 Connection: close\r\n{headers}\r\n",
                body.len()
            )
            .as_bytes(),
        );
        wire.extend_from_slice(body);
        let _ = stream.write_all(&wire).await;
        let mut buf = Vec::new();
        let _ = tokio::time::timeout(
            logit_pipeline::test_util::RECV_TIMEOUT,
            stream.read_to_end(&mut buf),
        )
        .await;
        (String::from_utf8_lossy(&buf).into_owned(), port)
    }

    /// Every event of `batch` carries the expected `network.peer.*` and `client.*`.
    fn expect_stamped(
        batch: &EventBatch,
        peer_port: Option<u16>,
        client: Option<(&str, Option<i64>)>,
    ) {
        assert_eq!(batch.events.len(), 2);
        for event in &batch.events {
            let attrs = &event.attributes;
            match peer_port {
                Some(port) => {
                    assert_eq!(str_attr(attrs, "network.peer.address"), Some("127.0.0.1"));
                    assert_eq!(attrs.get("network.peer.port"), Some(&Value::I64(i64::from(port))));
                }
                None => assert_eq!(attrs.get("network.peer.address"), None),
            }
            assert_eq!(str_attr(attrs, "client.address"), client.map(|(a, _)| a));
            assert_eq!(
                attrs.get("client.port"),
                client.and_then(|(_, p)| p).map(Value::I64).as_ref()
            );
        }
    }

    #[tokio::test]
    async fn peer_stamps_the_socket_peer() {
        for peer in [true, false] {
            let (addr, mut rx) = start(InfluxDbInput::new("127.0.0.1:0").with_peer(peer), 16).await;
            let (response, port) = write_after(&addr, b"", "").await;
            assert!(response.starts_with("HTTP/1.1 204"), "got: {response}");
            expect_stamped(&recv_batch(&mut rx).await, peer.then_some(port), None);
        }
    }

    #[tokio::test]
    async fn a_proxy_header_stamps_the_client_and_forwarded_replaces_it() {
        let input = InfluxDbInput::new("127.0.0.1:0").with_peer(true).with_proxy_protocol(true);
        let (addr, mut rx) = start(input, 16).await;
        let (response, port) = write_after(&addr, &v2_ipv4_header(), "").await;
        assert!(response.starts_with("HTTP/1.1 204"), "got: {response}");
        expect_stamped(&recv_batch(&mut rx).await, Some(port), Some(("203.0.113.5", Some(41000))));

        let input = InfluxDbInput::new("127.0.0.1:0")
            .with_peer(true)
            .with_proxy_protocol(true)
            .with_forwarded(Some(ForwardedHeader::XForwardedFor));
        let (addr, mut rx) = start(input, 16).await;
        let (response, port) =
            write_after(&addr, &v2_ipv4_header(), "X-Forwarded-For: 203.0.113.9\r\n").await;
        assert!(response.starts_with("HTTP/1.1 204"), "got: {response}");
        expect_stamped(&recv_batch(&mut rx).await, Some(port), Some(("203.0.113.9", None)));
    }

    #[tokio::test]
    async fn a_connection_without_a_proxy_header_is_rejected_and_counted() {
        let probe = logit_pipeline::test_util::TelemetryProbe::new();
        let input = InfluxDbInput::new("127.0.0.1:0")
            .with_telemetry(probe.telemetry("influx", "influxdb_in", "listener"))
            .with_proxy_protocol(true);
        let mut probe = probe;
        let (addr, rx) = start(input, 16).await;
        let mut bare = tokio::net::TcpStream::connect(&addr).await.unwrap();
        bare.write_all(
            format!("POST {WRITE} HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nm f=1")
                .as_bytes(),
        )
        .await
        .unwrap();
        logit_pipeline::test_util::expect_closed(&mut bare, "a request with no PROXY header").await;
        probe
            .wait_for("the proxy_header rejection", |t| {
                t.sum("logit.input.connections.rejected", &[("reason", "proxy_header")]) == 1.0
            })
            .await;
        assert!(rx.is_empty(), "nothing is delivered");
    }
}
