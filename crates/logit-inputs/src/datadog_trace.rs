//! `datadog_trace_in`: a stand-in for the Datadog Agent's APM receiver, what a dd-trace tracer's
//! `DD_TRACE_AGENT_URL` (or `DD_AGENT_HOST` and `DD_TRACE_AGENT_PORT`) points at
//! ([ADR `datadog-agent-and-intake-relay`](../../../../docs/adr/datadog-agent-and-intake-relay.md),
//! [`docs/plans/datadog-relay.md`](../../../../docs/plans/datadog-relay.md) §2). It serves
//! HTTP/1.1 and h2c through [`hyper_util::server::conn::auto::Builder`] on a TCP listener
//! (`bind`, the Agent's `:8126`, optionally TLS), a Unix stream socket (`socket`, the Agent's
//! `receiver_socket`), or both at once, and every body decodes through
//! [`logit_proto::datadog::DatadogDecoder`]. The payload mappings live in that codec's module doc;
//! this module owns HTTP: routing, the tracer headers, size caps, the `/info` document, and
//! backpressure.
//!
//! Structured as `datadog_in` ([`crate::datadog`]) is, with the same accept loop, connection cap,
//! handshake and idle timeouts, and request helpers from [`crate::http`]. Where this module is
//! silent, `datadog_in`'s module doc applies.
//!
//! # Routes
//!
//! | Request | Handling | Response |
//! |---|---|---|
//! | `POST`/`PUT /v0.3/traces`, `/v0.4/traces` | `decode_traces_v04`; a `Content-Type: application/json` body is `415` | the trace reply (below) |
//! | `POST`/`PUT /v0.5/traces` | `decode_traces_v05` | the trace reply |
//! | `POST`/`PUT /v0.7/traces` | `decode_tracer_payload_v07` | the trace reply |
//! | `POST`/`PUT /v0.6/stats` | `decode_client_stats_v06` | `200` `{}` |
//! | `GET /info` | none: a static document (below) | `200` |
//! | `/v0.1/traces`, `/v0.2/traces`, `/v1.0/traces`, `/v0.1/pipeline_stats`, `/telemetry/proxy/*`, `/v0.7/config`, any method | none | `404` |
//! | `POST`/`PUT /evp_proxy/v1`–`v4/*`, `/profiling/v1/input`, `/debugger/v1/input`, `/debugger/v1/diagnostics`, `/debugger/v2/input`, `/symdb/v1/input`, `/dogstatsd/v1/proxy`, `/dogstatsd/v2/proxy`, `/tracer_flare/v1`, `/openlineage/api/v1/lineage` | none: read within the cap, then discarded | `200` `{}` |
//! | another method on a path above | none | `405` + `Allow` |
//! | any other path | none | `404` |
//!
//! Tracers send `PUT` as well as `POST` (dd-trace-java uses `PUT`), so every data route takes
//! both. The Agent itself answers any method on these routes; this listener is stricter, and says
//! so with a `405`.
//!
//! **The `404` routes are what an Agent with the feature turned off answers**, and `/info` doesn't
//! list them, so a tracer that reads `/info` never turns them on: no telemetry forwarding, no
//! Remote Configuration, no data-streams pipeline stats, and no v1.0 string-table form (a known
//! gap: the codec doesn't implement it). Each is counted by route, so a tracer that tries one
//! anyway shows up.
//!
//! **The `200` stubs carry data this relay doesn't carry**: profiles, dynamic-instrumentation
//! snapshots, symbol uploads, flares, `evp_proxy` passthroughs. Each body is read within
//! [`MAX_REQUEST_BYTES`] and discarded, and counted `logit.input.requests.acknowledged{route}`.
//! Answering them keeps a tracer from logging an error per upload; nothing in `/info` asks for
//! them, but several tracers send them regardless of what `/info` says.
//!
//! # The trace reply
//!
//! Every trace route answers `200`, `Content-Type: application/json`, with the header
//! `Datadog-Rates-Payload-Version: logit-1` and the body
//! `{"rate_by_service":{"service:,env:":1.0}}`: the default rate for every service, and a rate of
//! 1.0, which a tracer's priority sampler reads as "keep everything". This listener samples
//! nothing, and asking the tracer to sample would make the relay see less than the application
//! produced. When the request already carries `Datadog-Rates-Payload-Version: logit-1` the body is
//! `{}`, the Agent's own shortcut for "your rates haven't changed".
//!
//! # `/info`
//!
//! A tracer reads `/info` once at startup (and periodically, by language) to decide which features
//! the Agent supports. This listener's document is static except `receiver_port` and
//! `receiver_socket`, and each field is chosen for what it makes a tracer do:
//!
//! - `version` `logit/<version>` and an empty `git_commit`: identifies the stand-in in a
//!   tracer's debug logs.
//! - `endpoints`: the routes this listener decodes (`/v0.3`, `/v0.4`, `/v0.5`, and
//!   `/v0.7/traces`, `/v0.6/stats`, `/info`). `/v0.6/stats` is listed so a tracer that computes
//!   client-side stats keeps sending them; they relay losslessly to a real Agent. The telemetry
//!   proxy, `/v0.7/config`, `/v0.1/pipeline_stats`, and `/v1.0/traces` are left out so a tracer
//!   never enables telemetry forwarding, Remote Configuration, data-streams stats, or the v1.0
//!   form.
//! - `client_drop_p0s: false`: a tracer must not drop priority-0 traces client-side. The relay has
//!   to see every span (plan §14): a tracer that dropped them would leave its client stats as the
//!   only record of those spans.
//! - `span_meta_structs: true` and `span_events: true`: the codec carries `meta_struct` and native
//!   span events, so a tracer may send them rather than flattening them into `meta`.
//! - `long_running_spans: false`: partial flushes of an unfinished span aren't something the
//!   downstream Agent is known to reassemble when they arrive through a relay, so a tracer isn't
//!   asked to send them.
//! - `evp_proxy_allowed_headers`, `peer_tags`, and `span_kinds_stats_computed` empty, and
//!   `obfuscation_version: 0`: no `evp_proxy` passthrough, no peer-tag stats aggregation, no
//!   span-kind stats, and no Agent-side obfuscation for a tracer to rely on (plan §14). A tracer
//!   that obfuscates on its own keeps doing so.
//! - `config`: the Agent's own defaults (`target_tps` 10, `max_eps` 200, `connection_limit`
//!   1024, `receiver_timeout` 5, `max_request_bytes` 25 MiB, `statsd_port` 8125), with every
//!   obfuscation switch off because nothing here obfuscates, and the listener's own
//!   `receiver_port` (0 without `bind`) and `receiver_socket` (empty without `socket`).
//!
//! # Tracer headers
//!
//! v0.3, v0.4, and v0.5 carry no `TracerPayload`, so what a tracer says about itself arrives only
//! in request headers. `Datadog-Meta-Lang`, `-Lang-Version`, `-Lang-Interpreter`,
//! `-Lang-Interpreter-Vendor`, `-Tracer-Version`, `Datadog-Container-ID`, `Datadog-Entity-ID`,
//! `Datadog-Client-Computed-Top-Level`, `Datadog-Client-Computed-Stats`, and
//! `Datadog-Client-Dropped-P0-{Traces,Spans}` become `datadog.tracer.*` batch resource attributes,
//! per the codec's Traces table. On v0.7 the payload's own fields win: a header fills a field only
//! where the payload left it empty. `datadog_trace_out` restores the headers from the same
//! attributes. `/v0.6/stats` takes none of them: its `ClientStatsPayload` carries the tracer's
//! identity itself, and the stats encoder has no field for the rest.
//!
//! `X-Datadog-Trace-Count` is compared with the number of traces on the wire (v0.3/v0.4's trace
//! arrays, v0.5's, v0.7's chunks). A mismatch is the throttled diagnostic `trace_count_mismatch`,
//! not a rejection: the body is what gets relayed, and the header only says what the tracer meant
//! to send.
//!
//! # Request handling
//!
//! In order, after the connection-level steps `datadog_in` also takes:
//!
//! 1. **Route and method**, as the routes table.
//! 2. **Size.** A `Content-Length` over [`MAX_REQUEST_BYTES`] (25 MiB, the Agent's
//!    `max_request_bytes`) is a `413` before any byte is read, and the body is read through
//!    [`Limited`] at the same cap. No API key is checked: tracers send none.
//! 3. **`Content-Encoding`.** `identity` (or none) or `gzip`, else `415`. Tracers don't compress,
//!    and the Agent accepts gzip. The decompressed size is capped at the same 25 MiB.
//! 4. **Decode.** `CodecError::Malformed` is a `400`.
//! 5. **Delivery**, bounded (below), then the route's `200`. A body that decodes to no events is
//!    answered without a send.
//!
//! # Backpressure: a short bounded wait, then `503`, which a tracer treats as loss
//!
//! Delivery works as `datadog_in`'s does: the request's one batch is sent through
//! [`Fanout::send_with_deadline`], reaching every downstream consumer or none, under a
//! [`BUSY_AFTER`] deadline, and a request that misses it is answered `503` with
//! `Retry-After: 1`, counted `logit.input.requests{class="busy"}` and
//! `logit.input.batches.dropped{reason="busy"}`. Unlike `datadog_in`, that `503` is loss here, not
//! deferral, and `BUSY_AFTER` is shorter (2 s, not 5 s) — see
//! [ADR `datadog-agent-and-intake-relay`](../../../../docs/adr/datadog-agent-and-intake-relay.md),
//! decision 11.
//!
//! # The Unix socket
//!
//! `socket:` binds a Unix stream socket, the Agent's `receiver_socket`
//! (`/var/run/datadog/apm.socket` by default), which a tracer reaches with
//! `DD_TRACE_AGENT_URL=unix:///var/run/datadog/apm.socket`. [`Input::bind`] refuses a path whose
//! directory doesn't exist, replaces a stale socket file left by an earlier run (what the Agent does
//! at startup), and refuses a path that exists and isn't a socket, so a typo can't delete a
//! regular file. The socket file is then made mode `0666`, so a tracer running as any user can
//! connect: the Agent's DogStatsD socket uses `0722`, and its APM socket's mode is UNVERIFIED.
//! Restrict access with the directory's permissions. The socket file isn't removed on shutdown;
//! the next start replaces it.
//!
//! Both listeners share one connection cap and one handler. A Unix connection has no TLS and no
//! peer address; its diagnostics name the socket path.
//!
//! # Telemetry
//!
//! Every name and tag is `&'static`. Per request: `logit.input.requests{route, class}` (class `ok`,
//! `rejected`, or `busy`; route one of [`Route::name`], or `unknown`), `logit.input.request.duration`
//! (timing, every exit), and `logit.input.request.bytes` (the compressed body size, once read).
//! Rejections: `logit.input.requests.rejected{reason}`, reason `unknown_route`, `unsupported_route`
//! (also tagged `route`), `method`, `oversize`, `encoding`, `json_traces`, `malformed_encoding`,
//! `malformed`, `stalled`, or `body_read`. `logit.input.requests.acknowledged{route}` counts a
//! stub's upload, `logit.input.batches.dropped{reason="busy"}` a batch a `503` left undelivered,
//! and `logit.input.spans` the spans delivered. The connection metrics are `otlp_in`'s, with one
//! difference: the kernel accept-queue gauges (`crate::tcp::AcceptQueueSampler`) cover the TCP
//! listener only, since they read `TCP_INFO`, which a Unix socket has no counterpart for.

use crate::http::{
    body_read_error_message, collect_with_stall_bound, declared_length, decompress,
    deliver_with_deadline, drive_with_idle, error_response, is_length_limit, json_response,
    media_type, now_nanos, Activity, BodyReadError, DecompressError, Encoding, MediaType,
};
use crate::Input;
use anyhow::Context as _;
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use http_body_util::{Full, Limited};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use logit_core::{Diagnostics, EventBatch, Resource, Telemetry, Value};
use logit_pipeline::Fanout;
use logit_proto::datadog::traces::RESOURCE_ATTR_TRACER_LANGUAGE_VERSION;
use logit_proto::datadog::{
    DatadogDecoder, HEADER_CLIENT_COMPUTED_STATS, HEADER_CLIENT_COMPUTED_TOP_LEVEL,
    HEADER_CLIENT_DROPPED_P0_SPANS, HEADER_CLIENT_DROPPED_P0_TRACES, HEADER_CONTAINER_ID,
    HEADER_ENTITY_ID, HEADER_META_LANG, HEADER_META_LANG_INTERPRETER,
    HEADER_META_LANG_INTERPRETER_VENDOR, HEADER_META_LANG_VERSION, HEADER_META_TRACER_VERSION,
    HEADER_TRACE_COUNT, RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_STATS,
    RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_TOP_LEVEL, RESOURCE_ATTR_TRACER_CONTAINER_ID,
    RESOURCE_ATTR_TRACER_DROPPED_P0_SPANS, RESOURCE_ATTR_TRACER_DROPPED_P0_TRACES,
    RESOURCE_ATTR_TRACER_ENTITY_ID, RESOURCE_ATTR_TRACER_LANGUAGE_INTERPRETER,
    RESOURCE_ATTR_TRACER_LANGUAGE_INTERPRETER_VENDOR, RESOURCE_ATTR_TRACER_LANGUAGE_NAME,
    RESOURCE_ATTR_TRACER_VERSION,
};
use logit_proto::msgpack::{Reader, Type};
use logit_proto::CodecError;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, UnixListener, UnixStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_rustls::TlsAcceptor;

/// The cap on a request's body, compressed and decompressed alike: 25 MiB, the Agent's own
/// `max_request_bytes` default. A denial-of-service bound, not a tuning knob, as on `datadog_in`.
const MAX_REQUEST_BYTES: usize = 25 * 1024 * 1024;

/// Bounds the connections [`Input::run`] serves at once, across the TCP listener and the Unix
/// socket together: the same 1024 as `datadog_in` and the Agent's own `connection_limit`.
const MAX_CONCURRENT_CONNECTIONS: usize = 1024;

/// Default for [`DatadogTraceInput::with_handshake_timeout`]: the same 5s as every other TCP
/// listener. Also the grace an idle close gives hyper.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// How long one request's delivery may wait on a full downstream before it is answered `503`
/// (this module's "Backpressure" section): short enough to answer inside a tracer's typical 2s
/// write timeout, measured from when the body has been read.
const BUSY_AFTER: Duration = Duration::from_secs(2);

/// The header naming the version of the sampling rates a reply carries, in both directions.
const RATES_PAYLOAD_VERSION_HEADER: &str = "datadog-rates-payload-version";

/// The one rates version this listener ever answers with: its rates never change.
const RATES_PAYLOAD_VERSION: &str = "logit-1";

/// Every service's default rate, 1.0: keep everything (this module's "The trace reply").
const RATE_BY_SERVICE: &[u8] = br#"{"rate_by_service":{"service:,env:":1.0}}"#;

/// The Unix socket file's mode after binding (this module's "The Unix socket").
const SOCKET_MODE: u32 = 0o666;

/// `crate::tls::TlsServerSettings`, re-exported as `datadog_in`'s is.
pub use crate::tls::TlsServerSettings;

/// The `datadog_trace_in` listener. See this module's doc.
pub struct DatadogTraceInput {
    bind: Option<String>,
    socket: Option<PathBuf>,
    diag: Diagnostics,
    telemetry: Telemetry,
    tls: Option<Arc<rustls::ServerConfig>>,
    /// Set by [`Input::bind`], taken by [`Input::run`]. `None` after a run, so a second run
    /// rebinds.
    listener: Option<TcpListener>,
    /// As `listener`, for `socket`.
    unix_listener: Option<UnixListener>,
    handshake_timeout: Duration,
    /// `None`, the default, means no idle timeout.
    idle_timeout: Option<Duration>,
    max_connections: usize,
    busy_after: Duration,
}

impl Default for DatadogTraceInput {
    fn default() -> Self {
        Self::new()
    }
}

impl DatadogTraceInput {
    /// A listener with neither a TCP address nor a socket path: give it one or both with
    /// [`Self::with_bind`] and [`Self::with_socket`] before binding.
    pub fn new() -> Self {
        Self {
            bind: None,
            socket: None,
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
            tls: None,
            listener: None,
            unix_listener: None,
            handshake_timeout: HANDSHAKE_TIMEOUT,
            idle_timeout: None,
            max_connections: MAX_CONCURRENT_CONNECTIONS,
            busy_after: BUSY_AFTER,
        }
    }

    /// Serves a TCP listener on `bind` (`host:port`; the Agent's is `:8126`).
    pub fn with_bind(mut self, bind: impl Into<String>) -> Self {
        self.bind = Some(bind.into());
        self
    }

    /// Serves a Unix stream socket at `path` (this module's "The Unix socket").
    pub fn with_socket(mut self, path: impl Into<PathBuf>) -> Self {
        self.socket = Some(path.into());
        self
    }

    /// The TCP address bound, once [`Input::bind`] has run, so a caller can learn the
    /// OS-assigned port without a bind-drop-rebind race.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.listener.as_ref().and_then(|l| l.local_addr().ok())
    }

    /// The configured Unix socket path, if any.
    pub fn socket_path(&self) -> Option<&Path> {
        self.socket.as_deref()
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Turns on TLS termination on the TCP listener (`tls:` in config); the Unix socket is always
    /// plaintext. Paths in `settings` resolve against `base_dir`, the config file's directory.
    pub fn with_tls(
        mut self,
        settings: &TlsServerSettings,
        base_dir: &Path,
    ) -> anyhow::Result<Self> {
        let alpn: &[&[u8]] = &[b"h2", b"http/1.1"];
        self.tls = Some(Arc::new(crate::tls::build_server_config(settings, base_dir, alpn)?));
        Ok(self)
    }

    /// Overrides [`HANDSHAKE_TIMEOUT`] for the TLS accept and the first-byte wait
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

    /// Test-only override of [`MAX_CONCURRENT_CONNECTIONS`].
    #[cfg(test)]
    fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Overrides [`BUSY_AFTER`]. A test/tuning hook, not a config field, as on `datadog_in`.
    pub fn with_busy_after(mut self, d: Duration) -> Self {
        self.busy_after = d;
        self
    }
}

#[async_trait::async_trait]
impl Input for DatadogTraceInput {
    /// Binds whichever of the TCP listener and the Unix socket is configured and not yet bound.
    async fn bind(&mut self) -> anyhow::Result<()> {
        if self.bind.is_none() && self.socket.is_none() {
            anyhow::bail!("datadog_trace_in needs a 'bind' address, a 'socket' path, or both");
        }
        if let Some(bind) = &self.bind {
            if self.listener.is_none() {
                let listener = TcpListener::bind(bind).await?;
                self.diag.info("bound", format_args!("listening on {bind}"));
                self.listener = Some(listener);
            }
        }
        if let Some(path) = &self.socket {
            if self.unix_listener.is_none() {
                let listener = bind_unix(path)?;
                self.diag.info("bound", format_args!("listening on {}", path.display()));
                self.unix_listener = Some(listener);
            }
        }
        Ok(())
    }

    /// `datadog_in`'s accept loop, once per configured listener, both under one connection cap.
    /// Returns only when an accept fails.
    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        self.bind().await?;
        let tcp = self.listener.take();
        let unix = self.unix_listener.take();
        let receiver_port = tcp.as_ref().and_then(|l| l.local_addr().ok()).map_or(0, |a| a.port());
        let receiver_socket =
            self.socket.as_ref().map(|p| p.display().to_string()).unwrap_or_default();
        let accept = Arc::new(AcceptContext {
            sink,
            telemetry: self.telemetry.clone(),
            diag: self.diag.clone(),
            connection_limit: Arc::new(Semaphore::new(self.max_connections)),
            live_connections: AtomicI64::new(0),
            handshake_timeout: self.handshake_timeout,
            idle_timeout: self.idle_timeout,
            busy_after: self.busy_after,
            info: Bytes::from(info_document(receiver_port, &receiver_socket)),
        });
        let tls_acceptor = self.tls.clone().map(TlsAcceptor::from);
        let socket_path: Option<Arc<Path>> = self.socket.as_deref().map(Arc::from);

        let tcp_loop = {
            let accept = Arc::clone(&accept);
            async move {
                match tcp {
                    Some(listener) => accept_tcp(listener, accept, tls_acceptor).await,
                    None => std::future::pending().await,
                }
            }
        };
        let unix_loop = async move {
            match (unix, socket_path) {
                (Some(listener), Some(path)) => accept_unix(listener, path, accept).await,
                _ => std::future::pending().await,
            }
        };
        tokio::select! {
            result = tcp_loop => result,
            result = unix_loop => result,
        }
    }
}

/// Binds the Unix socket at `path` (this module's "The Unix socket").
fn bind_unix(path: &Path) -> anyhow::Result<UnixListener> {
    use std::os::unix::fs::{FileTypeExt, PermissionsExt};
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        if !parent.is_dir() {
            anyhow::bail!(
                "datadog_trace_in: can't bind the Unix socket {}: its directory {} does not \
                 exist (create it first; the Agent's is /var/run/datadog)",
                path.display(),
                parent.display()
            );
        }
    }
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_socket() => std::fs::remove_file(path)
            .with_context(|| format!("removing the stale socket file {}", path.display()))?,
        Ok(_) => anyhow::bail!(
            "datadog_trace_in: {} exists and is not a socket; refusing to replace it",
            path.display()
        ),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).with_context(|| format!("inspecting {}", path.display()));
        }
    }
    let listener = UnixListener::bind(path)
        .with_context(|| format!("binding the Unix socket {}", path.display()))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(SOCKET_MODE))
        .with_context(|| format!("setting {}'s mode to {SOCKET_MODE:o}", path.display()))?;
    Ok(listener)
}

/// What both accept loops share, built once per [`Input::run`].
struct AcceptContext {
    sink: Fanout,
    telemetry: Telemetry,
    diag: Diagnostics,
    /// One cap across both listeners.
    connection_limit: Arc<Semaphore>,
    live_connections: AtomicI64,
    handshake_timeout: Duration,
    idle_timeout: Option<Duration>,
    busy_after: Duration,
    /// The `/info` body, rendered once.
    info: Bytes,
}

impl AcceptContext {
    /// A connection permit, or `None` (counted) at the cap: a connection past it is rejected, not
    /// queued.
    fn permit(&self) -> Option<OwnedSemaphorePermit> {
        let permit = Arc::clone(&self.connection_limit).try_acquire_owned().ok();
        if permit.is_none() {
            self.telemetry.count("logit.input.connections.rejected", 1.0, &[("reason", "limit")]);
        }
        permit
    }

    fn shared(&self, peer: Peer) -> Arc<Shared> {
        Arc::new(Shared {
            sink: self.sink.clone(),
            telemetry: self.telemetry.clone(),
            diag: self.diag.clone(),
            busy_after: self.busy_after,
            info: self.info.clone(),
            peer,
        })
    }

    /// Runs one connection on its own task: the live-connection gauge around it, the permit held
    /// for its lifetime, and a failure reported through the throttled `connection_error`.
    fn spawn(
        self: &Arc<Self>,
        permit: OwnedSemaphorePermit,
        connection: impl Future<Output = Result<(), String>> + Send + 'static,
    ) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let _permit = permit; // released on drop
            let live = this.live_connections.fetch_add(1, Ordering::Relaxed) + 1;
            this.telemetry.gauge("logit.input.connections", live as f64, &[]);
            let result = connection.await;
            let live = this.live_connections.fetch_sub(1, Ordering::Relaxed) - 1;
            this.telemetry.gauge("logit.input.connections", live as f64, &[]);
            if let Err(err) = result {
                this.diag.clone().warn_throttled("connection_error", err);
            }
        });
    }
}

/// The TCP accept loop: `datadog_in`'s, including the accept-queue gauges, the TLS handshake or
/// plaintext first-byte peek bounded inside the spawned task, and a clean close before the first
/// byte treated as a health check.
async fn accept_tcp(
    listener: TcpListener,
    accept: Arc<AcceptContext>,
    tls_acceptor: Option<TlsAcceptor>,
) -> anyhow::Result<()> {
    let mut accept_queue =
        crate::tcp::AcceptQueueSampler::new(accept.telemetry.clone(), accept.diag.clone());
    loop {
        let (stream, peer) = accept_queue.accept(&listener).await?;
        let Some(permit) = accept.permit() else {
            drop(stream);
            continue;
        };
        let shared = accept.shared(Peer::Tcp(peer));
        let tls_acceptor = tls_acceptor.clone();
        let handshake_timeout = accept.handshake_timeout;
        let idle_timeout = accept.idle_timeout;
        accept.spawn(permit, async move {
            match tls_acceptor {
                Some(acceptor) => {
                    match tokio::time::timeout(handshake_timeout, acceptor.accept(stream)).await {
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
                        tokio::time::timeout(handshake_timeout, stream.peek(&mut [0u8; 1])).await;
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
            }
        });
    }
}

/// The Unix-socket accept loop: the TCP loop's plaintext arm, with no accept-queue gauge (this
/// module's "Telemetry").
async fn accept_unix(
    listener: UnixListener,
    path: Arc<Path>,
    accept: Arc<AcceptContext>,
) -> anyhow::Result<()> {
    loop {
        let (stream, _addr) = listener.accept().await?;
        let Some(permit) = accept.permit() else {
            drop(stream);
            continue;
        };
        let shared = accept.shared(Peer::Unix(Arc::clone(&path)));
        let handshake_timeout = accept.handshake_timeout;
        let idle_timeout = accept.idle_timeout;
        accept.spawn(permit, async move {
            match tokio::time::timeout(handshake_timeout, has_first_byte(&stream)).await {
                Ok(Ok(false)) => Ok(()), // a health-check probe, not a fault
                Ok(Ok(true)) => {
                    serve_connection(TokioIo::new(stream), shared, idle_timeout, handshake_timeout)
                        .await
                }
                Ok(Err(err)) => Err(format!("waiting for a first byte failed: {err}")),
                Err(_elapsed) => {
                    Err(format!("no first byte received within {handshake_timeout:?}"))
                }
            }
        });
    }
}

/// `TcpStream::peek` for a Unix stream, which tokio doesn't offer: `false` when the peer closed
/// before sending anything. `MSG_PEEK` through `socket2`, under `try_io` so a spurious wakeup
/// clears the readiness it reported rather than spinning.
async fn has_first_byte(stream: &UnixStream) -> std::io::Result<bool> {
    loop {
        stream.readable().await?;
        let mut buf = [std::mem::MaybeUninit::<u8>::uninit(); 1];
        match stream
            .try_io(tokio::io::Interest::READABLE, || socket2::SockRef::from(stream).peek(&mut buf))
        {
            Ok(n) => return Ok(n > 0),
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(err) => return Err(err),
        }
    }
}

/// Who sent a request, for diagnostic text only, never a tag.
enum Peer {
    Tcp(SocketAddr),
    /// A Unix peer has no address worth naming; the socket's path says which listener it was.
    Unix(Arc<Path>),
}

impl fmt::Display for Peer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcp(addr) => write!(f, "{addr}"),
            Self::Unix(path) => write!(f, "unix:{}", path.display()),
        }
    }
}

/// What every request on one connection needs, built once per connection.
struct Shared {
    sink: Fanout,
    telemetry: Telemetry,
    diag: Diagnostics,
    busy_after: Duration,
    info: Bytes,
    peer: Peer,
}

/// Serves one accepted (and, with TLS on, handshaken) connection to completion: `datadog_in`'s,
/// with this listener's handler, over TCP, TLS, or a Unix stream alike.
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

/// One APM receiver route: a row of this module's routes table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Route {
    /// `/v0.3/traces` and `/v0.4/traces`, one decoder; the name tells them apart in telemetry.
    TracesV04(&'static str),
    TracesV05,
    TracesV07,
    StatsV06,
    Info,
    /// Answered `404`, as an Agent with the feature off answers; the name is the `route` tag.
    Unsupported(&'static str),
    /// Read, answered `200` `{}`, and discarded; the name is the `route` tag.
    Acknowledged(&'static str),
}

impl Route {
    fn from_path(path: &str) -> Option<Self> {
        Some(match path {
            "/v0.3/traces" => Self::TracesV04("traces_v03"),
            "/v0.4/traces" => Self::TracesV04("traces_v04"),
            "/v0.5/traces" => Self::TracesV05,
            "/v0.7/traces" => Self::TracesV07,
            "/v0.6/stats" => Self::StatsV06,
            "/info" => Self::Info,
            "/v0.1/traces" => Self::Unsupported("traces_v01"),
            "/v0.2/traces" => Self::Unsupported("traces_v02"),
            "/v1.0/traces" => Self::Unsupported("traces_v10"),
            "/v0.1/pipeline_stats" => Self::Unsupported("pipeline_stats"),
            "/v0.7/config" => Self::Unsupported("remote_config"),
            "/profiling/v1/input" => Self::Acknowledged("profiling"),
            "/debugger/v1/input" => Self::Acknowledged("debugger_v1_input"),
            "/debugger/v1/diagnostics" => Self::Acknowledged("debugger_v1_diagnostics"),
            "/debugger/v2/input" => Self::Acknowledged("debugger_v2_input"),
            "/symdb/v1/input" => Self::Acknowledged("symdb"),
            "/dogstatsd/v1/proxy" => Self::Acknowledged("dogstatsd_v1_proxy"),
            "/dogstatsd/v2/proxy" => Self::Acknowledged("dogstatsd_v2_proxy"),
            "/tracer_flare/v1" => Self::Acknowledged("tracer_flare"),
            "/openlineage/api/v1/lineage" => Self::Acknowledged("openlineage"),
            _ if under(path, "/telemetry/proxy") => Self::Unsupported("telemetry_proxy"),
            _ if under(path, "/evp_proxy/v1") => Self::Acknowledged("evp_proxy_v1"),
            _ if under(path, "/evp_proxy/v2") => Self::Acknowledged("evp_proxy_v2"),
            _ if under(path, "/evp_proxy/v3") => Self::Acknowledged("evp_proxy_v3"),
            _ if under(path, "/evp_proxy/v4") => Self::Acknowledged("evp_proxy_v4"),
            _ => return None,
        })
    }

    /// The `route` tag on this listener's request counters.
    fn name(self) -> &'static str {
        match self {
            Self::TracesV04(name) | Self::Unsupported(name) | Self::Acknowledged(name) => name,
            Self::TracesV05 => "traces_v05",
            Self::TracesV07 => "traces_v07",
            Self::StatsV06 => "stats_v06",
            Self::Info => "info",
        }
    }

    fn is_traces(self) -> bool {
        matches!(self, Self::TracesV04(_) | Self::TracesV05 | Self::TracesV07)
    }

    fn allows(self, method: &Method) -> bool {
        match self {
            Self::Info => method == Method::GET,
            _ => method == Method::POST || method == Method::PUT,
        }
    }

    fn allow_header(self) -> &'static str {
        match self {
            Self::Info => "GET",
            _ => "POST, PUT",
        }
    }
}

/// `path` is `prefix` or lies under it: the Agent registers these as `ServeMux` subtrees.
fn under(path: &str, prefix: &str) -> bool {
    path.strip_prefix(prefix).is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
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
    if let Route::Unsupported(_) = route {
        shared.telemetry.count(
            "logit.input.requests.rejected",
            1.0,
            &[("reason", "unsupported_route"), ("route", name)],
        );
        return (name, REJECTED, error_response(StatusCode::NOT_FOUND, "Not found"));
    }
    if !route.allows(req.method()) {
        let mut response =
            reject(shared, "method", StatusCode::METHOD_NOT_ALLOWED, "Method not allowed", false);
        response
            .headers_mut()
            .insert(http::header::ALLOW, HeaderValue::from_static(route.allow_header()));
        return (name, REJECTED, response);
    }
    if route == Route::Info {
        return (name, OK, json_response(StatusCode::OK, shared.info.clone()));
    }
    if declared_length(req.headers()).is_some_and(|len| len > MAX_REQUEST_BYTES as u64) {
        let message = format!("request body exceeds the {MAX_REQUEST_BYTES}-byte limit");
        let response = reject(shared, "oversize", StatusCode::PAYLOAD_TOO_LARGE, &message, true);
        return (name, REJECTED, response);
    }
    let acknowledged = matches!(route, Route::Acknowledged(_));
    // A stub's body is discarded unread, so its encoding doesn't matter.
    let encoding = if acknowledged {
        Encoding::Identity
    } else {
        match Encoding::from_headers(req.headers()) {
            Ok(encoding @ (Encoding::Identity | Encoding::Gzip)) => encoding,
            Ok(_) | Err(_) => {
                let sent = req
                    .headers()
                    .get(http::header::CONTENT_ENCODING)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("");
                let message = format!(
                    "unsupported Content-Encoding {sent:?} -- this input speaks identity and gzip"
                );
                let response =
                    reject(shared, "encoding", StatusCode::UNSUPPORTED_MEDIA_TYPE, &message, true);
                return (name, REJECTED, response);
            }
        }
    };
    if matches!(route, Route::TracesV04(_)) && media_type(req.headers()) == MediaType::Json {
        let message = "JSON trace bodies aren't supported -- send application/msgpack";
        let response =
            reject(shared, "json_traces", StatusCode::UNSUPPORTED_MEDIA_TYPE, message, true);
        return (name, REJECTED, response);
    }

    let (parts, body) = req.into_parts();
    let body = match collect_with_stall_bound(Limited::new(body, MAX_REQUEST_BYTES), stall).await {
        Ok(body) => body,
        // `otlp_in`'s `408`: the client's clock, not its size, and the connection closes once
        // this response is out.
        Err(BodyReadError::Stalled(stall)) => {
            activity.request_close();
            let message = format!("request body stalled for {stall:?}");
            let response = reject(shared, "stalled", StatusCode::REQUEST_TIMEOUT, &message, true);
            return (name, REJECTED, response);
        }
        Err(BodyReadError::Failed(err)) => {
            let reason = if is_length_limit(err.as_ref()) { "oversize" } else { "body_read" };
            let message = body_read_error_message(err.as_ref());
            let response = reject(shared, reason, StatusCode::PAYLOAD_TOO_LARGE, &message, true);
            return (name, REJECTED, response);
        }
    };
    shared.telemetry.count("logit.input.request.bytes", body.len() as f64, &[]);

    if acknowledged {
        shared.telemetry.count("logit.input.requests.acknowledged", 1.0, &[("route", name)]);
        return (name, OK, json_response(StatusCode::OK, Bytes::from_static(b"{}")));
    }

    let body = match decompress(encoding, body, MAX_REQUEST_BYTES) {
        Ok(body) => body,
        Err(DecompressError::TooLarge) => {
            let message =
                format!("decompressed request exceeds the {MAX_REQUEST_BYTES}-byte limit");
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

    let received_at = now_nanos();
    // Built per request, as on `datadog_in`.
    let mut decoder = DatadogDecoder::new()
        .with_telemetry(shared.telemetry.clone())
        .with_diagnostics(shared.diag.clone());
    let decoded: Result<EventBatch, CodecError> = match route {
        Route::TracesV04(_) => decoder.decode_traces_v04(&body, received_at),
        Route::TracesV05 => decoder.decode_traces_v05(&body, received_at),
        Route::TracesV07 => decoder.decode_tracer_payload_v07(&body, received_at),
        Route::StatsV06 => decoder.decode_client_stats_v06(&body, received_at),
        Route::Info | Route::Unsupported(_) | Route::Acknowledged(_) => {
            unreachable!("answered above")
        }
    };
    let mut batch = match decoded {
        Ok(batch) => batch,
        Err(err) => {
            let response =
                reject(shared, "malformed", StatusCode::BAD_REQUEST, &err.to_string(), true);
            return (name, REJECTED, response);
        }
    };
    if route.is_traces() {
        apply_tracer_headers(&mut batch, &parts.headers, &shared.diag);
        check_trace_count(route, &body, &parts.headers, shared);
    }
    let success = success(route, &parts.headers);
    if batch.events.is_empty() {
        return (name, OK, success);
    }

    let spans = batch.events.iter().filter(|event| event.span.is_some()).count();
    match deliver_with_deadline(&shared.sink, vec![batch], shared.busy_after).await {
        Ok(()) => {
            if spans > 0 {
                shared.telemetry.count("logit.input.spans", spans as f64, &[]);
            }
            (name, OK, success)
        }
        Err(not_sent) => {
            shared.telemetry.count(
                "logit.input.batches.dropped",
                not_sent as f64,
                &[("reason", "busy")],
            );
            shared.diag.clone().warn_throttled(
                "busy",
                format_args!(
                    "datadog_trace_in: answered 503 to {}: the pipeline did not accept a batch \
                     within {:?}, and a tracer does not retry, so the payload is lost",
                    shared.peer, shared.busy_after
                ),
            );
            let mut response = error_response(StatusCode::SERVICE_UNAVAILABLE, "busy");
            response.headers_mut().insert(http::header::RETRY_AFTER, HeaderValue::from_static("1"));
            (name, BUSY, response)
        }
    }
}

/// A decoding route's `200`: the trace reply (this module's "The trace reply") or the stats
/// route's `{}`.
fn success(route: Route, request_headers: &HeaderMap) -> http::Response<Full<Bytes>> {
    if !route.is_traces() {
        return json_response(StatusCode::OK, Bytes::from_static(b"{}"));
    }
    let current = request_headers
        .get(RATES_PAYLOAD_VERSION_HEADER)
        .is_some_and(|v| v.as_bytes() == RATES_PAYLOAD_VERSION.as_bytes());
    let body: &'static [u8] = if current { b"{}" } else { RATE_BY_SERVICE };
    let mut response = json_response(StatusCode::OK, Bytes::from_static(body));
    response
        .headers_mut()
        .insert(RATES_PAYLOAD_VERSION_HEADER, HeaderValue::from_static(RATES_PAYLOAD_VERSION));
    response
}

/// The tracer headers' `Str` carriers: header, then attribute.
const STR_HEADERS: [(&str, &str); 7] = [
    (HEADER_META_LANG, RESOURCE_ATTR_TRACER_LANGUAGE_NAME),
    (HEADER_META_LANG_VERSION, RESOURCE_ATTR_TRACER_LANGUAGE_VERSION),
    (HEADER_META_LANG_INTERPRETER, RESOURCE_ATTR_TRACER_LANGUAGE_INTERPRETER),
    (HEADER_META_LANG_INTERPRETER_VENDOR, RESOURCE_ATTR_TRACER_LANGUAGE_INTERPRETER_VENDOR),
    (HEADER_META_TRACER_VERSION, RESOURCE_ATTR_TRACER_VERSION),
    (HEADER_CONTAINER_ID, RESOURCE_ATTR_TRACER_CONTAINER_ID),
    (HEADER_ENTITY_ID, RESOURCE_ATTR_TRACER_ENTITY_ID),
];

/// The tracer headers' `U64` carriers: header, then attribute.
const U64_HEADERS: [(&str, &str); 2] = [
    (HEADER_CLIENT_DROPPED_P0_TRACES, RESOURCE_ATTR_TRACER_DROPPED_P0_TRACES),
    (HEADER_CLIENT_DROPPED_P0_SPANS, RESOURCE_ATTR_TRACER_DROPPED_P0_SPANS),
];

/// Copies the tracer headers into `batch`'s resource, each only where the resource has no value
/// for its attribute: on v0.3–v0.5 the resource starts empty, and on v0.7 the payload's own
/// fields win (this module's "Tracer headers").
fn apply_tracer_headers(batch: &mut EventBatch, headers: &HeaderMap, diag: &Diagnostics) {
    let text = |name: &str| {
        headers
            .get(name)
            .and_then(|v| std::str::from_utf8(v.as_bytes()).ok())
            .map(str::trim)
            .filter(|v| !v.is_empty())
    };
    let mut fills: Vec<(&'static str, Value)> = Vec::new();
    for (header, attr) in STR_HEADERS {
        if let Some(value) = text(header) {
            fills.push((attr, Value::str(value)));
        }
    }
    // The Agent reads any non-empty value as "set" for top-level, and Go's `strconv.ParseBool`
    // falses as "not set" for stats; either way the attribute exists only when set.
    if text(HEADER_CLIENT_COMPUTED_TOP_LEVEL).is_some() {
        fills.push((RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_TOP_LEVEL, Value::Bool(true)));
    }
    if text(HEADER_CLIENT_COMPUTED_STATS)
        .is_some_and(|v| !matches!(v, "0" | "f" | "F" | "false" | "FALSE" | "False"))
    {
        fills.push((RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_STATS, Value::Bool(true)));
    }
    for (header, attr) in U64_HEADERS {
        let Some(value) = text(header) else { continue };
        match value.parse::<u64>() {
            Ok(n) => fills.push((attr, Value::U64(n))),
            Err(_) => {
                diag.clone().warn_throttled(
                    "bad_header",
                    format_args!(
                        "datadog_trace_in: ignoring {header}: {value:?} isn't an unsigned integer"
                    ),
                );
            }
        }
    }
    fills.retain(|(attr, _)| batch.resource.attributes.get(attr).is_none());
    if fills.is_empty() {
        return;
    }
    let resource: &mut Resource = Arc::make_mut(&mut batch.resource);
    for (attr, value) in fills {
        resource.attributes.insert(attr, value);
    }
}

/// Compares `X-Datadog-Trace-Count` with the number of traces on the wire, and reports a
/// mismatch through the throttled `trace_count_mismatch` diagnostic (this module's "Tracer
/// headers"). The wire count is read only when the header is present.
fn check_trace_count(route: Route, body: &[u8], headers: &HeaderMap, shared: &Shared) {
    let Some(declared) = headers.get(HEADER_TRACE_COUNT) else {
        return;
    };
    let declared = declared.to_str().ok().and_then(|v| v.trim().parse::<usize>().ok());
    let on_wire = wire_trace_count(route, body);
    if let (Some(declared), Some(on_wire)) = (declared, on_wire) {
        if declared != on_wire {
            shared.diag.clone().warn_throttled(
                "trace_count_mismatch",
                format_args!(
                    "datadog_trace_in: {} declared X-Datadog-Trace-Count {declared} but sent \
                     {on_wire} traces; relaying what was sent",
                    shared.peer
                ),
            );
        }
    }
}

/// The number of traces `body` holds: v0.3/v0.4's and v0.5's trace arrays, v0.7's chunks. `None`
/// when the structure can't be read that far, which the decoder has already judged.
fn wire_trace_count(route: Route, body: &[u8]) -> Option<usize> {
    let mut r = Reader::new(body);
    match route {
        Route::TracesV04(_) => r.read_array_len().ok(),
        Route::TracesV05 => {
            if r.read_array_len().ok()? != 2 {
                return None;
            }
            let dict = r.read_array_len().ok()?;
            for _ in 0..dict {
                r.skip_value().ok()?;
            }
            r.read_array_len().ok()
        }
        Route::TracesV07 => {
            let fields = r.read_map_len().ok()?;
            let mut chunks = 0;
            for _ in 0..fields {
                let key = match r.peek_type().ok()? {
                    Type::Bin => r.read_bin().ok()?,
                    _ => r.read_str_bytes().ok()?,
                };
                if key == b"chunks" {
                    chunks = match r.peek_type().ok()? {
                        Type::Nil => {
                            r.read_nil().ok()?;
                            0
                        }
                        _ => r.read_array_len().ok()?,
                    };
                    // Walked rather than stopped at: a repeated `chunks` key replaces the first,
                    // as it does in the decoder.
                    for _ in 0..chunks {
                        r.skip_value().ok()?;
                    }
                } else {
                    r.skip_value().ok()?;
                }
            }
            Some(chunks)
        }
        _ => None,
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
            format_args!("datadog_trace_in: rejecting a request from {}: {message}", shared.peer),
        );
    }
    error_response(status, message)
}

/// The `/info` document (this module's "`/info`"), with the two values that depend on the
/// listener filled in.
fn info_document(receiver_port: u16, receiver_socket: &str) -> String {
    let version = serde_json::to_string(&format!("logit/{}", env!("CARGO_PKG_VERSION")))
        .expect("a string always serializes");
    let receiver_socket =
        serde_json::to_string(receiver_socket).expect("a string always serializes");
    format!(
        concat!(
            r#"{{"version":{version},"git_commit":"","#,
            r#""endpoints":["/v0.3/traces","/v0.4/traces","/v0.5/traces","/v0.7/traces","/v0.6/stats","/info"],"#,
            r#""client_drop_p0s":false,"span_meta_structs":true,"long_running_spans":false,"#,
            r#""span_events":true,"evp_proxy_allowed_headers":[],"#,
            r#""config":{{"default_env":"none","target_tps":10,"max_eps":200,"#,
            r#""receiver_port":{receiver_port},"receiver_socket":{receiver_socket},"#,
            r#""connection_limit":1024,"receiver_timeout":5,"max_request_bytes":26214400,"#,
            r#""statsd_port":8125,"max_memory":0,"max_cpu":0,"analyzed_spans_by_service":{{}},"#,
            r#""obfuscation":{{"elastic_search":false,"mongo":false,"sql_exec_plan":false,"#,
            r#""sql_exec_plan_normalize":false,"#,
            r#""http":{{"remove_query_string":false,"remove_path_digits":false}},"#,
            r#""remove_stack_traces":false,"redis":false,"memcached":false,"#,
            r#""credit_cards":{{"enabled":false,"luhn":false}}}}}},"#,
            r#""peer_tags":[],"span_kinds_stats_computed":[],"obfuscation_version":0}}"#,
        ),
        version = version,
        receiver_port = receiver_port,
        receiver_socket = receiver_socket,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_proto::msgpack::Writer;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::mpsc;

    async fn start(
        input: DatadogTraceInput,
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

    fn tcp() -> DatadogTraceInput {
        DatadogTraceInput::new().with_bind("127.0.0.1:0")
    }

    async fn start_default() -> (String, mpsc::Receiver<logit_pipeline::Delivered>) {
        start(tcp(), 16).await
    }

    fn metered() -> (Arc<logit_core::Registry>, DatadogTraceInput) {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("apm", "datadog_trace_in", "listener");
        (registry, tcp().with_telemetry(telemetry))
    }

    async fn recv_batch(rx: &mut mpsc::Receiver<logit_pipeline::Delivered>) -> EventBatch {
        let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("a batch within 5s")
            .expect("the channel is open");
        logit_pipeline::unwrap_batch(delivered)
    }

    /// `datadog_in`'s `request_raw`: one request on a fresh connection, `Connection: close`,
    /// returning the raw response.
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

    fn header_of<'a>(response: &'a str, name: &str) -> Option<&'a str> {
        let head = response.split_once("\r\n\r\n").map_or(response, |(head, _)| head);
        head.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case(name).then(|| value.trim())
        })
    }

    const MSGPACK: &str = "Content-Type: application/msgpack\r\n";

    /// One span map with `trace_id` and `span_id`, the fields a span needs.
    fn span_map(w: &mut Writer, trace_id: u64, span_id: u64) {
        w.write_map_len(4);
        w.write_str("trace_id");
        w.write_u64(trace_id);
        w.write_str("span_id");
        w.write_u64(span_id);
        w.write_str("name");
        w.write_str("http.request");
        w.write_str("start");
        w.write_i64(1_700_000_000_000_000_000);
    }

    /// v0.4: `traces` trace arrays of one span each.
    fn v04(traces: u64) -> Vec<u8> {
        let mut w = Writer::new();
        w.write_array_len(traces as usize);
        for t in 0..traces {
            w.write_array_len(1);
            span_map(&mut w, t + 1, t + 100);
        }
        w.into_inner()
    }

    /// v0.5: one trace of one span, `[dictionary, traces]`.
    fn v05() -> Vec<u8> {
        let mut w = Writer::new();
        w.write_array_len(2);
        w.write_array_len(2);
        w.write_str("");
        w.write_str("http.request");
        w.write_array_len(1);
        w.write_array_len(1);
        w.write_array_len(12);
        for v in [0u64, 1, 0, 7, 8, 0] {
            w.write_u64(v);
        }
        w.write_i64(1_700_000_000_000_000_000);
        w.write_i64(1_000);
        w.write_i64(0);
        w.write_map_len(0);
        w.write_map_len(0);
        w.write_u64(0);
        w.into_inner()
    }

    /// v0.7: a `TracerPayload` naming its language, with `chunks` chunks of one span each.
    fn v07(language: &str, chunks: u64) -> Vec<u8> {
        let mut w = Writer::new();
        w.write_map_len(2);
        w.write_str("language_name");
        w.write_str(language);
        w.write_str("chunks");
        w.write_array_len(chunks as usize);
        for c in 0..chunks {
            w.write_map_len(2);
            w.write_str("priority");
            w.write_i64(1);
            w.write_str("spans");
            w.write_array_len(1);
            span_map(&mut w, c + 1, c + 100);
        }
        w.into_inner()
    }

    #[tokio::test]
    async fn every_trace_route_answers_the_rate_reply_and_delivers_under_post_and_put() {
        let (addr, mut rx) = start_default().await;
        let cases: [(&str, Vec<u8>); 4] = [
            ("/v0.3/traces", v04(1)),
            ("/v0.4/traces", v04(2)),
            ("/v0.5/traces", v05()),
            ("/v0.7/traces", v07("go", 1)),
        ];
        for method in ["POST", "PUT"] {
            for (path, body) in &cases {
                let response = request_raw(&addr, method, path, MSGPACK, body).await;
                assert!(response.starts_with("HTTP/1.1 200"), "{method} {path}: {response}");
                assert_eq!(body_of(&response), r#"{"rate_by_service":{"service:,env:":1.0}}"#);
                assert_eq!(
                    header_of(&response, "datadog-rates-payload-version"),
                    Some("logit-1"),
                    "{path}"
                );
                assert_eq!(header_of(&response, "content-type"), Some("application/json"));
                let batch = recv_batch(&mut rx).await;
                assert!(!batch.events.is_empty(), "{path}");
                assert!(batch.events.iter().all(|e| e.span.is_some()), "{path}");
            }
        }
    }

    #[tokio::test]
    async fn a_request_already_on_the_current_rates_gets_an_empty_body() {
        let (addr, mut rx) = start_default().await;
        let headers = format!("{MSGPACK}Datadog-Rates-Payload-Version: logit-1\r\n");
        let response = post_raw(&addr, "/v0.4/traces", &headers, &v04(1)).await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert_eq!(body_of(&response), "{}");
        assert_eq!(header_of(&response, "datadog-rates-payload-version"), Some("logit-1"));
        recv_batch(&mut rx).await;

        // Another version still gets the rates.
        let headers = format!("{MSGPACK}Datadog-Rates-Payload-Version: 42\r\n");
        let response = post_raw(&addr, "/v0.4/traces", &headers, &v04(1)).await;
        assert_eq!(body_of(&response), r#"{"rate_by_service":{"service:,env:":1.0}}"#);
    }

    #[tokio::test]
    async fn stats_answers_200_with_an_empty_object() {
        let (addr, mut rx) = start_default().await;
        // An empty `ClientStatsPayload` map: valid, and decodes to no events.
        let response = post_raw(&addr, "/v0.6/stats", MSGPACK, &[0x80]).await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert_eq!(body_of(&response), "{}");
        assert_eq!(header_of(&response, "datadog-rates-payload-version"), None);
        assert!(rx.try_recv().is_err(), "an empty payload sends nothing");
    }

    #[tokio::test]
    async fn info_is_the_documented_json_with_this_listener_s_port() {
        let (addr, _rx) = start_default().await;
        let port: u16 = addr.rsplit(':').next().unwrap().parse().unwrap();
        let response = request_raw(&addr, "GET", "/info", "", b"").await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        let info: serde_json::Value = serde_json::from_str(body_of(&response)).unwrap();
        assert_eq!(
            info["endpoints"],
            serde_json::json!([
                "/v0.3/traces",
                "/v0.4/traces",
                "/v0.5/traces",
                "/v0.7/traces",
                "/v0.6/stats",
                "/info"
            ])
        );
        assert_eq!(info["version"], format!("logit/{}", env!("CARGO_PKG_VERSION")));
        assert_eq!(info["client_drop_p0s"], false);
        assert_eq!(info["span_meta_structs"], true);
        assert_eq!(info["span_events"], true);
        assert_eq!(info["config"]["receiver_port"], port);
        assert_eq!(info["config"]["receiver_socket"], "");
        assert_eq!(info["config"]["max_request_bytes"], MAX_REQUEST_BYTES);
        assert_eq!(info["config"]["obfuscation"]["credit_cards"]["enabled"], false);
        assert_eq!(info["obfuscation_version"], 0);
    }

    #[test]
    fn the_info_document_escapes_the_socket_path() {
        let info: serde_json::Value =
            serde_json::from_str(&info_document(0, r#"/tmp/a "b".sock"#)).unwrap();
        assert_eq!(info["config"]["receiver_socket"], r#"/tmp/a "b".sock"#);
        assert_eq!(info["config"]["receiver_port"], 0);
    }

    #[tokio::test]
    async fn a_wrong_method_is_405_with_allow() {
        let (addr, _rx) = start_default().await;
        let response = request_raw(&addr, "POST", "/info", "", b"").await;
        assert!(response.starts_with("HTTP/1.1 405"), "{response}");
        assert_eq!(header_of(&response, "allow"), Some("GET"));
        let response = request_raw(&addr, "GET", "/v0.4/traces", "", b"").await;
        assert!(response.starts_with("HTTP/1.1 405"), "{response}");
        assert_eq!(header_of(&response, "allow"), Some("POST, PUT"));
    }

    #[tokio::test]
    async fn the_unsupported_routes_are_404_and_counted_by_route() {
        let (registry, input) = metered();
        let (addr, _rx) = start(input, 16).await;
        let cases = [
            ("/v0.1/traces", "traces_v01"),
            ("/v0.2/traces", "traces_v02"),
            ("/v1.0/traces", "traces_v10"),
            ("/v0.1/pipeline_stats", "pipeline_stats"),
            ("/telemetry/proxy/api/v2/apmtelemetry", "telemetry_proxy"),
            ("/v0.7/config", "remote_config"),
        ];
        for (path, _) in cases {
            let response = post_raw(&addr, path, "", b"{}").await;
            assert!(response.starts_with("HTTP/1.1 404"), "{path}: {response}");
        }
        let response = post_raw(&addr, "/nope", "", b"{}").await;
        assert!(response.starts_with("HTTP/1.1 404"), "{response}");

        let events = registry.drain(0);
        for (path, route) in cases {
            assert_eq!(
                sum_of(&events, "logit.input.requests.rejected", &[("route", route)]),
                Some(1.0),
                "{path}"
            );
        }
        assert_eq!(
            sum_of(&events, "logit.input.requests.rejected", &[("reason", "unsupported_route")]),
            Some(cases.len() as f64)
        );
        assert_eq!(
            sum_of(&events, "logit.input.requests.rejected", &[("reason", "unknown_route")]),
            Some(1.0)
        );
        assert_eq!(sum_of(&events, "logit.input.requests", &[("route", "unknown")]), Some(1.0));
    }

    #[tokio::test]
    async fn the_stub_routes_answer_200_count_and_send_nothing() {
        let (registry, input) = metered();
        let (addr, mut rx) = start(input, 16).await;
        let cases = [
            ("/evp_proxy/v1/api/v2/exposures", "evp_proxy_v1"),
            ("/evp_proxy/v2/api/v2/citestcycle", "evp_proxy_v2"),
            ("/evp_proxy/v3/x", "evp_proxy_v3"),
            ("/evp_proxy/v4/api/v2/llmobs", "evp_proxy_v4"),
            ("/profiling/v1/input", "profiling"),
            ("/debugger/v1/input", "debugger_v1_input"),
            ("/debugger/v1/diagnostics", "debugger_v1_diagnostics"),
            ("/debugger/v2/input", "debugger_v2_input"),
            ("/symdb/v1/input", "symdb"),
            ("/dogstatsd/v1/proxy", "dogstatsd_v1_proxy"),
            ("/dogstatsd/v2/proxy", "dogstatsd_v2_proxy"),
            ("/tracer_flare/v1", "tracer_flare"),
            ("/openlineage/api/v1/lineage", "openlineage"),
        ];
        for (path, _) in cases {
            // Compressed or not, a stub's body is discarded unread.
            let response = post_raw(&addr, path, "Content-Encoding: zstd\r\n", b"anything").await;
            assert!(response.starts_with("HTTP/1.1 200"), "{path}: {response}");
            assert_eq!(body_of(&response), "{}", "{path}");
        }
        assert!(rx.try_recv().is_err(), "a stub's upload is never sent");
        let events = registry.drain(0);
        for (path, route) in cases {
            assert_eq!(
                sum_of(&events, "logit.input.requests.acknowledged", &[("route", route)]),
                Some(1.0),
                "{path}"
            );
        }
    }

    #[test]
    fn a_prefix_route_matches_only_on_a_path_boundary() {
        assert_eq!(Route::from_path("/evp_proxy/v1"), Some(Route::Acknowledged("evp_proxy_v1")));
        assert_eq!(Route::from_path("/evp_proxy/v10/x"), None);
        assert_eq!(Route::from_path("/telemetry/proxyish"), None);
    }

    #[tokio::test]
    async fn json_trace_bodies_are_415_and_counted() {
        let (registry, input) = metered();
        let (addr, _rx) = start(input, 16).await;
        for path in ["/v0.3/traces", "/v0.4/traces"] {
            let response =
                post_raw(&addr, path, "Content-Type: application/json\r\n", b"[[]]").await;
            assert!(response.starts_with("HTTP/1.1 415"), "{path}: {response}");
        }
        assert_eq!(
            sum_of(
                &registry.drain(0),
                "logit.input.requests.rejected",
                &[("reason", "json_traces")]
            ),
            Some(2.0)
        );
    }

    #[tokio::test]
    async fn gzip_decodes_and_every_other_encoding_is_415() {
        let (addr, mut rx) = start_default().await;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        std::io::Write::write_all(&mut encoder, &v04(1)).unwrap();
        let body = encoder.finish().unwrap();
        let headers = format!("{MSGPACK}Content-Encoding: gzip\r\n");
        let response = post_raw(&addr, "/v0.4/traces", &headers, &body).await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        recv_batch(&mut rx).await;
        for encoding in ["zstd", "deflate", "br"] {
            let headers = format!("{MSGPACK}Content-Encoding: {encoding}\r\n");
            let response = post_raw(&addr, "/v0.4/traces", &headers, &v04(1)).await;
            assert!(response.starts_with("HTTP/1.1 415"), "{encoding}: {response}");
        }
    }

    #[tokio::test]
    async fn a_malformed_body_is_400() {
        let (registry, input) = metered();
        let (addr, _rx) = start(input, 16).await;
        for path in ["/v0.4/traces", "/v0.5/traces", "/v0.7/traces", "/v0.6/stats"] {
            let response = post_raw(&addr, path, MSGPACK, b"\xc1 not msgpack").await;
            assert!(response.starts_with("HTTP/1.1 400"), "{path}: {response}");
        }
        assert_eq!(
            sum_of(&registry.drain(0), "logit.input.requests.rejected", &[("reason", "malformed")]),
            Some(4.0)
        );
    }

    #[tokio::test]
    async fn an_oversize_body_is_413() {
        let (addr, _rx) = start_default().await;
        let body = vec![0x90; MAX_REQUEST_BYTES + 1];
        let response = post_raw(&addr, "/v0.4/traces", MSGPACK, &body).await;
        assert!(response.starts_with("HTTP/1.1 413"), "{response}");
    }

    const ALL_HEADERS: &str = "Datadog-Meta-Lang: python\r\n\
        Datadog-Meta-Lang-Version: 3.12.1\r\n\
        Datadog-Meta-Lang-Interpreter: CPython\r\n\
        Datadog-Meta-Lang-Interpreter-Vendor: python.org\r\n\
        Datadog-Meta-Tracer-Version: 2.14.0\r\n\
        Datadog-Container-ID: abc123\r\n\
        Datadog-Entity-ID: ci-abc123\r\n\
        Datadog-Client-Computed-Top-Level: yes\r\n\
        Datadog-Client-Computed-Stats: true\r\n\
        Datadog-Client-Dropped-P0-Traces: 7\r\n\
        Datadog-Client-Dropped-P0-Spans: 21\r\n";

    #[tokio::test]
    async fn every_tracer_header_becomes_a_resource_attribute() {
        let (addr, mut rx) = start_default().await;
        for (path, body) in [("/v0.4/traces", v04(1)), ("/v0.5/traces", v05())] {
            let headers = format!("{MSGPACK}{ALL_HEADERS}");
            let response = post_raw(&addr, path, &headers, &body).await;
            assert!(response.starts_with("HTTP/1.1 200"), "{path}: {response}");
            let batch = recv_batch(&mut rx).await;
            let attrs = &batch.resource.attributes;
            let expected = [
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
            ];
            for (attr, value) in &expected {
                assert_eq!(attrs.get(attr), Some(value), "{path}: {attr}");
            }
            assert_eq!(attrs.len(), expected.len(), "{path}: nothing else");
        }
    }

    #[tokio::test]
    async fn a_v07_payload_s_own_fields_win_over_the_headers() {
        let (addr, mut rx) = start_default().await;
        let headers = format!("{MSGPACK}{ALL_HEADERS}");
        let response = post_raw(&addr, "/v0.7/traces", &headers, &v07("go", 1)).await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        let batch = recv_batch(&mut rx).await;
        let attrs = &batch.resource.attributes;
        assert_eq!(attrs.get(RESOURCE_ATTR_TRACER_LANGUAGE_NAME), Some(&Value::str("go")));
        assert_eq!(attrs.get(RESOURCE_ATTR_TRACER_VERSION), Some(&Value::str("2.14.0")));
    }

    #[tokio::test]
    async fn a_false_or_malformed_flag_header_is_left_out() {
        let diag = Diagnostics::new("apm");
        let (addr, mut rx) = start(tcp().with_diagnostics(diag.clone()), 16).await;
        let headers = format!(
            "{MSGPACK}Datadog-Client-Computed-Stats: false\r\n\
             Datadog-Client-Dropped-P0-Traces: many\r\n"
        );
        post_raw(&addr, "/v0.4/traces", &headers, &v04(1)).await;
        let batch = recv_batch(&mut rx).await;
        assert!(batch.resource.attributes.is_empty(), "{:?}", batch.resource);
        assert_eq!(diag.occurrences("bad_header"), 1);
    }

    #[tokio::test]
    async fn stats_takes_no_tracer_headers() {
        let (addr, mut rx) = start_default().await;
        // `ClientStatsPayload{Stats:[{Start:1, Duration:10, Stats:[{Name:"x", Hits:1}]}]}`.
        let mut w = Writer::new();
        w.write_map_len(1);
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
        w.write_str("x");
        w.write_str("Hits");
        w.write_u64(1);
        let headers = format!("{MSGPACK}{ALL_HEADERS}");
        let response = post_raw(&addr, "/v0.6/stats", &headers, &w.into_inner()).await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        let batch = recv_batch(&mut rx).await;
        assert_eq!(batch.resource.attributes.get(RESOURCE_ATTR_TRACER_ENTITY_ID), None);
    }

    #[tokio::test]
    async fn a_trace_count_mismatch_is_a_diagnostic_not_a_rejection() {
        let diag = Diagnostics::new("apm");
        let (addr, mut rx) = start(tcp().with_diagnostics(diag.clone()), 16).await;
        let cases: [(&str, Vec<u8>, usize); 3] = [
            ("/v0.4/traces", v04(2), 2),
            ("/v0.5/traces", v05(), 1),
            ("/v0.7/traces", v07("go", 3), 3),
        ];
        for (path, body, traces) in &cases {
            let headers = format!("{MSGPACK}X-Datadog-Trace-Count: {traces}\r\n");
            let response = post_raw(&addr, path, &headers, body).await;
            assert!(response.starts_with("HTTP/1.1 200"), "{path}: {response}");
            recv_batch(&mut rx).await;
        }
        assert_eq!(diag.occurrences("trace_count_mismatch"), 0, "every count matched");

        for (path, body, traces) in &cases {
            let headers = format!("{MSGPACK}X-Datadog-Trace-Count: {}\r\n", traces + 1);
            let response = post_raw(&addr, path, &headers, body).await;
            assert!(response.starts_with("HTTP/1.1 200"), "{path}: {response}");
            recv_batch(&mut rx).await;
        }
        assert_eq!(diag.occurrences("trace_count_mismatch"), 3);
    }

    #[tokio::test]
    async fn delivered_spans_are_counted() {
        let (registry, input) = metered();
        let (addr, mut rx) = start(input, 16).await;
        post_raw(&addr, "/v0.4/traces", MSGPACK, &v04(3)).await;
        recv_batch(&mut rx).await;
        let events = registry.drain(0);
        assert_eq!(sum_of(&events, "logit.input.spans", &[]), Some(3.0));
        assert_eq!(sum_of(&events, "logit.input.requests", &[("route", "traces_v04")]), Some(1.0));
    }

    /// A one-slot channel nothing drains: the second request waits `busy_after` and is answered
    /// `503`, delivering nothing and counting the batch dropped.
    #[tokio::test]
    async fn a_full_downstream_is_answered_503_after_the_busy_bound() {
        let (registry, input) = metered();
        let input = input.with_busy_after(Duration::from_millis(200));
        let (addr, mut rx) = start(input, 1).await;

        let response = post_raw(&addr, "/v0.4/traces", MSGPACK, &v04(1)).await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        let started = Instant::now();
        let response = post_raw(&addr, "/v0.4/traces", MSGPACK, &v04(1)).await;
        assert!(response.starts_with("HTTP/1.1 503"), "{response}");
        assert_eq!(body_of(&response), r#"{"status":"error","errors":["busy"]}"#);
        assert!(started.elapsed() >= Duration::from_millis(200));

        recv_batch(&mut rx).await;
        assert!(rx.try_recv().is_err(), "the 503'd batch was never delivered");
        let events = registry.drain(0);
        assert_eq!(sum_of(&events, "logit.input.requests", &[("class", "busy")]), Some(1.0));
        assert_eq!(
            sum_of(&events, "logit.input.batches.dropped", &[("reason", "busy")]),
            Some(1.0)
        );
        assert_eq!(sum_of(&events, "logit.input.spans", &[]), Some(1.0), "only the first");
    }

    #[test]
    fn default_busy_after_is_two_seconds() {
        assert_eq!(BUSY_AFTER, Duration::from_secs(2));
        assert_eq!(DatadogTraceInput::new().busy_after, BUSY_AFTER);
    }

    #[tokio::test]
    async fn a_connection_past_the_cap_is_dropped_and_counted() {
        let (registry, input) = metered();
        let (addr, _rx) = start(input.with_max_connections(1), 16).await;
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
            sum_of(&registry.drain(0), "logit.input.connections.rejected", &[("reason", "limit")]),
            Some(1.0)
        );
        drop(first);
    }

    #[tokio::test]
    async fn a_second_bind_is_a_no_op() {
        let mut input = tcp();
        assert_eq!(input.local_addr(), None);
        input.bind().await.unwrap();
        let addr = input.local_addr().unwrap();
        input.bind().await.unwrap();
        assert_eq!(input.local_addr(), Some(addr));
    }

    #[tokio::test]
    async fn binding_with_neither_listener_fails() {
        let err = DatadogTraceInput::new().bind().await.unwrap_err();
        assert!(err.to_string().contains("'bind' address, a 'socket' path"), "{err}");
    }

    /// A fresh directory under the system temp dir, removed on drop. Short, because a Unix
    /// socket path is limited to about 100 bytes.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("ldti-{tag}-{}", std::process::id()));
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

    /// Sends one raw HTTP/1.1 request over the Unix socket at `path`, returning the response.
    async fn unix_request(
        path: &Path,
        method: &str,
        uri: &str,
        headers: &str,
        body: &[u8],
    ) -> String {
        let mut stream = UnixStream::connect(path).await.unwrap();
        let request = format!(
            "{method} {uri} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\
             Connection: close\r\n{headers}\r\n",
            body.len()
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
        let mut buf = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut buf)).await;
        String::from_utf8_lossy(&buf).into_owned()
    }

    #[tokio::test]
    async fn the_unix_socket_serves_alongside_tcp_with_mode_0666() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new("both");
        let path = dir.0.join("apm.socket");
        let diag = Diagnostics::new("apm");
        let input = tcp().with_socket(&path).with_diagnostics(diag.clone());
        let (addr, mut rx) = start(input, 16).await;

        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o666);

        let response = unix_request(&path, "PUT", "/v0.4/traces", MSGPACK, &v04(1)).await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert_eq!(recv_batch(&mut rx).await.events.len(), 1);
        let response = post_raw(&addr, "/v0.4/traces", MSGPACK, &v04(1)).await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        recv_batch(&mut rx).await;

        let response = unix_request(&path, "GET", "/info", "", b"").await;
        let info: serde_json::Value = serde_json::from_str(body_of(&response)).unwrap();
        assert_eq!(info["config"]["receiver_socket"], path.display().to_string());

        // A connect-and-close probe is not a connection error.
        drop(UnixStream::connect(&path).await.unwrap());
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(diag.occurrences("connection_error"), 0);
    }

    #[tokio::test]
    async fn a_stale_socket_file_is_replaced() {
        let dir = TempDir::new("stale");
        let path = dir.0.join("apm.socket");
        drop(std::os::unix::net::UnixListener::bind(&path).unwrap());
        assert!(path.exists(), "the stale socket file is left behind");
        let mut input = DatadogTraceInput::new().with_socket(&path);
        input.bind().await.expect("a stale socket is replaced");
        let second = input.bind().await;
        assert!(second.is_ok(), "a second bind is a no-op");
    }

    #[tokio::test]
    async fn a_regular_file_or_a_missing_directory_is_refused() {
        let dir = TempDir::new("refuse");
        let file = dir.0.join("not-a-socket");
        std::fs::write(&file, b"keep me").unwrap();
        let err = DatadogTraceInput::new().with_socket(&file).bind().await.unwrap_err();
        assert!(err.to_string().contains("is not a socket"), "{err}");
        assert_eq!(std::fs::read(&file).unwrap(), b"keep me");

        let missing = dir.0.join("no-such-dir").join("apm.socket");
        let err = DatadogTraceInput::new().with_socket(&missing).bind().await.unwrap_err();
        assert!(err.to_string().contains("does not exist"), "{err}");
    }

    /// The value of `metric`'s `Sum` in a drained snapshot, restricted to points carrying every
    /// tag in `tags`.
    fn sum_of(events: &[logit_core::Event], metric: &str, tags: &[(&str, &str)]) -> Option<f64> {
        let mut total = None;
        for event in events {
            if !tags
                .iter()
                .all(|(k, v)| event.attributes.get(k).and_then(|x| x.as_str()) == Some(*v))
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
