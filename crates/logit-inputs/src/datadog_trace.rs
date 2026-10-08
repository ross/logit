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
//!   only record of those spans. A tracer built on libdatadog (dd-trace-py 4.x) computes client
//!   stats only when this is `true`, so against this document it sends spans and no stats, and
//!   the downstream Agent computes them (`testdata/interop/datadog/README.md`).
//! - `span_meta_structs: true` and `span_events: true`: the codec carries `meta_struct` and native
//!   span events, so a tracer may send them rather than flattening them into `meta`.
//! - `long_running_spans: false`: partial flushes of an unfinished span aren't something the
//!   downstream Agent is known to reassemble when they arrive through a relay, so a tracer isn't
//!   asked to send them.
//! - `evp_proxy_allowed_headers`, `peer_tags`, and `span_kinds_stats_computed` empty, and
//!   `obfuscation_version: 0`: no `evp_proxy` passthrough, no peer-tag stats aggregation, no
//!   span-kind stats, and no Agent-side obfuscation for a tracer to rely on (plan §14). A tracer
//!   that obfuscates on its own keeps doing so.
//! - `config`: the recorded Agent's `target_tps` 10, `max_eps` 200, `max_request_bytes` 25 MiB,
//!   and `statsd_port` 8125, with every obfuscation switch off because nothing here obfuscates.
//!   The rest are `logit`'s own, not the Agent's: `connection_limit` is this listener's
//!   connection cap (`max_connections:`, 1024 by default) and `receiver_timeout` 5 matches its
//!   5 s handshake timeout (the recorded Agent reports 0 for both), and `receiver_port` (0
//!   without `bind`) and `receiver_socket` (empty without `socket`) are where it listens. Every
//!   field has the JSON type a real Agent's `/info` gives it (`redis`, `valkey`, and `memcached`
//!   are objects, not switches): libdatadog rejects the whole document over one mistyped field,
//!   then runs as if no Agent answered. A test holds this document to the recorded
//!   `testdata/interop/datadog/agent-info.json`.
//!
//! # Tracer headers
//!
//! v0.3, v0.4, and v0.5 carry no `TracerPayload`, so what a tracer says about itself arrives only
//! in request headers. `Datadog-Meta-Lang`, `-Lang-Version`, `-Lang-Interpreter`,
//! `-Lang-Interpreter-Vendor`, `-Tracer-Version`, `Datadog-Container-ID`, `Datadog-Entity-ID`,
//! `Datadog-External-Env`, `Datadog-Client-Computed-Top-Level`, `Datadog-Client-Computed-Stats`,
//! and `Datadog-Client-Dropped-P0-{Traces,Spans}` become `datadog.tracer.*` batch resource attributes,
//! per the codec's Traces table. On v0.7 the payload's own fields win: a header fills a field only
//! where the payload left it empty. `datadog_trace_out` restores the headers from the same
//! attributes. `/v0.6/stats` takes three of them, `Datadog-Meta-Lang`, `-Tracer-Version`, and
//! `Datadog-Container-ID`, each only where the `ClientStatsPayload` left `Lang`, `TracerVersion`,
//! or `ContainerID` empty: the Agent fills those three the same way before it forwards a payload,
//! and a recorded dd-trace-py 4.15 sends its stats with `Lang` and `TracerVersion` empty and the
//! values in the headers (`testdata/interop/datadog/tracer-v04-v0-6-stats-000.*`). The rest have
//! no stats field.
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
//! 3. **`Content-Encoding`.** `identity` (or none) or `gzip`, else `415`, including a header that
//!    is present but empty or not ASCII. Tracers don't compress,
//!    and the Agent accepts gzip. The decompressed size is capped at the same 25 MiB.
//! 4. **Decode.** `CodecError::Malformed` is a `400`.
//! 5. **Delivery**, bounded (below), then the route's `200`. A body that decodes to no events is
//!    answered without a send.
//!
//! # Backpressure: a short bounded wait, then `503`, which a tracer retries only briefly
//!
//! Delivery works as `datadog_in`'s does: the request's one batch is sent through
//! [`Fanout::send_with_deadline`], reaching every downstream consumer or none, under a
//! [`BUSY_AFTER`] deadline, and a request that misses it is answered `503` with
//! `Retry-After: 1`, counted `logit.input.requests{class="busy"}` and
//! `logit.input.batches.dropped{reason="busy"}`. `BUSY_AFTER` is shorter than `datadog_in`'s (2 s,
//! not 5 s), because a tracer's patience is: dd-trace-py retries a `503` a few times and then
//! drops the payload, where an Agent retries for minutes. A stall shorter than the window
//! [ADR `datadog-agent-and-intake-relay`](../../../../docs/adr/datadog-agent-and-intake-relay.md)'s
//! decision 11 derives defers the payload and a longer one loses it; the counter can't tell which,
//! so treat a sustained rate as loss.
//!
//! A batch no consumer takes, because every consumer of the listener has closed, is answered the
//! same `503` and `Retry-After: 1` with `closed_consumer` in place of `busy`, in the body and both
//! counters, as `datadog_in` does.
//!
//! # The Unix socket
//!
//! `socket:` binds a Unix stream socket, the Agent's `receiver_socket`
//! (`/var/run/datadog/apm.socket` by default), which a tracer reaches with
//! `DD_TRACE_AGENT_URL=unix:///var/run/datadog/apm.socket`. [`Input::bind`] refuses a path whose
//! directory doesn't exist, replaces a stale socket file left by an earlier run (what the Agent does
//! at startup), and refuses a path that exists and isn't a socket, so a typo can't delete a
//! regular file. The socket file is then made mode `socket_mode:`
//! ([`DatadogTraceInput::with_socket_mode`]), by default `0722`, the mode a recorded Agent 7.83
//! gives its own `apm.socket` (`testdata/interop/datadog/README.md`): connecting needs only write
//! permission, so a tracer running as any user can connect. Restrict access with the directory's
//! permissions. The socket file isn't removed on shutdown;
//! the next start replaces it.
//!
//! Both listeners share one connection cap and one handler. A Unix connection has no TLS, and its
//! diagnostics name the socket path.
//!
//! At shutdown each accept loop closes its own listener and drains its own connections, as
//! `otlp_in` does (`crate::otlp`'s "Idle timeout"), and the input returns once both have. A fatal
//! accept error on either ends both, aborting every connection.
//!
//! # Sender address
//!
//! Under `peer:` and `proxy_protocol:` ([`DatadogTraceInput::with_peer`],
//! [`DatadogTraceInput::with_proxy_protocol`]), each connection task builds one
//! [`ConnectionPeer`], and `respond` stamps it on the request's batch beside the tracer headers,
//! before delivery. On the TCP listener the PROXY header is read right after the permit, before
//! the TLS accept or the first-byte peek, with `otlp_in`'s rejection, counting, and health-check
//! rules (`crate::otlp`'s "Sender address"). The Unix socket never reads a header, and graph rule
//! 79 rejects `proxy_protocol:` without `bind`. Under `peer:` a Unix client that bound a path is
//! stamped with that path and no port, as on the shared drivers' `unix_stream`; an unbound
//! client, the usual tracer, is stamped with nothing. Under `forwarded:`
//! ([`DatadogTraceInput::with_forwarded`]), each request's forwarding header, on either listener,
//! replaces the PROXY origin's `client.*` for that request, per ADR `forwarded-header-parsing`.
//!
//! # Telemetry
//!
//! Every name and tag is `&'static`. Per request: `logit.input.requests{route, class}` (class `ok`,
//! `rejected`, `busy`, or `closed_consumer`; route one of [`Route::name`], or `unknown`),
//! `logit.input.request.duration` (timing, every exit), and `logit.input.request.bytes` (the
//! compressed body size, once read). Rejections: `logit.input.requests.rejected{reason}`, reason
//! `unknown_route`, `unsupported_route` (also tagged `route`), `method`, `oversize`, `encoding`,
//! `json_traces`, `malformed_encoding`, `malformed`, `stalled`, or `body_read`.
//! `logit.input.requests.acknowledged{route}` counts a stub's upload,
//! `logit.input.batches.dropped{reason}` (`busy` or `closed_consumer`) a batch a `503` left
//! undelivered,
//! and `logit.input.spans` the spans delivered. The connection metrics are `otlp_in`'s, with one
//! difference: the kernel accept-queue gauges (`crate::tcp::AcceptQueueSampler`) cover the TCP
//! listener only, since they read `TCP_INFO`, which a Unix socket has no counterpart for.

use crate::http::{
    body_read_error_message, collect_with_stall_bound, declared_length, decompress,
    deliver_with_deadline, drive_connection, error_response, is_length_limit, json_response,
    media_type, now_nanos, Activity, BodyReadError, DecompressError, Encoding, MediaType,
    Undelivered,
};
use crate::listener::ConnectionTasks;
use crate::peer::{ConnectionPeer, PeerAttrs};
use crate::Input;
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use http_body_util::{Full, Limited};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use logit_core::{Diagnostics, EventBatch, Resource, Telemetry, Value};
use logit_pipeline::listen::BindOptions;
use logit_pipeline::Fanout;
use logit_proto::datadog::{
    DatadogDecoder, HEADER_CLIENT_COMPUTED_STATS, HEADER_CLIENT_COMPUTED_TOP_LEVEL,
    HEADER_CONTAINER_ID, HEADER_META_LANG, HEADER_META_TRACER_VERSION, HEADER_TRACE_COUNT,
    RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_STATS, RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_TOP_LEVEL,
    RESOURCE_ATTR_TRACER_CONTAINER_ID, RESOURCE_ATTR_TRACER_LANGUAGE_NAME,
    RESOURCE_ATTR_TRACER_VERSION, TRACER_STR_HEADERS, TRACER_U64_HEADERS,
};
use logit_proto::forwarded::ForwardedHeader;
use logit_proto::msgpack::{Reader, Type};
use logit_proto::CodecError;
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};
use tokio_rustls::TlsAcceptor;

/// The cap on a request's body, compressed and decompressed alike: 25 MiB, the Agent's own
/// `max_request_bytes` default. A denial-of-service bound, not a tuning knob, as on `datadog_in`.
const MAX_REQUEST_BYTES: usize = 25 * 1024 * 1024;

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

/// `logit_pipeline::tls::TlsServerSettings`, re-exported as `datadog_in`'s is.
pub use logit_pipeline::tls::TlsServerSettings;

/// The `datadog_trace_in` listener. See this module's doc.
pub struct DatadogTraceInput {
    bind: Option<String>,
    socket: Option<PathBuf>,
    /// The `socket` file's mode after binding.
    socket_mode: u32,
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
    /// Bounds the connections [`Input::run`] serves at once, across the TCP listener and the Unix
    /// socket together, and the `connection_limit` `/info` reports;
    /// [`crate::DEFAULT_MAX_CONNECTIONS`] unless [`Self::with_max_connections`] sets it. With
    /// 25 MiB requests this listener's worst case at the default cap (`max_connections:`, 1024) is
    /// about 9.8 TiB, a bound rather than a memory budget ([`crate::http::MAX_CONCURRENT_STREAMS`]
    /// has the formula).
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
            socket_mode: crate::unix::DEFAULT_SOCKET_MODE,
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
            tls: None,
            listener: None,
            unix_listener: None,
            handshake_timeout: HANDSHAKE_TIMEOUT,
            idle_timeout: None,
            max_connections: crate::DEFAULT_MAX_CONNECTIONS,
            busy_after: BUSY_AFTER,
            peer: false,
            bind_options: BindOptions::default(),
            proxy_protocol: false,
            forwarded: None,
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

    /// Sets the `socket` file's mode (`socket_mode:`), overriding the default `0722`. Without a
    /// socket it has no effect; graph rule 78 rejects the field there.
    pub fn with_socket_mode(mut self, socket_mode: u32) -> Self {
        self.socket_mode = socket_mode;
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

    /// Overrides [`HANDSHAKE_TIMEOUT`] for the PROXY header, the TLS accept, and the first-byte
    /// wait, each on its own budget (`handshake_timeout:` in config). Graph rule 45 rejects `0s`.
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

    /// Overrides [`crate::DEFAULT_MAX_CONNECTIONS`]; `max_connections:` in config. Graph rule 74
    /// rejects `0` before it gets here.
    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Overrides [`BUSY_AFTER`]. A test/tuning hook, not a config field, as on `datadog_in`.
    pub fn with_busy_after(mut self, d: Duration) -> Self {
        self.busy_after = d;
        self
    }

    /// Stamps every event a request decodes with its connection's socket peer (`peer:` in
    /// config), per [`crate::peer`], on both listeners. Off by default.
    pub fn with_peer(mut self, peer: bool) -> Self {
        self.peer = peer;
        self
    }

    /// Sets `SO_REUSEPORT` before the bind (`reuse_port:` in config), so another process can bind
    /// the same address at the same time. Off by default.
    /// Applies to the TCP listener only; the Unix socket has no port to share.
    pub fn with_reuse_port(mut self, reuse_port: bool) -> Self {
        self.bind_options.reuse_port = reuse_port;
        self
    }

    /// Requires a PROXY protocol header ahead of every connection to the TCP listener and stamps
    /// the origin it names (`proxy_protocol:` in config). Off by default; the Unix socket never
    /// reads one. See this module's "Sender address".
    pub fn with_proxy_protocol(mut self, proxy_protocol: bool) -> Self {
        self.proxy_protocol = proxy_protocol;
        self
    }

    /// Reads the client from `header` on every request, on both listeners (`forwarded:` in
    /// config), per [`ConnectionPeer::request`]. Off by default.
    pub fn with_forwarded(mut self, header: Option<ForwardedHeader>) -> Self {
        self.forwarded = header;
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
                let listener = logit_pipeline::listen::bind_tcp(bind, self.bind_options).await?;
                self.diag.info("bound", format_args!("listening on {bind}"));
                self.listener = Some(listener);
            }
        }
        if let Some(path) = &self.socket {
            if self.unix_listener.is_none() {
                let listener =
                    crate::unix::bind_listener("datadog_trace_in", path, self.socket_mode)?;
                self.diag.info("bound", format_args!("listening on {}", path.display()));
                self.unix_listener = Some(listener);
            }
        }
        Ok(())
    }

    /// `datadog_in`'s accept loop, once per configured listener, both under one connection cap.
    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        // A never-firing `watch`, so `run` and `run_until_shutdown` share one implementation. The
        // sender lives for this scope: a dropped sender reads as shutdown.
        let (_tx, rx) = watch::channel(false);
        self.run_until_shutdown(sink, rx).await
    }

    /// Returns `Ok` once both accept loops have closed their listeners and drained their
    /// connections, or the first fatal accept error.
    async fn run_until_shutdown(
        &mut self,
        sink: Fanout,
        shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
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
            live_connections: crate::listener::LiveConnections::new(self.telemetry.clone()),
            handshake_timeout: self.handshake_timeout,
            idle_timeout: self.idle_timeout,
            busy_after: self.busy_after,
            info: Bytes::from(info_document(receiver_port, &receiver_socket, self.max_connections)),
            record_peer: self.peer,
            proxy_protocol: self.proxy_protocol,
            forwarded: self.forwarded,
        });
        let tls_acceptor = self.tls.clone().map(TlsAcceptor::from);
        let socket_path: Option<Arc<Path>> = self.socket.as_deref().map(Arc::from);

        let tcp_loop = {
            let accept = Arc::clone(&accept);
            let mut shutdown = shutdown.clone();
            async move {
                match tcp {
                    Some(listener) => accept_tcp(listener, accept, tls_acceptor, shutdown).await,
                    None => {
                        let _ = shutdown.wait_for(|&due| due).await;
                        Ok(())
                    }
                }
            }
        };
        let unix_loop = {
            let mut shutdown = shutdown;
            async move {
                match (unix, socket_path) {
                    (Some(listener), Some(path)) => {
                        accept_unix(listener, path, accept, shutdown).await
                    }
                    _ => {
                        let _ = shutdown.wait_for(|&due| due).await;
                        Ok(())
                    }
                }
            }
        };
        // Both loops are awaited to the end: see `docs/design/pipeline-graph.md`'s "Cancellation
        // points".
        tokio::try_join!(tcp_loop, unix_loop)?;
        Ok(())
    }
}

/// What both accept loops share, built once per [`Input::run_until_shutdown`]. Every connection
/// task holds a clone of the `Arc`, so the connection tasks live in each accept loop's own
/// [`ConnectionTasks`], never in here: a set reachable from its own tasks is a cycle that aborts
/// nothing.
struct AcceptContext {
    sink: Fanout,
    telemetry: Telemetry,
    diag: Diagnostics,
    /// One cap across both listeners.
    connection_limit: Arc<Semaphore>,
    live_connections: crate::listener::LiveConnections,
    handshake_timeout: Duration,
    idle_timeout: Option<Duration>,
    busy_after: Duration,
    /// The `/info` body, rendered once.
    info: Bytes,
    /// `peer:` in config.
    record_peer: bool,
    /// `proxy_protocol:` in config, read on the TCP listener only.
    proxy_protocol: bool,
    /// `forwarded:` in config, read on both listeners.
    forwarded: Option<ForwardedHeader>,
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

    fn shared(&self, peer: ConnectionPeer) -> Arc<Shared> {
        Arc::new(Shared {
            sink: self.sink.clone(),
            telemetry: self.telemetry.clone(),
            diag: self.diag.clone(),
            busy_after: self.busy_after,
            info: self.info.clone(),
            peer: peer.with_forwarded(self.forwarded, &self.diag),
        })
    }

    /// Runs one connection on its own task in `tasks`: the live-connection gauge around it, the
    /// permit held for its lifetime, and a failure reported through the throttled
    /// `connection_error`.
    fn spawn(
        self: &Arc<Self>,
        tasks: &mut ConnectionTasks,
        permit: OwnedSemaphorePermit,
        connection: impl Future<Output = Result<(), String>> + Send + 'static,
    ) {
        let this = Arc::clone(self);
        tasks.spawn(async move {
            let _permit = permit; // released on drop
            let live = this.live_connections.enter();
            let result = connection.await;
            drop(live);
            if let Err(err) = result {
                this.diag.clone().warn_throttled("connection_error", err);
            }
        });
    }
}

/// Where one TCP connection's pre-serve steps (the PROXY header, the TLS accept, the first-byte
/// peek) left it.
enum Prelude {
    /// Boxed: a rustls session is over 1 KiB, and this is built once per connection.
    Tls(Box<tokio_rustls::server::TlsStream<TcpStream>>, Arc<Shared>),
    Plain(TcpStream, Arc<Shared>),
    /// Ended before serving: a health-check probe or a rejected PROXY header (`Ok`), or a failed
    /// handshake or first byte (`Err`).
    Done(Result<(), String>),
}

/// The TCP accept loop: `datadog_in`'s, including the accept-queue gauges, the PROXY header under
/// `proxy_protocol:` and then the TLS handshake or plaintext first-byte peek, each bounded inside
/// the spawned task, and a clean close or a reset before the first byte treated as a health check.
async fn accept_tcp(
    listener: TcpListener,
    accept: Arc<AcceptContext>,
    tls_acceptor: Option<TlsAcceptor>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut accept_queue =
        crate::tcp::AcceptQueueSampler::new(accept.telemetry.clone(), accept.diag.clone());
    let mut accept_diag = accept.diag.clone();
    // A local of this future, never in `AcceptContext`, so dropping the future aborts every
    // connection still open (`crate::listener::ConnectionTasks`).
    let mut tasks = ConnectionTasks::new();
    loop {
        // Both waits race shutdown: see `docs/design/pipeline-graph.md`'s "Cancellation points".
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
                        &accept.telemetry,
                        &mut accept_diag,
                    ) => absorbed?,
                    _ = shutdown.wait_for(|&due| due) => break,
                }
                continue;
            }
        };
        let Some(permit) = accept.permit() else {
            drop(stream);
            continue;
        };
        let tls_acceptor = tls_acceptor.clone();
        let handshake_timeout = accept.handshake_timeout;
        let idle_timeout = accept.idle_timeout;
        let context = Arc::clone(&accept);
        let mut conn_shutdown = shutdown.clone();
        accept.spawn(&mut tasks, permit, async move {
            let prelude = async {
                // Ahead of the TLS accept and the first-byte peek, both of which would otherwise
                // read the header's bytes as the request's (this module's "Sender address").
                let origin = if context.proxy_protocol {
                    match crate::peer::read_proxy_origin(&mut stream, handshake_timeout).await {
                        Ok(origin) => Some(origin),
                        Err(err) => {
                            context.telemetry.count(
                                "logit.input.connections.rejected",
                                1.0,
                                &[("reason", "proxy_header")],
                            );
                            context.diag.clone().warn_throttled("proxy_header", err);
                            return Prelude::Done(Ok(()));
                        }
                    }
                } else {
                    None
                };
                let shared =
                    context.shared(ConnectionPeer::tcp(peer, context.record_peer, origin.as_ref()));
                match tls_acceptor {
                    Some(acceptor) => {
                        match tokio::time::timeout(handshake_timeout, acceptor.accept(stream)).await
                        {
                            Ok(Ok(tls_stream)) => Prelude::Tls(Box::new(tls_stream), shared),
                            Ok(Err(err)) => {
                                Prelude::Done(Err(format!("TLS handshake failed: {err}")))
                            }
                            Err(_elapsed) => Prelude::Done(Err(format!(
                                "TLS handshake did not complete within {handshake_timeout:?}"
                            ))),
                        }
                    }
                    None => {
                        let first_byte =
                            tokio::time::timeout(handshake_timeout, stream.peek(&mut [0u8; 1]))
                                .await;
                        match first_byte {
                            // A clean close or a reset before the first byte is a health-check
                            // probe, not a fault (`crate::otlp`'s "not a fault").
                            Ok(Ok(0)) => Prelude::Done(Ok(())),
                            Ok(Err(err)) if err.kind() == std::io::ErrorKind::ConnectionReset => {
                                Prelude::Done(Ok(()))
                            }
                            Ok(Ok(_)) => Prelude::Plain(stream, shared),
                            Ok(Err(err)) => Prelude::Done(Err(format!(
                                "waiting for a first byte failed: {err}"
                            ))),
                            Err(_elapsed) => Prelude::Done(Err(format!(
                                "no first byte received within {handshake_timeout:?}"
                            ))),
                        }
                    }
                }
            };
            // Races shutdown as a whole (`docs/design/pipeline-graph.md`'s "Cancellation
            // points"): nothing of a request has been read before it ends.
            let prelude = tokio::select! {
                prelude = prelude => prelude,
                _ = conn_shutdown.wait_for(|&due| due) => return Ok(()),
            };
            match prelude {
                Prelude::Tls(tls_stream, shared) => {
                    serve_connection(
                        TokioIo::new(*tls_stream),
                        shared,
                        idle_timeout,
                        handshake_timeout,
                        conn_shutdown,
                    )
                    .await
                }
                Prelude::Plain(stream, shared) => {
                    serve_connection(
                        TokioIo::new(stream),
                        shared,
                        idle_timeout,
                        handshake_timeout,
                        conn_shutdown,
                    )
                    .await
                }
                Prelude::Done(result) => result,
            }
        });
    }

    // Closed before the drain: under `reuse_port` the kernel keeps hashing new connections to a
    // bound socket that nothing accepts on any more.
    drop(listener);
    tasks.drain().await;
    Ok(())
}

/// The Unix-socket accept loop: the TCP loop's plaintext arm, with no accept-queue gauge (this
/// module's "Telemetry"), racing shutdown at the same points.
async fn accept_unix(
    listener: UnixListener,
    path: Arc<Path>,
    accept: Arc<AcceptContext>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut accept_diag = accept.diag.clone();
    let mut tasks = ConnectionTasks::new();
    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            _ = shutdown.wait_for(|&due| due) => break,
        };
        let (stream, addr) = match accepted {
            Ok(accepted) => accepted,
            Err(err) => {
                tokio::select! {
                    biased;
                    absorbed = crate::listener::absorb_accept_error(
                        err,
                        &accept.telemetry,
                        &mut accept_diag,
                    ) => absorbed?,
                    _ = shutdown.wait_for(|&due| due) => break,
                }
                continue;
            }
        };
        let Some(permit) = accept.permit() else {
            drop(stream);
            continue;
        };
        let stamped = if accept.record_peer { PeerAttrs::from_unix(&addr) } else { None };
        let shared = accept.shared(ConnectionPeer::unix(Arc::clone(&path), stamped));
        let handshake_timeout = accept.handshake_timeout;
        let idle_timeout = accept.idle_timeout;
        let mut conn_shutdown = shutdown.clone();
        accept.spawn(&mut tasks, permit, async move {
            let first_byte = tokio::select! {
                first_byte = tokio::time::timeout(handshake_timeout, has_first_byte(&stream)) => {
                    first_byte
                }
                _ = conn_shutdown.wait_for(|&due| due) => return Ok(()),
            };
            match first_byte {
                Ok(Ok(false)) => Ok(()), // a health-check probe, not a fault
                Ok(Ok(true)) => {
                    serve_connection(
                        TokioIo::new(stream),
                        shared,
                        idle_timeout,
                        handshake_timeout,
                        conn_shutdown,
                    )
                    .await
                }
                Ok(Err(err)) => Err(format!("waiting for a first byte failed: {err}")),
                Err(_elapsed) => {
                    Err(format!("no first byte received within {handshake_timeout:?}"))
                }
            }
        });
    }

    drop(listener);
    tasks.drain().await;
    Ok(())
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

/// What every request on one connection needs, built once per connection.
struct Shared {
    sink: Fanout,
    telemetry: Telemetry,
    diag: Diagnostics,
    busy_after: Duration,
    info: Bytes,
    /// Names the sender in diagnostic text, and stamps the batch under `peer:` or
    /// `proxy_protocol:`.
    peer: ConnectionPeer,
}

/// Serves one accepted (and, with TLS on, handshaken) connection to completion: `datadog_in`'s,
/// with this listener's handler, over TCP, TLS, or a Unix stream alike.
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
    } else {
        apply_stats_headers(&mut batch, &parts.headers);
    }
    shared.peer.request(&parts.headers).stamp_batches(std::slice::from_mut(&mut batch));
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
                        "datadog_trace_in: answered 503 to {}: the pipeline did not accept a \
                         batch within {:?}; a tracer retries a few times, then drops it",
                        shared.peer, shared.busy_after
                    ),
                ),
                Undelivered::Closed(_) => diag.warn_throttled(
                    "closed_consumer",
                    format_args!(
                        "datadog_trace_in: answered 503 to {}: no consumer took the batch",
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
    for (header, attr) in TRACER_STR_HEADERS {
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
    for (header, attr) in TRACER_U64_HEADERS {
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

/// The tracer headers the Agent copies into a `/v0.6/stats` payload whose own field is empty
/// (this module's "Tracer headers").
const STATS_HEADERS: [(&str, &str); 3] = [
    (HEADER_META_LANG, RESOURCE_ATTR_TRACER_LANGUAGE_NAME),
    (HEADER_META_TRACER_VERSION, RESOURCE_ATTR_TRACER_VERSION),
    (HEADER_CONTAINER_ID, RESOURCE_ATTR_TRACER_CONTAINER_ID),
];

/// Fills a stats batch's empty `Lang`, `TracerVersion`, and `ContainerID` carriers from the
/// request's headers, as the Agent's stats receiver does before it forwards the payload.
fn apply_stats_headers(batch: &mut EventBatch, headers: &HeaderMap) {
    let fills: Vec<(&'static str, Value)> = STATS_HEADERS
        .iter()
        .filter(|(_, attr)| batch.resource.attributes.get(attr).is_none())
        .filter_map(|&(header, attr)| {
            let value = headers.get(header)?.to_str().ok()?.trim();
            (!value.is_empty()).then(|| (attr, Value::str(value)))
        })
        .collect();
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

/// The `/info` document (this module's "`/info`"), with the three values that depend on the
/// listener filled in.
fn info_document(receiver_port: u16, receiver_socket: &str, max_connections: usize) -> String {
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
            r#""connection_limit":{max_connections},"receiver_timeout":5,"#,
            r#""max_request_bytes":26214400,"#,
            r#""statsd_port":8125,"max_memory":0,"max_cpu":0,"analyzed_spans_by_service":{{}},"#,
            r#""obfuscation":{{"elastic_search":false,"mongo":false,"sql_exec_plan":false,"#,
            r#""sql_exec_plan_normalize":false,"sql_obfuscation_mode":"","#,
            r#""http":{{"remove_query_string":false,"remove_path_digits":false}},"#,
            r#""remove_stack_traces":false,"#,
            r#""redis":{{"enabled":false,"remove_all_args":false}},"#,
            r#""valkey":{{"enabled":false,"remove_all_args":false}},"#,
            r#""memcached":{{"enabled":false,"keep_command":false}}}}}},"#,
            r#""peer_tags":[],"span_kinds_stats_computed":[],"obfuscation_version":0,"#,
            r#""filter_tags":{{}},"filter_tags_regex":{{}}}}"#,
        ),
        version = version,
        receiver_port = receiver_port,
        receiver_socket = receiver_socket,
        max_connections = max_connections,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_pipeline::test_util::{recv_batch, Totals};
    use logit_proto::datadog::traces::RESOURCE_ATTR_TRACER_LANGUAGE_VERSION;
    use logit_proto::datadog::{
        RESOURCE_ATTR_TRACER_CONTAINER_ID, RESOURCE_ATTR_TRACER_DROPPED_P0_SPANS,
        RESOURCE_ATTR_TRACER_DROPPED_P0_TRACES, RESOURCE_ATTR_TRACER_ENTITY_ID,
        RESOURCE_ATTR_TRACER_EXTERNAL_ENV, RESOURCE_ATTR_TRACER_LANGUAGE_INTERPRETER,
        RESOURCE_ATTR_TRACER_LANGUAGE_INTERPRETER_VENDOR, RESOURCE_ATTR_TRACER_LANGUAGE_NAME,
        RESOURCE_ATTR_TRACER_VERSION,
    };
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
        assert_eq!(info["config"]["connection_limit"], crate::DEFAULT_MAX_CONNECTIONS);
        assert_eq!(info["config"]["max_request_bytes"], MAX_REQUEST_BYTES);
        assert_eq!(info["config"]["obfuscation"]["redis"]["enabled"], false);
        assert_eq!(info["obfuscation_version"], 0);
    }

    // ---- recorded interop fixtures (testdata/interop/datadog/) --------------------------------

    fn repo_path(relative: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").join(relative)
    }

    fn recorded_json(relative: &str) -> serde_json::Value {
        let text = std::fs::read_to_string(repo_path(relative))
            .unwrap_or_else(|e| panic!("reading {relative}: {e}"));
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("{relative}: {e}"))
    }

    /// A recorded request's `.headers` sidecar as the `HeaderMap` the listener would have seen.
    fn recorded_headers(stem: &str) -> HeaderMap {
        let path = format!("testdata/interop/datadog/{stem}.headers");
        let text = std::fs::read_to_string(repo_path(&path))
            .unwrap_or_else(|e| panic!("reading {path}: {e}"));
        let mut headers = HeaderMap::new();
        for (name, value) in text.lines().filter_map(|line| line.split_once(": ")) {
            if name == "method" || name == "path" {
                continue;
            }
            headers.append(
                http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        headers
    }

    fn recorded_body(stem: &str) -> Vec<u8> {
        let path = format!("testdata/interop/datadog/{stem}.bin");
        std::fs::read(repo_path(&path)).unwrap_or_else(|e| panic!("reading {path}: {e}"))
    }

    /// Each field of `ours` exists in `theirs` with the same JSON type, recursing into objects
    /// and into the first element of two non-empty arrays.
    fn assert_same_shape(ours: &serde_json::Value, theirs: &serde_json::Value, at: &str) {
        use serde_json::Value as J;
        let kind = |v: &J| match v {
            J::Null => "null",
            J::Bool(_) => "bool",
            J::Number(_) => "number",
            J::String(_) => "string",
            J::Array(_) => "array",
            J::Object(_) => "object",
        };
        assert_eq!(kind(ours), kind(theirs), "{at}: {ours} against the Agent's {theirs}");
        match (ours, theirs) {
            (J::Object(ours), J::Object(theirs)) => {
                for (key, value) in ours {
                    let theirs = theirs.get(key).unwrap_or_else(|| {
                        panic!("{at}.{key}: the recorded Agent has no such field")
                    });
                    assert_same_shape(value, theirs, &format!("{at}.{key}"));
                }
            }
            (J::Array(ours), J::Array(theirs)) => {
                if let (Some(ours), Some(theirs)) = (ours.first(), theirs.first()) {
                    assert_same_shape(ours, theirs, &format!("{at}[0]"));
                }
            }
            _ => {}
        }
    }

    /// libdatadog (dd-trace-py 4.x) rejects a whole `/info` document over one mistyped field, and
    /// then never turns on client stats: the recorded tracer did that to `"redis": false`. Every
    /// field this listener serves has the type a real Agent 7.83 gives it.
    #[test]
    fn the_info_document_has_the_recorded_agent_s_field_types() {
        let ours: serde_json::Value =
            serde_json::from_str(&info_document(8126, "", crate::DEFAULT_MAX_CONNECTIONS)).unwrap();
        let agent = recorded_json("testdata/interop/datadog/agent-info.json");
        assert_same_shape(&ours, &agent, "info");
    }

    /// `script/record-fixtures datadog-tracer` answers `/info` with this file, standing in for
    /// this listener; it must stay this listener's document.
    #[test]
    fn the_recording_s_info_reply_is_this_listener_s_document() {
        let ours: serde_json::Value =
            serde_json::from_str(&info_document(8126, "", crate::DEFAULT_MAX_CONNECTIONS)).unwrap();
        assert_eq!(recorded_json("tools/record-fixtures/datadog-trace-info.json"), ours);
    }

    /// A recorded dd-trace-py request's headers become the tracer carriers, `Datadog-External-Env`
    /// included.
    #[test]
    fn a_recorded_tracer_s_headers_become_its_carriers() {
        let stem = "tracer-v04-v0-4-traces-000";
        let mut batch =
            DatadogDecoder::new().decode_traces_v04(&recorded_body(stem), 0).expect("decodes");
        apply_tracer_headers(&mut batch, &recorded_headers(stem), &Diagnostics::new("apm"));
        let attr = |key: &str| batch.resource.attributes.get(key).cloned();
        assert_eq!(attr(RESOURCE_ATTR_TRACER_LANGUAGE_NAME), Some(Value::str("python")));
        assert_eq!(attr(RESOURCE_ATTR_TRACER_LANGUAGE_INTERPRETER), Some(Value::str("CPython")));
        assert!(attr(RESOURCE_ATTR_TRACER_VERSION).is_some());
        assert!(attr(RESOURCE_ATTR_TRACER_LANGUAGE_VERSION).is_some());
        assert!(attr(RESOURCE_ATTR_TRACER_ENTITY_ID).is_some_and(|v| v.as_str().is_some()));
        assert_eq!(
            attr(RESOURCE_ATTR_TRACER_EXTERNAL_ENV),
            Some(Value::str("it-false,cn-record-fixtures,pu-00000000-0000-4000-8000-000000000001"))
        );
        assert_eq!(attr(RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_TOP_LEVEL), Some(Value::Bool(true)));
        assert_eq!(attr(RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_STATS), Some(Value::Bool(true)));
    }

    /// The recorded client stats name the tracer only in headers; the stats route fills the
    /// payload's empty fields from them, as the Agent does.
    #[test]
    fn recorded_client_stats_take_the_language_and_version_from_headers() {
        let stem = "tracer-v04-v0-6-stats-000";
        let mut batch = DatadogDecoder::new()
            .decode_client_stats_v06(&recorded_body(stem), 0)
            .expect("decodes");
        assert_eq!(batch.resource.attributes.get(RESOURCE_ATTR_TRACER_LANGUAGE_NAME), None);
        let headers = recorded_headers(stem);
        apply_stats_headers(&mut batch, &headers);
        let attr = |key: &str| batch.resource.attributes.get(key).and_then(Value::as_str);
        assert_eq!(attr(RESOURCE_ATTR_TRACER_LANGUAGE_NAME), Some("python"));
        assert_eq!(
            attr(RESOURCE_ATTR_TRACER_VERSION),
            headers.get(HEADER_META_TRACER_VERSION).and_then(|v| v.to_str().ok())
        );
        // Only the three fields the payload has; the rest of the headers stay out.
        assert_eq!(batch.resource.attributes.get(RESOURCE_ATTR_TRACER_ENTITY_ID), None);
    }

    /// A field the payload already carries wins over its header.
    #[test]
    fn a_stats_payload_s_own_language_wins_over_the_header() {
        let mut resource = Resource::default();
        resource.attributes.insert(RESOURCE_ATTR_TRACER_LANGUAGE_NAME, Value::str("go"));
        let mut batch =
            EventBatch { resource: Arc::new(resource), scope: None, events: Vec::new() };
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_META_LANG, HeaderValue::from_static("python"));
        headers.insert(HEADER_CONTAINER_ID, HeaderValue::from_static("abc"));
        apply_stats_headers(&mut batch, &headers);
        let attr = |key: &str| batch.resource.attributes.get(key).and_then(Value::as_str);
        assert_eq!(attr(RESOURCE_ATTR_TRACER_LANGUAGE_NAME), Some("go"));
        assert_eq!(attr(RESOURCE_ATTR_TRACER_CONTAINER_ID), Some("abc"));
    }

    /// `connection_limit` reports the configured cap, not the default.
    #[tokio::test]
    async fn info_reports_the_configured_connection_cap() {
        let input = DatadogTraceInput::new().with_bind("127.0.0.1:0").with_max_connections(7);
        let (addr, _rx) = start(input, 16).await;
        let response = request_raw(&addr, "GET", "/info", "", b"").await;
        let info: serde_json::Value = serde_json::from_str(body_of(&response)).unwrap();
        assert_eq!(info["config"]["connection_limit"], 7);
    }

    #[test]
    fn the_info_document_escapes_the_socket_path() {
        let info: serde_json::Value = serde_json::from_str(&info_document(
            0,
            r#"/tmp/a "b".sock"#,
            crate::DEFAULT_MAX_CONNECTIONS,
        ))
        .unwrap();
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

        let events = Totals::of(registry.drain(0));
        for (path, route) in cases {
            assert_eq!(
                events.sum("logit.input.requests.rejected", &[("route", route)]),
                1.0,
                "{path}"
            );
        }
        assert_eq!(
            events.sum("logit.input.requests.rejected", &[("reason", "unsupported_route")]),
            cases.len() as f64
        );
        assert_eq!(
            events.sum("logit.input.requests.rejected", &[("reason", "unknown_route")]),
            1.0
        );
        assert_eq!(events.sum("logit.input.requests", &[("route", "unknown")]), 1.0);
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
        let events = Totals::of(registry.drain(0));
        for (path, route) in cases {
            assert_eq!(
                events.sum("logit.input.requests.acknowledged", &[("route", route)]),
                1.0,
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
            Totals::of(registry.drain(0))
                .sum("logit.input.requests.rejected", &[("reason", "json_traces")]),
            2.0
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
            Totals::of(registry.drain(0))
                .sum("logit.input.requests.rejected", &[("reason", "malformed")]),
            4.0
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
        let events = Totals::of(registry.drain(0));
        assert_eq!(events.sum("logit.input.spans", &[]), 3.0);
        assert_eq!(events.sum("logit.input.requests", &[("route", "traces_v04")]), 1.0);
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
        let events = Totals::of(registry.drain(0));
        assert_eq!(events.sum("logit.input.requests", &[("class", "busy")]), 1.0);
        assert_eq!(events.sum("logit.input.batches.dropped", &[("reason", "busy")]), 1.0);
        assert_eq!(events.sum("logit.input.spans", &[]), 1.0, "only the first");
    }

    /// A batch no consumer takes is answered as busy is, but counted `closed_consumer`.
    #[tokio::test]
    async fn a_request_no_consumer_takes_is_answered_503_and_counted_closed_consumer() {
        let (registry, input) = metered();
        let (addr, rx) = start(input, 16).await;
        drop(rx);

        let response = post_raw(&addr, "/v0.4/traces", MSGPACK, &v04(1)).await;
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
    async fn the_unix_socket_serves_alongside_tcp_with_mode_0722() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new("both");
        let path = dir.0.join("apm.socket");
        let diag = Diagnostics::new("apm");
        let input = tcp().with_socket(&path).with_diagnostics(diag.clone());
        let (addr, mut rx) = start(input, 16).await;

        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o722);

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
    async fn with_socket_mode_sets_the_socket_files_mode() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new("mode");
        let path = dir.0.join("apm.socket");
        let mut input = DatadogTraceInput::new().with_socket(&path).with_socket_mode(0o660);
        input.bind().await.expect("bind");
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o660);
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

    // ---- shutdown -----------------------------------------------------------------------------
    //
    // Every test here goes through `spawn_input` and `Running`, so the listener runs
    // `run_until_shutdown` with a real signal. "Returned" means `Running::stop`'s 5s ceiling.

    /// The close grace (`handshake_timeout`, reused) for this section. Short, so a close that
    /// spends the whole grace returns 50x inside `Running::stop`'s 5s ceiling and never ties it. A
    /// grace cut short by a loaded host only drops the connection sooner, which every assertion
    /// here also reads as a close.
    const SHUTDOWN_GRACE: Duration = Duration::from_millis(100);

    /// A `busy_after` far longer than any test, so a send on a full edge parks in
    /// `send_with_deadline` instead of being answered `503` while the test waits on it.
    const LONG_BUSY: Duration = Duration::from_secs(3600);

    fn channel_sink() -> (Fanout, mpsc::Receiver<logit_pipeline::Delivered>) {
        let (tx, rx) = mpsc::channel(16);
        (Fanout::new(vec![tx]), rx)
    }

    /// Writes one `/v0.4/traces` request of one trace on a keep-alive connection.
    async fn write_keep_alive_traces<S: tokio::io::AsyncWrite + Unpin>(stream: &mut S) {
        let body = v04(1);
        let head = format!(
            "PUT /v0.4/traces HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n{MSGPACK}\r\n",
            body.len()
        );
        stream.write_all(head.as_bytes()).await.unwrap();
        stream.write_all(&body).await.unwrap();
    }

    /// Reads one response's head and its `Content-Length` body off a keep-alive connection,
    /// returning the head.
    async fn read_keep_alive_response<S: tokio::io::AsyncRead + Unpin>(
        stream: &mut S,
        what: &str,
    ) -> String {
        let read = async {
            let mut buf = Vec::new();
            let mut byte = [0u8; 1];
            while !buf.ends_with(b"\r\n\r\n") {
                let n = stream.read(&mut byte).await.unwrap();
                assert_eq!(n, 1, "{what}: the connection closed inside a response head");
                buf.push(byte[0]);
            }
            let head = String::from_utf8_lossy(&buf).into_owned();
            let length: usize =
                header_of(&head, "content-length").map_or(0, |v| v.parse().unwrap());
            let mut body = vec![0u8; length];
            stream.read_exact(&mut body).await.unwrap();
            head
        };
        tokio::time::timeout(logit_pipeline::test_util::RECV_TIMEOUT, read)
            .await
            .unwrap_or_else(|_| panic!("{what}: no response within the receive timeout"))
    }

    /// One request answered on a keep-alive connection, which is then idle (`KA::Idle`).
    async fn answered_keep_alive<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
        stream: &mut S,
        rx: &mut mpsc::Receiver<logit_pipeline::Delivered>,
        what: &str,
    ) {
        write_keep_alive_traces(stream).await;
        let head = read_keep_alive_response(stream, what).await;
        assert!(head.starts_with("HTTP/1.1 200"), "{what}: {head}");
        recv_batch(rx).await;
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
    async fn shutdown_closes_an_idle_keep_alive_tcp_connection_and_the_listener_returns() {
        let input = tcp().with_handshake_timeout(SHUTDOWN_GRACE);
        let (sink, mut rx) = channel_sink();
        let mut input = input;
        input.bind().await.unwrap();
        let addr = input.local_addr().unwrap().to_string();
        let running = logit_pipeline::test_util::spawn_input(input, sink).await;

        let mut client = tokio::net::TcpStream::connect(&addr).await.unwrap();
        answered_keep_alive(&mut client, &mut rx, "the TCP keep-alive request").await;

        running.stop().await;
        logit_pipeline::test_util::expect_closed(&mut client, "a TCP keep-alive connection").await;
        expect_inbox_closed(&mut rx).await;
    }

    /// The same over the Unix socket alone: the absent TCP listener's arm returns on the signal,
    /// and the Unix loop drains its own connection.
    #[tokio::test]
    async fn shutdown_closes_an_idle_keep_alive_unix_connection_and_the_listener_returns() {
        let dir = TempDir::new("shut");
        let path = dir.0.join("apm.socket");
        let input =
            DatadogTraceInput::new().with_socket(&path).with_handshake_timeout(SHUTDOWN_GRACE);
        let (sink, mut rx) = channel_sink();
        let running = logit_pipeline::test_util::spawn_input(input, sink).await;

        let mut client = UnixStream::connect(&path).await.unwrap();
        answered_keep_alive(&mut client, &mut rx, "the Unix keep-alive request").await;

        running.stop().await;
        logit_pipeline::test_util::expect_closed(&mut client, "a Unix keep-alive connection").await;
        expect_inbox_closed(&mut rx).await;
    }

    /// With both listeners up and an idle connection on each, the stop returns only once both
    /// loops have drained: both connections are closed.
    #[tokio::test]
    async fn shutdown_closes_connections_on_both_listeners() {
        let dir = TempDir::new("both-shut");
        let path = dir.0.join("apm.socket");
        let mut input = tcp().with_socket(&path).with_handshake_timeout(SHUTDOWN_GRACE);
        input.bind().await.unwrap();
        let addr = input.local_addr().unwrap().to_string();
        let (sink, mut rx) = channel_sink();
        let running = logit_pipeline::test_util::spawn_input(input, sink).await;

        let mut over_tcp = tokio::net::TcpStream::connect(&addr).await.unwrap();
        answered_keep_alive(&mut over_tcp, &mut rx, "the TCP keep-alive request").await;
        let mut over_unix = UnixStream::connect(&path).await.unwrap();
        answered_keep_alive(&mut over_unix, &mut rx, "the Unix keep-alive request").await;

        running.stop().await;
        logit_pipeline::test_util::expect_closed(&mut over_tcp, "the TCP connection").await;
        logit_pipeline::test_util::expect_closed(&mut over_unix, "the Unix connection").await;
        expect_inbox_closed(&mut rx).await;
    }

    /// One loop draining first doesn't end the other: the TCP loop's idle connection closes at
    /// once, and a request parked on the Unix socket is still served out, because the listener
    /// waits for both drains instead of dropping the loop still running.
    #[tokio::test]
    async fn shutdown_waits_for_both_listeners_to_drain() {
        let dir = TempDir::new("drain-both");
        let path = dir.0.join("apm.socket");
        let mut probe = logit_pipeline::test_util::TelemetryProbe::new();
        let mut input = tcp()
            .with_socket(&path)
            .with_telemetry(probe.telemetry("apm", "datadog_trace_in", "listener"))
            .with_handshake_timeout(SHUTDOWN_GRACE)
            .with_busy_after(LONG_BUSY);
        input.bind().await.unwrap();
        let addr = input.local_addr().unwrap().to_string();
        let (tx, mut rx) = mpsc::channel(1);
        let edge = logit_pipeline::fanout::Edge::new(tx)
            .with_telemetry(probe.telemetry("sink", "null_out", "sink"));
        let running =
            logit_pipeline::test_util::spawn_input(input, Fanout::from_edges(vec![edge])).await;

        // The TCP request fills the edge's one slot, and the Unix request parks behind it.
        let mut over_tcp = tokio::net::TcpStream::connect(&addr).await.unwrap();
        write_keep_alive_traces(&mut over_tcp).await;
        let head = read_keep_alive_response(&mut over_tcp, "the TCP request").await;
        assert!(head.starts_with("HTTP/1.1 200"), "{head}");
        let mut over_unix = UnixStream::connect(&path).await.unwrap();
        write_keep_alive_traces(&mut over_unix).await;
        probe
            .wait_for("the Unix request to park on the full edge", |t| {
                t.sum("logit.component.inbox.full", &[]) >= 1.0
            })
            .await;

        running.shutdown.send(true).unwrap();
        logit_pipeline::test_util::expect_closed(&mut over_tcp, "the TCP connection").await;
        recv_batch(&mut rx).await;
        recv_batch(&mut rx).await;
        let head = read_keep_alive_response(&mut over_unix, "the parked Unix request").await;
        assert!(head.starts_with("HTTP/1.1 200"), "{head}");
        logit_pipeline::test_util::expect_closed(&mut over_unix, "the Unix connection").await;
        tokio::time::timeout(logit_pipeline::test_util::RECV_TIMEOUT, running.handle)
            .await
            .expect("the listener should return once both loops drained")
            .expect("the listener task should not panic")
            .expect("the listener should return Ok");
        expect_inbox_closed(&mut rx).await;
    }

    /// A listener whose one consumer is a capacity-1 edge carrying its own telemetry, so a send
    /// parked on it counts `logit.component.inbox.full` the moment it parks; then two requests on
    /// one keep-alive connection, the first filling the edge's slot and the second parked.
    async fn parked_request() -> (
        tokio::net::TcpStream,
        logit_pipeline::test_util::Running,
        mpsc::Receiver<logit_pipeline::Delivered>,
        logit_pipeline::test_util::TelemetryProbe,
    ) {
        let mut probe = logit_pipeline::test_util::TelemetryProbe::new();
        let mut input = tcp()
            .with_telemetry(probe.telemetry("apm", "datadog_trace_in", "listener"))
            .with_handshake_timeout(SHUTDOWN_GRACE)
            .with_busy_after(LONG_BUSY);
        input.bind().await.unwrap();
        let addr = input.local_addr().unwrap().to_string();
        let (tx, rx) = mpsc::channel(1);
        let edge = logit_pipeline::fanout::Edge::new(tx)
            .with_telemetry(probe.telemetry("sink", "null_out", "sink"));
        let running =
            logit_pipeline::test_util::spawn_input(input, Fanout::from_edges(vec![edge])).await;

        let mut client = tokio::net::TcpStream::connect(&addr).await.unwrap();
        write_keep_alive_traces(&mut client).await;
        let head = read_keep_alive_response(&mut client, "the request that fills the slot").await;
        assert!(head.starts_with("HTTP/1.1 200"), "{head}");
        write_keep_alive_traces(&mut client).await;
        probe
            .wait_for("the second request to park on the full edge", |t| {
                t.sum("logit.component.inbox.full", &[]) >= 1.0
            })
            .await;
        (client, running, rx, probe)
    }

    /// A request parked in `send_with_deadline` when shutdown fires is served out: both batches
    /// reach the consumer, the client gets its 200, then the connection closes and the listener
    /// returns.
    #[tokio::test]
    async fn shutdown_serves_a_parked_request_out_then_closes_its_connection() {
        let (mut client, running, mut rx, _probe) = parked_request().await;

        running.shutdown.send(true).unwrap();
        recv_batch(&mut rx).await;
        recv_batch(&mut rx).await;
        let head = read_keep_alive_response(&mut client, "the parked request").await;
        assert!(head.starts_with("HTTP/1.1 200"), "{head}");
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
    /// connection tasks: the connection closes and the gauge reads 0. The parked request's
    /// reservation drops with its task, so its batch is never sent: only the first arrives, and
    /// the client resends what it wasn't answered for.
    #[tokio::test]
    async fn dropping_the_listener_future_aborts_its_connections_and_the_parked_send() {
        let (mut client, running, mut rx, mut probe) = parked_request().await;

        running.handle.abort();
        logit_pipeline::test_util::expect_closed(&mut client, "an aborted connection").await;
        recv_batch(&mut rx).await;
        let next = tokio::time::timeout(logit_pipeline::test_util::RECV_TIMEOUT, rx.recv())
            .await
            .expect("the inbox should close once the listener's tasks are aborted");
        assert!(next.is_none(), "the parked request's batch was never sent");
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
        let mut probe = logit_pipeline::test_util::TelemetryProbe::new();
        let settings = TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: None,
        };
        let mut input = tcp()
            .with_tls(&settings, &testdata_tls_dir(), &logit_pipeline::tls::TlsReloader::new())
            .unwrap()
            .with_telemetry(probe.telemetry("apm", "datadog_trace_in", "listener"))
            .with_handshake_timeout(Duration::from_secs(3600));
        input.bind().await.unwrap();
        let addr = input.local_addr().unwrap().to_string();
        let (sink, _rx) = channel_sink();
        let running = logit_pipeline::test_util::spawn_input(input, sink).await;

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
        configure: impl FnOnce(DatadogTraceInput) -> DatadogTraceInput,
    ) -> Stamping {
        let probe = logit_pipeline::test_util::TelemetryProbe::new();
        let telemetry = probe.telemetry("apm", "datadog_trace_in", "listener");
        let diag = Diagnostics::new("apm").with_telemetry(telemetry.clone());
        let input = configure(
            tcp()
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
        header.extend_from_slice(&8126u16.to_be_bytes());
        header
    }

    /// A v2 `LOCAL` header, a proxy's own health check, which names no origin.
    fn v2_local_header() -> Vec<u8> {
        let mut header = logit_proto::proxy::V2_SIGNATURE.to_vec();
        header.extend_from_slice(&[0x20, 0x00, 0x00, 0x00]);
        header
    }

    const V1_HEADER: &[u8] = b"PROXY TCP4 198.51.100.7 127.0.0.1 40000 8126\r\n";

    /// The HTTP/1.1 request carrying a two-trace v0.4 body, `Connection: close`.
    fn traces_request(host: &str) -> Vec<u8> {
        traces_request_with(host, &[])
    }

    /// [`traces_request`], with `headers` added.
    fn traces_request_with(host: &str, headers: &[(&str, &str)]) -> Vec<u8> {
        let body = v04(2);
        let extra: String =
            headers.iter().map(|(name, value)| format!("{name}: {value}\r\n")).collect();
        let mut wire = format!(
            "PUT /v0.4/traces HTTP/1.1\r\nHost: {host}\r\nContent-Length: {}\r\n{MSGPACK}\
             Connection: close\r\n{extra}\r\n",
            body.len()
        )
        .into_bytes();
        wire.extend_from_slice(&body);
        wire
    }

    /// Connects, writes `prefix`, then the two-trace request. Returns the response and the
    /// client's local port.
    async fn put_traces_after(addr: &str, prefix: &[u8]) -> (String, u16) {
        put_traces_with(addr, prefix, &[]).await
    }

    /// [`put_traces_after`], with `headers` added to the request.
    async fn put_traces_with(addr: &str, prefix: &[u8], headers: &[(&str, &str)]) -> (String, u16) {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let port = stream.local_addr().unwrap().port();
        let mut wire = prefix.to_vec();
        wire.extend_from_slice(&traces_request_with(addr, headers));
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

    /// The one batch the two-trace body decodes into, with every event checked against the
    /// expected `network.peer.*` and `client.*` values.
    async fn expect_stamped_batch(
        rx: &mut mpsc::Receiver<logit_pipeline::Delivered>,
        peer: Option<(&str, Option<u16>)>,
        client: Option<(&str, i64)>,
    ) {
        let batch = recv_batch(rx).await;
        assert_eq!(batch.events.len(), 2, "one event per span");
        for event in &batch.events {
            assert_eq!(str_attr(event, "network.peer.address"), peer.map(|(address, _)| address));
            assert_eq!(
                event.attributes.get("network.peer.port"),
                peer.and_then(|(_, port)| port)
                    .map(|port| logit_core::Value::I64(i64::from(port)))
                    .as_ref()
            );
            assert_eq!(str_attr(event, "client.address"), client.map(|(address, _)| address));
            assert_eq!(
                event.attributes.get("client.port"),
                client.map(|(_, port)| logit_core::Value::I64(port)).as_ref()
            );
        }
    }

    #[tokio::test]
    async fn peer_stamps_every_event_of_a_request_with_the_socket_peer() {
        for peer in [true, false] {
            let mut running = sender_input(peer, false, |i| i).await;
            let (response, port) = put_traces_after(&running.addr, b"").await;
            assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
            let expected = peer.then_some(("127.0.0.1", Some(port)));
            expect_stamped_batch(&mut running.rx, expected, None).await;
        }
    }

    /// The proxy is the socket peer, and the v2 header names the client.
    #[tokio::test]
    async fn a_v2_header_stamps_the_client_beside_the_peer() {
        let mut running = sender_input(true, true, |i| i).await;
        let (response, port) = put_traces_after(&running.addr, &v2_ipv4_header()).await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
        let client = Some(("203.0.113.5", 41000));
        expect_stamped_batch(&mut running.rx, Some(("127.0.0.1", Some(port))), client).await;
    }

    #[tokio::test]
    async fn a_v1_header_stamps_the_client() {
        let mut running = sender_input(false, true, |i| i).await;
        let (response, _port) = put_traces_after(&running.addr, V1_HEADER).await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
        expect_stamped_batch(&mut running.rx, None, Some(("198.51.100.7", 40000))).await;
    }

    fn testdata_tls_dir() -> PathBuf {
        // The repo root's `testdata/tls` (`testdata/tls/README.md`).
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
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
        let tls = |input: DatadogTraceInput| {
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
        tls_stream.write_all(&traces_request("localhost")).await.unwrap();
        let mut buf = Vec::new();
        let _ = tokio::time::timeout(
            logit_pipeline::test_util::RECV_TIMEOUT,
            tls_stream.read_to_end(&mut buf),
        )
        .await;
        let response = String::from_utf8_lossy(&buf);
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
        expect_stamped_batch(&mut running.rx, None, Some(("203.0.113.5", 41000))).await;
    }

    /// A direct request on a `proxy_protocol: true` port is closed, counted, and never decoded.
    #[tokio::test]
    async fn a_request_without_a_proxy_header_is_rejected_and_counted() {
        let mut running = sender_input(false, true, |i| i).await;
        let mut bare = tokio::net::TcpStream::connect(&running.addr).await.unwrap();
        bare.write_all(b"PUT /v0.4/traces HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n")
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
            let (response, _port) = put_traces_after(&running.addr, &v2_local_header()).await;
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

    /// Writes the two-trace request on `stream` and returns the response.
    async fn put_traces_on(mut stream: UnixStream) -> String {
        stream.write_all(&traces_request("localhost")).await.unwrap();
        let mut buf = Vec::new();
        let _ = tokio::time::timeout(
            logit_pipeline::test_util::RECV_TIMEOUT,
            stream.read_to_end(&mut buf),
        )
        .await;
        String::from_utf8_lossy(&buf).into_owned()
    }

    /// A tracer's usual Unix client binds no path, so `peer:` has nothing to stamp, and a socket
    /// connection is never read for a PROXY header even with `proxy_protocol:` on for `bind`.
    #[tokio::test]
    async fn an_unbound_unix_client_is_stamped_with_nothing() {
        let dir = TempDir::new("peer-unbound");
        let path = dir.0.join("apm.socket");
        let mut running = sender_input(true, true, |input| input.with_socket(path.clone())).await;
        let response = put_traces_on(UnixStream::connect(&path).await.unwrap()).await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
        expect_stamped_batch(&mut running.rx, None, None).await;
        assert_eq!(running.diag.occurrences("proxy_header"), 0);
    }

    /// A Unix client that bound a path is stamped with it, and no port.
    #[tokio::test]
    async fn a_bound_unix_client_is_stamped_with_its_path_and_no_port() {
        let dir = TempDir::new("peer-bound");
        let path = dir.0.join("apm.socket");
        let client_path = dir.0.join("client.socket");
        let mut running = sender_input(true, false, |input| input.with_socket(path.clone())).await;
        // Binding before connecting needs a raw socket; tokio's `UnixStream` has no such step.
        let socket =
            socket2::Socket::new(socket2::Domain::UNIX, socket2::Type::STREAM, None).unwrap();
        socket.bind(&socket2::SockAddr::unix(&client_path).unwrap()).unwrap();
        socket.connect(&socket2::SockAddr::unix(&path).unwrap()).unwrap();
        let std_stream: std::os::unix::net::UnixStream = socket.into();
        std_stream.set_nonblocking(true).unwrap();
        let response = put_traces_on(UnixStream::from_std(std_stream).unwrap()).await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
        let client_path = client_path.to_str().unwrap();
        expect_stamped_batch(&mut running.rx, Some((client_path, None)), None).await;
    }

    // ---- sender address: `forwarded:` ---------------------------------------------------------

    /// The one batch the request decodes into, each event carrying `client.address`
    /// `address` and `client.port` `port`, absent where `None`.
    async fn expect_forwarded_client(
        rx: &mut mpsc::Receiver<logit_pipeline::Delivered>,
        address: Option<&str>,
        port: Option<i64>,
        case: &str,
    ) {
        for _ in 0..1 {
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
            let (response, _) = put_traces_with(&running.addr, &prefix, headers).await;
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
        let (response, port) = put_traces_with(&running.addr, &v2_ipv4_header(), &headers).await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
        for _ in 0..1 {
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
