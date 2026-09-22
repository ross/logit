//! OTLP input -- accepts logs, metrics, and traces over either OTLP/HTTP (protobuf-over-POST) or
//! OTLP/gRPC, selected by `protocol` in config (`logit_config::OtlpProtocol`). See
//! `docs/adr/hand-rolled-grpc-over-hyper.md` for why the gRPC server is a ~200-line hand-rolled
//! `hyper::server::conn::http2` service rather than `tonic`, and why that's budgeted as roughly
//! half of this whole PR's effort -- HTTP/2 trailers, per-method routing, and gRPC status-code
//! framing are all things `tonic` gives away for free and this input has to build by hand.
//!
//! **One listener, one accept loop, one handler per connection.** [`Input::run`] binds a single
//! `TcpListener` and `tokio::spawn`s a handler per accepted connection -- [`Fanout`] is already
//! `Clone`, which is the whole mechanism that lets an arbitrary number of concurrent connections
//! each hold their own handle to the same downstream sink. HTTP connections are served by
//! [`hyper_util::server::conn::auto::Builder`], which transparently handles both HTTP/1.1 (what a
//! `curl`/most OTel HTTP exporters speak) and h2c (prior-knowledge HTTP/2 without TLS, what
//! `curl --http2-prior-knowledge` and some exporters prefer); gRPC connections are served by
//! [`hyper::server::conn::http2::Builder`] directly, since gRPC *is* HTTP/2 -- there's no h1
//! fallback to auto-detect.
//!
//! **This is the first listener with real backpressure to its source.** Every other input here is
//! UDP (statsd, syslog): a slow downstream just means the kernel silently drops datagrams. TCP
//! (both OTLP transports) has no such escape hatch -- a slow `sink.send(batch).await` blocks the
//! handler, which stalls reading the next request off that connection, which the client
//! eventually feels as its own write blocking. Correct for a reliable protocol (an OTLP exporter
//! is expected to retry/buffer on its own timeout, not silently lose data), and worth knowing
//! going in: `docs/design/pipeline-graph.md`'s backpressure section.
//!
//! **TLS termination is optional, per listener.** `tls:` in config (see [`TlsServerSettings`])
//! turns it on for both transports; a listener with none accepts plaintext HTTP/1.1, h2c, and h2
//! exactly as before. The TLS handshake itself runs *inside* the per-connection spawned task,
//! after that connection's [`MAX_CONCURRENT_CONNECTIONS`] permit is acquired -- a slow or hostile
//! handshake stalls only its own connection, counts against the same concurrency bound as a slow
//! request, and can't block the accept loop from serving the next connection
//! (`docs/adr/otlp-tls-and-pooled-grpc-client.md`).
//!
//! **Connection limit: reject, don't queue.** A [`tokio::sync::Semaphore`] capped at
//! [`MAX_CONCURRENT_CONNECTIONS`], acquired with `try_acquire_owned` -- at capacity the accepted
//! stream is dropped immediately and counted as
//! `logit.input.connections.rejected{reason="limit"}`, rather than being parked behind a permit
//! that may never come. Exactly `syslog_in`'s driver
//! (`crates/logit-inputs/src/tcp.rs`'s "Connection limit" section) and `logit_in`, and the
//! rejection happens *before* any TLS accept: OTLP has no in-band "try later" of its own to
//! deliver, so there is nothing to say and no reason to spend a handshake saying it. The
//! `logit.input.connections` gauge counts permit holders only. This replaces an earlier blocking
//! `acquire_owned().await`, under which a connection past the cap stalled the accept loop itself
//! -- enough silent connections then stopped this listener draining its backlog at all.
//!
//! **Handshake timeout.** [`OtlpInput::handshake_timeout`] (a field, defaulted to
//! [`HANDSHAKE_TIMEOUT`] and set from config by [`OtlpInput::with_handshake_timeout`]) bounds each
//! of a connection's pre-request phases, the same shape `syslog_in`'s driver uses
//! (`crates/logit-inputs/src/tcp.rs`'s "Pre-handshake timeout" section): on a TLS listener the
//! TLS accept, and on a plaintext one -- which has no TLS accept for it to bound -- the wait for
//! the connection's very first byte. Without either, a client that completes the TCP connect and
//! then never speaks pins a connection-limit permit forever.
//!
//! **The plaintext first-byte bound is a `peek`, not a read.** `tokio::net::TcpStream::peek` is
//! `recv(..., MSG_PEEK)`: it waits for the first byte to become *available* and leaves it in the
//! socket's receive queue, so the stream handed to `hyper` afterwards is byte-for-byte the one it
//! would have been with no bound at all. [`hyper_util::server::conn::auto::Builder`]'s own
//! `ReadVersion` sniff (up to 24 bytes, telling HTTP/1.1 from an h2 preface) then reads those
//! bytes itself and needs no rewind buffer -- which is the whole reason the bound is a peek and
//! not a wrapper around the sniff, since wrapping the sniff would mean reimplementing it. The TLS
//! arm deliberately gets no peek: `acceptor.accept` already waits on that connection's first
//! bytes under the same budget, so a peek ahead of it would bound nothing the handshake does not.
//!
//! **A peer that closes cleanly before sending anything is not a fault.** That is what every TCP
//! health check looks like -- `demo/haproxy/haproxy.cfg`'s `server logit logit:4318 check` against
//! `demo/logit.yaml`'s plaintext `browser_in`, a Kubernetes `tcpSocket` probe, `nc -z` -- and
//! before this peek existed `auto::Builder`'s `ReadVersion` read the immediate EOF as
//! `Version::H1` and the connection ended silently. So a `peek` of `Ok(0)` returns `Ok(())`, and
//! only the *deadline* (a connection held open saying nothing) and a genuine read error reach
//! `connection_error`. `crate::tcp` makes the same call for an EOF before its first frame.
//!
//! **Idle timeout.** [`OtlpInput::with_idle_timeout`] -- `otlp_in`'s operator-facing
//! `idle_timeout:` field, and off unless set -- bounds how long a connection may sit with no
//! request in flight before this listener closes it and hands its permit back
//! (`docs/adr/idle-connection-timeout.md`). Without it, a connection that sends *one* byte and
//! then stops has cleared the peek, is inside `hyper`'s own read loop, and holds its permit
//! indefinitely -- and under `protocol: grpc` the same is true of one byte of the HTTP/2 preface.
//!
//! *Tracked at the service, not at the socket.* One [`Activity`] per connection counts the
//! requests in flight and stamps the instant the last one finished ([`InFlight`](crate::http::InFlight), the guard
//! `service_fn` wraps each handler in, so an early return or an unwind stamps it too); the clock
//! is armed only while that count is zero. A timer wrapped around the IO instead would be wrong
//! here: hyper 1.11.1's h1 server polls the socket read *mid-message*
//! (`mid_message_detect_eof`'s `force_io_read`, so it can notice a peer closing while a handler
//! is still working), so an IO-level timer would tick during ordinary backpressure and read a
//! stalled downstream as a silent peer -- the exact failure the shared driver's reset rule exists
//! to avoid (`crates/logit-inputs/src/tcp.rs`'s "Idle timeout" section), relocated into hyper's
//! internals where this module could not see it.
//!
//! *Reset on request completion, not on bytes.* hyper owns the bytes, so the finest grain this
//! listener can see is a request starting and finishing. A request *head* that dribbles in more
//! slowly than `idle_timeout` on an otherwise-quiet keep-alive connection is therefore closed:
//! a documented narrowing of the one semantic every other listener implements, not a bug. A
//! request *body* that stalls mid-upload gets a narrower bound of its own instead --
//! [`collect_with_stall_bound`] puts a per-frame `timeout` on the body, answers `408` (HTTP) or
//! `grpc-status: 4` (gRPC), and closes the connection once the handler has returned, rather than
//! leaving a half-uploaded request to the whole-connection deadline.
//!
//! *`graceful_shutdown`, then a bounded grace, then drop.* [`drive_with_idle`] never drops a live
//! socket out from under hyper: it calls `graceful_shutdown`, polls the connection for at most
//! `handshake_timeout` (reused as the grace -- no new knob), and then drops it whatever that poll
//! returned. Both steps are load-bearing, verified against the pinned hyper 1.11.1 / hyper-util
//! 0.1.20 sources rather than assumed: `graceful_shutdown` closes an *idle keep-alive* h1
//! connection promptly (`disable_keep_alive` calls `state.close()` when the connection's `KA`
//! state is `Idle`) and GOAWAYs an established h2 one -- the common case for a connection this
//! tracker considers idle. But a *fresh* h1 connection stopped mid-head is `KA::Busy` and keeps
//! waiting regardless, hyper-util's own pre-sniff `ReadVersion` future resolves to
//! `Err("Cancelled")`, and an h2 connection still handshaking only sets an internal
//! `close_pending` flag. The bounded grace-then-drop step exists for exactly those three, which
//! is why the post-shutdown result is deliberately ignored. The one thing the drop waits for is
//! a request that *started* inside the grace window and has not returned: dropping the
//! connection while its handler is parked in `Fanout::send` would discard a batch that never
//! reached the fanout, so [`drive_with_idle`] polls that request out and then lets the grace run
//! again for its response. Nothing a *silent* peer does can extend the window -- only being
//! served can.
//!
//! *Policy, not a fault.* An idle close counts `logit.input.connections.closed{reason="idle"}`
//! and returns `Ok(())`, so it never reaches the `connection_error` diagnostic below -- counted,
//! not diagnosed, the same call `crate::tcp` makes for its own idle closes.
//!
//! *Why not `hyper`'s own `http1().header_read_timeout(..)`.* Still rejected, and still not
//! installed: in the pinned hyper 1.11.1 (`src/proto/h1/conn.rs`) that timer is armed at the
//! *top* of `poll_read_head`, before a single header byte has been parsed, and `State::idle` sets
//! `notify_read = true` whenever it is configured, with the comment "Next read will start and
//! poll the header read timeout, so we can close the connection if another header isn't received
//! in a timely manner" -- so it re-arms across every idle keep-alive gap. That is an idle timeout
//! wearing a first-head name, reachable only by also bounding first heads, and h1-only
//! ([`hyper::server::conn::http2::Builder`] has no equivalent knob). `idle_timeout` is that bound
//! made explicit, opt-in, and available on both transports.
//!
//! **Gzip is supported; nothing else is.** `Content-Encoding: gzip` (HTTP) and a gRPC frame's own
//! compressed flag plus `grpc-encoding: gzip` are both decoded via [`inflate`]; any other declared
//! encoding is rejected (`415`/`grpc-status: 12`) rather than silently mishandled. Decompressing
//! untrusted input is real, security-relevant surface (a compression-bomb-shaped request), so
//! `inflate` bounds the *decompressed* size to [`MAX_REQUEST_BYTES`] -- the same cap already
//! enforced on the compressed body -- rather than trusting the input to be well-behaved. See
//! `docs/adr/otlp-compression-and-decompression-bounds.md`.
//!
//! **OTLP/HTTP accepts protobuf or JSON; OTLP/gRPC accepts protobuf only.** `handle_http` picks
//! the decode path off `Content-Type` (absent/empty means protobuf, preserved for every client
//! that predates OTLP/JSON support); the success response mirrors whichever encoding the request
//! used, per spec. gRPC is unaffected -- OTLP/gRPC's framing *is* protobuf by definition, and no
//! OTel SDK speaks `application/grpc+json`. See
//! [ADR `otlp-json-decoding`](../../../../docs/adr/otlp-json-decoding.md) for the JSON dialect
//! itself (hex vs. base64 ids, string-or-number 64-bit fields, and why it's hand-parsed rather
//! than generated). Every error response, on both encodings, stays `text/plain` -- the spec wants
//! a protobuf-encoded `Status` message even for a JSON request's error; tracked in
//! `docs/known-gaps.md` as a pre-existing deviation, not something this input's OTLP/JSON support
//! introduced.
//!
//! **Size and concurrency limits.** `MAX_REQUEST_BYTES` (4 MiB) matches the OTel collector's own
//! default `max_recv_msg_size`; a request over that is rejected (`413`/`grpc-status: 8`,
//! `RESOURCE_EXHAUSTED`) before it can grow an unbounded buffer. That bounds one connection's
//! worst case, not the listener's as a whole -- `MAX_CONCURRENT_CONNECTIONS` bounds how many
//! connections `run` serves at once (rejecting past it, see "Connection limit" above), so total
//! worst-case memory stays a real (if generous) number rather than unbounded.
//!
//! **The response's `partial_success` is always empty on a successful decode.** OTLP's own
//! `Export*ServiceResponse.partial_success` exists to report *which* records within an otherwise-
//! accepted request were rejected -- but `logit_proto::SignalDecoder::decode_signal` doesn't
//! return a per-call skip/reject count today (it only counts skips against its own
//! `logit.input.metrics.skipped{metric_kind, reason}` telemetry, `logit-proto`'s `otlp::metrics`
//! module doc); there's nothing for this input to echo back into the wire response yet. A fully
//! malformed request (bad protobuf, an out-of-range span id) still fails the *whole* request
//! (`400`/`grpc-status: 3`), which is the one case this input's response does reflect correctly.
//! Threading a real per-call count through would be a `SignalDecoder` API change, out of this PR's
//! scope -- tracked in `docs/known-gaps.md`.

use crate::http::{
    body_read_error_message, collect_with_stall_bound, drive_with_idle, Activity, BodyReadError,
};
use crate::Input;
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use http_body_util::{Full, Limited};
// Only this module's own tests still collect a body directly -- `collect_with_stall_bound`, the
// lib path's only body reader, moved to `crate::http`.
#[cfg(test)]
use http_body_util::BodyExt;
use hyper::body::{Frame, Incoming};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use logit_core::{Diagnostics, Telemetry};
use logit_pipeline::Fanout;
use logit_proto::otlp::OtlpDecoder;
use logit_proto::{Signal, SignalDecoder};
// Only the test module's own `tls_connector` (a canned TLS client) still reads PEM files
// directly -- server-side TLS config building moved to `crate::tls` (workstream B).
#[cfg(test)]
use rustls_pki_types::pem::PemObject;
#[cfg(test)]
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

/// Matches the OTel collector's own default `max_recv_msg_size` -- see this module's doc comment.
const MAX_REQUEST_BYTES: usize = 4 * 1024 * 1024;

/// Bounds the number of connections [`Input::run`] serves concurrently -- without this, the
/// per-request cap [`MAX_REQUEST_BYTES`] bounds only *one* connection's worst case, and an
/// unbounded number of them can each be holding that much. 1024 is
/// the same order of magnitude `logit_pipeline::SinkQueueConfig::default`'s `max_batches` already
/// uses elsewhere in this codebase for "a generous but real bound, not unlimited" -- worst case
/// `1024 * MAX_REQUEST_BYTES` = 4 GiB in flight, not unbounded. The same number `logit_in` and
/// `syslog_in` use: there is no protocol reason for an OTLP listener to differ, and one shared
/// figure is one thing for an operator to learn. Not (yet) operator-tunable; revisit
/// as a config field if a real deployment needs a different number.
///
/// **A connection past the cap is rejected, not queued** -- see this module's "Connection limit"
/// doc section. [`OtlpInput::with_max_connections`] overrides this in tests, so the cap is
/// reachable with two connections instead of 1025.
///
/// **That 4 GiB figure is the protobuf path's worst case, not the JSON one's.** An OTLP/JSON
/// request (`docs/adr/otlp-json-decoding.md`) is parsed into a `serde_json::Value` tree before it
/// ever reaches the decoded event model -- a `Map`/`Vec`/`String`/`Number` allocation per JSON
/// node, several times the source bytes for a typically-nested OTLP payload, where the protobuf
/// path's `prost::Message::decode` builds the target structs directly with none of that
/// intermediate tree. The *bound* still holds -- one connection's JSON body is still capped at
/// `MAX_REQUEST_BYTES` before parsing starts, so total worst-case memory across all connections is
/// still a real, finite multiple of 4 GiB, not unbounded -- it just isn't exactly 4 GiB any more
/// for an all-JSON worst case. No number is asserted here rather than guessed; tracked in
/// `docs/known-gaps.md` for whoever needs a measured one.
const MAX_CONCURRENT_CONNECTIONS: usize = 1024;

/// Default for [`OtlpInput::handshake_timeout`] -- how long a connection has, per pre-request
/// phase, before this listener gives up on it and releases its
/// [`MAX_CONCURRENT_CONNECTIONS`] permit: its TLS accept on a TLS listener, its first byte on a
/// plaintext one. The same 5s `logit_in` and `syslog_in` default to
/// (`crates/logit-inputs/src/logit.rs`, `crates/logit-inputs/src/tcp.rs`), and mirrored by hand in
/// `logit_config`'s own `default_handshake_timeout`: one number across every TCP listener is one
/// thing for an operator to learn. Overridden by `otlp_in`'s `handshake_timeout:` config field
/// through [`OtlpInput::with_handshake_timeout`]. Reused as the grace period an idle close gives
/// hyper to shut down in -- see this module's "Handshake timeout" and "Idle timeout" doc
/// sections.
const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Which OTLP wire transport this listener accepts. See `logit_outputs::otlp::OtlpTransport`'s
/// identical doc comment -- same reasoning, mirrored independently rather than shared, since this
/// crate doesn't depend on `logit-config` any more than `logit-outputs` does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpTransport {
    Http,
    Grpc,
}

/// `crate::tls::TlsServerSettings`, re-exported at this path -- `logit_in` (`crates/logit-inputs/
/// src/logit.rs`) shares the same type and TLS-config builder now (`docs/plans/
/// native-transport.md` workstream B); kept reachable as `otlp::TlsServerSettings` so
/// `logit-cli::pipeline::build_spec`'s existing `logit_inputs::otlp::TlsServerSettings` path needs
/// no change.
pub use crate::tls::TlsServerSettings;

pub struct OtlpInput {
    bind: String,
    transport: OtlpTransport,
    diag: Diagnostics,
    telemetry: Telemetry,
    tls: Option<Arc<rustls::ServerConfig>>,
    /// Set by [`Input::bind`], taken back out by [`Input::run`] -- `docs/plans/operator-surface.md`,
    /// workstream B. `None` after a run, so a second run rebinds, same as before this field
    /// existed.
    listener: Option<TcpListener>,
    /// See this module's own "Handshake timeout" doc section.
    handshake_timeout: std::time::Duration,
    /// `None` -- the default -- means no idle timeout at all, the behaviour this listener had
    /// before the field existed. See [`Self::with_idle_timeout`] and this module's "Idle timeout"
    /// doc section.
    idle_timeout: Option<std::time::Duration>,
    /// [`MAX_CONCURRENT_CONNECTIONS`] unless [`OtlpInput::with_max_connections`] (test-only)
    /// lowers it -- see this module's "Connection limit" doc section.
    max_connections: usize,
}

impl OtlpInput {
    pub fn new(bind: impl Into<String>, transport: OtlpTransport) -> Self {
        Self {
            bind: bind.into(),
            transport,
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
            tls: None,
            listener: None,
            handshake_timeout: HANDSHAKE_TIMEOUT,
            idle_timeout: None,
            max_connections: MAX_CONCURRENT_CONNECTIONS,
        }
    }

    /// The address actually bound, once [`Input::bind`] has run -- lets a caller (a test, or a
    /// future admin-server precedent) learn the OS-assigned port without a bind-drop-rebind race.
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
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

    /// Turns on TLS termination for this listener (`tls:` in config) -- both transports. Every
    /// path in `settings` is resolved against `base_dir` (the config file's own directory), same
    /// as `logit-cli::pipeline::build_spec` resolves `lua_file`.
    pub fn with_tls(
        mut self,
        settings: &TlsServerSettings,
        base_dir: &Path,
    ) -> anyhow::Result<Self> {
        let alpn: &[&[u8]] = match self.transport {
            OtlpTransport::Http => &[b"h2", b"http/1.1"],
            OtlpTransport::Grpc => &[b"h2"],
        };
        self.tls = Some(Arc::new(crate::tls::build_server_config(settings, base_dir, alpn)?));
        Ok(self)
    }

    /// Overrides [`HANDSHAKE_TIMEOUT`] for both pre-request budgets -- the TLS accept on a TLS
    /// listener, the first-byte peek on a plaintext one -- which is what `otlp_in`'s
    /// `handshake_timeout:` config field sets. The constant stays the default when this is never
    /// called; a test uses it to observe a silent connection actually being closed without a
    /// multi-second sleep. Graph rule 45 rejects `0s` before it can reach here. See this module's
    /// "Handshake timeout" doc section.
    pub fn with_handshake_timeout(mut self, handshake_timeout: std::time::Duration) -> Self {
        self.handshake_timeout = handshake_timeout;
        self
    }

    /// Bounds how long a connection may sit with no request in flight before this listener closes
    /// it -- `otlp_in`'s `idle_timeout:` config field, and off (`None`) when never called. See
    /// this module's "Idle timeout" doc section for why the clock lives at the service rather than
    /// around the socket, why it resets on request *completion* rather than on bytes, and why the
    /// close is `graceful_shutdown` plus a bounded grace rather than a drop. Graph rule 53 rejects
    /// `Some(0s)` before it can reach here.
    ///
    /// Takes the `Option` rather than a bare `Duration`, exactly like
    /// `crate::tcp::TcpListener::with_idle_timeout`: the "no idle timeout" case is then one call
    /// from a config that omitted the field rather than a caller-side `if let`.
    pub fn with_idle_timeout(mut self, idle_timeout: Option<std::time::Duration>) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    /// Test-only override of [`MAX_CONCURRENT_CONNECTIONS`] -- opening 1025 real connections to
    /// exercise the cap would be slow and flaky; this makes the cap reachable with two. The twin
    /// of `crate::tcp::TcpListener::with_max_connections`.
    #[cfg(test)]
    fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }
}

#[async_trait::async_trait]
impl Input for OtlpInput {
    async fn bind(&mut self) -> anyhow::Result<()> {
        if self.listener.is_some() {
            return Ok(()); // idempotent, per `Input::bind`'s contract
        }
        let listener = TcpListener::bind(&self.bind).await?;
        self.diag.info("bound", format_args!("listening on {}", self.bind));
        self.listener = Some(listener);
        Ok(())
    }

    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        self.bind().await?;
        let listener = self.listener.take().expect("bind() leaves a listener behind");
        // Bounds this input's worst-case memory the same way `MAX_REQUEST_BYTES` bounds one
        // request's -- see [`MAX_CONCURRENT_CONNECTIONS`]'s own doc comment for the reasoning and
        // the resulting worst case.
        let connection_limit = Arc::new(tokio::sync::Semaphore::new(self.max_connections));
        // Built once outside the loop -- `TlsAcceptor::from` just wraps the `Arc<ServerConfig>`,
        // so cloning it per connection below is cheap (an `Arc` clone, not a config rebuild).
        let tls_acceptor = self.tls.clone().map(TlsAcceptor::from);
        let live_connections = Arc::new(AtomicI64::new(0));
        let handshake_timeout = self.handshake_timeout;
        let idle_timeout = self.idle_timeout;
        // `crate::tcp`'s accept-queue gauges, shared rather than reimplemented: the same
        // `accept()` this loop already awaited, plus `logit.input.accept_queue.depth`/
        // `.utilization` sampled before each accept and once a second while waiting for one.
        let mut accept_queue =
            crate::tcp::AcceptQueueSampler::new(self.telemetry.clone(), self.diag.clone());
        loop {
            let (stream, _peer) = accept_queue.accept(&listener).await?;

            // Non-blocking (`try_acquire_owned`, not `acquire_owned`): at capacity the connection
            // is closed immediately rather than queued behind a permit that may never come. And
            // it is closed *here*, before any TLS accept -- see this module's "Connection limit"
            // doc section. Copied from `crate::tcp`'s accept loop.
            let Ok(permit) = connection_limit.clone().try_acquire_owned() else {
                self.telemetry.count(
                    "logit.input.connections.rejected",
                    1.0,
                    &[("reason", "limit")],
                );
                drop(stream);
                continue;
            };

            let sink = sink.clone();
            let transport = self.transport;
            let mut diag = self.diag.clone();
            let telemetry = self.telemetry.clone();
            let tls_acceptor = tls_acceptor.clone();
            let live_connections = Arc::clone(&live_connections);
            tokio::spawn(async move {
                let _permit = permit; // held for the connection's lifetime; released on drop

                // Published from the read-modify-write's own return value, not a separate `load`:
                // `Telemetry::gauge` is last-write-wins per key, so two tasks that interleave an
                // add and a load would leave the stale one as the published value until the next
                // transition. `crate::tcp`'s own accept loop publishes this gauge the same way.
                let live = live_connections.fetch_add(1, Ordering::Relaxed) + 1;
                telemetry.gauge("logit.input.connections", live as f64, &[]);

                // The TLS handshake itself runs here, inside the spawned task and after the
                // permit above -- a slow or hostile handshake stalls only this connection and
                // counts against `MAX_CONCURRENT_CONNECTIONS` like any other slow request, rather
                // than blocking `run`'s own accept loop (this module's doc comment).
                let result = match tls_acceptor {
                    // Bounded exactly the way `logit_in`/`syslog_in` bound their own
                    // (`crates/logit-inputs/src/logit.rs`, `crates/logit-inputs/src/tcp.rs`): an
                    // unbounded accept lets a client that connects and then sends no ClientHello
                    // pin this permit forever. Both the failure and the timeout fall through to
                    // the `warn_throttled("connection_error", ..)` below, and the permit comes
                    // back because this task ends -- no explicit release needed. No first-byte
                    // peek on this arm: `acceptor.accept` is already waiting on this connection's
                    // first bytes under this same budget, so a peek ahead of it would bound
                    // nothing the handshake does not (this module's "peek, not a read" section).
                    Some(acceptor) => {
                        match tokio::time::timeout(handshake_timeout, acceptor.accept(stream)).await
                        {
                            Ok(Ok(tls_stream)) => {
                                serve_connection(
                                    TokioIo::new(tls_stream),
                                    transport,
                                    sink,
                                    telemetry.clone(),
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
                    // The plaintext arm's equivalent budget. `peek` is `recv(..., MSG_PEEK)`: it
                    // waits for the first byte to be *available* and consumes nothing, so
                    // `auto::Builder`'s own `ReadVersion` sniff below still sees a pristine
                    // stream and needs no `Rewind`-shaped buffer -- the reason this is the bound
                    // rather than a wrapper around the sniff, which would mean reimplementing it.
                    // A read error and the deadline become an `Err(String)` on the same
                    // `connection_error` path as the TLS arm's. A *clean* close before the first
                    // byte (`Ok(0)`) does not: that is what every TCP health check looks like --
                    // `demo/haproxy/haproxy.cfg`'s `server logit logit:4318 check` probing
                    // `demo/logit.yaml`'s plaintext `browser_in`, a Kubernetes `tcpSocket` probe,
                    // `nc -z` -- and before this peek existed hyper-util's `ReadVersion` read the
                    // immediate EOF as `Version::H1` and the connection ended silently. Turning it
                    // into a warn would log one line and one `connection_error` count per probe
                    // interval, forever. The shared driver makes the same call (`crate::tcp`'s
                    // `ReadStep::Eof` before any frame is `Ok(())`, and only the deadline is an
                    // error). The permit comes back on either path, when this task ends.
                    None => {
                        // Bound to a local rather than matched on directly: the scrutinee's
                        // temporaries (including `peek`'s borrow of `stream`) would otherwise
                        // outlive the arms, and the success arm moves `stream` into `hyper`.
                        let first_byte =
                            tokio::time::timeout(handshake_timeout, stream.peek(&mut [0u8; 1]))
                                .await;
                        match first_byte {
                            Ok(Ok(0)) => Ok(()), // a health-check probe, not a fault
                            Ok(Ok(_)) => {
                                serve_connection(
                                    TokioIo::new(stream),
                                    transport,
                                    sink,
                                    telemetry.clone(),
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

                // One connection's I/O error (a client disconnecting mid-request, a malformed
                // TLS-looking preamble on a plaintext port, ...) shouldn't be fatal to the
                // listener or its sibling connections -- only `TcpListener::accept` failing in
                // `run`'s own loop is.
                if let Err(err) = result {
                    diag.warn_throttled("connection_error", err);
                }
            });
        }
    }
}

/// Serves one already-accepted (and, if this listener has TLS on, already-handshaken) connection
/// to completion -- generic over the IO type so the plaintext (`TokioIo<TcpStream>`) and TLS
/// (`TokioIo<tokio_rustls::server::TlsStream<TcpStream>>`) cases share every line of dispatch
/// below `run`'s own `tls_acceptor` branch.
///
/// `idle_timeout` and `grace` are this connection's idle bound and the budget
/// [`drive_with_idle`] gives hyper to shut down in once that bound fires (`handshake_timeout`,
/// reused -- this module's "Idle timeout" doc section). With `idle_timeout: None` the connection
/// is simply awaited, exactly as it was before the field existed.
async fn serve_connection<IO>(
    io: IO,
    transport: OtlpTransport,
    sink: Fanout,
    telemetry: Telemetry,
    idle_timeout: Option<std::time::Duration>,
    grace: std::time::Duration,
) -> Result<(), String>
where
    IO: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
{
    // One tracker per connection, shared between the service (which stamps it as requests start
    // and finish) and the driver below (which reads it). The body-frame stall bound is
    // `idle_timeout` as well: a connection with no idle bound configured gets no per-frame one
    // either, which keeps "no `idle_timeout` means exactly today's behaviour" literally true.
    let activity = Arc::new(Activity::new());
    match transport {
        OtlpTransport::Http => {
            let svc = service_fn({
                let activity = Arc::clone(&activity);
                let (sink, telemetry) = (sink.clone(), telemetry.clone());
                move |req| {
                    // `enter` here rather than inside the returned future: hyper calls the
                    // service the moment a request head is parsed, so the in-flight count rises
                    // then, not whenever the future first happens to be polled.
                    let in_flight = activity.enter();
                    let (sink, telemetry) = (sink.clone(), telemetry.clone());
                    let activity = Arc::clone(&activity);
                    async move {
                        let _in_flight = in_flight;
                        handle_http(req, sink, telemetry, &activity, idle_timeout).await
                    }
                }
            });
            // Bound to a local: `auto::Connection` borrows its builder (`Connection<'a, ..>`), so
            // a temporary would not live long enough to be held across `drive_with_idle`'s loop.
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
        OtlpTransport::Grpc => {
            let svc = service_fn({
                let activity = Arc::clone(&activity);
                let (sink, telemetry) = (sink.clone(), telemetry.clone());
                move |req| {
                    let in_flight = activity.enter();
                    let (sink, telemetry) = (sink.clone(), telemetry.clone());
                    let activity = Arc::clone(&activity);
                    async move {
                        let _in_flight = in_flight;
                        handle_grpc(req, sink, telemetry, &activity, idle_timeout).await
                    }
                }
            });
            let conn = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .serve_connection(io, svc);
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
    }
}

async fn handle_http(
    req: http::Request<Incoming>,
    sink: Fanout,
    telemetry: Telemetry,
    activity: &Activity,
    stall: Option<std::time::Duration>,
) -> Result<http::Response<Full<Bytes>>, std::convert::Infallible> {
    if req.method() != Method::POST {
        return Ok(text_response(StatusCode::NOT_FOUND, "not found"));
    }
    let Some(signal) = route_path(req.uri().path()) else {
        return Ok(text_response(StatusCode::NOT_FOUND, "not found"));
    };
    let encoding = match request_encoding(req.headers()) {
        Ok(encoding) => encoding,
        Err(message) => return Ok(text_response(StatusCode::UNSUPPORTED_MEDIA_TYPE, &message)),
    };
    // `identity` and `gzip` are the only encodings this input speaks -- rejecting on the header's
    // mere *presence* would 415 a client that explicitly (if redundantly) declares no compression,
    // not just one sending an encoding this input can't decode. Mirrors the gRPC handler's
    // `grpc-encoding` check just below.
    let gzip_encoded = match req.headers().get("content-encoding") {
        None => false,
        Some(enc) if enc.as_bytes() == b"identity" => false,
        Some(enc) if enc.as_bytes() == b"gzip" => true,
        Some(_) => {
            return Ok(text_response(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported Content-Encoding -- this input speaks 'identity' and 'gzip' only",
            ));
        }
    };

    let limited = Limited::new(req.into_body(), MAX_REQUEST_BYTES);
    let bytes = match collect_with_stall_bound(limited, stall).await {
        Ok(bytes) => bytes,
        // A body that stopped arriving is the client's clock, not its size: `408`, and the
        // connection closes once this response is out rather than waiting for the whole-
        // connection idle deadline (this module's "Idle timeout" doc section).
        Err(BodyReadError::Stalled(stall)) => {
            activity.request_close();
            return Ok(text_response(
                StatusCode::REQUEST_TIMEOUT,
                &format!("request body stalled for {stall:?}"),
            ));
        }
        Err(BodyReadError::Failed(err)) => {
            return Ok(text_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                &body_read_error_message(err.as_ref()),
            ))
        }
    };
    let bytes = if gzip_encoded {
        match inflate(&bytes) {
            Ok(inflated) => inflated,
            Err(InflateError::TooLarge) => {
                return Ok(text_response(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "decompressed request exceeds the maximum allowed size",
                ));
            }
            Err(InflateError::Malformed) => {
                return Ok(text_response(StatusCode::BAD_REQUEST, "invalid gzip body"));
            }
        }
    } else {
        bytes
    };

    let mut decoder = OtlpDecoder::new().with_telemetry(telemetry);
    let result = match encoding {
        RequestEncoding::Protobuf => decoder.decode_signal(signal, bytes),
        RequestEncoding::Json => decoder.decode_signal_json(signal, bytes),
    };
    match result {
        Ok(batches) => {
            for batch in batches {
                sink.send(batch).await;
            }
            // Mirrors the request's own encoding -- the spec: "The server MUST use the same
            // Content-Type in the response as it received in the request." A protobuf request
            // gets `export_response`'s empty-on-success body; a JSON request gets `{}`, not a
            // zero-length body -- `opentelemetry-js`'s exporter parses the success body looking
            // for `partialSuccess`, and `JSON.parse("")` throws.
            let (content_type, body) = match encoding {
                RequestEncoding::Protobuf => ("application/x-protobuf", export_response(0, "")),
                RequestEncoding::Json => ("application/json", export_response_json()),
            };
            Ok(http::Response::builder()
                .status(StatusCode::OK)
                .header("content-type", content_type)
                .body(Full::new(Bytes::from(body)))
                .expect("a well-formed response always builds"))
        }
        Err(err) => Ok(text_response(StatusCode::BAD_REQUEST, &err.to_string())),
    }
}

/// Which OTLP/HTTP wire encoding a request's `Content-Type` declares -- protobuf (this input's
/// original, and still default, encoding) or JSON (`docs/adr/otlp-json-decoding.md`). `Err`
/// carries the 415 message for anything else.
enum RequestEncoding {
    Protobuf,
    Json,
}

/// Absent or empty `Content-Type` means protobuf -- **not new leniency, a preserved compatibility
/// promise**: every client this input accepted before OTLP/JSON existed sent no `Content-Type` at
/// all, or an empty one, and none of them meant JSON. Matched via `eq_ignore_ascii_case`: HTTP
/// media types are case-insensitive (`Content-Type: Application/JSON` is conformant), and the
/// exact-string match this replaces was a latent bug that would have 415'd it.
fn request_encoding(headers: &HeaderMap) -> Result<RequestEncoding, String> {
    let Some(ct) = headers.get("content-type") else {
        return Ok(RequestEncoding::Protobuf);
    };
    let ct = ct.to_str().unwrap_or("");
    let ct = ct.split(';').next().unwrap_or("").trim();
    if ct.is_empty()
        || ct.eq_ignore_ascii_case("application/x-protobuf")
        || ct.eq_ignore_ascii_case("application/protobuf")
    {
        return Ok(RequestEncoding::Protobuf);
    }
    if ct.eq_ignore_ascii_case("application/json") {
        return Ok(RequestEncoding::Json);
    }
    Err(format!(
        "unsupported Content-Type {ct:?} -- this input accepts application/x-protobuf, \
         application/protobuf, and application/json"
    ))
}

async fn handle_grpc(
    req: http::Request<Incoming>,
    sink: Fanout,
    telemetry: Telemetry,
    activity: &Activity,
    stall: Option<std::time::Duration>,
) -> Result<http::Response<GrpcBody>, std::convert::Infallible> {
    if req.method() != Method::POST {
        return Ok(grpc_response(12, "only POST is supported", None));
    }
    let path = req.uri().path();
    let Some(signal) = [Signal::Logs, Signal::Metrics, Signal::Traces]
        .into_iter()
        .find(|s| s.grpc_method() == path)
    else {
        return Ok(grpc_response(12, &format!("unknown method {path}"), None));
    };
    // `grpc-encoding` names the algorithm the client used; the frame's own compressed flag (read
    // below, via `grpc_unframe`) is what actually drives decompression -- this check exists to
    // reject an encoding this input can't decode with a clear message, up front, rather than
    // failing obscurely against the frame later.
    if let Some(enc) = req.headers().get("grpc-encoding") {
        if enc.as_bytes() != b"identity" && enc.as_bytes() != b"gzip" {
            return Ok(grpc_response(
                12,
                "unsupported grpc-encoding -- this input speaks 'identity' and 'gzip' only",
                None,
            ));
        }
    }

    let limited = Limited::new(req.into_body(), MAX_REQUEST_BYTES);
    let framed = match collect_with_stall_bound(limited, stall).await {
        Ok(bytes) => bytes,
        // `4`, `DEADLINE_EXCEEDED` -- the HTTP `408`'s gRPC twin, and the connection closes once
        // this response is out (this module's "Idle timeout" doc section).
        Err(BodyReadError::Stalled(stall)) => {
            activity.request_close();
            return Ok(grpc_response(4, &format!("request body stalled for {stall:?}"), None));
        }
        Err(BodyReadError::Failed(err)) => {
            return Ok(grpc_response(8, &body_read_error_message(err.as_ref()), None))
        }
    };
    let Some((compressed, payload)) = grpc_unframe(&framed) else {
        return Ok(grpc_response(3, "malformed gRPC message frame", None));
    };
    let payload = if compressed {
        match inflate(payload) {
            Ok(inflated) => inflated,
            Err(InflateError::TooLarge) => {
                return Ok(grpc_response(
                    8,
                    "decompressed request exceeds the maximum allowed size",
                    None,
                ));
            }
            Err(InflateError::Malformed) => {
                return Ok(grpc_response(3, "invalid gzip payload", None));
            }
        }
    } else {
        Bytes::copy_from_slice(payload)
    };

    let mut decoder = OtlpDecoder::new().with_telemetry(telemetry);
    match decoder.decode_signal(signal, payload) {
        Ok(batches) => {
            for batch in batches {
                sink.send(batch).await;
            }
            Ok(grpc_response(0, "", Some(export_response(0, ""))))
        }
        Err(err) => Ok(grpc_response(3, &err.to_string(), None)),
    }
}

/// Matches an OTLP/HTTP path (`/v1/logs`, `/v1/metrics`, `/v1/traces`) to its [`Signal`].
fn route_path(path: &str) -> Option<Signal> {
    [Signal::Logs, Signal::Metrics, Signal::Traces].into_iter().find(|s| s.path() == path)
}

fn text_response(status: StatusCode, message: &str) -> http::Response<Full<Bytes>> {
    http::Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Full::new(Bytes::copy_from_slice(message.as_bytes())))
        .expect("a well-formed response always builds")
}

/// Builds a gRPC response: `200` status, the framed `payload` (empty when `None`) as one data
/// frame, and `grpc-status`/`grpc-message` as trailers -- always via a real trailers frame after
/// the body, never a Trailers-Only (headers-only) response, which keeps this server's shape
/// uniform for every outcome including an immediate rejection (an unknown method, say) rather than
/// needing a second response-building path for that case.
fn grpc_response(status: u32, message: &str, payload: Option<Vec<u8>>) -> http::Response<GrpcBody> {
    let mut trailers = HeaderMap::new();
    trailers.insert(
        "grpc-status",
        HeaderValue::from_str(&status.to_string())
            .expect("a decimal number is a valid header value"),
    );
    if !message.is_empty() {
        trailers.insert(
            "grpc-message",
            HeaderValue::from_str(message).unwrap_or_else(|_| HeaderValue::from_static("error")),
        );
    }
    let framed = grpc_frame(&payload.unwrap_or_default());
    let body = GrpcBody { data: Some(Bytes::from(framed)), trailers: Some(trailers) };
    http::Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/grpc+proto")
        .header("grpc-accept-encoding", "identity, gzip")
        .body(body)
        .expect("a well-formed response always builds")
}

/// A response body that yields exactly one data frame, then one trailers frame, then ends -- the
/// shape every unary gRPC response takes on the wire: a single framed message (possibly
/// zero-length, for an error response with no payload), followed by the `grpc-status` trailer.
/// `http_body_util::Full` can't express this -- it has no trailers concept at all -- so this is
/// hand-rolled directly against [`hyper::body::Body`], the same "small enough to write by hand"
/// call this whole transport makes (`docs/adr/hand-rolled-grpc-over-hyper.md`).
struct GrpcBody {
    data: Option<Bytes>,
    trailers: Option<HeaderMap>,
}

impl hyper::body::Body for GrpcBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if let Some(data) = self.data.take() {
            return Poll::Ready(Some(Ok(Frame::data(data))));
        }
        if let Some(trailers) = self.trailers.take() {
            return Poll::Ready(Some(Ok(Frame::trailers(trailers))));
        }
        Poll::Ready(None)
    }
}

/// Frames `payload` as one unary gRPC message. See
/// `logit_outputs::otlp::grpc_frame`'s identical doc comment -- duplicated, not shared: these are
/// two independent crates, and this one function is a handful of lines.
fn grpc_frame(payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(5 + payload.len());
    buf.push(0u8);
    buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(payload);
    buf
}

/// The mirror of [`grpc_frame`] -- but unlike `logit_outputs::otlp::grpc_unframe`, this side
/// *does* accept a compressed frame (`compressed:u8 == 1`): an `otlp_in` request may legally be
/// gzipped, where `otlp_out` never accepts a compressed *response* (see that function's own doc
/// comment for why). Returns the frame's own compressed flag alongside its payload slice, so the
/// caller can decide whether `inflate` needs to run -- `None` for anything short of one complete
/// frame, or a declared length longer than what's actually present.
fn grpc_unframe(bytes: &[u8]) -> Option<(bool, &[u8])> {
    if bytes.len() < 5 {
        return None;
    }
    let compressed = match bytes[0] {
        0 => false,
        1 => true,
        _ => return None,
    };
    let len = u32::from_be_bytes(bytes[1..5].try_into().expect("checked len >= 5 above")) as usize;
    bytes.get(5..5 + len).map(|payload| (compressed, payload))
}

/// Why [`inflate`] failed -- distinguished so the caller can respond `400`/`INVALID_ARGUMENT`
/// (this was never valid gzip) rather than `413`/`RESOURCE_EXHAUSTED` (this decompressed to more
/// than we were willing to hold) for what are two very different client mistakes.
enum InflateError {
    Malformed,
    TooLarge,
}

/// Inflates `compressed` (gzip), bounded to [`MAX_REQUEST_BYTES`] -- the same cap [`Limited`]
/// already enforces on the *compressed* body above, applied again to the *decompressed* output so
/// a small, highly-compressible payload ("a few KiB of gzipped zeros") can't inflate to gigabytes
/// in memory (a compression-bomb-shaped request). `Read::take` stops reading one byte past the
/// cap rather than after it, so an input that would inflate to exactly `MAX_REQUEST_BYTES + 1`
/// bytes is caught, not silently truncated to fit.
fn inflate(compressed: &[u8]) -> Result<Bytes, InflateError> {
    use std::io::Read;
    let mut decoder = flate2::read::GzDecoder::new(compressed).take(MAX_REQUEST_BYTES as u64 + 1);
    let mut out = Vec::new();
    decoder.read_to_end(&mut out).map_err(|_| InflateError::Malformed)?;
    if out.len() > MAX_REQUEST_BYTES {
        return Err(InflateError::TooLarge);
    }
    Ok(Bytes::from(out))
}

fn write_varint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            buf.push(byte);
            break;
        }
        buf.push(byte | 0x80);
    }
}

/// Builds an `Export*ServiceResponse`'s bytes by hand -- see
/// `logit_outputs::otlp::parse_partial_success`'s doc comment for why the wire shape is identical
/// across all three signals and why neither side generates the collector-service types for it.
/// Empty when `rejected == 0` and `error_message` is empty: proto3's own "an unset field reads
/// back as its default" rule already makes an all-default message serialize to zero bytes, so a
/// fully successful response is simply an empty body -- exactly what every response this input
/// sends today looks like (see this module's doc comment on why `rejected` is always `0` for now).
fn export_response(rejected: i64, error_message: &str) -> Vec<u8> {
    if rejected == 0 && error_message.is_empty() {
        return Vec::new();
    }
    let mut sub = Vec::new();
    if rejected != 0 {
        sub.push(0x08); // field 1, varint
        write_varint(&mut sub, rejected as u64);
    }
    if !error_message.is_empty() {
        sub.push(0x12); // field 2, length-delimited
        write_varint(&mut sub, error_message.len() as u64);
        sub.extend_from_slice(error_message.as_bytes());
    }
    let mut out = Vec::new();
    out.push(0x0a); // field 1 (partial_success), length-delimited
    write_varint(&mut out, sub.len() as u64);
    out.extend_from_slice(&sub);
    out
}

/// The OTLP/JSON mirror of [`export_response`], for exactly the same reason and the same current
/// limitation: `rejected` is always `0` here too (this module's doc comment), so there's only ever
/// the all-default `ExportTraceServiceResponse` to render. Unlike the protobuf case, that does
/// **not** mean an empty body -- proto3 JSON's own rule is that an unset message field is simply
/// omitted from the object, and `partial_success` (a message-typed field) unset renders as no key
/// at all, giving `{}`, not `""`. Sending `""` with `content-type: application/json` would be
/// spec-conformant nowhere: `opentelemetry-js`'s HTTP exporter parses the success body looking for
/// `partialSuccess`, and `JSON.parse("")` throws before it gets the chance to find none. When
/// `partial_success` gains a real per-signal reject count (`docs/known-gaps.md`), this grows the
/// same `rejected`/`error_message` parameters `export_response` already has, rendering
/// `rejectedSpans`/`rejectedLogRecords`/`rejectedDataPoints` (the JSON key differs per [`Signal`],
/// unlike the protobuf field, which shares one tag number across all three
/// `Export*ServiceResponse` messages).
fn export_response_json() -> Vec<u8> {
    b"{}".to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::mpsc;

    async fn bound_input(transport: OtlpTransport) -> (String, OtlpInput) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        (addr.to_string(), OtlpInput::new(addr.to_string(), transport))
    }

    /// `Input::bind` (docs/plans/operator-surface.md, workstream B) makes the port live *before*
    /// `run`'s accept loop starts, and `local_addr` makes the OS-assigned port observable --
    /// retiring the bind-drop-rebind idiom `bound_input` above still uses for every other test in
    /// this module (kept there since it predates `bind`, but no longer the only way).
    #[tokio::test]
    async fn bind_makes_the_port_live_before_run_and_local_addr_reports_it() {
        let mut input = OtlpInput::new("127.0.0.1:0", OtlpTransport::Http);
        assert_eq!(input.local_addr(), None, "no address before bind()");

        input.bind().await.expect("binding an ephemeral port should succeed");
        let addr = input.local_addr().expect("bind() should leave a real address behind");

        // Connects successfully with `run` never having been spawned -- the listening socket is
        // already live purely from `bind()`.
        tokio::net::TcpStream::connect(addr)
            .await
            .expect("the port should already be accepting connections after bind() alone");
    }

    /// A second `bind()` call is a no-op, per [`logit_pipeline::Input::bind`]'s idempotency
    /// contract.
    #[tokio::test]
    async fn a_second_bind_is_a_no_op() {
        let mut input = OtlpInput::new("127.0.0.1:0", OtlpTransport::Http);
        input.bind().await.expect("first bind should succeed");
        let addr = input.local_addr().expect("bind() should leave a real address behind");
        input.bind().await.expect("second bind should be a harmless no-op");
        assert_eq!(input.local_addr(), Some(addr), "the address must not change");
    }

    fn fanout_into_channel() -> (Fanout, mpsc::Receiver<logit_pipeline::Delivered>) {
        fanout_into_channel_with_capacity(16)
    }

    /// [`fanout_into_channel`] with the channel capacity spelled out. Capacity 1 with nothing
    /// draining it is how a test parks a handler inside `Fanout::send`: the first batch is
    /// buffered, the second blocks until something receives.
    fn fanout_into_channel_with_capacity(
        capacity: usize,
    ) -> (Fanout, mpsc::Receiver<logit_pipeline::Delivered>) {
        let (tx, rx) = mpsc::channel(capacity);
        (Fanout::new(vec![tx]), rx)
    }

    async fn recv_batch(
        rx: &mut mpsc::Receiver<logit_pipeline::Delivered>,
    ) -> logit_core::EventBatch {
        let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("should receive within 5s")
            .expect("channel should still be open");
        logit_pipeline::unwrap_batch(delivered)
    }

    // ---- HTTP: raw-socket protocol tests ----

    async fn post_raw(addr: &str, path: &str, headers: &str, body: &[u8]) -> String {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\n{headers}\r\n",
            body.len()
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
        let mut buf = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut buf)).await;
        String::from_utf8_lossy(&buf).into_owned()
    }

    #[tokio::test]
    async fn a_protobuf_post_to_v1_traces_reaches_the_fanout_as_an_event_batch() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, mut rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut encoder = logit_proto::otlp::OtlpEncoder::new();
        let batch = logit_core::EventBatch {
            resource: std::sync::Arc::new(logit_core::Resource::default()),
            scope: None,
            events: vec![logit_core::Event::span(
                1,
                logit_core::AttrMap::new(),
                logit_core::SpanRecord {
                    trace_id: [9; 16],
                    span_id: [8; 8],
                    parent_span_id: None,
                    name: logit_core::Value::str("s"),
                    kind: logit_core::SpanKind::Internal,
                    status: logit_core::SpanStatus::Ok,
                    events: Vec::new(),
                    links: Vec::new(),
                    end_timestamp: 2,
                    flags: 0,
                    ext: None,
                },
            )],
        };
        let payloads = logit_proto::SignalEncoder::encode_signals(&mut encoder, &batch).unwrap();
        let (_, body) = payloads.into_iter().find(|(s, _)| *s == Signal::Traces).unwrap();

        let response = post_raw(
            &addr,
            "/v1/traces",
            "Content-Type: application/x-protobuf\r\nConnection: close\r\n",
            &body,
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");

        let received = recv_batch(&mut rx).await;
        assert_eq!(received.events.len(), 1);
        assert!(received.events[0].span.is_some());
    }

    /// One span, as an OTLP/JSON literal describing the same span [`one_span_payload`] encodes --
    /// used everywhere the JSON decode path needs a real, spec-shaped body.
    fn one_span_json() -> Vec<u8> {
        br#"{"resourceSpans": [{"scopeSpans": [{"spans": [{
            "traceId": "09090909090909090909090909090909",
            "spanId": "0808080808080808",
            "name": "s",
            "startTimeUnixNano": "1",
            "endTimeUnixNano": "2"
        }]}]}]}"#
            .to_vec()
    }

    #[tokio::test]
    async fn a_json_post_to_v1_traces_reaches_the_fanout_as_an_event_batch() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, mut rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let response = post_raw(
            &addr,
            "/v1/traces",
            "Content-Type: application/json\r\nConnection: close\r\n",
            &one_span_json(),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");

        let received = recv_batch(&mut rx).await;
        assert_eq!(received.events.len(), 1);
        let span = received.events[0].span.as_ref().expect("event should carry a span");
        assert_eq!(span.trace_id, [9; 16]);
        assert_eq!(span.span_id, [8; 8]);
    }

    /// The interop guard: `opentelemetry-js`'s HTTP exporter parses the success response body
    /// looking for `partialSuccess`, so a JSON request must never get protobuf's empty-body
    /// shortcut back -- `{}`, with a JSON content type, or a conformant client's own response
    /// parsing breaks before it ever sees "this succeeded."
    #[tokio::test]
    async fn a_json_request_gets_a_json_response_body_and_content_type() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let response = post_raw(
            &addr,
            "/v1/traces",
            "Content-Type: application/json\r\nConnection: close\r\n",
            &one_span_json(),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
        assert!(response.contains("content-type: application/json"), "got: {response}");
        assert!(response.trim_end().ends_with("{}"), "expected a `{{}}` body, got: {response}");
    }

    /// Regression guard on the arm that didn't change: a protobuf request must keep getting
    /// protobuf's response shape, not JSON's, now that both exist side by side.
    #[tokio::test]
    async fn a_protobuf_request_still_gets_a_protobuf_content_type() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let response = post_raw(
            &addr,
            "/v1/traces",
            "Content-Type: application/x-protobuf\r\nConnection: close\r\n",
            &one_span_payload(),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
        assert!(response.contains("content-type: application/x-protobuf"), "got: {response}");
    }

    /// The surviving half of the old JSON-always-415 test: an actually-unsupported type is still
    /// rejected, and the message now names every type this input *does* accept.
    #[tokio::test]
    async fn an_unknown_content_type_is_rejected_with_415_listing_what_is_accepted() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let response = post_raw(
            &addr,
            "/v1/traces",
            "Content-Type: text/xml\r\nConnection: close\r\n",
            b"<x/>",
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 415"), "got: {response}");
        assert!(response.contains("application/x-protobuf"), "got: {response}");
        assert!(response.contains("application/protobuf"), "got: {response}");
        assert!(response.contains("application/json"), "got: {response}");
    }

    /// HTTP media types are case-insensitive (`Content-Type: Application/JSON` is conformant) --
    /// the exact-string match this replaced would have 415'd this.
    #[tokio::test]
    async fn a_content_type_is_matched_case_insensitively() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, mut rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let response = post_raw(
            &addr,
            "/v1/traces",
            "Content-Type: Application/JSON\r\nConnection: close\r\n",
            &one_span_json(),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
        recv_batch(&mut rx).await;
    }

    #[tokio::test]
    async fn malformed_json_over_http_returns_400() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let response = post_raw(
            &addr,
            "/v1/traces",
            "Content-Type: application/json\r\nConnection: close\r\n",
            b"not json at all",
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 400"), "got: {response}");
    }

    /// The two headers compose: `Content-Encoding` and `Content-Type` are handled independently,
    /// so a gzipped JSON body is exactly as valid as a gzipped protobuf one.
    #[tokio::test]
    async fn a_gzipped_json_body_is_decoded() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, mut rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let gzipped = gzip(&one_span_json());
        let response = post_raw(
            &addr,
            "/v1/traces",
            "Content-Type: application/json\r\nContent-Encoding: gzip\r\n\
             Connection: close\r\n",
            &gzipped,
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");

        let received = recv_batch(&mut rx).await;
        assert_eq!(received.events.len(), 1);
        assert!(received.events[0].span.is_some());
    }

    /// The scope decision made testable: all three signals, not traces only.
    #[tokio::test]
    async fn a_json_post_to_v1_logs_and_v1_metrics_also_works() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, mut rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let log_body = br#"{"resourceLogs": [{"scopeLogs": [{"logRecords": [{
            "timeUnixNano": "1", "body": {"stringValue": "hi"}
        }]}]}]}"#;
        let response = post_raw(
            &addr,
            "/v1/logs",
            "Content-Type: application/json\r\nConnection: close\r\n",
            log_body,
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"), "logs, got: {response}");
        let received = recv_batch(&mut rx).await;
        assert!(received.events[0].log.is_some());

        let metric_body = br#"{"resourceMetrics": [{"scopeMetrics": [{"metrics": [{
            "name": "m", "gauge": {"dataPoints": [{"timeUnixNano": "1", "asDouble": 1.0}]}
        }]}]}]}"#;
        let response = post_raw(
            &addr,
            "/v1/metrics",
            "Content-Type: application/json\r\nConnection: close\r\n",
            metric_body,
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"), "metrics, got: {response}");
        let received = recv_batch(&mut rx).await;
        assert!(!received.events[0].metrics.is_empty());
    }

    /// `identity` is the standard, legal way to declare "not compressed" -- it must not be
    /// mistaken for "compressed, unsupported." Regression guard for rejecting on the header's mere
    /// *presence* rather than its value.
    #[tokio::test]
    async fn a_content_encoding_of_identity_is_accepted_not_rejected() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let response = post_raw(
            &addr,
            "/v1/traces",
            "Content-Type: application/x-protobuf\r\nContent-Encoding: identity\r\n\
             Connection: close\r\n",
            &[],
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
    }

    /// `gzip` is decoded (see the tests below); any *other* declared encoding is still rejected.
    #[tokio::test]
    async fn an_unsupported_content_encoding_is_rejected_with_415() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let response = post_raw(
            &addr,
            "/v1/traces",
            "Content-Type: application/x-protobuf\r\nContent-Encoding: br\r\n\
             Connection: close\r\n",
            &[],
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 415"), "got: {response}");
    }

    fn one_span_payload() -> Vec<u8> {
        let mut encoder = logit_proto::otlp::OtlpEncoder::new();
        let batch = logit_core::EventBatch {
            resource: std::sync::Arc::new(logit_core::Resource::default()),
            scope: None,
            events: vec![logit_core::Event::span(
                1,
                logit_core::AttrMap::new(),
                logit_core::SpanRecord {
                    trace_id: [9; 16],
                    span_id: [8; 8],
                    parent_span_id: None,
                    name: logit_core::Value::str("s"),
                    kind: logit_core::SpanKind::Internal,
                    status: logit_core::SpanStatus::Ok,
                    events: Vec::new(),
                    links: Vec::new(),
                    end_timestamp: 2,
                    flags: 0,
                    ext: None,
                },
            )],
        };
        let payloads = logit_proto::SignalEncoder::encode_signals(&mut encoder, &batch).unwrap();
        payloads.into_iter().find(|(s, _)| *s == Signal::Traces).unwrap().1.to_vec()
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut encoder, bytes).unwrap();
        encoder.finish().unwrap()
    }

    #[tokio::test]
    async fn a_gzip_compressed_body_is_decoded() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, mut rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let gzipped = gzip(&one_span_payload());
        let response = post_raw(
            &addr,
            "/v1/traces",
            "Content-Type: application/x-protobuf\r\nContent-Encoding: gzip\r\n\
             Connection: close\r\n",
            &gzipped,
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");

        let received = recv_batch(&mut rx).await;
        assert_eq!(received.events.len(), 1);
        assert!(received.events[0].span.is_some());
    }

    #[tokio::test]
    async fn a_malformed_gzip_body_is_rejected_with_400() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let response = post_raw(
            &addr,
            "/v1/traces",
            "Content-Type: application/x-protobuf\r\nContent-Encoding: gzip\r\n\
             Connection: close\r\n",
            b"not actually gzip",
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 400"), "got: {response}");
    }

    /// A compression-bomb-shaped request: a few KiB of gzipped zeros that would inflate to well
    /// over `MAX_REQUEST_BYTES` -- `inflate`'s own bound on the *decompressed* size must catch
    /// this, since `Limited`'s bound on the compressed body (checked first, and satisfied here)
    /// does not.
    #[tokio::test]
    async fn a_gzip_body_that_would_inflate_past_the_size_cap_is_rejected_with_413() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let zeros = vec![0u8; MAX_REQUEST_BYTES + 1];
        let gzipped = gzip(&zeros);
        assert!(
            gzipped.len() < MAX_REQUEST_BYTES,
            "the compressed body must itself fit under the cap for this test to isolate the \
             decompressed-size check"
        );
        let response = post_raw(
            &addr,
            "/v1/traces",
            "Content-Type: application/x-protobuf\r\nContent-Encoding: gzip\r\n\
             Connection: close\r\n",
            &gzipped,
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 413"), "got: {response}");
    }

    #[tokio::test]
    async fn a_body_over_the_size_cap_is_rejected_with_413() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let body = vec![0u8; MAX_REQUEST_BYTES + 1];
        let response = post_raw(
            &addr,
            "/v1/traces",
            "Content-Type: application/x-protobuf\r\nConnection: close\r\n",
            &body,
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 413"), "got: {response}");
    }

    #[tokio::test]
    async fn garbage_protobuf_over_http_returns_400() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let response = post_raw(
            &addr,
            "/v1/traces",
            "Content-Type: application/x-protobuf\r\nConnection: close\r\n",
            &[0xff, 0xff, 0xff],
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 400"), "got: {response}");
    }

    #[tokio::test]
    async fn a_request_with_two_resource_spans_produces_two_batches_on_the_fanout() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, mut rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        fn span_batch(host: &str, trace_byte: u8) -> logit_core::EventBatch {
            let mut resource = logit_core::Resource::default();
            resource.attributes.insert("host", host);
            logit_core::EventBatch {
                resource: std::sync::Arc::new(resource),
                scope: None,
                events: vec![logit_core::Event::span(
                    1,
                    logit_core::AttrMap::new(),
                    logit_core::SpanRecord {
                        trace_id: [trace_byte; 16],
                        span_id: [trace_byte; 8],
                        parent_span_id: None,
                        name: logit_core::Value::str("s"),
                        kind: logit_core::SpanKind::Internal,
                        status: logit_core::SpanStatus::Ok,
                        events: Vec::new(),
                        links: Vec::new(),
                        end_timestamp: 2,
                        flags: 0,
                        ext: None,
                    },
                )],
            }
        }

        let mut encoder = logit_proto::otlp::OtlpEncoder::new();
        let bytes_a =
            logit_proto::SignalEncoder::encode_signals(&mut encoder, &span_batch("a", 1)).unwrap();
        let bytes_b =
            logit_proto::SignalEncoder::encode_signals(&mut encoder, &span_batch("b", 2)).unwrap();

        // Combine the two single-resource requests into one two-ResourceSpans request by simple
        // byte concatenation, not by decoding and re-encoding through a generated type (`logit-
        // proto`'s `generated` module is `pub(crate)`, unreachable from here). This works because
        // `TracesData`'s only field is `repeated ResourceSpans resource_spans = 1` -- each of
        // `bytes_a`/`bytes_b` is a complete encoding of *one* such occurrence, and concatenating
        // two complete, self-delimited protobuf field occurrences is indistinguishable on the wire
        // from a single message that had both all along (the same "concatenation of encoded
        // messages is a valid merge" property `logit-proto`'s own two-`ResourceSpans` test relies
        // on, just exercised externally here instead of via the generated types directly).
        let mut combined_bytes = Vec::new();
        combined_bytes.extend_from_slice(&bytes_a[0].1);
        combined_bytes.extend_from_slice(&bytes_b[0].1);

        let response = post_raw(
            &addr,
            "/v1/traces",
            "Content-Type: application/x-protobuf\r\nConnection: close\r\n",
            &combined_bytes,
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");

        let first = recv_batch(&mut rx).await;
        let second = recv_batch(&mut rx).await;
        assert_eq!(first.resource.attributes.get("host").and_then(|v| v.as_str()), Some("a"));
        assert_eq!(second.resource.attributes.get("host").and_then(|v| v.as_str()), Some("b"));
    }

    // ---- gRPC: garbage-frame status test ----

    #[tokio::test]
    async fn garbage_protobuf_returns_grpc_status_three() {
        let (addr, mut input) = bound_input(OtlpTransport::Grpc).await;
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // `OtlpOutput` always sends well-formed frames -- to exercise "garbage protobuf" this
        // drives the raw framing directly instead, over a real HTTP/2 client connection.
        let stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let io = TokioIo::new(stream);
        let (mut sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
            .handshake(io)
            .await
            .unwrap();
        tokio::spawn(conn);

        let mut framed = vec![0u8, 0, 0, 0, 3];
        framed.extend_from_slice(&[0xff, 0xff, 0xff]);
        let req = http::Request::builder()
            .method(Method::POST)
            .uri(Signal::Traces.grpc_method())
            .header("content-type", "application/grpc+proto")
            .header("te", "trailers")
            .body(Full::new(Bytes::from(framed)))
            .unwrap();
        let res = sender.send_request(req).await.unwrap();
        let collected = res.into_body().collect().await.unwrap();
        let trailers = collected.trailers().expect("should carry trailers");
        assert_eq!(trailers.get("grpc-status").unwrap().to_str().unwrap(), "3");
    }

    #[tokio::test]
    async fn a_gzip_compressed_grpc_request_is_decoded() {
        let (addr, mut input) = bound_input(OtlpTransport::Grpc).await;
        let (sink, mut rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let io = TokioIo::new(stream);
        let (mut sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
            .handshake(io)
            .await
            .unwrap();
        tokio::spawn(conn);

        let compressed = gzip(&one_span_payload());
        let mut framed = vec![1u8]; // compressed flag set
        framed.extend_from_slice(&(compressed.len() as u32).to_be_bytes());
        framed.extend_from_slice(&compressed);
        let req = http::Request::builder()
            .method(Method::POST)
            .uri(Signal::Traces.grpc_method())
            .header("content-type", "application/grpc+proto")
            .header("te", "trailers")
            .header("grpc-encoding", "gzip")
            .body(Full::new(Bytes::from(framed)))
            .unwrap();
        let res = sender.send_request(req).await.unwrap();
        let collected = res.into_body().collect().await.unwrap();
        let trailers = collected.trailers().expect("should carry trailers");
        assert_eq!(trailers.get("grpc-status").unwrap().to_str().unwrap(), "0");

        let received = recv_batch(&mut rx).await;
        assert_eq!(received.events.len(), 1);
        assert!(received.events[0].span.is_some());
    }

    #[tokio::test]
    async fn a_malformed_gzip_grpc_payload_returns_grpc_status_three() {
        let (addr, mut input) = bound_input(OtlpTransport::Grpc).await;
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let io = TokioIo::new(stream);
        let (mut sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
            .handshake(io)
            .await
            .unwrap();
        tokio::spawn(conn);

        let not_gzip = b"not actually gzip";
        let mut framed = vec![1u8];
        framed.extend_from_slice(&(not_gzip.len() as u32).to_be_bytes());
        framed.extend_from_slice(not_gzip);
        let req = http::Request::builder()
            .method(Method::POST)
            .uri(Signal::Traces.grpc_method())
            .header("content-type", "application/grpc+proto")
            .header("te", "trailers")
            .header("grpc-encoding", "gzip")
            .body(Full::new(Bytes::from(framed)))
            .unwrap();
        let res = sender.send_request(req).await.unwrap();
        let collected = res.into_body().collect().await.unwrap();
        let trailers = collected.trailers().expect("should carry trailers");
        assert_eq!(trailers.get("grpc-status").unwrap().to_str().unwrap(), "3");
    }

    // ---- TLS: server termination against a real `tokio-rustls` client. ----

    fn testdata_dir() -> std::path::PathBuf {
        // `logit-inputs` lives at `crates/logit-inputs`; the fixtures live at the repo root's
        // `testdata/tls` (`testdata/tls/README.md`) -- two levels up from `CARGO_MANIFEST_DIR`.
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    fn test_tls_settings(client_ca_file: Option<&str>) -> TlsServerSettings {
        TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: client_ca_file.map(str::to_string),
        }
    }

    /// A `tokio-rustls` client trusting `testdata/tls/ca.pem` -- `client_cert` is `(cert, key)`
    /// file names under `testdata/tls`, for the mTLS tests; `None` for a client presenting no
    /// certificate at all.
    async fn tls_connector(client_cert: Option<(&str, &str)>) -> tokio_rustls::TlsConnector {
        let dir = testdata_dir();
        let mut roots = rustls::RootCertStore::empty();
        let ca: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(dir.join("ca.pem"))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        roots.add_parsable_certificates(ca);
        let builder = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots);
        let cfg = match client_cert {
            Some((cert_file, key_file)) => {
                let chain: Vec<CertificateDer<'static>> =
                    CertificateDer::pem_file_iter(dir.join(cert_file))
                        .unwrap()
                        .collect::<Result<_, _>>()
                        .unwrap();
                let key = PrivateKeyDer::from_pem_file(dir.join(key_file)).unwrap();
                builder.with_client_auth_cert(chain, key).unwrap()
            }
            None => builder.with_no_client_auth(),
        };
        tokio_rustls::TlsConnector::from(std::sync::Arc::new(cfg))
    }

    /// The TLS twin of `post_raw`: connects, performs a real TLS handshake against `addr`, then
    /// sends a plaintext HTTP/1.1 request over the encrypted stream.
    async fn post_raw_tls(
        connector: &tokio_rustls::TlsConnector,
        addr: &str,
        path: &str,
        headers: &str,
        body: &[u8],
    ) -> String {
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let server_name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
        let mut tls_stream = connector.connect(server_name, stream).await.unwrap();
        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\n{headers}\r\n",
            body.len()
        );
        tls_stream.write_all(request.as_bytes()).await.unwrap();
        tls_stream.write_all(body).await.unwrap();
        let mut buf = Vec::new();
        let _ =
            tokio::time::timeout(Duration::from_secs(2), tls_stream.read_to_end(&mut buf)).await;
        String::from_utf8_lossy(&buf).into_owned()
    }

    #[tokio::test]
    async fn an_http_request_over_tls_reaches_the_fanout() {
        let (addr, input) = bound_input(OtlpTransport::Http).await;
        let mut input = input.with_tls(&test_tls_settings(None), &testdata_dir()).unwrap();
        let (sink, mut rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut encoder = logit_proto::otlp::OtlpEncoder::new();
        let payloads =
            logit_proto::SignalEncoder::encode_signals(&mut encoder, &metric_batch()).unwrap();
        let (_, body) = payloads.into_iter().find(|(s, _)| *s == Signal::Metrics).unwrap();

        let connector = tls_connector(None).await;
        let response = post_raw_tls(
            &connector,
            &addr,
            "/v1/metrics",
            "Content-Type: application/x-protobuf\r\nConnection: close\r\n",
            &body,
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
        recv_batch(&mut rx).await;
    }

    #[tokio::test]
    async fn a_grpc_request_over_tls_reaches_the_fanout() {
        let (addr, input) = bound_input(OtlpTransport::Grpc).await;
        let mut input = input.with_tls(&test_tls_settings(None), &testdata_dir()).unwrap();
        let (sink, mut rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let connector = tls_connector(None).await;
        let stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let server_name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
        let tls_stream = connector.connect(server_name, stream).await.unwrap();
        let io = TokioIo::new(tls_stream);
        let (mut sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
            .handshake(io)
            .await
            .unwrap();
        tokio::spawn(conn);

        let mut framed = vec![0u8];
        let payload = one_span_payload();
        framed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        framed.extend_from_slice(&payload);
        let req = http::Request::builder()
            .method(Method::POST)
            .uri(Signal::Traces.grpc_method())
            .header("content-type", "application/grpc+proto")
            .header("te", "trailers")
            .body(Full::new(Bytes::from(framed)))
            .unwrap();
        let res = sender.send_request(req).await.unwrap();
        let collected = res.into_body().collect().await.unwrap();
        let trailers = collected.trailers().expect("should carry trailers");
        assert_eq!(trailers.get("grpc-status").unwrap().to_str().unwrap(), "0");

        let received = recv_batch(&mut rx).await;
        assert!(received.events[0].span.is_some());
    }

    #[tokio::test]
    async fn a_plaintext_client_against_a_tls_listener_is_refused_not_a_panic() {
        let (addr, input) = bound_input(OtlpTransport::Http).await;
        let mut input = input.with_tls(&test_tls_settings(None), &testdata_dir()).unwrap();
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // A plain (non-TLS) client speaking straight HTTP at a TLS-only listener: the server
        // reads what looks like garbage TLS record framing, sends a TLS alert, and closes --
        // this must not panic or otherwise take the listener down for the next connection. The
        // client never gets a valid HTTP response back (it may see raw alert bytes, or nothing).
        let response = post_raw(
            &addr,
            "/v1/traces",
            "Content-Type: application/x-protobuf\r\nConnection: close\r\n",
            &[],
        )
        .await;
        assert!(!response.starts_with("HTTP/1.1"), "expected no valid HTTP response: {response:?}");

        // The listener itself must still be alive for the next (well-formed, TLS) connection.
        let connector = tls_connector(None).await;
        let response = post_raw_tls(
            &connector,
            &addr,
            "/v1/traces",
            "Content-Type: application/x-protobuf\r\nConnection: close\r\n",
            &[],
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
    }

    #[tokio::test]
    async fn mutual_tls_accepts_a_valid_client_certificate_and_rejects_none() {
        let (addr, input) = bound_input(OtlpTransport::Http).await;
        let mut input =
            input.with_tls(&test_tls_settings(Some("ca.pem")), &testdata_dir()).unwrap();
        let (sink, mut rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let body = one_span_payload();
        let with_cert = tls_connector(Some(("client.pem", "client.key"))).await;
        let response = post_raw_tls(
            &with_cert,
            &addr,
            "/v1/traces",
            "Content-Type: application/x-protobuf\r\nConnection: close\r\n",
            &body,
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
        recv_batch(&mut rx).await;

        let without_cert = tls_connector(None).await;
        let response = post_raw_tls(
            &without_cert,
            &addr,
            "/v1/traces",
            "Content-Type: application/x-protobuf\r\nConnection: close\r\n",
            &body,
        )
        .await;
        assert!(
            !response.starts_with("HTTP/1.1"),
            "a client with no certificate should be rejected: got {response:?}"
        );
    }

    /// The handshake timeout's whole purpose (this module's "Handshake timeout" doc section):
    /// a client that completes the TCP connect and never sends a ClientHello must not pin a
    /// `MAX_CONCURRENT_CONNECTIONS` permit forever. What is asserted here is the observable half
    /// -- the silent connection is *closed* from the server side inside the budget, and the
    /// listener is still serving TLS traffic afterwards. Its plaintext twin,
    /// `a_silent_plaintext_connection_is_closed_after_the_handshake_timeout`, goes on to prove
    /// the permit itself came back, under `with_max_connections(1)`. Modelled on `crate::tcp`'s
    /// `a_silent_connection_releases_its_permit_after_the_handshake_timeout`.
    #[tokio::test]
    async fn a_silent_tls_connection_is_closed_after_the_handshake_timeout() {
        let (addr, input) = bound_input(OtlpTransport::Http).await;
        let mut input = input
            .with_tls(&test_tls_settings(None), &testdata_dir())
            .unwrap()
            .with_handshake_timeout(Duration::from_millis(50));
        let (sink, mut rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Raw TCP, not a byte sent, and held (not dropped) until the close is observed -- so
        // nothing but the server's own deadline could have closed it. A 1s read budget against a
        // 50ms handshake timeout.
        let mut silent = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let mut buf = [0u8; 1];
        let result = tokio::time::timeout(Duration::from_secs(1), silent.read(&mut buf))
            .await
            .expect("a silent TLS connection should be closed within 1s");
        match result {
            Ok(n) => assert_eq!(n, 0, "expected a close, got a byte"),
            Err(err) if err.kind() == std::io::ErrorKind::ConnectionReset => {}
            Err(err) => panic!("read failed outright: {err}"),
        }

        // And the listener is still serving real TLS traffic afterwards.
        let mut encoder = logit_proto::otlp::OtlpEncoder::new();
        let payloads =
            logit_proto::SignalEncoder::encode_signals(&mut encoder, &metric_batch()).unwrap();
        let (_, body) = payloads.into_iter().find(|(s, _)| *s == Signal::Metrics).unwrap();
        let connector = tls_connector(None).await;
        let response = post_raw_tls(
            &connector,
            &addr,
            "/v1/metrics",
            "Content-Type: application/x-protobuf\r\nConnection: close\r\n",
            &body,
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
        recv_batch(&mut rx).await;

        drop(silent);
    }

    // ---- connection limit, the plaintext first-byte bound, and the connections gauge ----------

    /// Reads one byte, expecting the peer to have closed instead. The twin of `crate::tcp`'s own
    /// test helper of the same name.
    async fn expect_closed<S: tokio::io::AsyncRead + Unpin>(stream: &mut S, what: &str) {
        let mut buf = [0u8; 1];
        let result = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
            .await
            .unwrap_or_else(|_| panic!("{what}: expected a close within 2s"));
        match result {
            Ok(n) => assert_eq!(n, 0, "{what}: expected a close, got a byte"),
            // A close with bytes still unread in the peer's receive queue is an RST, not a FIN.
            Err(err) if err.kind() == std::io::ErrorKind::ConnectionReset => {}
            Err(err) => panic!("{what}: read failed outright: {err}"),
        }
    }

    /// The value of `metric`'s `Sum` in a drained `Registry` snapshot, optionally restricted to
    /// the point carrying `tag` -- copied from `crate::tcp`'s test module.
    fn sum_of(
        events: &[logit_core::Event],
        metric: &str,
        tag: Option<(&str, &str)>,
    ) -> Option<f64> {
        events.iter().find_map(|e| {
            if let Some((key, value)) = tag {
                if e.attributes.get(key).and_then(|v| v.as_str()) != Some(value) {
                    return None;
                }
            }
            e.metrics.iter().find_map(|m| {
                if logit_core::interner::resolve(m.name) != metric {
                    return None;
                }
                match m.kind {
                    logit_core::MetricKind::Sum(sum) => Some(sum.value),
                    _ => None,
                }
            })
        })
    }

    /// `sum_of`'s gauge twin -- `Telemetry::gauge` is last-write-wins per `(name, tags)` until the
    /// next drain, so one drain reports whatever value the listener last wrote.
    fn gauge_of(events: &[logit_core::Event], metric: &str) -> Option<f64> {
        events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| {
                if logit_core::interner::resolve(m.name) != metric {
                    return None;
                }
                match m.kind {
                    logit_core::MetricKind::Gauge(v) => Some(v),
                    _ => None,
                }
            })
        })
    }

    /// The cap rejects rather than queues, and closes before any TLS handshake -- OTLP has no
    /// in-band "try later" to spend a handshake delivering (this module's "Connection limit" doc
    /// section). Modelled on `crate::tcp`'s
    /// `the_connection_cap_drops_a_connection_past_the_limit_and_counts_it`.
    #[tokio::test]
    async fn a_connection_past_the_cap_is_dropped_and_counted() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("otlp_in", "otlp_in", "listener");
        let (addr, input) = bound_input(OtlpTransport::Http).await;
        let mut input = input.with_telemetry(telemetry).with_max_connections(1);
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The first connection takes the one permit and holds it. One byte, so it clears the
        // first-byte peek and settles inside `hyper` rather than being closed by the deadline.
        let mut first = tokio::net::TcpStream::connect(&addr).await.unwrap();
        first.write_all(b"P").await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut second = tokio::net::TcpStream::connect(&addr).await.unwrap();
        expect_closed(&mut second, "a past-the-cap connection").await;

        assert_eq!(
            sum_of(
                &registry.drain(0),
                "logit.input.connections.rejected",
                Some(("reason", "limit"))
            ),
            Some(1.0)
        );

        drop(first);
    }

    /// A TCP health check -- `demo/haproxy/haproxy.cfg`'s `server logit logit:4318 check` against
    /// `demo/logit.yaml`'s plaintext `browser_in`, a Kubernetes `tcpSocket` probe, `nc -z` --
    /// connects and closes without sending a byte, at whatever interval it is configured with.
    /// Before the first-byte peek existed, `auto::Builder`'s own `ReadVersion` read that immediate
    /// EOF as `Version::H1` and the connection ended silently; the peek must not turn it into a
    /// warn plus a `connection_error` count per probe. Only the *deadline* (and a real read error)
    /// is a fault on that arm -- the same call `crate::tcp` makes for an EOF before its first
    /// frame.
    #[tokio::test]
    async fn a_plaintext_probe_that_closes_before_sending_is_not_a_connection_error() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("otlp_in", "otlp_in", "listener");
        let (addr, input) = bound_input(OtlpTransport::Http).await;
        let mut input = input.with_diagnostics(
            logit_core::Diagnostics::new("otlp_in").with_telemetry(telemetry.clone()),
        );
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Three probes, so a throttle that reports on powers of two could not hide a regression
        // behind suppression: `warn_throttled` counts `logit.component.diagnostics` on *every*
        // occurrence, reported or not.
        for _ in 0..3 {
            let probe = tokio::net::TcpStream::connect(&addr).await.unwrap();
            drop(probe);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert_eq!(
            sum_of(
                &registry.drain(0),
                "logit.component.diagnostics",
                Some(("key", "connection_error"))
            ),
            None,
            "a probe that connects and closes cleanly is not a connection error"
        );
    }

    /// The plaintext twin of `a_silent_tls_connection_is_closed_after_the_handshake_timeout`, and
    /// the case that actually matters in production: `otlp_in` with no `tls:` block is the default
    /// shape, and until the first-byte peek landed it had no pre-request bound at all. Under
    /// `with_max_connections(1)` the follow-up request can only be served if the silent
    /// connection's permit genuinely came back.
    #[tokio::test]
    async fn a_silent_plaintext_connection_is_closed_after_the_handshake_timeout() {
        let (addr, input) = bound_input(OtlpTransport::Http).await;
        let mut input =
            input.with_max_connections(1).with_handshake_timeout(Duration::from_millis(50));
        let (sink, mut rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Connected, not a byte sent, and held (not dropped) until the close is observed -- so
        // nothing but the server's own deadline could have closed it.
        let mut silent = tokio::net::TcpStream::connect(&addr).await.unwrap();
        expect_closed(&mut silent, "a plaintext connection that sent no bytes").await;

        let mut encoder = logit_proto::otlp::OtlpEncoder::new();
        let payloads =
            logit_proto::SignalEncoder::encode_signals(&mut encoder, &metric_batch()).unwrap();
        let (_, body) = payloads.into_iter().find(|(s, _)| *s == Signal::Metrics).unwrap();
        let response = post_raw(
            &addr,
            "/v1/metrics",
            "Content-Type: application/x-protobuf\r\nConnection: close\r\n",
            &body,
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
        recv_batch(&mut rx).await;

        drop(silent);
    }

    /// A first-byte deadline must not become a request deadline: the peek resolves on the very
    /// first byte, and everything after it belongs to `hyper`'s own read loop, which this module
    /// deliberately installs no timer on (this module's "What it still does not bound" section --
    /// `header_read_timeout` would re-arm across idle keep-alive gaps). So a client that dribbles
    /// its request head out over four times the budget still gets a 200.
    #[tokio::test]
    async fn the_first_byte_deadline_does_not_apply_once_a_request_has_started() {
        let (addr, input) = bound_input(OtlpTransport::Http).await;
        let mut input = input.with_handshake_timeout(Duration::from_millis(50));
        let (sink, mut rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut encoder = logit_proto::otlp::OtlpEncoder::new();
        let payloads =
            logit_proto::SignalEncoder::encode_signals(&mut encoder, &metric_batch()).unwrap();
        let (_, body) = payloads.into_iter().find(|(s, _)| *s == Signal::Metrics).unwrap();

        let head = format!(
            "POST /v1/metrics HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\nContent-Type: \
             application/x-protobuf\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();

        let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        // The first byte immediately -- that is all the peek ever waits for.
        stream.write_all(&head[..1]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await; // 4x the budget
        stream.write_all(&head[1..]).await.unwrap();
        stream.write_all(&body).await.unwrap();

        let mut buf = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut buf)).await;
        let response = String::from_utf8_lossy(&buf).into_owned();
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
        recv_batch(&mut rx).await;
    }

    /// `peek` is `MSG_PEEK`: it consumes nothing, so `auto::Builder`'s own `ReadVersion` sniff
    /// still sees the full 24-byte HTTP/2 preface and no rewind buffer is needed. h2c
    /// prior-knowledge is the case that would break first if the bound were an ordinary read.
    #[tokio::test]
    async fn an_h2c_prior_knowledge_request_still_negotiates_after_the_first_byte_peek() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, mut rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut encoder = logit_proto::otlp::OtlpEncoder::new();
        let payloads =
            logit_proto::SignalEncoder::encode_signals(&mut encoder, &metric_batch()).unwrap();
        let (_, body) = payloads.into_iter().find(|(s, _)| *s == Signal::Metrics).unwrap();

        let stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let io = TokioIo::new(stream);
        let (mut sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
            .handshake(io)
            .await
            .expect("h2c prior-knowledge handshake should succeed");
        tokio::spawn(conn);

        let req = http::Request::builder()
            .method(Method::POST)
            .uri("/v1/metrics")
            .header("content-type", "application/x-protobuf")
            .body(Full::new(body))
            .unwrap();
        let res = sender.send_request(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        recv_batch(&mut rx).await;
    }

    /// The gauge counts permit holders: 1 while a connection is being served, back to 0 once it
    /// ends. `Telemetry::gauge` is last-write-wins until a drain, so each drain reports the value
    /// the listener last wrote.
    #[tokio::test]
    async fn the_connections_gauge_tracks_a_live_connection_and_returns_to_zero() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("otlp_in", "otlp_in", "listener");
        let (addr, input) = bound_input(OtlpTransport::Http).await;
        let mut input = input.with_telemetry(telemetry);
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // One byte, so the connection clears the peek and stays open inside `hyper`.
        let mut open = tokio::net::TcpStream::connect(&addr).await.unwrap();
        open.write_all(b"P").await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(gauge_of(&registry.drain(0), "logit.input.connections"), Some(1.0));

        drop(open);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(gauge_of(&registry.drain(0), "logit.input.connections"), Some(0.0));
    }

    fn metric_batch() -> logit_core::EventBatch {
        logit_core::EventBatch {
            resource: std::sync::Arc::new(logit_core::Resource::default()),
            scope: None,
            events: vec![logit_core::Event::metric(
                1,
                logit_core::AttrMap::new(),
                logit_core::MetricRecord::new(
                    logit_core::interner::intern("x"),
                    logit_core::MetricKind::counter(1.0),
                ),
            )],
        }
    }

    // ---- idle timeout -------------------------------------------------------------------------
    //
    // Real durations (50-300ms), never `tokio::time::pause()`: these tests are about a timer
    // racing hyper's own read loop, and paused time would advance straight past the reads that
    // loop is sitting in. The "closed within" assertions go through `expect_closed`'s 2s ceiling
    // against deadlines of at most 300ms; the "still open" ones assert
    // `timeout(50ms, read) == Err(Elapsed)`, which scheduler lag can only make *more* true.

    /// [`metric_batch`] encoded as one OTLP/protobuf `/v1/metrics` body -- the three lines
    /// several tests above spell out inline, hoisted for the ones below that need a real request
    /// more than once.
    fn metric_body() -> Bytes {
        let mut encoder = logit_proto::otlp::OtlpEncoder::new();
        let payloads =
            logit_proto::SignalEncoder::encode_signals(&mut encoder, &metric_batch()).unwrap();
        payloads.into_iter().find(|(s, _)| *s == Signal::Metrics).unwrap().1
    }

    /// Asserts a connection is still open by reading from it and expecting nothing: this
    /// listener never speaks unprompted, so a blocked read means the connection is live, while a
    /// closed one returns `Ok(0)` (or `ECONNRESET`) immediately. The inverse of [`expect_closed`]
    /// and the twin of `crate::tcp`'s own helper of this name, and lag-proof in the direction
    /// that matters -- a slow scheduler makes the read *more* likely to time out, never less.
    async fn expect_still_open<S: tokio::io::AsyncRead + Unpin>(stream: &mut S, what: &str) {
        let mut buf = [0u8; 1];
        match tokio::time::timeout(Duration::from_millis(50), stream.read(&mut buf)).await {
            Err(_elapsed) => {}
            Ok(Ok(0)) => panic!("{what}: expected the connection to still be open, got a close"),
            Ok(Ok(n)) => panic!("{what}: expected no bytes, got {n}"),
            Ok(Err(err)) => panic!("{what}: expected the connection to still be open, got {err}"),
        }
    }

    /// [`post_raw`]'s keep-alive half: writes one complete HTTP/1.1 POST on an already-open
    /// stream and, crucially, sends **no** `Connection: close`, so hyper parks on the next
    /// request rather than closing once this one is answered. For the tests that send more than
    /// one request down one connection, or that keep reading from it afterwards.
    async fn write_request<S: tokio::io::AsyncWrite + Unpin>(
        stream: &mut S,
        addr: &str,
        path: &str,
        body: &[u8],
    ) {
        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\nContent-Type: \
             application/x-protobuf\r\n\r\n",
            body.len()
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
    }

    /// Reads exactly one response head (through the blank line) off a keep-alive connection --
    /// `read_to_end` would block until the *connection* ends, which is the thing under test here.
    /// Every response read this way is a protobuf success, whose body is empty
    /// (`export_response(0, "")`), so the head is all there is to consume before the next
    /// request.
    async fn read_response_head<S: tokio::io::AsyncRead + Unpin>(
        stream: &mut S,
        what: &str,
    ) -> String {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            let read = tokio::time::timeout_at(deadline, stream.read(&mut byte))
                .await
                .unwrap_or_else(|_| panic!("{what}: no complete response head within 5s"))
                .unwrap_or_else(|err| panic!("{what}: read failed: {err}"));
            assert_ne!(
                read,
                0,
                "{what}: the connection closed mid-head: {:?}",
                String::from_utf8_lossy(&head)
            );
            head.extend_from_slice(&byte[..read]);
        }
        String::from_utf8_lossy(&head).into_owned()
    }

    /// [`expect_closed`] for a peer that has already written something of its own: an HTTP/2
    /// server sends its `SETTINGS` frame the instant a connection arrives, *before* it has read
    /// the client's preface, so "the next byte off this socket is a close" is simply not true of
    /// `protocol: grpc`. Reads to EOF instead -- the same assertion, a few frames later.
    async fn expect_closed_after_draining<S: tokio::io::AsyncRead + Unpin>(
        stream: &mut S,
        what: &str,
    ) {
        let mut drained = Vec::new();
        match tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut drained)).await {
            Err(_elapsed) => panic!("{what}: expected a close within 2s"),
            Ok(Ok(_)) => {}
            // A close with bytes still unread in the peer's receive queue is an RST, not a FIN.
            Ok(Err(err)) if err.kind() == std::io::ErrorKind::ConnectionReset => {}
            Ok(Err(err)) => panic!("{what}: read failed outright: {err}"),
        }
    }

    /// A request body that yields `.0` once and then never yields again *and never wakes* --
    /// what a client that starts uploading and stalls looks like from the server's side, and the
    /// only thing [`collect_with_stall_bound`]'s per-frame bound can be driven by. `Pending` with
    /// no registered waker is the whole point: nothing will ever poll this body again.
    struct StalledBody(Option<Bytes>);

    impl hyper::body::Body for StalledBody {
        type Data = Bytes;
        type Error = std::convert::Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            match self.0.take() {
                Some(data) => Poll::Ready(Some(Ok(Frame::data(data)))),
                None => Poll::Pending,
            }
        }
    }

    /// The whole point of `idle_timeout:` on this listener: a pooled keep-alive connection that
    /// finished its export and then went quiet gives up its connection-cap permit instead of
    /// holding it forever. Proven under `with_max_connections(1)`, so the follow-up request can
    /// only be served if the first connection's permit genuinely came back.
    ///
    /// Also the pin for "policy, not a fault": the close is counted
    /// `logit.input.connections.closed{reason="idle"}` and the listener's `connection_error`
    /// diagnostic never fires, which is what `drive_with_idle` returning `Ok(())` rather than an
    /// `Err` buys (this module's "Idle timeout" doc section). The h1 case here is the one
    /// `graceful_shutdown` closes on its own -- an idle keep-alive connection is `KA::Idle`, so
    /// `disable_keep_alive` closes it immediately and the grace is never spent.
    #[tokio::test]
    async fn an_idle_keep_alive_http_connection_is_closed_after_the_idle_timeout_and_releases_its_permit(
    ) {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("otlp_in", "otlp_in", "listener");
        let diag = logit_core::Diagnostics::new("otlp_in").with_telemetry(telemetry.clone());
        let listener_diag = diag.clone();
        let (addr, input) = bound_input(OtlpTransport::Http).await;
        let mut input = input
            .with_telemetry(telemetry)
            .with_diagnostics(diag)
            .with_max_connections(1)
            .with_idle_timeout(Some(Duration::from_millis(100)));
        let (sink, mut rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // One complete export, keep-alive, so the connection settles idle inside hyper with the
        // first-byte peek long behind it -- the idle clock is the only thing that can end it.
        let mut keep_alive = tokio::net::TcpStream::connect(&addr).await.unwrap();
        write_request(&mut keep_alive, &addr, "/v1/metrics", &metric_body()).await;
        let head = read_response_head(&mut keep_alive, "the keep-alive export").await;
        assert!(head.starts_with("HTTP/1.1 200"), "got: {head}");
        recv_batch(&mut rx).await;

        expect_closed(&mut keep_alive, "a keep-alive connection quiet past its idle_timeout").await;

        let drained = registry.drain(0);
        assert_eq!(
            sum_of(&drained, "logit.input.connections.closed", Some(("reason", "idle"))),
            Some(1.0),
            "an idle close is counted"
        );
        assert_eq!(
            listener_diag.occurrences("connection_error"),
            0,
            "and never diagnosed -- an idle close returns Ok(()), so the accept loop's \
             connection_error path must not see it"
        );

        // Under `with_max_connections(1)` this can only be answered if the permit came back.
        let response = post_raw(
            &addr,
            "/v1/metrics",
            "Content-Type: application/x-protobuf\r\nConnection: close\r\n",
            &metric_body(),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"), "permit came back, got: {response}");
        recv_batch(&mut rx).await;

        drop(keep_alive);
    }

    /// The narrowing this listener's idle clock carries, made a test: it resets on request
    /// *completion*, so a connection that produced one head byte and then stopped has never
    /// completed anything and is closed at the idle deadline measured from the connection's own
    /// start. This is also the case `graceful_shutdown` alone cannot close -- one byte leaves
    /// hyper-util's `auto` builder inside its pre-sniff `ReadVersion` (`P` could still begin
    /// either `POST` or the h2 `PRI` preface), and a fresh h1 connection mid-head is `KA::Busy`
    /// -- so the bounded grace and the drop after it are what actually end it. The grace is
    /// `handshake_timeout`, set to 50ms here purely so that bound is visible inside
    /// `expect_closed`'s 2s ceiling.
    #[tokio::test]
    async fn a_fresh_http_connection_that_sent_one_head_byte_is_closed_after_the_idle_timeout() {
        let (addr, input) = bound_input(OtlpTransport::Http).await;
        let mut input = input
            .with_idle_timeout(Some(Duration::from_millis(100)))
            .with_handshake_timeout(Duration::from_millis(50));
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // One byte, immediately -- enough to clear the first-byte peek, so the close that
        // follows can only have come from the idle clock, not from `handshake_timeout`.
        let mut dribbling = tokio::net::TcpStream::connect(&addr).await.unwrap();
        dribbling.write_all(b"P").await.unwrap();

        expect_closed(&mut dribbling, "a connection that sent one head byte and then stopped")
            .await;
    }

    /// The gRPC transport's twin of the keep-alive case: an established h2 connection that
    /// finished its export and went quiet is GOAWAY'd and closed. "Closed" is observed from the
    /// client's own connection future ending -- an h2 client has no socket to read directly, and
    /// its connection task is exactly what a GOAWAY plus a close terminates. `sender` is held
    /// alive throughout, so nothing on this side could have initiated the shutdown.
    #[tokio::test]
    async fn an_idle_grpc_connection_is_closed_after_the_idle_timeout() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("otlp_in", "otlp_in", "listener");
        let (addr, input) = bound_input(OtlpTransport::Grpc).await;
        let mut input =
            input.with_telemetry(telemetry).with_idle_timeout(Some(Duration::from_millis(100)));
        let (sink, mut rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let io = TokioIo::new(stream);
        let (mut sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
            .handshake(io)
            .await
            .unwrap();
        let client_conn = tokio::spawn(conn);

        let payload = one_span_payload();
        let mut framed = vec![0u8];
        framed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        framed.extend_from_slice(&payload);
        let req = http::Request::builder()
            .method(Method::POST)
            .uri(Signal::Traces.grpc_method())
            .header("content-type", "application/grpc+proto")
            .header("te", "trailers")
            .body(Full::new(Bytes::from(framed)))
            .unwrap();
        let res = sender.send_request(req).await.unwrap();
        let collected = res.into_body().collect().await.unwrap();
        assert_eq!(
            collected.trailers().expect("should carry trailers").get("grpc-status").unwrap(),
            "0"
        );
        recv_batch(&mut rx).await;

        // Either outcome proves the close: hyper's client connection future ends `Ok` on a clean
        // GOAWAY-then-FIN and `Err` if the socket goes first.
        let _closed = tokio::time::timeout(Duration::from_secs(2), client_conn)
            .await
            .expect("an idle gRPC connection should be closed within 2s")
            .expect("the client's connection task should not panic");

        // The client's future ends on the GOAWAY, which the server writes *during* its grace
        // poll -- so the count, which lands after that poll returns, is a moment behind it.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            sum_of(&registry.drain(0), "logit.input.connections.closed", Some(("reason", "idle"))),
            Some(1.0),
            "an idle close is counted on the gRPC transport too"
        );
        drop(sender);
    }

    /// `protocol: grpc`'s equivalent of the one-head-byte case, and the third state
    /// `graceful_shutdown` cannot close on its own: an h2 connection still `Handshaking` only
    /// gets an internal `close_pending` flag set, so the bounded grace (`handshake_timeout`,
    /// 50ms here) and the drop after it are what free the permit.
    #[tokio::test]
    async fn a_stalled_h2_preface_is_closed_after_the_idle_timeout() {
        let (addr, input) = bound_input(OtlpTransport::Grpc).await;
        let mut input = input
            .with_idle_timeout(Some(Duration::from_millis(100)))
            .with_handshake_timeout(Duration::from_millis(50));
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The first three bytes of the 24-byte h2 preface ("PRI * HTTP/2.0..."), then silence.
        let mut stalled = tokio::net::TcpStream::connect(&addr).await.unwrap();
        stalled.write_all(b"PRI").await.unwrap();

        expect_closed_after_draining(&mut stalled, "a connection stalled mid-h2-preface").await;
    }

    /// **The test the reset rule exists for.** A connection whose downstream is full is not
    /// idle -- it is waiting on *us* -- so a request parked in `Fanout::send` must hold the idle
    /// clock off entirely, however long that park lasts. A capacity-1 channel with nothing
    /// draining it puts the second request exactly there, and three idle timeouts' worth of
    /// sleep must not close the connection. Then the drain happens, the blocked request's
    /// response arrives, and the same connection serves a third request.
    ///
    /// A clock that ran while a request was in flight would close this connection and lose a
    /// request that had already been fully received.
    #[tokio::test]
    async fn a_request_blocked_on_a_full_downstream_is_not_closed_as_idle() {
        let idle = Duration::from_millis(100);
        let (addr, input) = bound_input(OtlpTransport::Http).await;
        let mut input = input.with_idle_timeout(Some(idle));
        let (sink, mut rx) = fanout_into_channel_with_capacity(1);
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let body = metric_body();
        let mut client = tokio::net::TcpStream::connect(&addr).await.unwrap();

        // The first export fills the channel's one slot and completes.
        write_request(&mut client, &addr, "/v1/metrics", &body).await;
        let head = read_response_head(&mut client, "the first export").await;
        assert!(head.starts_with("HTTP/1.1 200"), "got: {head}");

        // The second, on the same connection, parks in `Fanout::send` with nothing draining.
        write_request(&mut client, &addr, "/v1/metrics", &body).await;
        tokio::time::sleep(idle * 3).await;
        expect_still_open(&mut client, "a request blocked on a full downstream").await;

        // Draining frees the slot, the parked send returns, and its response arrives.
        recv_batch(&mut rx).await;
        let head = read_response_head(&mut client, "the blocked export").await;
        assert!(head.starts_with("HTTP/1.1 200"), "got: {head}");
        recv_batch(&mut rx).await;

        // And the connection really was untouched: it still serves another request.
        write_request(&mut client, &addr, "/v1/metrics", &body).await;
        let head = read_response_head(&mut client, "a third export").await;
        assert!(head.starts_with("HTTP/1.1 200"), "got: {head}");
        recv_batch(&mut rx).await;
    }

    /// The body half of the bound: a request head that arrived in full followed by a body that
    /// stops mid-upload is answered `408` per *frame* rather than left to the whole-connection
    /// deadline, and the connection closes behind the response instead of waiting for another
    /// request that can never come. `read_to_end` asserts both halves at once -- it returns only
    /// when the peer closes, and what it returns is the response.
    #[tokio::test]
    async fn a_request_body_that_stalls_gets_408_and_the_connection_is_closed() {
        let (addr, input) = bound_input(OtlpTransport::Http).await;
        let mut input = input
            .with_idle_timeout(Some(Duration::from_millis(100)))
            // The grace `drive_with_idle` gives hyper to write the 408 out and close.
            .with_handshake_timeout(Duration::from_millis(200));
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // A complete head promising a real `Content-Length`, then two of those bytes and silence.
        let body = metric_body();
        let mut stalled = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let head = format!(
            "POST /v1/metrics HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\nContent-Type: \
             application/x-protobuf\r\n\r\n",
            body.len()
        );
        stalled.write_all(head.as_bytes()).await.unwrap();
        stalled.write_all(&body[..2]).await.unwrap();

        let mut buf = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), stalled.read_to_end(&mut buf))
            .await
            .expect("a stalled request body should be answered and closed within 5s")
            .expect("reading the response should not fail outright");
        let response = String::from_utf8_lossy(&buf);
        assert!(response.starts_with("HTTP/1.1 408"), "got: {response}");
        assert!(response.contains("stalled"), "the message should say what happened: {response}");
    }

    /// [`a_request_body_that_stalls_gets_408_and_the_connection_is_closed`]'s gRPC twin: the same
    /// per-frame bound, reported as `grpc-status: 4` (`DEADLINE_EXCEEDED`), and the same close
    /// once the response is out -- observed here through the client's connection future ending,
    /// since the client still believes it has a request body open.
    #[tokio::test]
    async fn a_stalled_grpc_request_body_gets_status_four_and_the_connection_is_closed() {
        let (addr, input) = bound_input(OtlpTransport::Grpc).await;
        let mut input = input
            .with_idle_timeout(Some(Duration::from_millis(100)))
            .with_handshake_timeout(Duration::from_millis(200));
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let io = TokioIo::new(stream);
        let (mut sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
            .handshake(io)
            .await
            .unwrap();
        let client_conn = tokio::spawn(conn);

        // A gRPC frame header promising eight payload bytes that never arrive.
        let req = http::Request::builder()
            .method(Method::POST)
            .uri(Signal::Traces.grpc_method())
            .header("content-type", "application/grpc+proto")
            .header("te", "trailers")
            .body(StalledBody(Some(Bytes::from_static(&[0u8, 0, 0, 0, 8]))))
            .unwrap();
        let res = sender.send_request(req).await.unwrap();
        let collected = res.into_body().collect().await.unwrap();
        let trailers = collected.trailers().expect("should carry trailers");
        assert_eq!(trailers.get("grpc-status").unwrap().to_str().unwrap(), "4");

        let _closed = tokio::time::timeout(Duration::from_secs(2), client_conn)
            .await
            .expect("the connection should be closed within 2s of the stalled body's response")
            .expect("the client's connection task should not panic");
        drop(sender);
    }

    /// The grace window's one real hazard, closed. A request that *starts* inside the grace --
    /// after `graceful_shutdown` has been called, before the connection has actually gone -- must
    /// be served, not dropped: on h1 the handler's own future is polled inside the connection
    /// future, so dropping the connection while that handler is parked in `Fanout::send` would
    /// discard a batch that never reached the fanout, the backpressure-causes-loss outcome
    /// `docs/adr/idle-connection-timeout.md` exists to prevent.
    ///
    /// **Two bytes at the start, not one.** One byte leaves hyper-util's `auto` builder inside
    /// its pre-sniff `ReadVersion` (`P` could still begin either `POST` or the h2 `PRI` preface),
    /// which `graceful_shutdown` cancels outright -- that is
    /// [`a_fresh_http_connection_that_sent_one_head_byte_is_closed_after_the_idle_timeout`]'s
    /// case, and it has nothing in flight to wait for. `PO` commits the sniff to HTTP/1.1, so
    /// what this test closes is a real h1 connection stopped mid-head: `KA::Busy`, which
    /// `graceful_shutdown` leaves running, which is exactly the state that can still pick a
    /// request up during the grace.
    ///
    /// The pre-filled capacity-1 channel is what parks the handler, and the batch it holds is a
    /// metric while the request carries a span, so "the request's batch arrived" is a distinct
    /// assertion from "the pre-filled one drained".
    ///
    /// **The timings, and why they are what they are.** A 200ms idle timeout and a 100ms grace,
    /// so the grace window runs from roughly 200ms to 300ms after this connection's first byte,
    /// and the rest of the request lands at 260ms -- 60ms *past* the idle deadline, so the
    /// request cannot have been in flight when it fired (which would make the test vacuous), and
    /// 40ms *inside* the grace, so it is genuinely picked up in the window under test. The
    /// downstream is then left blocked until 600ms, 300ms past the grace's expiry, which is what
    /// the old drop-on-expiry close would have lost the batch in.
    #[tokio::test]
    async fn a_request_that_starts_inside_the_grace_window_is_served_not_dropped() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("otlp_in", "otlp_in", "listener");
        let (addr, input) = bound_input(OtlpTransport::Http).await;
        let mut input = input
            .with_telemetry(telemetry)
            .with_idle_timeout(Some(Duration::from_millis(200)))
            // Doubles as the grace. Both halves of this test's margin are tens of milliseconds
            // wide (see the timings above), which is why neither number is any tighter.
            .with_handshake_timeout(Duration::from_millis(100));
        let (sink, mut rx) = fanout_into_channel_with_capacity(1);
        // Pre-filled, so the handler's own `Fanout::send` parks until this test drains it.
        sink.send(metric_batch()).await;
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let body = one_span_payload();
        let head = format!(
            "POST /v1/traces HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\nContent-Type: \
             application/x-protobuf\r\n\r\n",
            body.len()
        )
        .into_bytes();

        let mut late = tokio::net::TcpStream::connect(&addr).await.unwrap();
        late.write_all(&head[..2]).await.unwrap();

        // t+260ms: 60ms past the 200ms idle deadline, so `graceful_shutdown` has certainly been
        // called and nothing was in flight when it fired -- and 40ms inside the 100ms grace, so
        // the rest of the request is picked up by a connection that is already shutting down.
        tokio::time::sleep(Duration::from_millis(260)).await;
        late.write_all(&head[2..]).await.unwrap();
        late.write_all(&body).await.unwrap();

        // t+600ms: three further graces' worth of parked handler, 300ms past the window's
        // expiry. Under a close that dropped on grace expiry, this connection and this batch
        // would both be long gone by now.
        tokio::time::sleep(Duration::from_millis(340)).await;

        let prefilled = recv_batch(&mut rx).await;
        assert!(prefilled.events[0].span.is_none(), "the pre-filled batch drains first");
        let served = recv_batch(&mut rx).await;
        assert!(
            served.events[0].span.is_some(),
            "the request that started inside the grace window must reach the fanout"
        );

        let response = read_response_head(&mut late, "a request served inside the grace").await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");

        // And it still closes afterwards -- waiting the request out defers the close, it does
        // not cancel it.
        expect_closed(&mut late, "a connection that served a request inside its grace").await;
        assert_eq!(
            sum_of(&registry.drain(0), "logit.input.connections.closed", Some(("reason", "idle"))),
            Some(1.0),
            "the deferred close is still counted once, as an idle close"
        );
    }

    /// The default, and the promise that turning nothing on changes nothing: with no
    /// `idle_timeout` configured, a keep-alive connection that finished its export and went
    /// quiet stays open -- which is exactly what a long-interval OTLP exporter's pooled
    /// connection looks like between exports.
    #[tokio::test]
    async fn no_idle_timeout_leaves_a_keep_alive_connection_open() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, mut rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut keep_alive = tokio::net::TcpStream::connect(&addr).await.unwrap();
        write_request(&mut keep_alive, &addr, "/v1/metrics", &metric_body()).await;
        let head = read_response_head(&mut keep_alive, "the keep-alive export").await;
        assert!(head.starts_with("HTTP/1.1 200"), "got: {head}");
        recv_batch(&mut rx).await;

        // Three times the idle timeout every other test in this section configures.
        tokio::time::sleep(Duration::from_millis(300)).await;
        expect_still_open(&mut keep_alive, "a keep-alive connection with no idle_timeout").await;
    }
}
