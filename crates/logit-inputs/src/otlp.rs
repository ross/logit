//! `otlp_in`: OTLP logs, metrics, and traces over OTLP/HTTP (`POST` to `/v1/logs`,
//! `/v1/metrics`, `/v1/traces`, protobuf or JSON) or OTLP/gRPC (the three `Export` methods,
//! [`Signal::grpc_method`]), selected by `protocol` in config (`logit_config::OtlpProtocol`).
//! The gRPC server is a hand-rolled `hyper::server::conn::http2` service rather than `tonic`
//! (`docs/adr/hand-rolled-grpc-over-hyper.md`): each request and response body is one message
//! behind a 5-byte prefix (compressed flag, big-endian `u32` length), and the outcome rides in
//! `grpc-status`/`grpc-message` trailers on an HTTP `200`. A rejection maps to `12`
//! (`UNIMPLEMENTED`: a non-`POST`, an unknown method, an unsupported `grpc-encoding`), `3`
//! (`INVALID_ARGUMENT`: a malformed frame, gzip, or payload, or bytes after the one message a
//! unary body carries), `8` (`RESOURCE_EXHAUSTED`: over
//! [`MAX_REQUEST_BYTES`]), or `4` (`DEADLINE_EXCEEDED`: a stalled body).
//!
//! **One accept loop, one task per connection.** [`Input::run`] spawns a task per accepted
//! connection, each holding its own [`Fanout`] clone. HTTP connections are served by
//! [`hyper_util::server::conn::auto::Builder`], which handles HTTP/1.1 (what `curl` and most OTel
//! HTTP exporters speak) and h2c (prior-knowledge HTTP/2 without TLS) off one socket; gRPC
//! connections by [`hyper::server::conn::http2::Builder`] directly, since gRPC is HTTP/2 only.
//!
//! **Backpressure reaches the client.** A UDP listener's slow downstream means the kernel drops
//! datagrams; TCP has no such escape hatch. A slow downstream blocks the handler's delivery, which
//! stops reading that connection, which the client feels as its own write blocking. That is
//! correct for a reliable protocol (an OTLP exporter retries or buffers on its own timeout):
//! `docs/design/pipeline-graph.md`'s "Backpressure" section. A client that gives up and closes
//! cancels only the handler's wait: [`crate::http::deliver_detached`] runs the request's sends on
//! a task of their own, so every consumer still gets every batch of the request, however many
//! batches (one per resource) it decoded to. The retry then duplicates on every branch alike
//! rather than on some.
//!
//! **TLS is optional, per listener.** `tls:` in config ([`TlsServerSettings`]) turns it on for
//! both transports; without it the listener accepts plaintext. The handshake runs inside the
//! per-connection task, after that connection's [`MAX_CONCURRENT_CONNECTIONS`] permit is
//! acquired, so a slow or hostile handshake stalls only its own connection, counts against the
//! same bound as a slow request, and never blocks the accept loop
//! (`docs/adr/otlp-tls-and-pooled-grpc-client.md`).
//!
//! **Connection limit: reject, don't queue.** A [`tokio::sync::Semaphore`] capped at
//! [`MAX_CONCURRENT_CONNECTIONS`], acquired with `try_acquire_owned`: at capacity the accepted
//! stream is dropped and counted `logit.input.connections.rejected{reason="limit"}` rather than
//! parked behind a permit that may never come. A blocking `acquire_owned().await` would stall the
//! accept loop itself, and enough silent connections would stop this listener draining its
//! backlog. The rejection happens *before* any TLS accept: OTLP has no in-band "try later", so
//! there is nothing to spend a handshake saying. `crate::tcp`'s driver makes the same call
//! (`crates/logit-inputs/src/tcp.rs`'s "Connection limit" section; `logit_in` differs, finishing
//! the TLS accept first to send a `Reject` frame). The `logit.input.connections` gauge counts
//! permit holders only.
//!
//! **Handshake timeout.** [`OtlpInput::handshake_timeout`] (default [`HANDSHAKE_TIMEOUT`], set
//! from `handshake_timeout:` by [`OtlpInput::with_handshake_timeout`]) bounds each pre-request
//! phase, as `crate::tcp`'s driver does (`crates/logit-inputs/src/tcp.rs`'s "Pre-handshake
//! timeout" section): the TLS accept on a TLS listener, the wait for the first byte on a
//! plaintext one. Without it, a client that completes the TCP connect and never speaks pins a
//! permit forever.
//!
//! **The plaintext first-byte bound is a `peek`, not a read.** `tokio::net::TcpStream::peek` is
//! `recv(..., MSG_PEEK)`: it waits for the first byte and leaves it queued, so the stream handed
//! to `hyper` is untouched. [`hyper_util::server::conn::auto::Builder`]'s `ReadVersion` sniff (up
//! to 24 bytes, telling HTTP/1.1 from an h2 preface) then reads those bytes itself with no rewind
//! buffer; bounding the sniff instead would mean reimplementing it. The TLS arm gets no peek:
//! `acceptor.accept` already waits on the first bytes under the same budget.
//!
//! **A peer that closes before sending anything is not a fault.** That is every TCP health
//! check: `demo/haproxy/haproxy.cfg`'s `server logit logit:4318 check` against `demo/logit.yaml`'s
//! plaintext `browser_in`, a Kubernetes `tcpSocket` probe, `nc -z`. A `peek` of `Ok(0)` returns
//! `Ok(())`; only the deadline (a connection held open saying nothing) and a read error reach
//! `connection_error`, which a probe would otherwise hit once per interval, forever. `crate::tcp`
//! makes the same call for an EOF before its first frame.
//!
//! **Idle timeout.** [`OtlpInput::with_idle_timeout`] (`idle_timeout:`, off unless set) bounds how
//! long a connection may sit with no request in flight before this listener closes it and
//! returns its permit (`docs/adr/idle-connection-timeout.md`). Without it, a connection that
//! sends one byte and stops has cleared the peek, sits in `hyper`'s read loop, and holds its
//! permit indefinitely; under `protocol: grpc`, likewise one byte of the HTTP/2 preface.
//!
//! *Tracked at the service, not the socket.* One [`Activity`] per connection counts the requests
//! in flight and stamps when the last one finished; its guard,
//! [`InFlight`](crate::http::InFlight), wraps each handler inside `service_fn`, so an early
//! return or an unwind stamps it too. The clock runs only while the count is zero. A timer around
//! the IO would be wrong: hyper 1.11.1's h1 server polls the socket read *mid-message*
//! (`mid_message_detect_eof`'s `force_io_read`, to notice a peer closing while a handler works),
//! so an IO-level timer would tick during ordinary backpressure and read a stalled downstream as
//! a silent peer. That is the failure the shared driver's reset rule exists to avoid
//! (`crates/logit-inputs/src/tcp.rs`'s "Idle timeout" section), hidden inside hyper's internals.
//!
//! *Reset on request completion, not on bytes.* hyper owns the bytes, so the finest grain visible
//! here is a request starting and finishing. A request *head* that dribbles in more slowly than
//! `idle_timeout` on an otherwise-quiet keep-alive connection is therefore closed: a documented
//! narrowing of the semantic every other listener implements, not a bug. A request *body* that
//! stalls mid-upload gets its own narrower bound: [`collect_with_stall_bound`] puts a per-frame
//! timeout on the body, answers `408` (HTTP) or `grpc-status: 4` (gRPC), and closes the
//! connection once the handler returns, rather than leaving it to the whole-connection deadline.
//!
//! *`graceful_shutdown`, then a bounded grace, then drop.* [`drive_with_idle`] never drops a live
//! socket out from under hyper: it calls `graceful_shutdown`, polls the connection for at most
//! `handshake_timeout` (reused as the grace; no new knob), then drops it whatever that poll
//! returned. Both steps are needed, per the pinned hyper 1.11.1 / hyper-util 0.1.20 sources:
//! `graceful_shutdown` closes an *idle keep-alive* h1 connection promptly (`disable_keep_alive`
//! calls `state.close()` when the connection's `KA` state is `Idle`) and GOAWAYs an established
//! h2 one, the common cases. But a *fresh* h1 connection stopped mid-head is `KA::Busy` and keeps
//! waiting, and an h2 connection still handshaking only sets an internal `close_pending` flag: the
//! grace-then-drop exists for those two. hyper-util's pre-sniff `ReadVersion` is a third shape:
//! `graceful_shutdown` cancels it, and the first grace poll resolves at once to
//! `Err("Cancelled")`, which is why the post-shutdown result is ignored. The drop waits for
//! one thing: a request that *started* inside the grace and has not returned. Dropping the
//! connection while its handler is parked in `Fanout::send` would discard a batch that never
//! reached the fanout, so [`drive_with_idle`] polls that request out and then lets the grace run
//! again for its response. Nothing a *silent* peer does can extend the window; only being served
//! can.
//!
//! *Policy, not a fault.* An idle close counts `logit.input.connections.closed{reason="idle"}`
//! and returns `Ok(())`, so it never reaches the `connection_error` diagnostic: counted, not
//! diagnosed, as `crate::tcp` does for its own idle closes.
//!
//! *Why not hyper's `http1().header_read_timeout(..)`.* In the pinned hyper 1.11.1
//! (`src/proto/h1/conn.rs`) that timer is armed at the *top* of `poll_read_head`, before a header
//! byte is parsed, and `State::idle` sets `notify_read = true` whenever it is configured ("Next
//! read will start and poll the header read timeout, so we can close the connection if another
//! header isn't received in a timely manner"), so it re-arms across every idle keep-alive gap. It
//! is an idle timeout under a first-head name, reachable only by also bounding first heads, and
//! h1-only ([`hyper::server::conn::http2::Builder`] has no equivalent). `idle_timeout` is that
//! bound made explicit, opt-in, and available on both transports.
//!
//! **Gzip, and nothing else.** `Content-Encoding: gzip` (HTTP) and a gRPC frame's compressed flag
//! with `grpc-encoding: gzip` are decoded via [`grpc::inflate_bounded`]; any other declared
//! encoding is rejected (`415`/`grpc-status: 12`). Both headers are matched case-insensitively,
//! since content-coding names are (RFC 9110 §8.4.1). The *decompressed* size is bounded to
//! [`MAX_REQUEST_BYTES`], the cap already on the compressed body, so a compression bomb is
//! rejected rather than inflated (`docs/adr/otlp-compression-and-decompression-bounds.md`).
//!
//! **OTLP/HTTP accepts protobuf or JSON; OTLP/gRPC accepts protobuf only.** `handle_http` picks
//! the decode path off `Content-Type` (absent or empty means protobuf, for every client that
//! predates OTLP/JSON); the success response mirrors the request's encoding, per spec. No OTel SDK
//! speaks `application/grpc+json`.
//! [ADR `otlp-json-decoding`](../../../../docs/adr/otlp-json-decoding.md) covers the JSON dialect
//! (hex vs. base64 ids, string-or-number 64-bit fields, and why it's hand-parsed rather than
//! generated). Every error response, on both encodings, is `text/plain`, where the spec wants a
//! protobuf-encoded `Status`; tracked in `docs/known-gaps.md`.
//!
//! **Size and concurrency limits.** [`MAX_REQUEST_BYTES`] (4 MiB) matches the OTel collector's
//! default `max_recv_msg_size`; a larger request is rejected (`413`/`grpc-status: 8`,
//! `RESOURCE_EXHAUSTED`) before it can grow an unbounded buffer. That bounds one request;
//! [`crate::http::MAX_CONCURRENT_STREAMS`] bounds the requests on one HTTP/2 connection and
//! [`MAX_CONCURRENT_CONNECTIONS`] how many connections are served at once, so the listener's
//! worst-case memory is finite ([`MAX_CONCURRENT_CONNECTIONS`] has the figure).
//!
//! **`partial_success` is always empty on a successful decode.** It exists to report which
//! records in an accepted request were rejected, but `logit_proto::SignalDecoder::decode_signal`
//! returns no per-call reject count (it counts skips only in its own
//! `logit.input.metrics.skipped{metric_kind, reason}` telemetry, `logit-proto`'s `otlp::metrics`
//! module doc), so there is nothing to echo. A malformed request (bad protobuf, an out-of-range
//! span id) fails whole (`400`/`grpc-status: 3`). Threading a count through is a `SignalDecoder`
//! API change, tracked in `docs/known-gaps.md`.

use crate::http::{
    body_read_error_message, collect_with_stall_bound, drive_with_idle, Activity, BodyReadError,
};
use crate::Input;
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use http_body_util::{Full, Limited};
// Only this module's tests collect a body directly; the lib path uses `collect_with_stall_bound`.
#[cfg(test)]
use http_body_util::BodyExt;
use hyper::body::{Frame, Incoming};
use hyper::service::service_fn;
#[cfg(test)]
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use logit_core::{Diagnostics, Telemetry};
use logit_pipeline::Fanout;
use logit_proto::otlp::grpc::{self, InflateError};
use logit_proto::otlp::OtlpDecoder;
use logit_proto::{Signal, SignalDecoder};
// Only the test module's `tls_connector` reads PEM files directly; server TLS is `crate::tls`.
#[cfg(test)]
use rustls_pki_types::pem::PemObject;
#[cfg(test)]
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

/// Matches the OTel collector's default `max_recv_msg_size`.
const MAX_REQUEST_BYTES: usize = 4 * 1024 * 1024;

/// Bounds the connections [`Input::run`] serves at once. With 4 MiB requests this listener's worst
/// case is 1.6 TiB, a bound rather than a memory budget ([`crate::http::MAX_CONCURRENT_STREAMS`]
/// has the formula). The same 1024 as `logit_in` and `crate::tcp`'s listeners: no protocol reason
/// for an OTLP listener to differ, and one figure for an operator to learn. Not operator-tunable;
/// make it a config field if a deployment needs a different number.
///
/// **A connection past the cap is rejected, not queued** (this module's "Connection limit").
/// `OtlpInput::with_max_connections` lowers it in tests.
///
/// **That figure is the protobuf path's worst case, not JSON's.** An OTLP/JSON request is parsed
/// into a `serde_json::Value` tree first, one `Map`/`Vec`/`String`/`Number` allocation per node,
/// several times the source bytes for a nested OTLP payload, where `prost::Message::decode` builds
/// the target structs directly. The bound still holds (a JSON body is capped at
/// `MAX_REQUEST_BYTES` before parsing), so the all-JSON worst case is a finite multiple of it;
/// `docs/known-gaps.md`'s OTLP section has the measured multiple.
const MAX_CONCURRENT_CONNECTIONS: usize = 1024;

/// Default for [`OtlpInput::handshake_timeout`]: how long a connection has, per pre-request phase,
/// before this listener releases its [`MAX_CONCURRENT_CONNECTIONS`] permit. The same 5s as
/// `logit_in` and `crate::tcp`, mirrored by hand in `logit_config::default_handshake_timeout`.
/// Also the grace an idle close gives hyper (this module's "Idle timeout").
const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Which OTLP wire transport this listener accepts. Mirrors `logit_outputs::otlp::OtlpTransport`
/// rather than sharing a type, since neither crate depends on `logit-config`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpTransport {
    Http,
    Grpc,
}

/// `crate::tls::TlsServerSettings`, re-exported so `logit-cli::pipeline::build_spec` imports it as
/// `logit_inputs::otlp::TlsServerSettings`.
pub use crate::tls::TlsServerSettings;

pub struct OtlpInput {
    bind: String,
    transport: OtlpTransport,
    diag: Diagnostics,
    telemetry: Telemetry,
    tls: Option<Arc<rustls::ServerConfig>>,
    /// Set by [`Input::bind`], taken by [`Input::run`]. `None` after a run, so a second run
    /// rebinds.
    listener: Option<TcpListener>,
    /// See this module's "Handshake timeout" section.
    handshake_timeout: std::time::Duration,
    /// `None`, the default, means no idle timeout. See [`Self::with_idle_timeout`].
    idle_timeout: Option<std::time::Duration>,
    /// [`MAX_CONCURRENT_CONNECTIONS`] unless [`OtlpInput::with_max_connections`] (test-only)
    /// lowers it.
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

    /// The address bound, once [`Input::bind`] has run, so a caller can learn the OS-assigned port
    /// without a bind-drop-rebind race.
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

    /// Turns on TLS termination (`tls:` in config) for either transport. Paths in `settings`
    /// resolve against `base_dir`, the config file's directory.
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

    /// Overrides [`HANDSHAKE_TIMEOUT`] for both pre-request budgets, the TLS accept and the
    /// plaintext first-byte peek (`handshake_timeout:` in config). Graph rule 45 rejects `0s`.
    pub fn with_handshake_timeout(mut self, handshake_timeout: std::time::Duration) -> Self {
        self.handshake_timeout = handshake_timeout;
        self
    }

    /// Bounds how long a connection may sit with no request in flight before this listener closes
    /// it (`idle_timeout:` in config; off when `None` or never called). This module's "Idle
    /// timeout" section has the semantics. Graph rule 53 rejects `Some(0s)`.
    ///
    /// Takes the `Option`, like `crate::tcp::TcpListener::with_idle_timeout`, so a config that
    /// omitted the field needs no caller-side `if let`.
    pub fn with_idle_timeout(mut self, idle_timeout: Option<std::time::Duration>) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    /// Test-only override of [`MAX_CONCURRENT_CONNECTIONS`], so the cap is reachable with two
    /// connections instead of 1025.
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
        let connection_limit = Arc::new(tokio::sync::Semaphore::new(self.max_connections));
        // `TlsAcceptor::from` wraps the `Arc<ServerConfig>`, so a per-connection clone is an `Arc`
        // clone, not a config rebuild.
        let tls_acceptor = self.tls.clone().map(TlsAcceptor::from);
        let live_connections = crate::listener::LiveConnections::new(self.telemetry.clone());
        let handshake_timeout = self.handshake_timeout;
        let idle_timeout = self.idle_timeout;
        // `crate::tcp`'s accept-queue gauges: `logit.input.accept_queue.depth`/`.utilization`,
        // sampled before each accept and once a second while waiting for one.
        let mut accept_queue =
            crate::tcp::AcceptQueueSampler::new(self.telemetry.clone(), self.diag.clone());
        loop {
            let (stream, _peer) = accept_queue.accept(&listener).await?;

            // Non-blocking, and before any TLS accept (this module's "Connection limit").
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
            let live_connections = live_connections.clone();
            tokio::spawn(async move {
                // Held for the connection's lifetime; released on drop.
                let _permit = permit;
                // Counted out on drop, so a panicking handler brings the gauge back down too.
                let _live = live_connections.enter();

                // The handshake runs here, after the permit, so it stalls only this connection.
                let result = match tls_acceptor {
                    // Bounded, or a client that sends no ClientHello pins this permit forever.
                    // Failure and timeout both reach `connection_error` below, and the permit
                    // comes back when this task ends. No first-byte peek: `acceptor.accept`
                    // already waits on the first bytes under the same budget.
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
                    // The plaintext arm's budget: a `peek` that consumes nothing (this module's
                    // "peek, not a read"). A read error and the deadline reach `connection_error`
                    // like the TLS arm's; a clean close before the first byte (`Ok(0)`) is a TCP
                    // health check (this module's "not a fault"), as `crate::tcp`'s
                    // `ReadStep::Eof` before any frame is.
                    None => {
                        // Bound to a local, not matched directly: the scrutinee's temporaries
                        // (`peek`'s borrow of `stream`) would outlive the arms, and the success
                        // arm moves `stream` into `hyper`.
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

                // One connection's I/O error (a client disconnecting mid-request, a TLS preamble
                // on a plaintext port) is not fatal to the listener; only `accept` failing is.
                if let Err(err) = result {
                    diag.warn_throttled("connection_error", err);
                }
            });
        }
    }
}

/// Serves one accepted (and, with TLS on, handshaken) connection to completion. Generic over the
/// IO type so the plaintext and TLS cases share everything below `run`'s `tls_acceptor` branch.
///
/// `grace` is the budget [`drive_with_idle`] gives hyper to shut down in once `idle_timeout`
/// fires (`handshake_timeout`, reused). With `idle_timeout: None` the connection is awaited.
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
    // One tracker per connection: the service stamps it as requests start and finish, the driver
    // below reads it. The body-frame stall bound is `idle_timeout` too, so a connection with no
    // idle bound gets no per-frame one either.
    let activity = Arc::new(Activity::new());
    match transport {
        OtlpTransport::Http => {
            let svc = service_fn({
                let activity = Arc::clone(&activity);
                let (sink, telemetry) = (sink.clone(), telemetry.clone());
                move |req| {
                    // `enter` here, not inside the returned future: hyper calls the service as
                    // soon as a request head is parsed, so the in-flight count rises then.
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
            let builder = crate::http::auto_builder();
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
            let conn = crate::http::h2_builder().serve_connection(io, svc);
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
    #[cfg(test)]
    if req.headers().contains_key(tests::PANIC_HEADER) {
        panic!("a test asked this handler to panic");
    }
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
    // Matched on value, not presence: an explicit `identity` declares no compression and must not
    // be a `415`.
    let gzip_encoded = match crate::http::Encoding::from_headers(req.headers()) {
        Ok(crate::http::Encoding::Identity) => false,
        Ok(crate::http::Encoding::Gzip) => true,
        Ok(_) | Err(_) => {
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
        // connection closes once this response is out (this module's "Idle timeout").
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
        match grpc::inflate_bounded(&bytes, MAX_REQUEST_BYTES) {
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
            crate::http::deliver_detached(&sink, batches).await;
            // The spec: "The server MUST use the same Content-Type in the response as it received
            // in the request." A JSON request gets `{}`, not an empty body
            // ([`export_response_json`]).
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

/// Which OTLP/HTTP encoding a request's `Content-Type` declares: protobuf (the default) or JSON
/// (`docs/adr/otlp-json-decoding.md`).
enum RequestEncoding {
    Protobuf,
    Json,
}

/// Absent or empty `Content-Type` means protobuf, a compatibility promise: clients predating
/// OTLP/JSON sent none, and none of them meant JSON. Matched case-insensitively, since HTTP media
/// types are (`Content-Type: Application/JSON` is conformant). `Err` carries the `415` message.
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
    // The frame's compressed flag (`grpc::unframe`) drives decompression; this check only rejects
    // an undecodable encoding up front with a clear message.
    if let Some(enc) = req.headers().get("grpc-encoding") {
        // A content-coding name, so case-insensitive (RFC 9110 §8.4.1), as `Content-Encoding` is;
        // ADR `untrusted-input-bounds` makes that uniform across the HTTP listeners.
        let enc = enc.to_str().unwrap_or("").trim();
        if !enc.eq_ignore_ascii_case("identity") && !enc.eq_ignore_ascii_case("gzip") {
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
        // `4`, `DEADLINE_EXCEEDED`: the HTTP `408`'s gRPC twin, and the connection closes once
        // this response is out.
        Err(BodyReadError::Stalled(stall)) => {
            activity.request_close();
            return Ok(grpc_response(4, &format!("request body stalled for {stall:?}"), None));
        }
        Err(BodyReadError::Failed(err)) => {
            return Ok(grpc_response(8, &body_read_error_message(err.as_ref()), None))
        }
    };
    let Some((compressed, payload)) = grpc::unframe(&framed) else {
        return Ok(grpc_response(3, "malformed gRPC message frame", None));
    };
    // `Export` is unary, so its body is one message. Bytes after it are a sender's encoder bug,
    // answered rather than dropped unread (ADR `untrusted-input-bounds`).
    let leftover = framed.len() - 5 - payload.len();
    if leftover != 0 {
        return Ok(grpc_response(
            3,
            &format!(
                "the request body carries {leftover} bytes after its gRPC message; a unary call \
                 takes one message"
            ),
            None,
        ));
    }
    let payload = if compressed {
        match grpc::inflate_bounded(payload, MAX_REQUEST_BYTES) {
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
            crate::http::deliver_detached(&sink, batches).await;
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

/// Builds a gRPC response: `200`, the framed `payload` (empty when `None`) as one data frame, and
/// `grpc-status`/`grpc-message` as trailers. Never Trailers-Only (headers-only), so every outcome,
/// an immediate rejection included, takes one response-building path.
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

/// A body that yields one data frame, then one trailers frame, then ends: a unary gRPC response's
/// wire shape (a framed message, zero-length for an error, then the `grpc-status` trailer).
/// `http_body_util::Full` has no trailers, so this implements [`hyper::body::Body`] by hand
/// (`docs/adr/hand-rolled-grpc-over-hyper.md`).
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

/// Frames `payload` as one uncompressed unary gRPC message: `[compressed:u8][len:u32 BE][payload]`.
/// Duplicated from `logit_outputs::otlp::grpc_frame` rather than shared across crates.
fn grpc_frame(payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(5 + payload.len());
    buf.push(0u8);
    buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(payload);
    buf
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

/// Builds an `Export*ServiceResponse`'s bytes by hand (`logit_outputs::otlp::parse_partial_success`
/// says why the shape is identical across signals and neither side generates the types). Empty
/// when `rejected == 0` and `error_message` is empty: proto3 serializes an all-default message to
/// zero bytes, and `rejected` is always `0` today (this module's "`partial_success`").
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

/// The OTLP/JSON mirror of [`export_response`], with `rejected` always `0`. Proto3 JSON omits an
/// unset message field, so the all-default response is `{}`, **not** an empty body:
/// `opentelemetry-js`'s HTTP exporter parses the success body for `partialSuccess`, and
/// `JSON.parse("")` throws. A real reject count (`docs/known-gaps.md`) would render as
/// `rejectedSpans`/`rejectedLogRecords`/`rejectedDataPoints`: the JSON key differs per
/// [`Signal`], where the protobuf field shares one tag number across all three
/// `Export*ServiceResponse` messages.
fn export_response_json() -> Vec<u8> {
    b"{}".to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::mpsc;

    /// A request carrying this header panics in `handle_http`, so a test can unwind a connection
    /// task the way a handler bug would.
    pub(super) const PANIC_HEADER: &str = "x-logit-test-panic";

    async fn bound_input(transport: OtlpTransport) -> (String, OtlpInput) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        (addr.to_string(), OtlpInput::new(addr.to_string(), transport))
    }

    /// `Input::bind` makes the port live *before* `run`'s accept loop starts, and `local_addr`
    /// exposes the OS-assigned port, with no bind-drop-rebind (which `bound_input` above still
    /// uses).
    #[tokio::test]
    async fn bind_makes_the_port_live_before_run_and_local_addr_reports_it() {
        let mut input = OtlpInput::new("127.0.0.1:0", OtlpTransport::Http);
        assert_eq!(input.local_addr(), None, "no address before bind()");

        input.bind().await.expect("binding an ephemeral port should succeed");
        let addr = input.local_addr().expect("bind() should leave a real address behind");

        // Connects with `run` never spawned: the socket is live from `bind()` alone.
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

    /// [`fanout_into_channel`] with an explicit capacity. Capacity 1 with nothing draining parks a
    /// handler inside `Fanout::send`: the first batch is buffered, the second blocks.
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

    /// The span [`one_span_payload`] encodes, as an OTLP/JSON literal.
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

    /// A JSON request's success is `{}` with a JSON content type, never protobuf's empty body,
    /// which breaks `opentelemetry-js`'s response parsing.
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

    /// A protobuf request gets protobuf's response shape, not JSON's.
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

    /// An unsupported type is a `415` whose message names every accepted type.
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

    /// HTTP media types are case-insensitive: `Content-Type: Application/JSON` is JSON.
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

    /// `Content-Encoding` and `Content-Type` compose: a gzipped JSON body decodes.
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

    /// All three signals decode, not traces only.
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

    /// `Content-Encoding: identity` declares no compression and is accepted, not a `415`.
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

    /// Content-coding names are case-insensitive (RFC 9110 §8.4.1), so `GZIP`, `Gzip`, and
    /// `IDENTITY` are the codings `gzip` and `identity`, not a `415`.
    #[tokio::test]
    async fn a_capitalised_content_encoding_is_accepted() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, mut rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        for encoding in ["GZIP", "Gzip", "IDENTITY", "Identity"] {
            let body = if encoding.eq_ignore_ascii_case("gzip") {
                gzip(&one_span_payload())
            } else {
                one_span_payload()
            };
            let headers = format!(
                "Content-Type: application/x-protobuf\r\nContent-Encoding: {encoding}\r\n\
                 Connection: close\r\n"
            );
            let response = post_raw(&addr, "/v1/traces", &headers, &body).await;
            assert!(response.starts_with("HTTP/1.1 200"), "{encoding}: got {response}");
            let received = recv_batch(&mut rx).await;
            assert!(received.events[0].span.is_some(), "{encoding}: the span is delivered");
        }
    }

    /// A `Content-Encoding` header that is present but empty, or carries a non-ASCII byte, names
    /// no coding this input can trust, so it is a `415` like any unknown one, never identity.
    /// Only an absent header means identity.
    #[tokio::test]
    async fn an_empty_or_non_ascii_content_encoding_is_415() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        for encoding in ["", "gzip\u{e9}", "\u{e9}zstd"] {
            let headers = format!(
                "Content-Type: application/x-protobuf\r\nContent-Encoding: {encoding}\r\n\
                 Connection: close\r\n"
            );
            let response = post_raw(&addr, "/v1/traces", &headers, &one_span_payload()).await;
            assert!(response.starts_with("HTTP/1.1 415"), "{encoding:?}: got {response}");
        }
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

    /// A few KiB of gzipped zeros inflating past `MAX_REQUEST_BYTES` is caught by
    /// `grpc::inflate_bounded`'s decompressed bound, which `Limited`'s compressed bound (satisfied
    /// here) cannot.
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

        // Two single-resource requests concatenated into one two-`ResourceSpans` request
        // (`logit-proto`'s generated types are `pub(crate)`). Valid because `TracesData`'s only
        // field is `repeated ResourceSpans resource_spans = 1`, and concatenated encodings of a
        // message are a valid merge.
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

        // `OtlpOutput` always sends well-formed frames, so this drives the raw framing over a
        // real HTTP/2 client connection.
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

    /// One gRPC message frame: `[compressed:u8][len:u32 BE][payload]`.
    fn grpc_message(compressed: bool, payload: &[u8]) -> Vec<u8> {
        let mut framed = vec![u8::from(compressed)];
        framed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        framed.extend_from_slice(payload);
        framed
    }

    /// Sends one unary `Export` to `addr` over a fresh h2c connection and returns its trailers.
    async fn grpc_export(addr: &str, body: Vec<u8>, grpc_encoding: Option<&str>) -> HeaderMap {
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
            .handshake(TokioIo::new(stream))
            .await
            .unwrap();
        tokio::spawn(conn);
        let mut req = http::Request::builder()
            .method(Method::POST)
            .uri(Signal::Traces.grpc_method())
            .header("content-type", "application/grpc+proto")
            .header("te", "trailers");
        if let Some(encoding) = grpc_encoding {
            req = req.header("grpc-encoding", encoding);
        }
        let res = sender.send_request(req.body(Full::new(Bytes::from(body))).unwrap()).await;
        let collected = res.unwrap().into_body().collect().await.unwrap();
        collected.trailers().expect("should carry trailers").clone()
    }

    /// `grpc-encoding` names a content coding, and those are case-insensitive (RFC 9110 §8.4.1),
    /// so `GZIP` and `Identity` are decoded, not answered `UNIMPLEMENTED`.
    #[tokio::test]
    async fn a_capitalised_grpc_encoding_is_accepted() {
        let (addr, mut input) = bound_input(OtlpTransport::Grpc).await;
        let (sink, mut rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        for (encoding, body) in [
            ("GZIP", grpc_message(true, &gzip(&one_span_payload()))),
            ("Gzip", grpc_message(true, &gzip(&one_span_payload()))),
            ("IDENTITY", grpc_message(false, &one_span_payload())),
            ("Identity", grpc_message(false, &one_span_payload())),
        ] {
            let trailers = grpc_export(&addr, body, Some(encoding)).await;
            assert_eq!(trailers.get("grpc-status").unwrap(), "0", "{encoding}: {trailers:?}");
            let received = recv_batch(&mut rx).await;
            assert!(received.events[0].span.is_some(), "{encoding}: the span is delivered");
        }
    }

    /// `Export` is a unary method, so its request body is one message. A second frame after the
    /// first is an encoder bug on the sender, answered `INVALID_ARGUMENT` naming the leftover
    /// bytes, and neither message is delivered.
    #[tokio::test]
    async fn a_second_grpc_frame_in_one_body_is_invalid_argument() {
        let (addr, mut input) = bound_input(OtlpTransport::Grpc).await;
        let (sink, mut rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let first = grpc_message(false, &one_span_payload());
        let second = grpc_message(false, &one_span_payload());
        let leftover = second.len();
        let mut body = first;
        body.extend_from_slice(&second);

        let trailers = grpc_export(&addr, body, None).await;
        assert_eq!(trailers.get("grpc-status").unwrap(), "3", "{trailers:?}");
        let message = trailers.get("grpc-message").expect("a message").to_str().unwrap();
        assert!(
            message.contains(&format!("{leftover} bytes")),
            "the message names the leftover bytes: {message}"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(200), rx.recv()).await.is_err(),
            "no batch is delivered from a rejected body"
        );
    }

    // ---- TLS: server termination against a real `tokio-rustls` client. ----

    fn testdata_dir() -> std::path::PathBuf {
        // The repo root's `testdata/tls` (`testdata/tls/README.md`), two levels up.
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    fn test_tls_settings(client_ca_file: Option<&str>) -> TlsServerSettings {
        TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: client_ca_file.map(str::to_string),
        }
    }

    /// A `tokio-rustls` client trusting `testdata/tls/ca.pem`. `client_cert` is `(cert, key)` file
    /// names under `testdata/tls` for the mTLS tests; `None` presents no certificate.
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

    /// The TLS twin of `post_raw`: a real TLS handshake, then HTTP/1.1 over the encrypted stream.
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

        // Plain HTTP at a TLS-only listener: the server reads garbage TLS framing, sends an
        // alert, and closes, without taking the listener down. The client may see alert bytes or
        // nothing, never an HTTP response.
        let response = post_raw(
            &addr,
            "/v1/traces",
            "Content-Type: application/x-protobuf\r\nConnection: close\r\n",
            &[],
        )
        .await;
        assert!(!response.starts_with("HTTP/1.1"), "expected no valid HTTP response: {response:?}");

        // The listener still serves the next (TLS) connection.
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

    /// A client that connects and sends no ClientHello is closed server-side inside the budget,
    /// and the listener still serves TLS afterwards. The plaintext twin proves the permit returns.
    /// Modelled on `crate::tcp`'s
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

        // Raw TCP, no byte sent, held until the close is observed, so only the server's deadline
        // could have closed it. A 1s read budget against a 50ms handshake timeout.
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

        // The listener still serves real TLS traffic afterwards.
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

    /// Reads one byte, expecting the peer to have closed instead. This helper, `sum_of`, and
    /// `expect_still_open` are copies of `crate::tcp`'s test helpers of the same names.
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
    /// the point carrying `tag`.
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

    /// `sum_of` for a gauge: last-write-wins per `(name, tags)` until the next drain.
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

    /// The cap rejects rather than queues, before any TLS handshake, and counts the rejection.
    /// Modelled on `crate::tcp`'s
    /// `the_connection_cap_drops_a_connection_past_the_limit_and_counts_it`, whose accept loop
    /// this one copies.
    #[tokio::test]
    async fn a_connection_past_the_cap_is_dropped_and_counted() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("otlp_in", "otlp_in", "listener");
        let (addr, input) = bound_input(OtlpTransport::Http).await;
        let mut input = input.with_telemetry(telemetry).with_max_connections(1);
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The first connection holds the one permit. One byte, so it clears the first-byte peek
        // rather than being closed by the deadline.
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

    /// A TCP health check (connect, close, no byte) is not a `connection_error`: no warn and no
    /// count per probe.
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

        // Three probes, so power-of-two throttling can't hide a regression: `warn_throttled`
        // counts `logit.component.diagnostics` on every occurrence, reported or not.
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

    /// A silent plaintext connection (the default, no-`tls:` shape) is closed at the handshake
    /// timeout. Under `with_max_connections(1)` the follow-up request is served only if its permit
    /// came back.
    #[tokio::test]
    async fn a_silent_plaintext_connection_is_closed_after_the_handshake_timeout() {
        let (addr, input) = bound_input(OtlpTransport::Http).await;
        let mut input =
            input.with_max_connections(1).with_handshake_timeout(Duration::from_millis(50));
        let (sink, mut rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // No byte sent, held until the close is observed, so only the server's deadline could
        // have closed it.
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

    /// The first-byte deadline is not a request deadline: after the first byte, `hyper`'s read
    /// loop has no timer (this module's "Why not hyper's `http1().header_read_timeout(..)`"), so a
    /// head dribbled out over four times the budget still gets a 200.
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
        // The first byte immediately: all the peek waits for.
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

    /// The peek consumes nothing, so h2c prior-knowledge (the case an ordinary read would break
    /// first) still gets its full 24-byte preface to `ReadVersion`.
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

    /// The gauge counts permit holders: 1 while a connection is served, 0 once it ends.
    #[tokio::test]
    async fn the_connections_gauge_tracks_a_live_connection_and_returns_to_zero() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("otlp_in", "otlp_in", "listener");
        let (addr, input) = bound_input(OtlpTransport::Http).await;
        let mut input = input.with_telemetry(telemetry);
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // One byte, so the connection clears the peek and stays open.
        let mut open = tokio::net::TcpStream::connect(&addr).await.unwrap();
        open.write_all(b"P").await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(gauge_of(&registry.drain(0), "logit.input.connections"), Some(1.0));

        drop(open);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(gauge_of(&registry.drain(0), "logit.input.connections"), Some(0.0));
    }

    /// A handler that panics unwinds the h1 connection task, whose permit comes back on the
    /// unwind; the gauge has to come back with it, or it reads one live connection forever.
    #[tokio::test]
    async fn a_panicking_handler_still_returns_the_connections_gauge_to_zero() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("otlp_in", "otlp_in", "listener");
        let (addr, input) = bound_input(OtlpTransport::Http).await;
        let mut input = input.with_telemetry(telemetry).with_max_connections(1);
        let (sink, mut rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let headers = format!(
            "Content-Type: application/x-protobuf\r\n{PANIC_HEADER}: 1\r\nConnection: close\r\n"
        );
        let response = post_raw(&addr, "/v1/metrics", &headers, &metric_body()).await;
        assert_eq!(response, "", "a panicking handler writes no response");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(gauge_of(&registry.drain(0), "logit.input.connections"), Some(0.0));

        // The permit came back too: under `with_max_connections(1)` this is served only if so.
        let response = post_raw(
            &addr,
            "/v1/metrics",
            "Content-Type: application/x-protobuf\r\nConnection: close\r\n",
            &metric_body(),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
        recv_batch(&mut rx).await;
    }

    /// One `/v1/metrics` body carrying two `ResourceMetrics` (hosts `a` and `b`), which decodes to
    /// two batches. Concatenated encodings of a message are a valid merge, and `MetricsData`'s only
    /// field is the repeated `resource_metrics`.
    fn two_resource_metrics_payload() -> Vec<u8> {
        let mut body = Vec::new();
        for host in ["a", "b"] {
            let mut resource = logit_core::Resource::default();
            resource.attributes.insert("host", host);
            let batch = logit_core::EventBatch {
                resource: std::sync::Arc::new(resource),
                scope: None,
                events: metric_batch().events,
            };
            let mut encoder = logit_proto::otlp::OtlpEncoder::new();
            let payloads =
                logit_proto::SignalEncoder::encode_signals(&mut encoder, &batch).unwrap();
            body.extend_from_slice(&payloads[0].1);
        }
        body
    }

    /// The `host` resource attribute of each batch `rx` holds, waiting up to 300ms for each.
    async fn hosts_received(rx: &mut mpsc::Receiver<logit_pipeline::Delivered>) -> Vec<String> {
        let mut hosts = Vec::new();
        while let Ok(Some(delivered)) =
            tokio::time::timeout(Duration::from_millis(300), rx.recv()).await
        {
            let batch = logit_pipeline::unwrap_batch(delivered);
            let host = batch.resource.attributes.get("host").and_then(|v| v.as_str());
            hosts.push(host.unwrap_or("").to_string());
        }
        hosts
    }

    /// A client that gives up while its request waits on a full consumer (an exporter's own
    /// timeout under backpressure) cancels only the handler's wait, not the delivery: every
    /// consumer still gets every batch of the request. The client never saw a `200`, so its retry
    /// duplicates on every branch alike, the ordinary at-least-once outcome, where a split would
    /// duplicate on some branches and not others.
    #[tokio::test]
    async fn a_client_that_closes_while_its_batches_wait_still_delivers_them_to_every_consumer() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (tx_a, mut rx_a) = mpsc::channel(16);
        let (tx_b, mut rx_b) = mpsc::channel(1);
        let sink = Fanout::new(vec![tx_a, tx_b]);
        // Fills b's one slot, so the request's first send waits on b; a's copy is drained here.
        sink.send(metric_batch()).await;
        recv_batch(&mut rx_a).await;
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut client = tokio::net::TcpStream::connect(&addr).await.unwrap();
        write_request(&mut client, &addr, "/v1/metrics", &two_resource_metrics_payload()).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(client);
        tokio::time::sleep(Duration::from_millis(200)).await;

        let b = hosts_received(&mut rx_b).await;
        let a = hosts_received(&mut rx_a).await;
        assert_eq!(b, ["", "a", "b"], "b: the filler, then both of the request's batches");
        assert_eq!(a, ["a", "b"], "a: both of the request's batches");
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
    // Real durations (50-300ms), never `tokio::time::pause()`: these tests race a timer against
    // hyper's read loop, and paused time would skip past the reads that loop is in. "Closed
    // within" goes through `expect_closed`'s 2s ceiling against deadlines of at most 300ms;
    // "still open" asserts `timeout(50ms, read) == Err(Elapsed)`, which lag only makes truer.

    /// [`metric_batch`] encoded as one OTLP/protobuf `/v1/metrics` body.
    fn metric_body() -> Bytes {
        let mut encoder = logit_proto::otlp::OtlpEncoder::new();
        let payloads =
            logit_proto::SignalEncoder::encode_signals(&mut encoder, &metric_batch()).unwrap();
        payloads.into_iter().find(|(s, _)| *s == Signal::Metrics).unwrap().1
    }

    /// Asserts a connection is still open: this listener never speaks unprompted, so a blocked
    /// read means live, while a closed one returns `Ok(0)` (or `ECONNRESET`) at once. The inverse
    /// of [`expect_closed`]; scheduler lag only makes the read likelier to time out.
    async fn expect_still_open<S: tokio::io::AsyncRead + Unpin>(stream: &mut S, what: &str) {
        let mut buf = [0u8; 1];
        match tokio::time::timeout(Duration::from_millis(50), stream.read(&mut buf)).await {
            Err(_elapsed) => {}
            Ok(Ok(0)) => panic!("{what}: expected the connection to still be open, got a close"),
            Ok(Ok(n)) => panic!("{what}: expected no bytes, got {n}"),
            Ok(Err(err)) => panic!("{what}: expected the connection to still be open, got {err}"),
        }
    }

    /// [`post_raw`]'s keep-alive half: one complete HTTP/1.1 POST on an open stream with **no**
    /// `Connection: close`, so hyper waits for the next request rather than closing.
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

    /// Reads one response head (through the blank line) off a keep-alive connection, where
    /// `read_to_end` would block until the connection ends. Every response read this way is a
    /// protobuf success with an empty body, so the head is all there is.
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

    /// [`expect_closed`] for `protocol: grpc`, whose server sends `SETTINGS` before reading the
    /// client's preface, so the next byte is not the close. Reads to EOF instead.
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

    /// A body that yields `.0` once, then returns `Pending` with no registered waker, so nothing
    /// polls it again: a stalled upload, as [`collect_with_stall_bound`] sees it.
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

    /// A keep-alive connection that finished its export and went quiet gives its permit back:
    /// under `with_max_connections(1)` the follow-up request is served only if it did. The close
    /// is counted `logit.input.connections.closed{reason="idle"}` and `connection_error` never
    /// fires. `KA::Idle`, so `graceful_shutdown` closes it without spending the grace.
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

        // One complete export, keep-alive, so only the idle clock can end the connection.
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

    /// A connection that sent one head byte has completed nothing, so it is closed at the idle
    /// deadline from its own start. One byte leaves `auto`'s pre-sniff `ReadVersion` undecided
    /// (`P` begins `POST` or the h2 `PRI` preface). `graceful_shutdown` cancels it, and the first
    /// grace poll resolves at once to the `Err("Cancelled")` the driver discards, so the close
    /// does not wait out the grace. `handshake_timeout` (the grace) is 50ms regardless, to fit
    /// inside `expect_closed`'s 2s ceiling.
    #[tokio::test]
    async fn a_fresh_http_connection_that_sent_one_head_byte_is_closed_after_the_idle_timeout() {
        let (addr, input) = bound_input(OtlpTransport::Http).await;
        let mut input = input
            .with_idle_timeout(Some(Duration::from_millis(100)))
            .with_handshake_timeout(Duration::from_millis(50));
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // One byte clears the first-byte peek, so the close can only come from the idle clock.
        let mut dribbling = tokio::net::TcpStream::connect(&addr).await.unwrap();
        dribbling.write_all(b"P").await.unwrap();

        expect_closed(&mut dribbling, "a connection that sent one head byte and then stopped")
            .await;
    }

    /// An established, quiet h2 connection is GOAWAY'd and closed, observed as the client's
    /// connection future ending. `sender` is held throughout, so the client didn't initiate it.
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

        // Either outcome is the close: `Ok` on a clean GOAWAY-then-FIN, `Err` if the socket goes
        // first.
        let _closed = tokio::time::timeout(Duration::from_secs(2), client_conn)
            .await
            .expect("an idle gRPC connection should be closed within 2s")
            .expect("the client's connection task should not panic");

        // The client's future ends on the GOAWAY, written during the grace poll; the count lands
        // after that poll returns, a moment later.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            sum_of(&registry.drain(0), "logit.input.connections.closed", Some(("reason", "idle"))),
            Some(1.0),
            "an idle close is counted on the gRPC transport too"
        );
        drop(sender);
    }

    /// An h2 connection still `Handshaking` only gets `close_pending` from `graceful_shutdown`, so
    /// the grace (`handshake_timeout`, 50ms here) and the drop free the permit.
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

    /// **The reset rule.** A request parked in `Fanout::send` holds the idle clock off however
    /// long it parks: three idle timeouts don't close the connection, the drain delivers the
    /// response, and the connection serves another request. A clock running during the park
    /// would lose a fully received request.
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

        // The connection still serves another request.
        write_request(&mut client, &addr, "/v1/metrics", &body).await;
        let head = read_response_head(&mut client, "a third export").await;
        assert!(head.starts_with("HTTP/1.1 200"), "got: {head}");
        recv_batch(&mut rx).await;
    }

    /// A full head then a body that stops mid-upload gets `408` and the connection closes behind
    /// it. `read_to_end` asserts both: it returns only on close, and returns the response.
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

    /// The gRPC twin: a stalled body gets `grpc-status: 4` (`DEADLINE_EXCEEDED`) and the
    /// connection closes, observed as the client's connection future ending.
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

    /// A request that *starts* inside the grace (after `graceful_shutdown`, before the connection
    /// is gone) is served, not dropped: on h1 the handler is polled inside the connection future,
    /// so dropping it while parked in `Fanout::send` would lose a batch, the outcome
    /// `docs/adr/idle-connection-timeout.md` exists to prevent.
    ///
    /// **Two bytes, not one.** One byte leaves `auto`'s `ReadVersion` sniff undecided, which
    /// `graceful_shutdown` cancels outright
    /// (`a_fresh_http_connection_that_sent_one_head_byte_is_closed_after_the_idle_timeout`).
    /// `PO` commits to HTTP/1.1, giving an h1 connection stopped mid-head: `KA::Busy`, which
    /// `graceful_shutdown` leaves running and which can still pick a request up in the grace.
    ///
    /// The pre-filled capacity-1 channel parks the handler; its batch is a metric and the
    /// request's a span, so the two arrivals are distinct assertions.
    ///
    /// **Timings.** A 200ms idle timeout and a 100ms grace put the window at roughly 200-300ms
    /// after the first byte. The rest of the request lands at 260ms: 60ms *past* the idle
    /// deadline, so nothing was in flight when it fired, and 40ms *inside* the grace. The
    /// downstream stays blocked until 600ms, 300ms past the grace, where a drop-on-expiry close
    /// would have lost the batch.
    #[tokio::test]
    async fn a_request_that_starts_inside_the_grace_window_is_served_not_dropped() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("otlp_in", "otlp_in", "listener");
        let (addr, input) = bound_input(OtlpTransport::Http).await;
        let mut input = input
            .with_telemetry(telemetry)
            .with_idle_timeout(Some(Duration::from_millis(200)))
            // Doubles as the grace. Both margins are tens of milliseconds (see the timings above).
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

        // t+260ms: 60ms past the idle deadline and 40ms inside the grace, so a connection already
        // shutting down picks the rest of the request up.
        tokio::time::sleep(Duration::from_millis(260)).await;
        late.write_all(&head[2..]).await.unwrap();
        late.write_all(&body).await.unwrap();

        // t+600ms: 300ms past the window, where a drop-on-expiry close would have lost this batch.
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

        // It still closes afterwards: waiting the request out defers the close, not cancels it.
        expect_closed(&mut late, "a connection that served a request inside its grace").await;
        assert_eq!(
            sum_of(&registry.drain(0), "logit.input.connections.closed", Some(("reason", "idle"))),
            Some(1.0),
            "the deferred close is still counted once, as an idle close"
        );
    }

    /// With no `idle_timeout`, a quiet keep-alive connection (a long-interval exporter's pooled
    /// connection between exports) stays open.
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

        // Three times the idle timeout the other tests in this section configure.
        tokio::time::sleep(Duration::from_millis(300)).await;
        expect_still_open(&mut keep_alive, "a keep-alive connection with no idle_timeout").await;
    }
}
