//! `logit_in` -- the native `logit`-to-`logit` listener side
//! (`docs/design/wire-protocol.md`'s connection protocol, `docs/plans/native-transport.md`
//! workstream C). Accepts many TCP (optionally TLS) connections; each speaks a version/codec/
//! compression handshake (`Hello`/`HelloAck`, `logit_proto::native::control`), then a loop of
//! one native frame in, one `Fanout::send`, one `Ack` out.
//!
//! **Binding.** The listening socket is opened by [`Input::bind`], not lazily inside
//! [`Input::run_until_shutdown`] -- the bind pre-pass `otlp_in` (`crate::otlp`) and the shared TCP
//! driver (`crate::tcp`) already use (`docs/plans/operator-surface.md`, workstream B). The runtime
//! calls it for every input before a single node task is spawned, so an unavailable port fails
//! startup rather than surfacing once every sibling listener is already live, and
//! [`LogitInput::local_addr`] makes the OS-assigned port of a `:0` bind readable without a
//! bind-drop-rebind race. `run_until_shutdown` still calls `bind` itself when nobody did, so a
//! direct caller (this module's own tests) needs no extra step.
//!
//! **Ack point.** A connection's `Ack{seq}` is written only *after* `Fanout::send` returns, i.e.
//! after the batch is in every downstream inbox -- a stalled downstream delays the ack, which
//! stalls the sender's own `write_loop` on the other end. That *is* the backpressure this
//! listener relies on; there is no receive-side queue here the way a UDP listener has
//! (`crate::udp`) -- `docs/plans/native-transport.md`'s "Ack point" decision.
//!
//! **Shutdown.** Every spawned connection task holds its own [`Fanout`] clone
//! (`docs/adr/service-lifecycle-and-output-retry.md`'s cancel-by-drop shutdown depends on nothing
//! outliving the listener's own future) -- so each one races its "wait for the next frame" step
//! against a `shutdown` receiver cloned from [`Input::run_until_shutdown`]'s own, and closes
//! (sending `Reject{GOING_AWAY}` first) the moment shutdown fires *and* the connection is idle at
//! a frame boundary. An already-in-flight frame (past its header) is allowed to finish -- the
//! `select!` below only ever re-evaluates between frames, never mid-body-read.
//!
//! **Connection limit.** A non-blocking `try_acquire_owned` against the same 1024-connection cap
//! `otlp_in` ([`crate::otlp::MAX_CONCURRENT_CONNECTIONS`]) and `syslog_in`'s driver use: at
//! capacity, a connecting client gets a clean `Reject` and the connection closes immediately
//! rather than hanging with a handshake that never starts.
//! `logit`-to-`logit` peers are expected to retry/back off on their own, the same assumption the
//! connection protocol's ack-driven backpressure already leans on. **The reject goes out after
//! the TLS wrap, when TLS is configured, not onto the raw `TcpStream`** -- a TLS-configured
//! `logit_out` past the cap is waiting for a ServerHello, not framed bytes, so
//! [`reject_or_serve`] does the (now timeout-bounded, see "Pre-`Hello` timeout" below) TLS accept
//! first for *every* connection and only then decides reject-or-serve. The cost: a connection
//! rejected for being past the cap now spends one TLS handshake instead of one write -- bounded
//! per-connection by the same pre-`Hello` timeout, but not bounded in count, since by definition
//! there is no permit to hold while it happens. Judged acceptable: the alternative (closing with
//! no `Reject` at all when TLS is on) reintroduces exactly the opaque failure a clean `Reject` is
//! for.
//!
//! **Pre-`Hello` timeout.** [`LogitInput::handshake_timeout`] (a field, defaulted to
//! [`HANDSHAKE_TIMEOUT`] and set from config by
//! [`LogitInput::with_handshake_timeout`]) bounds each
//! of the two pre-`Hello` phases *independently*: the TLS accept itself (wrapped in
//! `tokio::time::timeout` in the accept loop) and, after it, the `Hello` read inside
//! [`handshake`], which starts a fresh timeout of the same length rather than inheriting a shared
//! deadline. So on the TLS path the worst case is *two* of these back to back -- 10s at the
//! default -- before a connection that has sent no `Hello` gives up its connection-limit permit.
//! That is deliberate: one knob applied per phase, rather than a shared deadline threaded through
//! the accept loop, and what this fixes is that the wait is bounded at all. Before this, only the
//! `Hello` read was bounded -- an unbounded TLS accept let a client that opened a connection and
//! sent nothing pin a connection-limit permit forever, which at 1024 connections could turn every
//! subsequent legitimate peer into an immediate `Reject`.
//!
//! **Idle timeout.** [`LogitInput::with_idle_timeout`] -- `logit_in`'s operator-facing
//! `idle_timeout:` config field -- is off unless set, and when set bounds how long an
//! already-handshaken connection may stay quiet before this listener closes it and hands its
//! permit back (`docs/adr/idle-connection-timeout.md`). Unlike the pre-`Hello` budget above it is
//! not a per-phase deadline but a rolling one, re-armed from the last thing this connection
//! actually did.
//!
//! *Measured from the last `Ack` written* (or from the handshake, on a connection that has never
//! sent a frame), never from the last frame *read* -- this protocol's own answer to what "idle"
//! means. A peer that has sent a frame and is waiting for its `Ack` is by definition **not** idle:
//! that ack is deliberately delayed by a slow downstream ("Ack point" above), so the party doing
//! the work in that gap is this listener, not the peer. Stamping the clock when the `Ack` is
//! written -- i.e. after `Fanout::send` has already returned -- is what makes "the peer went
//! quiet" and "we are still busy with what it last sent" two structurally different states rather
//! than two readings of the same missing byte, and it is why time blocked in `Fanout::send` can
//! never count against a peer.
//!
//! *A header that has started arriving is progress too.* The clock's absolute deadline bounds
//! only the wait for a frame's **first** byte; once one byte of the header has landed, the
//! remaining header bytes are read under the same per-`read` bound a body gets, so a frame whose
//! first byte arrives a moment before the deadline is read rather than rejected mid-header
//! ([`read_header`]'s [`IdleBounds`]).
//!
//! *A frame body gets its own bound*, per `read` rather than in total ([`read_frame_body`]'s
//! `stall` argument): a large frame arriving slowly but steadily is not idle either, while a peer
//! that sends a header, half a body, and then nothing is. Both cases end the same way.
//!
//! *And both say so on the wire.* An idle close writes `Reject{GOING_AWAY, "idle for <dur>"}`
//! before closing -- the same control message an ordinary shutdown sends ([`going_away`] writes
//! them both), so a `logit_out` peer needs no new case to handle it, and `logit_out`'s own
//! pooled-connection probe (`logit_outputs`' `poll_pending_close`) looks for exactly this before
//! reusing a connection rather than writing a batch into a socket the peer has already closed.
//! An idle close is policy, not a fault: [`serve_connection`] returns `Ok(())`, so the accept
//! loop's `connection_error` diagnostic never sees it, and it is counted
//! `logit.input.connections.closed{reason="idle"}` instead -- counted, not diagnosed.

use crate::Input;
use bytes::{Bytes, BytesMut};
use logit_core::{Diagnostics, Provenance, Telemetry};
use logit_pipeline::Fanout;
use logit_proto::frame::{self, Compression, FrameHeader};
use logit_proto::native::{self, control};
use logit_proto::CodecError;
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;

/// Default for [`LogitInput::handshake_timeout`] -- how long a connection has, per pre-`Hello`
/// phase, to finish its TLS accept (if configured) and send `Hello` before this listener gives up
/// on it -- generous enough for a loaded peer under TLS, tight enough that a connection opened and
/// then abandoned (a port scan, a misconfigured health check, or a TLS client that never sends its
/// ClientHello) doesn't pin a connection-limit permit forever.
///
/// The *default* only: `logit_in`'s `handshake_timeout:` config field overrides it through
/// [`LogitInput::with_handshake_timeout`]. `logit_config`'s own `default_handshake_timeout`
/// mirrors this number by hand (it cannot depend on this crate).
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// See this module's own doc comment's "Connection limit" section for why this listener rejects
/// outright rather than queuing, and for the one thing it does differently from `otlp_in` and
/// `syslog_in`'s driver, which reject at the same cap: the `Reject` goes out after the TLS wrap.
const MAX_CONCURRENT_CONNECTIONS: usize = 1024;

/// `crate::tls::TlsServerSettings`, re-exported here for symmetry with `crate::otlp`'s own path
/// (both listeners share the one definition in `crate::tls`).
pub use crate::tls::TlsServerSettings;

pub struct LogitInput {
    bind: String,
    diag: Diagnostics,
    telemetry: Telemetry,
    tls: Option<Arc<rustls::ServerConfig>>,
    max_frame_bytes: u32,
    max_connections: usize,
    /// Set by [`Input::bind`], taken back out by [`Input::run_until_shutdown`] -- the same
    /// bind pre-pass `otlp_in` (`crate::otlp`) and the shared TCP driver (`crate::tcp`) use
    /// (`docs/plans/operator-surface.md`, workstream B). Binding happens *before* `run` rather
    /// than inside it so a port that can't be opened fails startup with nothing else running yet,
    /// and so a caller can read the OS-assigned port off [`LogitInput::local_addr`] before
    /// anything is spawned. `None` again after a run, so a second run rebinds.
    listener: Option<TcpListener>,
    /// See this module's own doc comment's "Pre-`Hello` timeout" section.
    handshake_timeout: Duration,
    /// `None` -- the default -- means no idle timeout at all, the behaviour this listener had
    /// before the field existed. See [`Self::with_idle_timeout`] and this module's own doc
    /// comment's "Idle timeout" section.
    idle_timeout: Option<Duration>,
}

impl LogitInput {
    pub fn new(bind: impl Into<String>) -> Self {
        Self {
            bind: bind.into(),
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
            tls: None,
            max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
            max_connections: MAX_CONCURRENT_CONNECTIONS,
            listener: None,
            handshake_timeout: HANDSHAKE_TIMEOUT,
            idle_timeout: None,
        }
    }

    /// The address actually bound, once [`Input::bind`] has run -- lets a caller (a test, the
    /// runtime's own startup pass) learn the OS-assigned port without a bind-drop-rebind race.
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

    /// Turns on TLS termination for this listener (`tls:` in config) -- no ALPN, unlike
    /// `otlp_in`: this isn't an HTTP-shaped protocol, so there's nothing for a client to
    /// negotiate down to.
    pub fn with_tls(
        mut self,
        settings: &TlsServerSettings,
        base_dir: &Path,
    ) -> anyhow::Result<Self> {
        self.tls = Some(Arc::new(crate::tls::build_server_config(settings, base_dir, &[])?));
        Ok(self)
    }

    /// Caps the size (post- and pre-decompression alike) of a single frame this listener will
    /// accept, echoed to every connecting client in `HelloAck.max_frame_bytes`. Defaults to
    /// [`frame::MAX_SANE_UNCOMPRESSED_LEN`] (64 MiB) -- never above it, since that's also the
    /// hard ceiling `frame::read_frame_with_header` itself enforces.
    pub fn with_max_frame_bytes(mut self, max_frame_bytes: u32) -> Self {
        self.max_frame_bytes = max_frame_bytes.min(frame::MAX_SANE_UNCOMPRESSED_LEN);
        self
    }

    /// Test-only override of [`MAX_CONCURRENT_CONNECTIONS`] -- spinning up 1025 real TCP
    /// connections in a test to exercise the cap would be slow and flaky; this makes the cap
    /// itself small enough to reach with a handful of connections instead.
    #[cfg(test)]
    fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Overrides [`HANDSHAKE_TIMEOUT`] for both pre-`Hello` budgets (the TLS accept and the
    /// `Hello` read) -- what `logit_in`'s `handshake_timeout:` config field sets. The constant
    /// stays the default when this is never called; a test uses it to observe the budget actually
    /// firing (releasing a permit, timing out a silent TLS accept) without a multi-second sleep.
    /// Graph rule 45 rejects `0s` before it can reach here.
    pub fn with_handshake_timeout(mut self, handshake_timeout: Duration) -> Self {
        self.handshake_timeout = handshake_timeout;
        self
    }

    /// Bounds how long an already-handshaken connection may stay quiet -- `logit_in`'s
    /// `idle_timeout:` config field, and off (`None`) when never called. See this module's own
    /// doc comment's "Idle timeout" section for why the clock is measured from the last `Ack`
    /// rather than the last frame read, why a peer waiting on a delayed ack is never idle, and
    /// why an idle close is counted rather than diagnosed. Graph rule 53 rejects `Some(0s)`
    /// before it can reach here.
    ///
    /// Takes the `Option` rather than a bare `Duration`, so the "no idle timeout" case is one
    /// call from a config that omitted the field rather than a caller-side `if let` --
    /// `crate::tcp::TcpListener::with_idle_timeout`'s shape, so `logit-cli`'s `build_spec` passes
    /// what it has straight through on every listener arm alike.
    pub fn with_idle_timeout(mut self, idle_timeout: Option<Duration>) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }
}

#[async_trait::async_trait]
impl Input for LogitInput {
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
        // Mirrors `crate::udp::UdpListener::run`: a never-firing `watch` so `run` and
        // `run_until_shutdown` share one implementation rather than diverging.
        let (_tx, rx) = watch::channel(false);
        self.run_until_shutdown(sink, rx).await
    }

    async fn run_until_shutdown(
        &mut self,
        sink: Fanout,
        mut shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        self.bind().await?;
        let listener = self.listener.take().expect("bind() leaves a listener behind");
        let connection_limit = Arc::new(tokio::sync::Semaphore::new(self.max_connections));
        let tls_acceptor = self.tls.clone().map(TlsAcceptor::from);
        let live_connections = Arc::new(AtomicI64::new(0));
        let max_frame_bytes = self.max_frame_bytes;
        let handshake_timeout = self.handshake_timeout;
        let idle_timeout = self.idle_timeout;

        loop {
            let (stream, _peer) = tokio::select! {
                accepted = listener.accept() => accepted?,
                _ = shutdown.wait_for(|&due| due) => return Ok(()),
            };

            // Non-blocking: `None` here means "past the cap," handled inside `reject_or_serve`
            // rather than in this loop -- see this module's own "Connection limit" doc section
            // for why the reject has to happen *after* the (now timeout-bounded) TLS wrap below,
            // not onto this raw `stream`.
            let permit = connection_limit.clone().try_acquire_owned().ok();
            if permit.is_none() {
                self.telemetry.count(
                    "logit.input.connections.rejected",
                    1.0,
                    &[("reason", "limit")],
                );
            }

            let sink = sink.clone();
            let mut diag = self.diag.clone();
            let telemetry = self.telemetry.clone();
            let tls_acceptor = tls_acceptor.clone();
            let conn_shutdown = shutdown.clone();
            let live_connections = live_connections.clone();

            tokio::spawn(async move {
                let result = match tls_acceptor {
                    Some(acceptor) => {
                        match tokio::time::timeout(handshake_timeout, acceptor.accept(stream)).await
                        {
                            Ok(Ok(tls_stream)) => {
                                reject_or_serve(
                                    tls_stream,
                                    permit,
                                    sink,
                                    telemetry.clone(),
                                    max_frame_bytes,
                                    handshake_timeout,
                                    idle_timeout,
                                    conn_shutdown,
                                    live_connections,
                                )
                                .await
                            }
                            Ok(Err(err)) => Err(anyhow::anyhow!("TLS handshake failed: {err}")),
                            Err(_elapsed) => Err(anyhow::anyhow!(
                                "TLS handshake did not complete within {handshake_timeout:?}"
                            )),
                        }
                    }
                    None => {
                        reject_or_serve(
                            stream,
                            permit,
                            sink,
                            telemetry.clone(),
                            max_frame_bytes,
                            handshake_timeout,
                            idle_timeout,
                            conn_shutdown,
                            live_connections,
                        )
                        .await
                    }
                };
                // One connection's I/O error (a client disconnecting mid-frame, a malformed
                // preamble, a TLS accept that failed or timed out) shouldn't be fatal to the
                // listener or its sibling connections -- only `TcpListener::accept` failing in
                // the accept loop above is. This is also where the old, dedicated
                // `connection_limit_reject_failed` diagnostic used to live -- subsumed here since
                // a reject that fails to write is now just another connection-level I/O error.
                if let Err(err) = result {
                    diag.warn_throttled("connection_error", err);
                }
            });
        }
    }
}

/// A connection that arrived past this listener's cap gets the same clean, decodable
/// `Reject{INTERNAL}` whether or not TLS is configured -- which means the reject has to be
/// written *after* the TLS wrap, not onto a raw `TcpStream` where a TLS client is waiting for a
/// ServerHello. `permit` is `None` exactly when the connection arrived past the cap (this
/// module's own "Connection limit" doc section); the `logit.input.connections` gauge only ever
/// counts a connection that actually holds one, incremented/decremented around the `Some` arm
/// here rather than in the accept loop.
#[allow(clippy::too_many_arguments)] // one small helper is clearer here than a params struct for 9 mostly-unrelated threaded-through values
async fn reject_or_serve<S: AsyncRead + AsyncWrite + Unpin + Send>(
    mut stream: S,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    sink: Fanout,
    telemetry: Telemetry,
    max_frame_bytes: u32,
    handshake_timeout: Duration,
    idle_timeout: Option<Duration>,
    shutdown: watch::Receiver<bool>,
    live_connections: Arc<AtomicI64>,
) -> anyhow::Result<()> {
    let Some(_permit) = permit else {
        let reject = control::Reject {
            code: control::REJECT_INTERNAL,
            message: "connection limit reached, retry later".to_string(),
        };
        return write_control(&mut stream, &reject).await;
    };

    // Published from the read-modify-write's own return value, not a separate `load`:
    // `Telemetry::gauge` is last-write-wins per key, so two tasks that interleave an add and a
    // load would leave the stale one as the published value until the next transition.
    // `crate::tcp`/`crate::otlp`'s own accept loops publish this gauge the same way.
    let live = live_connections.fetch_add(1, Ordering::Relaxed) + 1;
    telemetry.gauge("logit.input.connections", live as f64, &[]);

    let result = serve_connection(
        stream,
        sink,
        telemetry.clone(),
        max_frame_bytes,
        handshake_timeout,
        idle_timeout,
        shutdown,
    )
    .await;

    let live = live_connections.fetch_sub(1, Ordering::Relaxed) - 1;
    telemetry.gauge("logit.input.connections", live as f64, &[]);

    result
}

/// What the handshake negotiated for one connection.
struct Negotiated {
    compression: Compression,
    /// `CODEC_NATIVE_V2` if the client offered it (so provenance crosses the wire), otherwise
    /// `CODEC_NATIVE_V1`. `handshake` picks the best codec this listener and the client both
    /// speak; every data frame on this connection must be framed under exactly this one
    /// (`serve_connection` checks it per frame).
    codec: u8,
}

fn compression_tag(compression: Compression) -> &'static str {
    match compression {
        Compression::None => "none",
        Compression::Lz4 => "lz4",
        Compression::Zstd => "zstd",
    }
}

/// Renders a connection's negotiated codec byte for the `logit.proto.frames` metric's `codec`
/// tag -- mirrors `logit_outputs::logit`'s own `codec_tag`.
fn codec_tag(codec: u8) -> &'static str {
    match codec {
        native::CODEC_NATIVE_V2 => "native_v2",
        _ => "native_v1",
    }
}

/// Serves one already-accepted (and, if this listener has TLS on, already-handshaken) connection
/// to completion -- generic over the IO type so the plaintext (`TcpStream`) and TLS
/// (`tokio_rustls::server::TlsStream<TcpStream>`) cases share every line below
/// `run_until_shutdown`'s own `tls_acceptor` branch, exactly as `crate::otlp::serve_connection`
/// does.
async fn serve_connection<S: AsyncRead + AsyncWrite + Unpin + Send>(
    mut stream: S,
    sink: Fanout,
    telemetry: Telemetry,
    max_frame_bytes: u32,
    handshake_timeout: Duration,
    idle_timeout: Option<Duration>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let negotiated = match handshake(&mut stream, max_frame_bytes, handshake_timeout).await {
        Ok(n) => n,
        Err(err) => {
            telemetry.count("logit.proto.errors", 1.0, &[("reason", "handshake")]);
            return Err(err);
        }
    };
    let compression = compression_tag(negotiated.compression);

    // The idle clock's origin: the handshake completing is this connection's first piece of
    // progress, and from here on only an `Ack` write advances it -- this module's own "Idle
    // timeout" doc section for why an `Ack` and not a frame read.
    let mut last_progress = tokio::time::Instant::now();

    let mut seq: u64 = 0;
    loop {
        // Only this first step -- reading the next frame's header -- races against `shutdown`.
        // An idle connection (nothing to read) is one blocked right here; once a header has
        // arrived, the rest of this iteration (body read, decode, forward, ack) runs to
        // completion uninterrupted, which is what "let an in-flight frame finish" means.
        // Checked explicitly, not folded into the `select!` below, so a shutdown that already
        // happened (this connection idle when it fired) is caught before even trying to read --
        // `changed()` alone only fires on a *transition* this receiver hasn't yet observed, which
        // wouldn't catch "already true when this loop iteration started." The `Ref` temporary
        // from `borrow()` is dropped at the end of this statement, well before any `.await`.
        if *shutdown.borrow() {
            going_away(&mut stream, "listener shutting down").await;
            return Ok(());
        }
        // `shutdown.changed()` here, not `wait_for` -- `wait_for`'s `Ref` guard makes the
        // `select!`'s combined future `!Send` the moment any arm (like this one) awaits
        // something afterward, which `tokio::spawn`ing this connection's task requires. `changed`
        // has no such guard and, given the explicit check just above, is equivalent here:
        // `shutdown` only ever flips false -> true once, and this receiver hasn't observed that
        // flip yet (the check above would have caught it if it had already happened).
        //
        // The header read is also where the idle clock is consulted -- an idle connection is one
        // parked in exactly this read, and nothing else in the loop below waits on the peer for
        // an unbounded time. [`IdleBounds`] is the deadline pair, if this connection has one: an
        // absolute one for the header's first byte, a per-`read` one for the rest of it.
        let header_buf = tokio::select! {
            result = read_header(&mut stream, IdleBounds::new(last_progress, idle_timeout)) => {
                match result {
                    Ok(header_buf) => header_buf,
                    Err(HeaderReadError::Idle(idle)) => {
                        return close_idle(&mut stream, &telemetry, idle).await
                    }
                    Err(err) => return Err(err.into_inner()),
                }
            }
            _ = shutdown.changed() => {
                going_away(&mut stream, "listener shutting down").await;
                return Ok(());
            }
        };
        let Some(header_buf) = header_buf else {
            // Clean close at a frame boundary -- the ordinary way a connection ends.
            return Ok(());
        };

        let (header, mut payload) =
            match read_frame_body(&mut stream, header_buf, max_frame_bytes, idle_timeout).await {
                Ok(v) => v,
                // A body that stopped arriving part-way through is the same condition as a gap
                // between frames, and ends the same way -- policy, not a fault, so no
                // `logit.proto.errors` here (this module's "Idle timeout" doc section).
                Err(FrameReadError::Stalled(idle)) => {
                    return close_idle(&mut stream, &telemetry, idle).await
                }
                Err(FrameReadError::TooLarge(err)) => {
                    telemetry.count("logit.proto.errors", 1.0, &[("reason", "too_large")]);
                    return Err(err);
                }
                Err(FrameReadError::Truncated(err)) => {
                    telemetry.count("logit.proto.errors", 1.0, &[("reason", "truncated")]);
                    return Err(err);
                }
                Err(FrameReadError::Crc(err)) => {
                    telemetry.count("logit.proto.errors", 1.0, &[("reason", "crc")]);
                    return Err(err);
                }
                Err(FrameReadError::Malformed(err)) => {
                    telemetry.count("logit.proto.errors", 1.0, &[("reason", "magic")]);
                    return Err(err);
                }
            };

        if header.flags & frame::FLAG_CONTROL != 0 {
            // A control frame mid-stream (after the handshake) is unexpected -- the only control
            // message a well-behaved client sends after `Hello` is none at all; close.
            anyhow::bail!("received an unexpected control frame after the handshake");
        }
        if header.codec != negotiated.codec {
            telemetry.count("logit.proto.errors", 1.0, &[("reason", "codec")]);
            anyhow::bail!(
                "frame codec byte {}, expected the negotiated codec ({})",
                header.codec,
                negotiated.codec
            );
        }

        let (batch, provenance) = if negotiated.codec == native::CODEC_NATIVE_V2 {
            native::decode_batch_v2(&mut payload).map_err(|err| {
                telemetry.count("logit.proto.errors", 1.0, &[("reason", "magic")]);
                anyhow::Error::new(err).context("decoding a native v2 batch")
            })?
        } else {
            let batch = native::decode_batch(&mut payload).map_err(|err| {
                telemetry.count("logit.proto.errors", 1.0, &[("reason", "magic")]);
                anyhow::Error::new(err).context("decoding a native batch")
            })?;
            (batch, Provenance::default())
        };

        telemetry.count(
            "logit.proto.frames",
            1.0,
            &[
                ("direction", "in"),
                ("codec", codec_tag(negotiated.codec)),
                ("compression", compression),
            ],
        );
        telemetry.count(
            "logit.proto.frame.bytes",
            header.compressed_len as f64,
            &[("direction", "in")],
        );

        // The ack point: written only after the batch is in every downstream inbox
        // (`Fanout::send`/`send_relayed` return once every consumer has accepted it) -- see this
        // module's own doc comment. `send_relayed` (not `send`) backfills only whatever provenance
        // the wire didn't carry -- a v1 peer, or a v2 peer that genuinely had none -- and passes a
        // v2 peer's own `origin`/`previous` through untouched, the property `logit_out ->
        // logit_in` exists for (`docs/adr/batch-provenance-on-delivered.md`).
        sink.send_relayed(batch, provenance).await;

        seq += 1;
        write_control(&mut stream, &control::Ack { seq }).await?;
        // The idle clock restarts here and nowhere else: stamped *after* the `Fanout::send`
        // above and after the ack has been written, so the whole time this connection spent
        // waiting on a full downstream is charged to this listener rather than to the peer that
        // was waiting for the ack (this module's "Idle timeout" doc section).
        last_progress = tokio::time::Instant::now();
    }
}

/// Writes the `Reject{GOING_AWAY, why}` this listener sends before closing a connection it has
/// decided to end: an ordinary shutdown ("listener shutting down") and an idle close ("idle for
/// <dur>") are the same signal to a peer, which is the point -- `logit_out` already treats
/// `REJECT_GOING_AWAY` as transient and reconnects, so neither case needs a new code or a new
/// case on the client side.
///
/// The write's own result is deliberately discarded: this connection is going away regardless,
/// and a peer that has already vanished is not a fault worth reporting. Every caller returns
/// `Ok(())` immediately afterward.
async fn going_away<S: AsyncWrite + Unpin>(stream: &mut S, why: &str) {
    let reject = control::Reject { code: control::REJECT_GOING_AWAY, message: why.to_string() };
    let _ = write_control(stream, &reject).await;
}

/// Ends a connection that has been quiet for longer than its `idle_timeout`, or whose frame body
/// stopped arriving for that long: tell the peer, count it, and return `Ok(())`.
///
/// `Ok(())`, never `Err`, because an idle close is policy rather than a fault -- an `Err` here
/// would reach the accept loop's `connection_error` diagnostic and report a connection this
/// listener closed *on purpose* as a problem. The counter is the signal instead
/// (`logit.input.connections.closed{reason="idle"}`), and the connection-cap permit comes back
/// the ordinary way, when the task ends. See this module's own "Idle timeout" doc section.
async fn close_idle<S: AsyncWrite + Unpin>(
    stream: &mut S,
    telemetry: &Telemetry,
    idle: Duration,
) -> anyhow::Result<()> {
    going_away(stream, &format!("idle for {idle:?}")).await;
    telemetry.count("logit.input.connections.closed", 1.0, &[("reason", "idle")]);
    Ok(())
}

/// Reads and negotiates the connection handshake: expects `Hello` within `handshake_timeout`
/// (a *fresh* timeout of that length, not the remainder of one shared with the accept loop's TLS
/// accept -- which is bounded by the same knob independently, so the two together are the
/// worst-case pre-`Hello` wait; see this module's own "Pre-`Hello` timeout" doc section), replies
/// `HelloAck` (codec/compression = the intersection with what this listener offers) or `Reject`
/// and returns an error either way a client can't proceed.
async fn handshake<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    max_frame_bytes: u32,
    handshake_timeout: Duration,
) -> anyhow::Result<Negotiated> {
    let read = tokio::time::timeout(handshake_timeout, async {
        let Some(header_buf) =
            read_header(stream, None).await.map_err(HeaderReadError::into_inner)?
        else {
            anyhow::bail!("connection closed before sending Hello");
        };
        // `None`: the whole `Hello` read, header and body alike, is already inside
        // `handshake_timeout`'s own `timeout` below, so a per-`read` stall bound here would be a
        // second, redundant clock on the same phase.
        read_frame_body(stream, header_buf, max_frame_bytes, None)
            .await
            .map_err(FrameReadError::into_inner)
    });
    let (header, mut payload) = match read.await {
        Ok(Ok(pair)) => pair,
        Ok(Err(err)) => return Err(err),
        Err(_elapsed) => anyhow::bail!("timed out waiting for Hello"),
    };
    if header.flags & frame::FLAG_CONTROL == 0 {
        anyhow::bail!("expected a control frame (Hello) first, got a data frame");
    }
    let hello = control::Hello::decode(&mut payload)
        .map_err(|e| anyhow::anyhow!("expected Hello, got an unparseable control message: {e}"))?;

    if hello.version != control::PROTOCOL_VERSION {
        let reject = control::Reject {
            code: control::REJECT_VERSION_MISMATCH,
            message: format!(
                "this listener speaks connection-protocol version {}, client offered {}",
                control::PROTOCOL_VERSION,
                hello.version
            ),
        };
        let _ = write_control(stream, &reject).await;
        anyhow::bail!(
            "version mismatch: listener {}, client {}",
            control::PROTOCOL_VERSION,
            hello.version
        );
    }

    // Prefer v2 (carries provenance) whenever the client offers it; fall back to v1 otherwise.
    // This is what lets an unmodified old `logit_out` (offering only `[1]`) keep talking to this
    // listener unchanged, and a new `logit_out` (offering `[2, 1]`) get provenance without either
    // side needing to know about the other's version ahead of time
    // (`docs/adr/batch-provenance-on-delivered.md`).
    let codec = if hello.codecs.contains(&native::CODEC_NATIVE_V2) {
        native::CODEC_NATIVE_V2
    } else if hello.codecs.contains(&native::CODEC_NATIVE_V1) {
        native::CODEC_NATIVE_V1
    } else {
        let reject = control::Reject {
            code: control::REJECT_NO_COMMON_CODEC,
            message: "this listener speaks native v1 or v2".to_string(),
        };
        let _ = write_control(stream, &reject).await;
        anyhow::bail!("client offered no codec this listener speaks: {:?}", hello.codecs);
    };

    // Compression always has a safe fallback (`None`), so there is no reject path for it --
    // unlike codec, where no shared choice means the connection genuinely cannot proceed.
    let compression = if hello.compressions.contains(&(Compression::Lz4 as u8)) {
        Compression::Lz4
    } else {
        Compression::None
    };

    let ack = control::HelloAck {
        version: control::PROTOCOL_VERSION,
        codec,
        compression: compression as u8,
        max_frame_bytes,
        window: 1,
    };
    write_control(stream, &ack).await?;
    Ok(Negotiated { compression, codec })
}

/// The two idle bounds a header read gets on a connection that has an `idle_timeout` -- `None`
/// everywhere it doesn't, which is unbounded, the behaviour [`read_header`] had before either
/// bound existed.
///
/// Two, and not one, because "the peer has sent nothing" and "the peer is part-way through
/// sending a header" are different states: see [`read_header`]'s own doc comment.
struct IdleBounds {
    /// Absolute -- `last_progress + idle_timeout` -- and so measured from the last `Ack` rather
    /// than restarted by each read. The deadline for the header's *first* byte only.
    first_byte: tokio::time::Instant,
    /// The per-`read` budget for every byte *after* the first: the configured `idle_timeout`
    /// itself, exactly as [`read_frame_body`]'s `stall` bounds each read of a body.
    stall: Duration,
}

impl IdleBounds {
    /// `None` when this connection has no `idle_timeout` at all. `checked_add` because
    /// `last_progress + idle` can overflow for an absurd (but legal) value, and rule 53 caps
    /// nothing above `0s`.
    fn new(last_progress: tokio::time::Instant, idle_timeout: Option<Duration>) -> Option<Self> {
        let idle = idle_timeout?;
        Some(Self {
            first_byte: last_progress.checked_add(idle).unwrap_or_else(crate::tcp::far_future),
            stall: idle,
        })
    }
}

/// Why [`read_header`] produced no header -- [`FrameReadError`]'s shape one step earlier in the
/// frame, and for the same reason: an idle bound elapsing is not an error at all, it is the idle
/// close, so a caller has to be able to tell it apart without re-parsing a message.
enum HeaderReadError {
    Io(anyhow::Error),
    /// A bound elapsed -- either the wait for the header's first byte reached the connection's
    /// absolute idle deadline, or one `read` after that byte made no progress for the whole
    /// per-`read` budget. Carries the configured `idle_timeout` so `close_idle` can name it.
    /// Only reachable when [`IdleBounds`] were passed.
    Idle(Duration),
}

impl HeaderReadError {
    fn into_inner(self) -> anyhow::Error {
        match self {
            HeaderReadError::Io(err) => err,
            // Only for the callers that flatten both variants into one error (the handshake and
            // the test helpers), none of which pass any bounds in the first place.
            HeaderReadError::Idle(idle) => {
                anyhow::anyhow!("a frame header stopped arriving for {idle:?}")
            }
        }
    }
}

/// Reads exactly [`frame::HEADER_LEN`] bytes off `stream`, distinguishing "the peer closed
/// cleanly with nothing pending" (`Ok(None)`) from "the peer closed mid-header" (a real error) --
/// the distinction `read_exact` alone can't make, since it only reports success or failure, never
/// how many bytes it managed before EOF. This is also exactly the step `serve_connection`'s
/// per-frame `select!` races against `shutdown`: an idle connection is one blocked here, in the
/// very first read of a frame boundary.
///
/// **Why `bounds` is two deadlines and not one.** [`IdleBounds::first_byte`] is absolute, so the
/// wait for a frame that never starts is measured from the last `Ack` (this module's "Idle
/// timeout" doc section) rather than restarted by each read. But once the first byte has arrived
/// the header *is* arriving, which is progress, so every remaining read is bounded by
/// [`IdleBounds::stall`] instead -- one `idle_timeout` budget per `read`, the same rule
/// [`read_frame_body`] applies to a body one step later.
///
/// A single absolute deadline around the whole header would instead reject a frame whose first
/// byte landed a moment before it -- discarding those bytes and answering
/// `Reject{GOING_AWAY}` to a peer that had already started writing, which costs a `logit_out`
/// exactly the `Fault::Ambiguous` batch the idle timeout's client-side probe exists to avoid.
/// Nothing is lost when the first-byte deadline itself fires, since by definition no byte of this
/// header has been read.
async fn read_header<S: AsyncRead + Unpin>(
    stream: &mut S,
    bounds: Option<IdleBounds>,
) -> Result<Option<[u8; frame::HEADER_LEN]>, HeaderReadError> {
    let mut buf = [0u8; frame::HEADER_LEN];
    let mut filled = 0usize;
    loop {
        let read = match &bounds {
            None => stream.read(&mut buf[filled..]).await,
            Some(bounds) if filled == 0 => {
                match tokio::time::timeout_at(bounds.first_byte, stream.read(&mut buf[filled..]))
                    .await
                {
                    Ok(read) => read,
                    Err(_elapsed) => return Err(HeaderReadError::Idle(bounds.stall)),
                }
            }
            Some(bounds) => {
                match tokio::time::timeout(bounds.stall, stream.read(&mut buf[filled..])).await {
                    Ok(read) => read,
                    Err(_elapsed) => return Err(HeaderReadError::Idle(bounds.stall)),
                }
            }
        };
        let n = read.map_err(|err| HeaderReadError::Io(anyhow::Error::new(err)))?;
        if n == 0 {
            if filled == 0 {
                return Ok(None);
            }
            return Err(HeaderReadError::Io(anyhow::anyhow!(
                "connection closed mid-header ({filled}/{} bytes)",
                frame::HEADER_LEN
            )));
        }
        filled += n;
        if filled == frame::HEADER_LEN {
            return Ok(Some(buf));
        }
    }
}

/// Why [`read_frame_body`] failed -- distinguished so callers can attribute the right
/// `logit.proto.errors{reason}` tag without re-parsing the underlying [`anyhow::Error`]'s text.
enum FrameReadError {
    TooLarge(anyhow::Error),
    Truncated(anyhow::Error),
    Crc(anyhow::Error),
    Malformed(anyhow::Error),
    /// A single `read` of the body made no progress for the whole `stall` bound -- carrying that
    /// duration rather than a rendered error, because this one is not an error at all: the caller
    /// turns it into an idle close (`close_idle`), which names the duration in its `Reject` and
    /// counts rather than diagnoses. Only reachable when a `stall` bound was passed, i.e. never
    /// during the handshake.
    Stalled(Duration),
}

impl FrameReadError {
    fn into_inner(self) -> anyhow::Error {
        match self {
            FrameReadError::TooLarge(e)
            | FrameReadError::Truncated(e)
            | FrameReadError::Crc(e)
            | FrameReadError::Malformed(e) => e,
            // Only for the callers that flatten every variant into one error (the handshake and
            // the test helpers), none of which pass a `stall` bound in the first place.
            FrameReadError::Stalled(idle) => {
                anyhow::anyhow!("a frame body stopped arriving for {idle:?}")
            }
        }
    }
}

/// Given an already-read frame header (`header_buf`, exactly [`frame::HEADER_LEN`] bytes), parses
/// it, bound-checks its declared lengths against `max_frame_bytes` *before* reading the body off
/// `stream`, then reads the body and hands the whole thing to
/// [`frame::read_frame_with_header`] (reusing its CRC/decompression/length-consistency checks
/// wholesale rather than reimplementing them). Shared by the handshake (no `shutdown` to race --
/// it's already time-bounded by [`handshake`]'s own `handshake_timeout`) and `serve_connection`'s
/// main loop (which only races `shutdown` against the header read that precedes this).
///
/// `stall` bounds each individual `read` of the body, **not** the body as a whole: a large frame
/// arriving slowly but steadily keeps making progress and is never idle, while a peer that sends
/// a header and then half a body and stops is (this module's own "Idle timeout" doc section). It
/// is the connection's `idle_timeout`, and `None` -- the handshake, and the test helpers -- means
/// the untimed `read_exact` this always did. A stalled read returns [`FrameReadError::Stalled`],
/// which `serve_connection` turns into an idle close rather than an error; `Ok(0)` still means a
/// peer that closed mid-body, which is [`FrameReadError::Truncated`] as before.
async fn read_frame_body<S: AsyncRead + Unpin>(
    stream: &mut S,
    header_buf: [u8; frame::HEADER_LEN],
    max_frame_bytes: u32,
    stall: Option<Duration>,
) -> Result<(FrameHeader, Bytes), FrameReadError> {
    let mut header_bytes = Bytes::copy_from_slice(&header_buf);
    let header = FrameHeader::read(&mut header_bytes).map_err(|e| {
        FrameReadError::Malformed(anyhow::Error::new(e).context("reading a frame header"))
    })?;

    let bound = max_frame_bytes.min(frame::MAX_SANE_UNCOMPRESSED_LEN);
    if header.uncompressed_len > bound || header.compressed_len > bound {
        return Err(FrameReadError::TooLarge(anyhow::anyhow!(
            "frame declares {}/{} (uncompressed/compressed) bytes, over the {bound}-byte bound",
            header.uncompressed_len,
            header.compressed_len
        )));
    }

    // A fill loop rather than `read_exact`, so each individual `read` can carry the `stall`
    // bound. The two outcomes `read_exact` folds into one `io::Error` stay distinguishable here:
    // `Ok(0)` is a peer that closed mid-body (`Truncated`, as before) and an elapsed `stall` is a
    // peer that simply stopped (`Stalled`, an idle close).
    let mut body = vec![0u8; header.compressed_len as usize];
    let mut filled = 0usize;
    while filled < body.len() {
        let read = match stall {
            Some(stall) => {
                match tokio::time::timeout(stall, stream.read(&mut body[filled..])).await {
                    Ok(read) => read,
                    Err(_elapsed) => return Err(FrameReadError::Stalled(stall)),
                }
            }
            None => stream.read(&mut body[filled..]).await,
        };
        let n = read.map_err(|e| {
            FrameReadError::Truncated(anyhow::Error::new(e).context("reading a frame body"))
        })?;
        if n == 0 {
            return Err(FrameReadError::Truncated(anyhow::anyhow!(
                "connection closed mid-body ({filled}/{} bytes)",
                body.len()
            )));
        }
        filled += n;
    }

    let mut full = BytesMut::with_capacity(frame::HEADER_LEN + body.len());
    full.extend_from_slice(&header_buf);
    full.extend_from_slice(&body);
    let mut full = full.freeze();
    match frame::read_frame_with_header(&mut full) {
        Ok((header, payload)) => Ok((header, payload)),
        Err(CodecError::Malformed(msg)) if msg.contains("crc32c") => {
            Err(FrameReadError::Crc(anyhow::anyhow!("crc32c mismatch -- frame is corrupt")))
        }
        Err(err) => {
            Err(FrameReadError::Malformed(anyhow::Error::new(err).context("reading a frame")))
        }
    }
}

/// Writes one control message, framed with [`frame::FLAG_CONTROL`] set -- `codec`/`compression`
/// are meaningless on a control frame (`logit_proto::native::control`'s own doc comment), so `0`/
/// `Compression::None` are used unconditionally.
async fn write_control<S: AsyncWrite + Unpin>(
    stream: &mut S,
    msg: &impl ControlEncode,
) -> anyhow::Result<()> {
    let framed =
        frame::write_frame_with_flags(0, Compression::None, frame::FLAG_CONTROL, &msg.encode())?;
    stream.write_all(&framed).await?;
    Ok(())
}

/// A tiny local trait so [`write_control`] can take any of the four control message types
/// directly (`Hello`, `HelloAck`, `Ack`, `Reject`) without callers wrapping each one in
/// `control::ControlMessage` first.
trait ControlEncode {
    fn encode(&self) -> Bytes;
}
impl ControlEncode for control::Hello {
    fn encode(&self) -> Bytes {
        control::Hello::encode(self)
    }
}
impl ControlEncode for control::HelloAck {
    fn encode(&self) -> Bytes {
        control::HelloAck::encode(self)
    }
}
impl ControlEncode for control::Ack {
    fn encode(&self) -> Bytes {
        control::Ack::encode(self)
    }
}
impl ControlEncode for control::Reject {
    fn encode(&self) -> Bytes {
        control::Reject::encode(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{
        AttrMap, Event, EventBatch, LogRecord, MetricKind, Registry, Resource, Severity, Value,
    };
    use logit_proto::native::NativeEncoder;
    use logit_proto::Encoder;
    use rustls_pki_types::pem::PemObject;
    use rustls_pki_types::CertificateDer;
    use std::sync::Arc;
    use tokio::net::TcpStream;
    use tokio::sync::mpsc;

    // ---- fixtures and harness -----------------------------------------------------------

    /// Binds an ephemeral port through [`Input::bind`] and hands back the OS-assigned address
    /// alongside the already-bound input -- the same shape `crate::tcp`'s own `bound_listener`
    /// uses, replacing the bind-drop-rebind probe socket this file needed before `logit_in` had a
    /// bind pre-pass. Because the socket is live on return, every test below can connect as soon
    /// as it has spawned `run`, with no readiness sleep in between.
    async fn bound_input() -> (String, LogitInput) {
        let mut input = LogitInput::new("127.0.0.1:0");
        input.bind().await.expect("binding an ephemeral port should succeed");
        let addr = input.local_addr().expect("bind() leaves a real address behind").to_string();
        (addr, input)
    }

    fn fanout_into_channel(capacity: usize) -> (Fanout, mpsc::Receiver<logit_pipeline::Delivered>) {
        let (tx, rx) = mpsc::channel(capacity);
        (Fanout::new(vec![tx]), rx)
    }

    /// Like [`fanout_into_channel`], but with a component id attached -- needed for any test that
    /// checks what `send_relayed` backfills into `origin`/`previous`
    /// (`docs/adr/batch-provenance-on-delivered.md`).
    fn fanout_into_channel_with_component(
        component: &str,
        capacity: usize,
    ) -> (Fanout, mpsc::Receiver<logit_pipeline::Delivered>) {
        let (tx, rx) = mpsc::channel(capacity);
        (Fanout::new(vec![tx]).with_component(component), rx)
    }

    async fn recv_delivered(
        rx: &mut mpsc::Receiver<logit_pipeline::Delivered>,
    ) -> logit_pipeline::Delivered {
        tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("should receive within 5s")
            .expect("channel should still be open")
    }

    async fn recv_batch(rx: &mut mpsc::Receiver<logit_pipeline::Delivered>) -> EventBatch {
        let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("should receive within 5s")
            .expect("channel should still be open");
        logit_pipeline::unwrap_batch(delivered)
    }

    fn sample_batch() -> EventBatch {
        let mut attrs = AttrMap::new();
        attrs.insert("host", "logit-in-test");
        let event = Event::log(
            1,
            attrs,
            LogRecord {
                message: Value::str("hello"),
                severity: Some(Severity::Info),
                body_format: logit_core::BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events: vec![event] }
    }

    // ---- client-side handshake/frame helpers ---------------------------------------------

    async fn connect(addr: &str) -> TcpStream {
        TcpStream::connect(addr).await.unwrap()
    }

    async fn write_msg(stream: &mut TcpStream, msg: &impl ControlEncode) {
        write_control(stream, msg).await.unwrap();
    }

    /// Reads one whole frame off `stream` with no bound (test client trusts the server) --
    /// returns the header (so a test can check `flags`) and the decoded payload bytes.
    async fn read_frame_raw(stream: &mut TcpStream) -> (FrameHeader, Bytes) {
        let header_buf = read_header(stream, None)
            .await
            .map_err(HeaderReadError::into_inner)
            .unwrap()
            .expect("expected a frame, got a clean close");
        read_frame_body(stream, header_buf, frame::MAX_SANE_UNCOMPRESSED_LEN, None)
            .await
            .map_err(FrameReadError::into_inner)
            .unwrap()
    }

    async fn client_hello(stream: &mut TcpStream, codecs: Vec<u8>, compressions: Vec<u8>) {
        let hello = control::Hello {
            version: control::PROTOCOL_VERSION,
            codecs,
            compressions,
            max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
            window: 1,
        };
        write_msg(stream, &hello).await;
    }

    async fn read_control_response(stream: &mut TcpStream) -> control::ControlMessage {
        let (header, mut payload) = read_frame_raw(stream).await;
        assert_eq!(
            header.flags & frame::FLAG_CONTROL,
            frame::FLAG_CONTROL,
            "expected a control frame"
        );
        control::ControlMessage::decode(&mut payload).unwrap()
    }

    async fn send_data_frame(stream: &mut TcpStream, batch: &EventBatch, compression: Compression) {
        let mut encoder = NativeEncoder::new(compression);
        let framed = encoder.encode(batch).unwrap();
        stream.write_all(&framed).await.unwrap();
    }

    /// Like [`send_data_frame`], but under `CODEC_NATIVE_V2` with a provenance trailer -- what a
    /// v2-capable `logit_out` actually sends.
    async fn send_data_frame_v2(
        stream: &mut TcpStream,
        batch: &EventBatch,
        provenance: Provenance,
        compression: Compression,
    ) {
        let payload = native::encode_batch_v2(batch, provenance);
        let framed = frame::write_frame(native::CODEC_NATIVE_V2, compression, &payload).unwrap();
        stream.write_all(&framed).await.unwrap();
    }

    async fn read_ack(stream: &mut TcpStream) -> control::Ack {
        match read_control_response(stream).await {
            control::ControlMessage::Ack(ack) => ack,
            other => panic!("expected Ack, got {other:?}"),
        }
    }

    fn drained_counter(registry: &Registry, metric: &str, tag: (&str, &str)) -> Option<f64> {
        registry.drain(0).into_iter().find_map(|e| {
            if e.attributes.get(tag.0).and_then(|v| v.as_str()) != Some(tag.1) {
                return None;
            }
            e.metrics.iter().find_map(|m| {
                if logit_core::interner::resolve(m.name) != metric {
                    return None;
                }
                match m.kind {
                    MetricKind::Sum(sum) => Some(sum.value),
                    _ => None,
                }
            })
        })
    }

    // ---- binding ----------------------------------------------------------------------------

    /// The bind pre-pass (this module's "Binding" doc section,
    /// `docs/plans/operator-surface.md` workstream B): the socket is listening, and its
    /// OS-assigned address readable, before `run`'s accept loop has been spawned at all -- which
    /// is what lets `logit run` fail startup on a taken port and lets a test connect with no
    /// readiness sleep.
    #[tokio::test]
    async fn bind_makes_the_port_live_before_run_and_local_addr_reports_it() {
        let mut input = LogitInput::new("127.0.0.1:0");
        assert_eq!(input.local_addr(), None, "no address before bind()");

        input.bind().await.expect("binding an ephemeral port should succeed");
        let addr = input.local_addr().expect("bind() should leave a real address behind");

        // Nothing is running yet -- this connection sits in the accept backlog, which is exactly
        // what makes the pre-pass worth having: no startup window where the port refuses.
        let _early = connect(&addr.to_string()).await;
    }

    /// A second `bind()` is a no-op, per [`Input::bind`]'s idempotency contract -- the runtime
    /// binds every input before spawning it and `run_until_shutdown` binds again for callers
    /// outside the runtime, so the two must not fight over the socket.
    #[tokio::test]
    async fn a_second_bind_is_a_no_op() {
        let mut input = LogitInput::new("127.0.0.1:0");
        input.bind().await.expect("first bind should succeed");
        let addr = input.local_addr().expect("bind() should leave a real address behind");
        input.bind().await.expect("second bind should be a harmless no-op");
        assert_eq!(input.local_addr(), Some(addr), "the address must not change");
    }

    /// The failure the pre-pass exists to surface early: a port someone else already holds is an
    /// `Err` out of `bind`, which `run_with_telemetry`'s startup phase turns into a startup
    /// failure with no node task spawned.
    #[tokio::test]
    async fn binding_a_port_already_held_is_an_error() {
        let held = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = held.local_addr().unwrap().to_string();

        let mut input = LogitInput::new(addr);
        let err = input.bind().await.expect_err("the port is still held by `held`");
        assert!(input.local_addr().is_none(), "a failed bind leaves no listener behind");
        assert_eq!(
            err.downcast_ref::<std::io::Error>().map(|e| e.kind()),
            Some(std::io::ErrorKind::AddrInUse),
            "expected AddrInUse, got {err}"
        );
    }

    // ---- provenance -------------------------------------------------------------------------

    /// A v2 client's own `origin`/`previous` cross the wire and come out the other side
    /// untouched -- the property `logit_out -> logit_in` exists for
    /// (`docs/adr/batch-provenance-on-delivered.md`).
    #[tokio::test]
    async fn a_v2_clients_provenance_is_relayed_untouched() {
        let (addr, input) = bound_input().await;
        let (sink, mut rx) = fanout_into_channel_with_component("logit_in_test", 16);
        let mut input = input;
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_NATIVE_V2], vec![0]).await;
        read_control_response(&mut client).await;

        let sent = Provenance {
            origin: Some(logit_core::interner::intern("remote_listener")),
            previous: Some(logit_core::interner::intern("remote_enrich")),
        };
        send_data_frame_v2(&mut client, &sample_batch(), sent, Compression::None).await;
        read_ack(&mut client).await;

        let delivered = recv_delivered(&mut rx).await;
        let provenance = delivered.provenance();
        assert_eq!(provenance.origin_str(), Some("remote_listener"));
        assert_eq!(provenance.previous_str(), Some("remote_enrich"));
    }

    /// A v2 client whose trailer carries no provenance at all (a v2 peer that genuinely had none)
    /// gets this listener's own id backfilled into both fields, rather than being relayed as
    /// `nil`/`nil` for no operator-visible reason.
    #[tokio::test]
    async fn a_v2_client_with_no_provenance_gets_this_listener_backfilled() {
        let (addr, input) = bound_input().await;
        let (sink, mut rx) = fanout_into_channel_with_component("logit_in_test", 16);
        let mut input = input;
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_NATIVE_V2], vec![0]).await;
        read_control_response(&mut client).await;

        send_data_frame_v2(&mut client, &sample_batch(), Provenance::default(), Compression::None)
            .await;
        read_ack(&mut client).await;

        let delivered = recv_delivered(&mut rx).await;
        let provenance = delivered.provenance();
        assert_eq!(provenance.origin_str(), Some("logit_in_test"));
        assert_eq!(provenance.previous_str(), Some("logit_in_test"));
    }

    /// A v1 client's batch carries no provenance on the wire at all -- this listener backfills
    /// its own id into both fields, exactly like the empty-v2-trailer case above.
    #[tokio::test]
    async fn a_v1_clients_batch_gets_this_listeners_own_provenance() {
        let (addr, input) = bound_input().await;
        let (sink, mut rx) = fanout_into_channel_with_component("logit_in_test", 16);
        let mut input = input;
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_NATIVE_V1], vec![0]).await;
        read_control_response(&mut client).await;

        send_data_frame(&mut client, &sample_batch(), Compression::None).await;
        read_ack(&mut client).await;

        let delivered = recv_delivered(&mut rx).await;
        let provenance = delivered.provenance();
        assert_eq!(provenance.origin_str(), Some("logit_in_test"));
        assert_eq!(provenance.previous_str(), Some("logit_in_test"));
    }

    // ---- handshake ------------------------------------------------------------------------

    /// A client offering both codecs negotiates v2, this listener's preferred choice -- see
    /// `handshake`'s own doc comment on why v2 is tried first.
    #[tokio::test]
    async fn a_client_offering_both_codecs_negotiates_v2() {
        let (addr, input) = bound_input().await;
        let (sink, _rx) = fanout_into_channel(16);
        let mut input = input;
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_NATIVE_V2, native::CODEC_NATIVE_V1], vec![0])
            .await;
        match read_control_response(&mut client).await {
            control::ControlMessage::HelloAck(ack) => {
                assert_eq!(ack.codec, native::CODEC_NATIVE_V2);
            }
            other => panic!("expected HelloAck, got {other:?}"),
        }
    }

    /// An unmodified old `logit_out`, offering only `[1]`, still negotiates and talks
    /// successfully -- the whole point of preferring v2 without requiring it.
    #[tokio::test]
    async fn a_client_offering_only_v1_still_negotiates_and_talks() {
        let (addr, input) = bound_input().await;
        let (sink, mut rx) = fanout_into_channel(16);
        let mut input = input;
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_NATIVE_V1], vec![0]).await;
        match read_control_response(&mut client).await {
            control::ControlMessage::HelloAck(ack) => {
                assert_eq!(ack.codec, native::CODEC_NATIVE_V1)
            }
            other => panic!("expected HelloAck, got {other:?}"),
        }

        send_data_frame(&mut client, &sample_batch(), Compression::None).await;
        read_ack(&mut client).await;
        let batch = recv_batch(&mut rx).await;
        assert_eq!(batch.events.len(), 1);
    }

    #[tokio::test]
    async fn handshake_happy_path_returns_the_negotiated_compression() {
        let (addr, input) = bound_input().await;
        let (sink, _rx) = fanout_into_channel(16);
        let mut input = input;
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_NATIVE_V1], vec![0, Compression::Lz4 as u8])
            .await;
        match read_control_response(&mut client).await {
            control::ControlMessage::HelloAck(ack) => {
                assert_eq!(ack.codec, native::CODEC_NATIVE_V1);
                assert_eq!(ack.compression, Compression::Lz4 as u8);
                assert_eq!(ack.version, control::PROTOCOL_VERSION);
            }
            other => panic!("expected HelloAck, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_client_offering_no_shared_codec_gets_reject_no_common_codec() {
        let (addr, input) = bound_input().await;
        let (sink, _rx) = fanout_into_channel(16);
        let mut input = input;
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![99], vec![0]).await;
        match read_control_response(&mut client).await {
            control::ControlMessage::Reject(reject) => {
                assert_eq!(reject.code, control::REJECT_NO_COMMON_CODEC);
            }
            other => panic!("expected Reject, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_wrong_version_hello_gets_reject_version_mismatch() {
        let (addr, input) = bound_input().await;
        let (sink, _rx) = fanout_into_channel(16);
        let mut input = input;
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        let hello = control::Hello {
            version: control::PROTOCOL_VERSION + 1,
            codecs: vec![native::CODEC_NATIVE_V1],
            compressions: vec![0],
            max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
            window: 1,
        };
        write_msg(&mut client, &hello).await;
        match read_control_response(&mut client).await {
            control::ControlMessage::Reject(reject) => {
                assert_eq!(reject.code, control::REJECT_VERSION_MISMATCH);
            }
            other => panic!("expected Reject, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_data_frame_before_hello_closes_the_connection() {
        let (addr, input) = bound_input().await;
        let (sink, _rx) = fanout_into_channel(16);
        let mut input = input;
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        send_data_frame(&mut client, &sample_batch(), Compression::None).await;

        // The listener closes without replying -- the client's next read should see EOF (0
        // bytes), not a HelloAck/Reject.
        let mut buf = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf))
            .await
            .expect("should observe a close within 2s")
            .unwrap();
        assert_eq!(n, 0, "expected the connection to close with no reply");
    }

    #[tokio::test]
    async fn a_frame_larger_than_max_frame_bytes_is_rejected_on_the_header_alone() {
        let (addr, input) = bound_input().await;
        let input = input.with_max_frame_bytes(64);
        let (sink, _rx) = fanout_into_channel(16);
        let mut input = input;
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_NATIVE_V1], vec![0]).await;
        let _ = read_control_response(&mut client).await;

        // Write only a header declaring a huge frame, never the body -- if the listener read the
        // header alone to decide, it closes promptly; if it tried to read the (never-sent) body
        // first, this would hang until the test's own timeout.
        let framed =
            frame::write_frame(native::CODEC_NATIVE_V1, Compression::None, &vec![0u8; 10_000])
                .unwrap();
        client.write_all(&framed[..frame::HEADER_LEN]).await.unwrap();

        let mut buf = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf))
            .await
            .expect("should observe a close within 2s, not hang waiting for a body")
            .unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn an_ack_is_written_only_after_the_downstream_inbox_accepted_the_batch() {
        let (addr, input) = bound_input().await;
        // Capacity 1: the first batch fills the channel; the second's `Fanout::send` blocks
        // until the test drains it.
        let (sink, mut rx) = fanout_into_channel(1);
        let mut input = input;
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_NATIVE_V1], vec![0]).await;
        let _ = read_control_response(&mut client).await;

        send_data_frame(&mut client, &sample_batch(), Compression::None).await;
        let ack1 = read_ack(&mut client).await;
        assert_eq!(ack1.seq, 1);

        send_data_frame(&mut client, &sample_batch(), Compression::None).await;
        // The second ack must not arrive while the channel is still full.
        let mut buf = [0u8; 1];
        let premature =
            tokio::time::timeout(Duration::from_millis(150), client.read(&mut buf)).await;
        assert!(premature.is_err(), "ack for the second batch arrived before the inbox drained");

        recv_batch(&mut rx).await; // drains the first batch, freeing capacity
        let ack2 = read_ack(&mut client).await;
        assert_eq!(ack2.seq, 2);
    }

    #[tokio::test]
    async fn a_crc_corrupt_frame_closes_the_connection_and_increments_the_crc_error_counter() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("logit_in", "logit_in", "listener");
        let (addr, input) = bound_input().await;
        let input = input.with_telemetry(telemetry);
        let (sink, _rx) = fanout_into_channel(16);
        let mut input = input;
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_NATIVE_V1], vec![0]).await;
        let _ = read_control_response(&mut client).await;

        let mut framed = BytesMut::from(
            &frame::write_frame(
                native::CODEC_NATIVE_V1,
                Compression::None,
                b"not a valid batch, but any bytes will do for a crc check",
            )
            .unwrap()[..],
        );
        let last = framed.len() - 1;
        framed[last] ^= 0xFF; // flip a payload byte without touching the header's crc field
        client.write_all(&framed).await.unwrap();

        let mut buf = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf))
            .await
            .expect("should observe a close within 2s")
            .unwrap();
        assert_eq!(n, 0);

        // Give the server task a moment to record the counter after closing the socket.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(drained_counter(&registry, "logit.proto.errors", ("reason", "crc")), Some(1.0));
    }

    #[tokio::test]
    async fn the_graph_closes_after_shutdown_with_an_idle_client_still_connected() {
        let (addr, mut input) = bound_input().await;
        let (sink, mut rx) = fanout_into_channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move { input.run_until_shutdown(sink, shutdown_rx).await });

        // Connect and handshake, then go idle -- never send a data frame.
        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_NATIVE_V1], vec![0]).await;
        let _ = read_control_response(&mut client).await;

        shutdown_tx.send(true).unwrap();

        // Every `Fanout` clone (the accept loop's own, and this one connection's) must be
        // dropped for `rx` to observe every sender gone.
        let closed = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
        assert!(closed.expect("should close within 2s").is_none(), "expected the fanout to close");

        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn the_connection_limit_rejects_a_connection_past_the_cap() {
        let (addr, input) = bound_input().await;
        let input = input.with_max_connections(1);
        let (sink, _rx) = fanout_into_channel(16);
        let mut input = input;
        tokio::spawn(async move { input.run(sink).await });

        // First connection: fills the one available slot, stays open and idle (never
        // handshakes -- holding the permit is all that matters here).
        let _first = connect(&addr).await;
        // Not a readiness wait (`bound_input` already bound the port) -- this gives the accept
        // loop time to actually take the permit for that connection before the next one arrives.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Second connection: over the cap -- should receive a Reject and close.
        let mut second = connect(&addr).await;
        match read_control_response(&mut second).await {
            control::ControlMessage::Reject(reject) => {
                assert_eq!(reject.code, control::REJECT_INTERNAL);
            }
            other => panic!("expected Reject, got {other:?}"),
        }
        let mut buf = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_secs(2), second.read(&mut buf))
            .await
            .expect("should observe a close within 2s")
            .unwrap();
        assert_eq!(n, 0);
    }

    // ---- TLS: pre-`Hello` timeout (F3) and cap-reject-over-TLS (F5) --------------------------

    fn testdata_dir() -> std::path::PathBuf {
        // `logit-inputs` lives at `crates/logit-inputs`; the fixtures live at the repo root's
        // `testdata/tls` (`testdata/tls/README.md`) -- two levels up from `CARGO_MANIFEST_DIR`,
        // the same path `crate::otlp`'s own TLS tests use.
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    fn test_tls_settings() -> TlsServerSettings {
        TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: None,
        }
    }

    /// A `tokio-rustls` client trusting `testdata/tls/ca.pem`, presenting no client certificate --
    /// mirrors `crate::otlp`'s own test-module `tls_connector` (no mTLS case needed here).
    async fn tls_connector() -> tokio_rustls::TlsConnector {
        let dir = testdata_dir();
        let mut roots = rustls::RootCertStore::empty();
        let ca: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(dir.join("ca.pem"))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        roots.add_parsable_certificates(ca);
        let cfg = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        tokio_rustls::TlsConnector::from(std::sync::Arc::new(cfg))
    }

    /// [`read_control_response`]'s generic twin -- that one is pinned to a plaintext `TcpStream`
    /// so plaintext tests read naturally; the TLS tests below need the same read logic over a
    /// `tokio_rustls::client::TlsStream`, which `read_header`/`read_frame_body` already support
    /// (both generic over `AsyncRead + Unpin`).
    async fn read_control_response_over<S: AsyncRead + Unpin>(
        stream: &mut S,
    ) -> control::ControlMessage {
        let header_buf = read_header(stream, None)
            .await
            .map_err(HeaderReadError::into_inner)
            .unwrap()
            .expect("expected a frame, got a clean close");
        let (header, mut payload) =
            read_frame_body(stream, header_buf, frame::MAX_SANE_UNCOMPRESSED_LEN, None)
                .await
                .map_err(FrameReadError::into_inner)
                .unwrap();
        assert_eq!(
            header.flags & frame::FLAG_CONTROL,
            frame::FLAG_CONTROL,
            "expected a control frame"
        );
        control::ControlMessage::decode(&mut payload).unwrap()
    }

    #[tokio::test]
    async fn a_tls_client_that_sends_nothing_releases_its_permit_after_the_handshake_timeout() {
        let (addr, input) = bound_input().await;
        let input = input
            .with_tls(&test_tls_settings(), &testdata_dir())
            .unwrap()
            .with_max_connections(1)
            .with_handshake_timeout(Duration::from_millis(200));
        let (sink, _rx) = fanout_into_channel(16);
        let mut input = input;
        tokio::spawn(async move { input.run(sink).await });

        // First connection: raw TCP, sends nothing (not even a TLS ClientHello) -- takes the
        // listener's one permit, then the TLS accept step it's stuck in must time out and
        // release it. Held past that -- not dropped -- so nothing but the timeout could free the
        // permit.
        let _silent = connect(&addr).await;

        // Comfortably longer than the 200ms handshake_timeout, short enough to keep the test
        // fast.
        tokio::time::sleep(Duration::from_millis(400)).await;

        // Second connection: a real TLS client. If the first connection's permit was never
        // released, this would get `Reject{INTERNAL}` (or hang against the accept loop's own
        // cap); a `HelloAck` proves the permit came back.
        let connector = tls_connector().await;
        let stream = connect(&addr).await;
        let server_name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
        let mut tls_stream =
            tokio::time::timeout(Duration::from_secs(2), connector.connect(server_name, stream))
                .await
                .expect("TLS handshake should complete once the permit is free")
                .unwrap();

        let hello = control::Hello {
            version: control::PROTOCOL_VERSION,
            codecs: vec![native::CODEC_NATIVE_V1],
            compressions: vec![0],
            max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
            window: 1,
        };
        write_control(&mut tls_stream, &hello).await.unwrap();
        match read_control_response_over(&mut tls_stream).await {
            control::ControlMessage::HelloAck(ack) => {
                assert_eq!(ack.codec, native::CODEC_NATIVE_V1);
            }
            other => panic!("expected HelloAck, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_tls_listener_at_its_connection_cap_rejects_over_tls_not_in_the_clear() {
        let (addr, input) = bound_input().await;
        let input =
            input.with_tls(&test_tls_settings(), &testdata_dir()).unwrap().with_max_connections(1);
        let (sink, _rx) = fanout_into_channel(16);
        let mut input = input;
        tokio::spawn(async move { input.run(sink).await });

        // First connection: raw TCP, holds the one permit for the (default, 5s) handshake
        // timeout -- plenty of time for the rest of this test.
        let _first = connect(&addr).await;
        // Not a readiness wait (`bound_input` already bound the port) -- this gives the accept
        // loop time to actually take the permit for that connection before the next one arrives.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Second connection: past the cap, but completes a *real* TLS handshake first -- the
        // point of this test is that the Reject arrives decodable over that TLS stream, not as
        // framed bytes where a client mid-handshake would otherwise see garbage.
        let connector = tls_connector().await;
        let stream = connect(&addr).await;
        let server_name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
        let mut tls_stream = tokio::time::timeout(
            Duration::from_secs(2),
            connector.connect(server_name, stream),
        )
        .await
        .expect("the TLS handshake itself must succeed even though the connection is over the cap")
        .unwrap();

        match read_control_response_over(&mut tls_stream).await {
            control::ControlMessage::Reject(reject) => {
                assert_eq!(reject.code, control::REJECT_INTERNAL);
            }
            other => panic!("expected Reject, got {other:?}"),
        }
    }

    // ---- idle timeout -------------------------------------------------------------------------
    //
    // Real durations (50-200ms), never `tokio::time::pause()`: these tests are about a timer
    // racing a socket read, and paused time would advance straight past the read the listener is
    // actually sitting in. The "closed" assertions read a real `Reject` frame under a 1-2s
    // timeout against deadlines of at most 200ms; the "still open" ones assert
    // `timeout(50ms, read) == Err(Elapsed)`, which scheduler lag can only make *more* true.

    /// Reads the control frame an idle close sends and asserts it is the documented
    /// `Reject{GOING_AWAY, "idle for <dur>"}` -- the signal a `logit_out` peer already knows how
    /// to treat as transient, and what `logit_out`'s pooled-connection probe looks for. Bounded
    /// generously against deadlines of at most 200ms.
    async fn expect_reject_going_away_for_idleness(stream: &mut TcpStream, what: &str) {
        let response = tokio::time::timeout(Duration::from_secs(2), read_control_response(stream))
            .await
            .unwrap_or_else(|_| panic!("{what}: expected an idle close within 2s"));
        match response {
            control::ControlMessage::Reject(reject) => {
                assert_eq!(reject.code, control::REJECT_GOING_AWAY, "{what}");
                assert!(
                    reject.message.contains("idle for"),
                    "{what}: the peer should be told why: {}",
                    reject.message
                );
            }
            other => panic!("{what}: expected Reject{{GOING_AWAY}}, got {other:?}"),
        }
    }

    /// Asserts nothing is readable on `stream` for 50ms -- this listener writes only in response
    /// to something (a `HelloAck`, an `Ack`, a `Reject`), so on a connection that has been given
    /// nothing to respond to, silence means "still open" and any byte at all would be the
    /// `Reject` of a close. Lag-proof in the direction that matters: a slow scheduler makes the
    /// read *more* likely to time out, never less.
    async fn expect_still_open(stream: &mut TcpStream, what: &str) {
        let mut buf = [0u8; 1];
        match tokio::time::timeout(Duration::from_millis(50), stream.read(&mut buf)).await {
            Err(_elapsed) => {}
            Ok(Ok(0)) => panic!("{what}: expected the connection to still be open, got a close"),
            Ok(Ok(_)) => panic!("{what}: expected no bytes, got a frame"),
            Ok(Err(err)) => panic!("{what}: expected the connection to still be open, got {err}"),
        }
    }

    /// The whole point of `idle_timeout:` on this listener: a handshaken connection that then
    /// goes quiet gives up its connection-cap permit instead of holding it forever. Proven under
    /// `with_max_connections(1)`, so the second connection can only handshake at all if the first
    /// one's permit genuinely came back -- and the quiet client is held (not dropped) throughout,
    /// so nothing but the idle clock could have freed it.
    ///
    /// Also the pin for the two things that make this "policy, not a fault": the peer is told
    /// with a `Reject{GOING_AWAY}` before the socket goes away, and the close is counted
    /// `logit.input.connections.closed{reason="idle"}` while the listener's `connection_error`
    /// diagnostic never fires (`serve_connection` returning `Ok(())`).
    #[tokio::test]
    async fn an_idle_handshaken_connection_gets_reject_going_away_and_releases_its_permit() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("logit_in", "logit_in", "listener");
        let (addr, input) = bound_input().await;
        let diag = Diagnostics::new("logit_in");
        let listener_diag = diag.clone();
        let mut input = input
            .with_telemetry(telemetry)
            .with_diagnostics(diag)
            .with_max_connections(1)
            .with_idle_timeout(Some(Duration::from_millis(100)));
        let (sink, _rx) = fanout_into_channel(16);
        tokio::spawn(async move { input.run(sink).await });

        // Handshake, so the pre-`Hello` budget is behind us and only the idle clock can close
        // this -- then nothing at all, with the socket held open.
        let mut quiet = connect(&addr).await;
        client_hello(&mut quiet, vec![native::CODEC_NATIVE_V1], vec![0]).await;
        let _ = read_control_response(&mut quiet).await;

        expect_reject_going_away_for_idleness(&mut quiet, "a handshaken connection gone quiet")
            .await;
        let mut buf = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_secs(2), quiet.read(&mut buf))
            .await
            .expect("the socket should close right behind the Reject")
            .unwrap();
        assert_eq!(n, 0, "the Reject is the last thing on this connection");

        assert_eq!(
            drained_counter(&registry, "logit.input.connections.closed", ("reason", "idle")),
            Some(1.0),
            "an idle close is counted"
        );
        assert_eq!(
            listener_diag.occurrences("connection_error"),
            0,
            "and never diagnosed -- an idle close returns Ok(()), so the accept loop's \
             connection_error path must not see it"
        );

        // The permit must be back: this listener's cap is 1, and `quiet` is still alive.
        let mut second = connect(&addr).await;
        client_hello(&mut second, vec![native::CODEC_NATIVE_V1], vec![0]).await;
        match read_control_response(&mut second).await {
            control::ControlMessage::HelloAck(ack) => {
                assert_eq!(ack.codec, native::CODEC_NATIVE_V1, "the permit came back");
            }
            other => panic!("expected HelloAck on the second connection, got {other:?}"),
        }
        drop(quiet);
    }

    /// **The test the from-the-last-`Ack` rule exists for.** A peer waiting for an `Ack` that a
    /// full downstream is delaying is not idle -- this listener is the one working (this module's
    /// "Ack point" and "Idle timeout" doc sections) -- so the clock must not be running while
    /// this connection's task is parked in `Fanout::send`.
    ///
    /// A capacity-1 channel with nothing draining it puts it exactly there: the first batch is
    /// buffered and acked, the second blocks. Three idle timeouts' worth of sleep must not close
    /// the connection, and once the test drains, *both* acks arrive, in order, with no idle close
    /// counted. A clock measured from the last frame *read*, or armed before the send, would fire
    /// here and cost a batch that was already accepted.
    #[tokio::test]
    async fn a_connection_waiting_on_a_delayed_ack_is_not_closed_as_idle() {
        let idle = Duration::from_millis(200);
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("logit_in", "logit_in", "listener");
        let (addr, input) = bound_input().await;
        let mut input = input.with_telemetry(telemetry).with_idle_timeout(Some(idle));
        // Capacity 1: the first send is buffered, the second blocks until something receives.
        let (sink, mut rx) = fanout_into_channel(1);
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_NATIVE_V1], vec![0]).await;
        let _ = read_control_response(&mut client).await;

        // Two frames, neither ack read yet -- the second leaves the listener parked in
        // `Fanout::send` with the channel full.
        send_data_frame(&mut client, &sample_batch(), Compression::None).await;
        send_data_frame(&mut client, &sample_batch(), Compression::None).await;

        // Long enough that a clock running across the blocked send would have fired three times.
        tokio::time::sleep(idle * 3).await;

        recv_batch(&mut rx).await; // drains the first batch, unblocking the second's send
        assert_eq!(read_ack(&mut client).await.seq, 1, "the first ack, written long before");
        assert_eq!(read_ack(&mut client).await.seq, 2, "and the second, after the drain");
        recv_batch(&mut rx).await;

        assert_eq!(
            drained_counter(&registry, "logit.input.connections.closed", ("reason", "idle")),
            None,
            "nothing was closed as idle, so the counter was never touched"
        );
    }

    /// The other half of the reset rule: every `Ack` re-arms the clock, so a connection sending
    /// steadily is never closed no matter how much total wall clock passes. Four frames 100ms
    /// apart against a 200ms idle timeout -- over twice the timeout in total -- all acked in
    /// order, and only *then*, once the sending stops, does the idle close arrive. Without the
    /// per-ack reset the third frame would land on a closed socket.
    #[tokio::test]
    async fn the_idle_clock_restarts_from_each_ack() {
        let (addr, input) = bound_input().await;
        let mut input = input.with_idle_timeout(Some(Duration::from_millis(200)));
        let (sink, mut rx) = fanout_into_channel(16);
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_NATIVE_V1], vec![0]).await;
        let _ = read_control_response(&mut client).await;

        for expected_seq in 1..=4u64 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            send_data_frame(&mut client, &sample_batch(), Compression::None).await;
            assert_eq!(read_ack(&mut client).await.seq, expected_seq);
            recv_batch(&mut rx).await;
        }

        // Now stop. The clock has only ever been armed from the last ack, so this is the first
        // gap that can reach 200ms.
        expect_reject_going_away_for_idleness(&mut client, "a connection that stopped sending")
            .await;
    }

    /// **The other side of the stall test below.** A header whose *first* byte lands just inside
    /// the idle deadline and whose remaining bytes land just outside it must be read, not
    /// rejected: the header arriving at all is progress, so from that byte on the bound is
    /// per-`read` rather than the absolute deadline ([`read_header`]'s [`IdleBounds`]).
    ///
    /// A single absolute deadline around the whole header fails this twice over -- it discards
    /// the bytes already read *and* answers `Reject{GOING_AWAY}` to a peer that had already
    /// started writing a frame, which a `logit_out` reads as `Fault::Ambiguous` for a batch it
    /// then drops under the default at-most-once posture. Exactly the loss the client-side probe
    /// exists to avoid, reintroduced from the listener side.
    ///
    /// 220ms then 160ms against a 300ms idle timeout. Both margins are deliberately wide, since
    /// a `sleep` can only *overshoot*: the first byte lands 80ms inside the absolute deadline
    /// (scheduler lag would have to eat all of that to push it past), the rest lands 80ms outside
    /// it (lag only makes that more true), and each gap sits 140ms under the 300ms per-`read`
    /// budget that applies from the first byte on. The earlier 100ms/80ms/60ms version left only
    /// ~20ms of slack on the one assertion lag could actually break.
    #[tokio::test]
    async fn a_frame_header_that_starts_arriving_at_the_idle_deadline_is_read_not_rejected() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("logit_in", "logit_in", "listener");
        let (addr, input) = bound_input().await;
        let mut input =
            input.with_telemetry(telemetry).with_idle_timeout(Some(Duration::from_millis(300)));
        let (sink, mut rx) = fanout_into_channel(16);
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_NATIVE_V1], vec![0]).await;
        let _ = read_control_response(&mut client).await;

        let mut encoder = NativeEncoder::new(Compression::None);
        let framed = encoder.encode(&sample_batch()).unwrap();

        // 220ms of the 300ms deadline spent, then one byte of the header -- progress, with 80ms
        // of slack against a sleep that can only overshoot.
        tokio::time::sleep(Duration::from_millis(220)).await;
        client.write_all(&framed[..1]).await.unwrap();
        // 160ms more, so 380ms since the handshake: 80ms past the absolute deadline, and 140ms
        // inside the per-`read` budget the byte above re-armed.
        tokio::time::sleep(Duration::from_millis(160)).await;
        client.write_all(&framed[1..]).await.unwrap();

        assert_eq!(
            read_ack(&mut client).await.seq,
            1,
            "a frame whose header started arriving before the deadline must be acked"
        );
        assert_eq!(recv_batch(&mut rx).await.events.len(), 1);
        assert_eq!(
            drained_counter(&registry, "logit.input.connections.closed", ("reason", "idle")),
            None,
            "and nothing closed as idle"
        );
    }

    /// And the failure side of that same per-`read` bound: a header that starts arriving and then
    /// stops is still closed. Switching to the per-`read` budget after the first byte must not
    /// turn a half-written header into an unbounded wait -- that would hand back the very permit
    /// leak the feature exists to close, one byte inside a frame.
    #[tokio::test]
    async fn a_frame_header_that_starts_and_then_stalls_is_closed_as_idle() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("logit_in", "logit_in", "listener");
        let (addr, input) = bound_input().await;
        let mut input =
            input.with_telemetry(telemetry).with_idle_timeout(Some(Duration::from_millis(100)));
        let (sink, mut rx) = fanout_into_channel(16);
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_NATIVE_V1], vec![0]).await;
        let _ = read_control_response(&mut client).await;

        let mut encoder = NativeEncoder::new(Compression::None);
        let framed = encoder.encode(&sample_batch()).unwrap();
        // One byte of the header, then nothing at all -- progress once, and never again.
        client.write_all(&framed[..1]).await.unwrap();

        expect_reject_going_away_for_idleness(&mut client, "a header that stopped arriving").await;
        assert_eq!(
            drained_counter(&registry, "logit.input.connections.closed", ("reason", "idle")),
            Some(1.0)
        );
        assert!(rx.try_recv().is_err(), "a one-byte header must never produce a batch");
    }

    /// A frame body that stops arriving part-way through is the same condition as a gap between
    /// frames, and ends the same way -- a `Reject{GOING_AWAY}` and a counted idle close, never an
    /// `Ack` for the half-arrived frame and never a batch downstream. Without the per-`read`
    /// stall bound the listener's `read_exact` would wait here forever, holding its permit: the
    /// gap the whole feature exists to close, one frame deeper.
    #[tokio::test]
    async fn a_frame_body_that_stalls_is_closed_as_idle_with_no_ack() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("logit_in", "logit_in", "listener");
        let (addr, input) = bound_input().await;
        let mut input =
            input.with_telemetry(telemetry).with_idle_timeout(Some(Duration::from_millis(100)));
        let (sink, mut rx) = fanout_into_channel(16);
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_NATIVE_V1], vec![0]).await;
        let _ = read_control_response(&mut client).await;

        // A real, well-formed frame -- header, declared lengths, CRC and all -- of which only the
        // header and one body byte are ever written. The listener parses the header, sizes the
        // body, and then reads a body that never finishes arriving.
        let mut encoder = NativeEncoder::new(Compression::None);
        let framed = encoder.encode(&sample_batch()).unwrap();
        assert!(framed.len() > frame::HEADER_LEN + 1, "the fixture needs a multi-byte body");
        client.write_all(&framed[..frame::HEADER_LEN + 1]).await.unwrap();

        expect_reject_going_away_for_idleness(&mut client, "a frame body that stopped arriving")
            .await;
        assert_eq!(
            drained_counter(&registry, "logit.input.connections.closed", ("reason", "idle")),
            Some(1.0),
            "a stalled body is counted exactly like an idle gap between frames"
        );
        assert!(rx.try_recv().is_err(), "a half-arrived frame must never be decoded or forwarded");
    }

    /// The default, and what every config without an `idle_timeout:` keeps getting: no bound at
    /// all on the gap between frames, so a handshaken `logit_out` peer with nothing to send stays
    /// connected indefinitely. `IdleBounds::new` returns `None` here, which takes
    /// [`read_header`]'s unbounded arm -- and the connection has to still *work* afterward, not
    /// merely be unclosed.
    #[tokio::test]
    async fn no_idle_timeout_leaves_a_handshaken_connection_open() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("logit_in", "logit_in", "listener");
        let (addr, input) = bound_input().await;
        // No `with_idle_timeout` call at all -- the shape every caller that never sets the field
        // produces.
        let mut input = input.with_telemetry(telemetry);
        let (sink, mut rx) = fanout_into_channel(16);
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_NATIVE_V1], vec![0]).await;
        let _ = read_control_response(&mut client).await;

        // Longer than any idle timeout in this section, with nothing sent at all.
        tokio::time::sleep(Duration::from_millis(300)).await;
        expect_still_open(&mut client, "a quiet connection with no idle_timeout").await;

        send_data_frame(&mut client, &sample_batch(), Compression::None).await;
        assert_eq!(read_ack(&mut client).await.seq, 1, "and still serving frames");
        recv_batch(&mut rx).await;

        assert_eq!(
            drained_counter(&registry, "logit.input.connections.closed", ("reason", "idle")),
            None,
            "nothing was closed as idle, so the counter was never touched"
        );
    }
}
