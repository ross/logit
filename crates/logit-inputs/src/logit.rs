//! `logit_in` -- the native `logit`-to-`logit` listener (`docs/design/wire-protocol.md`'s
//! connection protocol, `docs/adr/native-transport-handshake-and-ack.md`). Accepts many TCP
//! (optionally TLS) connections. Each speaks a `Hello`/`HelloAck` version/codec/compression
//! handshake (`logit_proto::native::control`), then loops: one native frame in, one
//! `Fanout::send`, one `Ack` out. One frame is in flight per connection; `HelloAck` always
//! answers `window: 1`.
//!
//! **Binding.** [`Input::bind`] opens the socket before any node task is spawned, so a taken port
//! fails startup, and [`LogitInput::local_addr`] reads a `:0` bind's port without a
//! bind-drop-rebind race. `run_until_shutdown` binds too when nobody did, for direct callers.
//!
//! **Ack point.** `Ack{seq}` is written only after `Fanout::send` returns, i.e. after the batch is
//! in every downstream inbox. A stalled downstream delays the ack, which stalls the sender's
//! `write_loop`. That is this listener's backpressure; there is no receive-side queue the way a
//! UDP listener has one (`crate::udp`).
//!
//! **Shutdown.** Every connection task holds its own [`Fanout`] clone, and the cancel-by-drop
//! shutdown (`docs/adr/service-lifecycle-and-output-retry.md`) needs nothing to outlive the
//! listener's future. So each connection races its wait for the next frame header against a clone
//! of [`Input::run_until_shutdown`]'s `shutdown` receiver, and once idle at a frame boundary sends
//! `Reject{GOING_AWAY}` and closes. A frame whose header has arrived always finishes: the
//! `select!` is re-evaluated only between frames.
//!
//! **Connection limit.** A non-blocking `try_acquire_owned` against the same 1024-connection cap
//! `otlp_in` ([`crate::otlp::MAX_CONCURRENT_CONNECTIONS`]) and `syslog_in`'s driver use: past the
//! cap, a client gets a clean `Reject{INTERNAL}` and the connection closes, rather than hanging
//! with a handshake that never starts. `logit`-to-`logit` peers retry on their own. **With TLS on,
//! the reject goes out after the TLS wrap, not onto the raw `TcpStream`**: a TLS `logit_out` past
//! the cap is waiting for a ServerHello, not framed bytes. So [`reject_or_serve`] runs after the
//! TLS accept for every connection. The cost is one TLS handshake per rejected connection,
//! bounded in time by the pre-`Hello` timeout but not in count, since a rejected connection holds
//! no permit. Closing with no `Reject` under TLS would be cheaper, but the peer would see only an
//! opaque close.
//!
//! **Pre-`Hello` timeout.** [`LogitInput::handshake_timeout`] (default [`HANDSHAKE_TIMEOUT`], set
//! by [`LogitInput::with_handshake_timeout`]) bounds each of the two pre-`Hello` phases
//! independently: the TLS accept (a `tokio::time::timeout` in the accept loop), then the `Hello`
//! read in [`handshake`], which starts a fresh timeout rather than sharing a deadline. The worst
//! case on the TLS path is two of them back to back (10s at the default) before a silent
//! connection gives up its permit. Without the TLS-accept bound, a client that connects and sends
//! nothing pins a permit forever, and 1024 of them turn every later peer into a `Reject`.
//!
//! **Idle timeout.** [`LogitInput::with_idle_timeout`] (`idle_timeout:`, off unless set) bounds
//! how long a handshaken connection may stay quiet before this listener closes it and returns its
//! permit (`docs/adr/idle-connection-timeout.md`). It's a rolling deadline, not a per-phase one.
//!
//! *Measured from the last `Ack` written* (or from the handshake, before any frame), never from
//! the last frame read. A peer waiting for an `Ack` is not idle: a slow downstream is delaying
//! that ack ("Ack point" above), so this listener is the one working. Stamping the clock after the
//! `Ack` write, which follows `Fanout::send`, means time blocked in `Fanout::send` never counts
//! against a peer.
//!
//! *A header that has started arriving is progress.* The absolute deadline bounds only the wait
//! for a frame's first byte; the rest of the header is read under the per-`read` bound a body
//! gets, so a frame whose first byte arrives shortly before the deadline is read, not rejected
//! mid-header ([`read_header`]'s [`IdleBounds`]).
//!
//! *A frame body is bounded per `read`, not in total* ([`read_frame_body`]'s `stall` argument): a
//! large frame arriving slowly but steadily is not idle, while a peer that sends a header, half a
//! body, and then nothing is.
//!
//! *Every idle close says so on the wire*: `Reject{GOING_AWAY, "idle for <dur>"}`, the same
//! control message a shutdown sends ([`going_away`] writes both). A `logit_out` peer needs no new
//! case for it, and `logit_out`'s pooled-connection probe (`logit_outputs`' `poll_pending_close`)
//! looks for it before reusing a connection. An idle close is policy, not a fault:
//! [`serve_connection`] returns `Ok(())`, so the accept loop's `connection_error` diagnostic never
//! sees it; it's counted as `logit.input.connections.closed{reason="idle"}` instead.

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

/// Default for [`LogitInput::handshake_timeout`], per pre-`Hello` phase: generous for a loaded
/// peer under TLS, tight enough that an abandoned connection (a port scan, a TLS client that never
/// sends its ClientHello) doesn't pin a connection-limit permit.
///
/// `logit_config`'s `default_handshake_timeout` mirrors this number by hand (it can't depend on
/// this crate).
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// Rejected outright past this, never queued (module doc's "Connection limit").
const MAX_CONCURRENT_CONNECTIONS: usize = 1024;

/// Re-exported for symmetry with `crate::otlp`'s path; both share `crate::tls`'s definition.
pub use crate::tls::TlsServerSettings;

pub struct LogitInput {
    bind: String,
    diag: Diagnostics,
    telemetry: Telemetry,
    tls: Option<Arc<rustls::ServerConfig>>,
    max_frame_bytes: u32,
    max_connections: usize,
    /// Set by [`Input::bind`], taken by [`Input::run_until_shutdown`]; `None` again after a run,
    /// so a second run rebinds (module doc's "Binding").
    listener: Option<TcpListener>,
    /// Per pre-`Hello` phase (module doc's "Pre-`Hello` timeout").
    handshake_timeout: Duration,
    /// `None`, the default, means no idle timeout (module doc's "Idle timeout").
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

    /// The bound address, once [`Input::bind`] has run: a `:0` bind's OS-assigned port.
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

    /// Turns on TLS termination (`tls:` in config). No ALPN, unlike `otlp_in`: this isn't an
    /// HTTP-shaped protocol, so there's nothing to negotiate.
    pub fn with_tls(
        mut self,
        settings: &TlsServerSettings,
        base_dir: &Path,
    ) -> anyhow::Result<Self> {
        self.tls = Some(Arc::new(crate::tls::build_server_config(settings, base_dir, &[])?));
        Ok(self)
    }

    /// Caps one frame's size, compressed and uncompressed alike; echoed to every client in
    /// `HelloAck.max_frame_bytes`. Clamped to [`frame::MAX_SANE_UNCOMPRESSED_LEN`] (64 MiB, the
    /// default), the ceiling `frame::read_frame_with_header` enforces anyway.
    pub fn with_max_frame_bytes(mut self, max_frame_bytes: u32) -> Self {
        self.max_frame_bytes = max_frame_bytes.min(frame::MAX_SANE_UNCOMPRESSED_LEN);
        self
    }

    /// Test-only override of [`MAX_CONCURRENT_CONNECTIONS`], so a test reaches the cap without
    /// 1025 real connections.
    #[cfg(test)]
    fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Overrides [`HANDSHAKE_TIMEOUT`] for both pre-`Hello` phases (the TLS accept and the `Hello`
    /// read); `handshake_timeout:` in config. Graph rule 45 rejects `0s` before it gets here.
    pub fn with_handshake_timeout(mut self, handshake_timeout: Duration) -> Self {
        self.handshake_timeout = handshake_timeout;
        self
    }

    /// Bounds how long a handshaken connection may stay quiet; `idle_timeout:` in config, off
    /// (`None`) by default. Module doc's "Idle timeout" has the clock's rules. Graph rule 53
    /// rejects `Some(0s)` before it gets here.
    ///
    /// Takes the `Option` to match `crate::tcp::TcpListener::with_idle_timeout`, so `logit-cli`'s
    /// `build_spec` passes the config value straight through on every listener arm.
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
        // A never-firing `watch`, so `run` and `run_until_shutdown` share one implementation.
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
        // `crate::tcp`'s sampler: publishes `logit.input.accept_queue.depth`/`.utilization`, and
        // its `accept` is cancel-safe against the `shutdown` arm below.
        let mut accept_queue =
            crate::tcp::AcceptQueueSampler::new(self.telemetry.clone(), self.diag.clone());

        loop {
            let (stream, _peer) = tokio::select! {
                accepted = accept_queue.accept(&listener) => accepted?,
                _ = shutdown.wait_for(|&due| due) => return Ok(()),
            };

            // `None` means past the cap. `reject_or_serve` writes the reject, after the TLS wrap
            // below rather than onto this raw `stream` (module doc's "Connection limit").
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
                // One connection's error (a mid-frame disconnect, a malformed preamble, a failed
                // or timed-out TLS accept, a cap reject that failed to write) is never fatal to
                // the listener; only `accept` failing above is.
                if let Err(err) = result {
                    diag.warn_throttled("connection_error", err);
                }
            });
        }
    }
}

/// Writes `Reject{INTERNAL}` when `permit` is `None` (past the cap), else serves the connection.
/// Runs on the already-TLS-wrapped stream, so the reject is decodable with or without TLS (module
/// doc's "Connection limit"). The `logit.input.connections` gauge counts only connections holding
/// a permit, so it's updated here rather than in the accept loop.
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
    // `Telemetry::gauge` is last-write-wins per key, so two tasks interleaving an add and a load
    // could leave a stale value published until the next transition.
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
    /// `CODEC_NATIVE_V2` if the client offered it (so provenance crosses the wire), else
    /// `CODEC_NATIVE_V1`. Every data frame must carry this codec; `serve_connection` checks each.
    codec: u8,
}

fn compression_tag(compression: Compression) -> &'static str {
    match compression {
        Compression::None => "none",
        Compression::Lz4 => "lz4",
        Compression::Zstd => "zstd",
    }
}

/// The `logit.proto.frames` metric's `codec` tag; matches `logit_outputs::logit`'s `codec_tag`.
fn codec_tag(codec: u8) -> &'static str {
    match codec {
        native::CODEC_NATIVE_V2 => "native_v2",
        _ => "native_v1",
    }
}

/// Serves one accepted (and, with TLS on, already TLS-handshaken) connection to completion:
/// `Hello`/`HelloAck`, then frame, `Fanout::send`, `Ack`, until close, shutdown, or idle close.
///
/// `logit.proto.errors{reason}`: `handshake` (the handshake failed), `too_large` (a header
/// declared more than `max_frame_bytes`), `truncated` (the body read hit EOF or an I/O error),
/// `crc`, `codec` (a frame not under the negotiated codec), `magic` (any other malformed frame, or
/// an undecodable batch). A close or error mid-header is not counted.
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

    // The idle clock starts at the handshake; after this, only an `Ack` write advances it.
    let mut last_progress = tokio::time::Instant::now();

    let mut seq: u64 = 0;
    loop {
        // Only the header read races `shutdown`; once a header has arrived, the body read,
        // decode, forward, and ack run uninterrupted. This explicit check catches a shutdown
        // that fired before this iteration: `changed()` fires only on a transition this receiver
        // hasn't observed. The `borrow()` `Ref` drops at the end of the statement, before any
        // `.await`.
        if *shutdown.borrow() {
            going_away(&mut stream, "listener shutting down").await;
            return Ok(());
        }
        // `changed()`, not `wait_for`: `wait_for`'s `Ref` guard makes the `select!` future
        // `!Send` once an arm awaits afterward, and `tokio::spawn` needs `Send`. With the check
        // above they're equivalent, since `shutdown` flips false -> true only once.
        //
        // This read is also the only unbounded wait on the peer, so the idle clock lives here:
        // [`IdleBounds`] gives an absolute deadline for the first byte, a per-`read` one after.
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
            // Clean close at a frame boundary: the ordinary end of a connection.
            return Ok(());
        };

        let (header, mut payload) =
            match read_frame_body(&mut stream, header_buf, max_frame_bytes, idle_timeout).await {
                Ok(v) => v,
                // A stalled body is an idle close like a gap between frames: policy, not a
                // fault, so no `logit.proto.errors`.
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
            // A client sends no control message after `Hello`; one here closes the connection.
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

        // The ack point: `send_relayed` returns once every downstream inbox has the batch. It
        // backfills only provenance the wire didn't carry (a v1 peer, or a v2 peer with none) and
        // passes a v2 peer's `origin`/`previous` through untouched
        // (`docs/adr/batch-provenance-on-delivered.md`).
        sink.send_relayed(batch, provenance).await;

        // `seq` is implicit: the Nth data frame on a connection is acked as N.
        seq += 1;
        write_control(&mut stream, &control::Ack { seq }).await?;
        // The only place the idle clock restarts: after the send and the ack, so time spent on a
        // full downstream is charged to this listener, not the waiting peer.
        last_progress = tokio::time::Instant::now();
    }
}

/// Writes `Reject{GOING_AWAY, why}` before this listener closes a connection, for a shutdown and
/// an idle close alike. `logit_out` treats `REJECT_GOING_AWAY` as transient and reconnects.
///
/// The write's result is discarded: the connection is closing anyway, and a peer that already
/// vanished is not a fault. Every caller returns `Ok(())` right after.
async fn going_away<S: AsyncWrite + Unpin>(stream: &mut S, why: &str) {
    let reject = control::Reject { code: control::REJECT_GOING_AWAY, message: why.to_string() };
    let _ = write_control(stream, &reject).await;
}

/// Ends a connection quiet (or mid-frame stalled) past its `idle_timeout`: tells the peer, counts
/// `logit.input.connections.closed{reason="idle"}`, and returns `Ok(())`.
///
/// Never `Err`: an idle close is policy, and an `Err` would reach the accept loop's
/// `connection_error` diagnostic. The permit comes back when the task ends.
async fn close_idle<S: AsyncWrite + Unpin>(
    stream: &mut S,
    telemetry: &Telemetry,
    idle: Duration,
) -> anyhow::Result<()> {
    going_away(stream, &format!("idle for {idle:?}")).await;
    telemetry.count("logit.input.connections.closed", 1.0, &[("reason", "idle")]);
    Ok(())
}

/// Reads `Hello` within a fresh `handshake_timeout` (not the TLS accept's remainder; module doc's
/// "Pre-`Hello` timeout") and replies `HelloAck` or `Reject`, returning an error whenever the
/// client can't proceed.
///
/// `HelloAck` carries the best shared codec, `lz4` or no compression, this listener's
/// `max_frame_bytes` (every later frame is bounded by it), and `window: 1`.
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
        // `None`: the whole `Hello` read is already inside `handshake_timeout`.
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

    // Prefer v2 (carries provenance), else v1: a `logit_out` offering only `[1]` still talks,
    // and one offering `[2, 1]` gets provenance (`docs/adr/batch-provenance-on-delivered.md`).
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

    // Compression always falls back to `None`, so unlike codec it has no reject path.
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

/// A header read's idle bounds on a connection with an `idle_timeout`; `None` (unbounded) on one
/// without. Two bounds because "sent nothing" and "part-way through a header" differ (see
/// [`read_header`]).
struct IdleBounds {
    /// `last_progress + idle_timeout`, absolute, so measured from the last `Ack`. Bounds only the
    /// header's first byte.
    first_byte: tokio::time::Instant,
    /// The per-`read` budget after the first byte: `idle_timeout` itself, as a body's `stall`.
    stall: Duration,
}

impl IdleBounds {
    /// `None` without an `idle_timeout`. `checked_add` because an absurd but legal value can
    /// overflow: rule 53 sets no upper bound.
    fn new(last_progress: tokio::time::Instant, idle_timeout: Option<Duration>) -> Option<Self> {
        let idle = idle_timeout?;
        Some(Self {
            first_byte: last_progress.checked_add(idle).unwrap_or_else(crate::tcp::far_future),
            stall: idle,
        })
    }
}

/// Why [`read_header`] produced no header. `Idle` is separate so a caller can turn it into an
/// idle close rather than an error.
enum HeaderReadError {
    Io(anyhow::Error),
    /// An [`IdleBounds`] bound elapsed, first byte or later. Carries the configured
    /// `idle_timeout` for `close_idle` to name.
    Idle(Duration),
}

impl HeaderReadError {
    fn into_inner(self) -> anyhow::Error {
        match self {
            HeaderReadError::Io(err) => err,
            // Unreachable from today's flattening callers, which pass no bounds.
            HeaderReadError::Idle(idle) => {
                anyhow::anyhow!("a frame header stopped arriving for {idle:?}")
            }
        }
    }
}

/// Reads [`frame::HEADER_LEN`] bytes off `stream`, telling a clean close at a frame boundary
/// (`Ok(None)`) from a close mid-header (an error), which `read_exact` can't. This is the read
/// `serve_connection` races against `shutdown`.
///
/// **Why `bounds` is two deadlines.** [`IdleBounds::first_byte`] is absolute, measured from the
/// last `Ack`. Once the first byte arrives the header is progress, so each later read gets
/// [`IdleBounds::stall`], the per-`read` rule [`read_frame_body`] applies to a body. One absolute
/// deadline around the whole header would discard a header that started shortly before it, and send
/// `Reject{GOING_AWAY}` to a peer already writing a frame: for `logit_out`, the
/// `Fault::Ambiguous` batch its pooled-connection probe exists to avoid. The first-byte deadline
/// firing loses nothing, since no byte has been read.
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

/// Why [`read_frame_body`] failed, so a caller picks the `logit.proto.errors{reason}` tag without
/// parsing error text.
enum FrameReadError {
    TooLarge(anyhow::Error),
    Truncated(anyhow::Error),
    Crc(anyhow::Error),
    Malformed(anyhow::Error),
    /// One body `read` made no progress for the whole `stall` bound. Not an error: the caller
    /// turns it into `close_idle`, which names this duration. Never reached in the handshake.
    Stalled(Duration),
}

impl FrameReadError {
    fn into_inner(self) -> anyhow::Error {
        match self {
            FrameReadError::TooLarge(e)
            | FrameReadError::Truncated(e)
            | FrameReadError::Crc(e)
            | FrameReadError::Malformed(e) => e,
            // Unreachable from today's flattening callers, which pass no `stall` bound.
            FrameReadError::Stalled(idle) => {
                anyhow::anyhow!("a frame body stopped arriving for {idle:?}")
            }
        }
    }
}

/// Parses `header_buf` and checks both declared lengths against `max_frame_bytes` (capped at
/// [`frame::MAX_SANE_UNCOMPRESSED_LEN`]) *before* reading the body, then hands the whole frame to
/// [`frame::read_frame_with_header`] for its CRC, decompression, and length checks.
///
/// `stall` bounds each `read` of the body, not the body in total (module doc's "Idle timeout").
/// It's the connection's `idle_timeout`; `None` (the handshake, test helpers) means unbounded.
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

    // A fill loop rather than `read_exact`, so each `read` carries the `stall` bound and a peer
    // that closed mid-body (`Ok(0)`, `Truncated`) stays distinct from one that stopped
    // (`Stalled`, an idle close).
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

/// Writes one control message with [`frame::FLAG_CONTROL`] set. `codec`/`compression` mean
/// nothing on a control frame (`logit_proto::native::control`), so they're always `0`/`None`.
async fn write_control<S: AsyncWrite + Unpin>(
    stream: &mut S,
    msg: &impl ControlEncode,
) -> anyhow::Result<()> {
    let framed =
        frame::write_frame_with_flags(0, Compression::None, frame::FLAG_CONTROL, &msg.encode())?;
    stream.write_all(&framed).await?;
    Ok(())
}

/// Lets [`write_control`] take any control message type without wrapping it in
/// `control::ControlMessage`.
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

    /// An input already bound to an ephemeral port, and that port's address. The socket is live
    /// on return, so a test can connect right after spawning `run`, with no readiness sleep.
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

    /// [`fanout_into_channel`] with a component id, for tests of what `send_relayed` backfills.
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

    /// Reads one whole frame, unbounded, returning the header (for `flags`) and payload.
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

    /// [`send_data_frame`] under `CODEC_NATIVE_V2`, with a provenance trailer.
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

    /// `bind` makes the port live and `local_addr` readable before `run` is spawned.
    #[tokio::test]
    async fn bind_makes_the_port_live_before_run_and_local_addr_reports_it() {
        let mut input = LogitInput::new("127.0.0.1:0");
        assert_eq!(input.local_addr(), None, "no address before bind()");

        input.bind().await.expect("binding an ephemeral port should succeed");
        let addr = input.local_addr().expect("bind() should leave a real address behind");

        // Nothing is running yet; this connection sits in the accept backlog.
        let _early = connect(&addr.to_string()).await;
    }

    /// A second `bind()` is a no-op ([`Input::bind`]'s idempotency contract).
    #[tokio::test]
    async fn a_second_bind_is_a_no_op() {
        let mut input = LogitInput::new("127.0.0.1:0");
        input.bind().await.expect("first bind should succeed");
        let addr = input.local_addr().expect("bind() should leave a real address behind");
        input.bind().await.expect("second bind should be a harmless no-op");
        assert_eq!(input.local_addr(), Some(addr), "the address must not change");
    }

    /// A port already held is an `Err` out of `bind`, which startup turns into a startup failure.
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

    /// A v2 client's `origin`/`previous` are relayed untouched.
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

    /// An empty v2 provenance trailer gets this listener's id backfilled into both fields.
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

    /// A v1 client's batch gets this listener's id backfilled into both fields.
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

    /// A client offering both codecs negotiates v2.
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

    /// A client offering only `[1]` still negotiates and delivers.
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

        // The listener closes without replying: EOF, not a HelloAck/Reject.
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

        // Only a header declaring an oversized frame: a listener that waited for the body first
        // would hang here.
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
        // Capacity 1: the second batch's `Fanout::send` blocks until the test drains the first.
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

        // The server task records the counter after closing the socket.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(drained_counter(&registry, "logit.proto.errors", ("reason", "crc")), Some(1.0));
    }

    #[tokio::test]
    async fn the_graph_closes_after_shutdown_with_an_idle_client_still_connected() {
        let (addr, mut input) = bound_input().await;
        let (sink, mut rx) = fanout_into_channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move { input.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_NATIVE_V1], vec![0]).await;
        let _ = read_control_response(&mut client).await;

        shutdown_tx.send(true).unwrap();

        // `rx` closes only once the accept loop's and the connection's `Fanout` clones are gone.
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

        // Holds the one permit without ever handshaking.
        let _first = connect(&addr).await;
        // Not a readiness wait: lets the accept loop take `_first`'s permit before the next.
        tokio::time::sleep(Duration::from_millis(50)).await;

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

    // ---- TLS: pre-`Hello` timeout and cap-reject-over-TLS ------------------------------------

    fn testdata_dir() -> std::path::PathBuf {
        // The repo root's `testdata/tls` (`testdata/tls/README.md`).
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    fn test_tls_settings() -> TlsServerSettings {
        TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: None,
        }
    }

    /// A `tokio-rustls` client trusting `testdata/tls/ca.pem`, with no client certificate.
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

    /// [`read_control_response`] over any stream, for the TLS tests.
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

        // Takes the one permit and never sends a ClientHello; held open, so only the TLS-accept
        // timeout can free the permit.
        let _silent = connect(&addr).await;

        tokio::time::sleep(Duration::from_millis(400)).await;

        // A `HelloAck` here, not `Reject{INTERNAL}`, proves the permit came back.
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

        // Holds the one permit for the default 5s handshake timeout.
        let _first = connect(&addr).await;
        // Not a readiness wait: lets the accept loop take `_first`'s permit before the next.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Past the cap, yet the TLS handshake completes and the Reject arrives decodable over it.
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
    // Real durations, never `tokio::time::pause()`: these tests race a timer against a socket
    // read, and paused time would advance straight past the read the listener is parked in.
    // "Closed" assertions wait up to 2s against deadlines of at most 300ms; "still open" ones
    // assert `timeout(50ms, read)` elapses, which scheduler lag can only make more true.

    /// Asserts the next frame is the idle close's `Reject{GOING_AWAY, "idle for <dur>"}`.
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

    /// Asserts nothing is readable for 50ms. The listener writes only in response to something,
    /// so on a connection given nothing to answer, any byte would be a close's `Reject`.
    async fn expect_still_open(stream: &mut TcpStream, what: &str) {
        let mut buf = [0u8; 1];
        match tokio::time::timeout(Duration::from_millis(50), stream.read(&mut buf)).await {
            Err(_elapsed) => {}
            Ok(Ok(0)) => panic!("{what}: expected the connection to still be open, got a close"),
            Ok(Ok(_)) => panic!("{what}: expected no bytes, got a frame"),
            Ok(Err(err)) => panic!("{what}: expected the connection to still be open, got {err}"),
        }
    }

    /// A quiet handshaken connection gets `Reject{GOING_AWAY}`, is counted not diagnosed, and
    /// releases its permit (cap 1, quiet client held open, so only the idle clock could free it).
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

        // Cap 1 and `quiet` still alive: this handshakes only if the permit came back.
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

    /// Time parked in `Fanout::send` behind a full downstream never counts as idle: the clock
    /// runs from the last `Ack`, not the last frame read.
    #[tokio::test]
    async fn a_connection_waiting_on_a_delayed_ack_is_not_closed_as_idle() {
        let idle = Duration::from_millis(200);
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("logit_in", "logit_in", "listener");
        let (addr, input) = bound_input().await;
        let mut input = input.with_telemetry(telemetry).with_idle_timeout(Some(idle));
        // Capacity 1: the second frame leaves the listener parked in `Fanout::send`.
        let (sink, mut rx) = fanout_into_channel(1);
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_NATIVE_V1], vec![0]).await;
        let _ = read_control_response(&mut client).await;

        send_data_frame(&mut client, &sample_batch(), Compression::None).await;
        send_data_frame(&mut client, &sample_batch(), Compression::None).await;

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

    /// Every `Ack` re-arms the clock: frames 100ms apart outlast a 200ms timeout until they stop.
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

        expect_reject_going_away_for_idleness(&mut client, "a connection that stopped sending")
            .await;
    }

    /// A header whose first byte lands inside the idle deadline and whose rest lands outside it
    /// is read, not rejected ([`read_header`]'s [`IdleBounds`]).
    ///
    /// 220ms then 160ms against a 300ms timeout: the first byte lands 80ms inside the absolute
    /// deadline, the rest 80ms past it and 140ms inside the per-`read` budget. A `sleep` only
    /// overshoots, so only the first margin is lag-sensitive; keep it wide.
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

        tokio::time::sleep(Duration::from_millis(220)).await;
        client.write_all(&framed[..1]).await.unwrap();
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

    /// A header that starts arriving and then stops is still closed as idle.
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
        client.write_all(&framed[..1]).await.unwrap();

        expect_reject_going_away_for_idleness(&mut client, "a header that stopped arriving").await;
        assert_eq!(
            drained_counter(&registry, "logit.input.connections.closed", ("reason", "idle")),
            Some(1.0)
        );
        assert!(rx.try_recv().is_err(), "a one-byte header must never produce a batch");
    }

    /// A stalled frame body is an idle close: `Reject{GOING_AWAY}`, counted, no `Ack`, no batch.
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

        // A well-formed frame, of which only the header and one body byte are written.
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

    /// With no `idle_timeout`, a quiet handshaken connection stays open and still serves frames.
    #[tokio::test]
    async fn no_idle_timeout_leaves_a_handshaken_connection_open() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("logit_in", "logit_in", "listener");
        let (addr, input) = bound_input().await;
        let mut input = input.with_telemetry(telemetry);
        let (sink, mut rx) = fanout_into_channel(16);
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_NATIVE_V1], vec![0]).await;
        let _ = read_control_response(&mut client).await;

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
