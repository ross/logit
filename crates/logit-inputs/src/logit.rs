//! `logit_in` -- the native `logit`-to-`logit` listener (`docs/design/wire-protocol.md`'s
//! connection protocol, `docs/adr/native-transport-handshake-and-ack.md`). Accepts many TCP
//! (optionally TLS) connections. Each speaks a `Hello`/`HelloAck` version/codec/compression
//! handshake (`logit_proto::native::control`), then loops: one native frame in, one
//! `Fanout::send`, one `Ack` out.
//!
//! **Send window.** A peer may have several frames in flight
//! (`docs/adr/native-hop-send-window.md`). `HelloAck` answers the smaller of the `Hello`'s window
//! and `RECEIVER_MAX_WINDOW`; a `Hello` window of 0 fails to decode, so the answer is at least 1.
//! Nothing here tracks the window: one task per connection reads,
//! forwards, and answers its frames one at a time, so answers leave in frame order and the k-th
//! answer on a connection is the k-th frame's. Accepted sockets set `TCP_NODELAY`.
//!
//! **Binding.** [`Input::bind`] opens the socket before any node task is spawned, so a taken port
//! fails startup, and [`LogitInput::local_addr`] reads a `:0` bind's port without a
//! bind-drop-rebind race. `run_until_shutdown` binds too when nobody did, for direct callers.
//!
//! **Ack point.** `Ack`, which carries no fields, is written in one of two cases: after
//! `send_relayed` returns `true`, i.e. after the batch is in every open downstream inbox, or, for a
//! frame at or below its sender's mark ("Deduplication" below), at once and with no forward. A
//! stalled downstream delays the ack, which stalls the sender's `write_loop` once its window is
//! full. That is this listener's backpressure; there is no receive-side queue the way a UDP
//! listener has one (`crate::udp`). A frame no consumer took, because every consumer of this
//! listener has closed, is never acked: it is answered `Reject{GOING_AWAY}`, the connection closes,
//! its sender's mark stays where it was, and the frame's batch is counted
//! `logit.input.batches.dropped{reason="closed_consumer"}`.
//!
//! **Deduplication.** Every data frame's trailer carries its sender's identity and sequence
//! (`docs/adr/native-hop-identity-and-sequence.md`); a frame without a complete pair is
//! malformed, a protocol error that ends the connection
//! (`docs/adr/native-hop-no-compatibility.md`). Each component keeps one table of
//! high-water marks, one per identity, shared by its connections and bounded at
//! `max_connections + max_connections / 4` identities (`max_connections:` in config; 1280 at the
//! default cap of 1024); a new identity at a full table evicts the least recently seen one. A
//! frame at or below its identity's mark is a resend: acked and not forwarded. Any other frame is
//! forwarded, and a consumer taking it raises the mark to its sequence; gaps above the mark are
//! ignored. No lock spans a forward, so a frame an ended
//! connection still holds can be forwarded beside the sender's resend of it on a new connection
//! (`docs/known-gaps.md`, "A resend can race the frames an ended connection still holds"). The
//! identity is advisory, never trusted: a peer minting a new identity per frame costs one scan of
//! the full table each and can evict honest senders, whose resends are then forwarded, a
//! duplicate and never a loss. Counted as `logit.input.batches.resends`, `logit.input.senders` (a
//! gauge), and `logit.input.senders.evicted`.
//!
//! **Shutdown.** Every connection task holds its own [`Fanout`] clone, and the cancel-by-drop
//! shutdown (`docs/adr/service-lifecycle-and-output-retry.md`) needs nothing to outlive the
//! listener's future. So each connection races its wait for the next frame header against a clone
//! of [`Input::run_until_shutdown`]'s `shutdown` receiver, and once idle at a frame boundary sends
//! `Reject{GOING_AWAY}` and closes. A frame whose header this listener has finished reading always
//! finishes: the `select!` is re-evaluated only between frames. A frame still in the socket
//! buffer, or partly read into the header, when shutdown fires is answered `GOING_AWAY` and never
//! forwarded.
//!
//! *`GOING_AWAY` and forwarding exclude each other.* `GOING_AWAY` is written only for a frame that
//! wasn't forwarded, for one of three causes: shutdown (the loop-top and `select!` arms), an idle
//! close, or no consumer taking the frame's batch (`send_relayed` returned `false`). Every other
//! `Reject` (the past-the-cap one, the handshake's, and `FRAME_TOO_LARGE`) also goes out before
//! the frame it answers reaches `send_relayed`, and a forwarded frame's only answer is its `Ack`.
//! So a `logit_out` that gets `GOING_AWAY` in place of an `Ack` knows the batch never landed, and
//! resends it at any delivery posture.
//!
//! **Bounded, flushed writes.** Every control write (`HelloAck`, `Ack`, and every `Reject`,
//! including `GOING_AWAY`) is flushed, and the write and flush finish within `handshake_timeout`
//! or are abandoned ([`write_control`]). The flush is what sends a TLS write's queued tail: the
//! waiting read on this side doesn't, and the peer is waiting for the whole message. A peer
//! that sends frames but never reads its `Ack`s fills this side's send buffer; unbounded, the
//! blocked write would hold the task, its permit, and its [`Fanout`] clone, and so the
//! shutdown, for as long as the peer stayed connected. `idle_timeout` bounds reads only and can't
//! reach a blocked write. A stalled `Ack` ends the connection as an error, counted as
//! `logit.proto.errors{reason="ack_write_stalled"}`; a stalled `Reject` is abandoned, since the
//! connection is closing anyway, and counted as `reason="reject_write_stalled"`.
//!
//! **Close.** A peer that closes between frames ends the connection with `Ok(())`: an EOF, or,
//! under TLS, `UnexpectedEof` from a peer gone without `close_notify` (a `logit_out` dropping a
//! connection after a failed attempt), since no frame is in flight either way. A close or read
//! error part-way through a header is an error, counted
//! `logit.proto.errors{reason="truncated_header"}`; part-way through a body, `reason="truncated"`.
//!
//! *Every other end after the handshake lingers* ([`close_lingering`]): the connection's
//! [`Fanout`] clone is dropped first, then the stream is shut down and read to EOF or for
//! `handshake_timeout`. A peer with frames pipelined behind the one answered leaves unread bytes,
//! and closing over them sends a reset that discards the `GOING_AWAY` or `Ack`s already written.
//! The permit is held through the linger.
//!
//! **Connection limit.** A non-blocking `try_acquire_owned` against the connection cap
//! (`max_connections:`, 1024 by default, the [`crate::DEFAULT_MAX_CONNECTIONS`] `otlp_in` and
//! `syslog_in`'s driver share): past the cap, a client gets a clean `Reject{INTERNAL}` and the
//! connection closes, rather than hanging with a handshake that never starts. `logit`-to-`logit`
//! peers retry on their own. **With TLS on,
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
//! nothing pins a permit forever, and a cap's worth of them turn every later peer into a
//! `Reject`.
//!
//! **Idle timeout.** [`LogitInput::with_idle_timeout`] (`idle_timeout:`, off unless set) bounds
//! how long a handshaken connection may stay quiet before this listener closes it and returns its
//! permit (`docs/adr/idle-connection-timeout.md`). It's a rolling deadline, not a per-phase one.
//! It bounds reads only; a write blocked on a peer that stopped reading is `handshake_timeout`'s
//! ("Bounded, flushed writes" above).
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
use logit_core::{Diagnostics, Telemetry};
use logit_pipeline::Fanout;
use logit_proto::frame::{self, Compression, FrameHeader};
use logit_proto::native::{self, control};
use logit_proto::CodecError;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;

mod senders;
use senders::SenderTable;

/// Default for [`LogitInput::handshake_timeout`], per pre-`Hello` phase: generous for a loaded
/// peer under TLS, tight enough that an abandoned connection (a port scan, a TLS client that never
/// sends its ClientHello) doesn't pin a connection-limit permit.
///
/// `logit_config`'s `default_handshake_timeout` mirrors this number by hand (it can't depend on
/// this crate).
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// The largest window `HelloAck` answers (module doc's "Send window"). A peer can leave this
/// many `Ack`s unread in this side's send buffer, about 47 KB under TLS, under the default
/// `tcp_rmem`.
const RECEIVER_MAX_WINDOW: u32 = 1024;

/// Re-exported for symmetry with `crate::otlp`'s path; both share `crate::tls`'s definition.
pub use crate::tls::TlsServerSettings;

pub struct LogitInput {
    bind: String,
    diag: Diagnostics,
    telemetry: Telemetry,
    tls: Option<Arc<rustls::ServerConfig>>,
    max_frame_bytes: u32,
    /// Rejected outright past this, never queued (module doc's "Connection limit"); also sizes the
    /// sender table (module doc's "Deduplication"). The worst case it bounds is `max_frame_bytes`
    /// per connection, 64 MiB × the cap at the defaults.
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
            max_connections: crate::DEFAULT_MAX_CONNECTIONS,
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

    /// Overrides [`crate::DEFAULT_MAX_CONNECTIONS`]; `max_connections:` in config. Graph rule 74
    /// rejects `0` before it gets here.
    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
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
        // One per run and per component: a second `logit_in` in the graph keeps its own.
        let senders = Arc::new(SenderTable::new(self.max_connections, self.telemetry.clone()));
        let tls_acceptor = self.tls.clone().map(TlsAcceptor::from);
        let live_connections = crate::listener::LiveConnections::new(self.telemetry.clone());
        let max_frame_bytes = self.max_frame_bytes;
        let handshake_timeout = self.handshake_timeout;
        let idle_timeout = self.idle_timeout;
        // `crate::tcp`'s sampler: publishes `logit.input.accept_queue.depth`/`.utilization`.
        // Both accepts race `shutdown`: see `docs/design/pipeline-graph.md`'s "Cancellation
        // points".
        let mut accept_queue =
            crate::tcp::AcceptQueueSampler::new(self.telemetry.clone(), self.diag.clone());

        let mut accept_diag = self.diag.clone();

        loop {
            let accepted = tokio::select! {
                accepted = accept_queue.accept(&listener) => accepted,
                _ = shutdown.wait_for(|&due| due) => return Ok(()),
            };
            let (stream, _peer) = match accepted {
                Ok(accepted) => accepted,
                Err(err) => {
                    // `biased`, absorb first: see `docs/design/pipeline-graph.md`'s
                    // "Cancellation points".
                    tokio::select! {
                        biased;
                        absorbed = crate::listener::absorb_accept_error(
                            err,
                            &self.telemetry,
                            &mut accept_diag,
                        ) => absorbed?,
                        _ = shutdown.wait_for(|&due| due) => return Ok(()),
                    }
                    continue;
                }
            };

            // An `Ack` the peer waits on can otherwise sit behind Nagle's algorithm and the
            // peer's delayed ACK. A failure costs only that latency.
            let _ = stream.set_nodelay(true);

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
            let senders = senders.clone();

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
                                    senders,
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
                            senders,
                        )
                        .await
                    }
                };
                // One connection's error (a mid-frame disconnect, a malformed preamble, a failed
                // or timed-out TLS accept, a cap reject that failed to write) is never fatal to
                // the listener; only `accept` failing above is.
                if let Err(err) = result {
                    match err.downcast_ref::<CodecError>() {
                        Some(CodecError::BudgetExceeded { limit }) => {
                            diag.warn_throttled(
                                "decode_budget",
                                format_args!(
                                    "closing a connection whose batch decodes past its \
                                     {limit}-byte budget ({}x max_frame_bytes of \
                                     {max_frame_bytes}); the sender's batches are too large \
                                     for this listener: {err:#}",
                                    native::budget::DECODE_BUDGET_PER_FRAME_BYTE
                                ),
                            );
                        }
                        _ => {
                            diag.warn_throttled("connection_error", err);
                        }
                    }
                }
            });
        }
    }
}

/// Writes `Reject{INTERNAL}` when `permit` is `None` (past the cap), else serves the connection.
/// Runs on the already-TLS-wrapped stream, so the reject is decodable with or without TLS (module
/// doc's "Connection limit"). The `logit.input.connections` gauge counts only connections holding
/// a permit, so it's updated here rather than in the accept loop.
#[allow(clippy::too_many_arguments)] // one small helper is clearer here than a params struct for 10 mostly-unrelated threaded-through values
async fn reject_or_serve<S: AsyncRead + AsyncWrite + Unpin + Send>(
    mut stream: S,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    sink: Fanout,
    telemetry: Telemetry,
    max_frame_bytes: u32,
    handshake_timeout: Duration,
    idle_timeout: Option<Duration>,
    shutdown: watch::Receiver<bool>,
    live_connections: crate::listener::LiveConnections,
    senders: Arc<SenderTable>,
) -> anyhow::Result<()> {
    let Some(_permit) = permit else {
        let reject = control::Reject {
            code: control::REJECT_INTERNAL,
            message: "connection limit reached, retry later".to_string(),
        };
        return write_reject(&mut stream, &reject, handshake_timeout, &telemetry).await;
    };

    // Counted out on drop, so a panic in the connection brings the gauge back down too.
    let _live = live_connections.enter();
    serve_connection(
        stream,
        sink,
        telemetry.clone(),
        max_frame_bytes,
        handshake_timeout,
        idle_timeout,
        shutdown,
        senders,
    )
    .await
}

/// What the handshake negotiated for one connection. The codec is always `CODEC_HOP_BATCH`;
/// `serve_connection` checks every data frame carries it.
struct Negotiated {
    compression: Compression,
}

fn compression_tag(compression: Compression) -> &'static str {
    match compression {
        Compression::None => "none",
        Compression::Lz4 => "lz4",
        Compression::Zstd => "zstd",
    }
}

/// The `logit.proto.errors` reason for a batch that failed to decode.
fn decode_error_reason(err: &CodecError) -> &'static str {
    match err {
        CodecError::BudgetExceeded { .. } => "decode_budget",
        _ => "malformed",
    }
}

/// Serves one accepted (and, with TLS on, already TLS-handshaken) connection to completion:
/// `Hello`/`HelloAck`, then frame, `Fanout::send`, `Ack`, until close, shutdown, or idle close.
///
/// Counts every rejection under `logit.proto.errors{reason}`; `docs/design/internal-telemetry.md`'s
/// `logit_in` section is the canonical list of reasons. A close or read error between frames is
/// not counted; one mid-header is `truncated_header`.
#[allow(clippy::too_many_arguments)] // threaded through from `reject_or_serve`, which has the same allow
async fn serve_connection<S: AsyncRead + AsyncWrite + Unpin + Send>(
    mut stream: S,
    sink: Fanout,
    telemetry: Telemetry,
    max_frame_bytes: u32,
    handshake_timeout: Duration,
    idle_timeout: Option<Duration>,
    shutdown: watch::Receiver<bool>,
    senders: Arc<SenderTable>,
) -> anyhow::Result<()> {
    let negotiated =
        match handshake(&mut stream, max_frame_bytes, handshake_timeout, &telemetry).await {
            Ok(n) => n,
            Err(err) => {
                telemetry.count("logit.proto.errors", 1.0, &[("reason", "handshake")]);
                return Err(err);
            }
        };
    // `sink` moves into `serve_frames` and drops when it returns, so a lingering close never
    // holds the graph's cancel-by-drop shutdown open.
    let ended = serve_frames(
        &mut stream,
        sink,
        &telemetry,
        &negotiated,
        max_frame_bytes,
        handshake_timeout,
        idle_timeout,
        shutdown,
        &senders,
    )
    .await;
    if !matches!(ended, Ok(Ended::PeerClosed)) {
        close_lingering(&mut stream, handshake_timeout).await;
    }
    ended.map(|_| ())
}

/// How [`serve_frames`] ended without an error.
enum Ended {
    /// The peer closed at a frame boundary: nothing is left to read, so no linger.
    PeerClosed,
    /// This listener closed the connection (shutdown, an idle close, no consumer).
    Closing,
}

/// Closes a connection this listener is ending while the peer may still be sending: shuts the
/// write side down (`close_notify` under TLS), then reads and discards until EOF or `bound`,
/// whichever comes first. Closing with unread bytes in the receive buffer sends a reset, which
/// discards the `GOING_AWAY` or trailing `Ack`s already written; a peer with a window of frames
/// in flight leaves such bytes as a matter of course. Errors are ignored: the connection is over.
async fn close_lingering<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S, bound: Duration) {
    let _ = tokio::time::timeout(bound, async {
        let _ = stream.shutdown().await;
        let mut discard = [0u8; 8192];
        while let Ok(1..) = stream.read(&mut discard).await {}
    })
    .await;
}

/// The frame loop of [`serve_connection`], after the handshake: frame, `Fanout::send`, `Ack`,
/// until close, shutdown, or idle close.
#[allow(clippy::too_many_arguments)] // threaded through from `serve_connection`
async fn serve_frames<S: AsyncRead + AsyncWrite + Unpin + Send>(
    stream: &mut S,
    sink: Fanout,
    telemetry: &Telemetry,
    negotiated: &Negotiated,
    max_frame_bytes: u32,
    handshake_timeout: Duration,
    idle_timeout: Option<Duration>,
    mut shutdown: watch::Receiver<bool>,
    senders: &SenderTable,
) -> anyhow::Result<Ended> {
    let compression = compression_tag(negotiated.compression);

    // The idle clock starts at the handshake; after this, only an `Ack` write advances it.
    let mut last_progress = tokio::time::Instant::now();

    loop {
        // Only the header read races `shutdown` (`docs/design/pipeline-graph.md`'s "Cancellation
        // points"). This explicit check catches a shutdown
        // that fired before this iteration: `changed()` fires only on a transition this receiver
        // hasn't observed. The `borrow()` `Ref` drops at the end of the statement, before any
        // `.await`.
        if *shutdown.borrow() {
            going_away(stream, "listener shutting down", handshake_timeout, telemetry).await;
            return Ok(Ended::Closing);
        }
        // `changed()`, not `wait_for`: `wait_for`'s `Ref` guard makes the `select!` future
        // `!Send` once an arm awaits afterward, and `tokio::spawn` needs `Send`. With the check
        // above they're equivalent, since `shutdown` flips false -> true only once.
        //
        // This read is also the only unbounded wait on the peer, so the idle clock lives here:
        // [`IdleBounds`] gives an absolute deadline for the first byte, a per-`read` one after.
        let header_buf = tokio::select! {
            result = read_header(stream, IdleBounds::new(last_progress, idle_timeout)) => {
                match result {
                    Ok(header_buf) => header_buf,
                    Err(HeaderReadError::Idle(idle)) => {
                        return close_idle(stream, telemetry, idle, handshake_timeout)
                            .await
                            .map(|()| Ended::Closing)
                    }
                    Err(HeaderReadError::Truncated(err)) => {
                        let reason = [("reason", "truncated_header")];
                        telemetry.count("logit.proto.errors", 1.0, &reason);
                        return Err(err);
                    }
                    Err(err) => return Err(err.into_inner()),
                }
            }
            _ = shutdown.changed() => {
                going_away(stream, "listener shutting down", handshake_timeout, telemetry).await;
                return Ok(Ended::Closing);
            }
        };
        let Some(header_buf) = header_buf else {
            // Clean close at a frame boundary: the ordinary end of a connection.
            return Ok(Ended::PeerClosed);
        };

        let (header, mut payload) =
            match read_frame_body(stream, header_buf, max_frame_bytes, idle_timeout).await {
                Ok(v) => v,
                // A stalled body is an idle close like a gap between frames: policy, not a
                // fault, so no `logit.proto.errors`.
                Err(FrameReadError::Stalled(idle)) => {
                    return close_idle(stream, telemetry, idle, handshake_timeout)
                        .await
                        .map(|()| Ended::Closing)
                }
                // Answered before the close, so the peer sees a permanent refusal rather than an
                // EOF it can't tell from a crash. Nothing of the body has been read.
                Err(FrameReadError::TooLarge(err)) => {
                    telemetry.count("logit.proto.errors", 1.0, &[("reason", "too_large")]);
                    let reject = control::Reject {
                        code: control::REJECT_FRAME_TOO_LARGE,
                        message: err.to_string(),
                    };
                    let _ = write_reject(stream, &reject, handshake_timeout, telemetry).await;
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
                Err(FrameReadError::Malformed { reason, err }) => {
                    telemetry.count("logit.proto.errors", 1.0, &[("reason", reason)]);
                    return Err(err);
                }
            };

        if header.flags & frame::FLAG_CONTROL != 0 {
            // A client sends no control message after `Hello`; one here closes the connection.
            anyhow::bail!("received an unexpected control frame after the handshake");
        }
        if header.codec != native::CODEC_HOP_BATCH {
            telemetry.count("logit.proto.errors", 1.0, &[("reason", "codec")]);
            anyhow::bail!(
                "frame codec byte {}, expected the hop batch ({})",
                header.codec,
                native::CODEC_HOP_BATCH
            );
        }

        // A fresh budget per frame, scaled to the cap this peer's frames arrive under.
        let budget = native::DecodeBudget::for_frame_cap(max_frame_bytes);
        let (batch, provenance, seq) = match native::decode_hop_batch(&mut payload, &budget) {
            Ok(decoded) => decoded,
            Err(err) => {
                telemetry.count(
                    "logit.proto.errors",
                    1.0,
                    &[("reason", decode_error_reason(&err))],
                );
                // A batch past its budget would be past it on every resend, so it's answered as
                // a frame too large: a `logit_out` drops it as permanent rather than retrying.
                if matches!(err, CodecError::BudgetExceeded { .. }) {
                    let reject = control::Reject {
                        code: control::REJECT_FRAME_TOO_LARGE,
                        message: err.to_string(),
                    };
                    let _ = write_reject(stream, &reject, handshake_timeout, telemetry).await;
                }
                return Err(anyhow::Error::new(err).context("decoding a native hop batch"));
            }
        };

        telemetry.count(
            "logit.proto.frames",
            1.0,
            &[("direction", "in"), ("compression", compression)],
        );
        telemetry.count(
            "logit.proto.frame.bytes",
            header.compressed_len as f64,
            &[("direction", "in")],
        );

        // A frame at or below its sender's mark is acknowledged on the mark alone (module doc's
        // "Deduplication").
        let resend = senders.is_resend(seq);
        if resend {
            telemetry.count(senders::RESENDS, 1.0, &[]);
        } else {
            // The ack point: `send_relayed` returns once every open downstream inbox has the
            // batch. It backfills an `origin` or `previous` the frame didn't carry and passes the
            // peer's own through untouched (`docs/adr/batch-provenance-on-delivered.md`). The
            // sender pair names this hop only and stays here; a relay's own store numbers the next.
            if !sink.send_relayed(batch, provenance).await {
                telemetry.count(
                    "logit.input.batches.dropped",
                    1.0,
                    &[("reason", "closed_consumer")],
                );
                going_away(stream, "no consumer took the batch", handshake_timeout, telemetry)
                    .await;
                return Ok(Ended::Closing);
            }
            // Only after a consumer took the batch: a batch answered `GOING_AWAY` comes back,
            // and a raised mark would drop it as a resend.
            senders.raise(seq);
        }

        // After a forwarding `send_relayed` this is the only write: a frame is never both
        // forwarded and answered `GOING_AWAY` (module doc's "Shutdown").
        if let Err(err) = write_control(stream, &control::Ack, handshake_timeout).await {
            if err.is::<WriteStalled>() {
                telemetry.count("logit.proto.errors", 1.0, &[("reason", "ack_write_stalled")]);
            }
            return Err(err);
        }
        // The only place the idle clock restarts: after the send and the ack, so time spent on a
        // full downstream is charged to this listener, not the waiting peer.
        last_progress = tokio::time::Instant::now();
    }
}

/// Writes `Reject{GOING_AWAY, why}` before this listener closes a connection, for a shutdown, an
/// idle close, and a frame no consumer took alike. `logit_out` treats `REJECT_GOING_AWAY` as transient and reconnects.
///
/// The write is bounded by `bound` and its result discarded: the connection is closing anyway,
/// and a peer that already vanished or stopped reading is not a fault ([`write_reject`] counts a
/// stall). Every caller returns `Ok(())` right after.
async fn going_away<S: AsyncWrite + Unpin>(
    stream: &mut S,
    why: &str,
    bound: Duration,
    telemetry: &Telemetry,
) {
    let reject = control::Reject { code: control::REJECT_GOING_AWAY, message: why.to_string() };
    let _ = write_reject(stream, &reject, bound, telemetry).await;
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
    bound: Duration,
) -> anyhow::Result<()> {
    going_away(stream, &format!("idle for {idle:?}"), bound, telemetry).await;
    telemetry.count("logit.input.connections.closed", 1.0, &[("reason", "idle")]);
    Ok(())
}

/// Reads `Hello` within a fresh `handshake_timeout` (not the TLS accept's remainder; module doc's
/// "Pre-`Hello` timeout") and replies `HelloAck` or `Reject`, returning an error whenever the
/// client can't proceed.
///
/// `HelloAck` carries the best shared codec, `lz4` or no compression, this listener's
/// `max_frame_bytes` (every later frame is bounded by it), and the smaller of the offered window
/// and `RECEIVER_MAX_WINDOW`. A `Hello` that fails to decode, a missing field or a window of 0
/// among the reasons, ends the connection with no reply. Each reply is written within
/// `handshake_timeout` too, a fresh bound per write.
async fn handshake<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    max_frame_bytes: u32,
    handshake_timeout: Duration,
    telemetry: &Telemetry,
) -> anyhow::Result<Negotiated> {
    let read = tokio::time::timeout(handshake_timeout, async {
        let Some(header_buf) =
            read_header(stream, None).await.map_err(HeaderReadError::into_inner)?
        else {
            anyhow::bail!("connection closed before sending Hello");
        };
        // A control message, so bounded by the control cap, not `max_frame_bytes`; an over-cap
        // header fails like any bad `Hello`. A control frame is never compressed, so its
        // `compressed_len` has the same cap, as `logit_out`'s `read_control` applies, not lz4's
        // worst case over it. A header that doesn't parse is left to `read_frame_body`.
        if let Ok(header) = FrameHeader::read(&mut Bytes::copy_from_slice(&header_buf)) {
            if header.compressed_len > control::MAX_CONTROL_MESSAGE_BYTES {
                anyhow::bail!(
                    "Hello declares {} compressed bytes, over the {}-byte control message cap",
                    header.compressed_len,
                    control::MAX_CONTROL_MESSAGE_BYTES
                );
            }
        }
        // `None`: the whole read is inside `handshake_timeout`.
        read_frame_body(stream, header_buf, control::MAX_CONTROL_MESSAGE_BYTES, None)
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
        let _ = write_reject(stream, &reject, handshake_timeout, telemetry).await;
        anyhow::bail!(
            "version mismatch: listener {}, client {}",
            control::PROTOCOL_VERSION,
            hello.version
        );
    }

    // One codec (`docs/adr/native-hop-no-compatibility.md`, decision 3).
    if !hello.codecs.contains(&native::CODEC_HOP_BATCH) {
        let reject = control::Reject {
            code: control::REJECT_NO_COMMON_CODEC,
            message: "this listener speaks the native hop codec".to_string(),
        };
        let _ = write_reject(stream, &reject, handshake_timeout, telemetry).await;
        anyhow::bail!("client offered no codec this listener speaks: {:?}", hello.codecs);
    }

    // Compression always falls back to `None`, so unlike codec it has no reject path.
    let compression = if hello.compressions.contains(&(Compression::Lz4 as u8)) {
        Compression::Lz4
    } else {
        Compression::None
    };

    let ack = control::HelloAck {
        version: control::PROTOCOL_VERSION,
        codec: native::CODEC_HOP_BATCH,
        compression: compression as u8,
        max_frame_bytes,
        window: hello.window.min(RECEIVER_MAX_WINDOW),
    };
    write_control(stream, &ack, handshake_timeout).await?;
    Ok(Negotiated { compression })
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
    /// A read failed before any byte of the header arrived.
    Io(anyhow::Error),
    /// The peer closed, or a read failed, part-way through the header.
    Truncated(anyhow::Error),
    /// An [`IdleBounds`] bound elapsed, first byte or later. Carries the configured
    /// `idle_timeout` for `close_idle` to name.
    Idle(Duration),
}

impl HeaderReadError {
    fn into_inner(self) -> anyhow::Error {
        match self {
            HeaderReadError::Io(err) | HeaderReadError::Truncated(err) => err,
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
/// `Reject{GOING_AWAY}` to a peer already writing a frame, costing `logit_out` a reconnect and a
/// resend of a batch that was on its way. The first-byte deadline firing loses nothing, since no
/// byte has been read.
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
        let n = match read {
            Ok(n) => n,
            // A TLS peer gone without `close_notify`: a close at a frame boundary, like `Ok(0)`
            // (module doc's "Close").
            Err(err) if filled == 0 && err.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None)
            }
            Err(err) if filled == 0 => return Err(HeaderReadError::Io(anyhow::Error::new(err))),
            Err(err) => {
                return Err(HeaderReadError::Truncated(anyhow::Error::new(err).context(format!(
                    "reading a frame header ({filled}/{} bytes)",
                    frame::HEADER_LEN
                ))))
            }
        };
        if n == 0 {
            if filled == 0 {
                return Ok(None);
            }
            return Err(HeaderReadError::Truncated(anyhow::anyhow!(
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
    /// A header or body that doesn't parse: `reason` is `magic`, `version`, or `malformed`.
    Malformed {
        reason: &'static str,
        err: anyhow::Error,
    },
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
            | FrameReadError::Malformed { err: e, .. } => e,
            // Unreachable from today's flattening callers, which pass no `stall` bound.
            FrameReadError::Stalled(idle) => {
                anyhow::anyhow!("a frame body stopped arriving for {idle:?}")
            }
        }
    }
}

/// Parses `header_buf` and checks its declared lengths before reading the body:
/// `uncompressed_len` against `max_frame_bytes` (capped at [`frame::MAX_SANE_UNCOMPRESSED_LEN`]),
/// and `compressed_len` against [`frame::compressed_bound`] of that, since an incompressible
/// payload at the cap grows under lz4. Then reads the body into the frame's final buffer, after a
/// copy of the header, and hands it to [`frame::read_frame_with_header`] for its CRC,
/// decompression, and length checks. The body is held once: peak heap is one
/// `HEADER_LEN + compressed_len` buffer.
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
        let reason = if header_buf[..4] != frame::MAGIC {
            "magic"
        } else if matches!(e, CodecError::Unsupported(_)) {
            "version"
        } else {
            "malformed"
        };
        FrameReadError::Malformed {
            reason,
            err: anyhow::Error::new(e).context("reading a frame header"),
        }
    })?;

    let bound = max_frame_bytes.min(frame::MAX_SANE_UNCOMPRESSED_LEN);
    let compressed_bound = frame::compressed_bound(bound);
    if header.uncompressed_len > bound || header.compressed_len > compressed_bound {
        return Err(FrameReadError::TooLarge(anyhow::anyhow!(
            "frame declares {}/{} (uncompressed/compressed) bytes, over the \
             {bound}/{compressed_bound}-byte bound",
            header.uncompressed_len,
            header.compressed_len
        )));
    }

    // A fill loop rather than `read_exact`, so each `read` carries the `stall` bound and a peer
    // that closed mid-body (`Ok(0)`, `Truncated`) stays distinct from one that stopped
    // (`Stalled`, an idle close).
    let body_len = header.compressed_len as usize;
    let mut full = BytesMut::zeroed(frame::HEADER_LEN + body_len);
    full[..frame::HEADER_LEN].copy_from_slice(&header_buf);
    let mut filled = frame::HEADER_LEN;
    while filled < full.len() {
        let read = match stall {
            Some(stall) => {
                match tokio::time::timeout(stall, stream.read(&mut full[filled..])).await {
                    Ok(read) => read,
                    Err(_elapsed) => return Err(FrameReadError::Stalled(stall)),
                }
            }
            None => stream.read(&mut full[filled..]).await,
        };
        let n = read.map_err(|e| {
            FrameReadError::Truncated(anyhow::Error::new(e).context("reading a frame body"))
        })?;
        if n == 0 {
            return Err(FrameReadError::Truncated(anyhow::anyhow!(
                "connection closed mid-body ({}/{body_len} bytes)",
                filled - frame::HEADER_LEN
            )));
        }
        filled += n;
    }

    // `read_frame_with_header` re-parses the header from the front of `full`, then splits the
    // body off it for the CRC: the buffer must hold both.
    let mut full = full.freeze();
    match frame::read_frame_with_header(&mut full) {
        Ok((header, payload)) => Ok((header, payload)),
        Err(CodecError::Malformed(msg)) if msg.contains("crc32c") => {
            Err(FrameReadError::Crc(anyhow::anyhow!("crc32c mismatch -- frame is corrupt")))
        }
        Err(err) => Err(FrameReadError::Malformed {
            reason: "malformed",
            err: anyhow::Error::new(err).context("reading a frame"),
        }),
    }
}

/// Writes and flushes one control message with [`frame::FLAG_CONTROL`] set, within `bound` (the
/// connection's `handshake_timeout`; module doc's "Bounded, flushed writes"). A write and flush not
/// finished within it fail with [`WriteStalled`]. `codec`/`compression` mean nothing on a control
/// frame (`logit_proto::native::control`), so they're always `0`/`None`.
async fn write_control<S: AsyncWrite + Unpin>(
    stream: &mut S,
    msg: &impl ControlEncode,
    bound: Duration,
) -> anyhow::Result<()> {
    let framed =
        frame::write_frame_with_flags(0, Compression::None, frame::FLAG_CONTROL, &msg.encode())?;
    // The flush sends a TLS write's queued tail (module doc's "Bounded, flushed writes").
    let written = async {
        stream.write_all(&framed).await?;
        stream.flush().await
    };
    match tokio::time::timeout(bound, written).await {
        Ok(written) => Ok(written?),
        Err(_elapsed) => Err(anyhow::Error::new(WriteStalled { what: msg.name(), bound })),
    }
}

/// A [`write_control`] that didn't finish within its bound: the peer has stopped reading, and
/// the kernel's send buffer toward it is full. A caller tells it from an I/O error with
/// `anyhow::Error::is`.
#[derive(Debug)]
struct WriteStalled {
    what: &'static str,
    bound: Duration,
}

impl std::fmt::Display for WriteStalled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} write stalled for {:?}: the peer is not reading", self.what, self.bound)
    }
}

impl std::error::Error for WriteStalled {}

/// Writes `reject` within `bound` before this listener closes a connection. A stalled write is
/// counted as `logit.proto.errors{reason="reject_write_stalled"}` and returned like any other
/// write error; every caller is about to close the connection either way.
async fn write_reject<S: AsyncWrite + Unpin>(
    stream: &mut S,
    reject: &control::Reject,
    bound: Duration,
    telemetry: &Telemetry,
) -> anyhow::Result<()> {
    let result = write_control(stream, reject, bound).await;
    if let Err(err) = &result {
        if err.is::<WriteStalled>() {
            telemetry.count("logit.proto.errors", 1.0, &[("reason", "reject_write_stalled")]);
        }
    }
    result
}

/// Lets [`write_control`] take any control message type without wrapping it in
/// `control::ControlMessage`.
trait ControlEncode {
    fn encode(&self) -> Bytes;
    /// The message's name, for [`WriteStalled`].
    fn name(&self) -> &'static str;
}
impl ControlEncode for control::Hello {
    fn encode(&self) -> Bytes {
        control::Hello::encode(self)
    }
    fn name(&self) -> &'static str {
        "Hello"
    }
}
impl ControlEncode for control::HelloAck {
    fn encode(&self) -> Bytes {
        control::HelloAck::encode(self)
    }
    fn name(&self) -> &'static str {
        "HelloAck"
    }
}
impl ControlEncode for control::Ack {
    fn encode(&self) -> Bytes {
        control::Ack::encode(self)
    }
    fn name(&self) -> &'static str {
        "Ack"
    }
}
impl ControlEncode for control::Reject {
    fn encode(&self) -> Bytes {
        control::Reject::encode(self)
    }
    fn name(&self) -> &'static str {
        "Reject"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::Provenance;
    use logit_core::{AttrMap, Event, EventBatch, LogRecord, Registry, Resource, Severity, Value};
    use logit_pipeline::test_util::{
        expect_closed, expect_still_open, recv_batch, TelemetryProbe, Totals, RECV_TIMEOUT,
    };
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

    /// Writes a control message the way a client does: unbounded, so a test's writes never
    /// depend on the listener-side bound [`write_control`] applies.
    async fn write_msg<S: AsyncWrite + Unpin>(stream: &mut S, msg: &impl ControlEncode) {
        let framed =
            frame::write_frame_with_flags(0, Compression::None, frame::FLAG_CONTROL, &msg.encode())
                .unwrap();
        stream.write_all(&framed).await.unwrap();
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

    /// The next number of one sender shared by every test in this process, so two frames a test
    /// sends never read as one resend.
    fn next_seq() -> native::SeqId {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        native::SeqId {
            id: *b"logit-in-tests!!",
            seq: NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        }
    }

    /// `batch` as one hop data frame with no provenance, under [`next_seq`].
    fn hop_frame(batch: &EventBatch, compression: Compression) -> Bytes {
        let payload = native::encode_hop_batch(batch, Provenance::default(), next_seq());
        frame::write_frame(native::CODEC_HOP_BATCH, compression, &payload).unwrap()
    }

    /// Writes [`hop_frame`]`(batch)`.
    async fn send_data_frame(stream: &mut TcpStream, batch: &EventBatch, compression: Compression) {
        stream.write_all(&hop_frame(batch, compression)).await.unwrap();
    }

    /// [`send_data_frame`] with `provenance` in the trailer.
    async fn send_data_frame_with(
        stream: &mut TcpStream,
        batch: &EventBatch,
        provenance: Provenance,
        compression: Compression,
    ) {
        send_data_frame_seq(stream, batch, provenance, next_seq(), compression).await;
    }

    /// [`send_data_frame_with`] under a chosen sender identity and sequence.
    async fn send_data_frame_seq(
        stream: &mut TcpStream,
        batch: &EventBatch,
        provenance: Provenance,
        seq: native::SeqId,
        compression: Compression,
    ) {
        let payload = native::encode_hop_batch(batch, provenance, seq);
        send_payload(stream, &payload, compression).await;
    }

    /// Frames a hand-built hop payload, for a trailer `encode_hop_batch` never writes.
    async fn send_payload(stream: &mut TcpStream, payload: &[u8], compression: Compression) {
        let framed = frame::write_frame(native::CODEC_HOP_BATCH, compression, payload).unwrap();
        stream.write_all(&framed).await.unwrap();
    }

    /// A fresh table at the default cap, for tests that call [`serve_connection`] directly.
    fn senders() -> Arc<SenderTable> {
        Arc::new(SenderTable::new(crate::DEFAULT_MAX_CONNECTIONS, Telemetry::default()))
    }

    /// [`sample_batch`] with `mark` as its one event's timestamp, so a test reads which batch
    /// arrived.
    fn batch_marked(mark: i64) -> EventBatch {
        let mut batch = sample_batch();
        batch.events[0].timestamp = mark;
        batch
    }

    async fn recv_mark(rx: &mut mpsc::Receiver<logit_pipeline::Delivered>) -> i64 {
        recv_batch(rx).await.events[0].timestamp
    }

    fn sid(id: u8, seq: u64) -> native::SeqId {
        native::SeqId { id: [id; 16], seq }
    }

    /// A connected, handshaken client.
    async fn hop_client(addr: &str) -> TcpStream {
        let mut client = connect(addr).await;
        client_hello(&mut client, vec![native::CODEC_HOP_BATCH], vec![0]).await;
        match read_control_response(&mut client).await {
            control::ControlMessage::HelloAck(ack) => {
                assert_eq!(ack.codec, native::CODEC_HOP_BATCH)
            }
            other => panic!("expected HelloAck, got {other:?}"),
        }
        client
    }

    /// Sends [`batch_marked`]`(mark)` under `seq` and waits for its `Ack`. The `Ack` follows the
    /// forward and every counter, so a test reads both right after.
    async fn send_acked(client: &mut TcpStream, mark: i64, seq: native::SeqId) {
        let batch = batch_marked(mark);
        send_data_frame_seq(client, &batch, Provenance::default(), seq, Compression::None).await;
        read_ack(client).await;
    }

    /// A running `logit_in` counting into `probe`, its address, and its consumer.
    async fn spawn_counted(
        probe: &TelemetryProbe,
        max_connections: usize,
    ) -> (String, mpsc::Receiver<logit_pipeline::Delivered>) {
        let (addr, input) = bound_input().await;
        let mut input = input
            .with_telemetry(probe.telemetry("logit_in", "logit_in", "listener"))
            .with_max_connections(max_connections);
        let (sink, rx) = fanout_into_channel(16);
        tokio::spawn(async move { input.run(sink).await });
        (addr, rx)
    }

    async fn read_ack(stream: &mut TcpStream) -> control::Ack {
        match read_control_response(stream).await {
            control::ControlMessage::Ack(ack) => ack,
            other => panic!("expected Ack, got {other:?}"),
        }
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

    /// A client's `origin`/`previous` are relayed untouched.
    #[tokio::test]
    async fn a_clients_provenance_is_relayed_untouched() {
        let (addr, input) = bound_input().await;
        let (sink, mut rx) = fanout_into_channel_with_component("logit_in_test", 16);
        let mut input = input;
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_HOP_BATCH], vec![0]).await;
        read_control_response(&mut client).await;

        let sent = Provenance {
            origin: Some(logit_core::interner::intern("remote_listener")),
            previous: Some(logit_core::interner::intern("remote_enrich")),
        };
        send_data_frame_with(&mut client, &sample_batch(), sent, Compression::None).await;
        read_ack(&mut client).await;

        let delivered = recv_delivered(&mut rx).await;
        let provenance = delivered.provenance();
        assert_eq!(provenance.origin_str(), Some("remote_listener"));
        assert_eq!(provenance.previous_str(), Some("remote_enrich"));
    }

    /// A trailer with no provenance gets this listener's id backfilled into both fields.
    #[tokio::test]
    async fn a_client_with_no_provenance_gets_this_listener_backfilled() {
        let (addr, input) = bound_input().await;
        let (sink, mut rx) = fanout_into_channel_with_component("logit_in_test", 16);
        let mut input = input;
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_HOP_BATCH], vec![0]).await;
        read_control_response(&mut client).await;

        send_data_frame_with(
            &mut client,
            &sample_batch(),
            Provenance::default(),
            Compression::None,
        )
        .await;
        read_ack(&mut client).await;

        let delivered = recv_delivered(&mut rx).await;
        let provenance = delivered.provenance();
        assert_eq!(provenance.origin_str(), Some("logit_in_test"));
        assert_eq!(provenance.previous_str(), Some("logit_in_test"));
    }

    // ---- handshake ------------------------------------------------------------------------

    #[tokio::test]
    async fn handshake_happy_path_returns_the_negotiated_compression() {
        let (addr, input) = bound_input().await;
        let (sink, _rx) = fanout_into_channel(16);
        let mut input = input;
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_HOP_BATCH], vec![0, Compression::Lz4 as u8])
            .await;
        match read_control_response(&mut client).await {
            control::ControlMessage::HelloAck(ack) => {
                assert_eq!(ack.codec, native::CODEC_HOP_BATCH);
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

        // An unknown codec, and the bare batch codec, which is the file format and not the hop's.
        for codecs in [vec![99], vec![native::CODEC_BATCH]] {
            let mut client = connect(&addr).await;
            client_hello(&mut client, codecs.clone(), vec![0]).await;
            match read_control_response(&mut client).await {
                control::ControlMessage::Reject(reject) => {
                    assert_eq!(reject.code, control::REJECT_NO_COMMON_CODEC, "{codecs:?}");
                }
                other => panic!("expected Reject for {codecs:?}, got {other:?}"),
            }
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
            codecs: vec![native::CODEC_HOP_BATCH],
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
        client_hello(&mut client, vec![native::CODEC_HOP_BATCH], vec![0]).await;
        let _ = read_control_response(&mut client).await;

        // Only a header declaring an oversized frame: a listener that waited for the body first
        // would hang here.
        let framed =
            frame::write_frame(native::CODEC_HOP_BATCH, Compression::None, &vec![0u8; 10_000])
                .unwrap();
        client.write_all(&framed[..frame::HEADER_LEN]).await.unwrap();

        match tokio::time::timeout(Duration::from_secs(2), read_control_response(&mut client))
            .await
            .expect("should answer within 2s, not hang waiting for a body")
        {
            control::ControlMessage::Reject(reject) => {
                assert_eq!(reject.code, control::REJECT_FRAME_TOO_LARGE)
            }
            other => panic!("expected Reject{{FRAME_TOO_LARGE}}, got {other:?}"),
        }
        let mut buf = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf))
            .await
            .expect("should observe a close within 2s")
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
        client_hello(&mut client, vec![native::CODEC_HOP_BATCH], vec![0]).await;
        let _ = read_control_response(&mut client).await;

        send_data_frame(&mut client, &sample_batch(), Compression::None).await;
        read_ack(&mut client).await;

        send_data_frame(&mut client, &sample_batch(), Compression::None).await;
        let mut buf = [0u8; 1];
        let premature =
            tokio::time::timeout(Duration::from_millis(150), client.read(&mut buf)).await;
        assert!(premature.is_err(), "ack for the second batch arrived before the inbox drained");

        recv_batch(&mut rx).await; // drains the first batch, freeing capacity
        read_ack(&mut client).await;
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
        client_hello(&mut client, vec![native::CODEC_HOP_BATCH], vec![0]).await;
        let _ = read_control_response(&mut client).await;

        let mut framed = BytesMut::from(
            &frame::write_frame(
                native::CODEC_HOP_BATCH,
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

        let mut probe = TelemetryProbe::with_registry(registry);
        let totals = probe
            .wait_for("the crc error counted", |t| {
                t.sum("logit.proto.errors", &[("reason", "crc")]) >= 1.0
            })
            .await;
        assert_eq!(totals.sum("logit.proto.errors", &[("reason", "crc")]), 1.0);
    }

    /// A frame well under `max_frame_bytes` whose batch decodes past 4x that cap closes the
    /// connection, counted as `decode_budget` rather than `malformed` and diagnosed under its own
    /// key.
    #[tokio::test]
    async fn a_batch_past_the_decode_budget_is_counted_and_diagnosed_as_decode_budget() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("logit_in", "logit_in", "listener");
        let diag = Diagnostics::new("logit_in");
        let listener_diag = diag.clone();
        let (addr, input) = bound_input().await;
        // A 4 KiB budget: five empty events (864 bytes each) exceed it in a ~10-byte payload.
        let mut input =
            input.with_telemetry(telemetry).with_diagnostics(diag).with_max_frame_bytes(1024);
        let (sink, _rx) = fanout_into_channel(16);
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_HOP_BATCH], vec![0]).await;
        let _ = read_control_response(&mut client).await;

        let events = (0..5).map(|i| Event::empty(i, AttrMap::new())).collect();
        let batch = EventBatch { resource: Arc::new(Resource::default()), scope: None, events };
        send_data_frame(&mut client, &batch, Compression::None).await;

        // `a_frame_past_the_decode_budget_is_answered_frame_too_large` pins the `Reject`.
        let _ = read_control_response(&mut client).await;
        let mut buf = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf))
            .await
            .expect("should observe a close within 2s")
            .unwrap();
        assert_eq!(n, 0);
        // The listener lingers after the `Reject` until this side closes.
        drop(client);

        let mut probe = TelemetryProbe::with_registry(registry);
        let totals = probe
            .wait_for("the decode_budget error counted", |t| {
                t.sum("logit.proto.errors", &[("reason", "decode_budget")]) >= 1.0
            })
            .await;
        assert_eq!(totals.sum("logit.proto.errors", &[("reason", "decode_budget")]), 1.0);
        // The diagnostic is reported by the accept loop's task after the connection returns, so
        // it can follow both the count and the close.
        logit_pipeline::test_util::wait_until("the decode_budget diagnostic", || {
            listener_diag.occurrences("decode_budget") >= 1
        })
        .await;
        assert_eq!(listener_diag.occurrences("decode_budget"), 1);
        assert_eq!(listener_diag.occurrences("connection_error"), 0);
    }

    /// A batch past its decode budget is answered `Reject{FRAME_TOO_LARGE}` before the close, so
    /// a `logit_out` drops it as `Fault::Permanent` instead of resending it under at-least-once.
    /// Counted once, as `decode_budget`.
    #[tokio::test]
    async fn a_frame_past_the_decode_budget_is_answered_frame_too_large() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("logit_in", "logit_in", "listener");
        let (addr, input) = bound_input().await;
        // A 4 KiB budget: five empty events exceed it in a ~10-byte payload.
        let mut input = input.with_telemetry(telemetry).with_max_frame_bytes(1024);
        let (sink, mut rx) = fanout_into_channel(16);
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_HOP_BATCH], vec![0]).await;
        let _ = read_control_response(&mut client).await;

        let events = (0..5).map(|i| Event::empty(i, AttrMap::new())).collect();
        let batch = EventBatch { resource: Arc::new(Resource::default()), scope: None, events };
        send_data_frame(&mut client, &batch, Compression::None).await;

        match tokio::time::timeout(Duration::from_secs(2), read_control_response(&mut client))
            .await
            .expect("the listener answers within 2s")
        {
            control::ControlMessage::Reject(reject) => {
                assert_eq!(reject.code, control::REJECT_FRAME_TOO_LARGE, "{}", reject.message);
                assert!(reject.message.contains("decode budget"), "{}", reject.message);
            }
            other => panic!("expected Reject{{FRAME_TOO_LARGE}}, got {other:?}"),
        }
        let mut buf = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf))
            .await
            .expect("the connection closes right behind the Reject")
            .unwrap();
        assert_eq!(n, 0);
        // The listener lingers after the `Reject` until this side closes.
        drop(client);

        let mut probe = TelemetryProbe::with_registry(registry);
        let totals = probe
            .wait_for("the decode_budget error counted", |t| {
                t.sum("logit.proto.errors", &[("reason", "decode_budget")]) >= 1.0
            })
            .await;
        assert_eq!(totals.sum("logit.proto.errors", &[("reason", "decode_budget")]), 1.0);
        assert!(
            !totals.has("logit.proto.errors", &[("reason", "too_large")]),
            "counted once, as decode_budget"
        );
        assert!(rx.try_recv().is_err(), "nothing was forwarded");
    }

    #[tokio::test]
    async fn the_graph_closes_after_shutdown_with_an_idle_client_still_connected() {
        let (addr, mut input) = bound_input().await;
        let (sink, mut rx) = fanout_into_channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move { input.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_HOP_BATCH], vec![0]).await;
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
        // The accept loop takes a connection's permit as it accepts it, one accept at a time and in
        // the kernel queue's order, so `_first` holds it before the next is accepted.

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
        let diag = Diagnostics::new("logit_in");
        let listener_diag = diag.clone();
        let (addr, input) = bound_input().await;
        let input = input
            .with_tls(&test_tls_settings(), &testdata_dir())
            .unwrap()
            .with_diagnostics(listener_diag)
            .with_max_connections(1)
            .with_handshake_timeout(Duration::from_millis(200));
        let (sink, _rx) = fanout_into_channel(16);
        let mut input = input;
        tokio::spawn(async move { input.run(sink).await });

        // Takes the one permit and never sends a ClientHello; held open, so only the TLS-accept
        // timeout can free the permit.
        let mut silent = connect(&addr).await;
        logit_pipeline::test_util::expect_closed(&mut silent, "a silent TLS connection").await;

        // The close alone doesn't prove the permit is back: the connection's task drops the
        // stream, then reports `connection_error`, then drops the permit, with no `.await` in
        // between. The `logit.input.connections` gauge can't say either, since it counts only a
        // connection whose TLS accept succeeded. The diagnostic is the task's last step, and on
        // this current-thread runtime the permit drop runs in the same poll.
        logit_pipeline::test_util::wait_until("the silent connection's task to end", || {
            diag.occurrences("connection_error") >= 1
        })
        .await;

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
            codecs: vec![native::CODEC_HOP_BATCH],
            compressions: vec![0],
            max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
            window: 1,
        };
        write_msg(&mut tls_stream, &hello).await;
        match read_control_response_over(&mut tls_stream).await {
            control::ControlMessage::HelloAck(ack) => {
                assert_eq!(ack.codec, native::CODEC_HOP_BATCH);
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
        // The accept loop takes a connection's permit as it accepts it, one accept at a time and in
        // the kernel queue's order, so `_first` holds it before the next is accepted.

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
    // "Closed" assertions wait up to 2s against idle deadlines of at most 500ms; "still open" ones
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
        client_hello(&mut quiet, vec![native::CODEC_HOP_BATCH], vec![0]).await;
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
            Totals::of(registry.drain(0))
                .sum("logit.input.connections.closed", &[("reason", "idle")]),
            1.0,
            "an idle close is counted"
        );
        assert_eq!(
            listener_diag.occurrences("connection_error"),
            0,
            "and never diagnosed -- an idle close returns Ok(()), so the accept loop's \
             connection_error path must not see it"
        );

        // The permit is held through the linger after the `Reject`, until `quiet` closes. Cap 1:
        // the second connection handshakes only if the permit came back.
        let mut probe = TelemetryProbe::with_registry(registry);
        drop(quiet);
        probe
            .wait_for("the lingering connection to end", |t| {
                t.gauge("logit.input.connections", &[]) == Some(0.0)
            })
            .await;
        let mut second = connect(&addr).await;
        client_hello(&mut second, vec![native::CODEC_HOP_BATCH], vec![0]).await;
        match read_control_response(&mut second).await {
            control::ControlMessage::HelloAck(ack) => {
                assert_eq!(ack.codec, native::CODEC_HOP_BATCH, "the permit came back");
            }
            other => panic!("expected HelloAck on the second connection, got {other:?}"),
        }
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
        client_hello(&mut client, vec![native::CODEC_HOP_BATCH], vec![0]).await;
        let _ = read_control_response(&mut client).await;

        send_data_frame(&mut client, &sample_batch(), Compression::None).await;
        send_data_frame(&mut client, &sample_batch(), Compression::None).await;

        tokio::time::sleep(idle * 3).await;

        recv_batch(&mut rx).await; // drains the first batch, unblocking the second's send
        read_ack(&mut client).await; // the first ack, written long before
        read_ack(&mut client).await; // and the second, after the drain
        recv_batch(&mut rx).await;

        assert!(
            !Totals::of(registry.drain(0))
                .has("logit.input.connections.closed", &[("reason", "idle")]),
            "nothing was closed as idle, so the counter was never touched"
        );
    }

    /// Every `Ack` re-arms the clock: eight frames 100ms apart outlast a 500ms timeout until they
    /// stop.
    /// The 400ms margin covers scheduler lag between an `Ack` and the next frame, the one gap
    /// that separates an ack-driven clock from an idle close.
    #[tokio::test]
    async fn the_idle_clock_restarts_from_each_ack() {
        let (addr, input) = bound_input().await;
        let mut input = input.with_idle_timeout(Some(Duration::from_millis(500)));
        let (sink, mut rx) = fanout_into_channel(16);
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_HOP_BATCH], vec![0]).await;
        let _ = read_control_response(&mut client).await;

        for _ in 0..8 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            send_data_frame(&mut client, &sample_batch(), Compression::None).await;
            read_ack(&mut client).await;
            recv_batch(&mut rx).await;
        }

        expect_reject_going_away_for_idleness(&mut client, "a connection that stopped sending")
            .await;
    }

    /// A header whose first byte lands inside the idle deadline and whose rest lands outside it
    /// is read, not rejected ([`read_header`]'s [`IdleBounds`]).
    ///
    /// A 1s `idle_timeout`, the first byte written at 700ms and the rest 500ms later. The first
    /// byte lands 300ms inside the absolute deadline, which protects the case under test: past
    /// it, the header is rejected before it starts. The rest lands 200ms past that deadline,
    /// which proves the absolute bound no longer applies once a byte has arrived, and 500ms
    /// inside the per-`read` budget (`idle_timeout` again, from the first byte), which keeps the
    /// stall bound from firing instead. A `sleep` only overshoots, so lag can eat only the 300ms
    /// and the 500ms margins; the 200ms one only grows.
    #[tokio::test]
    async fn a_frame_header_that_starts_arriving_at_the_idle_deadline_is_read_not_rejected() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("logit_in", "logit_in", "listener");
        let (addr, input) = bound_input().await;
        let mut input =
            input.with_telemetry(telemetry).with_idle_timeout(Some(Duration::from_secs(1)));
        let (sink, mut rx) = fanout_into_channel(16);
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_HOP_BATCH], vec![0]).await;
        let _ = read_control_response(&mut client).await;

        let framed = hop_frame(&sample_batch(), Compression::None);

        tokio::time::sleep(Duration::from_millis(700)).await;
        client.write_all(&framed[..1]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        client.write_all(&framed[1..]).await.unwrap();

        // A frame whose header started arriving before the deadline must be acked.
        read_ack(&mut client).await;
        assert_eq!(recv_batch(&mut rx).await.events.len(), 1);
        assert!(
            !Totals::of(registry.drain(0))
                .has("logit.input.connections.closed", &[("reason", "idle")]),
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
        client_hello(&mut client, vec![native::CODEC_HOP_BATCH], vec![0]).await;
        let _ = read_control_response(&mut client).await;

        let framed = hop_frame(&sample_batch(), Compression::None);
        client.write_all(&framed[..1]).await.unwrap();

        expect_reject_going_away_for_idleness(&mut client, "a header that stopped arriving").await;
        // `close_idle` counts after writing the `Reject` and before the stream drops, so the
        // close, not the `Reject`, is what says the count is in.
        expect_closed(&mut client, "a header that stopped arriving").await;
        assert_eq!(
            Totals::of(registry.drain(0))
                .sum("logit.input.connections.closed", &[("reason", "idle")]),
            1.0
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
        client_hello(&mut client, vec![native::CODEC_HOP_BATCH], vec![0]).await;
        let _ = read_control_response(&mut client).await;

        // A well-formed frame, of which only the header and one body byte are written.
        let framed = hop_frame(&sample_batch(), Compression::None);
        assert!(framed.len() > frame::HEADER_LEN + 1, "the fixture needs a multi-byte body");
        client.write_all(&framed[..frame::HEADER_LEN + 1]).await.unwrap();

        expect_reject_going_away_for_idleness(&mut client, "a frame body that stopped arriving")
            .await;
        // `close_idle` counts after writing the `Reject` and before the stream drops, so the
        // close, not the `Reject`, is what says the count is in.
        expect_closed(&mut client, "a frame body that stopped arriving").await;
        assert_eq!(
            Totals::of(registry.drain(0))
                .sum("logit.input.connections.closed", &[("reason", "idle")]),
            1.0,
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
        client_hello(&mut client, vec![native::CODEC_HOP_BATCH], vec![0]).await;
        let _ = read_control_response(&mut client).await;

        tokio::time::sleep(Duration::from_millis(300)).await;
        expect_still_open(
            &mut client,
            Duration::from_millis(50),
            "a quiet connection with no idle_timeout",
        )
        .await;

        send_data_frame(&mut client, &sample_batch(), Compression::None).await;
        read_ack(&mut client).await; // and still serving frames
        recv_batch(&mut rx).await;

        assert!(
            !Totals::of(registry.drain(0))
                .has("logit.input.connections.closed", &[("reason", "idle")]),
            "nothing was closed as idle, so the counter was never touched"
        );
    }

    // ---- bounded control writes, the GOING_AWAY invariant, frame bounds, and the body copy -----

    /// The `Hello` every test client below sends: the hop codec, no compression.
    fn hello() -> control::Hello {
        control::Hello {
            version: control::PROTOCOL_VERSION,
            codecs: vec![native::CODEC_HOP_BATCH],
            compressions: vec![0],
            max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
            window: 1,
        }
    }

    /// [`sample_batch`] as a data frame under a fresh [`next_seq`]: two calls are two batches.
    fn sample_frame() -> Bytes {
        hop_frame(&sample_batch(), Compression::None)
    }

    /// The encoded length of `msg` as a control frame: sizes a `duplex` to hold a set number.
    fn control_frame_len(msg: &impl ControlEncode) -> usize {
        frame::write_frame_with_flags(0, Compression::None, frame::FLAG_CONTROL, &msg.encode())
            .unwrap()
            .len()
    }

    /// A handshaken peer that keeps writing frames but never reads its `Ack`s fills this
    /// listener's send buffer, and the next `Ack` write blocks. The bound ends the connection,
    /// returns the permit, and counts the stall; unbounded, the task, its permit, and its
    /// `Fanout` clone stay held for as long as the peer does.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_ack_write_to_a_peer_that_never_reads_ends_the_connection_within_the_bound() {
        const BOUND: Duration = Duration::from_millis(300);
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("logit_in", "logit_in", "listener");

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // A 4 KiB client receive window: the listener's `Ack`s fill it, and then its own send
        // buffer, after tens of thousands of frames.
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_recv_buffer_size(4096).unwrap();
        let (client, accepted) = tokio::join!(socket.connect(addr), listener.accept());
        let mut client = client.unwrap();
        let (server, _) = accepted.unwrap();

        let limit = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = limit.clone().try_acquire_owned().unwrap();
        let live = crate::listener::LiveConnections::new(telemetry.clone());
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(reject_or_serve(
            server,
            Some(permit),
            sink,
            telemetry,
            frame::MAX_SANE_UNCOMPRESSED_LEN,
            BOUND,
            None,
            shutdown_rx,
            live.clone(),
            senders(),
        ));

        write_msg(&mut client, &hello()).await;
        let _ = read_control_response(&mut client).await;
        let (unread, mut writer) = client.into_split();
        let spam =
            tokio::spawn(async move { while writer.write_all(&sample_frame()).await.is_ok() {} });
        // Drains the listener's forwards and stamps the last one: the blocked `Ack` write
        // follows it.
        let last_forward = Arc::new(std::sync::Mutex::new(std::time::Instant::now()));
        let stamp = last_forward.clone();
        let drain = tokio::spawn(async move {
            while rx.recv().await.is_some() {
                *stamp.lock().unwrap() = std::time::Instant::now();
            }
        });

        let result = tokio::time::timeout(Duration::from_secs(30), task)
            .await
            .expect("the connection task must end once an Ack write stalls past the bound")
            .unwrap();
        let stalled_for = last_forward.lock().unwrap().elapsed();
        let err = result.expect_err("a stalled Ack write ends the connection as an error");
        assert!(
            err.to_string().contains("Ack") && err.to_string().contains("stalled"),
            "the error names the stalled Ack write: {err:#}"
        );
        assert!(
            stalled_for < BOUND + Duration::from_secs(2),
            "the task ended {stalled_for:?} after its last forward, past the {BOUND:?} bound"
        );
        assert_eq!(limit.available_permits(), 1, "the permit came back");
        assert_eq!(live.count(), 0, "the live-connection count came back to 0");

        let events = Totals::of(registry.drain(0));
        assert_eq!(events.sum("logit.proto.errors", &[("reason", "ack_write_stalled")]), 1.0);
        assert_eq!(events.gauge("logit.input.connections", &[]), Some(0.0));

        spam.abort();
        drain.abort();
        drop(unread);
    }

    /// Shutdown's `GOING_AWAY` to a peer whose receive path is full returns within the bound: a
    /// `duplex` sized for two `Ack`s, both written and never read.
    #[tokio::test]
    async fn going_away_to_a_peer_with_a_full_send_buffer_returns_within_the_bound() {
        const BOUND: Duration = Duration::from_millis(300);
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("logit_in", "logit_in", "listener");
        let ack_len = control_frame_len(&control::Ack);
        let (mut client, server) = tokio::io::duplex(2 * ack_len);
        let (sink, mut rx) = fanout_into_channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(serve_connection(
            server,
            sink,
            telemetry,
            frame::MAX_SANE_UNCOMPRESSED_LEN,
            BOUND,
            None,
            shutdown_rx,
            senders(),
        ));

        write_msg(&mut client, &hello()).await;
        let _ = read_control_response_over(&mut client).await;
        client.write_all(&sample_frame()).await.unwrap();
        client.write_all(&sample_frame()).await.unwrap();
        recv_batch(&mut rx).await;
        recv_batch(&mut rx).await;
        // Whether or not the second `Ack` is written yet: it fits the buffer, and no shutdown check
        // precedes it, so the task writes it and then meets the shutdown with the buffer full.
        shutdown_tx.send(true).unwrap();
        let result = tokio::time::timeout(BOUND + Duration::from_secs(2), task)
            .await
            .expect("going_away must return within the bound, not wait on the peer")
            .unwrap();
        assert!(result.is_ok(), "a shutdown close is policy, not a fault: {result:?}");
        assert_eq!(
            Totals::of(registry.drain(0))
                .sum("logit.proto.errors", &[("reason", "reject_write_stalled")]),
            1.0,
            "the discarded GOING_AWAY is counted"
        );
        drop(client);
    }

    /// End to end: with a peer that never reads its `Ack`s connected, a shutdown still closes
    /// the listener's `Fanout` within the runtime's 5s grace.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_graph_closes_after_shutdown_with_a_peer_that_never_reads_its_acks() {
        let (addr, input) = bound_input().await;
        let mut input = input.with_handshake_timeout(Duration::from_millis(300));
        let (sink, mut rx) = fanout_into_channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move { input.run_until_shutdown(sink, shutdown_rx).await });

        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_recv_buffer_size(4096).unwrap();
        let mut client = socket.connect(addr.parse().unwrap()).await.unwrap();
        write_msg(&mut client, &hello()).await;
        let _ = read_control_response(&mut client).await;
        let (unread, mut writer) = client.into_split();
        let spam =
            tokio::spawn(async move { while writer.write_all(&sample_frame()).await.is_ok() {} });
        // Forwards stop once the listener's `Ack` write blocks.
        while tokio::time::timeout(Duration::from_secs(1), rx.recv()).await.is_ok() {}

        shutdown_tx.send(true).unwrap();
        handle.await.unwrap().unwrap();
        let closed = tokio::time::timeout(Duration::from_secs(5), async {
            while rx.recv().await.is_some() {}
        })
        .await;
        assert!(closed.is_ok(), "every Fanout clone must be gone within the 5s grace");

        spam.abort();
        drop(unread);
    }

    /// `Reject{GOING_AWAY}` answers only a frame that wasn't forwarded, so a `logit_out` may
    /// resend it at any delivery posture. A whole frame buffered as shutdown fires races the
    /// header read against the shutdown arm; this runs the race until both outcomes have
    /// occurred.
    #[tokio::test]
    async fn a_frame_answered_with_going_away_is_never_forwarded() {
        let (mut acked, mut going_away) = (0, 0);
        for _ in 0..400 {
            let (mut client, server) = tokio::io::duplex(1 << 16);
            let (sink, mut rx) = fanout_into_channel(16);
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let task = tokio::spawn(serve_connection(
                server,
                sink,
                Telemetry::default(),
                frame::MAX_SANE_UNCOMPRESSED_LEN,
                HANDSHAKE_TIMEOUT,
                None,
                shutdown_rx,
                senders(),
            ));
            write_msg(&mut client, &hello()).await;
            let _ = read_control_response_over(&mut client).await;
            tokio::task::yield_now().await; // the task parks in its `select!`
            client.write_all(&sample_frame()).await.unwrap(); // fits the buffer: no yield
            shutdown_tx.send(true).unwrap();
            match read_control_response_over(&mut client).await {
                control::ControlMessage::Ack(_) => {
                    acked += 1;
                    assert!(rx.try_recv().is_ok(), "an acked frame was forwarded first");
                }
                control::ControlMessage::Reject(reject) => {
                    assert_eq!(reject.code, control::REJECT_GOING_AWAY);
                    going_away += 1;
                    // The listener lingers until the peer closes.
                    drop(client);
                    task.await.unwrap().unwrap();
                    assert!(
                        rx.try_recv().is_err(),
                        "a frame answered with GOING_AWAY must never be forwarded"
                    );
                }
                other => panic!("expected Ack or Reject, got {other:?}"),
            }
        }
        assert!(
            going_away > 0 && acked > 0,
            "both arms ran: {acked} acked, {going_away} going away"
        );
    }

    /// A frame no consumer takes (the one consumer closed) is answered `Reject{GOING_AWAY}` in
    /// place of its `Ack`, counted as a batch dropped for `closed_consumer`, and ends the
    /// connection as a clean close.
    #[tokio::test]
    async fn a_frame_no_consumer_takes_is_answered_going_away_and_never_acked() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("logit_in", "logit_in", "listener");
        let (mut client, server) = tokio::io::duplex(1 << 16);
        let (sink, rx) = fanout_into_channel(16);
        drop(rx);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(serve_connection(
            server,
            sink,
            telemetry,
            frame::MAX_SANE_UNCOMPRESSED_LEN,
            HANDSHAKE_TIMEOUT,
            None,
            shutdown_rx,
            senders(),
        ));
        write_msg(&mut client, &hello()).await;
        let _ = read_control_response_over(&mut client).await;

        client.write_all(&sample_frame()).await.unwrap();

        match read_control_response_over(&mut client).await {
            control::ControlMessage::Reject(reject) => {
                assert_eq!(reject.code, control::REJECT_GOING_AWAY);
                assert_eq!(reject.message, "no consumer took the batch");
            }
            other => panic!("expected Reject{{GOING_AWAY}} in place of the Ack, got {other:?}"),
        }
        let mut rest = Vec::new();
        client.read_to_end(&mut rest).await.unwrap();
        assert!(rest.is_empty(), "no Ack follows the Reject");
        // The listener lingers until the peer closes.
        drop(client);
        tokio::time::timeout(logit_pipeline::test_util::RECV_TIMEOUT, task)
            .await
            .expect("the connection should end once the peer closes")
            .unwrap()
            .expect("a refused frame is a clean close, not a connection error");
        assert_eq!(
            Totals::of(registry.drain(0))
                .sum("logit.input.batches.dropped", &[("reason", "closed_consumer")]),
            1.0
        );
    }

    // ---- control writes over TLS ----------------------------------------------------------------

    /// A tokio-rustls client and server over `tokio::io::duplex(capacity)`, handshake complete.
    /// The server sends no session tickets: over a pipe smaller than them, its accept would wait
    /// for a client that has returned from its own handshake and stopped reading.
    async fn tls_duplex(
        capacity: usize,
    ) -> (
        tokio_rustls::client::TlsStream<tokio::io::DuplexStream>,
        tokio_rustls::server::TlsStream<tokio::io::DuplexStream>,
    ) {
        let mut config =
            crate::tls::build_server_config(&test_tls_settings(), &testdata_dir(), &[]).unwrap();
        config.send_tls13_tickets = 0;
        let acceptor = TlsAcceptor::from(Arc::new(config));
        let connector = tls_connector().await;
        let (client_io, server_io) = tokio::io::duplex(capacity);
        let name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
        let (client, server) =
            tokio::join!(connector.connect(name, client_io), acceptor.accept(server_io));
        (client.unwrap(), server.unwrap())
    }

    /// Writes `bytes` and flushes them, as a TLS client must before it waits for a reply.
    async fn write_flushed<S: AsyncWrite + Unpin>(stream: &mut S, bytes: &[u8]) {
        stream.write_all(bytes).await.unwrap();
        stream.flush().await.unwrap();
    }

    /// A TLS write returns with ciphertext still queued in the session, and this listener's
    /// waiting read never sends it. Over a 16-byte pipe, a `HelloAck` or `Ack` reaches the client
    /// only because [`write_control`] flushes it.
    #[tokio::test]
    async fn hello_ack_and_ack_reach_a_tls_client_over_a_pipe_smaller_than_one_record() {
        let (mut client, server) = tls_duplex(16).await;
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        tokio::spawn(serve_connection(
            server,
            sink,
            Telemetry::default(),
            frame::MAX_SANE_UNCOMPRESSED_LEN,
            HANDSHAKE_TIMEOUT,
            None,
            shutdown_rx,
            senders(),
        ));

        let hello = frame::write_frame_with_flags(
            0,
            Compression::None,
            frame::FLAG_CONTROL,
            &hello().encode(),
        )
        .unwrap();
        write_flushed(&mut client, &hello).await;
        let reply = tokio::time::timeout(
            logit_pipeline::test_util::RECV_TIMEOUT,
            read_control_response_over(&mut client),
        )
        .await
        .expect("the HelloAck reaches the client");
        assert!(matches!(reply, control::ControlMessage::HelloAck(_)), "{reply:?}");

        write_flushed(&mut client, &sample_frame()).await;
        let reply = tokio::time::timeout(
            logit_pipeline::test_util::RECV_TIMEOUT,
            read_control_response_over(&mut client),
        )
        .await
        .expect("the Ack reaches the client");
        assert_eq!(reply, control::ControlMessage::Ack(control::Ack));
        recv_batch(&mut rx).await;
    }

    /// [`hello_ack_and_ack_reach_a_tls_client_over_a_pipe_smaller_than_one_record`] for a
    /// `Reject`, written before the connection closes.
    #[tokio::test]
    async fn a_reject_reaches_a_tls_client_over_a_pipe_smaller_than_one_record() {
        let (mut client, mut server) = tls_duplex(16).await;
        tokio::spawn(async move {
            let _ = handshake(
                &mut server,
                frame::MAX_SANE_UNCOMPRESSED_LEN,
                HANDSHAKE_TIMEOUT,
                &Telemetry::default(),
            )
            .await;
        });

        let hello = control::Hello { version: control::PROTOCOL_VERSION + 1, ..hello() };
        let hello = frame::write_frame_with_flags(
            0,
            Compression::None,
            frame::FLAG_CONTROL,
            &hello.encode(),
        )
        .unwrap();
        write_flushed(&mut client, &hello).await;
        let reply = tokio::time::timeout(
            logit_pipeline::test_util::RECV_TIMEOUT,
            read_control_response_over(&mut client),
        )
        .await
        .expect("the Reject reaches the client");
        match reply {
            control::ControlMessage::Reject(reject) => {
                assert_eq!(reject.code, control::REJECT_VERSION_MISMATCH)
            }
            other => panic!("expected Reject, got {other:?}"),
        }
    }

    /// Handshakes `client` against a spawned [`serve_connection`] with `telemetry`, and returns
    /// the task.
    fn serve_over<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
        server: S,
        telemetry: Telemetry,
    ) -> tokio::task::JoinHandle<anyhow::Result<()>> {
        let (sink, rx) = fanout_into_channel(16);
        // Held by the task, so a forward never blocks and the channel outlives it.
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        tokio::spawn(async move {
            let _keep = (rx, shutdown_tx);
            serve_connection(
                server,
                sink,
                telemetry,
                frame::MAX_SANE_UNCOMPRESSED_LEN,
                HANDSHAKE_TIMEOUT,
                None,
                shutdown_rx,
                senders(),
            )
            .await
        })
    }

    /// A TLS client gone without `close_notify` between frames reads as `UnexpectedEof`
    /// (`logit_outputs`' `stream_pins`). No frame is in flight, so it's a clean close: no error
    /// for the accept loop's `connection_error`, and no `logit.proto.errors`.
    #[tokio::test]
    async fn a_tls_client_gone_without_close_notify_between_frames_is_a_clean_close() {
        let registry = Registry::new();
        let (mut client, server) = tls_duplex(64 * 1024).await;
        let task = serve_over(server, registry.telemetry_for("logit_in", "logit_in", "listener"));

        write_msg(&mut client, &hello()).await;
        let _ = read_control_response_over(&mut client).await;
        client.write_all(&sample_frame()).await.unwrap();
        assert_eq!(
            read_control_response_over(&mut client).await,
            control::ControlMessage::Ack(control::Ack)
        );
        drop(client);

        let result = tokio::time::timeout(logit_pipeline::test_util::RECV_TIMEOUT, task)
            .await
            .expect("the connection ends")
            .unwrap();
        assert!(result.is_ok(), "{result:?}");
        assert!(!Totals::of(registry.drain(0)).has("logit.proto.errors", &[]));
    }

    /// A peer gone part-way through a frame header, over TLS (`UnexpectedEof`) or plaintext
    /// (`Ok(0)`), is an error counted as `truncated_header`.
    #[tokio::test]
    async fn a_client_gone_mid_header_is_an_error_counted_as_a_truncated_header() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("logit_in", "logit_in", "listener");

        let (mut tls_client, tls_server) = tls_duplex(64 * 1024).await;
        let tls_task = serve_over(tls_server, telemetry.clone());
        write_msg(&mut tls_client, &hello()).await;
        let _ = read_control_response_over(&mut tls_client).await;
        write_flushed(&mut tls_client, &sample_frame()[..10]).await;
        drop(tls_client);

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let task = serve_over(server, telemetry);
        write_msg(&mut client, &hello()).await;
        let _ = read_control_response_over(&mut client).await;
        client.write_all(&sample_frame()[..10]).await.unwrap();
        drop(client);

        for (what, task) in [("tls", tls_task), ("plaintext", task)] {
            let result = tokio::time::timeout(logit_pipeline::test_util::RECV_TIMEOUT, task)
                .await
                .expect("the connection ends")
                .unwrap();
            let err = result.expect_err(what);
            assert!(format!("{err:#}").contains("10/24 bytes"), "{what}: {err:#}");
        }
        let totals = Totals::of(registry.drain(0));
        assert_eq!(totals.sum("logit.proto.errors", &[("reason", "truncated_header")]), 2.0);
        assert_eq!(totals.sum("logit.proto.errors", &[]), 2.0);
    }

    /// An xorshift-generated printable-ASCII string: lz4 finds almost no 4-byte match in it, so
    /// its lz4 frame is larger than its payload.
    fn incompressible_text(len: usize) -> String {
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                char::from(b'!' + (x % 94) as u8)
            })
            .collect()
    }

    /// A one-event batch whose hop encoding is at most `target` bytes under any sequence, within
    /// a few bytes of it, and whose message is [`incompressible_text`].
    fn incompressible_batch_encoding_to(target: usize) -> EventBatch {
        let batch_with = |len: usize| {
            let mut batch = sample_batch();
            batch.events[0].log.as_mut().unwrap().message = Value::str(incompressible_text(len));
            batch
        };
        let mut len = target;
        loop {
            // The largest sequence is the longest uvarint, so a real one encodes no longer.
            let widest = native::SeqId { id: [0; 16], seq: u64::MAX };
            let encoded =
                native::encode_hop_batch(&batch_with(len), Provenance::default(), widest).len();
            if encoded <= target {
                return batch_with(len);
            }
            len -= encoded - target;
        }
    }

    /// A payload a few bytes under `max_frame_bytes` that lz4 expands past it still relays: the
    /// compressed length is bounded by lz4's worst case over the cap, not by the cap itself.
    #[tokio::test]
    async fn an_incompressible_batch_just_under_the_cap_relays_under_lz4() {
        const CAP: u32 = 64 * 1024;
        let (addr, input) = bound_input().await;
        let mut input = input.with_max_frame_bytes(CAP);
        let (sink, mut rx) = fanout_into_channel(16);
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_HOP_BATCH], vec![Compression::Lz4 as u8])
            .await;
        match read_control_response(&mut client).await {
            control::ControlMessage::HelloAck(ack) => {
                assert_eq!(ack.compression, Compression::Lz4 as u8)
            }
            other => panic!("expected HelloAck, got {other:?}"),
        }

        let batch = incompressible_batch_encoding_to(CAP as usize - 8);
        let framed = hop_frame(&batch, Compression::Lz4);
        let compressed_len = framed.len() - frame::HEADER_LEN;
        assert!(
            compressed_len > CAP as usize,
            "precondition: the lz4 frame ({compressed_len} bytes) is larger than the cap"
        );
        client.write_all(&framed).await.unwrap();

        read_ack(&mut client).await; // the frame is acked, not rejected
        let relayed = recv_batch(&mut rx).await;
        assert_eq!(relayed.events.len(), 1);
    }

    /// A header declaring a `compressed_len` one past lz4's worst case over `max_frame_bytes`
    /// is answered `Reject{FRAME_TOO_LARGE}` on the header alone, counted, and closed.
    #[tokio::test]
    async fn a_frame_over_the_compressed_bound_is_answered_frame_too_large() {
        const CAP: u32 = 64;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("logit_in", "logit_in", "listener");
        let (addr, input) = bound_input().await;
        let mut input = input.with_max_frame_bytes(CAP).with_telemetry(telemetry);
        let (sink, mut rx) = fanout_into_channel(16);
        tokio::spawn(async move { input.run(sink).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_HOP_BATCH], vec![Compression::Lz4 as u8])
            .await;
        let _ = read_control_response(&mut client).await;

        // A real lz4 frame's header, its `compressed_len` (bytes 16..20) raised to one past
        // `CAP + CAP / 255 + 16`, sent with no body.
        let mut header = BytesMut::from(
            &frame::write_frame(native::CODEC_HOP_BATCH, Compression::Lz4, &[0u8; CAP as usize])
                .unwrap()[..frame::HEADER_LEN],
        );
        let over = CAP + CAP / 255 + 16 + 1;
        header[16..20].copy_from_slice(&over.to_le_bytes());
        client.write_all(&header).await.unwrap();

        match tokio::time::timeout(Duration::from_secs(2), read_control_response(&mut client))
            .await
            .expect("the listener answers within 2s, not after waiting for a body")
        {
            control::ControlMessage::Reject(reject) => {
                assert_eq!(reject.code, control::REJECT_FRAME_TOO_LARGE, "{}", reject.message)
            }
            other => panic!("expected Reject{{FRAME_TOO_LARGE}}, got {other:?}"),
        }
        let mut buf = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf))
            .await
            .expect("the connection closes right behind the Reject")
            .unwrap();
        assert_eq!(n, 0);
        let mut probe = TelemetryProbe::with_registry(registry);
        let totals = probe
            .wait_for("the too_large error counted", |t| {
                t.sum("logit.proto.errors", &[("reason", "too_large")]) >= 1.0
            })
            .await;
        assert_eq!(totals.sum("logit.proto.errors", &[("reason", "too_large")]), 1.0);
        assert!(rx.try_recv().is_err());
    }

    /// A header partly read when shutdown fires is discarded with the connection: the client
    /// gets `GOING_AWAY`, the rest of its frame is never read, and nothing is forwarded.
    #[tokio::test]
    async fn a_partial_header_at_shutdown_is_discarded_and_the_connection_closes() {
        let (addr, mut input) = bound_input().await;
        let (sink, mut rx) = fanout_into_channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move { input.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        client_hello(&mut client, vec![native::CODEC_HOP_BATCH], vec![0]).await;
        let _ = read_control_response(&mut client).await;
        let frame = sample_frame();
        client.write_all(&frame[..10]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await; // the listener reads those 10 bytes

        shutdown_tx.send(true).unwrap();
        match read_control_response(&mut client).await {
            control::ControlMessage::Reject(reject) => {
                assert_eq!(reject.code, control::REJECT_GOING_AWAY)
            }
            other => panic!("expected Reject{{GOING_AWAY}}, got {other:?}"),
        }
        let _ = client.write_all(&frame[10..]).await; // may fail: the listener has closed
        let mut buf = [0u8; 1];
        let after = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf))
            .await
            .expect("the connection closes right behind the Reject");
        assert!(matches!(after, Ok(0) | Err(_)), "expected a close, got {after:?}");

        handle.await.unwrap().unwrap();
        let closed = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
        assert!(
            closed.expect("the fanout closes within 2s").is_none(),
            "the partial frame was never forwarded"
        );
    }

    /// Something that isn't `logit` on this port (an HTTP request, a syslog line) fails the
    /// header's magic check. That check comes before the length bound and the body allocation, so
    /// the connection closes at once, counted as a handshake error. `tests/robustness.rs`'s
    /// `a_stray_client_allocates_nothing_sized_from_its_bytes` pins the allocation side.
    #[tokio::test]
    async fn a_stray_http_or_syslog_client_is_reset_before_any_allocation() {
        const STRAYS: [(&str, &[u8]); 2] = [
            ("http", b"GET /metrics HTTP/1.1\r\nHost: logit\r\n\r\n"),
            ("syslog", b"<13>1 2026-09-25T00:00:00Z host app - - - hello world\n"),
        ];

        for (name, bytes) in STRAYS {
            let mut header = [0u8; frame::HEADER_LEN];
            header.copy_from_slice(&bytes[..frame::HEADER_LEN]);
            let (_client, mut server) = tokio::io::duplex(64);
            let result =
                read_frame_body(&mut server, header, frame::MAX_SANE_UNCOMPRESSED_LEN, None).await;
            match result {
                Err(FrameReadError::Malformed { reason, err }) => {
                    assert_eq!(reason, "magic", "{name}: {err:#}");
                }
                Err(other) => panic!("{name}: expected Malformed, got {:#}", other.into_inner()),
                Ok(_) => panic!("{name}: stray bytes parsed as a frame"),
            }
        }

        let registry = Registry::new();
        let telemetry = registry.telemetry_for("logit_in", "logit_in", "listener");
        let (addr, input) = bound_input().await;
        // Far longer than the test waits: a close comes from the magic check, not a timeout.
        let mut input =
            input.with_telemetry(telemetry).with_handshake_timeout(Duration::from_secs(30));
        let (sink, _rx) = fanout_into_channel(16);
        tokio::spawn(async move { input.run(sink).await });
        for (name, bytes) in STRAYS {
            let mut stray = connect(&addr).await;
            stray.write_all(bytes).await.unwrap();
            let mut buf = [0u8; 64];
            let read = tokio::time::timeout(Duration::from_secs(2), stray.read(&mut buf))
                .await
                .unwrap_or_else(|_| panic!("{name}: expected a close within 2s"));
            assert!(matches!(read, Ok(0) | Err(_)), "{name}: expected a close, got {read:?}");
        }
        let mut probe = TelemetryProbe::with_registry(registry);
        let totals = probe
            .wait_for("both strays counted as handshake errors", |t| {
                t.sum("logit.proto.errors", &[("reason", "handshake")]) >= 2.0
            })
            .await;
        assert_eq!(totals.sum("logit.proto.errors", &[("reason", "handshake")]), 2.0);
    }

    /// A `Hello` is read against the control-message cap, not `max_frame_bytes`: a header
    /// declaring a body one byte over it closes the connection before any body is read, counted as
    /// a handshake error like any other bad `Hello`. No valid control message reaches the cap (a
    /// `Hello`'s lists are capped at 16 entries each), so it only bounds a malformed length.
    #[tokio::test]
    async fn a_hello_is_bounded_by_the_control_message_cap() {
        let cap = control::MAX_CONTROL_MESSAGE_BYTES as usize;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("logit_in", "logit_in", "listener");
        let (addr, input) = bound_input().await;
        // Far longer than the test waits: the close must come from the cap, not a timeout.
        let mut input =
            input.with_telemetry(telemetry).with_handshake_timeout(Duration::from_secs(30));
        let (sink, _rx) = fanout_into_channel(16);
        tokio::spawn(async move { input.run(sink).await });

        let over = frame::write_frame_with_flags(
            0,
            Compression::None,
            frame::FLAG_CONTROL,
            &vec![0u8; cap + 1],
        )
        .unwrap();
        let mut over_cap = connect(&addr).await;
        // The header alone: a listener that waited for the body would hold the connection open.
        over_cap.write_all(&over[..frame::HEADER_LEN]).await.unwrap();
        expect_closed(&mut over_cap, "a Hello header over the control-message cap").await;

        let mut probe = TelemetryProbe::with_registry(registry);
        let totals = probe
            .wait_for("the oversized Hello counted", |t| {
                t.sum("logit.proto.errors", &[("reason", "handshake")]) >= 1.0
            })
            .await;
        assert_eq!(totals.sum("logit.proto.errors", &[("reason", "handshake")]), 1.0);
    }

    /// A `Hello` offering a window of 0 is malformed: the connection closes with no reply,
    /// counted as a handshake error. `Hello::encode` asserts a nonzero window in debug builds, so
    /// the test patches the window field's one-byte value on the wire.
    #[tokio::test]
    async fn a_hello_with_a_window_of_zero_is_a_protocol_error() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("logit_in", "logit_in", "listener");
        let (addr, input) = bound_input().await;
        // Far longer than the test waits: the close must come from the decode, not a timeout.
        let mut input =
            input.with_telemetry(telemetry).with_handshake_timeout(Duration::from_secs(30));
        let (sink, _rx) = fanout_into_channel(16);
        tokio::spawn(async move { input.run(sink).await });

        // `window` is the last field, `tag len value`, and 1 encodes as the single byte 1.
        let mut payload = BytesMut::from(&hello().encode()[..]);
        let last = payload.len() - 1;
        assert_eq!(payload[last - 2..], [5, 1, 1]);
        payload[last] = 0;
        let framed =
            frame::write_frame_with_flags(0, Compression::None, frame::FLAG_CONTROL, &payload)
                .unwrap();
        let mut client = connect(&addr).await;
        client.write_all(&framed).await.unwrap();
        expect_closed(&mut client, "a Hello with a window of 0").await;

        let mut probe = TelemetryProbe::with_registry(registry);
        let totals = probe
            .wait_for("the malformed Hello counted", |t| {
                t.sum("logit.proto.errors", &[("reason", "handshake")]) >= 1.0
            })
            .await;
        assert_eq!(totals.sum("logit.proto.errors", &[("reason", "handshake")]), 1.0);
    }

    /// A control frame is never compressed, so a `Hello` header whose `compressed_len` is one
    /// past the control-message cap is refused on the header, as `logit_out`'s `read_control`
    /// refuses a reply, not admitted up to lz4's worst case over the cap.
    #[tokio::test]
    async fn a_hello_whose_compressed_length_is_over_the_control_message_cap_is_refused() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("logit_in", "logit_in", "listener");
        let (addr, input) = bound_input().await;
        // Far longer than the test waits: the close must come from the cap, not a timeout.
        let mut input =
            input.with_telemetry(telemetry).with_handshake_timeout(Duration::from_secs(30));
        let (sink, _rx) = fanout_into_channel(16);
        tokio::spawn(async move { input.run(sink).await });

        // A real lz4 `Hello` header with `compressed_len` (bytes 16..20) raised past the cap.
        let mut header = BytesMut::from(
            &frame::write_frame_with_flags(
                0,
                Compression::Lz4,
                frame::FLAG_CONTROL,
                &hello().encode(),
            )
            .unwrap()[..frame::HEADER_LEN],
        );
        let over = control::MAX_CONTROL_MESSAGE_BYTES + 1;
        assert!(over <= frame::compressed_bound(control::MAX_CONTROL_MESSAGE_BYTES));
        header[16..20].copy_from_slice(&over.to_le_bytes());

        let mut client = connect(&addr).await;
        client.write_all(&header).await.unwrap();
        expect_closed(&mut client, "a Hello whose compressed length is over the cap").await;

        let mut probe = TelemetryProbe::with_registry(registry);
        let totals = probe
            .wait_for("the oversized Hello counted", |t| {
                t.sum("logit.proto.errors", &[("reason", "handshake")]) >= 1.0
            })
            .await;
        assert_eq!(totals.sum("logit.proto.errors", &[("reason", "handshake")]), 1.0);
    }

    /// A connection turned away at the cap holds no permit: with the cap at 1, the rejected
    /// connection still open, and the first one closed, a third handshakes.
    #[tokio::test]
    async fn a_past_the_cap_connection_holds_no_permit() {
        let mut probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("logit_in", "logit_in", "listener");
        let (addr, input) = bound_input().await;
        let mut input = input.with_telemetry(telemetry).with_max_connections(1);
        let (sink, _rx) = fanout_into_channel(16);
        tokio::spawn(async move { input.run(sink).await });

        let mut first = connect(&addr).await;
        client_hello(&mut first, vec![native::CODEC_HOP_BATCH], vec![0]).await;
        let _ = read_control_response(&mut first).await;

        let mut rejected = connect(&addr).await;
        match read_control_response(&mut rejected).await {
            control::ControlMessage::Reject(reject) => {
                assert_eq!(reject.code, control::REJECT_INTERNAL)
            }
            other => panic!("expected Reject{{INTERNAL}}, got {other:?}"),
        }

        drop(first);
        // The gauge counts only permit holders. The first task drops its gauge guard and then its
        // permit with no `.await` between, so on this current-thread runtime a 0 means the permit
        // is back.
        probe
            .wait_for("the first connection to end", |t| {
                t.gauge("logit.input.connections", &[]) == Some(0.0)
            })
            .await;
        let mut third = connect(&addr).await;
        client_hello(&mut third, vec![native::CODEC_HOP_BATCH], vec![0]).await;
        match read_control_response(&mut third).await {
            control::ControlMessage::HelloAck(_) => {}
            other => {
                panic!("expected HelloAck with the rejected connection still open, got {other:?}")
            }
        }
        drop(rejected);
    }

    // ---- deduplication ------------------------------------------------------------------------
    //
    // "Not forwarded" is read off FIFO order: every `Ack` follows its forward, so the next batch
    // on the channel after a resend's `Ack` is the later batch sent behind it.

    /// A sender that lost an `Ack` and resends the batch on a new connection gets an `Ack`, and
    /// the batch reaches the consumer once.
    #[tokio::test]
    async fn a_resend_after_a_lost_ack_is_forwarded_once() {
        let mut probe = TelemetryProbe::new();
        let (addr, mut rx) = spawn_counted(&probe, crate::DEFAULT_MAX_CONNECTIONS).await;

        let mut first = hop_client(&addr).await;
        send_acked(&mut first, 1, sid(7, 1)).await;
        assert_eq!(recv_mark(&mut rx).await, 1);
        drop(first);

        let mut second = hop_client(&addr).await;
        send_acked(&mut second, 1, sid(7, 1)).await;
        send_acked(&mut second, 2, sid(7, 2)).await;
        assert_eq!(recv_mark(&mut rx).await, 2, "the resend was not forwarded");
        assert_eq!(probe.sum("logit.input.batches.resends", &[]), 1.0);
    }

    /// A data frame without a complete sender pair is malformed: it's never forwarded, the
    /// connection ends, and `logit.proto.errors` counts it. So is a data frame under the bare
    /// batch codec.
    #[tokio::test]
    async fn a_frame_without_a_complete_pair_is_a_protocol_error() {
        let mut probe = TelemetryProbe::new();
        let (addr, mut rx) = spawn_counted(&probe, crate::DEFAULT_MAX_CONNECTIONS).await;

        // A bare payload, then a hand-built trailer: its length (one byte, under 128) and fields.
        let with_trailer = |mark: i64, trailer: &[u8]| {
            let mut payload = native::encode_batch(&batch_marked(mark)).to_vec();
            payload.push(u8::try_from(trailer.len()).unwrap());
            payload.extend_from_slice(trailer);
            payload
        };
        let id_15 = [[3u8, 15].as_slice(), &[9; 15], &[4, 1, 1]].concat();
        let seq_0 = [[3u8, 16].as_slice(), &[9; 16], &[4, 1, 0]].concat();
        let tag_4_twice = [[3u8, 16].as_slice(), &[9; 16], &[4, 1, 1, 4, 1, 2]].concat();
        let payloads = [
            with_trailer(1, &[]),
            with_trailer(2, &id_15),
            with_trailer(3, &seq_0),
            with_trailer(4, &tag_4_twice),
        ];

        for (n, payload) in payloads.iter().enumerate() {
            let mut client = hop_client(&addr).await;
            send_payload(&mut client, payload, Compression::None).await;
            expect_closed(&mut client, "a frame without a complete pair").await;
            probe
                .wait_for("the malformed frame counted", |t| {
                    t.sum("logit.proto.errors", &[("reason", "malformed")]) >= (n + 1) as f64
                })
                .await;
        }

        let mut bare = connect(&addr).await;
        client_hello(&mut bare, vec![native::CODEC_HOP_BATCH], vec![0]).await;
        read_control_response(&mut bare).await;
        let framed = frame::write_frame(
            native::CODEC_BATCH,
            Compression::None,
            &native::encode_batch(&batch_marked(5)),
        )
        .unwrap();
        bare.write_all(&framed).await.unwrap();
        expect_closed(&mut bare, "a data frame under the bare batch codec").await;
        let totals = probe
            .wait_for("the codec error counted", |t| {
                t.sum("logit.proto.errors", &[("reason", "codec")]) >= 1.0
            })
            .await;
        assert_eq!(totals.sum("logit.proto.errors", &[("reason", "malformed")]), 4.0);
        assert_eq!(totals.sum("logit.proto.errors", &[("reason", "magic")]), 0.0);
        assert_eq!(totals.sum("logit.proto.errors", &[("reason", "codec")]), 1.0);
        assert!(rx.try_recv().is_err(), "no malformed frame was forwarded");
    }

    /// Sends `framed` as the first data frame after a handshake, waits for the connection to
    /// close and `reason` to be counted, and checks no other parse reason was.
    async fn expect_frame_error(framed: &[u8], reason: &str) {
        let mut probe = TelemetryProbe::new();
        let (addr, mut rx) = spawn_counted(&probe, crate::DEFAULT_MAX_CONNECTIONS).await;
        let mut client = hop_client(&addr).await;
        client.write_all(framed).await.unwrap();
        expect_closed(&mut client, reason).await;
        let totals = probe
            .wait_for("the frame error counted", |t| {
                t.sum("logit.proto.errors", &[("reason", reason)]) >= 1.0
            })
            .await;
        for other in ["magic", "version", "malformed", "crc", "handshake"] {
            let want = if other == reason { 1.0 } else { 0.0 };
            assert_eq!(totals.sum("logit.proto.errors", &[("reason", other)]), want, "{other}");
        }
        assert!(rx.try_recv().is_err(), "no bad frame was forwarded");
    }

    fn raw_frame(compression: Compression, payload: &[u8]) -> Vec<u8> {
        frame::write_frame(native::CODEC_HOP_BATCH, compression, payload).unwrap().to_vec()
    }

    /// A data frame whose header magic isn't `LGIT` is counted as `magic`.
    #[tokio::test]
    async fn a_data_frame_with_bad_magic_counts_magic() {
        let mut framed = raw_frame(Compression::None, &native::encode_batch(&batch_marked(1)));
        framed[..4].copy_from_slice(b"XXXX");
        expect_frame_error(&framed, "magic").await;
    }

    /// A data frame with an unknown frame version is counted as `version`.
    #[tokio::test]
    async fn a_data_frame_with_an_unknown_version_counts_version() {
        let mut framed = raw_frame(Compression::None, &native::encode_batch(&batch_marked(1)));
        framed[4..6].copy_from_slice(&2u16.to_le_bytes());
        expect_frame_error(&framed, "version").await;
    }

    /// A frame whose lz4 body is garbage under a valid CRC is counted as `malformed`, not `crc`.
    /// The frame is written uncompressed, so the CRC covers the garbage, then its compression
    /// byte (header offset 9) is flipped to lz4.
    #[tokio::test]
    async fn a_data_frame_with_an_undecompressable_body_counts_malformed() {
        let mut framed = raw_frame(Compression::None, &[0xFF; 64]);
        framed[9] = Compression::Lz4 as u8;
        expect_frame_error(&framed, "malformed").await;
    }

    /// A sender that restarts without a spool comes back under a new identity from 1, and its
    /// first batch is new, not a resend of the old identity's 1.
    #[tokio::test]
    async fn a_new_identity_after_a_restart_is_not_a_resend() {
        let mut probe = TelemetryProbe::new();
        let (addr, mut rx) = spawn_counted(&probe, crate::DEFAULT_MAX_CONNECTIONS).await;

        let mut client = hop_client(&addr).await;
        for seq in 1..=3 {
            send_acked(&mut client, seq as i64, sid(1, seq)).await;
        }
        send_acked(&mut client, 4, sid(2, 1)).await;
        for mark in 1..=4 {
            assert_eq!(recv_mark(&mut rx).await, mark);
        }
        assert_eq!(probe.sum("logit.input.batches.resends", &[]), 0.0);
    }

    /// A number below the mark that was never forwarded (a batch the sender dropped, replayed by
    /// its spool) is acknowledged and not forwarded; the mark rule doesn't tell it from a resend.
    #[tokio::test]
    async fn a_dropped_then_replayed_number_is_acknowledged_and_not_forwarded() {
        let mut probe = TelemetryProbe::new();
        let (addr, mut rx) = spawn_counted(&probe, crate::DEFAULT_MAX_CONNECTIONS).await;

        let mut client = hop_client(&addr).await;
        send_acked(&mut client, 2, sid(1, 2)).await;
        send_acked(&mut client, 1, sid(1, 1)).await;
        send_acked(&mut client, 3, sid(1, 3)).await;
        assert_eq!(recv_mark(&mut rx).await, 2);
        assert_eq!(recv_mark(&mut rx).await, 3, "the number below the mark was not forwarded");
        assert_eq!(probe.sum("logit.input.batches.resends", &[]), 1.0);
    }

    /// At a cap of one connection the table holds one identity: a second evicts the first, and
    /// the first's resend is then forwarded, a duplicate rather than a loss.
    #[tokio::test]
    async fn an_evicted_senders_resend_is_forwarded() {
        let mut probe = TelemetryProbe::new();
        let (addr, mut rx) = spawn_counted(&probe, 1).await;

        let mut client = hop_client(&addr).await;
        send_acked(&mut client, 1, sid(1, 1)).await;
        send_acked(&mut client, 2, sid(2, 1)).await;
        send_acked(&mut client, 3, sid(1, 1)).await;
        for mark in 1..=3 {
            assert_eq!(recv_mark(&mut rx).await, mark);
        }
        let totals = probe.poll();
        // The second identity evicts the first, and the first's forwarded resend evicts the
        // second in turn.
        assert_eq!(totals.sum("logit.input.senders.evicted", &[]), 2.0);
        assert_eq!(totals.gauge("logit.input.senders", &[]), Some(1.0));
        assert_eq!(totals.sum("logit.input.batches.resends", &[]), 0.0);
    }

    /// Two `logit_in` components keep separate tables: a batch one forwarded is new to the other.
    #[tokio::test]
    async fn two_logit_in_components_do_not_share_a_table() {
        let mut probe_a = TelemetryProbe::new();
        let mut probe_b = TelemetryProbe::new();
        let (addr_a, mut rx_a) = spawn_counted(&probe_a, crate::DEFAULT_MAX_CONNECTIONS).await;
        let (addr_b, mut rx_b) = spawn_counted(&probe_b, crate::DEFAULT_MAX_CONNECTIONS).await;

        let mut client_a = hop_client(&addr_a).await;
        send_acked(&mut client_a, 1, sid(1, 1)).await;
        let mut client_b = hop_client(&addr_b).await;
        send_acked(&mut client_b, 1, sid(1, 1)).await;
        assert_eq!(recv_mark(&mut rx_a).await, 1);
        assert_eq!(recv_mark(&mut rx_b).await, 1);
        assert_eq!(probe_a.sum("logit.input.batches.resends", &[]), 0.0);
        assert_eq!(probe_b.sum("logit.input.batches.resends", &[]), 0.0);
    }

    // ---- the send window ------------------------------------------------------------------------

    /// [`client_hello`] offering `window`.
    async fn hello_offering(stream: &mut TcpStream, window: u32) {
        let hello = control::Hello {
            version: control::PROTOCOL_VERSION,
            codecs: vec![native::CODEC_HOP_BATCH],
            compressions: vec![0],
            max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
            window,
        };
        write_msg(stream, &hello).await;
    }

    /// [`send_data_frame_seq`] of [`batch_marked`]`(mark)` under `sid(9, mark)`, without
    /// waiting for its `Ack`.
    async fn pipeline_marked(client: &mut TcpStream, mark: i64) {
        let batch = batch_marked(mark);
        let seq = sid(9, mark as u64);
        send_data_frame_seq(client, &batch, Provenance::default(), seq, Compression::None).await;
    }

    #[tokio::test]
    async fn hello_ack_answers_the_offered_window_clamped_to_the_receiver_maximum() {
        let (addr, mut input) = bound_input().await;
        let (sink, _rx) = fanout_into_channel(16);
        tokio::spawn(async move { input.run(sink).await });

        for (offered, answered) in [(1, 1), (32, 32), (1024, 1024), (1025, 1024), (u32::MAX, 1024)]
        {
            let mut client = connect(&addr).await;
            hello_offering(&mut client, offered).await;
            match read_control_response(&mut client).await {
                control::ControlMessage::HelloAck(ack) => {
                    assert_eq!(ack.window, answered, "offered {offered}")
                }
                other => panic!("expected HelloAck, got {other:?}"),
            }
        }
    }

    /// Frames written back to back are forwarded and answered one at a time, in the order they
    /// arrived: the k-th `Ack` follows the k-th frame's forward. A one-slot consumer holds the
    /// second frame's forward until the first is taken, and its `Ack` with it.
    #[tokio::test]
    async fn pipelined_frames_are_acked_in_frame_order() {
        let (addr, mut input) = bound_input().await;
        let (sink, mut rx) = fanout_into_channel(1);
        tokio::spawn(async move { input.run(sink).await });
        let mut client = connect(&addr).await;
        hello_offering(&mut client, 8).await;
        let _ = read_control_response(&mut client).await;

        for mark in 1..=3 {
            pipeline_marked(&mut client, mark).await;
        }
        read_ack(&mut client).await;
        // The second frame waits on the full consumer, so its `Ack` hasn't been written.
        let mut early = [0u8; 1];
        let pending = tokio::time::timeout(Duration::from_millis(50), client.read(&mut early));
        assert!(pending.await.is_err(), "no second Ack before the second forward");

        for mark in 1..=3 {
            assert_eq!(recv_mark(&mut rx).await, mark);
            if mark < 3 {
                read_ack(&mut client).await;
            }
        }
    }

    /// A shutdown with frames buffered behind the one being forwarded: the listener answers that
    /// frame, writes `GOING_AWAY`, and lingers over the unread frames, so the peer reads both
    /// answers and then an orderly EOF, not a reset.
    #[tokio::test]
    async fn a_shutdown_with_frames_still_buffered_reaches_the_peer_as_going_away_not_a_reset() {
        let mut probe = TelemetryProbe::new();
        let (addr, input) = bound_input().await;
        let mut input = input.with_telemetry(probe.telemetry("logit_in", "logit_in", "listener"));
        let (sink, mut rx) = fanout_into_channel(1);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move { input.run_until_shutdown(sink, shutdown_rx).await });
        let mut client = connect(&addr).await;
        hello_offering(&mut client, 8).await;
        let _ = read_control_response(&mut client).await;

        // The first batch fills the one-slot consumer; the second's forward then waits on it,
        // with three more frames unread behind it.
        send_acked(&mut client, 1, sid(9, 1)).await;
        for mark in 2..=5 {
            pipeline_marked(&mut client, mark).await;
        }
        // Counted once decoded, before the forward that waits on the full consumer.
        probe
            .wait_for("the second frame's forward to start", |t| {
                t.sum("logit.proto.frames", &[("direction", "in")]) == 2.0
            })
            .await;
        shutdown_tx.send(true).unwrap();
        handle.await.unwrap().unwrap();
        assert_eq!(recv_mark(&mut rx).await, 1, "frees the slot the second forward waits on");

        read_ack(&mut client).await;
        match read_control_response(&mut client).await {
            control::ControlMessage::Reject(reject) => {
                assert_eq!(reject.code, control::REJECT_GOING_AWAY);
            }
            other => panic!("expected Reject{{GOING_AWAY}}, got {other:?}"),
        }
        let mut rest = Vec::new();
        let read = tokio::time::timeout(RECV_TIMEOUT, client.read_to_end(&mut rest)).await;
        let read = read.expect("the listener shuts its side down");
        assert!(read.is_ok(), "an orderly close, not a reset: {read:?}");
        assert!(rest.is_empty(), "nothing after GOING_AWAY: {rest:?}");
        assert_eq!(recv_mark(&mut rx).await, 2);
        assert!(rx.try_recv().is_err(), "the frames behind it were never forwarded");
    }

    /// The consumer closes while a window of frames is in flight: the frame it didn't take is
    /// answered `GOING_AWAY`, the frames behind it are never read, and the peer reads an orderly
    /// close.
    #[tokio::test]
    async fn a_frame_no_consumer_took_mid_window_is_answered_going_away_and_nothing_after_it_is_forwarded(
    ) {
        let mut probe = TelemetryProbe::new();
        let (addr, input) = bound_input().await;
        let mut input = input.with_telemetry(probe.telemetry("logit_in", "logit_in", "listener"));
        let (sink, rx) = fanout_into_channel(1);
        tokio::spawn(async move { input.run(sink).await });
        let mut client = connect(&addr).await;
        hello_offering(&mut client, 8).await;
        let _ = read_control_response(&mut client).await;

        for mark in 1..=4 {
            pipeline_marked(&mut client, mark).await;
        }
        // The first batch takes the one slot; the second's forward waits on it.
        read_ack(&mut client).await;
        drop(rx);

        match read_control_response(&mut client).await {
            control::ControlMessage::Reject(reject) => {
                assert_eq!(reject.code, control::REJECT_GOING_AWAY);
                assert_eq!(reject.message, "no consumer took the batch");
            }
            other => panic!("expected Reject{{GOING_AWAY}}, got {other:?}"),
        }
        let mut rest = Vec::new();
        let read = tokio::time::timeout(RECV_TIMEOUT, client.read_to_end(&mut rest)).await;
        assert!(read.expect("an orderly close").is_ok() && rest.is_empty());

        let totals = probe.poll();
        assert_eq!(
            totals.sum("logit.input.batches.dropped", &[("reason", "closed_consumer")]),
            1.0
        );
        assert_eq!(
            totals.sum("logit.proto.frames", &[("direction", "in")]),
            2.0,
            "the frames behind the refused one are never read"
        );
    }

    /// A connection lingering after `GOING_AWAY`, on a peer that neither reads nor closes, holds
    /// its permit but not its `Fanout` clone: the graph closes during the linger.
    #[tokio::test]
    async fn the_graph_closes_while_a_connection_lingers_after_going_away() {
        let mut probe = TelemetryProbe::new();
        let (addr, input) = bound_input().await;
        // A linger that outlasts every wait below.
        let mut input = input
            .with_telemetry(probe.telemetry("logit_in", "logit_in", "listener"))
            .with_handshake_timeout(Duration::from_secs(60));
        let (sink, mut rx) = fanout_into_channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move { input.run_until_shutdown(sink, shutdown_rx).await });
        let mut client = connect(&addr).await;
        hello_offering(&mut client, 8).await;
        let _ = read_control_response(&mut client).await;
        send_acked(&mut client, 1, sid(9, 1)).await;
        assert_eq!(recv_mark(&mut rx).await, 1);

        shutdown_tx.send(true).unwrap();
        handle.await.unwrap().unwrap();
        let closed = tokio::time::timeout(RECV_TIMEOUT, rx.recv()).await;
        assert!(closed.expect("the graph closes within the bound").is_none());
        assert_eq!(
            probe.poll().gauge("logit.input.connections", &[]),
            Some(1.0),
            "the connection is still lingering"
        );

        drop(client);
        probe
            .wait_for("the linger to end on the peer's close", |t| {
                t.gauge("logit.input.connections", &[]) == Some(0.0)
            })
            .await;
    }
}
