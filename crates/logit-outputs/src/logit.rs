//! `logit_out`: the native `logit`-to-`logit` sink (`docs/design/wire-protocol.md`'s "Connection
//! protocol", `docs/adr/native-transport-handshake-and-ack.md`). One TCP (optionally TLS)
//! connection; a `Hello`/`HelloAck` negotiates version, codec, compression, and the peer's
//! `max_frame_bytes`, and window; then one native frame per batch. `send` is `submit` then
//! `await_ack`: one frame, and its `Ack`.
//!
//! **One attempt per `send`, `submit`, or `await_ack`** ([`crate::Output`]'s contract).
//! `write_loop` owns retry and races each call against a timeout, so the connection is
//! `take()`n into a local before any write or ack read and put back only when the call leaves it
//! usable. A cancelled call drops the local, closing the connection rather than leaving
//! `self.stream` partway through a frame, and every frame in flight on it with it.
//!
//! **Send window** (`docs/adr/native-hop-send-window.md`, decision 5). `Hello` offers the
//! configured window and the connection uses `max(1, min(offered, answered))`. `logit_in`
//! answers a connection's frames in the order they arrive, so an `Ack` names nothing and answers
//! the oldest frame in flight. [`Output::submit`] writes a frame without waiting for its `Ack`;
//! [`Output::await_ack`] reads the next one.
//! - `submit` fails `Ambiguous`, and drops the connection, when the caller's `in_flight` differs
//!   from the connection's count, so a drifted count can't let an `Ack` deliver a batch never
//!   sent.
//! - With frames in flight the probe below is skipped (it would consume an `Ack`'s byte), and
//!   the frame is written in chunks, each `write` and the final flush bounded by the request
//!   timeout: a frame making progress on a slow link never trips it, and a receiver parked on an
//!   earlier frame does. A stall or a write error there marks the connection `broken` and
//!   returns an error with no [`Fault`]: the acks already owed are still read, nothing more is
//!   written, and the connection is dropped once none is owed. A stall can leave the frame
//!   part-written; `logit_in` then sees a clean close at a frame boundary, a truncated header, or
//!   a truncated frame, by where the stall fell.
//! - With nothing in flight the write is the "Write phase" below, as for `send`.
//!
//! **Lazy connect.** `LogitOutput::new` never touches the network: a peer that isn't up yet is
//! not a config error. A failed connection is dropped; the next `send` reconnects. The dial is
//! `crate::stream::connect`, shared with the pooled line sinks; the handshake after it is this
//! module's.
//!
//! **Fault classification.** A fault says what the peer can hold, and `logit_in` holds a batch
//! only once it has read the whole frame and checked its CRC: it has no partial decode, and it
//! forwards before it acks. So the one `Ambiguous` window is the ack wait.
//! - Connect, TLS, `Hello` write, or `HelloAck` read failure: `Clean`.
//! - A `HelloAck` that doesn't answer the `Hello` (another protocol version, or a codec or
//!   compression never offered): `Permanent`. The peer answers the same `Hello` the same way.
//! - A `Reject`: [`reject_is_permanent`] decides, not where it arrives.
//!   `REJECT_VERSION_MISMATCH`/`REJECT_NO_COMMON_CODEC`/`REJECT_FRAME_TOO_LARGE` would recur
//!   identically, so `Permanent`. Any other code (`REJECT_INTERNAL`, the peer at its connection
//!   cap; `REJECT_GOING_AWAY`, the peer shutting down, closing an idle connection, or finding no
//!   consumer to take a frame; a code a newer peer adds) is transient: `Clean` at the handshake.
//!   After a data frame left, `REJECT_GOING_AWAY` is still `Clean`: `logit_in` writes it only for
//!   a frame it didn't forward (its module doc's "Shutdown"), so the batch never landed and is
//!   resent at any delivery posture. Any other transient code there is `Ambiguous`.
//! - A batch over the sanity cap or the peer's `max_frame_bytes`, or a compressed frame over
//!   `frame::compressed_bound` of that: `Permanent`, nothing written, a pooled connection kept.
//! - **Write phase**: any failure before the frame is completely written and flushed (a write
//!   `Err` or `Ok(0)`, a failed flush) is `Clean`, with the `io::Error` kept, and the connection
//!   is dropped. Bytes of the frame may have left the host, but not all of them, so the peer can't
//!   hold the batch. The flush is part of the phase because a TLS write can return with the
//!   frame's tail still queued in the session, and a waiting ack read doesn't send it. ADR
//!   `sink-send-path-and-attempt-accounting`, decisions 6 and 7, has the one residual (a TLS 1.3
//!   `KeyUpdate` queued behind the frame).
//! - **Ack wait**: a timeout, a read error, an EOF, or a message other than `Ack` or `Reject`:
//!   `Ambiguous`, and the connection is dropped. A `Reject{GOING_AWAY}` there is `Clean` for every
//!   frame still unanswered: `logit_in` writes it only for a frame it didn't forward and reads
//!   nothing after it.
//!
//! **Delivery posture.** The default, `at_least_once` (`docs/adr/delivery-semantics.md`, item 5),
//! retries an `Ambiguous` attempt. Every frame carries the sender identity and sequence the
//! sink's store gave the batch, and a resend reuses them, so `logit_in` recognizes a resend at or
//! below its sender's mark, acks it, and doesn't forward it again
//! (`docs/adr/native-hop-identity-and-sequence.md`). A resend still reaches `logit_in`'s consumers
//! twice after a `logit_in` restart (the marks are in memory), for a sender evicted from
//! `logit_in`'s table, behind a load balancer that sends the resend to another `logit_in`, and
//! when a connection that ended mid-window still holds the first copy as the resend arrives
//! (`docs/known-gaps.md`, "A resend can race the frames an ended connection still holds").
//!
//! **Close.** `Output::flush`, called once after the last batch, shuts the pooled connection
//! down, which under TLS sends `close_notify`. A connection dropped after a failed or cancelled
//! attempt closes without one, which `logit_in` reads as a close when it falls between frames.
//!
//! **Pooled-connection probe.** Before the first write on a connection inherited from an earlier
//! batch with nothing in flight, the stream gets one non-consuming `poll_read`
//! (`crate::tls::poll_pending_close`, whose doc says why never a cancellable `timeout(read)`).
//! An EOF, or unsolicited bytes (on this protocol, a `Reject{GOING_AWAY}` from a shutdown or a
//! `logit_in` `idle_timeout:`, `docs/adr/idle-connection-timeout.md`), drops it and reconnects
//! before anything leaves the host, the `Clean` path. A FIN arriving between the probe and the write is `Ambiguous` when the
//! write completes first and the ack wait meets it, and `Clean` when the write or flush fails.
//!
//! **Telemetry** (`docs/design/internal-telemetry.md`'s `logit_out` section):
//! `logit.output.requests{class}` counts every `submit` failure that carries a `Fault` and
//! every `await_ack` result, once, as `ok` or the failure's `Fault` (`clean`/`ambiguous`/`permanent`), so a `send` counts
//! once; a cancelled call isn't counted. `logit.output.reconnects` counts every validated
//! handshake after the first, probe-driven ones included. `logit.output.ack.duration` times each
//! ack wait alone. The gauges `logit.output.in_flight` and `logit.output.window` hold the frames
//! awaiting an `Ack` and the negotiated window, and read 0 and 1 after every connection drop.
//! A drift counts once, in `submit`; a `Permanent` past the head is counted when it becomes the
//! head.

use crate::Output;
use anyhow::Context;
use bytes::{Bytes, BytesMut};
use logit_core::{Diagnostics, EventBatch, Provenance, Telemetry};
use logit_pipeline::{BatchContext, Fault, SeqId};
use logit_proto::frame::{self, Compression};
use logit_proto::native::{self, control};
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Bounds the TCP connect, the TLS handshake, the `HelloAck` wait, the ack wait, the shutdown in
/// `Output::flush`, and, with frames in flight, each chunk of a data-frame write and its flush,
/// each separately. The `Hello`, and a data frame written with nothing in flight, have only
/// `write_loop`'s retry budget, the outer bound.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// `crate::tls::TlsClientSettings`, re-exported to match `crate::otlp`'s path.
pub use crate::tls::TlsClientSettings;

// Shared with the pooled line sinks: the dial (`crate::stream`), the plain-or-TLS stream
// erasure, and the probe of a reused connection.
use crate::count_request;
use crate::stream::{Dial, Target, TlsTarget};
use crate::tls::{poll_pending_close, AsyncStream, PendingClose};

/// A live, handshaken connection.
struct Conn {
    stream: Box<dyn AsyncStream>,
    /// The peer's `HelloAck.max_frame_bytes`. A larger batch is rejected locally as `Permanent`
    /// rather than sent and rejected by the peer.
    peer_max_frame_bytes: u32,
    /// The negotiated compression; `None` when the peer doesn't support what was offered.
    compression: Compression,
    /// The negotiated window: `max(1, min(offered, answered))`.
    window: usize,
    /// Frames written whole and not yet acknowledged. `logit_in` answers a connection's frames
    /// in the order they arrive, so the next `Ack` answers the oldest of them.
    in_flight: usize,
    /// A write failed or stalled with frames in flight. Nothing more is written on it; it's
    /// kept for the acks already owed and dropped once none is.
    broken: bool,
    /// Where the drop resets `logit.output.in_flight` and `logit.output.window`.
    telemetry: Telemetry,
}

impl Drop for Conn {
    /// Every drop, a cancelled call's included, leaves nothing in flight and no negotiated
    /// window, so the gauges say so rather than keep the dropped connection's last values.
    fn drop(&mut self) {
        self.telemetry.gauge("logit.output.in_flight", 0.0, &[]);
        self.telemetry.gauge("logit.output.window", 1.0, &[]);
    }
}

pub struct LogitOutput {
    endpoint: String,
    /// Offered in `Hello`; [`Conn::compression`] may still be `None`.
    compression: Compression,
    timeout: Duration,
    /// Offered in `Hello`; [`Conn::window`] may be smaller.
    window: u32,
    tls: Option<TlsTarget>,
    diag: Diagnostics,
    telemetry: Telemetry,
    stream: Option<Conn>,
    /// Set by the first handshake that passes validation, so only later ones count as
    /// `logit.output.reconnects`.
    has_connected_once: bool,
    /// The next batch's provenance, set by `Output::observe_batch` once per batch and carried in
    /// the hop trailer (`docs/adr/batch-provenance-on-delivered.md`).
    pending_provenance: Provenance,
    /// The next batch's sender identity and sequence from the sink's store, set by
    /// `Output::observe_batch`. Every attempt at one batch carries the same pair; cleared once an
    /// attempt succeeds, so a `send` without its own `observe_batch` fails rather than go out
    /// under the last batch's pair, which `logit_in` would read as a resend and not forward.
    pending_seq: Option<SeqId>,
    /// Set when a submit with frames in flight found the connection holding a different count.
    /// The connection was dropped, and the next `await_ack` fails `Ambiguous` rather than read
    /// no connection as nothing outstanding.
    drifted: bool,
}

/// The window offered when none is configured, `default_logit_out_window` in `logit-config`.
const DEFAULT_WINDOW: u32 = 32;

impl LogitOutput {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            compression: Compression::None,
            timeout: DEFAULT_TIMEOUT,
            window: DEFAULT_WINDOW,
            tls: None,
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
            stream: None,
            has_connected_once: false,
            pending_provenance: Provenance::default(),
            pending_seq: None,
            drifted: false,
        }
    }

    /// Sets the window offered in `Hello` (default 32): how many frames may be in flight before
    /// the oldest is acknowledged. The connection uses the smaller of this and the peer's answer,
    /// and at least 1.
    pub fn with_window(mut self, window: u32) -> Self {
        self.window = window;
        self
    }

    /// Offers `compression` in `Hello` alongside `None`; the peer may still choose `None`.
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Sets the connect, handshake, ack-wait, and close timeout (default 10s).
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Turns on TLS (`tls:` in config). Presence alone turns it on: `endpoint` is a bare
    /// `host:port` with no scheme to select it, unlike `otlp_out`. Warns when
    /// `insecure_skip_verify` is set, as `TlsClientConfig::insecure_skip_verify`'s doc promises.
    /// Errors when the endpoint's host is no valid TLS server name (`TlsTarget::new`), so a bad
    /// endpoint fails startup and not every batch.
    pub fn with_tls(
        mut self,
        settings: &TlsClientSettings,
        base_dir: &Path,
    ) -> anyhow::Result<Self> {
        if settings.insecure_skip_verify {
            self.diag.warn(
                "tls.insecure_skip_verify is set -- the connection is encrypted, but this \
                 output will accept any certificate the peer presents, self-signed or otherwise",
            );
        }
        let config = crate::tls::build_client_config(settings, base_dir)?;
        self.tls = Some(TlsTarget::new("logit_out", &self.endpoint, config)?);
        Ok(self)
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// [`LogitOutput::dial`], then [`LogitOutput::handshake`].
    async fn connect_and_handshake(&mut self) -> anyhow::Result<Conn> {
        let stream = self.dial().await?;
        self.handshake(stream).await
    }

    /// The shared dial (`crate::stream::connect`): a TCP connect and, with TLS, a handshake, each
    /// bounded by `self.timeout`, every failure `Fault::Clean`. `&mut self` because
    /// `&LogitOutput` isn't `Send`: its pooled stream isn't `Sync`.
    async fn dial(&mut self) -> anyhow::Result<Box<dyn AsyncStream>> {
        let dial = Dial {
            target: Target::Tcp { endpoint: &self.endpoint, tls: self.tls.as_ref() },
            connect_timeout: self.timeout,
            sink: "logit_out",
            nodelay: true,
        };
        crate::stream::connect(&dial).await
    }

    /// `Hello`/`HelloAck` over a dialed `stream`. The `HelloAck` wait is bounded by
    /// `self.timeout`; the `Hello` write only by `write_loop`'s remaining retry budget. Counts
    /// `logit.output.reconnects` from the second handshake that passes [`validate_hello_ack`] on.
    async fn handshake(&mut self, mut stream: Box<dyn AsyncStream>) -> anyhow::Result<Conn> {
        let hello = control::Hello {
            version: control::PROTOCOL_VERSION,
            codecs: vec![native::CODEC_HOP_BATCH],
            compressions: vec![Compression::None as u8, self.compression as u8],
            max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
            window: self.window,
        };
        write_control(&mut stream, &hello)
            .await
            .context("writing Hello to logit_in")
            .context(Fault::Clean)?;

        let response = tokio::time::timeout(self.timeout, read_control(&mut stream))
            .await
            .context("timed out waiting for HelloAck")
            .context(Fault::Clean)?
            .context(Fault::Clean)?;

        let ack = match response {
            control::ControlMessage::HelloAck(ack) => ack,
            control::ControlMessage::Reject(reject) => {
                // Nothing of this batch was written, so a transient reject is `Clean`.
                let fault =
                    if reject_is_permanent(reject.code) { Fault::Permanent } else { Fault::Clean };
                return Err(anyhow::anyhow!(
                    "logit_in rejected this connection (code {}): {}",
                    reject.code,
                    reject.message
                ))
                .context(fault);
            }
            other => {
                return Err(anyhow::anyhow!("expected HelloAck or Reject, got {other:?}"))
                    .context(Fault::Clean)
            }
        };

        let compression = validate_hello_ack(&ack, &hello).context(Fault::Permanent)?;
        // A `HelloAck.window` of 0 reads as 1.
        let window = self.window.min(ack.window).max(1) as usize;
        self.telemetry.gauge("logit.output.window", window as f64, &[]);

        if self.has_connected_once {
            self.telemetry.count("logit.output.reconnects", 1.0, &[]);
        } else {
            self.has_connected_once = true;
        }

        Ok(Conn {
            stream,
            peer_max_frame_bytes: ack.max_frame_bytes,
            compression,
            window,
            in_flight: 0,
            broken: false,
            telemetry: self.telemetry.clone(),
        })
    }
}

/// Checks that `ack` answers `hello`: the same protocol version, and a codec and compression
/// `hello` offered. Returns the compression to frame with. A peer that answers one `Hello` this
/// way answers every identical one the same way, so the caller's verdict is `Permanent`, like a
/// version or codec `Reject`.
fn validate_hello_ack(
    ack: &control::HelloAck,
    hello: &control::Hello,
) -> anyhow::Result<Compression> {
    if ack.version != hello.version {
        anyhow::bail!(
            "logit_in answered HelloAck version {}, and this sink speaks version {}",
            ack.version,
            hello.version
        );
    }
    if !hello.codecs.contains(&ack.codec) {
        anyhow::bail!(
            "logit_in acked codec {}, which this sink's Hello didn't offer ({:?})",
            ack.codec,
            hello.codecs
        );
    }
    match compression_from_u8(ack.compression) {
        Some(compression) if hello.compressions.contains(&ack.compression) => Ok(compression),
        _ => anyhow::bail!(
            "logit_in acked compression {}, which this sink's Hello didn't offer ({:?})",
            ack.compression,
            hello.compressions
        ),
    }
}

/// The compressions a frame can be written with (`frame::write_frame_with_flags` rejects zstd).
fn compression_from_u8(b: u8) -> Option<Compression> {
    match b {
        0 => Some(Compression::None),
        1 => Some(Compression::Lz4),
        _ => None,
    }
}

fn compression_tag(compression: Compression) -> &'static str {
    match compression {
        Compression::None => "none",
        Compression::Lz4 => "lz4",
        Compression::Zstd => "zstd",
    }
}

/// Whether retrying the identical `Hello` or frame would hit this `Reject` again, the only case
/// that justifies `Fault::Permanent`. Any other code, including one a newer peer adds
/// (`Reject.code` is a u16 so reasons can be added without a version bump), is transient.
fn reject_is_permanent(code: u16) -> bool {
    matches!(
        code,
        control::REJECT_VERSION_MISMATCH
            | control::REJECT_NO_COMMON_CODEC
            | control::REJECT_FRAME_TOO_LARGE
    )
}

impl LogitOutput {
    /// `Output::submit`'s body: writes one frame without waiting for its `Ack`. Every failure with
    /// `in_flight == 0` carries a [`Fault`]; a write failure or stall with frames in flight
    /// carries none (the module doc's "Send window").
    async fn submit_frame(
        &mut self,
        batch: &EventBatch,
        ctx: BatchContext,
        seq: SeqId,
        in_flight: usize,
    ) -> anyhow::Result<()> {
        // Encoded once per attempt, before touching the network, so an oversized batch never
        // connects.
        let payload = native::encode_hop_batch(batch, ctx.provenance, seq);
        if payload.len() as u64 > frame::MAX_SANE_UNCOMPRESSED_LEN as u64 {
            self.warn_at_head(
                in_flight,
                format!(
                    "batch encodes to {} bytes, over the {}-byte sanity cap -- dropping it \
                     rather than ever attempting to send it",
                    payload.len(),
                    frame::MAX_SANE_UNCOMPRESSED_LEN
                ),
            );
            return Err(anyhow::anyhow!("batch too large to send")).context(Fault::Permanent);
        }

        if in_flight == 0 {
            self.drifted = false;
        }
        let held = self.stream.as_ref().map_or(0, |conn| conn.in_flight);
        if in_flight != held {
            // The caller's count can't be trusted to match an `Ack` to a batch, so neither can
            // this connection: dropped, and the next `await_ack` fails rather than deliver.
            self.stream = None;
            self.drifted = in_flight > 0;
            self.record_in_flight(0);
            return Err(anyhow::anyhow!(
                "submitted with {in_flight} frame(s) in flight, and the connection holds {held}"
            ))
            .context(Fault::Ambiguous);
        }

        let mut conn = match self.stream.take() {
            // The module doc's "Send window": nothing more is written on a broken connection.
            Some(conn) if conn.broken => {
                self.stream = Some(conn);
                return Err(anyhow::anyhow!(
                    "this connection stopped taking frames with {in_flight} in flight"
                ));
            }
            // With frames in flight the probe would consume a byte of an `Ack`.
            Some(conn) if in_flight > 0 => conn,
            // The module doc's "Pooled-connection probe": nothing is written yet, so replacing
            // the connection is the `Clean` path, counted as a reconnect.
            Some(mut conn) => {
                let mut probe = [0u8; 1];
                let pending = poll_pending_close(&mut *conn.stream, &mut probe).await;
                match pending {
                    PendingClose::Open => conn,
                    // `Bytes` is treated like `Eof`: unprompted, `logit_in` only ever writes
                    // `Reject{GOING_AWAY}`, and the probe consumed a byte of it, so the stream
                    // can't be read coherently again.
                    _closed => {
                        drop(conn);
                        self.connect_and_handshake().await?
                    }
                }
            }
            None => self.connect_and_handshake().await?,
        };
        let bound = conn.peer_max_frame_bytes.min(frame::MAX_SANE_UNCOMPRESSED_LEN);
        if payload.len() as u32 > bound {
            // Only this batch doesn't fit; the connection is kept and nothing is written.
            self.stream = Some(conn);
            self.warn_at_head(
                in_flight,
                format!(
                    "batch encodes to {} bytes, over this connection's {bound}-byte bound",
                    payload.len()
                ),
            );
            return Err(anyhow::anyhow!("batch too large for this connection"))
                .context(Fault::Permanent);
        }

        let framed = match frame::write_frame_with_flags(
            native::CODEC_HOP_BATCH,
            conn.compression,
            0,
            &payload,
        ) {
            Ok(framed) => framed,
            Err(err) => {
                self.stream = Some(conn);
                return Err(err).context(Fault::Permanent);
            }
        };
        // The peer bounds `compressed_len` by `frame::compressed_bound`, lz4's worst case over
        // the payload bound; checked here so this side never sends a frame the peer refuses.
        let compressed_len = framed.len() - frame::HEADER_LEN;
        if compressed_len as u64 > frame::compressed_bound(bound) as u64 {
            self.stream = Some(conn);
            self.warn_at_head(
                in_flight,
                format!(
                    "batch compresses to {compressed_len} bytes, over this connection's {}-byte \
                     compressed bound",
                    frame::compressed_bound(bound)
                ),
            );
            return Err(anyhow::anyhow!("compressed batch too large for this connection"))
                .context(Fault::Permanent);
        }

        if in_flight == 0 {
            // The module doc's "Write phase": `Clean` on any failure, and the connection is
            // dropped. Flushed outside `self.timeout`, which a large frame on a slow link can
            // outlast; the retry budget bounds it, as it bounds the write.
            let written = async {
                conn.stream.write_all(&framed).await.context("writing a frame to logit_in")?;
                conn.stream.flush().await.context("flushing a frame to logit_in")
            };
            if let Err(err) = written.await {
                return Err(err).context(Fault::Clean);
            }
        } else if let Err(err) = write_with_progress(&mut conn.stream, &framed, self.timeout).await
        {
            // The acks already owed still arrive; `await_ack` reads them on this connection.
            conn.broken = true;
            self.stream = Some(conn);
            return Err(err);
        }

        self.telemetry.count(
            "logit.proto.frames",
            1.0,
            &[("direction", "out"), ("compression", compression_tag(conn.compression))],
        );
        self.telemetry.count(
            "logit.proto.frame.bytes",
            framed.len() as f64,
            &[("direction", "out")],
        );
        conn.in_flight += 1;
        self.record_in_flight(conn.in_flight);
        self.stream = Some(conn);
        Ok(())
    }

    /// `Output::await_ack`'s body: reads the `Ack` for the oldest frame in flight. Every `Err`
    /// carries a [`Fault`] and leaves no connection.
    async fn read_ack(&mut self) -> anyhow::Result<()> {
        // Taken into a local for the read, so a cancelled wait drops the connection.
        let mut conn = match self.stream.take() {
            Some(conn) if conn.in_flight > 0 => conn,
            pooled => {
                self.stream = pooled;
                return Ok(());
            }
        };

        let ack_timer = self.telemetry.timer("logit.output.ack.duration");
        let ack_result = tokio::time::timeout(self.timeout, read_control(&mut conn.stream)).await;
        drop(ack_timer);

        let err = match ack_result {
            Ok(Ok(control::ControlMessage::Ack(_))) => {
                conn.in_flight -= 1;
                self.record_in_flight(conn.in_flight);
                // A broken connection is kept only for the acks it still owes.
                if !(conn.broken && conn.in_flight == 0) {
                    self.stream = Some(conn);
                }
                return Ok(());
            }
            Ok(Ok(control::ControlMessage::Reject(reject))) => {
                // `logit_in` writes `GOING_AWAY` only for a frame it didn't forward (shutdown, an
                // idle close, or no consumer taking it), and reads nothing after writing it, so
                // in place of an `Ack` it means no frame still unanswered here landed: `Clean`.
                // Any other transient code after a frame left is `Ambiguous`.
                let fault = if reject_is_permanent(reject.code) {
                    Fault::Permanent
                } else if reject.code == control::REJECT_GOING_AWAY {
                    Fault::Clean
                } else {
                    Fault::Ambiguous
                };
                anyhow::anyhow!(
                    "logit_in rejected this connection (code {}): {}",
                    reject.code,
                    reject.message
                )
                .context(fault)
            }
            Ok(Ok(other)) => {
                anyhow::anyhow!("expected Ack, got {other:?}").context(Fault::Ambiguous)
            }
            Ok(Err(err)) => err.context("reading the ack").context(Fault::Ambiguous),
            Err(_elapsed) => {
                anyhow::anyhow!("timed out waiting for the ack").context(Fault::Ambiguous)
            }
        };
        self.record_in_flight(0);
        Err(err)
    }

    /// Warns `frame_too_large` for the batch at the head only; one past it is warned once it
    /// becomes the head.
    fn warn_at_head(&mut self, in_flight: usize, message: String) {
        if in_flight == 0 {
            self.diag.warn_throttled("frame_too_large", message);
        }
    }

    /// Sets the `logit.output.in_flight` gauge.
    fn record_in_flight(&self, in_flight: usize) {
        self.telemetry.gauge("logit.output.in_flight", in_flight as f64, &[]);
    }
}

/// The write-chunk size with frames in flight: each chunk's write must accept something within
/// the request timeout.
const WRITE_CHUNK: usize = 64 * 1024;

/// Writes and flushes `framed` with frames already in flight, bounding progress rather than the
/// whole write: each `write` call, at most [`WRITE_CHUNK`] bytes, and the final flush must
/// complete within `bound`. A large frame on a slow link makes progress and never trips it; a
/// receiver parked on an earlier frame stops reading and does. Errors carry no [`Fault`].
async fn write_with_progress(
    stream: &mut Box<dyn AsyncStream>,
    framed: &[u8],
    bound: Duration,
) -> anyhow::Result<()> {
    let stalled = || anyhow::anyhow!("a frame write to logit_in made no progress for {bound:?}");
    let mut written = 0;
    while written < framed.len() {
        let end = framed.len().min(written + WRITE_CHUNK);
        let n = tokio::time::timeout(bound, stream.write(&framed[written..end]))
            .await
            .map_err(|_elapsed| stalled())?
            .context("writing a frame to logit_in")?;
        if n == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::WriteZero))
                .context("writing a frame to logit_in");
        }
        written += n;
    }
    tokio::time::timeout(bound, stream.flush())
        .await
        .map_err(|_elapsed| stalled())?
        .context("flushing a frame to logit_in")
}

#[async_trait::async_trait]
impl Output for LogitOutput {
    /// Records `ctx.provenance` and `seq` for `send`. `write_loop` calls this once per batch,
    /// before its first attempt, so every attempt at one batch carries the same provenance and
    /// the same pair. A successful attempt clears the pair; a direct caller observes once per
    /// batch it sends.
    fn observe_batch(&mut self, ctx: BatchContext, seq: SeqId) {
        self.pending_provenance = ctx.provenance;
        self.pending_seq = Some(seq);
    }

    /// `submit` with the pending provenance and pair and nothing in flight, then `await_ack`. A
    /// `send` with no pair pending is a caller bug: every batch is observed before it's sent.
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let Some(seq) = self.pending_seq else {
            anyhow::bail!("logit_out: send before observe_batch, so the batch has no sequence");
        };
        let ctx = BatchContext { provenance: self.pending_provenance, ..BatchContext::default() };
        self.submit(batch, ctx, seq, 0).await?;
        self.await_ack().await?;
        self.pending_seq = None;
        Ok(())
    }

    /// The live connection's negotiated window, or 1 with none: a window above 1 is known only
    /// once a `HelloAck` answers.
    fn window(&self) -> usize {
        self.stream.as_ref().map_or(1, |conn| conn.window)
    }

    /// Counts a failure that carries a [`Fault`] in `logit.output.requests`. A failure with
    /// frames in flight carries none and isn't counted: the `await_ack`s after it count the
    /// round's outcome, as the `await_ack` after a success does. A `Permanent` past the head is
    /// counted only once its batch is the head: `write_loop` stops the fill there and submits
    /// that batch again each round until then.
    async fn submit(
        &mut self,
        batch: &EventBatch,
        ctx: BatchContext,
        seq: SeqId,
        in_flight: usize,
    ) -> anyhow::Result<()> {
        let result = self.submit_frame(batch, ctx, seq, in_flight).await;
        let counted = match result.as_ref().map_err(|err| err.downcast_ref::<Fault>()) {
            Err(Some(Fault::Permanent)) => in_flight == 0,
            Err(Some(_)) => true,
            Ok(()) | Err(None) => false,
        };
        if counted {
            count_request(&self.telemetry, &result);
        }
        result
    }

    /// Counts every result in `logit.output.requests`, so a `send` counts once, except the
    /// failure that reports a drifted submit: that submit counted it.
    async fn await_ack(&mut self) -> anyhow::Result<()> {
        if std::mem::take(&mut self.drifted) {
            return Err(anyhow::anyhow!(
                "a submit's in-flight count disagreed with the connection's, which was dropped"
            ))
            .context(Fault::Ambiguous);
        }
        let result = self.read_ack().await;
        count_request(&self.telemetry, &result);
        result
    }

    /// Shuts the pooled connection down, which under TLS sends `close_notify`, so `logit_in`
    /// reads a clean close and not `UnexpectedEof`. Bounded by `self.timeout`. A failure isn't
    /// reported: a frame still in flight on the connection stays in the sink's store.
    async fn flush(&mut self) -> anyhow::Result<()> {
        if let Some(mut conn) = self.stream.take() {
            let _ = tokio::time::timeout(self.timeout, conn.stream.shutdown()).await;
        }
        Ok(())
    }
}

/// Writes and flushes one control message with [`frame::FLAG_CONTROL`] set. Flushed because every
/// control message is followed by a wait for the peer, and a waiting TLS read doesn't send it
/// (ADR `sink-send-path-and-attempt-accounting`, decision 7). Duplicates
/// `logit_inputs::logit`'s `write_control` rather than add a cross-crate dependency for it.
async fn write_control<S: AsyncWrite + Unpin>(
    stream: &mut S,
    msg: &impl ControlEncode,
) -> anyhow::Result<()> {
    let framed =
        frame::write_frame_with_flags(0, Compression::None, frame::FLAG_CONTROL, &msg.encode())?;
    stream.write_all(&framed).await?;
    stream.flush().await?;
    Ok(())
}

trait ControlEncode {
    fn encode(&self) -> Bytes;
}
impl ControlEncode for control::Hello {
    fn encode(&self) -> Bytes {
        control::Hello::encode(self)
    }
}
// Only a peer writes `HelloAck`, `Reject`, and `Ack`; these impls let the tests' fake peer reuse
// `write_control`.
#[cfg(test)]
impl ControlEncode for control::HelloAck {
    fn encode(&self) -> Bytes {
        control::HelloAck::encode(self)
    }
}
#[cfg(test)]
impl ControlEncode for control::Reject {
    fn encode(&self) -> Bytes {
        control::Reject::encode(self)
    }
}
#[cfg(test)]
impl ControlEncode for control::Ack {
    fn encode(&self) -> Bytes {
        control::Ack::encode(self)
    }
}

/// Reads and decodes one control frame. Both declared lengths are checked against
/// [`control::MAX_CONTROL_MESSAGE_BYTES`] before sizing an allocation: the first call reads
/// `HelloAck` from a peer not yet trusted. A control frame is never compressed, so the compressed
/// length shares the cap, as `logit_in` applies it to a `Hello`.
async fn read_control<S: AsyncRead + Unpin>(
    stream: &mut S,
) -> anyhow::Result<control::ControlMessage> {
    let mut header_buf = [0u8; frame::HEADER_LEN];
    stream.read_exact(&mut header_buf).await?;
    let mut header_bytes = Bytes::copy_from_slice(&header_buf);
    let header = frame::FrameHeader::read(&mut header_bytes)
        .map_err(|e| anyhow::Error::new(e).context("reading a control frame header"))?;
    if header.uncompressed_len > control::MAX_CONTROL_MESSAGE_BYTES
        || header.compressed_len > control::MAX_CONTROL_MESSAGE_BYTES
    {
        anyhow::bail!(
            "control frame declares {}/{} (uncompressed/compressed) bytes, over the {}-byte \
             control message cap",
            header.uncompressed_len,
            header.compressed_len,
            control::MAX_CONTROL_MESSAGE_BYTES
        );
    }
    let mut body = vec![0u8; header.compressed_len as usize];
    stream.read_exact(&mut body).await?;
    let mut full = BytesMut::with_capacity(frame::HEADER_LEN + body.len());
    full.extend_from_slice(&header_buf);
    full.extend_from_slice(&body);
    let mut full = full.freeze();
    let (header, mut payload) = frame::read_frame_with_header(&mut full)
        .map_err(|e| anyhow::Error::new(e).context("reading a control frame"))?;
    if header.flags & frame::FLAG_CONTROL == 0 {
        anyhow::bail!("expected a control frame, got a data frame");
    }
    control::ControlMessage::decode(&mut payload)
        .map_err(|e| anyhow::Error::new(e).context("decoding a control message"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        testdata_dir, tls_client_connector, tls_pair, tls_pair_without_tickets, FakeStream, TapIo,
        WriteStep,
    };
    use logit_core::{AttrMap, Event, LogRecord, Resource, Severity, Value};
    use logit_inputs::logit::{LogitInput, TlsServerSettings};
    use logit_inputs::Input;
    use logit_pipeline::test_util::{TelemetryProbe, RECV_TIMEOUT};
    use logit_pipeline::{
        classify, is_explicitly_permanent, is_retryable, DeliveryPosture, Fanout,
    };
    use rustls_pki_types::ServerName;
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    /// The next number of one sender shared by every test in this process, so no two sends
    /// read as one resend at a real `logit_in`.
    fn next_seq() -> SeqId {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        SeqId {
            id: *b"logit-out-tests!",
            seq: NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        }
    }

    /// Observes `batch` under [`next_seq`] with an empty context, then sends it: what
    /// `write_loop` does once per batch.
    async fn send_next(output: &mut LogitOutput, batch: &EventBatch) -> anyhow::Result<()> {
        output.observe_batch(BatchContext::default(), next_seq());
        output.send(batch).await
    }

    /// A `send` with no `observe_batch` before it fails without connecting.
    #[tokio::test]
    async fn a_send_before_observe_batch_fails_without_connecting() {
        let mut output = LogitOutput::new("127.0.0.1:1");
        let err = output.send(&sample_batch()).await.unwrap_err();
        assert!(format!("{err:#}").contains("send before observe_batch"), "{err:#}");
        assert!(output.stream.is_none());
    }

    fn sample_batch() -> EventBatch {
        let mut attrs = AttrMap::new();
        attrs.insert("host", "logit-out-test");
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

    /// A real `LogitInput` on an ephemeral port, so round-trip tests can't drift from `logit_in`.
    async fn spawn_real_listener() -> (String, mpsc::Receiver<logit_pipeline::Delivered>) {
        spawn_real_listener_with_idle_timeout(None, Telemetry::default()).await
    }

    /// [`spawn_real_listener`] with `logit_in`'s `idle_timeout:` set and a telemetry handle, for
    /// the probe tests. Binds before spawning, so the returned address is already live.
    async fn spawn_real_listener_with_idle_timeout(
        idle_timeout: Option<Duration>,
        telemetry: Telemetry,
    ) -> (String, mpsc::Receiver<logit_pipeline::Delivered>) {
        let mut input = LogitInput::new("127.0.0.1:0")
            .with_idle_timeout(idle_timeout)
            .with_telemetry(telemetry);
        input.bind().await.expect("bind should succeed");
        let addr = input.local_addr().expect("a bound listener reports its address").to_string();
        let (tx, rx) = mpsc::channel(16);
        let sink = Fanout::new(vec![tx]);
        tokio::spawn(async move { input.run(sink).await });
        (addr, rx)
    }

    async fn recv_batch(rx: &mut mpsc::Receiver<logit_pipeline::Delivered>) -> EventBatch {
        let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("should receive within 5s")
            .expect("channel should still be open");
        logit_pipeline::unwrap_batch(delivered)
    }

    // ---- happy path / reconnects ------------------------------------------------------------

    #[tokio::test]
    async fn first_send_connects_and_handshakes_second_reuses_the_connection() {
        let (addr, mut rx) = spawn_real_listener().await;
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("out", "logit_out", "sink");
        let mut output = LogitOutput::new(addr).with_telemetry(telemetry);

        send_next(&mut output, &sample_batch()).await.expect("first send should succeed");
        recv_batch(&mut rx).await;
        send_next(&mut output, &sample_batch()).await.expect("second send should succeed");
        recv_batch(&mut rx).await;

        let reconnects = registry
            .drain(0)
            .into_iter()
            .flat_map(|e| e.metrics.into_iter())
            .find(|m| logit_core::interner::resolve(m.name) == "logit.output.reconnects");
        assert!(reconnects.is_none(), "expected no reconnects after two sends on one connection");
    }

    // ---- the pooled-connection probe ---------------------------------------------------------
    //
    // Real durations, never `tokio::time::pause()`: a listener's own timer closing a socket is
    // the condition under test, which paused time can't produce.

    /// A batch sent after `logit_in`'s `idle_timeout:` closed the pooled connection lands: one
    /// reconnect and no `ambiguous` request, which `at_most_once` would have dropped.
    #[tokio::test]
    async fn a_pooled_connection_the_peer_closed_is_replaced_before_the_next_write_with_no_batch_lost(
    ) {
        let mut probe = TelemetryProbe::new();
        let (addr, mut rx) = spawn_real_listener_with_idle_timeout(
            Some(Duration::from_millis(100)),
            probe.telemetry("logit_in", "logit_in", "listener"),
        )
        .await;
        let mut output =
            LogitOutput::new(addr).with_telemetry(probe.telemetry("out", "logit_out", "sink"));

        send_next(&mut output, &sample_batch()).await.expect("first send should succeed");
        assert_eq!(recv_batch(&mut rx).await.events.len(), 1);

        // `logit_in` counts the idle close after its `Reject` is written and flushed, so the
        // `Reject` is already queued here; the FIN follows it, before the listener lingers.
        probe
            .wait_for("the idle connection to close", |t| {
                t.sum("logit.input.connections.closed", &[("reason", "idle")]) == 1.0
            })
            .await;

        send_next(&mut output, &sample_batch())
            .await
            .expect("the probe should replace the closed connection before writing anything");
        assert_eq!(
            recv_batch(&mut rx).await.events.len(),
            1,
            "the second batch must actually reach the listener, not be lost into a dead socket"
        );

        let totals = probe.poll();
        assert_eq!(
            totals.sum("logit.output.reconnects", &[]),
            1.0,
            "exactly one reconnect -- the probe's, not a retry of a failed write"
        );
        assert_eq!(
            totals.sum("logit.output.requests", &[("class", "ok")]),
            2.0,
            "both batches delivered cleanly"
        );
        assert!(
            !totals.has("logit.output.requests", &[("class", "ambiguous")]),
            "and neither may be classified ambiguous -- an ambiguous batch is a dropped one here"
        );
    }

    /// An unsolicited `Reject{GOING_AWAY}` (`PendingClose::Bytes`) is replaced like an EOF.
    #[tokio::test]
    async fn a_pooled_connection_with_an_unsolicited_reject_is_replaced() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let accepts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let server_accepts = Arc::clone(&accepts);
        // Fires once the first connection's `Reject` write has returned, so the probe test waits
        // on that instead of guessing how long the bytes take to reach this host's receive queue.
        let (reject_sent, reject_arrived) = tokio::sync::oneshot::channel();
        let mut reject_sent = Some(reject_sent);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else { break };
                let nth = server_accepts.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                let control::ControlMessage::Hello(_hello) =
                    read_control(&mut stream).await.unwrap()
                else {
                    panic!("expected Hello");
                };
                let ack = control::HelloAck {
                    version: control::PROTOCOL_VERSION,
                    codec: native::CODEC_HOP_BATCH,
                    compression: 0,
                    max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
                    window: 1,
                };
                write_control(&mut stream, &ack).await.unwrap();

                // One data frame, acked like a healthy peer.
                let mut header = [0u8; frame::HEADER_LEN];
                stream.read_exact(&mut header).await.unwrap();
                let mut header_bytes = Bytes::copy_from_slice(&header);
                let h = frame::FrameHeader::read(&mut header_bytes).unwrap();
                let mut body = vec![0u8; h.compressed_len as usize];
                stream.read_exact(&mut body).await.unwrap();
                write_control(&mut stream, &control::Ack).await.unwrap();

                if nth == 1 {
                    // Then, unprompted, an idle close's going-away; `stream` drops after.
                    let reject = control::Reject {
                        code: control::REJECT_GOING_AWAY,
                        message: "idle for 100ms".to_string(),
                    };
                    write_control(&mut stream, &reject).await.unwrap();
                    if let Some(tx) = reject_sent.take() {
                        let _ = tx.send(());
                    }
                }
            }
        });

        let mut probe = TelemetryProbe::new();
        let mut output =
            LogitOutput::new(addr).with_telemetry(probe.telemetry("out", "logit_out", "sink"));

        send_next(&mut output, &sample_batch()).await.expect("first send should succeed");
        reject_arrived.await.expect("the peer task must signal before this test proceeds");

        send_next(&mut output, &sample_batch())
            .await
            .expect("an unsolicited Reject must cost a reconnect, not the batch");

        assert_eq!(
            accepts.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "the probe must have dialled a second connection"
        );
        let totals = probe.poll();
        assert_eq!(totals.sum("logit.output.reconnects", &[]), 1.0);
        assert_eq!(totals.sum("logit.output.requests", &[("class", "ok")]), 2.0);
        assert!(!totals.has("logit.output.requests", &[("class", "ambiguous")]));
    }

    // ---- the same over TLS ---------------------------------------------------------------------

    /// A TLS `logit_in` on an ephemeral port, bound before it's spawned, with `idle_timeout` and
    /// both handles set.
    async fn spawn_real_tls_listener(
        idle_timeout: Option<Duration>,
        telemetry: Telemetry,
        diag: Diagnostics,
    ) -> (String, mpsc::Receiver<logit_pipeline::Delivered>) {
        let mut input = LogitInput::new("127.0.0.1:0")
            .with_idle_timeout(idle_timeout)
            .with_telemetry(telemetry)
            .with_diagnostics(diag)
            .with_tls(&tls_server_settings(), &testdata_dir())
            .unwrap();
        input.bind().await.expect("bind should succeed");
        let addr = input.local_addr().expect("a bound listener reports its address").to_string();
        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move { input.run(Fanout::new(vec![tx])).await });
        (addr, rx)
    }

    /// A `logit_out` dialing `addr` over TLS, trusting `testdata/tls/ca.pem`.
    fn tls_output(addr: String) -> LogitOutput {
        let settings = crate::test_support::tls_settings(|s| s.ca_file = Some("ca.pem".into()));
        LogitOutput::new(addr).with_tls(&settings, &testdata_dir()).unwrap()
    }

    /// A TLS accept loop on an ephemeral port, handing each handshaken connection and its
    /// 1-based index to `serve`, one connection at a time.
    async fn spawn_tls_peer<F, Fut>(serve: F) -> String
    where
        F: Fn(tokio_rustls::server::TlsStream<tokio::net::TcpStream>, usize) -> Fut
            + Send
            + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let acceptor =
            tokio_rustls::TlsAcceptor::from(crate::test_support::server_tls_config(false));
        tokio::spawn(async move {
            for nth in 1.. {
                let Ok((tcp, _)) = listener.accept().await else { break };
                let Ok(stream) = acceptor.accept(tcp).await else { continue };
                serve(stream, nth).await;
            }
        });
        addr
    }

    /// Answers a stream's `Hello` with [`hello_ack`] and reads one data frame.
    async fn handshake_and_read_one_frame<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S) {
        let control::ControlMessage::Hello(_) = read_control(stream).await.unwrap() else {
            panic!("expected Hello");
        };
        write_control(stream, &hello_ack()).await.unwrap();
        read_data_frame(stream).await;
    }

    /// [`a_pooled_connection_the_peer_closed_is_replaced_before_the_next_write_with_no_batch_lost`]
    /// over TLS: the idle close's `Reject` arrives as a TLS record, which the probe reads.
    #[tokio::test]
    async fn a_pooled_tls_connection_the_peer_closed_is_replaced_before_the_next_write_with_no_batch_lost(
    ) {
        let mut probe = TelemetryProbe::new();
        let (addr, mut rx) = spawn_real_tls_listener(
            Some(Duration::from_millis(100)),
            probe.telemetry("logit_in", "logit_in", "listener"),
            Diagnostics::default(),
        )
        .await;
        let mut output =
            tls_output(addr).with_telemetry(probe.telemetry("out", "logit_out", "sink"));

        send_next(&mut output, &sample_batch()).await.expect("first send should succeed");
        assert_eq!(recv_batch(&mut rx).await.events.len(), 1);
        // Counted after the `Reject` is written and flushed.
        probe
            .wait_for("the idle connection to close", |t| {
                t.sum("logit.input.connections.closed", &[("reason", "idle")]) == 1.0
            })
            .await;

        send_next(&mut output, &sample_batch())
            .await
            .expect("the probe should replace the closed connection before writing anything");
        assert_eq!(recv_batch(&mut rx).await.events.len(), 1);

        let totals = probe.poll();
        assert_eq!(totals.sum("logit.output.reconnects", &[]), 1.0);
        assert_eq!(totals.sum("logit.output.requests", &[("class", "ok")]), 2.0);
        assert!(!totals.has("logit.output.requests", &[("class", "ambiguous")]));
    }

    /// [`a_pooled_connection_with_an_unsolicited_reject_is_replaced`] over TLS.
    #[tokio::test]
    async fn a_pooled_tls_connection_with_an_unsolicited_reject_is_replaced() {
        let (reject_sent, reject_arrived) = tokio::sync::oneshot::channel();
        let reject_sent = Arc::new(Mutex::new(Some(reject_sent)));
        let accepts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let server_accepts = Arc::clone(&accepts);
        let addr = spawn_tls_peer(move |mut stream, nth| {
            let reject_sent = Arc::clone(&reject_sent);
            server_accepts.store(nth, std::sync::atomic::Ordering::SeqCst);
            async move {
                handshake_and_read_one_frame(&mut stream).await;
                write_control(&mut stream, &control::Ack).await.unwrap();
                if nth == 1 {
                    let reject = control::Reject {
                        code: control::REJECT_GOING_AWAY,
                        message: "idle for 100ms".to_string(),
                    };
                    write_control(&mut stream, &reject).await.unwrap();
                    let _ = reject_sent.lock().unwrap().take().unwrap().send(());
                }
            }
        })
        .await;

        let mut probe = TelemetryProbe::new();
        let mut output =
            tls_output(addr).with_telemetry(probe.telemetry("out", "logit_out", "sink"));
        send_next(&mut output, &sample_batch()).await.expect("first send should succeed");
        reject_arrived.await.expect("the peer task signals before this test proceeds");

        send_next(&mut output, &sample_batch())
            .await
            .expect("an unsolicited Reject must cost a reconnect, not the batch");

        assert_eq!(accepts.load(std::sync::atomic::Ordering::SeqCst), 2);
        let totals = probe.poll();
        assert_eq!(totals.sum("logit.output.reconnects", &[]), 1.0);
        assert_eq!(totals.sum("logit.output.requests", &[("class", "ok")]), 2.0);
        assert!(!totals.has("logit.output.requests", &[("class", "ambiguous")]));
    }

    /// A TLS peer that reads the whole frame and goes away without acking it: the frame may have
    /// been forwarded, so `Ambiguous`, and the next `send` dials a fresh connection.
    #[tokio::test]
    async fn a_tls_peer_gone_between_the_frame_and_its_ack_is_ambiguous_and_the_next_send_reconnects(
    ) {
        let addr = spawn_tls_peer(|mut stream, nth| async move {
            handshake_and_read_one_frame(&mut stream).await;
            if nth > 1 {
                write_control(&mut stream, &control::Ack).await.unwrap();
            }
        })
        .await;
        let mut probe = TelemetryProbe::new();
        let mut output = tls_output(addr)
            .with_timeout(RECV_TIMEOUT)
            .with_telemetry(probe.telemetry("out", "logit_out", "sink"));

        let err = send_next(&mut output, &sample_batch()).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Ambiguous, "{err:#}");
        assert!(!is_retryable(classify(&err), DeliveryPosture::AtMostOnce));
        assert!(output.stream.is_none(), "a connection with an unresolved ack is dropped");

        send_next(&mut output, &sample_batch()).await.expect("the next send reconnects");
        let totals = probe.poll();
        assert_eq!(totals.sum("logit.output.reconnects", &[]), 1.0);
        assert_eq!(requests(totals), ([1.0, 0.0, 1.0, 0.0], 2.0));
    }

    // ---- close_notify ------------------------------------------------------------------------

    /// `flush` runs once after the last batch, and shuts the pooled connection down: under TLS
    /// that sends `close_notify`, which the peer reads as a clean close. Dropped without it, the
    /// peer reads `UnexpectedEof` (`crate::stream_pins`).
    #[tokio::test]
    async fn flush_sends_close_notify_on_the_pooled_tls_connection() {
        let (client_io, server_io) = tokio::io::duplex(4096);
        let (client, mut server) = tls_pair(client_io, server_io).await;
        let mut output = LogitOutput::new("127.0.0.1:1");
        output.stream = Some(conn_over(client));

        output.flush().await.expect("flush never fails");

        assert!(output.stream.is_none(), "the connection is closed, not pooled");
        let mut buf = [0u8; 16];
        let n = server.read(&mut buf).await.expect("close_notify reads as a clean close");
        assert_eq!(n, 0);
    }

    /// End to end: a TLS `logit_in` ends the connection with no `connection_error` and no
    /// `logit.proto.errors`, whether this sink flushed (`close_notify`) or was dropped at a frame
    /// boundary (`UnexpectedEof`).
    #[tokio::test]
    async fn logit_in_reads_a_tls_connection_ended_after_an_ack_as_a_clean_close() {
        for flushed in [true, false] {
            let mut probe = TelemetryProbe::new();
            let diag = Diagnostics::new("logit_in");
            let (addr, mut rx) = spawn_real_tls_listener(
                None,
                probe.telemetry("logit_in", "logit_in", "listener"),
                diag.clone(),
            )
            .await;
            let mut output = tls_output(addr);
            send_next(&mut output, &sample_batch()).await.expect("the send is acked");
            recv_batch(&mut rx).await;

            if flushed {
                output.flush().await.unwrap();
            }
            drop(output);

            // The connection task reports `connection_error` in the same poll as the gauge drop.
            probe
                .wait_for("logit_in to end the connection", |t| {
                    t.gauge("logit.input.connections", &[]) == Some(0.0)
                })
                .await;
            assert_eq!(diag.occurrences("connection_error"), 0, "flushed: {flushed}");
            assert!(!probe.poll().has("logit.proto.errors", &[]), "flushed: {flushed}");
        }
    }

    // ---- provenance / codec negotiation ------------------------------------------------------

    /// The sink offers the hop codec alone, and its frame carries `observe_batch`'s provenance
    /// and sender pair.
    #[tokio::test]
    async fn the_hop_frame_carries_provenance_and_the_sender_pair() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let control::ControlMessage::Hello(hello) = read_control(&mut stream).await.unwrap()
            else {
                panic!("expected Hello");
            };
            assert_eq!(hello.codecs, [native::CODEC_HOP_BATCH], "the one codec this sink speaks");
            let ack = control::HelloAck {
                version: control::PROTOCOL_VERSION,
                codec: native::CODEC_HOP_BATCH,
                compression: 0,
                max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
                window: 1,
            };
            write_control(&mut stream, &ack).await.unwrap();

            let mut header = [0u8; frame::HEADER_LEN];
            stream.read_exact(&mut header).await.unwrap();
            let mut header_bytes = Bytes::copy_from_slice(&header);
            let h = frame::FrameHeader::read(&mut header_bytes).unwrap();
            assert_eq!(h.codec, native::CODEC_HOP_BATCH, "should send under the negotiated codec");
            let mut body = vec![0u8; h.compressed_len as usize];
            stream.read_exact(&mut body).await.unwrap();
            let mut payload = Bytes::from(body);
            let (_batch, provenance, seq) =
                native::decode_hop_batch(&mut payload, &Default::default()).unwrap();

            write_control(&mut stream, &control::Ack).await.unwrap();
            (provenance, seq)
        });

        let mut output = LogitOutput::new(addr);
        output.observe_batch(
            logit_pipeline::BatchContext {
                trace: logit_pipeline::TraceContext::new_root(),
                provenance: logit_core::Provenance {
                    origin: Some(logit_core::interner::intern("logit_out_test_origin")),
                    previous: Some(logit_core::interner::intern("logit_out_test_previous")),
                },
            },
            SeqId { id: [7; 16], seq: 5 },
        );
        output.send(&sample_batch()).await.expect("send should succeed");

        let (provenance, seq) = server.await.expect("server task should not panic");
        assert_eq!(provenance.origin_str(), Some("logit_out_test_origin"));
        assert_eq!(provenance.previous_str(), Some("logit_out_test_previous"));
        assert_eq!(seq, SeqId { id: [7; 16], seq: 5 });
        assert_eq!(output.pending_seq, None, "an acked attempt clears the pair");
    }

    /// A spool that loses its cursor in a crash replays a batch `logit_in` already forwarded,
    /// under the pair it was written with. `logit_in` acks the replay without forwarding it, so
    /// the next batch its consumer gets is the one behind it.
    #[tokio::test]
    async fn a_spool_replay_after_a_crash_is_forwarded_once() {
        let mut probe = TelemetryProbe::new();
        let (addr, mut rx) = spawn_real_listener_with_idle_timeout(
            None,
            probe.telemetry("logit_in", "logit_in", "listener"),
        )
        .await;
        let dir = logit_pipeline::test_util::scratch_dir("logit-out-spool-replay");
        // An hour between checkpoints: a commit never persists the cursor before the drop.
        let open = || {
            let config = logit_pipeline::DiskQueueConfig {
                dir: dir.clone(),
                max_bytes: 10 * 1024 * 1024,
                segment_bytes: 1024 * 1024,
                overflow: logit_pipeline::OverflowPolicy::Block,
                compression: Compression::None,
                checkpoint_interval: Duration::from_secs(3600),
            };
            logit_pipeline::DiskQueue::open(
                config,
                Telemetry::default(),
                logit_core::Diagnostics::new("test"),
            )
            .unwrap()
        };
        let marked = |mark: i64| {
            let mut batch = sample_batch();
            batch.events[0].timestamp = mark;
            Arc::new(batch)
        };
        let ctx = || logit_pipeline::BatchContext::from(logit_pipeline::TraceContext::default());
        let mut output = LogitOutput::new(addr);

        let spool = open();
        spool.push((marked(1), ctx())).await;
        spool.push((marked(2), ctx())).await;
        let (first, first_ctx, first_seq) = spool.peek().await.expect("the first batch");
        output.observe_batch(first_ctx, first_seq);
        output.send(&first).await.expect("the first send");
        assert_eq!(recv_batch(&mut rx).await.events[0].timestamp, 1);
        spool.commit().expect("commit the first batch");
        drop(spool); // a crash: no `finish`, so the cursor stays where the open persisted it

        let spool = open();
        let (replayed, replayed_ctx, replayed_seq) = spool.peek().await.expect("the replay");
        assert_eq!(replayed.events[0].timestamp, 1);
        assert_eq!(replayed_seq, first_seq, "a replay goes out under its recorded pair");
        output.observe_batch(replayed_ctx, replayed_seq);
        output.send(&replayed).await.expect("the replay is acked");
        spool.commit().expect("commit the replay");

        let (second, second_ctx, second_seq) = spool.peek().await.expect("the second batch");
        output.observe_batch(second_ctx, second_seq);
        output.send(&second).await.expect("the second send");
        spool.commit().expect("commit the second batch");

        assert_eq!(recv_batch(&mut rx).await.events[0].timestamp, 2, "the replay not forwarded");
        assert_eq!(probe.sum("logit.input.batches.resends", &[]), 1.0);
        drop(spool);
        std::fs::remove_dir_all(&dir).ok();
    }

    // ---- logit.output.requests: one count per returned attempt ------------------------------

    /// Every `logit.output.requests` point by class, and their total.
    fn requests(totals: &logit_pipeline::test_util::Totals) -> ([f64; 4], f64) {
        let by_class = ["ok", "clean", "ambiguous", "permanent"]
            .map(|class| totals.sum("logit.output.requests", &[("class", class)]));
        (by_class, totals.sum("logit.output.requests", &[]))
    }

    #[tokio::test]
    async fn a_refused_connect_counts_one_clean_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        let mut probe = TelemetryProbe::new();
        let mut output =
            LogitOutput::new(addr).with_telemetry(probe.telemetry("out", "logit_out", "sink"));

        let err = send_next(&mut output, &sample_batch()).await.unwrap_err();

        assert_eq!(classify(&err), Fault::Clean);
        assert_eq!(requests(probe.poll()), ([0.0, 1.0, 0.0, 0.0], 1.0));
    }

    #[tokio::test]
    async fn a_handshake_reject_counts_one_request_of_its_class() {
        for (code, class) in
            [(control::REJECT_VERSION_MISMATCH, "permanent"), (control::REJECT_INTERNAL, "clean")]
        {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap().to_string();
            tokio::spawn(fake_peer(listener, move |_hello| {
                FakePeerBehavior::Reject(control::Reject { code, message: "no".to_string() })
            }));
            let mut probe = TelemetryProbe::new();
            let mut output =
                LogitOutput::new(addr).with_telemetry(probe.telemetry("out", "logit_out", "sink"));

            send_next(&mut output, &sample_batch()).await.unwrap_err();

            let totals = probe.poll();
            assert_eq!(totals.sum("logit.output.requests", &[("class", class)]), 1.0, "{code}");
            assert_eq!(totals.sum("logit.output.requests", &[]), 1.0, "{code}");
        }
    }

    /// Both reachable too-large returns count `permanent` and keep a pooled connection. The
    /// third, a compressed frame over `frame::compressed_bound`, can't be reached with lz4: that
    /// bound is lz4's worst case over a payload the check before it already bounded.
    #[tokio::test]
    async fn each_too_large_return_counts_one_permanent_request_and_keeps_the_connection() {
        let (addr, mut rx) = spawn_real_listener().await;
        let mut probe = TelemetryProbe::new();
        let mut output =
            LogitOutput::new(addr).with_telemetry(probe.telemetry("out", "logit_out", "sink"));
        send_next(&mut output, &sample_batch()).await.expect("the first send connects");
        recv_batch(&mut rx).await;

        let over_the_sanity_cap = batch_of(frame::MAX_SANE_UNCOMPRESSED_LEN as usize + 1);
        let err = send_next(&mut output, &over_the_sanity_cap).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Permanent, "{err:#}");
        assert!(output.stream.is_some(), "the sanity-cap check runs before the pool is touched");

        output.stream.as_mut().unwrap().peer_max_frame_bytes = 8;
        let err = send_next(&mut output, &sample_batch()).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Permanent, "{err:#}");
        assert!(output.stream.is_some(), "a batch over the peer's bound keeps the connection");

        assert_eq!(requests(probe.poll()), ([1.0, 0.0, 0.0, 2.0], 3.0));
    }

    /// Over a run of every kind of outcome, the `requests` total is the number of `send` calls.
    #[tokio::test]
    async fn every_returned_send_counts_one_request() {
        let refused = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let refused_addr = refused.local_addr().unwrap().to_string();
        drop(refused);
        let (addr, mut rx) = spawn_real_listener().await;
        let rejecting = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rejecting_addr = rejecting.local_addr().unwrap().to_string();
        tokio::spawn(fake_peer(rejecting, |_hello| FakePeerBehavior::AckThenReject {
            code: control::REJECT_INTERNAL,
        }));
        let mut probe = TelemetryProbe::new();
        let mut output = LogitOutput::new(refused_addr).with_telemetry(probe.telemetry(
            "out",
            "logit_out",
            "sink",
        ));

        let batch = sample_batch();
        // Clean: nothing listens.
        send_next(&mut output, &batch).await.unwrap_err();
        output.endpoint = addr;
        send_next(&mut output, &batch).await.unwrap();
        recv_batch(&mut rx).await;
        // Permanent: over the peer's bound.
        output.stream.as_mut().unwrap().peer_max_frame_bytes = 8;
        send_next(&mut output, &batch).await.unwrap_err();
        output.stream.as_mut().unwrap().peer_max_frame_bytes = frame::MAX_SANE_UNCOMPRESSED_LEN;
        send_next(&mut output, &batch).await.unwrap();
        recv_batch(&mut rx).await;
        // Ambiguous: a transient `Reject` in place of the `Ack`.
        output.stream = None;
        output.endpoint = rejecting_addr;
        send_next(&mut output, &batch).await.unwrap_err();

        const SENDS: f64 = 5.0;
        assert_eq!(requests(probe.poll()), ([2.0, 1.0, 1.0, 1.0], SENDS));
    }

    // ---- HelloAck validation --------------------------------------------------------------------

    /// A peer answering its first connection's `Hello` with `first` and holding it open, and
    /// every later connection as a stock `logit_in` does: [`hello_ack`], then one `Ack` per
    /// data frame.
    async fn spawn_peer_answering_first_with(first: control::HelloAck) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let mut first = Some(first);
            while let Ok((mut stream, _)) = listener.accept().await {
                let answer = first.take();
                tokio::spawn(async move {
                    let Ok(control::ControlMessage::Hello(_)) = read_control(&mut stream).await
                    else {
                        return;
                    };
                    if let Some(bad) = answer {
                        let _ = write_control(&mut stream, &bad).await;
                        return std::future::pending().await;
                    }
                    write_control(&mut stream, &hello_ack()).await.unwrap();
                    loop {
                        let mut header = [0u8; frame::HEADER_LEN];
                        if stream.read_exact(&mut header).await.is_err() {
                            return;
                        }
                        let h =
                            frame::FrameHeader::read(&mut Bytes::copy_from_slice(&header)).unwrap();
                        let mut body = vec![0u8; h.compressed_len as usize];
                        stream.read_exact(&mut body).await.unwrap();
                        write_control(&mut stream, &control::Ack).await.unwrap();
                    }
                });
            }
        });
        addr
    }

    /// A `HelloAck` that doesn't answer this sink's `Hello` fails the attempt `Permanent`, as a
    /// version or codec `Reject` does, and never counts as a connection: the next handshake that
    /// passes is this sink's first, not a reconnect.
    async fn assert_hello_ack_is_refused_as_permanent(bad: control::HelloAck, what: &str) {
        let addr = spawn_peer_answering_first_with(bad).await;
        let mut probe = TelemetryProbe::new();
        let mut output =
            LogitOutput::new(addr).with_telemetry(probe.telemetry("out", "logit_out", "sink"));

        let err = send_next(&mut output, &sample_batch()).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Permanent, "{what}: {err:#}");
        assert!(is_explicitly_permanent(&err), "{what}");
        assert!(output.stream.is_none(), "{what}: the refused connection is dropped");

        send_next(&mut output, &sample_batch())
            .await
            .expect("a stock logit_in's HelloAck is accepted");
        let totals = probe.poll();
        assert!(
            !totals.has("logit.output.reconnects", &[]),
            "{what}: a refused handshake is not a connection"
        );
        assert_eq!(totals.sum("logit.output.requests", &[("class", "permanent")]), 1.0, "{what}");
        assert_eq!(totals.sum("logit.output.requests", &[("class", "ok")]), 1.0, "{what}");
    }

    #[tokio::test]
    async fn a_hello_ack_naming_a_codec_never_offered_is_permanent() {
        let bad = control::HelloAck { codec: 99, ..hello_ack() };
        assert_hello_ack_is_refused_as_permanent(bad, "codec 99").await;
    }

    #[tokio::test]
    async fn a_hello_ack_naming_an_unknown_compression_is_permanent() {
        let bad = control::HelloAck { compression: 7, ..hello_ack() };
        assert_hello_ack_is_refused_as_permanent(bad, "compression 7").await;
    }

    /// This sink offers lz4 only when configured to; by default its `Hello` offers none.
    #[tokio::test]
    async fn a_hello_ack_naming_a_compression_never_offered_is_permanent() {
        let bad = control::HelloAck { compression: Compression::Lz4 as u8, ..hello_ack() };
        assert_hello_ack_is_refused_as_permanent(bad, "lz4 not offered").await;
    }

    #[tokio::test]
    async fn a_hello_ack_with_another_protocol_version_is_permanent() {
        let bad = control::HelloAck { version: control::PROTOCOL_VERSION + 1, ..hello_ack() };
        assert_hello_ack_is_refused_as_permanent(bad, "version mismatch").await;
    }

    #[tokio::test]
    async fn a_cancelled_send_dropped_mid_await_leaves_stream_none() {
        // A peer that never acks, so the ack read can only be cancelled: deterministic.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (ready, frame_read) = tokio::sync::oneshot::channel();
        tokio::spawn(fake_peer(listener, move |_hello| FakePeerBehavior::AckThenHang {
            ack_compression: 0,
            ready,
        }));

        let mut output = LogitOutput::new(addr);
        let batch = sample_batch();
        tokio::select! {
            _ = send_next(&mut output, &batch) => panic!("send should never resolve against a peer that never acks"),
            // Cancels only once the peer confirms the frame was read in full: had `send` been
            // cancelled any earlier, `self.stream` would trivially be `None` regardless of the
            // pooled-connection contract this test means to exercise.
            _ = frame_read => {}
        }
        assert!(
            output.stream.is_none(),
            "a cancelled send must not leave a half-used stream in place"
        );
    }

    // ---- fault classification --------------------------------------------------------------

    #[tokio::test]
    async fn connect_refused_is_classified_clean() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener); // nothing listens here now

        let mut output = LogitOutput::new(addr).with_timeout(Duration::from_millis(300));
        let err = send_next(&mut output, &sample_batch()).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Clean);
    }

    /// A fake peer for misbehavior a real `LogitInput` can't be made to produce: accepts one
    /// connection, reads its `Hello`, then does what `respond` says.
    async fn fake_peer(
        listener: TcpListener,
        respond: impl FnOnce(control::Hello) -> FakePeerBehavior + Send + 'static,
    ) {
        let (mut stream, _) = listener.accept().await.unwrap();
        let control::ControlMessage::Hello(hello) = read_control(&mut stream).await.unwrap() else {
            panic!("expected Hello");
        };
        match respond(hello) {
            FakePeerBehavior::Reject(reject) => {
                write_control(&mut stream, &reject).await.unwrap();
            }
            FakePeerBehavior::AckThenClose { ack_compression } => {
                let ack = control::HelloAck {
                    version: control::PROTOCOL_VERSION,
                    codec: native::CODEC_HOP_BATCH,
                    compression: ack_compression,
                    max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
                    window: 1,
                };
                write_control(&mut stream, &ack).await.unwrap();
                // Read one data frame, then close without acking.
                let mut header = [0u8; frame::HEADER_LEN];
                if stream.read_exact(&mut header).await.is_ok() {
                    let mut header_bytes = Bytes::copy_from_slice(&header);
                    if let Ok(h) = frame::FrameHeader::read(&mut header_bytes) {
                        let mut body = vec![0u8; h.compressed_len as usize];
                        let _ = stream.read_exact(&mut body).await;
                    }
                }
            }
            FakePeerBehavior::AckThenHang { ack_compression, ready } => {
                let ack = control::HelloAck {
                    version: control::PROTOCOL_VERSION,
                    codec: native::CODEC_HOP_BATCH,
                    compression: ack_compression,
                    max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
                    window: 1,
                };
                write_control(&mut stream, &ack).await.unwrap();
                let mut header = [0u8; frame::HEADER_LEN];
                stream.read_exact(&mut header).await.unwrap();
                let mut header_bytes = Bytes::copy_from_slice(&header);
                let h = frame::FrameHeader::read(&mut header_bytes).unwrap();
                let mut body = vec![0u8; h.compressed_len as usize];
                stream.read_exact(&mut body).await.unwrap();
                let _ = ready.send(());
                // Open and silent forever: the ack read can only be cancelled.
                std::future::pending::<()>().await;
            }
            FakePeerBehavior::AckThenReject { code } => {
                let ack = control::HelloAck {
                    version: control::PROTOCOL_VERSION,
                    codec: native::CODEC_HOP_BATCH,
                    compression: 0,
                    max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
                    window: 1,
                };
                write_control(&mut stream, &ack).await.unwrap();
                // Read one data frame, then reject it: the post-send `Reject` arm.
                let mut header = [0u8; frame::HEADER_LEN];
                stream.read_exact(&mut header).await.unwrap();
                let mut header_bytes = Bytes::copy_from_slice(&header);
                let h = frame::FrameHeader::read(&mut header_bytes).unwrap();
                let mut body = vec![0u8; h.compressed_len as usize];
                stream.read_exact(&mut body).await.unwrap();
                let reject = control::Reject { code, message: "rejected after send".to_string() };
                write_control(&mut stream, &reject).await.unwrap();
            }
            FakePeerBehavior::HelloAckInPlaceOfAck => {
                write_control(&mut stream, &hello_ack()).await.unwrap();
                let mut header = [0u8; frame::HEADER_LEN];
                stream.read_exact(&mut header).await.unwrap();
                let h = frame::FrameHeader::read(&mut Bytes::copy_from_slice(&header)).unwrap();
                let mut body = vec![0u8; h.compressed_len as usize];
                stream.read_exact(&mut body).await.unwrap();
                write_control(&mut stream, &hello_ack()).await.unwrap();
                // Held open, so the attempt fails on the message and not on a close.
                std::future::pending::<()>().await;
            }
        }
    }

    enum FakePeerBehavior {
        Reject(control::Reject),
        AckThenClose {
            ack_compression: u8,
        },
        /// `ready` fires once the data frame has been read in full, proving the client's write
        /// completed and it is now blocked awaiting the ack this peer never sends.
        AckThenHang {
            ack_compression: u8,
            ready: tokio::sync::oneshot::Sender<()>,
        },
        AckThenReject {
            code: u16,
        },
        /// Reads one data frame and answers it with a second `HelloAck` in place of the `Ack`.
        HelloAckInPlaceOfAck,
    }

    #[tokio::test]
    async fn reject_version_mismatch_is_permanent_and_explicitly_so() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(fake_peer(listener, |_hello| {
            FakePeerBehavior::Reject(control::Reject {
                code: control::REJECT_VERSION_MISMATCH,
                message: "nope".to_string(),
            })
        }));

        let mut output = LogitOutput::new(addr);
        let err = send_next(&mut output, &sample_batch()).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Permanent);
        assert!(is_explicitly_permanent(&err));
    }

    #[tokio::test]
    async fn reject_internal_at_the_handshake_is_clean_not_permanent() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(fake_peer(listener, |_hello| {
            FakePeerBehavior::Reject(control::Reject {
                code: control::REJECT_INTERNAL,
                message: "connection limit reached, retry later".to_string(),
            })
        }));

        let mut output = LogitOutput::new(addr);
        let err = send_next(&mut output, &sample_batch()).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Clean);
        assert!(!is_explicitly_permanent(&err));
    }

    #[tokio::test]
    async fn reject_going_away_at_the_handshake_is_clean_not_permanent() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(fake_peer(listener, |_hello| {
            FakePeerBehavior::Reject(control::Reject {
                code: control::REJECT_GOING_AWAY,
                message: "listener shutting down".to_string(),
            })
        }));

        let mut output = LogitOutput::new(addr);
        let err = send_next(&mut output, &sample_batch()).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Clean);
        assert!(!is_explicitly_permanent(&err));
    }

    #[tokio::test]
    async fn a_reject_internal_after_the_frame_was_sent_is_ambiguous() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(fake_peer(listener, |_hello| FakePeerBehavior::AckThenReject {
            code: control::REJECT_INTERNAL,
        }));

        let mut output = LogitOutput::new(addr);
        let err = send_next(&mut output, &sample_batch()).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Ambiguous);
        assert!(!is_explicitly_permanent(&err));
    }

    #[tokio::test]
    async fn a_message_other_than_ack_or_reject_after_the_frame_is_ambiguous() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(fake_peer(listener, |_hello| FakePeerBehavior::HelloAckInPlaceOfAck));

        let mut output = LogitOutput::new(addr);
        let err = send_next(&mut output, &sample_batch()).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Ambiguous, "{err:#}");
        assert!(output.stream.is_none(), "a connection with an unresolved ack must not be reused");
    }

    #[tokio::test]
    async fn a_reject_frame_too_large_after_the_frame_was_sent_is_still_permanent() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(fake_peer(listener, |_hello| FakePeerBehavior::AckThenReject {
            code: control::REJECT_FRAME_TOO_LARGE,
        }));

        let mut output = LogitOutput::new(addr);
        let err = send_next(&mut output, &sample_batch()).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Permanent);
        assert!(is_explicitly_permanent(&err));
    }

    /// `logit_in` writes `Reject{GOING_AWAY}` only before the frame it answers is forwarded
    /// (`logit_inputs::logit`'s module doc, "Shutdown"), so the batch never landed: `Clean`,
    /// retried at every posture, and the retry on a fresh connection delivers the same frame.
    #[tokio::test]
    async fn a_going_away_in_place_of_an_ack_is_a_clean_fault_and_the_batch_is_resent() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (frames_tx, mut frames_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        tokio::spawn(async move {
            // The first connection's frame is answered `GOING_AWAY`, the second's `Ack`.
            for acked in [false, true] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let control::ControlMessage::Hello(_) = read_control(&mut stream).await.unwrap()
                else {
                    panic!("expected Hello");
                };
                let ack = control::HelloAck {
                    version: control::PROTOCOL_VERSION,
                    codec: native::CODEC_HOP_BATCH,
                    compression: 0,
                    max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
                    window: 1,
                };
                write_control(&mut stream, &ack).await.unwrap();
                let mut header = [0u8; frame::HEADER_LEN];
                stream.read_exact(&mut header).await.unwrap();
                let h = frame::FrameHeader::read(&mut Bytes::copy_from_slice(&header)).unwrap();
                let mut body = vec![0u8; h.compressed_len as usize];
                stream.read_exact(&mut body).await.unwrap();
                frames_tx.send(body).unwrap();
                if acked {
                    write_control(&mut stream, &control::Ack).await.unwrap();
                } else {
                    let reject = control::Reject {
                        code: control::REJECT_GOING_AWAY,
                        message: "listener shutting down".to_string(),
                    };
                    write_control(&mut stream, &reject).await.unwrap();
                }
            }
        });

        let mut output = LogitOutput::new(addr);
        let batch = sample_batch();
        let err = send_next(&mut output, &batch).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Clean, "{err:#}");
        assert!(is_retryable(classify(&err), DeliveryPosture::AtMostOnce));
        assert!(output.stream.is_none(), "the rejected connection is dropped");

        output.send(&batch).await.expect("the resend on a fresh connection is acked");
        let first = frames_rx.recv().await.unwrap();
        let second = frames_rx.recv().await.unwrap();
        assert_eq!(first, second, "the same batch was sent again");
    }

    /// A resend on a new connection carries the identity and sequence the batch was first sent
    /// with, which is what lets `logit_in` recognize it (ADR `native-hop-identity-and-sequence`,
    /// decision 2).
    #[tokio::test]
    async fn a_resend_on_a_new_connection_reuses_the_sender_pair() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (frames_tx, mut frames_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        tokio::spawn(async move {
            // The first connection's frame is answered `GOING_AWAY`, the second's `Ack`.
            for acked in [false, true] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let control::ControlMessage::Hello(_) = read_control(&mut stream).await.unwrap()
                else {
                    panic!("expected Hello");
                };
                let ack = control::HelloAck {
                    version: control::PROTOCOL_VERSION,
                    codec: native::CODEC_HOP_BATCH,
                    compression: 0,
                    max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
                    window: 1,
                };
                write_control(&mut stream, &ack).await.unwrap();
                let mut header = [0u8; frame::HEADER_LEN];
                stream.read_exact(&mut header).await.unwrap();
                let h = frame::FrameHeader::read(&mut Bytes::copy_from_slice(&header)).unwrap();
                assert_eq!(h.codec, native::CODEC_HOP_BATCH);
                let mut body = vec![0u8; h.compressed_len as usize];
                stream.read_exact(&mut body).await.unwrap();
                frames_tx.send(body).unwrap();
                if acked {
                    write_control(&mut stream, &control::Ack).await.unwrap();
                } else {
                    let reject = control::Reject {
                        code: control::REJECT_GOING_AWAY,
                        message: "listener shutting down".to_string(),
                    };
                    write_control(&mut stream, &reject).await.unwrap();
                }
            }
        });

        let pair = SeqId { id: [9; 16], seq: 3 };
        let mut output = LogitOutput::new(addr);
        output.observe_batch(
            logit_pipeline::BatchContext {
                trace: logit_pipeline::TraceContext::new_root(),
                provenance: Provenance::default(),
            },
            pair,
        );
        let batch = sample_batch();
        let err = output.send(&batch).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Clean, "{err:#}");
        output.send(&batch).await.expect("the resend on a fresh connection is acked");

        for which in ["first", "resent"] {
            let mut payload = Bytes::from(frames_rx.recv().await.unwrap());
            let (_batch, _provenance, seq) =
                native::decode_hop_batch(&mut payload, &Default::default()).unwrap();
            assert_eq!(seq, pair, "the {which} frame");
        }
    }

    #[tokio::test]
    async fn an_ack_that_never_arrives_is_classified_ambiguous() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(fake_peer(listener, |_hello| FakePeerBehavior::AckThenClose {
            ack_compression: 0,
        }));

        let mut output = LogitOutput::new(addr).with_timeout(Duration::from_millis(300));
        let err = send_next(&mut output, &sample_batch()).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Ambiguous);
        assert!(output.stream.is_none(), "a connection with an unresolved ack must not be reused");
    }

    /// After an unacked batch, the next `send` reconnects from scratch.
    #[tokio::test]
    async fn a_peer_that_never_acks_is_ambiguous_and_the_next_send_reconnects() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(fake_peer(listener, |_hello| FakePeerBehavior::AckThenClose {
            ack_compression: 0,
        }));

        let mut output = LogitOutput::new(dead_addr).with_timeout(Duration::from_millis(300));
        let err = send_next(&mut output, &sample_batch()).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Ambiguous);
        assert!(output.stream.is_none());

        let (real_addr, mut rx) = spawn_real_listener().await;
        output.endpoint = real_addr;
        send_next(&mut output, &sample_batch())
            .await
            .expect("should reconnect and succeed against a live peer");
        recv_batch(&mut rx).await;
    }

    #[tokio::test]
    async fn lz4_offered_is_negotiated_down_to_none_when_the_peer_offers_none() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(fake_peer(listener, |hello| {
            assert!(
                hello.compressions.contains(&(Compression::Lz4 as u8)),
                "should have offered lz4"
            );
            FakePeerBehavior::AckThenClose { ack_compression: Compression::None as u8 }
        }));

        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("out", "logit_out", "sink");
        let mut output = LogitOutput::new(addr)
            .with_compression(Compression::Lz4)
            .with_timeout(Duration::from_millis(300))
            .with_telemetry(telemetry);
        let _ = send_next(&mut output, &sample_batch()).await; // ambiguous (no ack) -- irrelevant here

        let frames_tag = registry.drain(0).into_iter().find_map(|e| {
            let is_frames_metric = e
                .metrics
                .iter()
                .any(|m| logit_core::interner::resolve(m.name) == "logit.proto.frames");
            if !is_frames_metric {
                return None;
            }
            e.attributes.get("compression").and_then(|v| v.as_str().map(str::to_string))
        });
        assert_eq!(frames_tag, Some("none".to_string()));
    }

    #[tokio::test]
    async fn an_oversized_batch_is_permanent_and_the_connection_is_kept() {
        let (addr, mut rx) = spawn_real_listener().await;
        let mut output = LogitOutput::new(addr);

        send_next(&mut output, &sample_batch()).await.expect("first send should succeed");
        recv_batch(&mut rx).await;

        // Lower the negotiated bound directly rather than stand up a second listener.
        output.stream.as_mut().unwrap().peer_max_frame_bytes = 8;

        let err = send_next(&mut output, &sample_batch()).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Permanent);
        assert!(
            output.stream.is_some(),
            "an oversized batch must not drop an otherwise-good connection"
        );

        output.stream.as_mut().unwrap().peer_max_frame_bytes = frame::MAX_SANE_UNCOMPRESSED_LEN;
        send_next(&mut output, &sample_batch())
            .await
            .expect("a normal batch should still send fine");
        recv_batch(&mut rx).await;
    }

    // ---- the write phase: flushes and fault classes ------------------------------------------
    //
    // `send` is driven over a stream installed as a handshaken `Conn`, so a scripted fault lands
    // on the data frame and nothing else. TLS cases use a real tokio-rustls pair; `FakeStream`
    // never goes under tokio-rustls (its doc says why).

    /// A handshaken, uncompressed connection over `stream`, as [`LogitOutput::handshake`]
    /// leaves one against a stock `logit_in`.
    fn conn_over(stream: impl AsyncStream + 'static) -> Conn {
        Conn {
            stream: Box::new(stream),
            peer_max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
            compression: Compression::None,
            window: 1,
            in_flight: 0,
            broken: false,
            telemetry: Telemetry::default(),
        }
    }

    /// [`conn_over`] with `window` negotiated, `in_flight` frames outstanding, and `telemetry`.
    fn conn_with(
        stream: impl AsyncStream + 'static,
        window: usize,
        in_flight: usize,
        telemetry: Telemetry,
    ) -> Conn {
        let mut conn = conn_over(stream);
        conn.window = window;
        conn.in_flight = in_flight;
        conn.telemetry = telemetry;
        conn
    }

    /// A `logit_in` presenting `testdata/tls/server.pem`, which `tls_client_connector` trusts.
    fn tls_server_settings() -> TlsServerSettings {
        TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: None,
        }
    }

    /// A one-event batch whose message is `len` bytes, so its frame is a little over `len`.
    fn batch_of(len: usize) -> EventBatch {
        let mut batch = sample_batch();
        let mut text = "abcdefghijklmnopqrstuvwxyz".repeat(len / 26 + 1);
        text.truncate(len);
        batch.events[0].log.as_mut().unwrap().message = Value::str(text);
        batch
    }

    /// The `HelloAck` a stock `logit_in` answers this sink's `Hello` with, compression off.
    fn hello_ack() -> control::HelloAck {
        control::HelloAck {
            version: control::PROTOCOL_VERSION,
            codec: native::CODEC_HOP_BATCH,
            compression: 0,
            max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
            window: 1,
        }
    }

    /// Reads one whole data frame off `stream` and returns its body.
    async fn read_data_frame<S: AsyncRead + Unpin>(stream: &mut S) -> Vec<u8> {
        let mut header = [0u8; frame::HEADER_LEN];
        stream.read_exact(&mut header).await.unwrap();
        let h = frame::FrameHeader::read(&mut Bytes::copy_from_slice(&header)).unwrap();
        assert_eq!(h.flags & frame::FLAG_CONTROL, 0, "expected a data frame");
        let mut body = vec![0u8; h.compressed_len as usize];
        stream.read_exact(&mut body).await.unwrap();
        body
    }

    /// Whether `err`'s chain holds an `io::Error` of `kind`.
    fn has_io_error(err: &anyhow::Error, kind: std::io::ErrorKind) -> bool {
        err.chain().any(|e| e.downcast_ref::<std::io::Error>().is_some_and(|e| e.kind() == kind))
    }

    /// A TLS write returns with ciphertext still queued in the session, and a waiting ack read
    /// never sends it (`crate::stream_pins`). A frame larger than the socket can take at once
    /// reaches the peer only through the flush after it; without one the peer never holds the
    /// frame, and the ack wait times out `Ambiguous`: `at_most_once` drops a batch the peer never
    /// received.
    #[tokio::test]
    async fn a_tls_frame_larger_than_the_socket_buffer_is_flushed_before_the_ack_wait() {
        let (client_io, server_io) = tokio::io::duplex(4096);
        let (client, mut server) = tls_pair(client_io, server_io).await;
        let (frames_tx, mut frames_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let body = read_data_frame(&mut server).await;
            frames_tx.send(body).unwrap();
            write_control(&mut server, &control::Ack).await.unwrap();
            std::future::pending::<()>().await;
        });

        let mut output = LogitOutput::new("127.0.0.1:1").with_timeout(RECV_TIMEOUT);
        output.stream = Some(conn_over(client));
        let batch = batch_of(32 * 1024);
        let result = send_next(&mut output, &batch).await;

        let delivered = frames_rx.try_recv();
        assert!(delivered.is_ok(), "the peer never received a whole frame; send said {result:?}");
        result.expect("the peer acks the frame it received");
        let mut payload = Bytes::from(delivered.unwrap());
        let (decoded, ..) = native::decode_hop_batch(&mut payload, &Default::default()).unwrap();
        assert_eq!(native::encode_batch(&decoded), native::encode_batch(&batch));
        assert!(output.stream.is_some(), "an acked connection is pooled");
    }

    /// A write that fails after part of the frame was accepted is `Clean`: `logit_in` reads a
    /// whole frame and checks its CRC before it decodes or forwards anything.
    #[tokio::test]
    async fn a_write_that_fails_part_way_through_the_frame_is_clean_and_keeps_the_io_error() {
        let fake = FakeStream::new()
            .on_write(1, WriteStep::Short(10))
            .on_write(2, WriteStep::Fail(std::io::ErrorKind::ConnectionReset));
        let mut output = LogitOutput::new("127.0.0.1:1");
        output.stream = Some(conn_over(fake.clone()));

        let err = send_next(&mut output, &sample_batch()).await.unwrap_err();

        assert_eq!(classify(&err), Fault::Clean, "{err:#}");
        assert!(is_retryable(classify(&err), DeliveryPosture::AtMostOnce));
        assert!(has_io_error(&err, std::io::ErrorKind::ConnectionReset), "{err:#}");
        assert!(format!("{err:#}").contains("scripted write failure"), "{err:#}");
        assert!(output.stream.is_none(), "a connection with part of a frame on it is dropped");
        assert_eq!(fake.state().unflushed.len(), 10, "part of the frame was accepted");
    }

    #[tokio::test]
    async fn a_first_write_of_zero_bytes_is_clean_and_keeps_the_io_error() {
        let fake = FakeStream::new().on_write(1, WriteStep::Zero);
        let mut output = LogitOutput::new("127.0.0.1:1");
        output.stream = Some(conn_over(fake.clone()));

        let err = send_next(&mut output, &sample_batch()).await.unwrap_err();

        assert_eq!(classify(&err), Fault::Clean, "{err:#}");
        assert!(has_io_error(&err, std::io::ErrorKind::WriteZero), "{err:#}");
        assert!(output.stream.is_none());
    }

    /// A flush that fails leaves the frame's tail unsent: `Clean`, like a failed write. Reads on
    /// `FakeStream` are `Pending` with no waker, so without the flush the ack wait would run to
    /// its timeout.
    #[tokio::test]
    async fn a_failed_flush_is_clean_and_drops_the_connection() {
        let fake = FakeStream::new().failing_flush(std::io::ErrorKind::BrokenPipe);
        let mut output = LogitOutput::new("127.0.0.1:1").with_timeout(Duration::from_millis(100));
        output.stream = Some(conn_over(fake.clone()));

        let err = send_next(&mut output, &sample_batch()).await.unwrap_err();

        assert_eq!(classify(&err), Fault::Clean, "{err:#}");
        assert!(has_io_error(&err, std::io::ErrorKind::BrokenPipe), "{err:#}");
        assert!(output.stream.is_none());
        assert_eq!(fake.state().flushes, 1);
    }

    /// Under TLS a write `Err` can follow whole records of this frame reaching the peer
    /// (`crate::stream_pins`). The verdict is still `Clean`: a real `logit_in` holds a truncated
    /// frame, counts it, and forwards nothing.
    #[tokio::test]
    async fn a_tls_write_error_after_a_whole_record_left_is_clean_and_logit_in_forwards_nothing() {
        let mut probe = TelemetryProbe::new();
        let mut input = LogitInput::new("127.0.0.1:0")
            .with_telemetry(probe.telemetry("in", "logit_in", "listener"))
            .with_tls(&tls_server_settings(), &testdata_dir())
            .unwrap();
        input.bind().await.unwrap();
        let addr = input.local_addr().unwrap();
        let (tx, mut rx) = mpsc::channel(16);
        tokio::spawn(async move { input.run(Fanout::new(vec![tx])).await });

        let (tcp, tap) = TapIo::new(tokio::net::TcpStream::connect(addr).await.unwrap());
        let name = ServerName::try_from("localhost").unwrap();
        let tls = tls_client_connector().connect(name, tcp).await.unwrap();
        let mut output = LogitOutput::new(addr.to_string()).with_telemetry(probe.telemetry(
            "out",
            "logit_out",
            "sink",
        ));
        let conn = output.handshake(Box::new(tls)).await.expect("the handshake completes");
        output.stream = Some(conn);

        // One whole record (16 KiB of plaintext, header included) and part of the next.
        const PASSED: usize = 20_000;
        let before = tap.written();
        tap.fail_writes_after(PASSED);
        let err = send_next(&mut output, &batch_of(40_000)).await.unwrap_err();

        assert_eq!(classify(&err), Fault::Clean, "{err:#}");
        assert!(has_io_error(&err, std::io::ErrorKind::BrokenPipe), "{err:#}");
        assert_eq!(tap.written() - before, PASSED, "ciphertext of the frame reached the peer");
        assert!(output.stream.is_none());
        drop(output);

        let totals = probe
            .wait_for("logit_in to count the truncated frame", |t| {
                t.sum("logit.proto.errors", &[("reason", "truncated")]) >= 1.0
            })
            .await;
        assert_eq!(totals.sum("logit.proto.errors", &[("reason", "truncated")]), 1.0);
        assert!(rx.try_recv().is_err(), "a truncated frame is never forwarded");
    }

    /// The `Hello` has the frame's shape: over a pipe smaller than its TLS record, it reaches the
    /// peer only through a flush, and the `HelloAck` wait depends on it.
    #[tokio::test]
    async fn the_hello_is_flushed_before_the_hello_ack_wait() {
        let (client_io, server_io) = tokio::io::duplex(16);
        let (client, mut server) = tls_pair_without_tickets(client_io, server_io).await;
        tokio::spawn(async move {
            let control::ControlMessage::Hello(_) = read_control(&mut server).await.unwrap() else {
                panic!("expected Hello");
            };
            write_control(&mut server, &hello_ack()).await.unwrap();
            std::future::pending::<()>().await;
        });

        let mut output = LogitOutput::new("127.0.0.1:1").with_timeout(RECV_TIMEOUT);
        output.handshake(Box::new(client)).await.expect("the HelloAck arrives");
    }

    // ---- read_control's own allocation bound -------------------------------------------------

    #[tokio::test]
    async fn read_control_rejects_a_control_header_declaring_more_than_the_sanity_cap() {
        // A header declaring `compressed_len: u32::MAX`; unchecked, that's a ~4 GiB allocation.
        let mut header = BytesMut::new();
        header.extend_from_slice(&frame::MAGIC);
        header.extend_from_slice(&frame::VERSION.to_le_bytes());
        header.extend_from_slice(&frame::FLAG_CONTROL.to_le_bytes());
        header.extend_from_slice(&[0u8]); // codec -- meaningless on a control frame
        header.extend_from_slice(&[Compression::None as u8]);
        header.extend_from_slice(&0u16.to_le_bytes()); // reserved
        header.extend_from_slice(&16u32.to_le_bytes()); // uncompressed_len: small, unremarkable
        header.extend_from_slice(&u32::MAX.to_le_bytes()); // compressed_len: hostile
        header.extend_from_slice(&0u32.to_le_bytes()); // crc32c -- never reached
        assert_eq!(header.len(), frame::HEADER_LEN, "test header must match the real wire shape");

        let (mut client, mut server) = tokio::io::duplex(64);
        tokio::spawn(async move {
            let _ = client.write_all(&header).await;
        });

        let err = read_control(&mut server).await.unwrap_err();
        assert!(
            err.to_string().contains("control message cap"),
            "expected an error mentioning the control message cap, got: {err}"
        );
    }

    /// The cap is the control-message bound, not the data-frame one: a header one byte over it is
    /// refused before a body is read, and a message at it, padded with a field a later protocol
    /// version might add, decodes.
    #[tokio::test]
    async fn read_control_accepts_a_message_at_the_control_message_cap_and_refuses_one_over() {
        let cap = control::MAX_CONTROL_MESSAGE_BYTES as usize;
        // A `HelloAck` padded with unknown field 99: tag, a 2-byte uvarint length, then bytes.
        let mut payload = BytesMut::from(&hello_ack().encode()[..]);
        let pad = cap - payload.len() - 3;
        payload.extend_from_slice(&[99, (pad as u8 & 0x7f) | 0x80, (pad >> 7) as u8]);
        payload.extend_from_slice(&vec![0u8; pad]);
        assert_eq!(payload.len(), cap);

        let at_cap =
            frame::write_frame_with_flags(0, Compression::None, frame::FLAG_CONTROL, &payload)
                .unwrap();
        let (mut client, mut server) = tokio::io::duplex(2 * cap);
        client.write_all(&at_cap).await.unwrap();
        assert_eq!(
            read_control(&mut server).await.unwrap(),
            control::ControlMessage::HelloAck(hello_ack())
        );

        payload.extend_from_slice(&[0]);
        let over =
            frame::write_frame_with_flags(0, Compression::None, frame::FLAG_CONTROL, &payload)
                .unwrap();
        // The header alone: a reader that waited for the body would never return.
        client.write_all(&over[..frame::HEADER_LEN]).await.unwrap();
        let err = tokio::time::timeout(RECV_TIMEOUT, read_control(&mut server))
            .await
            .expect("refused on the header, before any body")
            .unwrap_err();
        assert!(err.to_string().contains("control message cap"), "{err:#}");
    }

    /// A TLS endpoint whose host is no valid server name fails at construction, naming the
    /// endpoint, rather than failing every batch; an IP literal is a valid name.
    #[test]
    fn with_tls_rejects_an_endpoint_with_no_valid_server_name() {
        for endpoint in ["[fe80::1%eth0]:5140", ":5140"] {
            let err = LogitOutput::new(endpoint)
                .with_tls(&TlsClientSettings::default(), &testdata_dir())
                .err()
                .expect(endpoint);
            assert!(err.to_string().contains(endpoint), "{err}");
        }
        for endpoint in ["127.0.0.1:5140", "[::1]:5140", "central.example.com:5140"] {
            LogitOutput::new(endpoint)
                .with_tls(&TlsClientSettings::default(), &testdata_dir())
                .unwrap_or_else(|err| panic!("{endpoint}: {err}"));
        }
    }

    // -- `with_tls`'s `insecure_skip_verify` warning -------------------------------------------

    /// Collects rendered `tracing` output, since `Diagnostics::warn` has no telemetry counterpart.
    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
        type Writer = CapturedLogs;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[test]
    fn tls_insecure_skip_verify_warns() {
        use tracing_subscriber::util::SubscriberInitExt as _;

        let logs = CapturedLogs::default();
        let guard = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_ansi(false)
            .finish()
            .set_default();

        LogitOutput::new("localhost:0")
            .with_diagnostics(Diagnostics::new("logit_out"))
            .with_tls(
                &TlsClientSettings { insecure_skip_verify: true, ..Default::default() },
                Path::new("."),
            )
            .expect("insecure_skip_verify is legal, if loud");
        drop(guard);

        let logged = String::from_utf8_lossy(&logs.0.lock().unwrap()).into_owned();
        assert!(
            logged.contains("tls.insecure_skip_verify is set"),
            "the warning must actually be emitted: {logged}"
        );
    }

    #[test]
    fn tls_default_settings_emit_no_insecure_skip_verify_warning() {
        use tracing_subscriber::util::SubscriberInitExt as _;

        let logs = CapturedLogs::default();
        let guard = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_ansi(false)
            .finish()
            .set_default();

        LogitOutput::new("localhost:0")
            .with_diagnostics(Diagnostics::new("logit_out"))
            .with_tls(&TlsClientSettings::default(), Path::new("."))
            .expect("default tls settings are legal");
        drop(guard);

        let logged = String::from_utf8_lossy(&logs.0.lock().unwrap()).into_owned();
        assert!(
            !logged.contains("tls.insecure_skip_verify is set"),
            "default settings must not warn: {logged}"
        );
    }

    // ---- the send window ------------------------------------------------------------------------

    /// Accepts one connection on `listener` and answers its `Hello` with [`hello_ack`] at
    /// `window`, returning the stream and the window the `Hello` offered.
    async fn accept_with_window(
        listener: &TcpListener,
        window: u32,
    ) -> (tokio::net::TcpStream, u32) {
        let (mut stream, _) = listener.accept().await.unwrap();
        let control::ControlMessage::Hello(hello) = read_control(&mut stream).await.unwrap() else {
            panic!("expected Hello");
        };
        write_control(&mut stream, &control::HelloAck { window, ..hello_ack() }).await.unwrap();
        (stream, hello.window)
    }

    /// [`sample_batch`] with `mark` as its one event's timestamp, so a test reads which batch
    /// arrived.
    fn batch_marked(mark: i64) -> EventBatch {
        let mut batch = sample_batch();
        batch.events[0].timestamp = mark;
        batch
    }

    /// `submit` at `in_flight` with an empty context and [`next_seq`].
    async fn submit_at(
        output: &mut LogitOutput,
        batch: &EventBatch,
        in_flight: usize,
    ) -> anyhow::Result<()> {
        output.submit(batch, BatchContext::default(), next_seq(), in_flight).await
    }

    /// A peer answering window 8 reads three frames before it acks any: three `submit`s return
    /// with nothing acked, and three `await_ack`s then deliver them.
    #[tokio::test]
    async fn a_negotiated_window_puts_several_frames_on_the_wire_before_the_first_ack() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut stream, offered) = accept_with_window(&listener, 8).await;
            // A failed assertion here leaves the frames unacked, so the test times out on it.
            assert_eq!(offered, DEFAULT_WINDOW, "the Hello offers the configured window");
            for _ in 0..3 {
                read_data_frame(&mut stream).await;
            }
            for _ in 0..3 {
                write_control(&mut stream, &control::Ack).await.unwrap();
            }
            std::future::pending::<()>().await;
        });
        let mut probe = TelemetryProbe::new();
        let mut output =
            LogitOutput::new(addr).with_telemetry(probe.telemetry("out", "logit_out", "sink"));

        for in_flight in 0..3 {
            submit_at(&mut output, &sample_batch(), in_flight).await.expect("a submit");
        }
        assert_eq!(output.window(), 8, "the smaller of 32 offered and 8 answered");
        let totals = probe.poll();
        assert_eq!(totals.gauge("logit.output.window", &[]), Some(8.0));
        assert_eq!(totals.gauge("logit.output.in_flight", &[]), Some(3.0));
        assert!(!totals.has("logit.output.requests", &[]), "nothing is counted before an ack");

        for _ in 0..3 {
            tokio::time::timeout(RECV_TIMEOUT, output.await_ack())
                .await
                .expect("the peer acks after reading all three")
                .expect("an Ack");
        }
        let totals = probe.poll();
        assert_eq!(requests(totals), ([3.0, 0.0, 0.0, 0.0], 3.0));
        assert_eq!(totals.gauge("logit.output.in_flight", &[]), Some(0.0));
        assert_eq!(output.stream.as_ref().map(|conn| conn.in_flight), Some(0));
        peer.abort();
    }

    /// A peer answering window 1 gets one frame in flight: `window()` reads 1, so `write_loop`
    /// awaits each `Ack` before the next `submit`.
    #[tokio::test]
    async fn a_peer_answering_window_one_keeps_one_frame_in_flight() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = accept_with_window(&listener, 1).await;
            loop {
                read_data_frame(&mut stream).await;
                write_control(&mut stream, &control::Ack).await.unwrap();
            }
        });
        let mut probe = TelemetryProbe::new();
        let mut output = LogitOutput::new(addr).with_window(16).with_telemetry(probe.telemetry(
            "out",
            "logit_out",
            "sink",
        ));

        for _ in 0..3 {
            submit_at(&mut output, &sample_batch(), 0).await.expect("a submit");
            assert_eq!(output.window(), 1);
            output.await_ack().await.expect("an Ack");
        }
        let totals = probe.poll();
        assert_eq!(totals.gauge("logit.output.window", &[]), Some(1.0));
        assert!(!totals.has("logit.output.reconnects", &[]));
        peer.abort();
    }

    /// A `HelloAck.window` of 0 reads as 1, never as a window that can send nothing.
    #[tokio::test]
    async fn a_hello_ack_window_of_zero_is_read_as_one() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = accept_with_window(&listener, 0).await;
            read_data_frame(&mut stream).await;
            write_control(&mut stream, &control::Ack).await.unwrap();
            std::future::pending::<()>().await;
        });
        let mut output = LogitOutput::new(addr);

        send_next(&mut output, &sample_batch()).await.expect("one frame goes out and is acked");
        assert_eq!(output.window(), 1);
        peer.abort();
    }

    /// `GOING_AWAY` after some `Ack`s answers the oldest unanswered frame, and `logit_in` reads
    /// nothing after writing it, so it is `Clean` for every frame still unanswered.
    #[tokio::test]
    async fn a_going_away_after_some_acks_is_clean_for_every_unanswered_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = accept_with_window(&listener, 8).await;
            for _ in 0..3 {
                read_data_frame(&mut stream).await;
            }
            write_control(&mut stream, &control::Ack).await.unwrap();
            let reject = control::Reject {
                code: control::REJECT_GOING_AWAY,
                message: "listener shutting down".to_string(),
            };
            write_control(&mut stream, &reject).await.unwrap();
            std::future::pending::<()>().await;
        });
        let mut probe = TelemetryProbe::new();
        let mut output =
            LogitOutput::new(addr).with_telemetry(probe.telemetry("out", "logit_out", "sink"));

        for in_flight in 0..3 {
            submit_at(&mut output, &sample_batch(), in_flight).await.expect("a submit");
        }
        output.await_ack().await.expect("the first frame is acked");
        let err = output.await_ack().await.unwrap_err();
        assert_eq!(classify(&err), Fault::Clean, "{err:#}");
        assert!(is_retryable(classify(&err), DeliveryPosture::AtMostOnce));
        assert!(output.stream.is_none(), "the connection is dropped with both unanswered frames");
        assert_eq!(output.window(), 1, "no connection, no negotiated window");
        let totals = probe.poll();
        assert_eq!(requests(totals), ([1.0, 1.0, 0.0, 0.0], 2.0));
        assert_eq!(totals.gauge("logit.output.in_flight", &[]), Some(0.0));
        peer.abort();
    }

    /// An EOF with frames unanswered is `Ambiguous`, and the next `submit`, at 0 in flight,
    /// dials a new connection.
    #[tokio::test]
    async fn an_eof_mid_window_is_ambiguous_and_the_next_submit_reconnects() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut first, _) = accept_with_window(&listener, 8).await;
            read_data_frame(&mut first).await;
            read_data_frame(&mut first).await;
            write_control(&mut first, &control::Ack).await.unwrap();
            drop(first);
            let (mut second, _) = accept_with_window(&listener, 8).await;
            loop {
                read_data_frame(&mut second).await;
                write_control(&mut second, &control::Ack).await.unwrap();
            }
        });
        let mut probe = TelemetryProbe::new();
        let mut output =
            LogitOutput::new(addr).with_telemetry(probe.telemetry("out", "logit_out", "sink"));

        submit_at(&mut output, &sample_batch(), 0).await.unwrap();
        submit_at(&mut output, &sample_batch(), 1).await.unwrap();
        output.await_ack().await.expect("the first frame is acked");
        let err = output.await_ack().await.unwrap_err();
        assert_eq!(classify(&err), Fault::Ambiguous, "{err:#}");
        assert!(output.stream.is_none());

        submit_at(&mut output, &sample_batch(), 0).await.expect("the resubmit reconnects");
        output.await_ack().await.expect("and is acked");
        let totals = probe.poll();
        assert_eq!(totals.sum("logit.output.reconnects", &[]), 1.0);
        assert_eq!(requests(totals), ([2.0, 0.0, 1.0, 0.0], 3.0));
    }

    /// A write that fails with frames in flight returns an unclassified error and leaves the
    /// connection `broken`: the `Ack`s already owed are read on it, and it is dropped after the
    /// last one.
    #[tokio::test]
    async fn a_write_failure_with_frames_in_flight_still_reads_the_acks_already_sent() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            read_data_frame(&mut stream).await;
            read_data_frame(&mut stream).await;
            write_control(&mut stream, &control::Ack).await.unwrap();
            write_control(&mut stream, &control::Ack).await.unwrap();
            std::future::pending::<()>().await;
        });
        let (tcp, tap) = TapIo::new(tokio::net::TcpStream::connect(addr).await.unwrap());
        let mut probe = TelemetryProbe::new();
        let mut output = LogitOutput::new(addr.to_string()).with_telemetry(probe.telemetry(
            "out",
            "logit_out",
            "sink",
        ));
        output.stream = Some(conn_with(tcp, 8, 0, output.telemetry.clone()));

        submit_at(&mut output, &sample_batch(), 0).await.unwrap();
        submit_at(&mut output, &sample_batch(), 1).await.unwrap();
        tap.fail_writes_after(0);
        let err = submit_at(&mut output, &sample_batch(), 2).await.unwrap_err();
        assert!(err.downcast_ref::<Fault>().is_none(), "unclassified: {err:#}");
        assert!(output.stream.as_ref().is_some_and(|conn| conn.broken && conn.in_flight == 2));
        let again = submit_at(&mut output, &sample_batch(), 2).await.unwrap_err();
        assert!(again.downcast_ref::<Fault>().is_none(), "nothing is written: {again:#}");

        output.await_ack().await.expect("the first Ack");
        assert!(output.stream.is_some(), "kept for the Ack still owed");
        output.await_ack().await.expect("the second Ack");
        assert!(output.stream.is_none(), "a broken connection is dropped once nothing is owed");
        let totals = probe.poll();
        assert_eq!(
            requests(totals),
            ([2.0, 0.0, 0.0, 0.0], 2.0),
            "an unclassified submit counts none"
        );
        peer.abort();
    }

    /// A receiver parked on one frame stops reading. The next frame's write stalls, which marks
    /// the connection `broken` once a write accepts nothing for the request timeout; the `Ack`s
    /// already sent still deliver their frames, and only the parked frame's ack wait times out.
    #[tokio::test]
    async fn a_write_that_stalls_with_frames_in_flight_still_reads_the_acks_already_sent_and_times_out_only_the_parked_frame(
    ) {
        // A small receive buffer, inherited by the accepted socket, so the stall comes sooner.
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_recv_buffer_size(4096).unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = socket.listen(16).unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (parked_tx, parked) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut stream, _) = accept_with_window(&listener, 8).await;
            for _ in 0..3 {
                read_data_frame(&mut stream).await;
            }
            write_control(&mut stream, &control::Ack).await.unwrap();
            write_control(&mut stream, &control::Ack).await.unwrap();
            let _ = parked_tx.send(());
            // Parked on the third frame: no more reads, no more acks.
            std::future::pending::<()>().await;
        });
        const TIMEOUT: Duration = Duration::from_millis(300);
        let mut probe = TelemetryProbe::new();
        let mut output = LogitOutput::new(addr)
            .with_timeout(TIMEOUT)
            .with_telemetry(probe.telemetry("out", "logit_out", "sink"));

        for in_flight in 0..3 {
            submit_at(&mut output, &sample_batch(), in_flight).await.unwrap();
        }
        parked.await.unwrap();
        // Larger than both socket buffers can take, so the write stops making progress.
        let large = batch_of(16 * 1024 * 1024);
        let err = tokio::time::timeout(RECV_TIMEOUT, submit_at(&mut output, &large, 3))
            .await
            .expect("the progress bound ends the stalled write")
            .unwrap_err();
        assert!(err.downcast_ref::<Fault>().is_none(), "unclassified: {err:#}");
        assert!(format!("{err:#}").contains("no progress"), "{err:#}");
        assert!(output.stream.as_ref().is_some_and(|conn| conn.broken && conn.in_flight == 3));

        output.await_ack().await.expect("the first frame's Ack");
        output.await_ack().await.expect("the second frame's Ack");
        let err = output.await_ack().await.unwrap_err();
        assert_eq!(classify(&err), Fault::Ambiguous, "the parked frame alone times out: {err:#}");
        assert!(output.stream.is_none());
        assert_eq!(
            requests(probe.poll()),
            ([2.0, 0.0, 1.0, 0.0], 3.0),
            "the stalled submit counts nothing; the drained acks count ok, the parked frame ambiguous"
        );
    }

    /// A `submit` whose `in_flight` isn't the connection's count fails `Ambiguous` and drops the
    /// connection; with frames claimed in flight, the next `await_ack` fails too rather than read
    /// no connection as an `Ack`.
    #[tokio::test]
    async fn a_submit_whose_in_flight_disagrees_with_the_connection_is_ambiguous() {
        let mut probe = TelemetryProbe::new();
        let mut output = LogitOutput::new("127.0.0.1:1").with_telemetry(probe.telemetry(
            "out",
            "logit_out",
            "sink",
        ));
        let err = submit_at(&mut output, &sample_batch(), 2).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Ambiguous, "{err:#}");
        let err = output.await_ack().await.unwrap_err();
        assert_eq!(classify(&err), Fault::Ambiguous, "{err:#}");
        // Counted once, by the submit, and not again by the `await_ack` that reports it.
        assert_eq!(requests(probe.poll()), ([0.0, 0.0, 1.0, 0.0], 1.0));
        output.await_ack().await.expect("the failure is reported once");

        let fake = FakeStream::new();
        output.stream = Some(conn_with(fake.clone(), 8, 1, Telemetry::default()));
        let err = submit_at(&mut output, &sample_batch(), 0).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Ambiguous, "{err:#}");
        assert!(output.stream.is_none(), "the connection whose count drifted is dropped");
        assert!(fake.state().unflushed.is_empty(), "nothing is written");
    }

    /// A cancelled `await_ack` drops the connection, and with it the window: `window()` reads 1
    /// and nothing is in flight.
    #[tokio::test]
    async fn a_cancelled_await_ack_drops_the_connection_and_its_window() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (read_tx, frames_read) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut stream, _) = accept_with_window(&listener, 8).await;
            read_data_frame(&mut stream).await;
            read_data_frame(&mut stream).await;
            let _ = read_tx.send(());
            std::future::pending::<()>().await;
        });
        let mut probe = TelemetryProbe::new();
        let mut output =
            LogitOutput::new(addr).with_telemetry(probe.telemetry("out", "logit_out", "sink"));
        submit_at(&mut output, &sample_batch(), 0).await.unwrap();
        submit_at(&mut output, &sample_batch(), 1).await.unwrap();
        assert_eq!(output.window(), 8);
        let totals = probe.poll();
        assert_eq!(totals.gauge("logit.output.in_flight", &[]), Some(2.0));
        assert_eq!(totals.gauge("logit.output.window", &[]), Some(8.0));
        frames_read.await.unwrap();

        tokio::select! {
            biased;
            _ = output.await_ack() => panic!("the peer never acks"),
            () = std::future::ready(()) => {}
        }
        assert!(output.stream.is_none(), "the cancelled wait dropped the connection");
        assert_eq!(output.window(), 1);
        let totals = probe.poll();
        assert_eq!(totals.gauge("logit.output.in_flight", &[]), Some(0.0), "reset by the drop");
        assert_eq!(totals.gauge("logit.output.window", &[]), Some(1.0), "reset by the drop");
    }

    /// A batch over the peer's bound past the head fails `Permanent` with no count and no warning:
    /// `write_loop` stops the fill there and submits it again each round, and it is counted and
    /// warned once, when it is the head.
    #[tokio::test]
    async fn a_permanent_frame_past_the_head_is_counted_once_when_it_becomes_the_head() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let control::ControlMessage::Hello(_) = read_control(&mut stream).await.unwrap() else {
                panic!("expected Hello");
            };
            let ack = control::HelloAck { window: 8, max_frame_bytes: 4096, ..hello_ack() };
            write_control(&mut stream, &ack).await.unwrap();
            loop {
                read_data_frame(&mut stream).await;
                write_control(&mut stream, &control::Ack).await.unwrap();
            }
        });
        let mut probe = TelemetryProbe::new();
        let diag = Diagnostics::new("logit_out");
        let mut output = LogitOutput::new(addr)
            .with_diagnostics(diag.clone())
            .with_telemetry(probe.telemetry("out", "logit_out", "sink"));
        let oversized = batch_of(8192);

        submit_at(&mut output, &sample_batch(), 0).await.unwrap();
        submit_at(&mut output, &sample_batch(), 1).await.unwrap();
        for _round in 0..2 {
            let err = submit_at(&mut output, &oversized, 2).await.unwrap_err();
            assert_eq!(classify(&err), Fault::Permanent, "{err:#}");
        }
        assert!(!probe.poll().has("logit.output.requests", &[]), "nothing counted past the head");
        assert_eq!(diag.occurrences("frame_too_large"), 0, "nor warned");
        output.await_ack().await.unwrap();
        output.await_ack().await.unwrap();

        let err = submit_at(&mut output, &oversized, 0).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Permanent, "{err:#}");
        assert!(output.stream.is_some(), "the connection is kept");
        assert_eq!(requests(probe.poll()), ([2.0, 0.0, 0.0, 1.0], 3.0));
        assert_eq!(diag.occurrences("frame_too_large"), 1);
        peer.abort();
    }

    /// Over TLS, `Ack`s written in one burst behind several frames arrive in a few records; each
    /// `await_ack` reads one, whatever the record boundaries.
    #[tokio::test]
    async fn a_window_over_tls_reads_acks_buffered_behind_several_frames() {
        let addr = spawn_tls_peer(|mut stream, _nth| async move {
            let control::ControlMessage::Hello(_) = read_control(&mut stream).await.unwrap() else {
                panic!("expected Hello");
            };
            let ack = control::HelloAck { window: 8, ..hello_ack() };
            write_control(&mut stream, &ack).await.unwrap();
            for _ in 0..4 {
                read_data_frame(&mut stream).await;
            }
            let mut acks = Vec::new();
            for _ in 0..4 {
                acks.extend_from_slice(
                    &frame::write_frame_with_flags(
                        0,
                        Compression::None,
                        frame::FLAG_CONTROL,
                        &control::Ack.encode(),
                    )
                    .unwrap(),
                );
            }
            stream.write_all(&acks).await.unwrap();
            stream.flush().await.unwrap();
            std::future::pending::<()>().await;
        })
        .await;
        let mut output = tls_output(addr).with_timeout(RECV_TIMEOUT);

        for in_flight in 0..4 {
            submit_at(&mut output, &sample_batch(), in_flight).await.unwrap();
        }
        for _ in 0..4 {
            output.await_ack().await.expect("an Ack");
        }
        assert_eq!(output.stream.as_ref().map(|conn| conn.in_flight), Some(0));
    }

    /// Drives `batches` through `output` as `write_loop` does under a window: fill to
    /// `output.window()`, then await the oldest, with each batch sequenced `seq(i)`.
    async fn send_windowed(
        output: &mut LogitOutput,
        batches: &[EventBatch],
        seq: impl Fn(usize) -> SeqId,
    ) {
        let (mut next, mut in_flight) = (0, 0);
        while next < batches.len() || in_flight > 0 {
            while next < batches.len() && (in_flight == 0 || in_flight < output.window()) {
                output
                    .submit(&batches[next], BatchContext::default(), seq(next), in_flight)
                    .await
                    .expect("a submit");
                next += 1;
                in_flight += 1;
            }
            output.await_ack().await.expect("an Ack");
            in_flight -= 1;
        }
    }

    /// A real `logit_in` under window 8: twenty batches, each forwarded once and in order.
    #[tokio::test]
    async fn a_window_against_logit_in_delivers_every_batch_once_in_order() {
        let (addr, mut rx) = spawn_real_listener().await;
        let collected = tokio::spawn(async move {
            let mut marks = Vec::new();
            for _ in 0..20 {
                marks.push(recv_batch(&mut rx).await.events[0].timestamp);
            }
            assert!(rx.try_recv().is_err(), "nothing past the twentieth batch");
            marks
        });
        let mut probe = TelemetryProbe::new();
        let mut output = LogitOutput::new(addr).with_window(8).with_telemetry(probe.telemetry(
            "out",
            "logit_out",
            "sink",
        ));
        let batches: Vec<_> = (1..=20).map(batch_marked).collect();

        send_windowed(&mut output, &batches, |i| SeqId { id: [3; 16], seq: i as u64 + 1 }).await;

        assert_eq!(collected.await.unwrap(), (1..=20).collect::<Vec<i64>>());
        assert_eq!(output.window(), 8, "logit_in answers the offered window");
        assert_eq!(requests(probe.poll()), ([20.0, 0.0, 0.0, 0.0], 20.0));
    }

    /// A connection dropped with two frames unacknowledged: both were forwarded, and the resend
    /// from the head on a new connection, under the same pairs, is acked without a second
    /// forward.
    #[tokio::test]
    async fn a_window_resent_after_a_dropped_connection_is_forwarded_once() {
        let mut probe = TelemetryProbe::new();
        let (addr, mut rx) = spawn_real_listener_with_idle_timeout(
            None,
            probe.telemetry("logit_in", "logit_in", "listener"),
        )
        .await;
        let mut output = LogitOutput::new(addr).with_window(8);
        let pair = |i: usize| SeqId { id: [4; 16], seq: i as u64 + 1 };
        let batches: Vec<_> = (1..=6).map(batch_marked).collect();

        for (i, batch) in batches[..5].iter().enumerate() {
            output.submit(batch, BatchContext::default(), pair(i), i).await.unwrap();
        }
        for _ in 0..3 {
            output.await_ack().await.expect("an Ack");
        }
        for mark in 1..=5 {
            assert_eq!(recv_batch(&mut rx).await.events[0].timestamp, mark);
        }
        output.stream = None;

        send_windowed(&mut output, &batches[3..], |i| pair(i + 3)).await;

        assert_eq!(recv_batch(&mut rx).await.events[0].timestamp, 6, "4 and 5 not forwarded again");
        assert_eq!(probe.sum("logit.input.batches.resends", &[]), 2.0);
    }
}
