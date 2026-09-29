//! `logit_out`: the native `logit`-to-`logit` sink (`docs/design/wire-protocol.md`'s "Connection
//! protocol", `docs/adr/native-transport-handshake-and-ack.md`). One TCP (optionally TLS)
//! connection; a `Hello`/`HelloAck` negotiates version, codec, compression, and the peer's
//! `max_frame_bytes`; then one native frame per batch, whose `Ack` must arrive before `send`
//! returns. One frame in flight, so the Nth data frame is seq N.
//!
//! **One attempt per `send`** ([`crate::Output`]'s contract). `write_loop` owns retry and races
//! each attempt against a timeout, so the connection is `take()`n into a local before any write
//! and put back only on success. A cancelled attempt drops the local, closing the connection
//! rather than leaving `self.stream` partway through a frame.
//!
//! **Lazy connect.** `LogitOutput::new` never touches the network: a peer that isn't up yet is
//! not a config error. A failed connection is dropped; the next `send` reconnects.
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
//!   cap; `REJECT_GOING_AWAY`, the peer shutting down or closing an idle connection; a code a
//!   newer peer adds) is transient: `Clean` at the handshake. After a data frame left,
//!   `REJECT_GOING_AWAY` is still `Clean`: `logit_in` writes it only before the frame it answers
//!   is forwarded (its module doc's "Shutdown"), so the batch never landed and is resent at any
//!   delivery posture. Any other transient code there is `Ambiguous`.
//! - A batch over the sanity cap or the peer's `max_frame_bytes`, or a compressed frame over
//!   `frame::compressed_bound` of that: `Permanent`, nothing written, a pooled connection kept.
//! - **Write phase**: any failure before the frame is completely written and flushed (a write
//!   `Err` or `Ok(0)`, a failed flush) is `Clean`, with the `io::Error` kept, and the connection
//!   is dropped. Bytes of the frame may have left the host, but not all of them, so the peer can't
//!   hold the batch. The flush is part of the phase because a TLS write can return with the
//!   frame's tail still queued in the session, and a waiting ack read doesn't send it. ADR
//!   `sink-send-path-and-attempt-accounting`, decisions 6 and 7, has the one residual (a TLS 1.3
//!   `KeyUpdate` queued behind the frame).
//! - **Ack wait**: a timeout, a read error, a message other than `Ack` or `Reject`, or a
//!   mismatched `Ack.seq`: `Ambiguous`, and the connection is dropped.
//!
//! `duplicate_safe()` is `false`: the receiver has no dedupe identity.
//!
//! **Close.** `Output::flush`, called once after the last batch, shuts the pooled connection
//! down, which under TLS sends `close_notify`. A connection dropped after a failed or cancelled
//! attempt closes without one, which `logit_in` reads as a close when it falls between frames.
//!
//! **Pooled-connection probe.** Before the first write on a connection inherited from an earlier
//! batch, the stream gets one non-consuming `poll_read` (`crate::tls::poll_pending_close`, whose
//! doc says why never a cancellable `timeout(read)`). An EOF, or unsolicited bytes (on this
//! protocol, a `Reject{GOING_AWAY}` from a shutdown or a `logit_in` `idle_timeout:`,
//! `docs/adr/idle-connection-timeout.md`), drops it and reconnects before anything leaves the
//! host, the `Clean` path. A FIN arriving between the probe and the write is `Ambiguous` when the
//! write completes first and the ack wait meets it, and `Clean` when the write or flush fails.
//!
//! **Telemetry** (`docs/design/internal-telemetry.md`'s `logit_out` section):
//! `logit.output.requests{class}` counts every attempt that returns, once, as `ok` or the
//! failure's `Fault` (`clean`/`ambiguous`/`permanent`); a cancelled attempt isn't counted.
//! `logit.output.reconnects` counts every validated handshake after the first, probe-driven ones
//! included. `logit.output.ack.duration` times the ack wait alone.

use crate::Output;
use anyhow::Context;
use bytes::{Bytes, BytesMut};
use logit_core::{Diagnostics, EventBatch, Provenance, Telemetry};
use logit_pipeline::{classify, BatchContext, Fault};
use logit_proto::frame::{self, Compression};
use logit_proto::native::{self, control};
use rustls_pki_types::ServerName;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

/// Bounds the TCP connect, the TLS handshake, the `HelloAck` wait, the ack wait, and the shutdown
/// in `Output::flush`, each separately. The `Hello` and data-frame writes and flushes have only
/// `write_loop`'s retry budget, the outer bound.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// `crate::tls::TlsClientSettings`, re-exported to match `crate::otlp`'s path.
pub use crate::tls::TlsClientSettings;

// Shared by every raw-TCP sink: `AsyncStream` erases plain-or-TLS, `host_only` derives the SNI
// name from a bare `host:port`.
use crate::tls::{host_only, poll_pending_close, AsyncStream, PendingClose};

/// A live, handshaken connection.
struct Conn {
    stream: Box<dyn AsyncStream>,
    /// The peer's `HelloAck.max_frame_bytes`. A larger batch is rejected locally as `Permanent`
    /// rather than sent and rejected by the peer.
    peer_max_frame_bytes: u32,
    /// The codec `HelloAck.codec` chose: `CODEC_NATIVE_V2` (provenance crosses the wire) or
    /// `CODEC_NATIVE_V1`. `handshake` refuses a codec it never offered.
    codec: u8,
    /// The negotiated compression; `None` when the peer doesn't support what was offered.
    compression: Compression,
    /// The seq of the last data frame sent. Implicit: the Nth frame is seq N, so `Ack.seq` is
    /// checked for equality and never carried on the frame.
    seq: u64,
}

pub struct LogitOutput {
    endpoint: String,
    /// Offered in `Hello`; [`Conn::compression`] may still be `None`.
    compression: Compression,
    timeout: Duration,
    tls: Option<Arc<rustls::ClientConfig>>,
    diag: Diagnostics,
    telemetry: Telemetry,
    stream: Option<Conn>,
    /// Set by the first handshake that passes validation, so only later ones count as
    /// `logit.output.reconnects`.
    has_connected_once: bool,
    /// The next batch's provenance, set by `Output::observe_batch` once per batch.
    /// Encoded only on a `CODEC_NATIVE_V2` connection; v1 has no trailer to carry it
    /// (`docs/adr/batch-provenance-on-delivered.md`).
    pending_provenance: Provenance,
}

impl LogitOutput {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            compression: Compression::None,
            timeout: DEFAULT_TIMEOUT,
            tls: None,
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
            stream: None,
            has_connected_once: false,
            pending_provenance: Provenance::default(),
        }
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
        self.tls = Some(Arc::new(crate::tls::build_client_config(settings, base_dir)?));
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

    /// Connects, then performs the TLS handshake if configured, each bounded by `self.timeout`.
    ///
    /// `&mut self` because `&LogitOutput` isn't `Send`: its pooled stream isn't `Sync`.
    async fn dial(&mut self) -> anyhow::Result<Box<dyn AsyncStream>> {
        let tcp = tokio::time::timeout(self.timeout, TcpStream::connect(&self.endpoint))
            .await
            .context("connecting to logit_out endpoint timed out")
            .and_then(|r| r.context("connecting to logit_out endpoint"))
            .context(Fault::Clean)?;

        let stream: Box<dyn AsyncStream> = match &self.tls {
            Some(cfg) => {
                let host = host_only(&self.endpoint);
                let server_name = ServerName::try_from(host.to_string())
                    .map_err(|e| {
                        anyhow::anyhow!("logit_out: invalid TLS server name {host:?}: {e}")
                    })
                    .context(Fault::Clean)?;
                let connector = TlsConnector::from(cfg.clone());
                let tls_stream =
                    tokio::time::timeout(self.timeout, connector.connect(server_name, tcp))
                        .await
                        .context("TLS handshake with logit_in endpoint timed out")
                        .and_then(|r| r.context("TLS handshake with logit_in endpoint"))
                        .context(Fault::Clean)?;
                Box::new(tls_stream)
            }
            None => Box::new(tcp),
        };
        Ok(stream)
    }

    /// `Hello`/`HelloAck` over a dialed `stream`. The `HelloAck` wait is bounded by
    /// `self.timeout`; the `Hello` write only by `write_loop`'s remaining retry budget. Counts
    /// `logit.output.reconnects` from the second handshake that passes [`validate_hello_ack`] on.
    async fn handshake(&mut self, mut stream: Box<dyn AsyncStream>) -> anyhow::Result<Conn> {
        let hello = control::Hello {
            version: control::PROTOCOL_VERSION,
            // v2 first: a v1-only `logit_in` acks the first codec it recognizes, so this costs
            // nothing against an old listener and gains provenance against a new one.
            codecs: vec![native::CODEC_NATIVE_V2, native::CODEC_NATIVE_V1],
            compressions: vec![Compression::None as u8, self.compression as u8],
            max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
            window: 1,
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

        if self.has_connected_once {
            self.telemetry.count("logit.output.reconnects", 1.0, &[]);
        } else {
            self.has_connected_once = true;
        }

        Ok(Conn {
            stream,
            peer_max_frame_bytes: ack.max_frame_bytes,
            compression,
            seq: 0,
            codec: ack.codec,
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

/// The negotiated codec byte as a fixed `codec` tag string for `logit.proto.frames`.
fn codec_tag(codec: u8) -> &'static str {
    match codec {
        native::CODEC_NATIVE_V2 => "native_v2",
        _ => "native_v1",
    }
}

fn compression_tag(compression: Compression) -> &'static str {
    match compression {
        Compression::None => "none",
        Compression::Lz4 => "lz4",
        Compression::Zstd => "zstd",
    }
}

fn fault_tag(fault: Fault) -> &'static str {
    match fault {
        Fault::Clean => "clean",
        Fault::Ambiguous => "ambiguous",
        Fault::Permanent => "permanent",
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
    /// One attempt at `batch`, `Output::send`'s body. Every return carries a [`Fault`], which
    /// `send` counts once as `logit.output.requests{class}`.
    async fn attempt(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        // Encoded under v1 before touching the network, so an oversized batch never connects.
        // v2 is this plus a trailer, never smaller, so the pre-check holds for either codec.
        let v1_payload = native::encode_batch(batch);
        if v1_payload.len() as u64 > frame::MAX_SANE_UNCOMPRESSED_LEN as u64 {
            self.diag.warn_throttled(
                "frame_too_large",
                format!(
                    "batch encodes to {} bytes, over the {}-byte sanity cap -- dropping it \
                     rather than ever attempting to send it",
                    v1_payload.len(),
                    frame::MAX_SANE_UNCOMPRESSED_LEN
                ),
            );
            return Err(anyhow::anyhow!("batch too large to send")).context(Fault::Permanent);
        }

        let mut conn = match self.stream.take() {
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
        // Re-encoded only on a v2 connection; a v1 connection reuses `v1_payload`.
        let payload = if conn.codec == native::CODEC_NATIVE_V2 {
            native::encode_batch_v2(batch, self.pending_provenance)
        } else {
            v1_payload
        };

        let bound = conn.peer_max_frame_bytes.min(frame::MAX_SANE_UNCOMPRESSED_LEN);
        if payload.len() as u32 > bound {
            // Only this batch doesn't fit; the connection is kept and nothing is written.
            self.stream = Some(conn);
            self.diag.warn_throttled(
                "frame_too_large",
                format!(
                    "batch encodes to {} bytes, over this connection's {bound}-byte bound",
                    payload.len()
                ),
            );
            return Err(anyhow::anyhow!("batch too large for this connection"))
                .context(Fault::Permanent);
        }

        let framed = frame::write_frame_with_flags(conn.codec, conn.compression, 0, &payload)
            .context(Fault::Permanent)?;
        // The peer bounds `compressed_len` by `frame::compressed_bound`, lz4's worst case over
        // the payload bound; checked here so this side never sends a frame the peer refuses.
        let compressed_len = framed.len() - frame::HEADER_LEN;
        if compressed_len as u64 > frame::compressed_bound(bound) as u64 {
            self.stream = Some(conn);
            self.diag.warn_throttled(
                "frame_too_large",
                format!(
                    "batch compresses to {compressed_len} bytes, over this connection's {}-byte \
                     compressed bound",
                    frame::compressed_bound(bound)
                ),
            );
            return Err(anyhow::anyhow!("compressed batch too large for this connection"))
                .context(Fault::Permanent);
        }

        // The module doc's "Write phase": `Clean` on any failure, and the connection is dropped.
        // Flushed outside `self.timeout`, which a large frame on a slow link can outlast; the
        // retry budget bounds it, as it bounds the write.
        let written = async {
            conn.stream.write_all(&framed).await.context("writing a frame to logit_in")?;
            conn.stream.flush().await.context("flushing a frame to logit_in")
        };
        written.await.context(Fault::Clean)?;

        conn.seq += 1;
        self.telemetry.count(
            "logit.proto.frames",
            1.0,
            &[
                ("direction", "out"),
                ("codec", codec_tag(conn.codec)),
                ("compression", compression_tag(conn.compression)),
            ],
        );
        self.telemetry.count(
            "logit.proto.frame.bytes",
            framed.len() as f64,
            &[("direction", "out")],
        );

        let ack_timer = self.telemetry.timer("logit.output.ack.duration");
        let ack_result = tokio::time::timeout(self.timeout, read_control(&mut conn.stream)).await;
        drop(ack_timer);

        let ack = match ack_result {
            Ok(Ok(control::ControlMessage::Ack(ack))) => ack,
            Ok(Ok(control::ControlMessage::Reject(reject))) => {
                // `logit_in` writes `GOING_AWAY` only before a frame is forwarded, so in place of
                // the `Ack` it means this batch never landed: `Clean`. Any other transient code
                // after the frame left is `Ambiguous`.
                let fault = if reject_is_permanent(reject.code) {
                    Fault::Permanent
                } else if reject.code == control::REJECT_GOING_AWAY {
                    Fault::Clean
                } else {
                    Fault::Ambiguous
                };
                return Err(anyhow::anyhow!(
                    "logit_in rejected this connection (code {}): {}",
                    reject.code,
                    reject.message
                ))
                .context(fault);
            }
            Ok(Ok(other)) => {
                return Err(anyhow::anyhow!("expected Ack, got {other:?}"))
                    .context(Fault::Ambiguous);
            }
            Ok(Err(err)) => {
                return Err(err.context("reading the ack")).context(Fault::Ambiguous);
            }
            Err(_elapsed) => {
                return Err(anyhow::anyhow!("timed out waiting for the ack"))
                    .context(Fault::Ambiguous);
            }
        };
        if ack.seq != conn.seq {
            return Err(anyhow::anyhow!(
                "ack.seq {} does not match the frame just sent (seq {})",
                ack.seq,
                conn.seq
            ))
            .context(Fault::Ambiguous);
        }

        self.stream = Some(conn);
        Ok(())
    }
}

#[async_trait::async_trait]
impl Output for LogitOutput {
    /// Records `ctx.provenance` for `send`. `write_loop` calls this once per batch, before its
    /// first attempt, so every attempt at one batch carries the same provenance.
    fn observe_batch(&mut self, ctx: BatchContext) {
        self.pending_provenance = ctx.provenance;
    }

    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let result = self.attempt(batch).await;
        let class = match &result {
            Ok(()) => "ok",
            Err(err) => fault_tag(classify(err)),
        };
        self.telemetry.count("logit.output.requests", 1.0, &[("class", class)]);
        result
    }

    /// Shuts the pooled connection down, which under TLS sends `close_notify`, so `logit_in`
    /// reads a clean close and not `UnexpectedEof`. Bounded by `self.timeout`. A failure isn't
    /// reported: every frame on a pooled connection is acked, so nothing is lost with it.
    async fn flush(&mut self) -> anyhow::Result<()> {
        if let Some(mut conn) = self.stream.take() {
            let _ = tokio::time::timeout(self.timeout, conn.stream.shutdown()).await;
        }
        Ok(())
    }

    fn duplicate_safe(&self) -> bool {
        false
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
/// `HelloAck` from a peer not yet trusted.
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
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

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

        output.send(&sample_batch()).await.expect("first send should succeed");
        recv_batch(&mut rx).await;
        output.send(&sample_batch()).await.expect("second send should succeed");
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
    /// reconnect and no `ambiguous` request, which at-most-once would have dropped.
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

        output.send(&sample_batch()).await.expect("first send should succeed");
        assert_eq!(recv_batch(&mut rx).await.events.len(), 1);

        // The listener's own live-connections gauge drops back to 0 only once its idle timer has
        // closed the connection: `crate::listener::LiveConnections::enter`'s guard runs after
        // `serve_connection` writes the `Reject` and returns, so the FIN is already queued here.
        probe
            .wait_for("the idle connection to close", |t| {
                t.gauge("logit.input.connections", &[]) == Some(0.0)
            })
            .await;

        output
            .send(&sample_batch())
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
                    codec: native::CODEC_NATIVE_V1,
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
                write_control(&mut stream, &control::Ack { seq: 1 }).await.unwrap();

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

        output.send(&sample_batch()).await.expect("first send should succeed");
        reject_arrived.await.expect("the peer task must signal before this test proceeds");

        output
            .send(&sample_batch())
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

    /// Answers a stream's `Hello` with [`hello_ack_v1`] and reads one data frame.
    async fn handshake_and_read_one_frame<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S) {
        let control::ControlMessage::Hello(_) = read_control(stream).await.unwrap() else {
            panic!("expected Hello");
        };
        write_control(stream, &hello_ack_v1()).await.unwrap();
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

        output.send(&sample_batch()).await.expect("first send should succeed");
        assert_eq!(recv_batch(&mut rx).await.events.len(), 1);
        // The gauge drops to 0 after `serve_connection` wrote the `Reject` and returned.
        probe
            .wait_for("the idle connection to close", |t| {
                t.gauge("logit.input.connections", &[]) == Some(0.0)
            })
            .await;

        output
            .send(&sample_batch())
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
                write_control(&mut stream, &control::Ack { seq: 1 }).await.unwrap();
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
        output.send(&sample_batch()).await.expect("first send should succeed");
        reject_arrived.await.expect("the peer task signals before this test proceeds");

        output
            .send(&sample_batch())
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
                write_control(&mut stream, &control::Ack { seq: 1 }).await.unwrap();
            }
        })
        .await;
        let mut probe = TelemetryProbe::new();
        let mut output = tls_output(addr)
            .with_timeout(RECV_TIMEOUT)
            .with_telemetry(probe.telemetry("out", "logit_out", "sink"));

        let err = output.send(&sample_batch()).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Ambiguous, "{err:#}");
        assert!(!is_retryable(classify(&err), DeliveryPosture::AtMostOnce));
        assert!(output.stream.is_none(), "a connection with an unresolved ack is dropped");

        output.send(&sample_batch()).await.expect("the next send reconnects");
        let totals = probe.poll();
        assert_eq!(totals.sum("logit.output.reconnects", &[]), 1.0);
        assert_eq!(requests(totals), ([1.0, 0.0, 1.0, 0.0], 2.0));
    }

    // ---- close_notify ----------------------------------------------------------------------------

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
            output.send(&sample_batch()).await.expect("the send is acked");
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

    /// A peer acking v2 gets a v2 frame carrying `observe_batch`'s provenance.
    #[tokio::test]
    async fn a_peer_that_acks_v2_gets_a_v2_frame_with_provenance() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let control::ControlMessage::Hello(hello) = read_control(&mut stream).await.unwrap()
            else {
                panic!("expected Hello");
            };
            assert!(
                hello.codecs.contains(&native::CODEC_NATIVE_V2),
                "this sink should offer v2: {:?}",
                hello.codecs
            );
            let ack = control::HelloAck {
                version: control::PROTOCOL_VERSION,
                codec: native::CODEC_NATIVE_V2,
                compression: 0,
                max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
                window: 1,
            };
            write_control(&mut stream, &ack).await.unwrap();

            let mut header = [0u8; frame::HEADER_LEN];
            stream.read_exact(&mut header).await.unwrap();
            let mut header_bytes = Bytes::copy_from_slice(&header);
            let h = frame::FrameHeader::read(&mut header_bytes).unwrap();
            assert_eq!(h.codec, native::CODEC_NATIVE_V2, "should send under the negotiated codec");
            let mut body = vec![0u8; h.compressed_len as usize];
            stream.read_exact(&mut body).await.unwrap();
            let mut payload = Bytes::from(body);
            let (_batch, provenance) =
                native::decode_batch_v2(&mut payload, &Default::default()).unwrap();

            write_control(&mut stream, &control::Ack { seq: 1 }).await.unwrap();
            provenance
        });

        let mut output = LogitOutput::new(addr);
        output.observe_batch(logit_pipeline::BatchContext {
            trace: logit_pipeline::TraceContext::new_root(),
            provenance: logit_core::Provenance {
                origin: Some(logit_core::interner::intern("logit_out_test_origin")),
                previous: Some(logit_core::interner::intern("logit_out_test_previous")),
            },
        });
        output.send(&sample_batch()).await.expect("send should succeed");

        let provenance = server.await.expect("server task should not panic");
        assert_eq!(provenance.origin_str(), Some("logit_out_test_origin"));
        assert_eq!(provenance.previous_str(), Some("logit_out_test_previous"));
    }

    /// A peer acking v1 gets a plain v1 frame with no provenance trailer.
    #[tokio::test]
    async fn a_peer_that_only_acks_v1_gets_a_plain_v1_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let control::ControlMessage::Hello(_hello) = read_control(&mut stream).await.unwrap()
            else {
                panic!("expected Hello");
            };
            let ack = control::HelloAck {
                version: control::PROTOCOL_VERSION,
                codec: native::CODEC_NATIVE_V1,
                compression: 0,
                max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
                window: 1,
            };
            write_control(&mut stream, &ack).await.unwrap();

            let mut header = [0u8; frame::HEADER_LEN];
            stream.read_exact(&mut header).await.unwrap();
            let mut header_bytes = Bytes::copy_from_slice(&header);
            let h = frame::FrameHeader::read(&mut header_bytes).unwrap();
            assert_eq!(h.codec, native::CODEC_NATIVE_V1, "should stay on v1 against this peer");
            let mut body = vec![0u8; h.compressed_len as usize];
            stream.read_exact(&mut body).await.unwrap();
            let mut payload = Bytes::from(body);
            assert!(native::decode_batch_v2(&mut payload.clone(), &Default::default()).is_err());
            native::decode_batch(&mut payload, &Default::default()).unwrap();

            write_control(&mut stream, &control::Ack { seq: 1 }).await.unwrap();
        });

        let mut output = LogitOutput::new(addr);
        output.observe_batch(logit_pipeline::BatchContext {
            trace: logit_pipeline::TraceContext::new_root(),
            provenance: logit_core::Provenance {
                origin: Some(logit_core::interner::intern("logit_out_test_origin")),
                previous: None,
            },
        });
        output.send(&sample_batch()).await.expect("send should succeed");
        server.await.expect("server task should not panic");
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

        let err = output.send(&sample_batch()).await.unwrap_err();

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

            output.send(&sample_batch()).await.unwrap_err();

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
        output.send(&sample_batch()).await.expect("the first send connects");
        recv_batch(&mut rx).await;

        let over_the_sanity_cap = batch_of(frame::MAX_SANE_UNCOMPRESSED_LEN as usize + 1);
        let err = output.send(&over_the_sanity_cap).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Permanent, "{err:#}");
        assert!(output.stream.is_some(), "the sanity-cap check runs before the pool is touched");

        output.stream.as_mut().unwrap().peer_max_frame_bytes = 8;
        let err = output.send(&sample_batch()).await.unwrap_err();
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
        let mut probe = TelemetryProbe::new();
        let mut output = LogitOutput::new(refused_addr).with_telemetry(probe.telemetry(
            "out",
            "logit_out",
            "sink",
        ));

        let batch = sample_batch();
        // Clean: nothing listens.
        output.send(&batch).await.unwrap_err();
        output.endpoint = addr;
        output.send(&batch).await.unwrap();
        recv_batch(&mut rx).await;
        // Permanent: over the peer's bound.
        output.stream.as_mut().unwrap().peer_max_frame_bytes = 8;
        output.send(&batch).await.unwrap_err();
        output.stream.as_mut().unwrap().peer_max_frame_bytes = frame::MAX_SANE_UNCOMPRESSED_LEN;
        output.send(&batch).await.unwrap();
        recv_batch(&mut rx).await;
        // Ambiguous: the listener acks its third frame as seq 3, and this side expects 8.
        output.stream.as_mut().unwrap().seq = 7;
        output.send(&batch).await.unwrap_err();
        recv_batch(&mut rx).await;

        const SENDS: f64 = 5.0;
        assert_eq!(requests(probe.poll()), ([2.0, 1.0, 1.0, 1.0], SENDS));
    }

    // ---- HelloAck validation --------------------------------------------------------------------

    /// A peer answering its first connection's `Hello` with `first` and holding it open, and
    /// every later connection as a stock `logit_in` does: [`hello_ack_v1`], then one `Ack` per
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
                    write_control(&mut stream, &hello_ack_v1()).await.unwrap();
                    for seq in 1.. {
                        let mut header = [0u8; frame::HEADER_LEN];
                        if stream.read_exact(&mut header).await.is_err() {
                            return;
                        }
                        let h =
                            frame::FrameHeader::read(&mut Bytes::copy_from_slice(&header)).unwrap();
                        let mut body = vec![0u8; h.compressed_len as usize];
                        stream.read_exact(&mut body).await.unwrap();
                        write_control(&mut stream, &control::Ack { seq }).await.unwrap();
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

        let err = output.send(&sample_batch()).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Permanent, "{what}: {err:#}");
        assert!(is_explicitly_permanent(&err), "{what}");
        assert!(output.stream.is_none(), "{what}: the refused connection is dropped");

        output.send(&sample_batch()).await.expect("a stock logit_in's HelloAck is accepted");
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
        let bad = control::HelloAck { codec: 99, ..hello_ack_v1() };
        assert_hello_ack_is_refused_as_permanent(bad, "codec 99").await;
    }

    #[tokio::test]
    async fn a_hello_ack_naming_an_unknown_compression_is_permanent() {
        let bad = control::HelloAck { compression: 7, ..hello_ack_v1() };
        assert_hello_ack_is_refused_as_permanent(bad, "compression 7").await;
    }

    /// This sink offers lz4 only when configured to; by default its `Hello` offers none.
    #[tokio::test]
    async fn a_hello_ack_naming_a_compression_never_offered_is_permanent() {
        let bad = control::HelloAck { compression: Compression::Lz4 as u8, ..hello_ack_v1() };
        assert_hello_ack_is_refused_as_permanent(bad, "lz4 not offered").await;
    }

    #[tokio::test]
    async fn a_hello_ack_with_another_protocol_version_is_permanent() {
        let bad = control::HelloAck { version: control::PROTOCOL_VERSION + 1, ..hello_ack_v1() };
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
            _ = output.send(&batch) => panic!("send should never resolve against a peer that never acks"),
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
        let err = output.send(&sample_batch()).await.unwrap_err();
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
                    codec: native::CODEC_NATIVE_V1,
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
                    codec: native::CODEC_NATIVE_V1,
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
                    codec: native::CODEC_NATIVE_V1,
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
        let err = output.send(&sample_batch()).await.unwrap_err();
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
        let err = output.send(&sample_batch()).await.unwrap_err();
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
        let err = output.send(&sample_batch()).await.unwrap_err();
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
        let err = output.send(&sample_batch()).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Ambiguous);
        assert!(!is_explicitly_permanent(&err));
    }

    #[tokio::test]
    async fn a_reject_frame_too_large_after_the_frame_was_sent_is_still_permanent() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(fake_peer(listener, |_hello| FakePeerBehavior::AckThenReject {
            code: control::REJECT_FRAME_TOO_LARGE,
        }));

        let mut output = LogitOutput::new(addr);
        let err = output.send(&sample_batch()).await.unwrap_err();
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
                    codec: native::CODEC_NATIVE_V1,
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
                    write_control(&mut stream, &control::Ack { seq: 1 }).await.unwrap();
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
        let err = output.send(&batch).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Clean, "{err:#}");
        assert!(is_retryable(classify(&err), DeliveryPosture::AtMostOnce));
        assert!(output.stream.is_none(), "the rejected connection is dropped");

        output.send(&batch).await.expect("the resend on a fresh connection is acked");
        let first = frames_rx.recv().await.unwrap();
        let second = frames_rx.recv().await.unwrap();
        assert_eq!(first, second, "the same batch was sent again");
    }

    #[tokio::test]
    async fn an_ack_that_never_arrives_is_classified_ambiguous() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(fake_peer(listener, |_hello| FakePeerBehavior::AckThenClose {
            ack_compression: 0,
        }));

        let mut output = LogitOutput::new(addr).with_timeout(Duration::from_millis(300));
        let err = output.send(&sample_batch()).await.unwrap_err();
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
        let err = output.send(&sample_batch()).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Ambiguous);
        assert!(output.stream.is_none());

        let (real_addr, mut rx) = spawn_real_listener().await;
        output.endpoint = real_addr;
        output
            .send(&sample_batch())
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
        let _ = output.send(&sample_batch()).await; // ambiguous (no ack) -- irrelevant here

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

        output.send(&sample_batch()).await.expect("first send should succeed");
        recv_batch(&mut rx).await;

        // Lower the negotiated bound directly rather than stand up a second listener.
        output.stream.as_mut().unwrap().peer_max_frame_bytes = 8;

        let err = output.send(&sample_batch()).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Permanent);
        assert!(
            output.stream.is_some(),
            "an oversized batch must not drop an otherwise-good connection"
        );

        output.stream.as_mut().unwrap().peer_max_frame_bytes = frame::MAX_SANE_UNCOMPRESSED_LEN;
        output.send(&sample_batch()).await.expect("a normal batch should still send fine");
        recv_batch(&mut rx).await;
    }

    // ---- the write phase: flushes and fault classes ------------------------------------------
    //
    // `send` is driven over a stream installed as a handshaken `Conn`, so a scripted fault lands
    // on the data frame and nothing else. TLS cases use a real tokio-rustls pair; `FakeStream`
    // never goes under tokio-rustls (its doc says why).

    /// A handshaken v1, uncompressed connection over `stream`, as [`LogitOutput::handshake`]
    /// leaves one against a stock `logit_in`.
    fn conn_over(stream: impl AsyncStream + 'static) -> Conn {
        Conn {
            stream: Box::new(stream),
            peer_max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
            codec: native::CODEC_NATIVE_V1,
            compression: Compression::None,
            seq: 0,
        }
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
    fn hello_ack_v1() -> control::HelloAck {
        control::HelloAck {
            version: control::PROTOCOL_VERSION,
            codec: native::CODEC_NATIVE_V1,
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
    /// never sends it (`crate::stream_pins`). A frame larger than the socket can take at once reaches
    /// the peer only through the flush after it; without one the peer never holds the frame, the
    /// ack wait times out `Ambiguous`, and at-most-once drops a batch the peer never received.
    #[tokio::test]
    async fn a_tls_frame_larger_than_the_socket_buffer_is_flushed_before_the_ack_wait() {
        let (client_io, server_io) = tokio::io::duplex(4096);
        let (client, mut server) = tls_pair(client_io, server_io).await;
        let (frames_tx, mut frames_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let body = read_data_frame(&mut server).await;
            frames_tx.send(body).unwrap();
            write_control(&mut server, &control::Ack { seq: 1 }).await.unwrap();
            std::future::pending::<()>().await;
        });

        let mut output = LogitOutput::new("127.0.0.1:1").with_timeout(RECV_TIMEOUT);
        output.stream = Some(conn_over(client));
        let batch = batch_of(32 * 1024);
        let result = output.send(&batch).await;

        let delivered = frames_rx.try_recv();
        assert!(delivered.is_ok(), "the peer never received a whole frame; send said {result:?}");
        result.expect("the peer acks the frame it received");
        assert_eq!(delivered.unwrap(), native::encode_batch(&batch).to_vec());
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

        let err = output.send(&sample_batch()).await.unwrap_err();

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

        let err = output.send(&sample_batch()).await.unwrap_err();

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

        let err = output.send(&sample_batch()).await.unwrap_err();

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
        let err = output.send(&batch_of(40_000)).await.unwrap_err();

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
            write_control(&mut server, &hello_ack_v1()).await.unwrap();
            std::future::pending::<()>().await;
        });

        let mut output = LogitOutput::new("127.0.0.1:1").with_timeout(RECV_TIMEOUT);
        let conn = output.handshake(Box::new(client)).await.expect("the HelloAck arrives");
        assert_eq!(conn.codec, native::CODEC_NATIVE_V1);
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
        let mut payload = BytesMut::from(&hello_ack_v1().encode()[..]);
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
            control::ControlMessage::HelloAck(hello_ack_v1())
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
}
