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
//! **Fault classification.**
//! - Connect, TLS, or `Hello`/`HelloAck` I/O failure: `Clean`.
//! - A `Reject`: [`reject_is_permanent`] decides, not where it arrives.
//!   `REJECT_VERSION_MISMATCH`/`REJECT_NO_COMMON_CODEC`/`REJECT_FRAME_TOO_LARGE` would recur
//!   identically, so `Permanent`. Any other code (`REJECT_INTERNAL`, the peer at its connection
//!   cap; `REJECT_GOING_AWAY`, the peer shutting down; a code a newer peer adds) is transient:
//!   `Clean` at the handshake, `Ambiguous` after a data frame left. The latter is reachable:
//!   `logit_in`'s `serve_connection` races shutdown only against the header read, so
//!   `GOING_AWAY` can replace the `Ack` of a batch that may have been forwarded.
//! - A `HelloAck` naming a codec never offered: `Ambiguous`.
//! - A batch over the sanity cap or the peer's `max_frame_bytes`: `Permanent`, nothing written.
//! - A first write that sends nothing: `Clean`. Any failure once a byte of the frame left, an ack
//!   timeout, or a mismatched `Ack.seq`: `Ambiguous`, and the connection is dropped.
//!
//! `duplicate_safe()` is `false`: the receiver has no dedupe identity.
//!
//! **Pooled-connection probe.** Before the first write on a connection inherited from an earlier
//! batch, the stream gets one non-consuming `poll_read` (`crate::tls::poll_pending_close`, whose
//! doc says why never a cancellable `timeout(read)`). An EOF, or unsolicited bytes (on this
//! protocol, a `Reject{GOING_AWAY}` from a shutdown or a `logit_in` `idle_timeout:`,
//! `docs/adr/idle-connection-timeout.md`), drops it and reconnects before anything leaves the
//! host, the `Clean` path. A FIN arriving between the probe and the write is still `Ambiguous`.
//!
//! **Telemetry** (`docs/design/internal-telemetry.md`'s `logit_out` section):
//! `logit.output.requests{class}` counts each attempt that reached the data-frame write, as `ok`
//! or the failure's `Fault` (`clean`/`ambiguous`/`permanent`); connect, handshake, and too-large
//! failures aren't counted there. `logit.output.reconnects` counts every successful handshake
//! after the first, probe-driven ones included. `logit.output.ack.duration` times the ack wait
//! alone.

use crate::Output;
use anyhow::Context;
use bytes::{Bytes, BytesMut};
use logit_core::{Diagnostics, EventBatch, Provenance, Telemetry};
use logit_pipeline::{BatchContext, Fault};
use logit_proto::frame::{self, Compression};
use logit_proto::native::{self, control};
use rustls_pki_types::ServerName;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

/// Bounds the TCP connect, the TLS handshake, the `HelloAck` wait, and the ack wait, each
/// separately. The `Hello` and data-frame writes have only `write_loop`'s retry budget, the outer
/// bound.
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
    /// `CODEC_NATIVE_V1`. `connect_and_handshake` refuses a codec it never offered.
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
    /// Set by the first handshake, so only later ones count as `logit.output.reconnects`.
    has_connected_once: bool,
    /// The next batch's provenance, set by `Output::observe_batch` before each delivery attempt.
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

    /// Sets the connect, handshake, and ack-wait timeout (default 10s).
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

    /// Connects, performs the TLS handshake if configured, then `Hello`/`HelloAck`. The connect,
    /// TLS handshake, and `HelloAck` wait are each bounded by `self.timeout`; the `Hello` write
    /// only by `write_loop`'s remaining retry budget. Counts `logit.output.reconnects` from the
    /// second success on.
    async fn connect_and_handshake(&mut self) -> anyhow::Result<Conn> {
        let tcp = tokio::time::timeout(self.timeout, TcpStream::connect(&self.endpoint))
            .await
            .context("connecting to logit_out endpoint timed out")
            .and_then(|r| r.context("connecting to logit_out endpoint"))
            .context(Fault::Clean)?;

        let mut stream: Box<dyn AsyncStream> = match &self.tls {
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

        let hello = control::Hello {
            version: control::PROTOCOL_VERSION,
            // v2 first: a v1-only `logit_in` acks the first codec it recognizes, so this costs
            // nothing against an old listener and gains provenance against a new one.
            codecs: vec![native::CODEC_NATIVE_V2, native::CODEC_NATIVE_V1],
            compressions: vec![Compression::None as u8, self.compression as u8],
            max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
            window: 1,
        };
        write_control(&mut stream, &hello).await.context(Fault::Clean)?;

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

        if self.has_connected_once {
            self.telemetry.count("logit.output.reconnects", 1.0, &[]);
        } else {
            self.has_connected_once = true;
        }

        let compression = compression_from_u8(ack.compression).unwrap_or(Compression::None);
        // A codec never offered is a protocol violation. Nothing was sent, so `Clean` would be
        // defensible; `Ambiguous` is the conservative choice for a case a real `logit_in` never
        // produces.
        if ack.codec != native::CODEC_NATIVE_V1 && ack.codec != native::CODEC_NATIVE_V2 {
            return Err(anyhow::anyhow!(
                "logit_in acked codec {}, which was never offered in this sink's Hello",
                ack.codec
            ))
            .context(Fault::Ambiguous);
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

#[async_trait::async_trait]
impl Output for LogitOutput {
    /// Records `ctx.provenance` for `send`. `write_loop` calls this before every attempt at a
    /// batch, retries included.
    fn observe_batch(&mut self, ctx: BatchContext) {
        self.pending_provenance = ctx.provenance;
    }

    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
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

        // One `write` first to learn whether anything left (`Clean` if not), `write_all` only for
        // the remainder: never resend once a byte of this frame reached the peer.
        let first_write = match conn.stream.write(&framed).await {
            Ok(0) if !framed.is_empty() => {
                Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "wrote zero bytes"))
            }
            Ok(n) => Ok(n),
            Err(err) => Err(err),
        };
        match first_write {
            Ok(n) => {
                if n < framed.len() {
                    if let Err(err) = conn.stream.write_all(&framed[n..]).await {
                        // At least one byte already left -- this connection is not reusable.
                        self.telemetry.count(
                            "logit.output.requests",
                            1.0,
                            &[("class", fault_tag(Fault::Ambiguous))],
                        );
                        return Err(anyhow::Error::new(err)).context(Fault::Ambiguous);
                    }
                }
            }
            Err(_) => {
                // Nothing left the host: safe to retry. The connection is dropped.
                self.telemetry.count(
                    "logit.output.requests",
                    1.0,
                    &[("class", fault_tag(Fault::Clean))],
                );
                return Err(anyhow::anyhow!(
                    "logit_out: connection closed before any byte was written"
                ))
                .context(Fault::Clean);
            }
        }

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
                // The frame already left, so a transient reject is `Ambiguous`.
                let fault = if reject_is_permanent(reject.code) {
                    Fault::Permanent
                } else {
                    Fault::Ambiguous
                };
                self.telemetry.count("logit.output.requests", 1.0, &[("class", fault_tag(fault))]);
                return Err(anyhow::anyhow!(
                    "logit_in rejected this connection (code {}): {}",
                    reject.code,
                    reject.message
                ))
                .context(fault);
            }
            Ok(Ok(other)) => {
                self.telemetry.count(
                    "logit.output.requests",
                    1.0,
                    &[("class", fault_tag(Fault::Ambiguous))],
                );
                return Err(anyhow::anyhow!("expected Ack, got {other:?}"))
                    .context(Fault::Ambiguous);
            }
            Ok(Err(err)) => {
                self.telemetry.count(
                    "logit.output.requests",
                    1.0,
                    &[("class", fault_tag(Fault::Ambiguous))],
                );
                return Err(err.context("reading the ack")).context(Fault::Ambiguous);
            }
            Err(_elapsed) => {
                self.telemetry.count(
                    "logit.output.requests",
                    1.0,
                    &[("class", fault_tag(Fault::Ambiguous))],
                );
                return Err(anyhow::anyhow!("timed out waiting for the ack"))
                    .context(Fault::Ambiguous);
            }
        };
        if ack.seq != conn.seq {
            self.telemetry.count(
                "logit.output.requests",
                1.0,
                &[("class", fault_tag(Fault::Ambiguous))],
            );
            return Err(anyhow::anyhow!(
                "ack.seq {} does not match the frame just sent (seq {})",
                ack.seq,
                conn.seq
            ))
            .context(Fault::Ambiguous);
        }

        self.telemetry.count("logit.output.requests", 1.0, &[("class", "ok")]);
        self.stream = Some(conn);
        Ok(())
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        if let Some(conn) = &mut self.stream {
            conn.stream.flush().await?;
        }
        Ok(())
    }

    fn duplicate_safe(&self) -> bool {
        false
    }
}

/// Writes one control message with [`frame::FLAG_CONTROL`] set. Duplicates
/// `logit_inputs::logit`'s `write_control` rather than add a cross-crate dependency for it.
async fn write_control<S: AsyncWrite + Unpin>(
    stream: &mut S,
    msg: &impl ControlEncode,
) -> anyhow::Result<()> {
    let framed =
        frame::write_frame_with_flags(0, Compression::None, frame::FLAG_CONTROL, &msg.encode())?;
    stream.write_all(&framed).await?;
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
/// [`frame::MAX_SANE_UNCOMPRESSED_LEN`] (what `Hello` advertises as `max_frame_bytes`) before
/// sizing an allocation: the first call reads `HelloAck` from a peer not yet trusted.
async fn read_control<S: AsyncRead + Unpin>(
    stream: &mut S,
) -> anyhow::Result<control::ControlMessage> {
    let mut header_buf = [0u8; frame::HEADER_LEN];
    stream.read_exact(&mut header_buf).await?;
    let mut header_bytes = Bytes::copy_from_slice(&header_buf);
    let header = frame::FrameHeader::read(&mut header_bytes)
        .map_err(|e| anyhow::Error::new(e).context("reading a control frame header"))?;
    if header.uncompressed_len > frame::MAX_SANE_UNCOMPRESSED_LEN
        || header.compressed_len > frame::MAX_SANE_UNCOMPRESSED_LEN
    {
        anyhow::bail!(
            "control frame declares {}/{} (uncompressed/compressed) bytes, over the {}-byte \
             sanity cap",
            header.uncompressed_len,
            header.compressed_len,
            frame::MAX_SANE_UNCOMPRESSED_LEN
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
    use logit_core::{AttrMap, Event, LogRecord, Resource, Severity, Value};
    use logit_inputs::logit::LogitInput;
    use logit_inputs::Input;
    use logit_pipeline::{classify, is_explicitly_permanent, Fanout};
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
        spawn_real_listener_with_idle_timeout(None).await
    }

    /// [`spawn_real_listener`] with `logit_in`'s `idle_timeout:` set, for the probe tests.
    async fn spawn_real_listener_with_idle_timeout(
        idle_timeout: Option<Duration>,
    ) -> (String, mpsc::Receiver<logit_pipeline::Delivered>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        let mut input = LogitInput::new(addr.clone()).with_idle_timeout(idle_timeout);
        let (tx, rx) = mpsc::channel(16);
        let sink = Fanout::new(vec![tx]);
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        (addr, rx)
    }

    /// `logit.output.reconnects` from a drained `Registry`, or `None` if never counted.
    fn reconnects_in(events: &[logit_core::Event]) -> Option<f64> {
        events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                logit_core::MetricKind::Sum(sum)
                    if logit_core::interner::resolve(m.name) == "logit.output.reconnects" =>
                {
                    Some(sum.value)
                }
                _ => None,
            })
        })
    }

    /// `logit.output.requests`' total for one `class`, or `None` if never counted. A sum, since
    /// `Telemetry` coalesces repeats into one point per drain.
    fn requests_in(events: &[logit_core::Event], class: &str) -> Option<f64> {
        events.iter().find_map(|e| {
            if e.attributes.get("class").and_then(|v| v.as_str()) != Some(class) {
                return None;
            }
            e.metrics.iter().find_map(|m| match &m.kind {
                logit_core::MetricKind::Sum(sum)
                    if logit_core::interner::resolve(m.name) == "logit.output.requests" =>
                {
                    Some(sum.value)
                }
                _ => None,
            })
        })
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
        let (addr, mut rx) =
            spawn_real_listener_with_idle_timeout(Some(Duration::from_millis(100))).await;
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("out", "logit_out", "sink");
        let mut output = LogitOutput::new(addr).with_telemetry(telemetry);

        output.send(&sample_batch()).await.expect("first send should succeed");
        assert_eq!(recv_batch(&mut rx).await.events.len(), 1);

        // 3x the idle timeout: the listener's `Reject{GOING_AWAY}` and FIN are queued here.
        tokio::time::sleep(Duration::from_millis(300)).await;

        output
            .send(&sample_batch())
            .await
            .expect("the probe should replace the closed connection before writing anything");
        assert_eq!(
            recv_batch(&mut rx).await.events.len(),
            1,
            "the second batch must actually reach the listener, not be lost into a dead socket"
        );

        let drained = registry.drain(0);
        assert_eq!(
            reconnects_in(&drained),
            Some(1.0),
            "exactly one reconnect -- the probe's, not a retry of a failed write"
        );
        assert_eq!(requests_in(&drained, "ok"), Some(2.0), "both batches delivered cleanly");
        assert_eq!(
            requests_in(&drained, "ambiguous"),
            None,
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
                }
            }
        });

        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("out", "logit_out", "sink");
        let mut output = LogitOutput::new(addr).with_telemetry(telemetry);

        output.send(&sample_batch()).await.expect("first send should succeed");
        // Long enough for the `Reject` to be sitting in this host's receive queue.
        tokio::time::sleep(Duration::from_millis(100)).await;

        output
            .send(&sample_batch())
            .await
            .expect("an unsolicited Reject must cost a reconnect, not the batch");

        assert_eq!(
            accepts.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "the probe must have dialled a second connection"
        );
        let drained = registry.drain(0);
        assert_eq!(reconnects_in(&drained), Some(1.0));
        assert_eq!(requests_in(&drained, "ok"), Some(2.0));
        assert_eq!(requests_in(&drained, "ambiguous"), None);
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

    /// A `HelloAck` naming a codec never offered is refused as `Ambiguous`.
    #[tokio::test]
    async fn an_ack_naming_a_codec_never_offered_is_rejected() {
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
                codec: 99, // never offered
                compression: 0,
                max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
                window: 1,
            };
            write_control(&mut stream, &ack).await.unwrap();
        });

        let mut output = LogitOutput::new(addr);
        let err = output.send(&sample_batch()).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Ambiguous);
        server.await.expect("server task should not panic");
    }

    #[tokio::test]
    async fn a_cancelled_send_dropped_mid_await_leaves_stream_none() {
        // A peer that never acks, so the ack read can only be cancelled: deterministic.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(fake_peer(listener, |_hello| FakePeerBehavior::AckThenHang {
            ack_compression: 0,
        }));

        let mut output = LogitOutput::new(addr);
        let batch = sample_batch();
        tokio::select! {
            _ = output.send(&batch) => panic!("send should never resolve against a peer that never acks"),
            () = tokio::time::sleep(Duration::from_millis(100)) => {}
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
            FakePeerBehavior::AckThenHang { ack_compression } => {
                let ack = control::HelloAck {
                    version: control::PROTOCOL_VERSION,
                    codec: native::CODEC_NATIVE_V1,
                    compression: ack_compression,
                    max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
                    window: 1,
                };
                write_control(&mut stream, &ack).await.unwrap();
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
        AckThenClose { ack_compression: u8 },
        AckThenHang { ack_compression: u8 },
        AckThenReject { code: u16 },
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
            err.to_string().contains("sanity cap"),
            "expected an error mentioning the sanity cap, got: {err}"
        );
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
