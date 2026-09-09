//! `logit_out` -- the native `logit`-to-`logit` sink side (`docs/design/wire-protocol.md`'s
//! connection protocol, `docs/plans/native-transport.md` workstream D). Opens one TCP
//! (optionally TLS) connection, negotiates version/codec/compression via `Hello`/`HelloAck`
//! (`logit_proto::native::control`), then sends one native frame per batch and waits for its
//! `Ack` before this sink's `send` returns.
//!
//! **One attempt per `send`, exactly [`crate::Output`]'s contract.** Retry, budget, and backoff
//! all live in `logit-pipeline`'s `write_loop`, which races every attempt against
//! `tokio::time::timeout` -- so, like `syslog_out`'s `Conn::Tcp`, the live connection is always
//! `take()`n into a local before any write and only put back on complete success. A cancelled
//! attempt (the timeout firing mid-write) drops that local, closing the connection rather than
//! leaving `self.stream` pointing at a socket some unknown number of this frame's bytes into.
//!
//! **Lazy connect.** `LogitOutput::new` never touches the network -- a `logit_out` pointed at a
//! peer that isn't up yet is not a config error, the same `syslog_out`/`Conn::Tcp` precedent.
//!
//! **Fault classification.** Connect/handshake failure (including a version/codec `Reject`
//! before any data frame is sent) -> `Clean`; `Reject` on the connection at any point -> that
//! `Reject`'s own severity (`REJECT_FRAME_TOO_LARGE`/`VERSION_MISMATCH`/`NO_COMMON_CODEC` ->
//! `Permanent`, since retrying the identical `Hello` would fail identically); any I/O failure
//! once at least one byte of a data frame has left -> `Ambiguous`; an ack timeout or a
//! mismatched `Ack.seq` -> `Ambiguous` (the batch may have landed; this connection's state is no
//! longer trustworthy either way, so it's dropped and the next `send` reconnects).
//! `duplicate_safe()` is `false` -- there is no receiver-side dedupe identity
//! (`docs/plans/native-transport.md`'s own "Explicitly out of scope" list).

use crate::Output;
use anyhow::Context;
use bytes::{Bytes, BytesMut};
use logit_core::{Diagnostics, EventBatch, Telemetry};
use logit_pipeline::Fault;
use logit_proto::frame::{self, Compression};
use logit_proto::native::{self, control};
use rustls_pki_types::ServerName;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

/// Connect timeout, handshake timeout, and ack-wait timeout all share this one knob -- same
/// default as `otlp_out`'s `DEFAULT_TIMEOUT`, for the same reason (a generous but real per-attempt
/// cap; `write_loop`'s own retry budget is the outer, much larger bound).
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// `crate::tls::TlsClientSettings`, re-exported here for symmetry with `crate::otlp`'s own path
/// (both sinks share the one definition in `crate::tls`).
pub use crate::tls::TlsClientSettings;

/// A live, handshaken connection -- everything about it that only exists once the handshake has
/// actually happened.
struct Conn {
    stream: Box<dyn AsyncStream>,
    /// The listener's own frame-size ceiling, from `HelloAck.max_frame_bytes` -- a batch framed
    /// larger than this is rejected locally (`Fault::Permanent`) rather than sent and rejected by
    /// the peer.
    peer_max_frame_bytes: u32,
    /// The negotiated compression -- the intersection of what this sink offered and what the
    /// peer's `HelloAck` chose, which may be `None` even if this sink offered `Lz4` (the peer
    /// doesn't support it).
    compression: Compression,
    /// The sequence number of the last data frame sent on this connection -- implicit, per
    /// `docs/plans/native-transport.md`'s "Sequence numbers" decision: the Nth data frame is
    /// always seq N, so `Ack.seq` need only be checked for equality, never carried on the frame
    /// itself.
    seq: u64,
}

/// A plain `TcpStream` or a TLS-wrapped one, behind one object-safe trait so [`Conn`] doesn't
/// need to be generic (a sink field can't be, without boxing the whole [`LogitOutput`] itself
/// generic in a way `logit-cli::pipeline::build_spec` would have to know about).
trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncStream for T {}

pub struct LogitOutput {
    endpoint: String,
    /// Offered in this sink's own `Hello`; the connection's actual [`Conn::compression`] may
    /// still end up `None` if the peer doesn't support it (`docs/plans/native-transport.md`'s
    /// "Compression" decision).
    compression: Compression,
    timeout: Duration,
    tls: Option<Arc<rustls::ClientConfig>>,
    diag: Diagnostics,
    telemetry: Telemetry,
    stream: Option<Conn>,
    /// `true` once this sink has ever completed a handshake -- the very first connect is not a
    /// "reconnect," only every one after it (`logit.output.reconnects`'s own doc comment on
    /// [`LogitOutput::connect_and_handshake`]).
    has_connected_once: bool,
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
        }
    }

    /// Offers `compression` in this sink's `Hello` -- the peer may still negotiate it down to
    /// `None` (never up: a `logit_in` this sink doesn't yet know about can't be assumed to
    /// support more than the baseline).
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Connect, handshake, and ack-wait timeout, all sharing this one knob. Default 10s, as
    /// `otlp_out`.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Turns on TLS for this connection (`tls:` in config) -- presence turns it on, the
    /// `otlp_in`/`logit_in` server-side precedent, since `endpoint` here is a bare `host:port`
    /// (the `syslog_out` shape) with no scheme to select TLS the way `otlp_out`'s URL-shaped
    /// endpoint does.
    pub fn with_tls(
        mut self,
        settings: &TlsClientSettings,
        base_dir: &Path,
    ) -> anyhow::Result<Self> {
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

    /// Connects, performs the TLS handshake if configured, then the `Hello`/`HelloAck` protocol
    /// handshake. Every step is raced against `self.timeout`. On success, records
    /// `logit.output.reconnects` -- but only from the *second* successful connect onward; the
    /// very first connection this sink ever makes isn't a "re"-connect.
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
            codecs: vec![native::CODEC_NATIVE_V1],
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
                return Err(anyhow::anyhow!(
                    "logit_in rejected this connection (code {}): {}",
                    reject.code,
                    reject.message
                ))
                .context(Fault::Permanent);
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
        Ok(Conn { stream, peer_max_frame_bytes: ack.max_frame_bytes, compression, seq: 0 })
    }
}

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

fn fault_tag(fault: Fault) -> &'static str {
    match fault {
        Fault::Clean => "clean",
        Fault::Ambiguous => "ambiguous",
        Fault::Permanent => "permanent",
    }
}

/// The host part of a bare `host:port` endpoint -- `rsplit_once` so a bracketed IPv6 literal's
/// own colons don't confuse this (an IPv6 endpoint here would need brackets, `[::1]:1234`, the
/// same convention `syslog_out`/every other bare `host:port` field in this codebase leaves to the
/// operator to write correctly; this only avoids splitting on the wrong colon, not validating the
/// address itself).
fn host_only(endpoint: &str) -> &str {
    endpoint
        .rsplit_once(':')
        .map(|(host, _port)| host)
        .unwrap_or(endpoint)
        .trim_start_matches('[')
        .trim_end_matches(']')
}

#[async_trait::async_trait]
impl Output for LogitOutput {
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let payload = native::encode_batch(batch);
        if payload.len() as u64 > frame::MAX_SANE_UNCOMPRESSED_LEN as u64 {
            self.diag.warn_throttled(
                "frame_too_large",
                format!(
                    "batch encodes to {} bytes, over the {}-byte sanity cap -- dropping it \
                     rather than ever attempting to send it",
                    payload.len(),
                    frame::MAX_SANE_UNCOMPRESSED_LEN
                ),
            );
            return Err(anyhow::anyhow!("batch too large to send")).context(Fault::Permanent);
        }

        let mut conn = match self.stream.take() {
            Some(conn) => conn,
            None => self.connect_and_handshake().await?,
        };

        let bound = conn.peer_max_frame_bytes.min(frame::MAX_SANE_UNCOMPRESSED_LEN);
        if payload.len() as u32 > bound {
            // This connection is otherwise fine -- only this batch doesn't fit -- so it's kept,
            // not dropped, and nothing is written to the socket for it at all.
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

        let framed =
            frame::write_frame_with_flags(native::CODEC_NATIVE_V1, conn.compression, 0, &payload)
                .context(Fault::Permanent)?;

        // `syslog_out::send_tcp`'s two-property rule: a single `write` first to learn whether
        // anything left at all, `write_all` only for the remainder -- never resend once any byte
        // of this frame reached the peer.
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
                // Nothing left this host at all -- safe to retry the whole batch fresh; this
                // connection is dropped (not put back) so the next `send` reconnects.
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
                ("codec", "native_v1"),
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
                self.telemetry.count(
                    "logit.output.requests",
                    1.0,
                    &[("class", fault_tag(Fault::Permanent))],
                );
                return Err(anyhow::anyhow!(
                    "logit_in rejected this connection (code {}): {}",
                    reject.code,
                    reject.message
                ))
                .context(Fault::Permanent);
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

/// Writes one control message, framed with [`frame::FLAG_CONTROL`] set -- mirrors
/// `logit_inputs::logit`'s own `write_control` exactly (the two sides speak the same framing;
/// duplicated rather than shared since sharing it would mean a new cross-crate dependency for one
/// tiny function).
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
// `HelloAck`/`Reject` are only ever written by a *peer* in production (this sink only ever
// writes `Hello`) -- these two impls exist solely so the test module's hand-rolled fake peer can
// reuse `write_control` instead of a third copy of the framing dance.
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

/// Reads one whole control frame off `stream` and decodes it -- no `max_frame_bytes` bound here
/// (unlike `logit_inputs::logit`'s server-side reader): a control message is always tiny, and
/// this sink trusts the peer it just successfully TLS/protocol-handshaked with for the length of
/// one connection.
async fn read_control<S: AsyncRead + Unpin>(
    stream: &mut S,
) -> anyhow::Result<control::ControlMessage> {
    let mut header_buf = [0u8; frame::HEADER_LEN];
    stream.read_exact(&mut header_buf).await?;
    let mut header_bytes = Bytes::copy_from_slice(&header_buf);
    let header = frame::FrameHeader::read(&mut header_bytes)
        .map_err(|e| anyhow::Error::new(e).context("reading a control frame header"))?;
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
    use std::sync::Arc;
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
            },
        );
        EventBatch { resource: Arc::new(Resource::default()), events: vec![event] }
    }

    /// Spins up a real `LogitInput` on an ephemeral port and returns its address plus a
    /// receiver for every batch it forwards -- the round-trip tests drive a real listener rather
    /// than a hand-rolled one, so nothing here can drift from what `logit_in` actually does.
    async fn spawn_real_listener() -> (String, mpsc::Receiver<logit_pipeline::Delivered>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        let mut input = LogitInput::new(addr.clone());
        let (tx, rx) = mpsc::channel(16);
        let sink = Fanout::new(vec![tx]);
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
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

    #[tokio::test]
    async fn a_cancelled_send_dropped_mid_await_leaves_stream_none() {
        // A peer that acks the handshake and then goes silent forever, so `send`'s ack read
        // can only ever be *cancelled*, never resolve on its own -- deterministic, unlike racing
        // against a fast, live peer where `send` might occasionally complete before the other
        // branch of a `select!` ever gets to fire.
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

    /// A minimal hand-rolled peer for the fault-classification cases a real `LogitInput` can't
    /// easily be made to misbehave into (rejecting on purpose, acking nothing, closing mid-frame).
    /// Accepts exactly one connection, reads its `Hello`, then does whatever `respond` says.
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
                // Read (and discard) exactly one data frame, then close without acking --
                // exercises both "ack never arrives" (a timeout) and "peer closes mid-frame"
                // (the read simply returns EOF instead of an Ack).
                let mut header = [0u8; frame::HEADER_LEN];
                if stream.read_exact(&mut header).await.is_ok() {
                    let mut header_bytes = Bytes::copy_from_slice(&header);
                    if let Ok(h) = frame::FrameHeader::read(&mut header_bytes) {
                        let mut body = vec![0u8; h.compressed_len as usize];
                        let _ = stream.read_exact(&mut body).await;
                    }
                }
                // Drop `stream` here -- closes the connection with no `Ack` ever sent.
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
                // Never read anything else, never close -- unlike `AckThenClose`, the connection
                // stays open and silent, so a caller's ack read can only ever be *cancelled*,
                // never resolve (successfully or with an error) on its own.
                std::future::pending::<()>().await;
            }
        }
    }

    enum FakePeerBehavior {
        Reject(control::Reject),
        AckThenClose { ack_compression: u8 },
        AckThenHang { ack_compression: u8 },
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

    /// Same shape as the ack-timeout case above (a peer that acks nothing), but exercises the
    /// half this test also names: the *next* `send` reconnects cleanly rather than trying to
    /// reuse anything from the dead connection.
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

        // Point the same sink at a real listener and confirm it reconnects from scratch --
        // nothing from the dead connection (its address, its negotiated compression) is reused.
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

        // Establish a connection at the listener's default (64 MiB) frame-size ceiling first.
        output.send(&sample_batch()).await.expect("first send should succeed");
        recv_batch(&mut rx).await;

        // Force the *connection's* own bound down without a second listener: reach into the
        // live `Conn` directly (test-only, same crate) rather than standing up a second
        // `LogitInput` with a tiny `with_max_frame_bytes` just to get a small negotiated bound.
        output.stream.as_mut().unwrap().peer_max_frame_bytes = 8;

        let err = output.send(&sample_batch()).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Permanent);
        assert!(
            output.stream.is_some(),
            "an oversized batch must not drop an otherwise-good connection"
        );

        // The connection is still good for a batch that actually fits.
        output.stream.as_mut().unwrap().peer_max_frame_bytes = frame::MAX_SANE_UNCOMPRESSED_LEN;
        output.send(&sample_batch()).await.expect("a normal batch should still send fine");
        recv_batch(&mut rx).await;
    }
}
