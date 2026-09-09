//! `logit_in` -- the native `logit`-to-`logit` listener side
//! (`docs/design/wire-protocol.md`'s connection protocol, `docs/plans/native-transport.md`
//! workstream C). Accepts many TCP (optionally TLS) connections; each speaks a version/codec/
//! compression handshake (`Hello`/`HelloAck`, `logit_proto::native::control`), then a loop of
//! one native frame in, one `Fanout::send`, one `Ack` out.
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
//! **Connection limit.** Unlike `otlp_in`'s [`crate::otlp::MAX_CONCURRENT_CONNECTIONS`] (a
//! blocking `acquire_owned` -- the 1025th connection just waits for a permit), this listener uses
//! a non-blocking `try_acquire_owned`: at capacity, a connecting client gets a clean `Reject` and
//! the connection closes immediately rather than hanging with a handshake that never starts.
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
//! **Pre-`Hello` timeout.** [`LogitInput::handshake_timeout`] (field, defaulted to 5s) bounds each
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

use crate::Input;
use bytes::{Bytes, BytesMut};
use logit_core::{Diagnostics, Telemetry};
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

/// Default for [`LogitInput::handshake_timeout`] -- how long a connection has, in total, to
/// finish its TLS accept (if configured) and send `Hello` before this listener gives up on it --
/// generous enough for a loaded peer under TLS, tight enough that a connection opened and then
/// abandoned (a port scan, a misconfigured health check, or a TLS client that never sends its
/// ClientHello) doesn't pin a connection-limit permit forever.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// See this module's own doc comment's "Connection limit" section for why this listener rejects
/// outright rather than queuing, unlike `otlp_in`.
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
    /// See this module's own doc comment's "Pre-`Hello` timeout" section.
    handshake_timeout: Duration,
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
            handshake_timeout: HANDSHAKE_TIMEOUT,
        }
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

    /// Test-only override of [`HANDSHAKE_TIMEOUT`] -- shortens the pre-`Hello` budget so a test
    /// can observe it actually firing (releasing a permit, timing out a silent TLS accept)
    /// without a multi-second sleep.
    #[cfg(test)]
    fn with_handshake_timeout(mut self, handshake_timeout: Duration) -> Self {
        self.handshake_timeout = handshake_timeout;
        self
    }
}

#[async_trait::async_trait]
impl Input for LogitInput {
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
        let listener = TcpListener::bind(&self.bind).await?;
        let connection_limit = Arc::new(tokio::sync::Semaphore::new(self.max_connections));
        let tls_acceptor = self.tls.clone().map(TlsAcceptor::from);
        let live_connections = Arc::new(AtomicI64::new(0));
        let max_frame_bytes = self.max_frame_bytes;
        let handshake_timeout = self.handshake_timeout;

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
#[allow(clippy::too_many_arguments)] // one small helper is clearer here than a params struct for 8 mostly-unrelated threaded-through values
async fn reject_or_serve<S: AsyncRead + AsyncWrite + Unpin + Send>(
    mut stream: S,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    sink: Fanout,
    telemetry: Telemetry,
    max_frame_bytes: u32,
    handshake_timeout: Duration,
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

    live_connections.fetch_add(1, Ordering::Relaxed);
    telemetry.gauge(
        "logit.input.connections",
        live_connections.load(Ordering::Relaxed) as f64,
        &[],
    );

    let result = serve_connection(
        stream,
        sink,
        telemetry.clone(),
        max_frame_bytes,
        handshake_timeout,
        shutdown,
    )
    .await;

    live_connections.fetch_sub(1, Ordering::Relaxed);
    telemetry.gauge(
        "logit.input.connections",
        live_connections.load(Ordering::Relaxed) as f64,
        &[],
    );

    result
}

/// What the handshake negotiated for one connection.
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
            let reject = control::Reject {
                code: control::REJECT_GOING_AWAY,
                message: "listener shutting down".to_string(),
            };
            let _ = write_control(&mut stream, &reject).await;
            return Ok(());
        }
        // `shutdown.changed()` here, not `wait_for` -- `wait_for`'s `Ref` guard makes the
        // `select!`'s combined future `!Send` the moment any arm (like this one) awaits
        // something afterward, which `tokio::spawn`ing this connection's task requires. `changed`
        // has no such guard and, given the explicit check just above, is equivalent here:
        // `shutdown` only ever flips false -> true once, and this receiver hasn't observed that
        // flip yet (the check above would have caught it if it had already happened).
        let header_buf = tokio::select! {
            result = read_header(&mut stream) => result?,
            _ = shutdown.changed() => {
                let reject = control::Reject {
                    code: control::REJECT_GOING_AWAY,
                    message: "listener shutting down".to_string(),
                };
                let _ = write_control(&mut stream, &reject).await;
                return Ok(());
            }
        };
        let Some(header_buf) = header_buf else {
            // Clean close at a frame boundary -- the ordinary way a connection ends.
            return Ok(());
        };

        let (header, mut payload) =
            match read_frame_body(&mut stream, header_buf, max_frame_bytes).await {
                Ok(v) => v,
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
        if header.codec != native::CODEC_NATIVE_V1 {
            telemetry.count("logit.proto.errors", 1.0, &[("reason", "codec")]);
            anyhow::bail!("frame codec byte {}, expected native v1", header.codec);
        }

        let batch = native::decode_batch(&mut payload).map_err(|err| {
            telemetry.count("logit.proto.errors", 1.0, &[("reason", "magic")]);
            anyhow::Error::new(err).context("decoding a native batch")
        })?;

        telemetry.count(
            "logit.proto.frames",
            1.0,
            &[("direction", "in"), ("codec", "native_v1"), ("compression", compression)],
        );
        telemetry.count(
            "logit.proto.frame.bytes",
            header.compressed_len as f64,
            &[("direction", "in")],
        );

        // The ack point: written only after the batch is in every downstream inbox
        // (`Fanout::send` returns once every consumer has accepted it) -- see this module's own
        // doc comment.
        sink.send(batch).await;

        seq += 1;
        write_control(&mut stream, &control::Ack { seq }).await?;
    }
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
        let Some(header_buf) = read_header(stream).await? else {
            anyhow::bail!("connection closed before sending Hello");
        };
        read_frame_body(stream, header_buf, max_frame_bytes)
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

    if !hello.codecs.contains(&native::CODEC_NATIVE_V1) {
        let reject = control::Reject {
            code: control::REJECT_NO_COMMON_CODEC,
            message: "this listener only speaks native v1".to_string(),
        };
        let _ = write_control(stream, &reject).await;
        anyhow::bail!("client offered no codec this listener speaks: {:?}", hello.codecs);
    }

    // Compression always has a safe fallback (`None`), so there is no reject path for it --
    // unlike codec, where no shared choice means the connection genuinely cannot proceed.
    let compression = if hello.compressions.contains(&(Compression::Lz4 as u8)) {
        Compression::Lz4
    } else {
        Compression::None
    };

    let ack = control::HelloAck {
        version: control::PROTOCOL_VERSION,
        codec: native::CODEC_NATIVE_V1,
        compression: compression as u8,
        max_frame_bytes,
        window: 1,
    };
    write_control(stream, &ack).await?;
    Ok(Negotiated { compression })
}

/// Reads exactly [`frame::HEADER_LEN`] bytes off `stream`, distinguishing "the peer closed
/// cleanly with nothing pending" (`Ok(None)`) from "the peer closed mid-header" (a real error) --
/// the distinction `read_exact` alone can't make, since it only reports success or failure, never
/// how many bytes it managed before EOF. This is also exactly the step `serve_connection`'s
/// per-frame `select!` races against `shutdown`: an idle connection is one blocked here, in the
/// very first read of a frame boundary.
async fn read_header<S: AsyncRead + Unpin>(
    stream: &mut S,
) -> anyhow::Result<Option<[u8; frame::HEADER_LEN]>> {
    let mut buf = [0u8; frame::HEADER_LEN];
    let mut filled = 0usize;
    loop {
        let n = stream.read(&mut buf[filled..]).await?;
        if n == 0 {
            if filled == 0 {
                return Ok(None);
            }
            anyhow::bail!("connection closed mid-header ({filled}/{} bytes)", frame::HEADER_LEN);
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
}

impl FrameReadError {
    fn into_inner(self) -> anyhow::Error {
        match self {
            FrameReadError::TooLarge(e)
            | FrameReadError::Truncated(e)
            | FrameReadError::Crc(e)
            | FrameReadError::Malformed(e) => e,
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
async fn read_frame_body<S: AsyncRead + Unpin>(
    stream: &mut S,
    header_buf: [u8; frame::HEADER_LEN],
    max_frame_bytes: u32,
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

    let mut body = vec![0u8; header.compressed_len as usize];
    stream.read_exact(&mut body).await.map_err(|e| {
        FrameReadError::Truncated(anyhow::Error::new(e).context("reading a frame body"))
    })?;

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

    async fn bound_input() -> (String, LogitInput) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        (addr.to_string(), LogitInput::new(addr.to_string()))
    }

    fn fanout_into_channel(capacity: usize) -> (Fanout, mpsc::Receiver<logit_pipeline::Delivered>) {
        let (tx, rx) = mpsc::channel(capacity);
        (Fanout::new(vec![tx]), rx)
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
            },
        );
        EventBatch { resource: Arc::new(Resource::default()), events: vec![event] }
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
        let header_buf =
            read_header(stream).await.unwrap().expect("expected a frame, got a clean close");
        read_frame_body(stream, header_buf, frame::MAX_SANE_UNCOMPRESSED_LEN)
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
                    MetricKind::Counter(v) => Some(v),
                    _ => None,
                }
            })
        })
    }

    // ---- handshake ------------------------------------------------------------------------

    #[tokio::test]
    async fn handshake_happy_path_returns_the_negotiated_compression() {
        let (addr, input) = bound_input().await;
        let (sink, _rx) = fanout_into_channel(16);
        let mut input = input;
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

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
        tokio::time::sleep(Duration::from_millis(50)).await;

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
        tokio::time::sleep(Duration::from_millis(50)).await;

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
        tokio::time::sleep(Duration::from_millis(50)).await;

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
        tokio::time::sleep(Duration::from_millis(50)).await;

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
        tokio::time::sleep(Duration::from_millis(50)).await;

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
        tokio::time::sleep(Duration::from_millis(50)).await;

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
        tokio::time::sleep(Duration::from_millis(50)).await;

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
        tokio::time::sleep(Duration::from_millis(50)).await;

        // First connection: fills the one available slot, stays open and idle (never
        // handshakes -- holding the permit is all that matters here).
        let _first = connect(&addr).await;
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
        let header_buf =
            read_header(stream).await.unwrap().expect("expected a frame, got a clean close");
        let (header, mut payload) =
            read_frame_body(stream, header_buf, frame::MAX_SANE_UNCOMPRESSED_LEN)
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
        tokio::time::sleep(Duration::from_millis(50)).await;

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
        tokio::time::sleep(Duration::from_millis(50)).await;

        // First connection: raw TCP, holds the one permit for the (default, 5s) handshake
        // timeout -- plenty of time for the rest of this test.
        let _first = connect(&addr).await;
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
}
