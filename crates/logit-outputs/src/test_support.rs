//! Receivers that sink tests send to: a TCP, TLS, or UDP collector that reports each message on
//! a channel, and the `testdata/tls` fixtures a TLS collector and client are built from. Also the
//! stream doubles: [`FakeStream`] for the sinks' plaintext `Box<dyn AsyncStream>` seam, and
//! [`tls_pair`] with [`TapIo`] for tests that need real tokio-rustls behavior.
//!
//! A [`Collector`] message depends on how it reads:
//!
//! - [`ReadMode::ToEof`]: one message per connection, holding every byte read until the peer
//!   closed (or the read failed). A test drops the sink before [`Collector::next`], so the
//!   connection reaches EOF.
//! - [`ReadMode::FirstReadThenClose`]: the first read's bytes, sent after the collector has
//!   dropped the stream. Receiving the message implies the collector's FIN is already on its way
//!   to the sink, which is the observable a pooled-connection test waits on.
//! - [`Collector::udp`]: one message per datagram.
//!
//! Each connection runs on its own task, so a stalled or rejected connection never delays
//! another; messages from different connections arrive in the order the connections finish.
//! [`Collector::accepts`] counts accepted connections, or for [`Collector::tls`] completed
//! handshakes, so a rejected client never counts. A connection is counted before its message is
//! sent, so a test reads the count after [`Collector::take`].

use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::Duration;

use logit_pipeline::test_util::RECV_TIMEOUT;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, DuplexStream, ReadBuf};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc;
use tokio::task::AbortHandle;
use tokio_rustls::TlsAcceptor;

use crate::tls::TlsClientSettings;

/// How a TCP or TLS [`Collector`] reads each connection; the module doc says what one message
/// is under each.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ReadMode {
    ToEof,
    FirstReadThenClose,
}

/// A bound receiver reporting each message on a channel. Dropping it stops the accept loop.
pub(crate) struct Collector {
    addr: SocketAddr,
    rx: mpsc::UnboundedReceiver<Vec<u8>>,
    accepts: Arc<AtomicUsize>,
    task: AbortHandle,
}

impl Collector {
    /// A plaintext TCP collector on `127.0.0.1`.
    pub(crate) async fn tcp(mode: ReadMode) -> Self {
        Self::stream(None, mode).await
    }

    /// A TLS collector on `127.0.0.1`; [`Collector::accepts`] counts completed handshakes.
    pub(crate) async fn tls(acceptor: TlsAcceptor, mode: ReadMode) -> Self {
        Self::stream(Some(acceptor), mode).await
    }

    /// A UDP collector on `127.0.0.1`: one message per datagram.
    pub(crate) async fn udp() -> Self {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut buf = vec![0u8; 65536];
            while let Ok((n, _)) = socket.recv_from(&mut buf).await {
                if tx.send(buf[..n].to_vec()).is_err() {
                    break;
                }
            }
        });
        Self { addr, rx, accepts: Arc::new(AtomicUsize::new(0)), task: task.abort_handle() }
    }

    async fn stream(acceptor: Option<TlsAcceptor>, mode: ReadMode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        let accepts = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&accepts);
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let (acceptor, tx, counter) = (acceptor.clone(), tx.clone(), Arc::clone(&counter));
                tokio::spawn(async move {
                    match acceptor {
                        None => {
                            counter.fetch_add(1, Ordering::SeqCst);
                            read_one(stream, mode, &tx).await;
                        }
                        Some(acceptor) => {
                            let Ok(stream) = acceptor.accept(stream).await else { return };
                            counter.fetch_add(1, Ordering::SeqCst);
                            read_one(stream, mode, &tx).await;
                        }
                    }
                });
            }
        });
        Self { addr, rx, accepts, task: task.abort_handle() }
    }

    pub(crate) fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Connections accepted, or TLS handshakes completed.
    pub(crate) fn accepts(&self) -> usize {
        self.accepts.load(Ordering::SeqCst)
    }

    /// The next message, panicking after [`RECV_TIMEOUT`].
    pub(crate) async fn next(&mut self) -> Vec<u8> {
        match tokio::time::timeout(RECV_TIMEOUT, self.rx.recv()).await {
            Ok(Some(message)) => message,
            Ok(None) => panic!("collector on {} stopped before a message arrived", self.addr),
            Err(_) => {
                panic!("timed out after {RECV_TIMEOUT:?} waiting for a message on {}", self.addr)
            }
        }
    }

    /// The next `n` messages, each under [`Collector::next`]'s timeout.
    pub(crate) async fn take(&mut self, n: usize) -> Vec<Vec<u8>> {
        let mut messages = Vec::with_capacity(n);
        for _ in 0..n {
            messages.push(self.next().await);
        }
        messages
    }

    /// Asserts no message arrives within `window`, a negative window sized per
    /// `docs/adr/test-timing-and-observables.md`'s first rule.
    pub(crate) async fn assert_quiet(&mut self, window: Duration, what: &str) {
        if let Ok(Some(message)) = tokio::time::timeout(window, self.rx.recv()).await {
            let message = String::from_utf8_lossy(&message);
            panic!("{what}: expected no message within {window:?}, got {message:?}");
        }
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn read_one<S: AsyncRead + Unpin>(
    mut stream: S,
    mode: ReadMode,
    tx: &mpsc::UnboundedSender<Vec<u8>>,
) {
    let buf = match mode {
        ReadMode::ToEof => {
            let mut buf = Vec::new();
            let _ = stream.read_to_end(&mut buf).await;
            buf
        }
        ReadMode::FirstReadThenClose => {
            let mut buf = vec![0u8; 8192];
            let Ok(n) = stream.read(&mut buf).await else { return };
            buf.truncate(n);
            // Closed before the send, so a received message implies the close.
            drop(stream);
            buf
        }
    };
    let _ = tx.send(buf);
}

/// The repo root's `testdata/tls` (`testdata/tls/README.md`).
pub(crate) fn testdata_dir() -> PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
}

/// Default client TLS settings with `overrides` applied.
pub(crate) fn tls_settings(overrides: impl FnOnce(&mut TlsClientSettings)) -> TlsClientSettings {
    let mut settings = TlsClientSettings::default();
    overrides(&mut settings);
    settings
}

/// A `rustls::ServerConfig` presenting `testdata/tls/server.{pem,key}` (SANs `localhost` and
/// `127.0.0.1`), optionally requiring a client certificate chaining to `testdata/tls/ca.pem`. No
/// ALPN: neither RFC 5425 syslog nor statsd over TLS has an identifier.
pub(crate) fn server_tls_config(require_client_auth: bool) -> Arc<rustls::ServerConfig> {
    use rustls_pki_types::pem::PemObject;
    use rustls_pki_types::{CertificateDer, PrivateKeyDer};

    let dir = testdata_dir();
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(dir.join("server.pem"))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let key = PrivateKeyDer::from_pem_file(dir.join("server.key")).unwrap();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap();
    let cfg = if require_client_auth {
        let mut roots = rustls::RootCertStore::empty();
        let ca: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(dir.join("ca.pem"))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        roots.add_parsable_certificates(ca);
        let verifier =
            rustls::server::WebPkiClientVerifier::builder(Arc::new(roots)).build().unwrap();
        builder.with_client_cert_verifier(verifier).with_single_cert(chain, key).unwrap()
    } else {
        builder.with_no_client_auth().with_single_cert(chain, key).unwrap()
    };
    Arc::new(cfg)
}

/// A scripted in-memory stream standing in for the connection behind the sinks'
/// `Box<dyn AsyncStream>` seam, with shared state a test inspects after the call under test.
///
/// Writes are accepted whole unless a [`WriteStep`] is scripted for that call. Accepted bytes
/// land in [`FakeState::unflushed`]; a successful `flush` moves them to [`FakeState::flushed`], so
/// a test tells "the stream took it" from "the stream was told to deliver it". Reads follow one
/// [`ReadStep`], `Pending` by default.
///
/// Never wrap this in tokio-rustls. It returns `Ok(0)` or `Pending` without registering a waker,
/// and tokio-rustls treats an IO `Ok(0)` as would-block, so a TLS write over it can park forever
/// with nothing to wake it (`crate::stream_pins`). TLS behavior is tested against a real
/// tokio-rustls pair ([`tls_pair`]) instead.
#[derive(Clone, Default)]
pub(crate) struct FakeStream(Arc<Mutex<FakeState>>);

/// What a [`FakeStream`] has seen, and its script.
#[derive(Default)]
pub(crate) struct FakeState {
    /// Accepted by `write` and not yet flushed.
    pub(crate) unflushed: Vec<u8>,
    /// Moved out of `unflushed` by a successful `flush`.
    pub(crate) flushed: Vec<u8>,
    /// `poll_write` calls, including scripted failures.
    pub(crate) writes: usize,
    /// `poll_flush` calls, including a scripted failure.
    pub(crate) flushes: usize,
    /// `poll_read` calls.
    pub(crate) reads: usize,
    /// Per 1-based write call number.
    write_steps: Vec<(usize, WriteStep)>,
    flush_error: Option<io::ErrorKind>,
    read: ReadStep,
}

/// A scripted outcome for one `write` call of a [`FakeStream`].
#[derive(Clone, Copy, Debug)]
pub(crate) enum WriteStep {
    /// Accepts at most this many bytes.
    Short(usize),
    /// Returns `Ok(0)`, accepting nothing.
    Zero,
    /// Fails with this kind, accepting nothing.
    Fail(io::ErrorKind),
}

/// How every `read` of a [`FakeStream`] answers.
#[derive(Clone, Debug, Default)]
pub(crate) enum ReadStep {
    /// `Pending`, as a live, quiet peer is. No waker is registered, so a test that awaits a read
    /// here hangs instead of passing.
    #[default]
    Pending,
    /// `Ready(Ok(()))` with nothing filled: the peer closed.
    Eof,
    /// `Ready(Err(kind))`.
    Fail(io::ErrorKind),
    /// Unsolicited bytes, handed out across reads; `Pending` once they run out.
    Bytes(Vec<u8>),
}

impl FakeStream {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Scripts the `call`th write (1-based).
    pub(crate) fn on_write(self, call: usize, step: WriteStep) -> Self {
        self.state().write_steps.push((call, step));
        self
    }

    /// Every `flush` fails with `kind` and moves nothing.
    pub(crate) fn failing_flush(self, kind: io::ErrorKind) -> Self {
        self.state().flush_error = Some(kind);
        self
    }

    pub(crate) fn reading(self, step: ReadStep) -> Self {
        self.state().read = step;
        self
    }

    pub(crate) fn state(&self) -> MutexGuard<'_, FakeState> {
        self.0.lock().unwrap()
    }
}

impl AsyncWrite for FakeStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut state = self.state();
        state.writes += 1;
        let call = state.writes;
        let step = state.write_steps.iter().find(|(n, _)| *n == call).map(|(_, step)| *step);
        let accepted = match step {
            None => buf.len(),
            Some(WriteStep::Short(n)) => n.min(buf.len()),
            Some(WriteStep::Zero) => 0,
            Some(WriteStep::Fail(kind)) => {
                return Poll::Ready(Err(io::Error::new(kind, "scripted write failure")));
            }
        };
        state.unflushed.extend_from_slice(&buf[..accepted]);
        Poll::Ready(Ok(accepted))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut state = self.state();
        state.flushes += 1;
        if let Some(kind) = state.flush_error {
            return Poll::Ready(Err(io::Error::new(kind, "scripted flush failure")));
        }
        let unflushed = std::mem::take(&mut state.unflushed);
        state.flushed.extend_from_slice(&unflushed);
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncRead for FakeStream {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut state = self.state();
        state.reads += 1;
        match &mut state.read {
            ReadStep::Pending => Poll::Pending,
            ReadStep::Eof => Poll::Ready(Ok(())),
            ReadStep::Fail(kind) => {
                Poll::Ready(Err(io::Error::new(*kind, "scripted read failure")))
            }
            ReadStep::Bytes(bytes) if bytes.is_empty() => Poll::Pending,
            ReadStep::Bytes(bytes) => {
                let n = bytes.len().min(buf.remaining());
                buf.put_slice(&bytes[..n]);
                bytes.drain(..n);
                Poll::Ready(Ok(()))
            }
        }
    }
}

/// Wraps the IO beneath a real TLS session and counts what crosses it, so a test sees what a TLS
/// call moved to or from the socket. Its [`Tap`] can also arm write failures.
pub(crate) struct TapIo<T> {
    inner: T,
    tap: Arc<Tap>,
}

/// A [`TapIo`]'s shared counters and write script.
#[derive(Default)]
pub(crate) struct Tap {
    read: AtomicUsize,
    written: AtomicUsize,
    writes: Mutex<TapWrites>,
}

#[derive(Clone, Copy, Default)]
enum TapWrites {
    #[default]
    Pass,
    /// Passes this many more bytes, then every write fails with `BrokenPipe`.
    FailAfter(usize),
    /// Every write returns `Ok(0)`.
    Zero,
}

impl Tap {
    /// Bytes the wrapped IO has returned from reads.
    pub(crate) fn read(&self) -> usize {
        self.read.load(Ordering::SeqCst)
    }

    /// Bytes the wrapped IO has accepted from writes.
    pub(crate) fn written(&self) -> usize {
        self.written.load(Ordering::SeqCst)
    }

    /// From now on, writes pass `bytes` more bytes in total and then fail with `BrokenPipe`.
    pub(crate) fn fail_writes_after(&self, bytes: usize) {
        *self.writes.lock().unwrap() = TapWrites::FailAfter(bytes);
    }

    /// From now on, every write returns `Ok(0)` without touching the wrapped IO.
    pub(crate) fn zero_writes(&self) {
        *self.writes.lock().unwrap() = TapWrites::Zero;
    }
}

impl<T> TapIo<T> {
    pub(crate) fn new(inner: T) -> (Self, Arc<Tap>) {
        let tap = Arc::new(Tap::default());
        (Self { inner, tap: Arc::clone(&tap) }, tap)
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for TapIo<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let result = std::task::ready!(Pin::new(&mut this.inner).poll_read(cx, buf));
        this.tap.read.fetch_add(buf.filled().len() - before, Ordering::SeqCst);
        Poll::Ready(result)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for TapIo<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let mut writes = this.tap.writes.lock().unwrap();
        let limit = match *writes {
            TapWrites::Pass => buf.len(),
            TapWrites::Zero => return Poll::Ready(Ok(0)),
            TapWrites::FailAfter(0) => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "armed write failure",
                )));
            }
            TapWrites::FailAfter(left) => left.min(buf.len()),
        };
        let result = std::task::ready!(Pin::new(&mut this.inner).poll_write(cx, &buf[..limit]));
        if let Ok(n) = result {
            this.tap.written.fetch_add(n, Ordering::SeqCst);
            if let TapWrites::FailAfter(left) = &mut *writes {
                *left -= n;
            }
        }
        Poll::Ready(result)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// One end of a [`tapped_duplex`] and its [`Tap`].
pub(crate) type TappedEnd = (TapIo<DuplexStream>, Arc<Tap>);

/// A tapped `tokio::io::duplex(capacity)` pair: the client end and its [`Tap`], then the server
/// end and its [`Tap`]. `capacity` bounds each direction's in-flight bytes, which is what makes a
/// TLS write stop mid-record deterministically.
pub(crate) fn tapped_duplex(capacity: usize) -> (TappedEnd, TappedEnd) {
    let (client, server) = tokio::io::duplex(capacity);
    (TapIo::new(client), TapIo::new(server))
}

/// A real tokio-rustls client and server over `client_io`/`server_io`, handshake complete. The
/// server presents `testdata/tls/server.pem`, and the client trusts `ca.pem` and names
/// `localhost`. Both sides negotiate TLS 1.3, and the server's session tickets, sent after the
/// handshake, wait unread in the client's inbound direction.
pub(crate) async fn tls_pair<C, S>(
    client_io: C,
    server_io: S,
) -> (tokio_rustls::client::TlsStream<C>, tokio_rustls::server::TlsStream<S>)
where
    C: AsyncRead + AsyncWrite + Unpin,
    S: AsyncRead + AsyncWrite + Unpin,
{
    tls_pair_with(client_io, server_io, server_tls_config(false)).await
}

/// [`tls_pair`] with a server that sends no TLS 1.3 session tickets. Over a pipe too small to hold
/// the tickets, the server's accept waits for the client to read them, and the client has
/// returned from its handshake, so a [`tls_pair`] there never completes.
pub(crate) async fn tls_pair_without_tickets<C, S>(
    client_io: C,
    server_io: S,
) -> (tokio_rustls::client::TlsStream<C>, tokio_rustls::server::TlsStream<S>)
where
    C: AsyncRead + AsyncWrite + Unpin,
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut config = (*server_tls_config(false)).clone();
    config.send_tls13_tickets = 0;
    tls_pair_with(client_io, server_io, Arc::new(config)).await
}

async fn tls_pair_with<C, S>(
    client_io: C,
    server_io: S,
    server_config: Arc<rustls::ServerConfig>,
) -> (tokio_rustls::client::TlsStream<C>, tokio_rustls::server::TlsStream<S>)
where
    C: AsyncRead + AsyncWrite + Unpin,
    S: AsyncRead + AsyncWrite + Unpin,
{
    let connector = tls_client_connector();
    let acceptor = TlsAcceptor::from(server_config);
    let name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
    let (client, server) =
        tokio::join!(connector.connect(name, client_io), acceptor.accept(server_io));
    (client.expect("client handshake"), server.expect("server handshake"))
}

/// A tokio-rustls client trusting `testdata/tls/ca.pem`, built as a sink builds its own.
pub(crate) fn tls_client_connector() -> tokio_rustls::TlsConnector {
    let settings = tls_settings(|s| s.ca_file = Some("ca.pem".to_string()));
    let client_config = crate::tls::build_client_config(&settings, &testdata_dir()).unwrap();
    tokio_rustls::TlsConnector::from(Arc::new(client_config))
}

/// One `poll_read`, never waiting: `Ready` with the byte count (0 is EOF), or `Pending`.
///
/// A tokio IO resource answers `Pending` once the task's cooperative budget runs out, whatever
/// it holds. A test that reads a `Pending` from here as "nothing available" runs its body under
/// `tokio::task::unconstrained`.
pub(crate) async fn read_once<S: AsyncRead + Unpin + ?Sized>(
    stream: &mut S,
    buf: &mut [u8],
) -> Poll<io::Result<usize>> {
    std::future::poll_fn(|cx| {
        let mut read_buf = ReadBuf::new(buf);
        Poll::Ready(match Pin::new(&mut *stream).poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(read_buf.filled().len())),
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => Poll::Pending,
        })
    })
    .await
}

/// Everything `stream` yields before a read is `Pending` or reads EOF, one [`read_once`] at a
/// time; the same budget rule applies. Panics on a read error.
pub(crate) async fn drain_available<S: AsyncRead + Unpin + ?Sized>(stream: &mut S) -> Vec<u8> {
    let mut out = Vec::new();
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        match read_once(stream, &mut buf).await {
            Poll::Pending | Poll::Ready(Ok(0)) => return out,
            Poll::Ready(Ok(n)) => out.extend_from_slice(&buf[..n]),
            Poll::Ready(Err(err)) => panic!("draining the stream failed: {err}"),
        }
    }
}
