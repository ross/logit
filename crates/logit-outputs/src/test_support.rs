//! Receivers that sink tests send to: a TCP, TLS, or UDP collector that reports each message on
//! a channel, [`http_recorder`] for the HTTP sinks, which records each request and answers it by
//! its index, [`answers_once`] and [`answers_once_unix`], which answer one request and refuse the
//! next connect, and the `testdata/tls` fixtures a TLS collector and client are built from. The
//! attempt-accounting helpers run a sink through the real write loop and compare its counters. Also the
//! stream doubles: [`FakeStream`] for the sinks' plaintext `Box<dyn AsyncStream>` seam,
//! [`ScriptedDial`] for the fresh connections `crate::stream`'s driver dials, and [`tls_pair`]
//! with [`TapIo`] for tests that need real tokio-rustls behavior. [`ScriptedDest`] is the datagram
//! double behind `crate::datagram`'s `Scripted` seams.
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
        Self::udp_at("127.0.0.1:0").await.unwrap()
    }

    /// A UDP collector bound to `bind`, or the bind's error, so a test can skip where an address
    /// family is unavailable (IPv6 loopback in some containers).
    pub(crate) async fn udp_at(bind: &str) -> io::Result<Self> {
        let socket = UdpSocket::bind(bind).await?;
        let addr = socket.local_addr()?;
        let (tx, rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut buf = vec![0u8; 65536];
            while let Ok((n, _)) = socket.recv_from(&mut buf).await {
                if tx.send(buf[..n].to_vec()).is_err() {
                    break;
                }
            }
        });
        Ok(Self { addr, rx, accepts: Arc::new(AtomicUsize::new(0)), task: task.abort_handle() })
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

/// The OS error number of the first `io::Error` in `err`'s chain.
pub(crate) fn errno_in(err: &anyhow::Error) -> Option<i32> {
    err.chain()
        .find_map(|cause| cause.downcast_ref::<io::Error>())
        .and_then(io::Error::raw_os_error)
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

/// `settings` built against `testdata/tls` as a sink builds them, registered with a reloader
/// nothing checks.
pub(crate) fn client_config(settings: &TlsClientSettings) -> rustls::ClientConfig {
    logit_pipeline::tls::build_client_config(
        settings,
        &testdata_dir(),
        &logit_pipeline::tls::TlsReloader::new(),
        &logit_core::Diagnostics::default(),
        &logit_core::Telemetry::default(),
    )
    .unwrap()
}

/// A `rustls::ServerConfig` presenting fixture `cert` (`testdata/tls/<cert>.pem` and `.key`),
/// requiring a client certificate chaining to fixture `client_ca` when given, built as a listener
/// builds its own.
pub(crate) fn fixture_server_config(
    cert: &str,
    client_ca: Option<&str>,
    alpn: &[&[u8]],
) -> Arc<rustls::ServerConfig> {
    let settings = logit_pipeline::tls::TlsServerSettings {
        cert_file: format!("{cert}.pem"),
        key_file: format!("{cert}.key"),
        client_ca_file: client_ca.map(|ca| format!("{ca}.pem")),
    };
    let cfg = logit_pipeline::tls::build_server_config(
        &settings,
        &testdata_dir(),
        alpn,
        &logit_pipeline::tls::TlsReloader::new(),
        &logit_core::Diagnostics::default(),
        &logit_core::Telemetry::default(),
    )
    .unwrap();
    Arc::new(cfg)
}

/// Fixtures copied into a fresh scratch directory as `(name, fixture)` pairs, so a reload test
/// can rewrite them with [`rewrite_tls_file`].
pub(crate) fn scratch_tls_files(label: &str, files: &[(&str, &str)]) -> PathBuf {
    let dir = logit_pipeline::test_util::scratch_dir(label);
    for (name, fixture) in files {
        std::fs::copy(testdata_dir().join(fixture), dir.join(name)).unwrap();
    }
    dir
}

/// Overwrites `dir/name` in place with `testdata/tls/<fixture>`.
pub(crate) fn rewrite_tls_file(dir: &std::path::Path, name: &str, fixture: &str) {
    std::fs::write(dir.join(name), std::fs::read(testdata_dir().join(fixture)).unwrap()).unwrap();
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

/// Connections handed out in order by `crate::stream::connect` for a
/// `crate::stream::Target::Scripted` dial, so a driver test scripts each fresh connection's
/// behavior and counts the dials.
pub(crate) struct ScriptedDial {
    steps: Mutex<std::collections::VecDeque<DialStep>>,
    dials: AtomicUsize,
    tls: bool,
}

/// What one scripted dial does.
pub(crate) enum DialStep {
    /// Succeeds with this connection.
    Connect(Box<dyn crate::tls::AsyncStream>),
    /// Fails `Fault::Clean`, as a refused connect does.
    Refuse,
    /// Never completes, as a dial to a blackholed peer.
    Hang,
}

impl ScriptedDial {
    /// A plaintext dial (`tls: false`) or one the driver treats as TLS. An exhausted script
    /// refuses.
    pub(crate) fn new(tls: bool, steps: impl IntoIterator<Item = DialStep>) -> Self {
        Self { steps: Mutex::new(steps.into_iter().collect()), dials: AtomicUsize::new(0), tls }
    }

    pub(crate) fn is_tls(&self) -> bool {
        self.tls
    }

    /// Dials started, including one still hanging.
    pub(crate) fn dials(&self) -> usize {
        self.dials.load(Ordering::SeqCst)
    }

    /// Scripted steps not yet used.
    pub(crate) fn unused(&self) -> usize {
        self.steps.lock().unwrap().len()
    }

    pub(crate) async fn connect(&self) -> anyhow::Result<Box<dyn crate::tls::AsyncStream>> {
        self.dials.fetch_add(1, Ordering::SeqCst);
        let step = self.steps.lock().unwrap().pop_front();
        match step {
            Some(DialStep::Connect(stream)) => Ok(stream),
            Some(DialStep::Refuse) | None => {
                Err(anyhow::anyhow!("scripted connect refused")
                    .context(logit_pipeline::Fault::Clean))
            }
            Some(DialStep::Hang) => std::future::pending().await,
        }
    }
}

/// A datagram destination for `crate::datagram`'s `Scripted` seams: each send follows the next
/// scripted [`SendStep`] (then [`SendStep::Accept`] once the script runs out), and an accepted
/// datagram is recorded with the entry count the packer said it holds.
#[derive(Default)]
pub(crate) struct ScriptedDest(Mutex<ScriptedState>);

/// What a [`ScriptedDest`] has seen, and its script.
#[derive(Default)]
pub(crate) struct ScriptedState {
    steps: std::collections::VecDeque<SendStep>,
    /// Accepted datagrams, each with its entry count (`None` through a Unix socket, which isn't
    /// told it).
    pub(crate) accepted: Vec<(Vec<u8>, Option<usize>)>,
    /// Send calls, whatever their outcome.
    pub(crate) sends: usize,
    /// Connects through a scripted Unix target.
    pub(crate) connects: usize,
}

/// A scripted outcome for one send of a [`ScriptedDest`].
#[derive(Clone, Copy, Debug)]
pub(crate) enum SendStep {
    Accept,
    /// Fails with the kernel's `EMSGSIZE`.
    TooLarge,
    /// Fails with this kind and no OS error number.
    Fail(io::ErrorKind),
    /// Never completes, as a send parked on a full receiver.
    Park,
}

impl ScriptedDest {
    pub(crate) fn new(steps: impl IntoIterator<Item = SendStep>) -> Arc<Self> {
        let dest = Self::default();
        dest.then(steps);
        Arc::new(dest)
    }

    /// Appends `steps` to the script.
    pub(crate) fn then(&self, steps: impl IntoIterator<Item = SendStep>) {
        self.state().steps.extend(steps);
    }

    pub(crate) fn state(&self) -> MutexGuard<'_, ScriptedState> {
        self.0.lock().unwrap()
    }

    /// The accepted datagrams' bytes.
    pub(crate) fn datagrams(&self) -> Vec<Vec<u8>> {
        self.state().accepted.iter().map(|(bytes, _)| bytes.clone()).collect()
    }

    pub(crate) fn connect(&self) {
        self.state().connects += 1;
    }

    pub(crate) async fn send(&self, datagram: &[u8], entries: Option<usize>) -> io::Result<usize> {
        let step = {
            let mut state = self.state();
            state.sends += 1;
            let step = state.steps.pop_front().unwrap_or(SendStep::Accept);
            if let SendStep::Accept = step {
                state.accepted.push((datagram.to_vec(), entries));
            }
            step
        };
        match step {
            SendStep::Accept => Ok(datagram.len()),
            SendStep::TooLarge => Err(io::Error::from_raw_os_error(90)),
            SendStep::Fail(kind) => Err(io::Error::new(kind, "scripted send failure")),
            SendStep::Park => std::future::pending().await,
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
    let client_config = client_config(&settings);
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

/// Every `Sum` series a sink and the runtime counted, keyed by name and sorted tags.
pub(crate) type Sums = std::collections::BTreeMap<(String, Vec<(String, String)>), f64>;

/// Counters that count once per attempt, which a retried batch counts once more per retry.
const PER_ATTEMPT: [&str; 4] = [
    "logit.output.requests",
    "logit.output.reconnects",
    "logit.component.errors",
    "logit.component.retries",
];

/// A write-loop config whose retries take a millisecond.
pub(crate) fn fast_retry() -> logit_pipeline::WriteLoopConfig {
    logit_pipeline::WriteLoopConfig {
        retry: logit_pipeline::RetryConfig {
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
        },
        ..logit_pipeline::WriteLoopConfig::default()
    }
}

/// Runs `batches` through the runtime's write loop over `output`, whose telemetry is `probe`'s
/// component `out`, and returns every `Sum` series counted.
pub(crate) async fn sums_through_write_loop<O: logit_pipeline::Output + Send>(
    output: &mut O,
    probe: &mut logit_pipeline::test_util::TelemetryProbe,
    kind: &'static str,
    batches: Vec<logit_core::EventBatch>,
    config: logit_pipeline::WriteLoopConfig,
) -> Sums {
    let telemetry = probe.telemetry("out", kind, "sink");
    logit_pipeline::test_util::drive_write_loop(output, batches, config, telemetry).await;
    probe.poll().sums().map(|(name, tags, v)| ((name.to_string(), tags.to_vec()), v)).collect()
}

/// A counter name and the tags a series must carry to match it.
pub(crate) type SumSeries<'a> = (&'a str, &'a [(&'a str, &'a str)]);

/// Whether the series `(name, tags)` matches `series`: the same name, and every tag it names.
fn matches(series: &SumSeries<'_>, name: &str, tags: &[(String, String)]) -> bool {
    series.0 == name && series.1.iter().all(|(k, v)| tags.iter().any(|(tk, tv)| tk == k && tv == v))
}

/// `sums` without the [`PER_ATTEMPT`] counters and without the series `per_attempt` names.
fn once_per_batch(sums: &Sums, per_attempt: &[SumSeries<'_>]) -> Sums {
    sums.iter()
        .filter(|((name, tags), _)| {
            !PER_ATTEMPT.contains(&name.as_str())
                && !per_attempt.iter().any(|series| matches(series, name, tags))
        })
        .map(|(key, v)| (key.clone(), *v))
        .collect()
}

/// The total of every series in `sums` named `name` whose tags include `tags`.
pub(crate) fn sum_of(sums: &Sums, name: &str, tags: &[(&str, &str)]) -> f64 {
    sums.iter()
        .filter(|((n, t), _)| {
            n == name && tags.iter().all(|(k, v)| t.iter().any(|(tk, tv)| tk == k && tv == v))
        })
        .map(|(_, v)| v)
        .sum()
}

/// Asserts a retried run counted every series outside [`PER_ATTEMPT`] and the sink's own
/// `per_attempt` series as a single-attempt run of the same batch did, and that each of
/// `encode_side` was counted at all, so the comparison has something to compare.
///
/// `per_attempt` names the transport and server-verdict series a sink counts per attempt beyond
/// [`PER_ATTEMPT`] (ADR `sink-send-path-and-attempt-accounting`, decision 1). It may not cover a
/// series of `encode_side`, so a sink can't exempt an encode-side counter from the comparison.
pub(crate) fn assert_counted_once_per_batch(
    single: &Sums,
    retried: &Sums,
    encode_side: &[SumSeries<'_>],
    per_attempt: &[SumSeries<'_>],
) {
    for (name, tags) in encode_side {
        assert!(sum_of(single, name, tags) > 0.0, "{name} {tags:?} was never counted: {single:?}");
        let owned: Vec<(String, String)> =
            tags.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        assert!(
            !per_attempt.iter().any(|series| matches(series, name, &owned)),
            "{name} {tags:?} is encode-side and can't be exempted as per attempt"
        );
    }
    let (single, retried) =
        (once_per_batch(single, per_attempt), once_per_batch(retried, per_attempt));
    let keys: std::collections::BTreeSet<_> = single.keys().chain(retried.keys()).collect();
    let differing: Vec<_> = keys
        .into_iter()
        .map(|key| (key, single.get(key).copied(), retried.get(key).copied()))
        .filter(|(_, single, retried)| single.unwrap_or(0.0) != retried.unwrap_or(0.0))
        .collect();
    assert!(differing.is_empty(), "(series, single attempt, retried) that differ: {differing:?}");
}

/// What a sink counts when it sends something: a batch that encoded to nothing counts none of
/// these, which [`assert_direct_sends_count_after_an_empty_batch`] checks before relying on it.
const SENT: [&str; 6] = [
    "logit.output.requests",
    "logit.output.batch.bytes",
    "logit.output.records",
    "logit.output.request.bytes",
    "logit.output.messages",
    "logit.output.datagrams",
];

/// Runs `empty`, a batch the encoder skips whole, through the write loop, then sends `batch`
/// twice directly with no `observe_batch`, and asserts each of `encode_side` counted both direct
/// sends: a batch that encoded to nothing leaves the sink's accounting disarmed. Asserts first
/// that `empty` was delivered and sent nothing ([`SENT`]), so the early `Ok` is what ran.
pub(crate) async fn assert_direct_sends_count_after_an_empty_batch<O>(
    output: &mut O,
    probe: &mut logit_pipeline::test_util::TelemetryProbe,
    kind: &'static str,
    empty: logit_core::EventBatch,
    batch: impl Fn() -> logit_core::EventBatch,
    encode_side: &[SumSeries<'_>],
) where
    O: logit_pipeline::Output + Send,
{
    let before = sums_through_write_loop(output, probe, kind, vec![empty], fast_retry()).await;
    assert_eq!(sum_of(&before, "logit.component.batches.delivered", &[]), 1.0, "{before:?}");
    for name in SENT {
        assert_eq!(sum_of(&before, name, &[]), 0.0, "the empty batch counted {name}: {before:?}");
    }
    let mut after: Vec<Sums> = Vec::new();
    for _ in 0..2 {
        output.send(&batch()).await.expect("a direct send delivers");
        let sums = probe.poll().sums().map(|(n, t, v)| ((n.to_string(), t.to_vec()), v));
        after.push(sums.collect());
    }
    for (name, tags) in encode_side {
        let base = sum_of(&before, name, tags);
        let first = sum_of(&after[0], name, tags) - base;
        let second = sum_of(&after[1], name, tags) - base;
        assert!(first > 0.0, "{name} {tags:?}: the first direct send counted nothing");
        assert_eq!(second, 2.0 * first, "{name} {tags:?}: the second direct send wasn't counted");
    }
}

/// One request an [`http_recorder`] received, its body as it arrived (still compressed).
#[derive(Clone, Debug)]
pub(crate) struct Recorded {
    pub(crate) path: String,
    pub(crate) body: Vec<u8>,
}

/// How an [`http_recorder`] answers one request.
pub(crate) enum Reply {
    /// A status and a body.
    Answer(u16, Vec<u8>),
    /// No answer, ever: the request stays in flight until the client gives up on it.
    Hang,
}

/// The requests an [`http_recorder`] received, in arrival order.
pub(crate) type RecordLog = Arc<Mutex<Vec<Recorded>>>;

/// An HTTP/1.1 server on `127.0.0.1` that records every request, then answers the `n`th (from 0)
/// with `respond(n, path, body)`. A request is recorded before it is answered, so the log counts
/// the attempts a sink made, a hung one included.
pub(crate) async fn http_recorder(
    respond: impl Fn(usize, &str, &[u8]) -> Reply + Send + Sync + 'static,
) -> (SocketAddr, RecordLog) {
    use http_body_util::BodyExt as _;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let log: RecordLog = Arc::default();
    let respond = Arc::new(respond);
    let task_log = Arc::clone(&log);
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let (log, respond) = (Arc::clone(&task_log), Arc::clone(&respond));
            tokio::spawn(async move {
                let svc = hyper::service::service_fn(move |req: http::Request<_>| {
                    let (log, respond) = (Arc::clone(&log), Arc::clone(&respond));
                    async move {
                        let path = req.uri().path().to_string();
                        let incoming: hyper::body::Incoming = req.into_body();
                        let body = incoming.collect().await.unwrap().to_bytes().to_vec();
                        let n = {
                            let mut log = log.lock().unwrap();
                            log.push(Recorded { path: path.clone(), body: body.clone() });
                            log.len() - 1
                        };
                        let (status, body) = match respond(n, &path, &body) {
                            Reply::Answer(status, body) => (status, body),
                            Reply::Hang => std::future::pending().await,
                        };
                        Ok::<_, std::convert::Infallible>(
                            http::Response::builder()
                                .status(status)
                                .body(http_body_util::Full::new(bytes::Bytes::from(body)))
                                .unwrap(),
                        )
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(hyper_util::rt::TokioIo::new(stream), svc)
                    .await;
            });
        }
    });
    (addr, log)
}

/// A server that answers one request with `status` and `body`, then refuses every later
/// connect. It stops listening once it accepted its one connection, before it replies, and the
/// reply carries `Connection: close`, so a pooled client dials again for its next request and is
/// refused. The log holds the one request it answered.
pub(crate) async fn answers_once(status: u16, body: impl Into<Vec<u8>>) -> (SocketAddr, RecordLog) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let log: RecordLog = Arc::default();
    let (task_log, body) = (Arc::clone(&log), body.into());
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        drop(listener);
        serve_one_request(stream, task_log, status, body).await;
    });
    (addr, log)
}

/// [`answers_once`] over a Unix socket bound at `path`: once it accepted its one connection it
/// stops listening and removes the socket file, so a later connect fails.
pub(crate) async fn answers_once_unix(
    path: &std::path::Path,
    status: u16,
    body: impl Into<Vec<u8>>,
) -> RecordLog {
    let listener = tokio::net::UnixListener::bind(path).unwrap();
    let log: RecordLog = Arc::default();
    let (task_log, body, path) = (Arc::clone(&log), body.into(), path.to_path_buf());
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        drop(listener);
        std::fs::remove_file(&path).unwrap();
        serve_one_request(stream, task_log, status, body).await;
    });
    log
}

/// Serves HTTP/1.1 on `io`, recording each request and answering it with `status`, `body`, and
/// `Connection: close`, which ends the connection after the first reply.
async fn serve_one_request<IO>(io: IO, log: RecordLog, status: u16, body: Vec<u8>)
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    use http_body_util::BodyExt as _;

    let svc = hyper::service::service_fn(move |req: http::Request<hyper::body::Incoming>| {
        let (log, body) = (Arc::clone(&log), body.clone());
        async move {
            let path = req.uri().path().to_string();
            let received = req.into_body().collect().await.unwrap().to_bytes().to_vec();
            log.lock().unwrap().push(Recorded { path, body: received });
            Ok::<_, std::convert::Infallible>(
                http::Response::builder()
                    .status(status)
                    .header(http::header::CONNECTION, "close")
                    .body(http_body_util::Full::new(bytes::Bytes::from(body)))
                    .unwrap(),
            )
        }
    });
    let _ = hyper::server::conn::http1::Builder::new()
        .serve_connection(hyper_util::rt::TokioIo::new(io), svc)
        .await;
}

/// A `127.0.0.1` address nothing listens on: a connect to it is refused.
pub(crate) async fn refused_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

/// An [`http_recorder`] that answers the `k`th request (from 0) on each path with
/// `script(path, k)`, for a sink that sends to several routes and fails one of them.
pub(crate) async fn per_path_recorder(
    script: impl Fn(&str, usize) -> Reply + Send + Sync + 'static,
) -> (SocketAddr, RecordLog) {
    let per_path: Mutex<std::collections::HashMap<String, usize>> = Mutex::default();
    http_recorder(move |_, path, _| {
        let k = {
            let mut per_path = per_path.lock().unwrap();
            let seen = per_path.entry(path.to_string()).or_default();
            *seen += 1;
            *seen - 1
        };
        script(path, k)
    })
    .await
}

/// The path of each request in `log`, in arrival order.
pub(crate) fn recorded_paths(log: &[Recorded]) -> Vec<&str> {
    log.iter().map(|r| r.path.as_str()).collect()
}

/// The bodies of the requests in `log` to `path`, in arrival order.
pub(crate) fn bodies(log: &[Recorded], path: &str) -> Vec<Vec<u8>> {
    log.iter().filter(|r| r.path == path).map(|r| r.body.clone()).collect()
}

/// [`fast_retry`] with `delivery: at_most_once` set as an override: an `Ambiguous` failure drops the
/// batch instead of retrying it, so a test can end a batch's life on a `5xx`.
pub(crate) fn at_most_once() -> logit_pipeline::WriteLoopConfig {
    logit_pipeline::WriteLoopConfig {
        delivery_override: Some(logit_pipeline::DeliveryPosture::AtMostOnce),
        ..fast_retry()
    }
}

/// [`fast_retry`] with `delivery: at_least_once` set as an override, so a test that retries an
/// `Ambiguous` failure doesn't depend on the sink's default posture.
pub(crate) fn at_least_once() -> logit_pipeline::WriteLoopConfig {
    logit_pipeline::WriteLoopConfig {
        delivery_override: Some(logit_pipeline::DeliveryPosture::AtLeastOnce),
        ..fast_retry()
    }
}
