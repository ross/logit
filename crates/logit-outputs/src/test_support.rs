//! Receivers that sink tests send to: a TCP, TLS, or UDP collector that reports each message on
//! a channel, and the `testdata/tls` fixtures a TLS collector and client are built from.
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

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use logit_pipeline::test_util::RECV_TIMEOUT;
use tokio::io::{AsyncRead, AsyncReadExt};
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
