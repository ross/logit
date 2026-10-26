//! The pieces raw-TCP sinks share whether or not TLS is on: [`AsyncStream`], [`host_only`],
//! [`poll_pending_close`], the one-poll probe a pooled sink runs on a reused connection before
//! writing to it, and [`RecordsCertRequest`], which tells a dial whether the server asked for a
//! client certificate.
//!
//! Every sink's `rustls::ClientConfig` comes from `logit_pipeline::tls::build_client_config`,
//! which registers its files for reload; [`TlsClientSettings`] is re-exported from there.

use std::cell::Cell;
use std::future::{poll_fn, Future};
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use rustls::client::ResolvesClientCert;
use rustls::sign::CertifiedKey;
use rustls::{AlertDescription, SignatureScheme};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub use logit_pipeline::tls::TlsClientSettings;

/// A plain `TcpStream` or a TLS-wrapped one, behind one object-safe trait, so a sink's connection
/// field isn't generic (a generic field would make the sink type generic, which
/// `logit-cli::pipeline::build_spec` would have to know about). Shared by every sink that dials a
/// raw TCP connection that may be TLS-wrapped.
pub(crate) trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncStream for T {}

/// The host part of a bare `host:port` endpoint: the SNI `ServerName` for an endpoint with no
/// scheme to parse (a URL endpoint gets this from `reqwest`/`hyper`). `rsplit_once`, so a
/// bracketed IPv6 literal (`[::1]:1234`) splits on the port's colon. The brackets are the
/// operator's to write; this doesn't validate the address.
pub(crate) fn host_only(endpoint: &str) -> &str {
    endpoint
        .rsplit_once(':')
        .map(|(host, _port)| host)
        .unwrap_or(endpoint)
        .trim_start_matches('[')
        .trim_end_matches(']')
}

/// What one poll of a pooled stream found ([`poll_pending_close`]'s answer).
pub(crate) enum PendingClose {
    /// Nothing readable now. Every protocol these sinks speak has a peer that is silent unless
    /// answering, so this is the healthy case: write.
    Open,
    /// The peer closed its end (an immediate EOF), or the poll failed; either way the connection
    /// is finished.
    Eof,
    /// The peer sent something unprompted: from `logit_in`, a `Reject{GOING_AWAY}` before a
    /// graceful shutdown or idle close; a line-oriented receiver never sends anything. Either
    /// way, don't write a batch into it.
    Bytes(usize),
}

impl std::fmt::Display for PendingClose {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PendingClose::Open => f.write_str("still open"),
            PendingClose::Eof => f.write_str("closed by the peer"),
            PendingClose::Bytes(n) => write!(f, "carrying {n} unsolicited byte(s) from the peer"),
        }
    }
}

/// Polls `stream` for readability **once**, never waiting: the check a pooled sink runs on a
/// *reused* connection before a send attempt's first write, so a batch isn't written into a socket
/// the peer already closed (`docs/adr/idle-connection-timeout.md`'s "The client-side probe"
/// section).
///
/// **One `poll_read`, because the probe must not wait.** The peers these sinks talk to are silent
/// unless answering, so a healthy connection has nothing to read, and any wait, however short,
/// would be added to every send. One poll answers from what has already arrived.
///
/// **What a `Pending` poll can move.** On a TLS stream, tokio-rustls reads whatever the socket
/// holds into the session before it answers: part of a record, or whole records that carry no
/// plaintext (the TLS 1.3 session tickets a server sends after the handshake). `Pending` then
/// means no plaintext is ready, not that nothing was read. Nothing is lost: the session keeps
/// those bytes, and [`PendingClose::Open`] keeps the stream and so the session, so a later read
/// completes the record. A read future dropped mid-record loses nothing either, since the bytes
/// belong to the session and not to the read call (`crate::stream_pins` pins both). The answers
/// that may take plaintext off the stream ([`PendingClose::Eof`], [`PendingClose::Bytes`]) both
/// drop the connection.
///
/// `?Sized` so `&mut *boxed_stream` (a `&mut dyn AsyncStream`) works like a `&mut TcpStream`.
///
/// Point-in-time only: a FIN arriving between this poll and the write is still missed
/// (`Fault::Ambiguous` for `logit_out`, undetected for the line-oriented sinks). It catches the
/// common case, a FIN already in this host's receive queue.
pub(crate) async fn poll_pending_close<S: AsyncRead + Unpin + ?Sized>(
    stream: &mut S,
    buf: &mut [u8],
) -> PendingClose {
    poll_pending_close_or_alert(stream, buf).await.unwrap_or(PendingClose::Eof)
}

/// [`poll_pending_close`], except that a TLS alert from the peer is `Err` with the alert's
/// description instead of [`PendingClose::Eof`]: the stream sinks report it as a refusal rather
/// than redial past it (`crate::stream`'s module doc).
pub(crate) async fn poll_pending_close_or_alert<S: AsyncRead + Unpin + ?Sized>(
    stream: &mut S,
    buf: &mut [u8],
) -> Result<PendingClose, AlertDescription> {
    poll_fn(|cx| {
        let mut read_buf = ReadBuf::new(buf);
        match Pin::new(&mut *stream).poll_read(cx, &mut read_buf) {
            Poll::Pending => Poll::Ready(Ok(PendingClose::Open)),
            Poll::Ready(Ok(())) if read_buf.filled().is_empty() => {
                Poll::Ready(Ok(PendingClose::Eof))
            }
            Poll::Ready(Ok(())) => Poll::Ready(Ok(PendingClose::Bytes(read_buf.filled().len()))),
            Poll::Ready(Err(err)) => match alert_in(&err) {
                Some(alert) => Poll::Ready(Err(alert)),
                // A reset, a TLS peer closing without `close_notify` (`UnexpectedEof`), or a bad
                // record all leave nothing worth writing to, and a transient error costs one
                // reconnect with nothing written.
                None => Poll::Ready(Ok(PendingClose::Eof)),
            },
        }
    })
    .await
}

/// The alert a TLS peer sent, when `err` is tokio-rustls's report of one: an `InvalidData` error
/// wrapping `rustls::Error::AlertReceived`.
pub(crate) fn alert_in(err: &io::Error) -> Option<AlertDescription> {
    match err.get_ref()?.downcast_ref::<rustls::Error>()? {
        rustls::Error::AlertReceived(alert) => Some(*alert),
        _ => None,
    }
}

/// An alert's RFC 8446 name, such as `certificate_required`, for an operator-facing message.
/// rustls names its variants in camel case (`CertificateRequired`, `UnknownCA`). An alert rustls
/// has no name for is its number, since callers write "alert" before it.
pub(crate) fn alert_name(alert: AlertDescription) -> String {
    let Some(camel) = alert.as_str() else {
        return u8::from(alert).to_string();
    };
    let chars: Vec<char> = camel.chars().collect();
    let mut name = String::with_capacity(camel.len() + 4);
    for (i, &c) in chars.iter().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            let after_lower = chars[i - 1].is_ascii_lowercase();
            let ends_acronym = chars[i - 1].is_ascii_uppercase()
                && chars.get(i + 1).is_some_and(char::is_ascii_lowercase);
            if after_lower || ends_acronym {
                name.push('_');
            }
        }
        name.push(c.to_ascii_lowercase());
    }
    name
}

tokio::task_local! {
    /// Set by [`RecordsCertRequest::resolve`] while [`record_cert_request`] polls one handshake.
    static CERT_REQUESTED: Cell<bool>;
}

/// Runs `handshake` and reports whether the server sent a CertificateRequest during it, read
/// from the [`RecordsCertRequest`] resolver of the config the handshake uses. rustls calls the
/// resolver synchronously inside a poll of the handshake, so a task-local scoped to this one
/// future is a per-connection record.
pub(crate) async fn record_cert_request<F: Future>(handshake: F) -> (F::Output, bool) {
    CERT_REQUESTED
        .scope(Cell::new(false), async move {
            let output = handshake.await;
            (output, CERT_REQUESTED.with(Cell::get))
        })
        .await
}

/// A client-certificate resolver that records, for the handshake [`record_cert_request`] is
/// running, that the server asked for a certificate, then answers as `inner` does.
///
/// One instance per `ClientConfig`, never one per dial: rustls offers a stored session ticket
/// only to a config whose resolver is the same `Arc` the ticket was issued under, so a fresh
/// wrapper per connection would turn every handshake into a full one. `inner` is the config's own
/// resolver (`logit_pipeline::tls`'s `ReloadingCert` under a client certificate), so a reloaded
/// certificate still reaches the next handshake.
#[derive(Debug)]
pub(crate) struct RecordsCertRequest {
    inner: Arc<dyn ResolvesClientCert>,
}

impl RecordsCertRequest {
    /// `config` with its client-certificate resolver wrapped.
    pub(crate) fn install(mut config: rustls::ClientConfig) -> rustls::ClientConfig {
        let inner = Arc::clone(&config.client_auth_cert_resolver);
        config.client_auth_cert_resolver = Arc::new(Self { inner });
        config
    }
}

impl ResolvesClientCert for RecordsCertRequest {
    fn resolve(
        &self,
        root_hint_subjects: &[&[u8]],
        sigschemes: &[SignatureScheme],
    ) -> Option<Arc<CertifiedKey>> {
        // Outside `record_cert_request` there's nothing to record.
        let _ = CERT_REQUESTED.try_with(|requested| requested.set(true));
        self.inner.resolve(root_hint_subjects, sigschemes)
    }

    fn only_raw_public_keys(&self) -> bool {
        self.inner.only_raw_public_keys()
    }

    fn has_certs(&self) -> bool {
        self.inner.has_certs()
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::task::unconstrained;

    use super::*;
    use crate::test_support::{
        read_once, rejected_tls_client, tapped_duplex, tls_pair, FakeStream, ReadStep,
    };

    async fn probe(stream: &mut (dyn AsyncStream + '_)) -> PendingClose {
        let mut buf = [0u8; 1];
        poll_pending_close(stream, &mut buf).await
    }

    #[tokio::test]
    async fn a_pending_poll_is_open() {
        let fake = FakeStream::new();
        let mut stream: Box<dyn AsyncStream> = Box::new(fake.clone());
        assert!(matches!(probe(&mut *stream).await, PendingClose::Open));
        assert_eq!(fake.state().reads, 1, "the probe polls once");
    }

    #[tokio::test]
    async fn an_empty_ready_is_eof() {
        let mut stream: Box<dyn AsyncStream> = Box::new(FakeStream::new().reading(ReadStep::Eof));
        assert!(matches!(probe(&mut *stream).await, PendingClose::Eof));
    }

    #[tokio::test]
    async fn a_read_error_is_eof() {
        let fake = FakeStream::new().reading(ReadStep::Fail(io::ErrorKind::ConnectionReset));
        let mut stream: Box<dyn AsyncStream> = Box::new(fake);
        assert!(matches!(probe(&mut *stream).await, PendingClose::Eof));
    }

    #[tokio::test]
    async fn unsolicited_bytes_are_counted_up_to_the_probe_buffer() {
        let fake = FakeStream::new().reading(ReadStep::Bytes(b"xyz".to_vec()));
        let mut stream: Box<dyn AsyncStream> = Box::new(fake);
        assert!(matches!(probe(&mut *stream).await, PendingClose::Bytes(1)));

        let fake = FakeStream::new().reading(ReadStep::Bytes(b"xyz".to_vec()));
        let mut stream: Box<dyn AsyncStream> = Box::new(fake);
        let mut buf = [0u8; 8];
        let pending = poll_pending_close(&mut *stream, &mut buf).await;
        assert!(matches!(pending, PendingClose::Bytes(3)));
    }

    /// Part of a server record is on the pipe when the probe runs. The probe moves it into the
    /// session and answers `Open`, and the record still completes and decrypts afterwards.
    #[tokio::test]
    async fn a_partial_tls_record_probes_open_and_is_still_readable() {
        unconstrained(async {
            // Less than one record of the message below, so the pipe holds part of it.
            const CAPACITY: usize = 4096;
            let ((client_io, client_tap), (server_io, _)) = tapped_duplex(CAPACITY);
            let (client, mut server) = tls_pair(client_io, server_io).await;
            let mut client: Box<dyn AsyncStream> = Box::new(client);
            // The session tickets first, so the probe below reads record bytes only.
            assert!(read_once(&mut *client, &mut [0u8; 64]).await.is_pending());

            let message: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();
            assert_eq!(server.write(&message).await.unwrap(), message.len());
            let read_before = client_tap.read();

            assert!(matches!(probe(&mut *client).await, PendingClose::Open));
            assert_eq!(client_tap.read() - read_before, CAPACITY, "the probe read record bytes");

            let mut got = vec![0u8; message.len()];
            let (flushed, read) = tokio::join!(server.flush(), client.read_exact(&mut got));
            flushed.unwrap();
            read.expect("the record completes");
            assert_eq!(got, message);
        })
        .await;
    }

    #[tokio::test]
    async fn a_tls_close_notify_probes_eof() {
        unconstrained(async {
            let ((client_io, _), (server_io, _)) = tapped_duplex(4096);
            let (client, mut server) = tls_pair(client_io, server_io).await;
            let mut client: Box<dyn AsyncStream> = Box::new(client);
            server.shutdown().await.unwrap();
            assert!(matches!(probe(&mut *client).await, PendingClose::Eof));
        })
        .await;
    }

    /// A TLS peer gone without `close_notify` reads as `UnexpectedEof`, an error, and probes
    /// `Eof` through the error arm.
    #[tokio::test]
    async fn a_tls_transport_close_without_close_notify_probes_eof() {
        unconstrained(async {
            let ((client_io, _), (server_io, _)) = tapped_duplex(4096);
            let (client, server) = tls_pair(client_io, server_io).await;
            let mut client: Box<dyn AsyncStream> = Box::new(client);
            drop(server);
            assert!(matches!(probe(&mut *client).await, PendingClose::Eof));
        })
        .await;
    }

    /// A server's rejection alert is the alert under the alert-aware probe, and `Eof` under
    /// [`poll_pending_close`], which `logit_out` and `kafka_out` keep.
    #[tokio::test]
    async fn a_tls_alert_probes_as_the_alert_or_as_eof() {
        unconstrained(async {
            let ((client_io, _), (server_io, _)) = tapped_duplex(4096);
            let mut client = rejected_tls_client(client_io, server_io).await;
            let probed = poll_pending_close_or_alert(&mut client, &mut [0u8; 1]).await;
            assert!(matches!(probed, Err(AlertDescription::CertificateRequired)));

            let ((client_io, _), (server_io, _)) = tapped_duplex(4096);
            let mut client: Box<dyn AsyncStream> =
                Box::new(rejected_tls_client(client_io, server_io).await);
            assert!(matches!(probe(&mut *client).await, PendingClose::Eof));
        })
        .await;
    }

    #[test]
    fn alert_name_is_the_rfc_name() {
        let cases = [
            (AlertDescription::CertificateRequired, "certificate_required"),
            (AlertDescription::BadCertificate, "bad_certificate"),
            (AlertDescription::UnknownCA, "unknown_ca"),
            (AlertDescription::UnknownPSKIdentity, "unknown_psk_identity"),
            (AlertDescription::HandshakeFailure, "handshake_failure"),
            (AlertDescription::Unknown(200), "200"),
        ];
        for (alert, want) in cases {
            assert_eq!(alert_name(alert), want, "{alert:?}");
        }
    }

    /// An unbracketed IPv6 literal has no unambiguous port colon; the doc leaves the brackets
    /// to the operator, and the last rows pin what an unbracketed one yields.
    #[test]
    fn host_only_takes_the_host_of_a_bare_endpoint() {
        let cases = [
            ("statsd.example.com:8125", "statsd.example.com"),
            ("127.0.0.1:514", "127.0.0.1"),
            ("[::1]:6514", "::1"),
            ("[2001:db8::1]:6514", "2001:db8::1"),
            ("localhost", "localhost"),
            ("", ""),
            ("::1", ":"),
            ("2001:db8::1", "2001:db8:"),
        ];
        for (endpoint, want) in cases {
            assert_eq!(host_only(endpoint), want, "host_only({endpoint:?})");
        }
    }
}
