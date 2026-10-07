//! The pieces raw-TCP sinks share whether or not TLS is on: [`AsyncStream`], [`host_only`], and
//! [`poll_pending_close`], the one-poll probe a pooled sink runs on a reused connection before
//! writing to it.
//!
//! Every sink's `rustls::ClientConfig` comes from `logit_pipeline::tls::build_client_config`,
//! which registers its files for reload; [`TlsClientSettings`] is re-exported from there.

use std::future::poll_fn;
use std::pin::Pin;
use std::task::Poll;

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
    poll_fn(|cx| {
        let mut read_buf = ReadBuf::new(buf);
        match Pin::new(&mut *stream).poll_read(cx, &mut read_buf) {
            Poll::Pending => Poll::Ready(PendingClose::Open),
            Poll::Ready(Ok(())) if read_buf.filled().is_empty() => Poll::Ready(PendingClose::Eof),
            Poll::Ready(Ok(())) => Poll::Ready(PendingClose::Bytes(read_buf.filled().len())),
            // A reset, a TLS peer closing without `close_notify` (`UnexpectedEof`), or a bad
            // record all leave nothing worth writing to, and a transient error costs one
            // reconnect with nothing written.
            Poll::Ready(Err(_)) => Poll::Ready(PendingClose::Eof),
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::io;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::task::unconstrained;

    use super::*;
    use crate::test_support::{read_once, tapped_duplex, tls_pair, FakeStream, ReadStep};

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
