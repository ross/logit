//! Characterization tests for the third-party I/O behavior the sinks' send path rests on
//! (`docs/adr/sink-send-path-and-attempt-accounting.md`, decision 8), pinned against
//! tokio-rustls 0.26.5, rustls 0.23.45, and tokio 1.53.1. Each test names the source it pins; a
//! bump of any of the three crates re-runs these and re-reads that source.
//!
//! The TLS tests run a real client and server over `tokio::io::duplex` with a small capacity
//! ([`tls_pair`], [`tapped_duplex`]), so a socket write stops mid-record at a known byte count,
//! and a [`crate::test_support::Tap`] counts what crossed the pipe. No test sleeps or reads a
//! clock. "Nothing more is available" is a one-poll read answering `Pending` on an in-memory
//! pipe, and every test body runs under `tokio::task::unconstrained` so the cooperative budget
//! can't produce that `Pending` on its own.

use std::future::Future;
use std::io;
use std::pin::{pin, Pin};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::task::unconstrained;

use crate::test_support::{
    drain_available, read_once, tapped_duplex, tls_pair, FakeStream, WriteStep,
};

/// Less than one full TLS record (16384 bytes of plaintext plus framing), so a TLS write stops
/// mid-record.
const CAPACITY: usize = 4096;

/// rustls's cap on queued ciphertext, applied to plaintext as it is accepted
/// (`DEFAULT_BUFFER_LIMIT` in `common_state.rs`, `CommonState::send_appdata_encrypt`).
const RUSTLS_BUFFER_LIMIT: usize = 64 * 1024;

/// rustls's plaintext fragment size (`MAX_FRAGMENT_LEN` in `msgs/fragmenter.rs`).
const MAX_FRAGMENT_LEN: usize = 16384;

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// tokio-rustls `common/mod.rs`, `Stream::poll_write`: the plaintext goes into the session,
/// ciphertext goes to the IO until a write is `Pending`, and the call returns `Ok(n)` for all the
/// session took. `client::TlsStream::poll_write`'s own doc says it doesn't guarantee the data is
/// sent. So `Ok(n)` means the session accepted `n` bytes, not that they left.
#[tokio::test]
async fn a_tls_write_returns_ok_with_ciphertext_still_queued_in_the_session() {
    unconstrained(async {
        let ((client_io, client_tap), (server_io, _)) = tapped_duplex(CAPACITY);
        let (mut client, _server) = tls_pair(client_io, server_io).await;
        let written_before = client_tap.written();

        let payload = payload(100_000);
        let n = client.write(&payload).await.expect("the session accepts plaintext");

        // An empty send queue after the handshake flush, so rustls accepts its whole limit.
        assert_eq!(n, RUSTLS_BUFFER_LIMIT);
        // The duplex capacity is what reached the IO; the rest of `n` is queued ciphertext.
        assert_eq!(client_tap.written() - written_before, CAPACITY);
        assert!(client.get_ref().1.wants_write(), "the session still holds unsent ciphertext");
    })
    .await;
}

/// tokio-rustls `common/mod.rs`, `Stream::poll_fill_buf` and `Stream::read_io`: a read that
/// processes its records cleanly calls only `read_io`, so a reader waiting for a reply doesn't
/// push out an unflushed request, even with room in the socket for it. The exception, a read whose
/// record processing fails, is pinned by the next test.
#[tokio::test]
async fn a_tls_read_that_succeeds_leaves_queued_ciphertext_queued() {
    unconstrained(async {
        let ((client_io, client_tap), (server_io, server_tap)) = tapped_duplex(CAPACITY);
        let (mut client, mut server) = tls_pair(client_io, server_io).await;
        let payload = payload(100_000);
        let n = client.write(&payload).await.expect("the session accepts plaintext");
        let written = client_tap.written();

        // The server takes everything on the pipe, so the client's direction has room again.
        let server_read_before = server_tap.read();
        let plaintext = drain_available(&mut server).await;
        assert_eq!(server_tap.read() - server_read_before, CAPACITY);
        assert!(plaintext.len() < n, "the server received {} of {n} bytes", plaintext.len());

        // The client reads (the session tickets, then nothing), and writes nothing.
        let mut buf = [0u8; 64];
        assert!(read_once(&mut client, &mut buf).await.is_pending());
        assert_eq!(client_tap.written(), written, "a read moved queued ciphertext");
        assert!(drain_available(&mut server).await.is_empty());
        assert!(client.get_ref().1.wants_write(), "the queued ciphertext is still queued");
    })
    .await;
}

/// tokio-rustls `common/mod.rs`, `Stream::read_io`: when `process_new_packets` fails, the
/// `map_err` closure makes one last-gasp `write_io` for the alert before returning
/// `InvalidData`. rustls's `write_tls` drains the send queue from the front, so queued
/// application ciphertext reaches the socket ahead of the alert.
#[tokio::test]
async fn a_tls_read_failing_on_a_bad_record_sends_queued_ciphertext() {
    unconstrained(async {
        let ((client_io, client_tap), (server_io, _)) = tapped_duplex(CAPACITY);
        let (mut client, mut server) = tls_pair(client_io, server_io).await;
        let payload = payload(100_000);
        let n = client.write(&payload).await.expect("the session accepts plaintext");
        assert_eq!(n, RUSTLS_BUFFER_LIMIT, "ciphertext is queued in the session");
        let written = client_tap.written();
        // The server takes everything on the pipe, so the client's direction has room again.
        drain_available(&mut server).await;

        // An application-data record header and a body no key decrypts, written raw toward the
        // client, around the server's session.
        let mut bad_record = vec![0x17, 0x03, 0x03, 0x00, 0x20];
        bad_record.extend_from_slice(&[0xAB; 32]);
        server.get_mut().0.write_all(&bad_record).await.unwrap();

        let mut buf = [0u8; 64];
        let read = read_once(&mut client, &mut buf).await;
        let Poll::Ready(Err(err)) = read else { panic!("expected a failed read, got {read:?}") };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let after = client_tap.written();
        assert!(
            after > written,
            "the failed read wrote nothing ({written} bytes before and after)"
        );
    })
    .await;
}

/// tokio-rustls `common/mod.rs`, `Stream::poll_flush`: writes to the IO while the session wants
/// to write, then flushes the IO. A flush before the read delivers every accepted byte.
#[tokio::test]
async fn a_tls_flush_drives_every_queued_byte_to_the_peer() {
    unconstrained(async {
        let ((client_io, _), (server_io, _)) = tapped_duplex(CAPACITY);
        let (mut client, mut server) = tls_pair(client_io, server_io).await;
        let payload = payload(100_000);
        let n = client.write(&payload).await.expect("the session accepts plaintext");

        let mut got = vec![0u8; n];
        let (flushed, read) = tokio::join!(client.flush(), server.read_exact(&mut got));
        flushed.expect("the flush completes while the server reads");
        read.expect("the server receives every accepted byte");

        assert_eq!(got, payload[..n]);
        assert!(!client.get_ref().1.wants_write(), "nothing is left queued after a flush");
    })
    .await;
}

/// rustls `conn.rs`, `Reader::check_no_bytes_state`: with no plaintext left, a received
/// `close_notify` reads as `Ok(0)`.
#[tokio::test]
async fn a_peer_close_notify_reads_as_an_empty_ready() {
    unconstrained(async {
        let ((client_io, _), (server_io, _)) = tapped_duplex(CAPACITY);
        let (mut client, mut server) = tls_pair(client_io, server_io).await;
        server.shutdown().await.expect("the server sends close_notify");

        let mut buf = [0u8; 64];
        let n = client.read(&mut buf).await.expect("a clean close is not an error");
        assert_eq!(n, 0);
    })
    .await;
}

/// rustls `conn.rs`, `Reader::check_no_bytes_state`: a transport EOF with no `close_notify`
/// reads as `ErrorKind::UnexpectedEof`, not as an empty read.
#[tokio::test]
async fn a_peer_transport_close_without_close_notify_reads_as_unexpected_eof() {
    unconstrained(async {
        let ((client_io, _), (server_io, _)) = tapped_duplex(CAPACITY);
        let (mut client, server) = tls_pair(client_io, server_io).await;
        // Dropping a tokio-rustls stream drops its IO without sending `close_notify`.
        drop(server);

        let mut buf = [0u8; 64];
        let err = client.read(&mut buf).await.expect_err("an unclean close is an error");
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    })
    .await;
}

/// tokio-rustls `common/mod.rs`, `Stream::poll_fill_buf`: records with no plaintext (the TLS 1.3
/// session tickets a server sends after the handshake) are read into the session, and the poll
/// then answers `Pending`, not an empty `Ready(Ok)` that a caller would read as EOF.
#[tokio::test]
async fn one_poll_of_an_idle_tls_13_connection_after_the_handshake_is_pending() {
    unconstrained(async {
        let ((client_io, client_tap), (server_io, _)) = tapped_duplex(CAPACITY);
        let (mut client, _server) = tls_pair(client_io, server_io).await;
        assert_eq!(client.get_ref().1.protocol_version(), Some(rustls::ProtocolVersion::TLSv1_3));
        let read_before = client_tap.read();

        let mut buf = [0u8; 64];
        assert!(read_once(&mut client, &mut buf).await.is_pending());
        assert!(
            client_tap.read() > read_before,
            "the post-handshake messages moved from the pipe into the session"
        );
    })
    .await;
}

/// rustls keeps a partial record in its deframer buffer, owned by the session, not by the read
/// call. A read that is polled once (`Pending`) and then dropped loses nothing: the rest of the
/// record completes it.
#[tokio::test]
async fn a_partial_tls_record_survives_a_dropped_read() {
    unconstrained(async {
        let ((client_io, client_tap), (server_io, _)) = tapped_duplex(CAPACITY);
        let (mut client, mut server) = tls_pair(client_io, server_io).await;
        let mut buf = [0u8; 64];
        // Take the session tickets first, so the next read moves record bytes only.
        assert!(read_once(&mut client, &mut buf).await.is_pending());

        let message = payload(10_000);
        let n = server.write(&message).await.expect("the session accepts plaintext");
        assert_eq!(n, message.len());

        let read_before = client_tap.read();
        {
            let mut buf = vec![0u8; message.len()];
            let mut read = pin!(client.read(&mut buf));
            let polled = std::future::poll_fn(|cx| Poll::Ready(read.as_mut().poll(cx))).await;
            assert!(polled.is_pending(), "a partial record yields no plaintext");
        }
        // The duplex capacity bounds what the server's write put on the pipe.
        assert_eq!(client_tap.read() - read_before, CAPACITY, "part of the record was read");

        let mut got = vec![0u8; message.len()];
        let (flushed, read) = tokio::join!(server.flush(), client.read_exact(&mut got));
        flushed.expect("the server flushes the rest of the record");
        read.expect("the completed record decrypts");
        assert_eq!(got, message);
    })
    .await;
}

/// tokio-rustls `common/mod.rs`, `Stream::poll_write`: an IO error inside the write loop returns
/// `Err` for the whole call, after earlier IO writes of the same call succeeded. A full record
/// before the error reaches the peer and decrypts, so a TLS write `Err` never proves that nothing
/// of the call arrived.
#[tokio::test]
async fn a_tls_write_error_can_follow_a_whole_record_reaching_the_peer() {
    unconstrained(async {
        let ((client_io, client_tap), (server_io, _)) = tapped_duplex(RUSTLS_BUFFER_LIMIT);
        let (mut client, mut server) = tls_pair(client_io, server_io).await;
        // One whole record (16384 bytes of plaintext plus 22 of framing) and part of the next.
        const PASSED: usize = 20_000;
        client_tap.fail_writes_after(PASSED);
        let written_before = client_tap.written();

        let payload = payload(40_000);
        let err = client.write(&payload).await.expect_err("the armed IO fails the write");
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(client_tap.written() - written_before, PASSED);
        drop(client);

        let mut got = Vec::new();
        let mut buf = vec![0u8; RUSTLS_BUFFER_LIMIT];
        let end = loop {
            match server.read(&mut buf).await {
                Ok(0) => break Ok(()),
                Ok(n) => got.extend_from_slice(&buf[..n]),
                Err(err) => break Err(err),
            }
        };
        assert_eq!(got, payload[..MAX_FRAGMENT_LEN], "the first record reached the peer whole");
        assert_eq!(end.expect_err("the second record is cut").kind(), io::ErrorKind::UnexpectedEof);
    })
    .await;
}

/// Counts wakes, so a test sees whether anything kept its waker.
#[derive(Default)]
struct CountingWaker(AtomicUsize);

impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// tokio-rustls `common/mod.rs`, `Stream::poll_write` and `Stream::poll_flush`: an IO `Ok(0)`
/// counts as would-block in a write. Once the session is full, a write accepts nothing and
/// answers `Pending` with the waker held by no one, so it parks forever. A flush turns the same
/// `Ok(0)` into `ErrorKind::WriteZero`. This is why `FakeStream` never goes under tokio-rustls.
#[tokio::test]
async fn an_io_ok_zero_under_tokio_rustls_parks_a_full_session_write_with_no_waker() {
    unconstrained(async {
        let ((client_io, client_tap), (server_io, _)) = tapped_duplex(CAPACITY);
        let (mut client, _server) = tls_pair(client_io, server_io).await;
        client_tap.zero_writes();
        let payload = payload(100_000);

        // The session takes its limit, and the IO's `Ok(0)` ends the call as a would-block.
        let n = client.write(&payload).await.expect("the session accepts plaintext");
        assert_eq!(n, RUSTLS_BUFFER_LIMIT);

        let wakes = Arc::new(CountingWaker::default());
        let waker = Waker::from(Arc::clone(&wakes));
        let polled = Pin::new(&mut client).poll_write(&mut Context::from_waker(&waker), &payload);
        assert!(polled.is_pending(), "a full session with an Ok(0) IO answers Pending");
        drop(waker);
        assert_eq!(Arc::strong_count(&wakes), 1, "nothing kept the waker to wake the write");
        assert_eq!(wakes.0.load(Ordering::SeqCst), 0);

        let err = client.flush().await.expect_err("a flush over an Ok(0) IO fails");
        assert_eq!(err.kind(), io::ErrorKind::WriteZero);
    })
    .await;
}

/// tokio `io/util/write_all.rs`, `WriteAll::poll`: an `Ok(0)` from the writer is
/// `ErrorKind::WriteZero`, not a retry.
#[tokio::test]
async fn write_all_maps_an_ok_zero_write_to_write_zero() {
    let fake = FakeStream::new().on_write(1, WriteStep::Zero);
    let mut stream = fake.clone();

    let err = stream.write_all(b"hits:1|c\n").await.expect_err("Ok(0) fails write_all");

    assert_eq!(err.kind(), io::ErrorKind::WriteZero);
    let state = fake.state();
    assert_eq!(state.writes, 1, "write_all gives up on the first Ok(0)");
    assert!(state.unflushed.is_empty());
}
