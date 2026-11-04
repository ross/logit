//! The pooled-stream send driver `statsd_out`, `syslog_out`, and `graphite_out` share: one
//! connection kept between batches, dialed lazily over TCP, TCP with TLS, or a Unix stream, and
//! one frame written per `send` (ADR `sink-send-path-and-attempt-accounting`, decisions 4 to 6,
//! and ADR `stream-sink-stalled-connection`).
//!
//! A caller builds the frame with its own framing and counts its own encode-side and per-message
//! results; [`PooledStream::send`] sees only the bytes. A stream has no application response, so
//! the I/O outcome decides the class, for `syslog_out`, `statsd_out`, and `graphite_out` alike. An
//! encode-side refusal (a message over the sink's cap) is the caller's: dropped and counted per
//! message, never a `Fault`. The Evidence column names the test that pins each row.
//!
//! | Response | Class | Why | Evidence |
//! |---|---|---|---|
//! | dial failure: TCP connect refused, DNS failure, TLS handshake failure or stall, Unix connect, each under `connect_timeout` | `Clean`, not retried inside `send` | no byte of the frame left | `a_refused_dial_is_clean_leaves_the_pool_empty_and_counts_no_reconnect`, `a_stalled_tls_handshake_times_out_clean_within_twice_the_connect_timeout`, `unix_stream_dial_failures_are_clean` |
//! | a new TLS 1.3 connection, full or resumed, to a destination whose server asked for a client certificate: nothing but session tickets for `accept_timeout` | no fault: the frame is written | TLS sends no acceptance, so silence for the bound is taken as one; a ticket proves nothing, since a server may send it before its own code rejects the client | `syslog::tests::tls_tcp_with_a_client_certificate_satisfies_a_client_ca_requiring_collector`, `a_resumed_dial_to_a_server_that_asked_for_a_certificate_waits_and_reads_a_close` |
//! | that connection: an alert before the first write (`certificate_required`, `bad_certificate`, any other) | `Refused`, not retried inside `send` | the server rejected the client certificate or the handshake, so a redial would be rejected the same way | `syslog::tests::tls_tcp_without_a_client_certificate_delivers_nothing_to_a_client_ca_requiring_collector`, `statsd::tests::tls_tcp_without_a_client_certificate_is_refused_by_a_client_ca_requiring_collector` |
//! | that connection: a close before the first write (EOF, `close_notify`, a reset, another read error), tickets or none before it | `Refused`, not retried inside `send` | a server that checks the certificate in its own code after the handshake turns the client away by closing, often with no alert | `a_close_after_session_tickets_is_refused_and_data_is_clean`, `a_resumed_dial_to_a_server_that_asked_for_a_certificate_waits_and_reads_a_close` |
//! | that connection: data from the server before the first write | `Clean`, not retried inside `send` | no byte of the frame left, the bytes are already off the stream, and a line receiver never sends any | `a_close_after_session_tickets_is_refused_and_data_is_clean` |
//! | a reused connection's probe answers anything but open (EOF, unsolicited bytes, a reset) | no fault: the driver counts what the connection lost (below) and redials, and the redial doesn't consume the retry | the old connection was dead before this frame | `a_reused_connection_that_probes_eof_is_redialed_and_the_retry_survives`, `a_reused_connection_that_probes_unsolicited_bytes_is_redialed_and_the_retry_survives`, `unix_stream_redials_when_the_probe_finds_the_pooled_connection_closed` |
//! | a reused TLS connection's probe reads an alert | `Refused`, nothing written, no redial; what the connection lost is counted | the server ended the session, usually by rejecting the client certificate after the batch before this one was written; a silent redial would hide that | `a_reused_connection_that_probes_a_tls_alert_is_refused_and_never_redialed` |
//! | the plaintext first `write` fails (`Err`, or `Ok(0)` read as `WriteZero`) | retried once on a fresh connection; a second failure is `Clean` | a failed plaintext first write accepted nothing | `a_plaintext_first_write_error_is_retried_once_then_clean`, `a_plaintext_first_write_of_zero_bytes_is_write_zero_retried_once_then_clean`, `unix_stream_retries_a_first_write_the_peer_refused` |
//! | the first `write`, plaintext or TLS, accepts nothing for `connect_timeout` | `Clean`, the connection kept | it accepted nothing: tokio-rustls answers `Pending` only when its session took nothing (`crate::stream_pins`) | `a_first_write_that_accepts_nothing_is_clean_and_keeps_the_connection`, `a_stalled_connection_is_kept_and_a_reset_counts_what_the_receiver_never_acknowledged` |
//! | the TLS first `write` fails | `Ambiguous`, never retried | rustls may have put whole records on the wire first (below) | `a_tls_write_error_is_ambiguous_and_never_retried` |
//! | the remainder's `write_all`, or the `flush`, fails, a reset mid-frame included | `Ambiguous`, never retried | part of the frame left, and a line receiver keeps every complete line it got | `a_remainder_failure_after_a_short_first_write_is_ambiguous_and_never_resent`, `a_flush_failure_after_a_complete_write_is_ambiguous_and_drops_the_connection`, `a_real_reset_mid_frame_is_ambiguous` |
//! | any other write, or the flush, makes no progress for `connect_timeout` | `Ambiguous`, the connection kept with the frame's unwritten tail | part of the frame left; the tail goes before the next frame, so the receiver never sees a frame cut and another begun | `a_write_that_stalls_mid_frame_is_ambiguous_and_its_tail_goes_first_on_the_kept_connection`, `a_stalled_tls_connection_is_kept_and_a_reset_counts_a_loss` |
//! | a kept connection's unwritten tail makes no progress for `connect_timeout` | `Clean`, the connection kept with what is left of the tail | nothing of this frame was written | `a_write_that_stalls_mid_frame_is_ambiguous_and_its_tail_goes_first_on_the_kept_connection` |
//! | a kept connection's unwritten tail fails to write | no fault: the driver counts what the connection lost, redials, and writes the frame there; the redial doesn't consume the retry | nothing of this frame was written | `a_pending_tail_that_fails_is_dropped_and_the_frame_goes_on_a_fresh_connection` |
//! | any stall above, on a TCP connection whose peer isn't answering ([`sockstat::tcp_peer_answering`]) | the stall's class; the connection dropped, nothing counted | a vanished or partitioned host never answers a window probe, and the kernel goes on sending what the socket holds after the close | `sockstat`'s `a_receiver_that_stopped_reading_still_answers_window_probes` and `the_peer_answers_while_nothing_is_retransmitted_and_one_probe_at_most_is_out` |
//!
//! **The acceptance wait.** Under TLS 1.3 a server checks the client certificate after the client's
//! last handshake message, and may check it again in its own code after its TLS library has
//! accepted it, so a dial completes before a rejection arrives. TLS sends nothing that means
//! accepted, and a sink that only writes never reads the alert or close that means rejected: the
//! frame it wrote would count delivered. So before the first write on a new TLS 1.3 connection to
//! a destination whose server has asked for a client certificate, the driver reads for the whole
//! `accept_timeout` ([`await_acceptance`]). Session tickets don't end the wait: a server may send
//! them before its own check. A resumed handshake carries no CertificateRequest, so the target
//! remembers that the server asked on an earlier full handshake ([`RecordsCertRequest`] is how a
//! dial knows, `a_full_handshake_records_the_certificate_request_and_a_resumed_one_does_not`),
//! and a resumed connection waits too; without that, a server that rejects in its own code would
//! turn away every resumed redial after the first batch, unseen. A destination whose server never asked
//! doesn't wait (`a_destination_that_never_asked_for_a_certificate_never_waits`), and neither does
//! TLS 1.2, which checks inside the handshake, so a rejection is a failed dial
//! (`a_tls12_client_certificate_rejection_fails_the_handshake_clean`), nor any dial under an
//! `accept_timeout` of zero (`syslog::tests::tls_tcp_with_a_zero_accept_timeout_never_waits`). A
//! rejection that arrives after `accept_timeout` still loses that connection's first frame: the
//! next send's probe reports an alert, or redials past a close, and the redial's own wait reads
//! the rejection.
//!
//! **A receiver that stops reading.** `connect_timeout` bounds each write's progress as well as
//! each dial phase ([`write_with_progress`]), so a peer that accepts the connection and stops
//! reading fails the attempt instead of parking it until shutdown; a large frame on a slow link
//! keeps making progress and never trips it (`a_slow_write_that_keeps_making_progress_is_never_cut`).
//! The stalled connection is kept, with any unwritten tail of its frame, while its peer still
//! answers. A redial would land in the frozen listener's accept backlog and fill a second buffer
//! pair there, and a third on the next stall, all lost if the receiver dies; a kept connection
//! puts at most one pair at risk (ADR `stream-sink-stalled-connection`). Under
//! `at_least_once` the whole stalled batch is delivered twice once the receiver reads again: the
//! kept connection finishes its frame, and the retry sends the frame whole.
//!
//! **Counting what a dropped connection lost.** When a connection ends with its data destroyed
//! (a probe's EOF, reset, unsolicited bytes, or alert, or a failed write), the driver reads the
//! TCP socket's unacknowledged send queue ([`sockstat::send_queue`]) before closing it and counts
//! what earlier batches left there as `logit.output.bytes.dropped{reason="unacknowledged"}`, with
//! a throttled `warn` keyed `unacknowledged`. Bytes of every frame an attempt returned
//! `Ambiguous` for are left out ([`Sent::unaccounted`]), however many attempts ago: that batch
//! is resent, or counted dropped under `at_most_once`. The count is on the sink's ungated handles, since it is the destination's
//! verdict on earlier batches, not an encode outcome. Under TLS it is wire bytes, record framing
//! included, and leaves out up to 64 KiB of ciphertext still queued in the session. Nothing is
//! counted on a Unix stream, whose queue the receiver's close frees, on a stall, whose closed
//! connection the kernel keeps sending, or on shutdown. A receiver that half-closes and keeps
//! reading is overcounted: its FIN reads as a close.
//!
//! A TLS write `Err` is `Ambiguous` because rustls may have put whole records on the wire first.
//! rustls splits what each session write accepted into records of at most 16384 bytes of
//! plaintext, with no regard for line or message boundaries, so a record can end mid-line, but a
//! line or message stream's receiver keeps every complete line or message in what arrived
//! (`crate::stream_pins` pins the tokio-rustls side). `flush` runs before `Ok`, because a TLS
//! write's `Ok` can leave ciphertext queued in the session; a connection kept after its flush
//! stalled owes that flush, as an empty tail, before its next frame. A write the stall bound cut
//! leaves any ciphertext it queued in the session, and the next write or flush sends it first.
//!
//! The connection is moved out of the pool for the whole attempt and put back only after the
//! flush or a stall, so a `send` future dropped at any await leaves the pool empty and the next
//! `send` dials fresh rather than write into a stream holding part of a frame it no longer knows.
//! The shutdown flush finishes a kept tail under the same bound and drops the connection on a
//! stall, so a frozen receiver can't hold shutdown.
//!
//! Counters: `logit.output.reconnects` on every successful dial after the first,
//! `logit.output.requests{class}` once per returned attempt ([`count_request`]), and
//! `logit.output.bytes.dropped{reason="unacknowledged"}` above. A dropped attempt counts no
//! `requests`.

use std::future::poll_fn;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use logit_core::{redact, Diagnostics, Telemetry};
use logit_pipeline::{sockstat, Fault};
use rustls_pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpStream, UnixStream};
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

use crate::count_request;
use crate::tls::{
    alert_in, alert_name, host_only, poll_pending_close_or_alert, record_cert_request, AsyncStream,
    PendingClose, RecordsCertRequest,
};

/// A sink's TLS client settings with the endpoint's server name parsed once, at construction, so a
/// bad endpoint fails startup rather than every batch (decision 10).
pub(crate) struct TlsTarget {
    config: Arc<rustls::ClientConfig>,
    server_name: ServerName<'static>,
    /// Set, and never cleared, once a full handshake to this destination carried a
    /// CertificateRequest. A resumed TLS 1.3 handshake carries none, so this is how a resumed
    /// connection to a server that checks client certificates knows to wait too.
    asks_for_client_cert: AtomicBool,
}

impl TlsTarget {
    /// Takes the server name from `endpoint`'s host ([`host_only`]). Errors on a host that is
    /// neither an IP literal nor a DNS name, such as an empty host or a scoped IPv6 address.
    pub(crate) fn new(
        sink: &str,
        endpoint: &str,
        config: rustls::ClientConfig,
    ) -> anyhow::Result<Self> {
        let host = host_only(endpoint);
        let server_name = ServerName::try_from(host.to_string()).map_err(|err| {
            anyhow::anyhow!(
                "{sink}: endpoint {endpoint:?} has no valid TLS server name in its host \
                 {host:?}: {err}",
                endpoint = redact::url(endpoint),
            )
        })?;
        Ok(Self {
            config: Arc::new(RecordsCertRequest::install(config)),
            server_name,
            asks_for_client_cert: AtomicBool::new(false),
        })
    }

    fn asks_for_client_cert(&self) -> bool {
        self.asks_for_client_cert.load(Ordering::Relaxed)
    }
}

/// Where a [`Dial`] connects.
pub(crate) enum Target<'a> {
    /// A `host:port`, TLS-wrapped when `tls` is set.
    Tcp { endpoint: &'a str, tls: Option<&'a TlsTarget> },
    /// A Unix stream socket; always plaintext.
    Unix { path: &'a Path },
    /// Connections a test scripts in advance.
    #[cfg(test)]
    Scripted(&'a crate::test_support::ScriptedDial),
}

/// `target`, or `script` in its place when a test installed one on a sink.
#[cfg(test)]
pub(crate) fn scripted_or<'a>(
    target: Target<'a>,
    script: &'a Option<Arc<crate::test_support::ScriptedDial>>,
) -> Target<'a> {
    match script {
        Some(script) => Target::Scripted(script),
        None => target,
    }
}

/// What [`connect`] needs, borrowed per `send` from the sink's fields.
pub(crate) struct Dial<'a> {
    pub(crate) target: Target<'a>,
    /// Bounds each dial phase on its own, so a TLS dial takes at most twice this.
    pub(crate) connect_timeout: Duration,
    /// Bounds [`await_acceptance`] on a new TLS 1.3 connection to a server that asks for a client
    /// certificate. Zero skips the wait, as `logit_out` does: it reads the peer's answer to its
    /// own `Hello`.
    pub(crate) accept_timeout: Duration,
    /// The sink kind, for error context.
    pub(crate) sink: &'static str,
    /// Sets `TCP_NODELAY` on a TCP connection before any TLS wrap. `logit_out` sets it: an ack
    /// it waits on can otherwise sit behind Nagle's algorithm and the peer's delayed ACK.
    pub(crate) nodelay: bool,
}

impl Dial<'_> {
    /// Whether a write `Err` can follow bytes reaching the peer, which decides its fault.
    fn is_tls(&self) -> bool {
        match self.target {
            Target::Tcp { tls, .. } => tls.is_some(),
            Target::Unix { .. } => false,
            #[cfg(test)]
            Target::Scripted(script) => script.is_tls(),
        }
    }
}

/// One fresh connection: a TCP connect then, with TLS, a handshake, each under
/// `dial.connect_timeout`, and [`await_acceptance`] under `dial.accept_timeout`; or a Unix connect
/// under `dial.connect_timeout`. No byte of a frame is written before a dial completes, so every
/// failure is `Fault::Clean`, except a TLS alert during the acceptance wait, which is
/// `Fault::Refused`. Counts nothing, so `logit_out` can share it with its own accounting.
pub(crate) async fn connect(dial: &Dial<'_>) -> anyhow::Result<Box<dyn AsyncStream>> {
    connect_tcp_fd(dial).await.map(|(stream, _fd)| stream)
}

/// [`connect`], with the TCP socket's descriptor, taken before any TLS wrap; `None` for a Unix
/// stream and a scripted one. It stays valid while the stream lives.
async fn connect_tcp_fd(
    dial: &Dial<'_>,
) -> anyhow::Result<(Box<dyn AsyncStream>, Option<sockstat::RawFd>)> {
    let sink = dial.sink;
    match dial.target {
        Target::Tcp { endpoint, tls } => {
            let shown = redact::url(endpoint);
            let tcp = tokio::time::timeout(dial.connect_timeout, TcpStream::connect(endpoint))
                .await
                .map_err(|_elapsed| {
                    anyhow::anyhow!(
                        "connecting to {sink} endpoint {shown} timed out after {:?}",
                        dial.connect_timeout
                    )
                })
                .and_then(|r| r.with_context(|| format!("connecting to {sink} endpoint {shown}")))
                .context(Fault::Clean)?;
            if dial.nodelay {
                tcp.set_nodelay(true)
                    .with_context(|| format!("setting TCP_NODELAY toward {sink} endpoint {shown}"))
                    .context(Fault::Clean)?;
            }
            let fd = sockstat::fd_of(&tcp);
            let Some(tls) = tls else { return Ok((Box::new(tcp), fd)) };
            let (mut stream, _) = handshake(tls, tcp, dial.connect_timeout, sink).await?;
            let tls13 =
                stream.get_ref().1.protocol_version() == Some(rustls::ProtocolVersion::TLSv1_3);
            if tls13 && tls.asks_for_client_cert() && !dial.accept_timeout.is_zero() {
                await_acceptance(&mut stream, dial.accept_timeout, sink).await?;
            }
            Ok((Box::new(stream), fd))
        }
        Target::Unix { path } => {
            let unix = tokio::time::timeout(dial.connect_timeout, UnixStream::connect(path))
                .await
                .map_err(|_elapsed| {
                    anyhow::anyhow!(
                        "connecting to {sink} socket {} timed out after {:?}",
                        path.display(),
                        dial.connect_timeout
                    )
                })
                .and_then(|r| {
                    r.with_context(|| format!("connecting to {sink} socket {}", path.display()))
                })
                .context(Fault::Clean)?;
            Ok((Box::new(unix), None))
        }
        #[cfg(test)]
        Target::Scripted(script) => Ok((script.connect().await?, None)),
    }
}

/// The TLS phase of [`connect`], under its own `connect_timeout`, with whether the server sent a
/// CertificateRequest ([`record_cert_request`]), which it also records on `tls`.
async fn handshake<IO>(
    tls: &TlsTarget,
    io: IO,
    connect_timeout: Duration,
    sink: &str,
) -> anyhow::Result<(TlsStream<IO>, bool)>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let connector = TlsConnector::from(Arc::clone(&tls.config));
    let connecting = connector.connect(tls.server_name.clone(), io);
    let (result, cert_requested) =
        record_cert_request(tokio::time::timeout(connect_timeout, connecting)).await;
    let stream = result
        .map_err(|_elapsed| {
            anyhow::anyhow!(
                "TLS handshake with {sink} endpoint timed out after {connect_timeout:?}"
            )
        })
        .and_then(|r| r.with_context(|| format!("TLS handshake with {sink} endpoint")))
        .context(Fault::Clean)?;
    if cert_requested {
        tls.asks_for_client_cert.store(true, Ordering::Relaxed);
    }
    Ok((stream, cert_requested))
}

/// Reads for `bound` before the first write on a new TLS 1.3 connection to a server that asks
/// for a client certificate, so a rejection is read before a frame is written into it (the
/// module doc's "The acceptance wait"). TLS sends no acceptance, so only `bound` passing in
/// silence ends the wait as accepted. A session ticket is read and ignored: a server may send one
/// before its own code rejects the peer. Otherwise:
///
/// - an alert, or the server closing the connection by any means (EOF, `close_notify`, a reset,
///   another read error), is `Fault::Refused`: the server turned the client away before it wrote
///   anything, and a redial would meet the same answer;
/// - application data is `Fault::Clean`, as [`PendingClose::Bytes`] is for a reused connection:
///   the bytes are already off the stream, and a line receiver never sends any.
async fn await_acceptance<IO>(
    stream: &mut TlsStream<IO>,
    bound: Duration,
    sink: &str,
) -> anyhow::Result<()>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let mut buf = [0u8; 1];
    let read = poll_fn(|cx| {
        let mut read_buf = ReadBuf::new(&mut buf);
        Pin::new(&mut *stream)
            .poll_read(cx, &mut read_buf)
            .map(|read| read.map(|()| read_buf.filled().len()))
    });
    let refused = match tokio::time::timeout(bound, read).await {
        Err(_elapsed) => return Ok(()),
        Ok(Ok(0)) => anyhow::anyhow!("the peer closed the connection"),
        Ok(Ok(_)) => {
            return Err(anyhow::anyhow!("the peer sent data before any was written")
                .context(format!("{sink}: waiting for the TLS peer to accept the handshake"))
                .context(Fault::Clean));
        }
        Ok(Err(err)) => match alert_in(&err) {
            Some(alert) => anyhow::anyhow!("the peer sent alert {}", alert_name(alert)),
            None => anyhow::Error::new(err).context("the peer closed the connection"),
        },
    };
    Err(refused
        .context(format!(
            "{sink}: the TLS peer turned the connection away before the first write, as a \
             server does when it rejects the client certificate"
        ))
        .context(Fault::Refused))
}

/// The write-chunk size under [`write_with_progress`]: each chunk's write must accept something
/// within the bound.
const WRITE_CHUNK: usize = 64 * 1024;

/// A write that accepted nothing for the whole bound: the peer stopped reading.
#[derive(Debug)]
pub(crate) struct Stalled(pub(crate) Duration);

impl std::fmt::Display for Stalled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "a write made no progress for {:?}", self.0)
    }
}

impl std::error::Error for Stalled {}

/// Writes and flushes `bytes`, bounding progress rather than the whole write: each `write` call,
/// at most [`WRITE_CHUNK`] bytes, and the final flush must complete within `bound`. A large write
/// on a slow link makes progress and never trips it; a peer that stops reading does, as a
/// [`Stalled`] error. Errors carry no [`Fault`]: the caller knows what already left.
pub(crate) async fn write_with_progress<W>(
    stream: &mut W,
    bytes: &[u8],
    bound: Duration,
) -> anyhow::Result<()>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    write_counting(stream, bytes, bound, &mut 0).await
}

/// [`write_with_progress`], adding to `written` each byte a `write` call accepted, whatever the
/// result: a stalled or failed call leaves the bytes before it accepted. An empty `bytes` only
/// flushes.
async fn write_counting<W>(
    stream: &mut W,
    bytes: &[u8],
    bound: Duration,
    written: &mut usize,
) -> anyhow::Result<()>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    let mut done = 0;
    while done < bytes.len() {
        let end = bytes.len().min(done + WRITE_CHUNK);
        let n = tokio::time::timeout(bound, stream.write(&bytes[done..end]))
            .await
            .map_err(|_elapsed| Stalled(bound))??;
        if n == 0 {
            return Err(io::Error::from(io::ErrorKind::WriteZero).into());
        }
        done += n;
        *written += n;
    }
    tokio::time::timeout(bound, stream.flush()).await.map_err(|_elapsed| Stalled(bound))??;
    Ok(())
}

fn is_stall(err: &anyhow::Error) -> bool {
    err.chain().any(|e| e.is::<Stalled>())
}

/// TLS framing a record adds to its plaintext, at most: a 5-byte header, TLS 1.2 GCM's 8-byte
/// explicit nonce, and a 16-byte tag. TLS 1.3 adds 22.
const TLS_RECORD_OVERHEAD: usize = 29;

/// rustls's plaintext fragment size: each record carries at most this much.
const TLS_MAX_FRAGMENT: usize = 16384;

/// rustls's cap on ciphertext queued in a session (`DEFAULT_BUFFER_LIMIT`).
const TLS_SESSION_BUFFER: u64 = 64 * 1024;

/// What ended a connection the driver abandons, for the warning that reports its lost bytes.
#[derive(Clone, Copy)]
enum Abandoned {
    /// The probe of a reused connection read an EOF or a read error.
    Closed,
    /// The probe read bytes, and closing a connection with unread bytes sends a reset.
    Unsolicited,
    /// The probe read a TLS alert.
    Alert,
    /// A write or flush failed.
    WriteFailed,
}

impl std::fmt::Display for Abandoned {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Closed => "the destination closed or reset the connection",
            Self::Unsolicited => "the destination sent bytes, so closing the connection reset it",
            Self::Alert => "the TLS destination ended the session with an alert",
            Self::WriteFailed => "a write to the destination failed",
        })
    }
}

/// A pooled connection and what the driver tracks beside it.
struct Conn {
    stream: Box<dyn AsyncStream>,
    /// The TCP socket under `stream`, for [`sockstat::tcp_peer_answering`] and
    /// [`sockstat::send_queue`]. `None` on a Unix stream, which is kept and never counted.
    tcp_fd: Option<sockstat::RawFd>,
    /// What a stalled attempt didn't get into `stream`: the rest of its frame, or nothing but a
    /// flush. The next attempt finishes it before writing its own frame, so the receiver never
    /// sees a frame cut and another begun.
    pending: Option<Vec<u8>>,
    /// What `stream` has accepted, and which of it no batch's own accounting covers.
    sent: Sent,
}

impl Conn {
    fn new(stream: Box<dyn AsyncStream>, tcp_fd: Option<sockstat::RawFd>) -> Self {
        Self { stream, tcp_fd, pending: None, sent: Sent::default() }
    }

    /// The connection's send queue, if it is TCP and the kernel answers.
    fn queued(&self) -> Option<u32> {
        self.tcp_fd.and_then(|fd| sockstat::send_queue(fd).ok())
    }
}

/// The bytes a connection's stream accepted, and the stream offsets of each frame an attempt
/// returned `Ambiguous` for that may still be in the send queue. Each such batch is resent, or
/// counted dropped under `at_most_once`, so its bytes aren't counted again as unacknowledged.
/// A frame cut by a failed write needs no pruning: the abandon that follows reads the queue.
#[derive(Default)]
struct Sent {
    /// Bytes accepted over the connection's life: plaintext under TLS.
    total: u64,
    /// In stream order. The last is the frame a kept connection's pending tail belongs to.
    ambiguous: Vec<std::ops::Range<u64>>,
}

impl Sent {
    fn wrote(&mut self, n: usize) {
        self.total += n as u64;
    }

    /// `n` more bytes of the pending tail, which belongs to the last `Ambiguous` frame.
    fn wrote_tail(&mut self, n: usize) {
        self.wrote(n);
        if let Some(last) = self.ambiguous.last_mut() {
            last.end = self.total;
        }
    }

    /// Records `range` as an `Ambiguous` frame's and forgets every earlier one `queued` shows
    /// has left the send queue. With no queue reading, only `range` is kept: a connection whose
    /// queue can't be read counts nothing when abandoned.
    fn ambiguous(&mut self, range: std::ops::Range<u64>, queued: Option<u32>, tls: bool) {
        match queued {
            Some(queued) => {
                let total = self.total;
                self.ambiguous.retain(|range| in_queue(total, range, queued, tls));
            }
            None => self.ambiguous.clear(),
        }
        self.ambiguous.push(range);
    }

    /// The unacknowledged bytes `queued` holds that no batch's own accounting covers: `queued`
    /// less each `Ambiguous` range's part still in it. On plaintext the queue is the last
    /// `queued` bytes sent, so each overlap is computed. Under TLS each range still in reach is
    /// taken out whole, with the most framing it could carry.
    fn unaccounted(&self, queued: u32, tls: bool) -> u64 {
        let queue_start = self.total.saturating_sub(u64::from(queued));
        let overlap: u64 = self
            .ambiguous
            .iter()
            .filter(|range| in_queue(self.total, range, queued, tls))
            .map(|range| {
                if tls {
                    let len = (range.end - range.start) as usize;
                    let records = len.div_ceil(TLS_MAX_FRAGMENT) + len.div_ceil(WRITE_CHUNK) + 1;
                    (len + records * TLS_RECORD_OVERHEAD) as u64
                } else {
                    range.end.saturating_sub(range.start.max(queue_start))
                }
            })
            .sum();
        u64::from(queued).saturating_sub(overlap)
    }
}

/// Whether any of `range` can still be in a send queue holding `queued` bytes, `total` bytes
/// into the stream. The queue only drains from the front, so a range that left it never
/// returns. Under TLS `queued` is ciphertext and up to [`TLS_SESSION_BUFFER`] more may wait in
/// the session.
fn in_queue(total: u64, range: &std::ops::Range<u64>, queued: u32, tls: bool) -> bool {
    let reach = u64::from(queued) + if tls { TLS_SESSION_BUFFER } else { 0 };
    total.saturating_sub(range.end) < reach
}

/// A sink's pooled connection. `has_connected_once` lives here, not on the sink, because a sink
/// holds one transport for its life, so no other arm dials under the same sink.
#[derive(Default)]
pub(crate) struct PooledStream {
    conn: Option<Conn>,
    has_connected_once: bool,
    /// Set once a kernel read reported itself unavailable, so that is reported once.
    kernel_reads_reported: bool,
}

impl PooledStream {
    /// Writes `frame` (non-empty) on the pooled connection or a fresh one, with the module doc's
    /// fault rules, and counts `logit.output.requests` for the attempt. `telemetry` and `diag`
    /// are the sink's ungated handles: the bytes an abandoned connection lost are a destination
    /// verdict, counted once whatever batch is being attempted.
    pub(crate) async fn send(
        &mut self,
        dial: &Dial<'_>,
        frame: &[u8],
        telemetry: &Telemetry,
        diag: &mut Diagnostics,
    ) -> anyhow::Result<()> {
        let result = self.attempt(dial, frame, telemetry, diag).await;
        count_request(telemetry, &result);
        result
    }

    async fn attempt(
        &mut self,
        dial: &Dial<'_>,
        frame: &[u8],
        telemetry: &Telemetry,
        diag: &mut Diagnostics,
    ) -> anyhow::Result<()> {
        let bound = dial.connect_timeout;
        let tls = dial.is_tls();
        let mut retried = false;
        loop {
            let mut conn = match self.conn.take() {
                // Only a reused connection is probed. The probe never suspends, so a dropped
                // `send` can't stop inside it.
                Some(mut conn) => {
                    let probed =
                        poll_pending_close_or_alert(&mut *conn.stream, &mut [0u8; 1]).await;
                    match probed {
                        Ok(PendingClose::Open) => conn,
                        Ok(PendingClose::Eof) => {
                            self.abandon(conn, Abandoned::Closed, tls, telemetry, diag);
                            self.dial(dial, telemetry).await?
                        }
                        Ok(PendingClose::Bytes(_)) => {
                            self.abandon(conn, Abandoned::Unsolicited, tls, telemetry, diag);
                            self.dial(dial, telemetry).await?
                        }
                        // A redial would hide a rejection that already cost the batch written
                        // on this connection.
                        Err(alert) => {
                            self.abandon(conn, Abandoned::Alert, tls, telemetry, diag);
                            return Err(anyhow::anyhow!(
                                "{}: the TLS peer sent alert {} on a pooled connection: it \
                                 rejected the client certificate or ended the session",
                                dial.sink,
                                alert_name(alert)
                            )
                            .context(Fault::Refused));
                        }
                    }
                }
                None => self.dial(dial, telemetry).await?,
            };

            // An earlier attempt's frame goes first.
            if let Some(tail) = conn.pending.take() {
                let mut written = 0;
                let result = write_counting(&mut *conn.stream, &tail, bound, &mut written).await;
                conn.sent.wrote_tail(written);
                match result {
                    Ok(()) => {}
                    // Nothing of this frame was written.
                    Err(err) if is_stall(&err) => {
                        conn.pending = Some(tail[written..].to_vec());
                        let err = err.context(format!(
                            "{}: finishing the frame an earlier attempt left part-written",
                            dial.sink
                        ));
                        return self.keep_stalled(conn, err, Fault::Clean, diag);
                    }
                    // Nothing of this frame was written either, so it goes on a fresh
                    // connection, and that doesn't consume the retry.
                    Err(_) => {
                        self.abandon(conn, Abandoned::WriteFailed, tls, telemetry, diag);
                        conn = self.dial(dial, telemetry).await?;
                    }
                }
            }

            let start = conn.sent.total;
            let mut written = 0;
            let result = write_counting(&mut *conn.stream, frame, bound, &mut written).await;
            conn.sent.wrote(written);
            let err = match result {
                Ok(()) => {
                    self.conn = Some(conn);
                    return Ok(());
                }
                Err(err) => err.context(format!("{}: writing a frame", dial.sink)),
            };
            if is_stall(&err) {
                // The peer stopped reading. A write that stayed pending handed the stream
                // nothing, plaintext or TLS (tokio-rustls answers `Pending` only when its
                // session took nothing), so with nothing accepted the frame never left.
                if written == 0 {
                    return self.keep_stalled(conn, err, Fault::Clean, diag);
                }
                let queued = conn.queued();
                conn.sent.ambiguous(start..conn.sent.total, queued, tls);
                conn.pending = Some(frame[written..].to_vec());
                return self.keep_stalled(conn, err, Fault::Ambiguous, diag);
            }
            // A plaintext `write(2)` that failed accepted nothing of this frame. Under TLS the
            // failed call's chunk may have left as whole records first.
            let accepted_nothing = written == 0 && !tls;
            let reached = if tls { (written + WRITE_CHUNK).min(frame.len()) } else { written };
            conn.sent.ambiguous.push(start..start + reached as u64);
            self.abandon(conn, Abandoned::WriteFailed, tls, telemetry, diag);
            if accepted_nothing && !retried {
                retried = true;
                continue;
            }
            let fault = if accepted_nothing { Fault::Clean } else { Fault::Ambiguous };
            return Err(err.context(fault));
        }
    }

    /// Pools `conn` after a stall, unless its TCP peer isn't answering
    /// ([`sockstat::tcp_peer_answering`]): a vanished host is closed, as a redial would find
    /// nothing better, and the kernel keeps sending what it holds. Returns `err` under `fault`.
    fn keep_stalled(
        &mut self,
        conn: Conn,
        err: anyhow::Error,
        fault: Fault,
        diag: &mut Diagnostics,
    ) -> anyhow::Result<()> {
        let answering = match conn.tcp_fd.map(sockstat::tcp_peer_answering) {
            None | Some(Ok(true)) => true,
            Some(Ok(false)) => false,
            // Kept: a dead peer still ends in a write error once the kernel gives up.
            Some(Err(unavailable)) => {
                self.report_unavailable(&unavailable, diag);
                true
            }
        };
        let err = if answering {
            self.conn = Some(conn);
            err.context("the destination stopped reading; the connection is kept")
        } else {
            err.context("the destination stopped answering; the connection is closed")
        };
        Err(err.context(fault))
    }

    /// Drops `conn`, which `cause` ended with its unacknowledged bytes destroyed, and counts
    /// them ([`Sent::unaccounted`]) as `logit.output.bytes.dropped{reason="unacknowledged"}`
    /// with a throttled warning. Reads the send queue before the drop closes the socket.
    fn abandon(
        &mut self,
        conn: Conn,
        cause: Abandoned,
        tls: bool,
        telemetry: &Telemetry,
        diag: &mut Diagnostics,
    ) {
        let Some(fd) = conn.tcp_fd else { return };
        match sockstat::send_queue(fd) {
            Ok(queued) => {
                let lost = conn.sent.unaccounted(queued, tls);
                if lost > 0 {
                    telemetry.count(
                        "logit.output.bytes.dropped",
                        lost as f64,
                        &[("reason", "unacknowledged")],
                    );
                    diag.warn_throttled(
                        "unacknowledged",
                        format_args!(
                            "{cause}: {lost} bytes of earlier batches the destination never \
                             acknowledged are lost"
                        ),
                    );
                }
            }
            Err(unavailable) => self.report_unavailable(&unavailable, diag),
        }
    }

    /// Reports once that a kernel read isn't available, at `debug` off Linux, where nothing
    /// can be done about it, and at `warn` otherwise.
    fn report_unavailable(&mut self, unavailable: &sockstat::Unavailable, diag: &Diagnostics) {
        if std::mem::replace(&mut self.kernel_reads_reported, true) {
            return;
        }
        let message = format_args!(
            "the kernel's per-connection send counters are not available: {unavailable}; bytes a \
             destination never acknowledged are not counted in logit.output.bytes.dropped, and a \
             stalled connection is kept whether or not its peer still answers"
        );
        if matches!(
            unavailable,
            sockstat::Unavailable::NotLinux | sockstat::Unavailable::NoDescriptor
        ) {
            diag.debug(message);
        } else {
            diag.warn(message);
        }
    }

    async fn dial(&mut self, dial: &Dial<'_>, telemetry: &Telemetry) -> anyhow::Result<Conn> {
        let (stream, fd) = connect_tcp_fd(dial).await?;
        if self.has_connected_once {
            telemetry.count("logit.output.reconnects", 1.0, &[]);
        } else {
            self.has_connected_once = true;
        }
        Ok(Conn::new(stream, fd))
    }

    /// The shutdown backstop `Output::flush` calls: finishes the pooled connection's unwritten
    /// tail, if any, and flushes it, each step bounded by `bound`. A stall or a failure drops the
    /// connection, uncounted: what it held may still reach a receiver that resumes reading.
    pub(crate) async fn flush(&mut self, bound: Duration) -> anyhow::Result<()> {
        let Some(conn) = &mut self.conn else { return Ok(()) };
        let tail = conn.pending.take().unwrap_or_default();
        let result = write_counting(&mut *conn.stream, &tail, bound, &mut 0).await;
        if result.is_err() {
            self.conn = None;
        }
        result
    }

    /// A pool holding `stream`, as after a first successful dial.
    #[cfg(test)]
    pub(crate) fn pooled(stream: Box<dyn AsyncStream>) -> Self {
        Self {
            conn: Some(Conn::new(stream, None)),
            has_connected_once: true,
            kernel_reads_reported: false,
        }
    }

    /// The pooled connection, for a test that breaks it in place.
    #[cfg(test)]
    pub(crate) fn stream_mut(&mut self) -> Option<&mut Box<dyn AsyncStream>> {
        self.conn.as_mut().map(|conn| &mut conn.stream)
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.conn.is_none()
    }

    /// The unwritten tail the pooled connection holds, if any.
    #[cfg(test)]
    fn pending(&self) -> Option<&[u8]> {
        self.conn.as_ref()?.pending.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use std::future::{poll_fn, Future};
    use std::pin::{pin, Pin};
    use std::task::Poll;

    use logit_pipeline::test_util::{scratch_dir, wait_until, TelemetryProbe, RECV_TIMEOUT};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, UnixListener};

    use super::*;
    use crate::test_support::{
        client_config, read_once, rejected_tls_client, server_tls_config, tapped_duplex,
        tls12_server_tls_config, tls_pair, tls_settings, DialStep, FakeStream, ReadStep,
        ScriptedDial, WriteStep,
    };
    use crate::tls::poll_pending_close;

    const FRAME: &[u8] = b"a:1|c\nb:2|c\n";
    const CLASSES: [&str; 5] = ["ok", "clean", "ambiguous", "rejected", "refused"];

    fn scripted(script: &ScriptedDial) -> Dial<'_> {
        Dial {
            target: Target::Scripted(script),
            connect_timeout: Duration::from_secs(1),
            accept_timeout: Duration::ZERO,
            sink: "test_out",
            nodelay: false,
        }
    }

    fn connect_to(fake: &FakeStream) -> DialStep {
        DialStep::Connect(Box::new(fake.clone()))
    }

    /// A connection's ledger after an `Ambiguous` stall on frame A (100 bytes, 40 written), a
    /// later attempt that finishes A's tail and writes batch B (50 bytes) whole, and an
    /// `Ambiguous` stall on frame C (80 bytes, 30 written) with all 180 bytes still queued.
    fn two_stalls_around_a_success(queued_at_second_stall: u32) -> Sent {
        let mut sent = Sent::default();
        sent.wrote(40);
        sent.ambiguous(0..40, Some(40), false);
        sent.wrote_tail(60);
        sent.wrote(50);
        sent.wrote(30);
        sent.ambiguous(150..180, Some(queued_at_second_stall), false);
        sent
    }

    #[test]
    fn every_ambiguous_frame_still_queued_is_left_out_of_the_unacknowledged_count() {
        let sent = two_stalls_around_a_success(180);
        assert_eq!(sent.unaccounted(180, false), 50, "only B's bytes are unaccounted");
        assert_eq!(sent.unaccounted(120, false), 50, "A's last 40 bytes, B, and C's 30");
        assert_eq!(sent.unaccounted(20, false), 0, "only part of C is queued");
    }

    #[test]
    fn a_resent_frame_after_its_finished_tail_leaves_both_out() {
        // Frame A stalls after 40 of 100 bytes; the next attempt finishes its tail and then
        // stalls after 20 bytes of A's resend.
        let mut sent = Sent::default();
        sent.wrote(40);
        sent.ambiguous(0..40, Some(40), false);
        sent.wrote_tail(60);
        sent.wrote(20);
        sent.ambiguous(100..120, Some(120), false);
        assert_eq!(sent.unaccounted(120, false), 0);
    }

    #[test]
    fn an_ambiguous_frame_that_left_the_queue_is_forgotten() {
        // At the second stall only C's 30 bytes are queued, so A has left the queue.
        let sent = two_stalls_around_a_success(30);
        assert_eq!((sent.ambiguous.len(), sent.ambiguous.first()), (1, Some(&(150..180))));
        assert_eq!(sent.unaccounted(30, false), 0);

        let mut unread = Sent::default();
        unread.ambiguous(0..10, None, false);
        unread.wrote(10);
        unread.ambiguous(10..20, None, false);
        assert_eq!(
            (unread.ambiguous.len(), unread.ambiguous.first()),
            (1, Some(&(10..20))),
            "with no queue reading, only the last is kept"
        );
    }

    #[test]
    fn a_tls_ambiguous_frame_is_left_out_whole_with_its_most_framing() {
        // Batch B (500 bytes) whole, then frame A (1,000 bytes) cut. A spans one record and one
        // write call, so it is taken out as 1,000 bytes plus three records' framing.
        let mut sent = Sent::default();
        sent.wrote(500);
        sent.wrote(1000);
        sent.ambiguous(500..1500, Some(2000), true);
        assert_eq!(sent.unaccounted(2000, true), 2000 - (1000 + 3 * 29));
    }

    #[test]
    fn a_tls_ambiguous_frame_too_far_back_for_the_queue_and_a_session_is_not_left_out() {
        // 200,000 bytes followed A, more than the 1,000 queued and a full 64 KiB session.
        let mut sent = Sent::default();
        sent.wrote(100);
        sent.ambiguous(0..100, Some(100), true);
        sent.wrote(200_000);
        assert_eq!(sent.unaccounted(1000, true), 1000);
    }

    fn sink_telemetry() -> (TelemetryProbe, Telemetry) {
        let probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("out", "test_out", "sink");
        (probe, telemetry)
    }

    /// `requests` counted once, under `class` and no other.
    fn assert_one_request(probe: &mut TelemetryProbe, class: &str) {
        let totals = probe.poll();
        for c in CLASSES {
            let want = if c == class { 1.0 } else { 0.0 };
            assert_eq!(totals.sum("logit.output.requests", &[("class", c)]), want, "class={c}");
        }
    }

    fn reconnects(probe: &mut TelemetryProbe) -> f64 {
        probe.sum("logit.output.reconnects", &[])
    }

    /// One poll of `fut`, never waiting.
    async fn poll_once<F: Future + Unpin>(fut: &mut F) -> Poll<F::Output> {
        poll_fn(|cx| Poll::Ready(Pin::new(&mut *fut).poll(cx))).await
    }

    fn io_kind(err: &anyhow::Error) -> Option<io::ErrorKind> {
        err.root_cause().downcast_ref::<io::Error>().map(io::Error::kind)
    }

    // -- The dial ---------------------------------------------------------------------------------

    #[tokio::test]
    async fn a_refused_dial_is_clean_leaves_the_pool_empty_and_counts_no_reconnect() {
        let (endpoint, _held) = logit_pipeline::test_util::refused_addr();
        let endpoint = endpoint.to_string();
        let (mut probe, telemetry) = sink_telemetry();
        let dial = Dial {
            target: Target::Tcp { endpoint: &endpoint, tls: None },
            connect_timeout: Duration::from_secs(1),
            accept_timeout: Duration::ZERO,
            sink: "test_out",
            nodelay: false,
        };
        // A pooled connection the probe finds closed, so the refused dial is a redial: a wrongly
        // counted reconnect would show, and so would the closed connection put back.
        let mut pool = PooledStream::pooled(Box::new(FakeStream::new().reading(ReadStep::Eof)));

        let err = pool
            .send(&dial, FRAME, &telemetry, &mut Diagnostics::default())
            .await
            .expect_err("nothing listens");

        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
        assert_eq!(io_kind(&err), Some(io::ErrorKind::ConnectionRefused), "{err:#}");
        assert!(pool.is_empty());
        assert_one_request(&mut probe, "clean");
        assert_eq!(reconnects(&mut probe), 0.0);
    }

    /// A failed dial names the endpoint with its userinfo masked, whether resolution fails or the
    /// connect timeout fires first.
    #[tokio::test]
    async fn a_failed_dial_prints_the_endpoint_without_its_userinfo() {
        let dial = Dial {
            target: Target::Tcp { endpoint: "tcp://user:hunter2@127.0.0.1:1", tls: None },
            connect_timeout: Duration::from_secs(1),
            accept_timeout: Duration::ZERO,
            sink: "test_out",
            nodelay: false,
        };

        let err = connect(&dial).await.err().expect("the endpoint isn't a socket address");

        let message = format!("{err:#}");
        assert!(message.contains("test_out endpoint tcp://***@127.0.0.1:1"), "{message}");
        assert!(!message.contains("hunter2"), "{message}");
    }

    // -- The probe of a reused connection ---------------------------------------------------------

    /// A pooled connection that probes `read` is replaced before anything is written, and the
    /// replacement's failed first write still gets the one retry: the probe redial didn't
    /// consume it.
    async fn a_closed_pooled_connection_is_redialed_without_consuming_the_retry(read: ReadStep) {
        let closed = FakeStream::new().reading(read);
        let failing = FakeStream::new().on_write(1, WriteStep::Fail(io::ErrorKind::BrokenPipe));
        let healthy = FakeStream::new();
        let script = ScriptedDial::new(false, [connect_to(&failing), connect_to(&healthy)]);
        let (mut probe, telemetry) = sink_telemetry();
        let mut pool = PooledStream::pooled(Box::new(closed.clone()));

        pool.send(&scripted(&script), FRAME, &telemetry, &mut Diagnostics::default())
            .await
            .expect("the retry delivers");

        assert_eq!(closed.state().reads, 1, "the reused connection is probed once");
        assert_eq!(closed.state().writes, 0, "and never written");
        assert_eq!(failing.state().writes, 1);
        assert!(failing.state().flushed.is_empty());
        assert_eq!(healthy.state().flushed, FRAME, "the frame is delivered once, whole");
        assert_eq!(healthy.state().reads, 0, "a fresh connection is never probed");
        assert_eq!(script.dials(), 2, "the probe redial, then the one retry");
        assert!(!pool.is_empty(), "the healthy connection is pooled");
        assert_eq!(reconnects(&mut probe), 2.0, "both dials follow an earlier connect");
        assert_one_request(&mut probe, "ok");
    }

    #[tokio::test]
    async fn a_reused_connection_that_probes_eof_is_redialed_and_the_retry_survives() {
        a_closed_pooled_connection_is_redialed_without_consuming_the_retry(ReadStep::Eof).await;
    }

    #[tokio::test]
    async fn a_reused_connection_that_probes_unsolicited_bytes_is_redialed_and_the_retry_survives()
    {
        let bytes = ReadStep::Bytes(b"x".to_vec());
        a_closed_pooled_connection_is_redialed_without_consuming_the_retry(bytes).await;
    }

    /// The server rejected the client certificate after the handshake, and its alert waits on
    /// the pooled connection. The probe reports it `Refused` with nothing written, and never
    /// redials: a redial would deliver to the scripted spare.
    #[tokio::test]
    async fn a_reused_connection_that_probes_a_tls_alert_is_refused_and_never_redialed() {
        tokio::task::unconstrained(async {
            let ((client_io, client_tap), (server_io, _)) = tapped_duplex(64 * 1024);
            let client = rejected_tls_client(client_io, server_io).await;
            let written = client_tap.written();
            let spare = FakeStream::new();
            let script = ScriptedDial::new(true, [connect_to(&spare)]);
            let (mut probe, telemetry) = sink_telemetry();
            let mut pool = PooledStream::pooled(Box::new(client));

            let err = pool
                .send(&scripted(&script), FRAME, &telemetry, &mut Diagnostics::default())
                .await
                .expect_err("alert");

            assert_eq!(logit_pipeline::classify(&err), Fault::Refused, "{err:#}");
            assert!(format!("{err:#}").contains("alert certificate_required"), "{err:#}");
            assert_eq!(script.dials(), 0, "never redialed");
            assert!(spare.state().unflushed.is_empty());
            assert!(pool.is_empty(), "the rejected connection is dropped");
            assert_eq!(client_tap.written(), written, "nothing of the frame was written");
            assert_one_request(&mut probe, "refused");
        })
        .await;
    }

    // -- A plaintext first write that accepted nothing --------------------------------------------

    /// The one retry after a first write that failed with `step`, delivered on the fresh
    /// connection; then the variant where the retry fails the same way and is `Clean`.
    async fn a_plaintext_first_write_that_accepted_nothing_is_retried_once(step: WriteStep) {
        let pooled = FakeStream::new().on_write(1, step);
        let fresh = FakeStream::new();
        let script = ScriptedDial::new(false, [connect_to(&fresh)]);
        let (mut probe, telemetry) = sink_telemetry();
        let mut pool = PooledStream::pooled(Box::new(pooled.clone()));

        pool.send(&scripted(&script), FRAME, &telemetry, &mut Diagnostics::default())
            .await
            .expect("the retry delivers");

        assert!(pooled.state().flushed.is_empty() && pooled.state().unflushed.is_empty());
        assert_eq!(fresh.state().flushed, FRAME);
        assert_eq!(script.dials(), 1);
        assert_eq!(reconnects(&mut probe), 1.0);
        assert_one_request(&mut probe, "ok");
        // The fresh connection is the one pooled: the next send probes it and dials nothing.
        pool.send(&scripted(&script), FRAME, &telemetry, &mut Diagnostics::default())
            .await
            .expect("reuse");
        assert_eq!((fresh.state().reads, script.dials()), (1, 1));

        // The retry fails too: `Clean`, and no third connection.
        let pooled = FakeStream::new().on_write(1, step);
        let fresh = FakeStream::new().on_write(1, step);
        let spare = FakeStream::new();
        let script = ScriptedDial::new(false, [connect_to(&fresh), connect_to(&spare)]);
        let (mut probe, telemetry) = sink_telemetry();
        let mut pool = PooledStream::pooled(Box::new(pooled.clone()));

        let err = pool
            .send(&scripted(&script), FRAME, &telemetry, &mut Diagnostics::default())
            .await
            .expect_err("both fail");

        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
        let want_kind = match step {
            WriteStep::Zero => io::ErrorKind::WriteZero,
            WriteStep::Fail(kind) => kind,
            WriteStep::Short(_) => unreachable!("a short write accepted bytes"),
        };
        assert_eq!(io_kind(&err), Some(want_kind), "{err:#}");
        assert_eq!((pooled.state().writes, fresh.state().writes), (1, 1));
        assert_eq!((script.dials(), script.unused()), (1, 1), "one retry, then no more");
        assert!(spare.state().unflushed.is_empty());
        assert!(pool.is_empty());
        assert_one_request(&mut probe, "clean");
    }

    #[tokio::test]
    async fn a_plaintext_first_write_error_is_retried_once_then_clean() {
        let step = WriteStep::Fail(io::ErrorKind::BrokenPipe);
        a_plaintext_first_write_that_accepted_nothing_is_retried_once(step).await;
    }

    #[tokio::test]
    async fn a_plaintext_first_write_of_zero_bytes_is_write_zero_retried_once_then_clean() {
        a_plaintext_first_write_that_accepted_nothing_is_retried_once(WriteStep::Zero).await;
    }

    // -- After a byte was accepted ----------------------------------------------------------------

    #[tokio::test]
    async fn a_remainder_failure_after_a_short_first_write_is_ambiguous_and_never_resent() {
        let pooled = FakeStream::new()
            .on_write(1, WriteStep::Short(3))
            .on_write(2, WriteStep::Fail(io::ErrorKind::BrokenPipe));
        let spare = FakeStream::new();
        let script = ScriptedDial::new(false, [connect_to(&spare)]);
        let (mut probe, telemetry) = sink_telemetry();
        let mut pool = PooledStream::pooled(Box::new(pooled.clone()));

        let err = pool
            .send(&scripted(&script), FRAME, &telemetry, &mut Diagnostics::default())
            .await
            .expect_err("remainder");

        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
        let state = pooled.state();
        assert_eq!(state.unflushed, FRAME[..3], "three bytes reached the stream");
        assert_eq!((state.writes, state.flushes), (2, 0));
        drop(state);
        assert!(pool.is_empty(), "a connection holding part of a frame is never pooled");
        assert_eq!(script.dials(), 0, "never resent");
        assert!(spare.state().unflushed.is_empty());
        assert_one_request(&mut probe, "ambiguous");
    }

    #[tokio::test]
    async fn a_flush_failure_after_a_complete_write_is_ambiguous_and_drops_the_connection() {
        let pooled = FakeStream::new().failing_flush(io::ErrorKind::BrokenPipe);
        let spare = FakeStream::new();
        let script = ScriptedDial::new(false, [connect_to(&spare)]);
        let (mut probe, telemetry) = sink_telemetry();
        let mut pool = PooledStream::pooled(Box::new(pooled.clone()));

        let err = pool
            .send(&scripted(&script), FRAME, &telemetry, &mut Diagnostics::default())
            .await
            .expect_err("flush");

        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
        let state = pooled.state();
        assert_eq!(state.unflushed, FRAME);
        assert_eq!((state.writes, state.flushes), (1, 1));
        drop(state);
        assert!(pool.is_empty());
        assert_eq!(script.dials(), 0, "never resent");
        assert_one_request(&mut probe, "ambiguous");
    }

    // -- TLS, over a real tokio-rustls pair -------------------------------------------------------

    /// The first IO write under the session fails. A retry would dial the scripted spare and
    /// deliver there, so `Ambiguous` with no dial proves there was none.
    #[tokio::test]
    async fn a_tls_write_error_is_ambiguous_and_never_retried() {
        let ((client_io, client_tap), (server_io, _)) = tapped_duplex(64 * 1024);
        let (client, _server) = tls_pair(client_io, server_io).await;
        client_tap.fail_writes_after(0);
        let spare = FakeStream::new();
        let script = ScriptedDial::new(true, [connect_to(&spare)]);
        let (mut probe, telemetry) = sink_telemetry();
        let mut pool = PooledStream::pooled(Box::new(client));

        let err = pool
            .send(&scripted(&script), FRAME, &telemetry, &mut Diagnostics::default())
            .await
            .expect_err("write");

        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
        assert_eq!(io_kind(&err), Some(io::ErrorKind::BrokenPipe), "{err:#}");
        assert_eq!(script.dials(), 0, "never retried");
        assert!(spare.state().unflushed.is_empty());
        assert!(pool.is_empty());
        assert_one_request(&mut probe, "ambiguous");
    }

    /// The pipe holds 4096 bytes and the frame is 100 000, so a TLS write returns with ciphertext
    /// still queued in the session. `send` must have pushed all of it before returning: `join!`
    /// never polls it again, and the peer still reads the whole frame.
    #[tokio::test]
    async fn a_tls_send_returns_only_once_the_peer_can_read_the_whole_frame() {
        let ((client_io, _), (server_io, _)) = tapped_duplex(4096);
        let (client, mut server) = tls_pair(client_io, server_io).await;
        let script = ScriptedDial::new(true, [DialStep::Connect(Box::new(client))]);
        let (mut probe, telemetry) = sink_telemetry();
        let mut pool = PooledStream::default();
        let frame: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();

        let mut got = vec![0u8; frame.len()];
        let dial = scripted(&script);
        let mut diag = Diagnostics::default();
        let (sent, read) = tokio::join!(
            pool.send(&dial, &frame, &telemetry, &mut diag),
            tokio::time::timeout(RECV_TIMEOUT, server.read_exact(&mut got)),
        );

        sent.expect("send");
        read.expect("the peer reads the whole frame with the client no longer polled").unwrap();
        assert_eq!(got, frame);
        assert!(!pool.is_empty());
        assert_one_request(&mut probe, "ok");
    }

    // -- Cancellation -----------------------------------------------------------------------------

    /// A `send` dropped inside `write_all`: a 64-byte frame into a 16-byte pipe nobody reads.
    #[tokio::test]
    async fn a_send_dropped_inside_write_all_leaves_the_pool_empty_and_the_next_send_dials_fresh() {
        let (first, mut first_peer) = tokio::io::duplex(16);
        let (second, mut second_peer) = tokio::io::duplex(1024);
        let script = ScriptedDial::new(
            false,
            [DialStep::Connect(Box::new(first)), DialStep::Connect(Box::new(second))],
        );
        let (mut probe, telemetry) = sink_telemetry();
        let mut pool = PooledStream::default();
        let frame = [b'x'; 64];

        {
            let dial = scripted(&script);
            let mut diag = Diagnostics::default();
            let mut send = pin!(pool.send(&dial, &frame, &telemetry, &mut diag));
            assert!(poll_once(&mut send).await.is_pending(), "parked on a full pipe");
        }
        // The first write took 16 bytes, so the send was parked in `write_all`.
        let mut buf = [0u8; 64];
        assert!(matches!(read_once(&mut first_peer, &mut buf).await, Poll::Ready(Ok(16))));
        assert!(pool.is_empty(), "a dropped send leaves nothing pooled");
        assert!(
            matches!(read_once(&mut first_peer, &mut buf).await, Poll::Ready(Ok(0))),
            "the half-written connection was closed"
        );

        pool.send(&scripted(&script), &frame, &telemetry, &mut Diagnostics::default())
            .await
            .expect("a fresh dial delivers");
        let mut got = [0u8; 64];
        second_peer.read_exact(&mut got).await.unwrap();
        assert_eq!(got, frame);
        assert_eq!(script.dials(), 2);
        // The dropped attempt counts no `requests`.
        assert_one_request(&mut probe, "ok");
    }

    /// A peer that stops reading: a 64-byte frame into a 16-byte pipe nobody reads. The write
    /// stalls after 16 bytes, so the attempt is `Ambiguous`, and the connection is kept with the
    /// frame's other 48 bytes. While the peer stays stalled, the next attempt can't finish them and
    /// is `Clean`. Once it reads, the tail goes ahead of the next frame on the same connection.
    #[tokio::test(start_paused = true)]
    async fn a_write_that_stalls_mid_frame_is_ambiguous_and_its_tail_goes_first_on_the_kept_connection(
    ) {
        let (first, mut peer) = tokio::io::duplex(16);
        let script = ScriptedDial::new(false, [DialStep::Connect(Box::new(first))]);
        let (mut probe, telemetry) = sink_telemetry();
        let mut diag = Diagnostics::default();
        let mut pool = PooledStream::default();
        let frame: Vec<u8> = (0..64u8).collect();
        let next: Vec<u8> = (100..164u8).collect();

        let err = pool.send(&scripted(&script), &frame, &telemetry, &mut diag).await.unwrap_err();
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous, "{err:#}");
        assert!(is_stall(&err), "{err:#}");
        assert_eq!(pool.pending(), Some(&frame[16..]), "kept with the unwritten tail");
        assert_one_request(&mut probe, "ambiguous");

        let err = pool.send(&scripted(&script), &next, &telemetry, &mut diag).await.unwrap_err();
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean, "{err:#}");
        assert!(is_stall(&err), "{err:#}");
        assert_eq!(pool.pending(), Some(&frame[16..]), "the tail is still owed");

        let mut got = vec![0u8; frame.len() + next.len()];
        let dial = scripted(&script);
        let (sent, read) = tokio::join!(
            pool.send(&dial, &next, &telemetry, &mut diag),
            peer.read_exact(&mut got),
        );
        sent.expect("the tail, then the frame");
        read.unwrap();
        assert_eq!(got, [frame, next].concat(), "the cut frame finished before the next began");
        assert_eq!(pool.pending(), None);
        assert_eq!(script.dials(), 1, "one connection throughout");
        assert_eq!(reconnects(&mut probe), 0.0);
    }

    /// A pending tail whose write fails is dropped with its connection, and the frame goes on a
    /// fresh one. That redial doesn't consume the retry: the fresh connection's failed first write
    /// still gets it.
    #[tokio::test]
    async fn a_pending_tail_that_fails_is_dropped_and_the_frame_goes_on_a_fresh_connection() {
        let broken = FakeStream::new().on_write(1, WriteStep::Fail(io::ErrorKind::BrokenPipe));
        let refusing = FakeStream::new().on_write(1, WriteStep::Fail(io::ErrorKind::BrokenPipe));
        let healthy = FakeStream::new();
        let script = ScriptedDial::new(false, [connect_to(&refusing), connect_to(&healthy)]);
        let (mut probe, telemetry) = sink_telemetry();
        let mut pool = PooledStream::pooled(Box::new(broken.clone()));
        pool.conn.as_mut().unwrap().pending = Some(b"tail\n".to_vec());

        pool.send(&scripted(&script), FRAME, &telemetry, &mut Diagnostics::default())
            .await
            .expect("the frame is delivered on the retry");

        assert_eq!(broken.state().writes, 1, "the tail was tried once");
        assert!(broken.state().unflushed.is_empty());
        assert_eq!(refusing.state().writes, 1);
        assert_eq!(healthy.state().flushed, FRAME, "the frame alone, with no tail before it");
        assert_eq!(script.dials(), 2, "the tail's redial, then the one retry");
        assert_eq!(pool.pending(), None);
        assert_one_request(&mut probe, "ok");
    }

    /// The shutdown flush finishes a pending tail, and a stalled one is bounded: the connection is
    /// dropped instead of holding shutdown.
    #[tokio::test(start_paused = true)]
    async fn the_shutdown_flush_finishes_a_pending_tail_and_gives_up_on_a_stalled_one() {
        let (stream, mut peer) = tokio::io::duplex(64);
        let mut pool = PooledStream::pooled(Box::new(stream));
        pool.conn.as_mut().unwrap().pending = Some(b"tail\n".to_vec());
        pool.flush(Duration::from_secs(1)).await.expect("the pipe has room");
        let mut got = [0u8; 5];
        peer.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"tail\n");
        assert_eq!(pool.pending(), None);
        assert!(!pool.is_empty());

        let mut pool = PooledStream::pooled(Box::new(NeverWrites));
        pool.conn.as_mut().unwrap().pending = Some(b"tail\n".to_vec());
        let err = pool.flush(Duration::from_secs(1)).await.expect_err("the peer never reads");
        assert!(is_stall(&err), "{err:#}");
        assert!(pool.is_empty(), "the stalled connection is dropped");
    }

    /// A writer whose `poll_write` stays pending forever.
    struct NeverWrites;

    impl AsyncRead for NeverWrites {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for NeverWrites {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }
        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
        fn poll_shutdown(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    /// A first `write` that never accepts a byte stalls past `connect_timeout`: nothing of the
    /// frame left, so the attempt is `Clean`, and the connection is kept with nothing owed. The
    /// next attempt probes it open and stalls the same way, with no redial.
    #[tokio::test(start_paused = true)]
    async fn a_first_write_that_accepts_nothing_is_clean_and_keeps_the_connection() {
        let script = ScriptedDial::new(false, [DialStep::Connect(Box::new(NeverWrites))]);
        let (mut probe, telemetry) = sink_telemetry();
        let mut diag = Diagnostics::default();
        let mut pool = PooledStream::default();
        for _ in 0..2 {
            let err =
                pool.send(&scripted(&script), FRAME, &telemetry, &mut diag).await.unwrap_err();
            assert_eq!(logit_pipeline::classify(&err), Fault::Clean, "{err:#}");
            assert!(is_stall(&err), "{err:#}");
            assert!(format!("{err:#}").contains("the connection is kept"), "{err:#}");
            assert!(!pool.is_empty());
            assert_eq!(pool.pending(), None);
        }
        assert_eq!(script.dials(), 1, "never redialed");
        assert_eq!(probe.poll().sum("logit.output.requests", &[("class", "clean")]), 2.0);
    }

    /// A slow peer that keeps reading never trips the progress bound, however long the whole
    /// write takes.
    #[tokio::test(start_paused = true)]
    async fn a_slow_write_that_keeps_making_progress_is_never_cut() {
        let (mut writer, mut reader) = tokio::io::duplex(16);
        let bound = Duration::from_secs(1);
        let frame = vec![b'y'; 256];
        let reading = tokio::spawn(async move {
            let mut got = Vec::new();
            let mut buf = [0u8; 16];
            while got.len() < 256 {
                // Half the bound between reads: the whole write takes several bounds.
                tokio::time::sleep(bound / 2).await;
                let n = reader.read(&mut buf).await.unwrap();
                got.extend_from_slice(&buf[..n]);
            }
            got
        });
        let start = tokio::time::Instant::now();
        write_with_progress(&mut writer, &frame, bound).await.expect("progress never stalls");
        assert!(start.elapsed() > bound * 4, "the write outlasted the bound several times");
        assert_eq!(reading.await.unwrap(), frame);
    }

    #[tokio::test]
    async fn a_send_dropped_while_dialing_counts_nothing_and_the_next_send_dials_again() {
        let fresh = FakeStream::new();
        let script = ScriptedDial::new(false, [DialStep::Hang, connect_to(&fresh)]);
        let (mut probe, telemetry) = sink_telemetry();
        let mut pool = PooledStream::default();

        {
            let dial = scripted(&script);
            let mut diag = Diagnostics::default();
            let mut send = pin!(pool.send(&dial, FRAME, &telemetry, &mut diag));
            assert!(poll_once(&mut send).await.is_pending());
        }
        assert_eq!(script.dials(), 1, "dropped inside the dial");

        pool.send(&scripted(&script), FRAME, &telemetry, &mut Diagnostics::default())
            .await
            .expect("the next send works");
        assert_eq!(fresh.state().flushed, FRAME);
        assert_eq!(reconnects(&mut probe), 0.0, "the hung dial never connected");
        assert_one_request(&mut probe, "ok");
    }

    /// The probe itself never suspends (`poll_pending_close` is one poll), so the await a `send`
    /// can be dropped at after it is the redial it triggers.
    #[tokio::test]
    async fn a_send_dropped_in_the_redial_after_a_probe_leaves_the_pool_empty() {
        let closed = FakeStream::new().reading(ReadStep::Eof);
        let fresh = FakeStream::new();
        let script = ScriptedDial::new(false, [DialStep::Hang, connect_to(&fresh)]);
        let (mut probe, telemetry) = sink_telemetry();
        let mut pool = PooledStream::pooled(Box::new(closed.clone()));

        {
            let dial = scripted(&script);
            let mut diag = Diagnostics::default();
            let mut send = pin!(pool.send(&dial, FRAME, &telemetry, &mut diag));
            assert!(poll_once(&mut send).await.is_pending());
        }
        assert_eq!((closed.state().reads, script.dials()), (1, 1), "probed, then dialing");
        assert!(pool.is_empty(), "the closed connection isn't put back");

        pool.send(&scripted(&script), FRAME, &telemetry, &mut Diagnostics::default())
            .await
            .expect("the next send works");
        assert_eq!(fresh.state().flushed, FRAME);
        assert_eq!(fresh.state().reads, 0, "a fresh connection is never probed");
        assert!(closed.state().unflushed.is_empty());
        assert_one_request(&mut probe, "ok");
    }

    // -- A real reset mid-frame -------------------------------------------------------------------

    /// The peer reads one byte, which proves the first `write` returned, then resets. 32 MiB is
    /// more than loopback's send and receive buffers together, so that first write was partial
    /// and the reset lands inside `write_all`.
    // `set_linger` is deprecated for blocking the thread on drop; a zero linger doesn't block.
    #[allow(deprecated)]
    #[tokio::test]
    async fn a_real_reset_mid_frame_is_ambiguous() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.read_exact(&mut [0u8; 1]).await.unwrap();
            stream.set_linger(Some(Duration::ZERO)).unwrap();
            drop(stream);
            // Kept listening, so a wrong retry would connect and could succeed.
            listener
        });
        let dial = Dial {
            target: Target::Tcp { endpoint: &endpoint, tls: None },
            connect_timeout: Duration::from_secs(1),
            accept_timeout: Duration::ZERO,
            sink: "test_out",
            nodelay: false,
        };
        let (mut probe, telemetry) = sink_telemetry();
        let mut pool = PooledStream::default();
        let frame = vec![b'x'; 32 << 20];

        let err = tokio::time::timeout(
            RECV_TIMEOUT,
            pool.send(&dial, &frame, &telemetry, &mut Diagnostics::default()),
        )
        .await
        .expect("the reset ends the send")
        .expect_err("a reset mid-frame fails the send");

        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous, "{err:#}");
        assert!(pool.is_empty());
        assert_eq!(reconnects(&mut probe), 0.0, "never redialed");
        assert_one_request(&mut probe, "ambiguous");
        drop(peer.await.unwrap());
    }

    // -- A stalled TLS handshake ------------------------------------------------------------------

    /// The peer accepts TCP and never answers the ClientHello. The clock pauses once the peer has
    /// accepted, so the TCP phase ran on real time under its own timeout, and the handshake
    /// timeout fires on the paused clock: the dial takes less than twice the timeout.
    #[tokio::test]
    async fn a_stalled_tls_handshake_times_out_clean_within_twice_the_connect_timeout() {
        const TIMEOUT: Duration = Duration::from_secs(30);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let config = client_config(&tls_settings(|t| t.ca_file = Some("ca.pem".to_string())));
        let tls = TlsTarget::new("test_out", "localhost:1", config).unwrap();
        let dial = Dial {
            target: Target::Tcp { endpoint: &endpoint, tls: Some(&tls) },
            connect_timeout: TIMEOUT,
            accept_timeout: Duration::ZERO,
            sink: "test_out",
            nodelay: false,
        };

        let start = tokio::time::Instant::now();
        let mut dialing = pin!(connect(&dial));
        let _held = tokio::select! {
            biased;
            done = &mut dialing => panic!("the dial ended before the peer accepted: {:?}", done.err()),
            accepted = listener.accept() => accepted.unwrap().0,
        };
        tokio::time::pause();
        // A bound on the paused clock, so a missing handshake timeout fails rather than hangs.
        let err = tokio::time::timeout(3 * TIMEOUT, dialing)
            .await
            .expect("the handshake phase has its own timeout")
            .err()
            .expect("the peer never answers");
        let elapsed = start.elapsed();

        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
        let message = format!("{err:#}");
        assert!(message.contains("TLS handshake with test_out endpoint timed out"), "{message}");
        assert!(elapsed >= TIMEOUT && elapsed < 2 * TIMEOUT, "took {elapsed:?}");
    }

    // -- The client-certificate request record ----------------------------------------------------

    /// What a test server does with each connection once its handshake completed.
    #[derive(Clone, Copy, Debug)]
    enum AfterAccept {
        Hold,
        CloseNotify,
        DropWithoutCloseNotify,
        Reset,
        Write,
    }

    /// A TLS server on loopback answering one connection per entry of `then`, in order. Its task
    /// returns each handshake's kind, keeping held connections open until it's awaited.
    // `set_linger` is deprecated for blocking the thread on drop; a zero linger doesn't block.
    #[allow(deprecated)]
    async fn serve(
        config: Arc<rustls::ServerConfig>,
        then: Vec<AfterAccept>,
    ) -> (String, tokio::task::JoinHandle<Vec<rustls::HandshakeKind>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let acceptor = tokio_rustls::TlsAcceptor::from(config);
        let server = tokio::spawn(async move {
            let (mut kinds, mut held) = (Vec::new(), Vec::new());
            for then in then {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut stream = acceptor.accept(tcp).await.expect("the handshake completes");
                kinds.push(stream.get_ref().1.handshake_kind().unwrap());
                match then {
                    AfterAccept::Hold => held.push(stream),
                    AfterAccept::CloseNotify => stream.shutdown().await.unwrap(),
                    AfterAccept::DropWithoutCloseNotify => drop(stream.into_inner().0),
                    AfterAccept::Reset => {
                        let tcp = stream.into_inner().0;
                        tcp.set_linger(Some(Duration::ZERO)).unwrap();
                        drop(tcp);
                    }
                    AfterAccept::Write => {
                        stream.write_all(b"x").await.unwrap();
                        stream.flush().await.unwrap();
                        held.push(stream);
                    }
                }
            }
            drop(held);
            kinds
        });
        (endpoint, server)
    }

    /// A target presenting `client.pem` and trusting `ca.pem`.
    fn mtls_target() -> TlsTarget {
        let settings = tls_settings(|t| {
            t.ca_file = Some("ca.pem".to_string());
            t.cert_file = Some("client.pem".to_string());
            t.key_file = Some("client.key".to_string());
        });
        TlsTarget::new("test_out", "localhost:1", client_config(&settings)).unwrap()
    }

    fn tls_dial<'a>(endpoint: &'a str, tls: &'a TlsTarget, accept_timeout: Duration) -> Dial<'a> {
        Dial {
            target: Target::Tcp { endpoint, tls: Some(tls) },
            connect_timeout: RECV_TIMEOUT,
            accept_timeout,
            sink: "test_out",
            nodelay: false,
        }
    }

    /// An hour, against a dial bounded by `RECV_TIMEOUT`: a dial that finishes didn't wait it.
    const NEVER: Duration = Duration::from_secs(3600);

    /// `connect(dial)`, failing the test if it runs past `RECV_TIMEOUT`.
    async fn connect_promptly(dial: &Dial<'_>) -> anyhow::Result<Box<dyn AsyncStream>> {
        tokio::time::timeout(RECV_TIMEOUT, connect(dial)).await.expect("the dial didn't wait")
    }

    /// Reads `stream` until its session tickets arrive, so the next dial can resume.
    async fn read_session_tickets(stream: &mut TlsStream<TcpStream>) {
        let mut buf = [0u8; 1];
        let tickets = poll_fn(|cx| {
            let mut read_buf = ReadBuf::new(&mut buf);
            let read = Pin::new(&mut *stream).poll_read(cx, &mut read_buf);
            match read {
                Poll::Pending if stream.get_ref().1.tls13_tickets_received() > 0 => Poll::Ready(()),
                Poll::Pending => Poll::Pending,
                Poll::Ready(read) => panic!("expected only session tickets, read {read:?}"),
            }
        });
        tokio::time::timeout(RECV_TIMEOUT, tickets).await.expect("the server sends tickets");
    }

    /// A full handshake with a server requiring a client certificate records the request on the
    /// target; the next dial resumes and records none. Resumption needs the config's resolver to
    /// be the one `Arc` the ticket was issued under, so this also pins that the recording resolver
    /// is installed once, not per dial.
    #[tokio::test]
    async fn a_full_handshake_records_the_certificate_request_and_a_resumed_one_does_not() {
        let then = vec![AfterAccept::Hold, AfterAccept::Hold];
        let (endpoint, server) = serve(server_tls_config(true), then).await;
        let tls = mtls_target();
        assert!(!tls.asks_for_client_cert());

        let tcp = TcpStream::connect(&endpoint).await.unwrap();
        let (mut first, requested) =
            handshake(&tls, tcp, RECV_TIMEOUT, "test_out").await.expect("full handshake");
        assert!(requested, "the server sent a CertificateRequest");
        assert!(tls.asks_for_client_cert(), "recorded on the target");
        read_session_tickets(&mut first).await;

        let tcp = TcpStream::connect(&endpoint).await.unwrap();
        let (_second, requested) =
            handshake(&tls, tcp, RECV_TIMEOUT, "test_out").await.expect("resumed handshake");
        assert!(!requested, "a resumed TLS 1.3 handshake carries no CertificateRequest");
        assert!(tls.asks_for_client_cert(), "the record is never cleared");
        let kinds = server.await.unwrap();
        assert_eq!(kinds, [rustls::HandshakeKind::Full, rustls::HandshakeKind::Resumed]);
    }

    // -- The acceptance wait ----------------------------------------------------------------------

    /// The server accepts the client certificate at the TLS layer and sends its session tickets,
    /// then turns the connection away in its own code, as a server that checks the peer's name
    /// after the handshake does: by any close, with no alert. Each close is `Refused`; data from the server is `Clean`.
    #[tokio::test]
    async fn a_close_after_session_tickets_is_refused_and_data_is_clean() {
        let cases = [
            (AfterAccept::CloseNotify, Fault::Refused, "the peer closed the connection"),
            (AfterAccept::DropWithoutCloseNotify, Fault::Refused, "close_notify"),
            (AfterAccept::Reset, Fault::Refused, "the peer closed the connection"),
            (AfterAccept::Write, Fault::Clean, "the peer sent data before any was written"),
        ];
        for (then, want, why) in cases {
            let (endpoint, server) = serve(server_tls_config(true), vec![then]).await;
            let tls = mtls_target();

            let err = connect_promptly(&tls_dial(&endpoint, &tls, NEVER))
                .await
                .err()
                .unwrap_or_else(|| panic!("{then:?}: the server turned the client away"));

            assert_eq!(logit_pipeline::classify(&err), want, "{then:?}: {err:#}");
            assert!(format!("{err:#}").contains(why), "{then:?}: {err:#}");
            server.await.unwrap();
        }
    }

    /// A resumed TLS 1.3 handshake carries no CertificateRequest, but the destination asked for a
    /// certificate on its full handshake, so the resumed dial waits too, and reads the server's
    /// close as `Refused`. The first dial shows silence for the whole bound is acceptance.
    #[tokio::test]
    async fn a_resumed_dial_to_a_server_that_asked_for_a_certificate_waits_and_reads_a_close() {
        const BOUND: Duration = Duration::from_millis(200);
        let then = vec![AfterAccept::Hold, AfterAccept::DropWithoutCloseNotify];
        let (endpoint, server) = serve(server_tls_config(true), then).await;
        let tls = mtls_target();

        let start = std::time::Instant::now();
        let first = connect(&tls_dial(&endpoint, &tls, BOUND)).await.expect("accepted");
        assert!(start.elapsed() >= BOUND, "the wait ran its bound: {:?}", start.elapsed());

        let err = connect_promptly(&tls_dial(&endpoint, &tls, NEVER))
            .await
            .err()
            .expect("the resumed dial waits and reads the close");
        assert_eq!(logit_pipeline::classify(&err), Fault::Refused, "{err:#}");
        drop(first);
        let kinds = server.await.unwrap();
        assert_eq!(kinds, [rustls::HandshakeKind::Full, rustls::HandshakeKind::Resumed]);
    }

    /// A server that never asks for a client certificate gets no wait on a full or a resumed dial.
    #[tokio::test]
    async fn a_destination_that_never_asked_for_a_certificate_never_waits() {
        let then = vec![AfterAccept::Hold, AfterAccept::Hold, AfterAccept::Hold];
        let (endpoint, server) = serve(server_tls_config(false), then).await;
        let tls = mtls_target();

        let full = connect_promptly(&tls_dial(&endpoint, &tls, NEVER)).await.expect("full");
        // The boxed stream hides its session, so a second full handshake reads the tickets.
        let tcp = TcpStream::connect(&endpoint).await.unwrap();
        let (mut ticketed, _) = handshake(&tls, tcp, RECV_TIMEOUT, "test_out").await.unwrap();
        read_session_tickets(&mut ticketed).await;
        let resumed = connect_promptly(&tls_dial(&endpoint, &tls, NEVER)).await.expect("resumed");

        assert!(!tls.asks_for_client_cert());
        drop((full, ticketed, resumed));
        let kinds = server.await.unwrap();
        use rustls::HandshakeKind::{Full, Resumed};
        assert_eq!(kinds, [Full, Full, Resumed]);
    }

    /// TLS 1.2 checks a client certificate inside the handshake, so a rejection is a failed dial,
    /// `Clean`, and no acceptance wait runs.
    #[tokio::test]
    async fn a_tls12_client_certificate_rejection_fails_the_handshake_clean() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let acceptor = tokio_rustls::TlsAcceptor::from(tls12_server_tls_config(true));
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            acceptor.accept(tcp).await.expect_err("no client certificate")
        });
        let settings = tls_settings(|t| t.ca_file = Some("ca.pem".to_string()));
        let tls = TlsTarget::new("test_out", "localhost:1", client_config(&settings)).unwrap();

        let err = connect_promptly(&tls_dial(&endpoint, &tls, NEVER))
            .await
            .err()
            .expect("the server rejects the handshake");

        assert_eq!(logit_pipeline::classify(&err), Fault::Clean, "{err:#}");
        assert!(format!("{err:#}").contains("TLS handshake with test_out endpoint"), "{err:#}");
        server.await.unwrap();
    }

    // -- The loop bound ---------------------------------------------------------------------------

    /// Every fresh connection fails its first write. However the pooled connection probes, one
    /// `send` dials at most twice: the probe redial and the one retry. On TLS it never retries.
    #[tokio::test]
    async fn one_send_dials_at_most_the_probe_redial_and_one_retry() {
        let reset = ReadStep::Fail(io::ErrorKind::ConnectionReset);
        let cases = [
            (false, None, 2, Fault::Clean),
            (false, Some(ReadStep::Pending), 1, Fault::Clean),
            (false, Some(ReadStep::Eof), 2, Fault::Clean),
            (false, Some(ReadStep::Bytes(b"x".to_vec())), 2, Fault::Clean),
            (false, Some(reset), 2, Fault::Clean),
            (true, None, 1, Fault::Ambiguous),
            (true, Some(ReadStep::Pending), 0, Fault::Ambiguous),
            (true, Some(ReadStep::Eof), 1, Fault::Ambiguous),
        ];
        let failing = || {
            FakeStream::new()
                .on_write(1, WriteStep::Fail(io::ErrorKind::BrokenPipe))
                .on_write(2, WriteStep::Fail(io::ErrorKind::BrokenPipe))
        };
        for (tls, pooled, want_dials, want_fault) in cases {
            let what = format!("tls={tls} pooled={pooled:?}");
            let script = ScriptedDial::new(tls, (0..5).map(|_| connect_to(&failing())));
            let (_probe, telemetry) = sink_telemetry();
            let mut pool = match pooled {
                None => PooledStream::default(),
                Some(read) => PooledStream::pooled(Box::new(failing().reading(read))),
            };

            let err = pool
                .send(&scripted(&script), FRAME, &telemetry, &mut Diagnostics::default())
                .await
                .expect_err(&what);

            assert_eq!(script.dials(), want_dials, "{what}");
            assert_eq!(logit_pipeline::classify(&err), want_fault, "{what}");
            assert!(pool.is_empty(), "{what}");
        }
    }

    // -- `logit.output.reconnects` ----------------------------------------------------------------

    #[tokio::test]
    async fn reconnects_count_every_successful_dial_after_the_first() {
        let (mut probe, telemetry) = sink_telemetry();

        // A first-ever dial that fails counts nothing and leaves the next success uncounted.
        let fresh = FakeStream::new();
        let script = ScriptedDial::new(false, [DialStep::Refuse, connect_to(&fresh)]);
        let mut pool = PooledStream::default();
        pool.send(&scripted(&script), FRAME, &telemetry, &mut Diagnostics::default())
            .await
            .expect_err("refused");
        assert!(!pool.has_connected_once);
        pool.send(&scripted(&script), FRAME, &telemetry, &mut Diagnostics::default())
            .await
            .expect("first connect");
        assert_eq!(reconnects(&mut probe), 0.0, "the first successful connect isn't one");
        assert!(pool.has_connected_once);

        // A probe-driven redial is one.
        let closed = FakeStream::new().reading(ReadStep::Eof);
        let script = ScriptedDial::new(false, [connect_to(&FakeStream::new()), DialStep::Refuse]);
        let mut pool = PooledStream::pooled(Box::new(closed));
        pool.send(&scripted(&script), FRAME, &telemetry, &mut Diagnostics::default())
            .await
            .expect("redial");
        assert_eq!(reconnects(&mut probe), 1.0);

        // A failed redial isn't.
        pool.conn = Some(Conn::new(Box::new(FakeStream::new().reading(ReadStep::Eof)), None));
        pool.send(&scripted(&script), FRAME, &telemetry, &mut Diagnostics::default())
            .await
            .expect_err("refused");
        assert_eq!(reconnects(&mut probe), 1.0);
        assert!(pool.has_connected_once);
    }

    // -- Unix streams -----------------------------------------------------------------------------

    fn unix_dial(path: &Path) -> Dial<'_> {
        Dial {
            target: Target::Unix { path },
            connect_timeout: Duration::from_secs(1),
            accept_timeout: Duration::ZERO,
            sink: "test_out",
            nodelay: false,
        }
    }

    async fn accept_frame(listener: &UnixListener) -> UnixStream {
        let (mut conn, _) = tokio::time::timeout(RECV_TIMEOUT, listener.accept())
            .await
            .expect("a connection")
            .unwrap();
        let mut got = vec![0u8; FRAME.len()];
        conn.read_exact(&mut got).await.unwrap();
        assert_eq!(got, FRAME);
        conn
    }

    /// The probe's answer now, from one poll with no task to wake: it reads the readiness the I/O
    /// driver has already stored for the socket.
    fn probe_now(stream: &mut (dyn AsyncStream + '_)) -> PendingClose {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        match pin!(poll_pending_close(stream, &mut [0u8; 1])).poll(&mut cx) {
            Poll::Ready(answer) => answer,
            Poll::Pending => unreachable!("poll_pending_close answers on its first poll"),
        }
    }

    /// The peer half-closes the pooled connection: it sends its FIN but keeps reading, so a write
    /// on that connection would still land there. The frame arriving on a second connection, with
    /// nothing more on the first, is what only the probe's redial produces. The test waits until
    /// the pooled stream probes `Eof`, so the driver's own probe sees the FIN; without the wait,
    /// nothing runs the I/O driver between the close and the `send`, and the probe answers open.
    /// Whether the retry survives is the scripted tests' to prove: a real socket can't count
    /// dials.
    #[tokio::test]
    async fn unix_stream_redials_when_the_probe_finds_the_pooled_connection_closed() {
        let dir = scratch_dir("stream-unix-probe");
        let path = dir.join("s.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let (mut probe, telemetry) = sink_telemetry();
        let mut pool = PooledStream::default();

        pool.send(&unix_dial(&path), FRAME, &telemetry, &mut Diagnostics::default())
            .await
            .expect("first");
        let mut first = accept_frame(&listener).await;
        first.shutdown().await.unwrap();
        let pooled = pool.stream_mut().expect("the first send pooled its connection");
        wait_until("the pooled connection to probe Eof", || {
            matches!(probe_now(&mut **pooled), PendingClose::Eof)
        })
        .await;

        pool.send(&unix_dial(&path), FRAME, &telemetry, &mut Diagnostics::default())
            .await
            .expect("second, on the redial");

        let _second = accept_frame(&listener).await;
        let mut rest = Vec::new();
        tokio::time::timeout(RECV_TIMEOUT, first.read_to_end(&mut rest))
            .await
            .expect("the driver dropped the first connection")
            .unwrap();
        assert!(rest.is_empty(), "the second frame never went to the first connection");
        assert_eq!(reconnects(&mut probe), 1.0);
        let totals = probe.poll();
        assert_eq!(totals.sum("logit.output.requests", &[("class", "ok")]), 2.0);
        assert_eq!(totals.sum("logit.output.requests", &[]), 2.0, "no other class");
    }

    /// The peer stops reading, so the pooled connection probes open and its first write fails
    /// with `EPIPE`: the one retry dials again and delivers.
    #[tokio::test]
    async fn unix_stream_retries_a_first_write_the_peer_refused() {
        let dir = scratch_dir("stream-unix-epipe");
        let path = dir.join("s.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let (mut probe, telemetry) = sink_telemetry();
        let mut pool = PooledStream::default();

        pool.send(&unix_dial(&path), FRAME, &telemetry, &mut Diagnostics::default())
            .await
            .expect("first");
        let first = accept_frame(&listener).await.into_std().unwrap();
        first.shutdown(std::net::Shutdown::Read).unwrap();
        pool.send(&unix_dial(&path), FRAME, &telemetry, &mut Diagnostics::default())
            .await
            .expect("second, on the retry");
        let _second = accept_frame(&listener).await;

        assert_eq!(reconnects(&mut probe), 1.0);
        assert_eq!(probe.sum("logit.output.requests", &[("class", "ok")]), 2.0);
        drop(first);
    }

    #[tokio::test]
    async fn unix_stream_dial_failures_are_clean() {
        let dir = scratch_dir("stream-unix-gone");
        let path = dir.join("s.sock");
        let (mut probe, telemetry) = sink_telemetry();

        // Nothing at the path.
        let mut pool = PooledStream::default();
        let err = pool
            .send(&unix_dial(&path), FRAME, &telemetry, &mut Diagnostics::default())
            .await
            .expect_err("no socket");
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
        assert!(format!("{err:#}").contains("connecting to test_out socket"), "{err:#}");
        assert!(pool.is_empty());
        assert_one_request(&mut probe, "clean");

        // The receiver goes away. Nothing awaits between the close and the next `send`, so the
        // I/O driver hasn't stored the hang-up: the probe answers open, the write fails with
        // `EPIPE`, and the `Clean` comes from the retry's dial finding no socket.
        let listener = UnixListener::bind(&path).unwrap();
        pool.send(&unix_dial(&path), FRAME, &telemetry, &mut Diagnostics::default())
            .await
            .expect("delivered");
        let conn = accept_frame(&listener).await;
        drop((conn, listener));
        std::fs::remove_file(&path).unwrap();
        let err = pool
            .send(&unix_dial(&path), FRAME, &telemetry, &mut Diagnostics::default())
            .await
            .expect_err("gone");
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
        assert!(pool.is_empty());
        assert_eq!(probe.sum("logit.output.requests", &[("class", "clean")]), 2.0);
    }

    // -- A receiver that stops reading, over real sockets -----------------------------------------

    /// How long a write may make no progress in these tests: each stalled send waits it once.
    const STALL_BOUND: Duration = Duration::from_millis(200);

    fn bytes_dropped(probe: &mut TelemetryProbe) -> f64 {
        probe.sum("logit.output.bytes.dropped", &[("reason", "unacknowledged")])
    }

    /// Sends `frame` until a send fails, then twice more, each failing on the stall too. Returns
    /// the bytes of the frames sent whole.
    async fn send_until_stalled_three_times(
        pool: &mut PooledStream,
        dial: &Dial<'_>,
        frame: &[u8],
        telemetry: &Telemetry,
        diag: &mut Diagnostics,
    ) -> u64 {
        let mut whole = 0;
        let first = loop {
            match pool.send(dial, frame, telemetry, diag).await {
                Ok(()) => whole += frame.len() as u64,
                Err(err) => break err,
            }
        };
        assert!(is_stall(&first), "{first:#}");
        for _ in 0..2 {
            let err = pool.send(dial, frame, telemetry, diag).await.expect_err("still stalled");
            assert_eq!(logit_pipeline::classify(&err), Fault::Clean, "{err:#}");
            assert!(is_stall(&err), "{err:#}");
        }
        assert!(!pool.is_empty(), "the stalled connection is kept");
        whole
    }

    /// The receiver accepts and never reads. Once its buffers fill, each send stalls, and the
    /// connection is kept: no redial fills another buffer pair behind the frozen listener. When
    /// the receiver closes with the bytes unread, its reset destroys what the client's kernel
    /// still held, and the next send's probe counts it: the bytes of every frame sent whole, less
    /// what the receiver's own buffer took and acknowledged. The head of the frame the stall cut
    /// is the stalled batch's own, so it isn't counted.
    #[tokio::test]
    async fn a_stalled_connection_is_kept_and_a_reset_counts_what_the_receiver_never_acknowledged()
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let dial = Dial {
            target: Target::Tcp { endpoint: &endpoint, tls: None },
            connect_timeout: STALL_BOUND,
            accept_timeout: Duration::ZERO,
            sink: "test_out",
            nodelay: false,
        };
        let (mut probe, telemetry) = sink_telemetry();
        let mut diag = Diagnostics::default();
        let mut pool = PooledStream::default();
        let frame = vec![b'x'; 64 * 1024];

        let whole =
            send_until_stalled_three_times(&mut pool, &dial, &frame, &telemetry, &mut diag).await;

        assert_eq!(reconnects(&mut probe), 0.0, "never redialed");
        let (receiver, _) = listener.accept().await.unwrap();
        let listener_fd = sockstat::fd_of(&listener).unwrap();
        let (waiting, _) = sockstat::listen_queue(listener_fd).unwrap();
        assert_eq!(waiting, 0, "no second connection waits behind the frozen receiver");
        assert_eq!(bytes_dropped(&mut probe), 0.0, "a stall loses nothing yet");

        // Everything the receiver's kernel acknowledged is in its receive queue, unread.
        let mut buf = vec![0u8; 64 << 20];
        let held = receiver.peek(&mut buf).await.unwrap() as u64;
        drop(receiver);
        let pooled = pool.stream_mut().expect("kept");
        wait_until("the pooled connection to probe the reset", || {
            matches!(probe_now(&mut **pooled), PendingClose::Eof)
        })
        .await;
        pool.send(&dial, &frame, &telemetry, &mut diag).await.expect("delivered on a redial");

        assert_eq!(bytes_dropped(&mut probe), (whole - held) as f64, "whole {whole}, held {held}");
        assert_eq!(reconnects(&mut probe), 1.0);
        assert_eq!(diag.occurrences("unacknowledged"), 1);
    }

    /// Waits until the connection is idle: every byte `total` put on the stream is read (`read`)
    /// or held in the receiver's buffer and acknowledged, so the client's send queue holds the
    /// rest and nothing is in flight. Returns what the receiver holds.
    async fn wait_idle(
        fd: sockstat::RawFd,
        receiver: &std::net::TcpStream,
        read: u64,
        total: u64,
    ) -> u64 {
        let mut buf = vec![0u8; 8 << 20];
        let mut held = 0;
        wait_until("the connection to go idle", || {
            held = receiver.peek(&mut buf).unwrap() as u64;
            u64::from(sockstat::send_queue(fd).unwrap()) + read + held == total
        })
        .await;
        held
    }

    /// Two `Ambiguous` stalls on one kept connection, with the first frame still in the send
    /// queue at the second. The second stall's queue reading is what keeps the first frame's
    /// range, so the reset's count leaves out the queued bytes of both frames and counts only
    /// the frames sent whole that the receiver never acknowledged.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_second_stall_keeps_the_first_stalled_frame_out_of_the_unacknowledged_count() {
        use std::io::Read;

        // A set receive buffer doesn't grow as the receiver reads, so it holds about as much when
        // full again as before.
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_recv_buffer_size(64 * 1024).unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = socket.listen(8).unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let dial = Dial {
            target: Target::Tcp { endpoint: &endpoint, tls: None },
            connect_timeout: STALL_BOUND,
            accept_timeout: Duration::ZERO,
            sink: "test_out",
            nodelay: false,
        };
        let (mut probe, telemetry) = sink_telemetry();
        let mut diag = Diagnostics::default();
        let mut pool = PooledStream::default();
        // Not a multiple of the write chunk, so a stall rarely falls on a frame boundary.
        let frame = vec![b'x'; 100_003];
        let len = frame.len() as u64;

        // Frame A stalls part-written.
        let mut start = 0;
        let err = loop {
            match pool.send(&dial, &frame, &telemetry, &mut diag).await {
                Ok(()) => start += len,
                Err(err) => break err,
            }
        };
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous, "{err:#}");
        let a = start..start + len;
        let a_tail = pool.pending().expect("A's tail is kept").len() as u64;
        let fd = pool.conn.as_ref().and_then(|conn| conn.tcp_fd).expect("a TCP connection");
        let (receiver, _) = listener.accept().await.unwrap();
        let mut receiver = receiver.into_std().unwrap();
        receiver.set_nonblocking(false).unwrap();
        receiver.set_read_timeout(Some(RECV_TIMEOUT)).unwrap();
        let held = wait_idle(fd, &receiver, 0, a.end - a_tail).await;
        let queued = a.end - a_tail - held;

        // Reading A's tail and half the queue frees room for the tail and more, while A's end
        // stays past everything the receiver can have read or acknowledged.
        let read = queued / 2 + a_tail;
        receiver.read_exact(&mut vec![0u8; read as usize]).unwrap();
        wait_idle(fd, &receiver, read, a.end - a_tail).await;

        // The next attempt finishes A; frames go whole until frame B stalls part-written.
        let mut start = a.end;
        let err = loop {
            match pool.send(&dial, &frame, &telemetry, &mut diag).await {
                Ok(()) => start += len,
                Err(err) => break err,
            }
        };
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous, "{err:#}");
        let b_tail = pool.pending().expect("B's tail is kept").len() as u64;
        let b = start..start + len - b_tail;
        let held = wait_idle(fd, &receiver, read, b.end).await;
        let acked = read + held;
        assert!(a.end > acked, "A is still queued: A {a:?}, acknowledged {acked}");
        assert_eq!(bytes_dropped(&mut probe), 0.0, "a stall loses nothing yet");

        // Closing with bytes unread resets the connection.
        drop(receiver);
        let pooled = pool.stream_mut().expect("kept");
        wait_until("the pooled connection to probe the reset", || {
            matches!(probe_now(&mut **pooled), PendingClose::Eof)
        })
        .await;
        pool.send(&dial, FRAME, &telemetry, &mut diag).await.expect("delivered on a redial");

        let queued_part = |range: &std::ops::Range<u64>| range.end - range.start.max(acked);
        let whole_unacknowledged = b.end - acked - queued_part(&a) - queued_part(&b);
        assert!(whole_unacknowledged > 0, "A {a:?}, B {b:?}, acknowledged {acked}");
        assert_eq!(
            bytes_dropped(&mut probe),
            whole_unacknowledged as f64,
            "A {a:?}, B {b:?}, acknowledged {acked}"
        );
        assert_eq!(reconnects(&mut probe), 1.0);
    }

    /// The same over TLS: the stalled connection is kept, and the receiver's reset counts a loss.
    /// Ciphertext's framing makes the count an estimate, so only its presence is checked.
    #[tokio::test]
    async fn a_stalled_tls_connection_is_kept_and_a_reset_counts_a_loss() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let acceptor = tokio_rustls::TlsAcceptor::from(server_tls_config(false));
        let (release, released) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let frozen = acceptor.accept(tcp).await.expect("the handshake completes");
            released.await.unwrap();
            drop(frozen);
            let (tcp, _) = listener.accept().await.unwrap();
            let mut fresh = acceptor.accept(tcp).await.expect("the redial's handshake completes");
            let mut got = vec![0u8; FRAME.len()];
            fresh.read_exact(&mut got).await.unwrap();
            (got, listener)
        });
        let tls = mtls_target();
        let dial = Dial {
            target: Target::Tcp { endpoint: &endpoint, tls: Some(&tls) },
            connect_timeout: STALL_BOUND,
            accept_timeout: Duration::ZERO,
            sink: "test_out",
            nodelay: false,
        };
        let (mut probe, telemetry) = sink_telemetry();
        let mut diag = Diagnostics::default();
        let mut pool = PooledStream::default();
        let frame = vec![b'x'; 64 * 1024];

        send_until_stalled_three_times(&mut pool, &dial, &frame, &telemetry, &mut diag).await;
        assert_eq!(reconnects(&mut probe), 0.0, "never redialed");

        release.send(()).unwrap();
        let pooled = pool.stream_mut().expect("kept");
        wait_until("the pooled connection to probe the reset", || {
            matches!(probe_now(&mut **pooled), PendingClose::Eof)
        })
        .await;
        pool.send(&dial, FRAME, &telemetry, &mut diag).await.expect("delivered on a redial");

        let (got, _listener) = server.await.unwrap();
        assert_eq!(got, FRAME, "the redial carries the new frame alone");
        assert!(bytes_dropped(&mut probe) > 0.0);
        assert_eq!(reconnects(&mut probe), 1.0);
    }

    /// The same over a Unix stream: kept while stalled, but nothing is counted when the receiver
    /// goes, since its close frees the queue the count would read.
    #[tokio::test]
    async fn a_stalled_unix_stream_is_kept_and_its_receiver_s_close_counts_nothing() {
        let dir = scratch_dir("stream-unix-stall");
        let path = dir.join("s.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let dial = Dial { connect_timeout: STALL_BOUND, ..unix_dial(&path) };
        let (mut probe, telemetry) = sink_telemetry();
        let mut diag = Diagnostics::default();
        let mut pool = PooledStream::default();
        let frame = vec![b'x'; 64 * 1024];

        send_until_stalled_three_times(&mut pool, &dial, &frame, &telemetry, &mut diag).await;
        assert_eq!(reconnects(&mut probe), 0.0, "never redialed");

        let (receiver, _) = listener.accept().await.unwrap();
        drop(receiver);
        let pooled = pool.stream_mut().expect("kept");
        wait_until("the pooled connection to probe the close", || {
            matches!(probe_now(&mut **pooled), PendingClose::Eof)
        })
        .await;
        pool.send(&dial, FRAME, &telemetry, &mut diag).await.expect("delivered on a redial");
        let _fresh = accept_frame(&listener).await;

        assert_eq!(reconnects(&mut probe), 1.0);
        assert_eq!(bytes_dropped(&mut probe), 0.0);
    }

    // -- The server name --------------------------------------------------------------------------

    #[test]
    fn a_tls_target_needs_a_server_name_in_the_endpoint_host() {
        let config = || client_config(&Default::default());
        for endpoint in ["[fe80::1%eth0]:514", ":514", "bad host:514"] {
            let err = TlsTarget::new("test_out", endpoint, config()).err().expect(endpoint);
            let message = err.to_string();
            assert!(message.contains("test_out") && message.contains(endpoint), "{message}");
        }
        for endpoint in ["127.0.0.1:514", "[::1]:514", "localhost:514", "logs.example.com:6514"] {
            TlsTarget::new("test_out", endpoint, config()).expect(endpoint);
        }
    }
}
