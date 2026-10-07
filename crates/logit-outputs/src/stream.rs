//! The pooled-stream send driver `statsd_out`, `syslog_out`, and `graphite_out` share: one
//! connection kept between batches, dialed lazily over TCP, TCP with TLS, or a Unix stream, and
//! one frame written per `send` (ADR `sink-send-path-and-attempt-accounting`, decisions 4 to 6).
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
//! | a reused connection's probe answers anything but open (EOF, unsolicited bytes, a reset) | no fault: the driver redials, and the redial doesn't consume the retry | the old connection was dead before this frame | `a_reused_connection_that_probes_eof_is_redialed_and_the_retry_survives`, `a_reused_connection_that_probes_unsolicited_bytes_is_redialed_and_the_retry_survives`, `unix_stream_redials_when_the_probe_finds_the_pooled_connection_closed` |
//! | the plaintext first `write` fails (`Err`, or `Ok(0)` read as `WriteZero`) | retried once on a fresh connection; a second failure is `Clean` | a failed plaintext first write accepted nothing | `a_plaintext_first_write_error_is_retried_once_then_clean`, `a_plaintext_first_write_of_zero_bytes_is_write_zero_retried_once_then_clean`, `unix_stream_retries_a_first_write_the_peer_refused` |
//! | the plaintext first `write` makes no progress for `connect_timeout` | `Clean`, the connection dropped | it accepted nothing | `a_stalled_plaintext_first_write_is_clean` |
//! | the TLS first `write` fails | `Ambiguous`, never retried | rustls may have put whole records on the wire first (below) | `a_tls_write_error_is_ambiguous_and_never_retried` |
//! | the remainder's `write_all`, or the `flush`, fails, a reset mid-frame included | `Ambiguous`, never retried | part of the frame left, and a line receiver keeps every complete line it got | `a_remainder_failure_after_a_short_first_write_is_ambiguous_and_never_resent`, `a_flush_failure_after_a_complete_write_is_ambiguous_and_drops_the_connection`, `a_real_reset_mid_frame_is_ambiguous` |
//! | any other write, or the flush, makes no progress for `connect_timeout` | `Ambiguous`, the connection dropped | part of the frame may have left | `a_write_that_makes_no_progress_fails_ambiguous_and_the_next_send_dials_fresh` |
//!
//! `connect_timeout` bounds each write's progress as well as each dial phase
//! ([`write_with_progress`]), so a peer that accepts the connection and stops reading fails the
//! attempt instead of parking it until shutdown; a large frame on a slow link keeps making progress
//! and never trips it (`a_slow_write_that_keeps_making_progress_is_never_cut`).
//!
//! A TLS write `Err` is `Ambiguous` because rustls may have put whole records on the wire first.
//! rustls splits what each session write accepted into records of at most 16384 bytes of
//! plaintext, with no regard for line or message boundaries, so a record can end mid-line, but a
//! line or message stream's receiver keeps every complete line or message in what arrived
//! (`crate::stream_pins` pins the tokio-rustls side). `flush` runs before a connection is pooled
//! and before `Ok`, because a TLS write's `Ok` can leave ciphertext queued in the session.
//!
//! The connection is moved out of the pool for the whole attempt and put back only after the
//! flush, so a `send` future dropped at any await leaves the pool empty and the next `send`
//! dials fresh rather than write into a stream holding part of a frame.
//!
//! Counters: `logit.output.reconnects` on every successful dial after the first, and
//! `logit.output.requests{class}` once per returned attempt ([`count_request`]). A dropped attempt
//! counts no `requests`.

use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use logit_core::{redact, Telemetry};
use logit_pipeline::Fault;
use rustls_pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpStream, UnixStream};
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

use crate::count_request;
use crate::tls::{host_only, poll_pending_close, AsyncStream, PendingClose};

/// A sink's TLS client settings with the endpoint's server name parsed once, at construction, so a
/// bad endpoint fails startup rather than every batch (decision 10).
pub(crate) struct TlsTarget {
    config: Arc<rustls::ClientConfig>,
    server_name: ServerName<'static>,
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
        Ok(Self { config: Arc::new(config), server_name })
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
/// `dial.connect_timeout`; or a Unix connect under it. Every failure is `Fault::Clean`, since no
/// byte of a frame is written before a dial completes. Counts nothing, so `logit_out` can share
/// it with its own accounting.
pub(crate) async fn connect(dial: &Dial<'_>) -> anyhow::Result<Box<dyn AsyncStream>> {
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
            match tls {
                Some(tls) => Ok(Box::new(handshake(tls, tcp, dial.connect_timeout, sink).await?)),
                None => Ok(Box::new(tcp)),
            }
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
            Ok(Box::new(unix))
        }
        #[cfg(test)]
        Target::Scripted(script) => script.connect().await,
    }
}

/// The TLS phase of [`connect`], under its own `connect_timeout`.
async fn handshake<IO>(
    tls: &TlsTarget,
    io: IO,
    connect_timeout: Duration,
    sink: &str,
) -> anyhow::Result<TlsStream<IO>>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let connector = TlsConnector::from(Arc::clone(&tls.config));
    tokio::time::timeout(connect_timeout, connector.connect(tls.server_name.clone(), io))
        .await
        .map_err(|_elapsed| {
            anyhow::anyhow!(
                "TLS handshake with {sink} endpoint timed out after {connect_timeout:?}"
            )
        })
        .and_then(|r| r.with_context(|| format!("TLS handshake with {sink} endpoint")))
        .context(Fault::Clean)
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
    let mut written = 0;
    while written < bytes.len() {
        let end = bytes.len().min(written + WRITE_CHUNK);
        let n = tokio::time::timeout(bound, stream.write(&bytes[written..end]))
            .await
            .map_err(|_elapsed| Stalled(bound))??;
        if n == 0 {
            return Err(io::Error::from(io::ErrorKind::WriteZero).into());
        }
        written += n;
    }
    tokio::time::timeout(bound, stream.flush()).await.map_err(|_elapsed| Stalled(bound))??;
    Ok(())
}

/// A sink's pooled connection. `has_connected_once` lives here, not on the sink, because a sink
/// holds one transport for its life, so no other arm dials under the same sink.
#[derive(Default)]
pub(crate) struct PooledStream {
    stream: Option<Box<dyn AsyncStream>>,
    has_connected_once: bool,
}

impl PooledStream {
    /// Writes `frame` (non-empty) on the pooled connection or a fresh one, with the module doc's
    /// fault rules, and counts `logit.output.requests` for the attempt.
    pub(crate) async fn send(
        &mut self,
        dial: &Dial<'_>,
        frame: &[u8],
        telemetry: &Telemetry,
    ) -> anyhow::Result<()> {
        let result = self.attempt(dial, frame, telemetry).await;
        count_request(telemetry, &result);
        result
    }

    async fn attempt(
        &mut self,
        dial: &Dial<'_>,
        frame: &[u8],
        telemetry: &Telemetry,
    ) -> anyhow::Result<()> {
        let mut retried = false;
        loop {
            let mut conn = match self.stream.take() {
                // Only a reused connection is probed. `poll_pending_close` never suspends, so a
                // dropped `send` can't stop inside it.
                Some(mut conn) => {
                    let probed = poll_pending_close(&mut *conn, &mut [0u8; 1]).await;
                    match probed {
                        PendingClose::Open => conn,
                        PendingClose::Eof | PendingClose::Bytes(_) => {
                            drop(conn);
                            self.dial(dial, telemetry).await?
                        }
                    }
                }
                None => self.dial(dial, telemetry).await?,
            };

            let bound = dial.connect_timeout;
            let first = match tokio::time::timeout(bound, conn.write(frame)).await {
                // The peer stopped reading. A plaintext `write` that stayed pending handed the
                // kernel nothing, so it's `Clean`, as a failed one is below; a TLS session may
                // hold part of the frame, so there it's `Ambiguous`. No retry inside `send`
                // either way: the peer that stalled this write would stall the next.
                Err(_elapsed) => {
                    let fault = if dial.is_tls() { Fault::Ambiguous } else { Fault::Clean };
                    return Err(anyhow::Error::new(Stalled(bound))
                        .context(format!("{}: writing a frame", dial.sink))
                        .context(fault));
                }
                Ok(Ok(0)) if !frame.is_empty() => {
                    Err(io::Error::new(io::ErrorKind::WriteZero, "wrote zero bytes"))
                }
                Ok(other) => other,
            };
            match first {
                Ok(n) => {
                    return match write_with_progress(&mut *conn, &frame[n..], bound).await {
                        Ok(()) => {
                            self.stream = Some(conn);
                            Ok(())
                        }
                        // Part of the frame may be at the peer; the connection is dropped.
                        Err(err) => Err(err
                            .context(format!("{}: writing a frame", dial.sink))
                            .context(Fault::Ambiguous)),
                    };
                }
                // A plaintext `write(2)` that failed accepted nothing of this frame.
                Err(_) if !dial.is_tls() && !retried => retried = true,
                Err(err) => {
                    let fault = if dial.is_tls() { Fault::Ambiguous } else { Fault::Clean };
                    return Err(anyhow::Error::new(err)
                        .context(format!("{}: writing a frame", dial.sink))
                        .context(fault));
                }
            }
        }
    }

    async fn dial(
        &mut self,
        dial: &Dial<'_>,
        telemetry: &Telemetry,
    ) -> anyhow::Result<Box<dyn AsyncStream>> {
        let conn = connect(dial).await?;
        if self.has_connected_once {
            telemetry.count("logit.output.reconnects", 1.0, &[]);
        } else {
            self.has_connected_once = true;
        }
        Ok(conn)
    }

    /// Flushes the pooled connection, if any. `send` already flushed it, so this is the
    /// shutdown backstop `Output::flush` calls.
    pub(crate) async fn flush(&mut self) -> io::Result<()> {
        match &mut self.stream {
            Some(stream) => stream.flush().await,
            None => Ok(()),
        }
    }

    /// A pool holding `stream`, as after a first successful dial.
    #[cfg(test)]
    pub(crate) fn pooled(stream: Box<dyn AsyncStream>) -> Self {
        Self { stream: Some(stream), has_connected_once: true }
    }

    /// The pooled connection, for a test that breaks it in place.
    #[cfg(test)]
    pub(crate) fn stream_mut(&mut self) -> Option<&mut Box<dyn AsyncStream>> {
        self.stream.as_mut()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.stream.is_none()
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
        client_config, read_once, tapped_duplex, tls_pair, tls_settings, DialStep, FakeStream,
        ReadStep, ScriptedDial, WriteStep,
    };

    const FRAME: &[u8] = b"a:1|c\nb:2|c\n";
    const CLASSES: [&str; 5] = ["ok", "clean", "ambiguous", "rejected", "refused"];

    fn scripted(script: &ScriptedDial) -> Dial<'_> {
        Dial {
            target: Target::Scripted(script),
            connect_timeout: Duration::from_secs(1),
            sink: "test_out",
            nodelay: false,
        }
    }

    fn connect_to(fake: &FakeStream) -> DialStep {
        DialStep::Connect(Box::new(fake.clone()))
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
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        drop(listener); // nothing listens there now
        let (mut probe, telemetry) = sink_telemetry();
        let dial = Dial {
            target: Target::Tcp { endpoint: &endpoint, tls: None },
            connect_timeout: Duration::from_secs(1),
            sink: "test_out",
            nodelay: false,
        };
        // A pooled connection the probe finds closed, so the refused dial is a redial: a wrongly
        // counted reconnect would show, and so would the closed connection put back.
        let mut pool = PooledStream::pooled(Box::new(FakeStream::new().reading(ReadStep::Eof)));

        let err = pool.send(&dial, FRAME, &telemetry).await.expect_err("nothing listens");

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

        pool.send(&scripted(&script), FRAME, &telemetry).await.expect("the retry delivers");

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

    // -- A plaintext first write that accepted nothing --------------------------------------------

    /// The one retry after a first write that failed with `step`, delivered on the fresh
    /// connection; then the variant where the retry fails the same way and is `Clean`.
    async fn a_plaintext_first_write_that_accepted_nothing_is_retried_once(step: WriteStep) {
        let pooled = FakeStream::new().on_write(1, step);
        let fresh = FakeStream::new();
        let script = ScriptedDial::new(false, [connect_to(&fresh)]);
        let (mut probe, telemetry) = sink_telemetry();
        let mut pool = PooledStream::pooled(Box::new(pooled.clone()));

        pool.send(&scripted(&script), FRAME, &telemetry).await.expect("the retry delivers");

        assert!(pooled.state().flushed.is_empty() && pooled.state().unflushed.is_empty());
        assert_eq!(fresh.state().flushed, FRAME);
        assert_eq!(script.dials(), 1);
        assert_eq!(reconnects(&mut probe), 1.0);
        assert_one_request(&mut probe, "ok");
        // The fresh connection is the one pooled: the next send probes it and dials nothing.
        pool.send(&scripted(&script), FRAME, &telemetry).await.expect("reuse");
        assert_eq!((fresh.state().reads, script.dials()), (1, 1));

        // The retry fails too: `Clean`, and no third connection.
        let pooled = FakeStream::new().on_write(1, step);
        let fresh = FakeStream::new().on_write(1, step);
        let spare = FakeStream::new();
        let script = ScriptedDial::new(false, [connect_to(&fresh), connect_to(&spare)]);
        let (mut probe, telemetry) = sink_telemetry();
        let mut pool = PooledStream::pooled(Box::new(pooled.clone()));

        let err = pool.send(&scripted(&script), FRAME, &telemetry).await.expect_err("both fail");

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

        let err = pool.send(&scripted(&script), FRAME, &telemetry).await.expect_err("remainder");

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

        let err = pool.send(&scripted(&script), FRAME, &telemetry).await.expect_err("flush");

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

        let err = pool.send(&scripted(&script), FRAME, &telemetry).await.expect_err("write");

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
        let (sent, read) = tokio::join!(
            pool.send(&dial, &frame, &telemetry),
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
            let mut send = pin!(pool.send(&dial, &frame, &telemetry));
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

        pool.send(&scripted(&script), &frame, &telemetry).await.expect("a fresh dial delivers");
        let mut got = [0u8; 64];
        second_peer.read_exact(&mut got).await.unwrap();
        assert_eq!(got, frame);
        assert_eq!(script.dials(), 2);
        // The dropped attempt counts no `requests`.
        assert_one_request(&mut probe, "ok");
    }

    /// A peer that stops reading: a 64-byte frame into a 16-byte pipe nobody reads. The write
    /// makes no progress for `connect_timeout`, so the attempt fails `Ambiguous` instead of
    /// parking, and the next send dials fresh.
    #[tokio::test(start_paused = true)]
    async fn a_write_that_makes_no_progress_fails_ambiguous_and_the_next_send_dials_fresh() {
        let (first, mut first_peer) = tokio::io::duplex(16);
        let (second, mut second_peer) = tokio::io::duplex(1024);
        let script = ScriptedDial::new(
            false,
            [DialStep::Connect(Box::new(first)), DialStep::Connect(Box::new(second))],
        );
        let (mut probe, telemetry) = sink_telemetry();
        let mut pool = PooledStream::default();
        let frame = [b'x'; 64];

        let err = pool.send(&scripted(&script), &frame, &telemetry).await.unwrap_err();
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous, "{err:#}");
        assert!(err.chain().any(|e| e.is::<Stalled>()), "{err:#}");
        assert!(pool.is_empty(), "the stalled connection isn't pooled");
        assert_one_request(&mut probe, "ambiguous");
        let mut buf = [0u8; 64];
        assert!(matches!(read_once(&mut first_peer, &mut buf).await, Poll::Ready(Ok(16))));
        assert!(
            matches!(read_once(&mut first_peer, &mut buf).await, Poll::Ready(Ok(0))),
            "the stalled connection was closed"
        );

        pool.send(&scripted(&script), &frame, &telemetry).await.expect("a fresh dial delivers");
        let mut got = [0u8; 64];
        second_peer.read_exact(&mut got).await.unwrap();
        assert_eq!(got, frame);
        assert_eq!(script.dials(), 2);
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

    /// A plaintext first `write` that never accepts a byte stalls past `connect_timeout`: nothing
    /// of the frame left, so the attempt is `Clean`, and nothing is pooled.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_plaintext_first_write_is_clean() {
        let script = ScriptedDial::new(false, [DialStep::Connect(Box::new(NeverWrites))]);
        let (mut probe, telemetry) = sink_telemetry();
        let mut pool = PooledStream::default();
        let err = pool.send(&scripted(&script), FRAME, &telemetry).await.unwrap_err();
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean, "{err:#}");
        assert!(err.chain().any(|e| e.is::<Stalled>()), "{err:#}");
        assert!(pool.is_empty());
        assert_eq!(script.dials(), 1, "no retry inside send");
        assert_one_request(&mut probe, "clean");
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
            let mut send = pin!(pool.send(&dial, FRAME, &telemetry));
            assert!(poll_once(&mut send).await.is_pending());
        }
        assert_eq!(script.dials(), 1, "dropped inside the dial");

        pool.send(&scripted(&script), FRAME, &telemetry).await.expect("the next send works");
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
            let mut send = pin!(pool.send(&dial, FRAME, &telemetry));
            assert!(poll_once(&mut send).await.is_pending());
        }
        assert_eq!((closed.state().reads, script.dials()), (1, 1), "probed, then dialing");
        assert!(pool.is_empty(), "the closed connection isn't put back");

        pool.send(&scripted(&script), FRAME, &telemetry).await.expect("the next send works");
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
            sink: "test_out",
            nodelay: false,
        };
        let (mut probe, telemetry) = sink_telemetry();
        let mut pool = PooledStream::default();
        let frame = vec![b'x'; 32 << 20];

        let err = tokio::time::timeout(RECV_TIMEOUT, pool.send(&dial, &frame, &telemetry))
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

            let err = pool.send(&scripted(&script), FRAME, &telemetry).await.expect_err(&what);

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
        pool.send(&scripted(&script), FRAME, &telemetry).await.expect_err("refused");
        assert!(!pool.has_connected_once);
        pool.send(&scripted(&script), FRAME, &telemetry).await.expect("first connect");
        assert_eq!(reconnects(&mut probe), 0.0, "the first successful connect isn't one");
        assert!(pool.has_connected_once);

        // A probe-driven redial is one.
        let closed = FakeStream::new().reading(ReadStep::Eof);
        let script = ScriptedDial::new(false, [connect_to(&FakeStream::new()), DialStep::Refuse]);
        let mut pool = PooledStream::pooled(Box::new(closed));
        pool.send(&scripted(&script), FRAME, &telemetry).await.expect("redial");
        assert_eq!(reconnects(&mut probe), 1.0);

        // A failed redial isn't.
        pool.stream = Some(Box::new(FakeStream::new().reading(ReadStep::Eof)));
        pool.send(&scripted(&script), FRAME, &telemetry).await.expect_err("refused");
        assert_eq!(reconnects(&mut probe), 1.0);
        assert!(pool.has_connected_once);
    }

    // -- Unix streams -----------------------------------------------------------------------------

    fn unix_dial(path: &Path) -> Dial<'_> {
        Dial {
            target: Target::Unix { path },
            connect_timeout: Duration::from_secs(1),
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

        pool.send(&unix_dial(&path), FRAME, &telemetry).await.expect("first");
        let mut first = accept_frame(&listener).await;
        first.shutdown().await.unwrap();
        let pooled = pool.stream_mut().expect("the first send pooled its connection");
        wait_until("the pooled connection to probe Eof", || {
            matches!(probe_now(&mut **pooled), PendingClose::Eof)
        })
        .await;

        pool.send(&unix_dial(&path), FRAME, &telemetry).await.expect("second, on the redial");

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

        pool.send(&unix_dial(&path), FRAME, &telemetry).await.expect("first");
        let first = accept_frame(&listener).await.into_std().unwrap();
        first.shutdown(std::net::Shutdown::Read).unwrap();
        pool.send(&unix_dial(&path), FRAME, &telemetry).await.expect("second, on the retry");
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
        let err = pool.send(&unix_dial(&path), FRAME, &telemetry).await.expect_err("no socket");
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
        assert!(format!("{err:#}").contains("connecting to test_out socket"), "{err:#}");
        assert!(pool.is_empty());
        assert_one_request(&mut probe, "clean");

        // The receiver goes away. Nothing awaits between the close and the next `send`, so the
        // I/O driver hasn't stored the hang-up: the probe answers open, the write fails with
        // `EPIPE`, and the `Clean` comes from the retry's dial finding no socket.
        let listener = UnixListener::bind(&path).unwrap();
        pool.send(&unix_dial(&path), FRAME, &telemetry).await.expect("delivered");
        let conn = accept_frame(&listener).await;
        drop((conn, listener));
        std::fs::remove_file(&path).unwrap();
        let err = pool.send(&unix_dial(&path), FRAME, &telemetry).await.expect_err("gone");
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
        assert!(pool.is_empty());
        assert_eq!(probe.sum("logit.output.requests", &[("class", "clean")]), 2.0);
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
