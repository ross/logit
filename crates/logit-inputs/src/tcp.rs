//! The shared TCP (optionally TLS) listener driver behind `syslog_in`, `graphite_in`, and
//! `statsd_in` under `transport: tcp`: an accept loop, one connection task per peer, framing, and
//! frame-to-batch assembly. The stream twin of [`crate::udp::UdpListener`]
//! (`docs/adr/decoupled-listener-io.md`), generic over the decoder for the same reason: the decoder
//! is the only thing two such listeners differ in. Nothing here is protocol-specific
//! (`docs/adr/syslog-tcp-ingress-and-tls.md`).
//!
//! **Framing is chosen per listener, not guessed per driver.** [`FramingMode`] is set once, at
//! construction, through [`TcpListener::with_framing`]; [`logit_proto::framing`]'s module doc lists
//! the modes and which listener speaks each. A builder rather than a [`TcpListenerConfig`] field:
//! that struct is the image of the `receive:` config block, and framing is not something an
//! operator sets.
//!
//! **A Unix stream socket runs on the same loop.** [`TcpListener::unix`] binds a `SOCK_STREAM`
//! Unix socket (`statsd_in`'s `transport: unix_stream`) through [`crate::unix`] and serves each
//! connection as a plaintext TCP one: the same cap, first-byte and idle deadlines, framing,
//! and batching. Two things don't carry over: TLS ([`TcpListener::with_tls`] refuses it, since a
//! Unix socket is local and plaintext), and the accept-queue gauges ([`AcceptQueueSampler`] reads
//! `TCP_INFO`, which a Unix socket has no counterpart for).
//!
//! **`D: Clone` is load-bearing.** Every connection gets its own decoder clone, because a decoder
//! may hold per-connection state (scratch buffers, or sticky identity the way `collectd`'s
//! decoder holds it per datagram). `SyslogDecoder`'s clonable state is only its `Diagnostics`,
//! whose counts every clone shares. One decoder behind a lock would serialize every connection's
//! decode against every other's.
//!
//! **Cancellation.** Every `select!` and `timeout` here, from the accept loop to `read_step`, is a
//! row of `docs/design/pipeline-graph.md`'s "Cancellation points" table.
//!
//! **No receive queue.** Unlike the UDP driver, there is no [`crate::udp::ReceiveQueue`] here and
//! no `receive.max_datagrams`/`max_bytes`/`overflow` to configure. TCP's own flow control *is* the
//! queue: a connection whose downstream has stalled stops being read, the kernel window closes,
//! and the sender blocks. That is correct for a reliable transport, where dropping bytes to keep
//! reading (the UDP driver's `drop_oldest` default) would corrupt the frame stream rather than
//! lose one self-contained datagram.
//!
//! **Batching is per connection.** Each connection task owns its own
//! [`logit_pipeline::BatchAccumulator`], so `batch_max_events` bounds one connection's in-flight
//! events, not the listener's: N concurrent connections can hold N times that.
//!
//! **Connection limit.** A [`tokio::sync::Semaphore`] with `try_acquire_owned`, capped at the
//! listener's `max_connections:` (1024 by default, [`crate::DEFAULT_MAX_CONNECTIONS`]), as in
//! `logit_in` (`crates/logit-inputs/src/logit.rs`'s "Connection limit" section): reject, don't
//! queue. **The one difference from `logit_in`:**
//! there, a past-the-cap connection is wrapped in TLS first so it can be told why it is being
//! closed (a `Reject` control frame). None of this driver's protocols has an in-band reject
//! message, so there is nothing to spend a handshake saying: a past-the-cap connection is dropped
//! immediately, before any TLS accept, and counted as
//! `logit.input.connections.rejected{reason="limit"}`. The `logit.input.connections` gauge counts
//! permit holders only.
//!
//! **Pre-handshake timeout.** [`HANDSHAKE_TIMEOUT`] (overridden by the operator's
//! `handshake_timeout:` through [`TcpListener::with_handshake_timeout`]) bounds each of a
//! connection's pre-message phases *independently*, as `logit_in` bounds its own two: the PROXY
//! header (when `proxy_protocol:` is on), the TLS accept (in the accept loop's `Some` arm, when TLS
//! is configured), then the wait for the connection's first byte inside [`serve_connection`]. Each
//! starts a fresh budget of the same length rather than inheriting a shared deadline. So with all
//! three the worst case is three of these back to back (15s at the default) before a silent
//! connection gives up its permit.
//!
//! **The first-byte bound applies on both arms, plaintext included.** No `tls:` block is the
//! default shape, and without the bound a cap's worth of connections that complete the TCP
//! handshake and then send nothing would hold every permit forever, at a cost to the peer of one
//! SYN each and no bytes.
//! The bound covers only the *first* byte (until [`Framer::first_byte_seen`] is true), the phase
//! with no legitimate reason to be slow. The opt-in idle timeout bounds the gaps after it.
//!
//! The predicate is `first_byte_seen`, not "has the framer latched a
//! [`Framing`](logit_proto::framing::Framing)": only [`FramingMode::Rfc6587Auto`] has anything to
//! latch, so under either explicit mode a latch-shaped predicate would read "already framed" on a
//! connection that has not sent a byte, and the deadline would never fire.
//! `the_first_byte_deadline_applies_under_every_framing_mode` pins it.
//!
//! **Peer address.** With [`TcpListener::with_peer`] on, the accept loop builds one [`PeerAttrs`]
//! from the address `accept()` returned, and [`absorb_frame`] stamps it on the events each
//! `decode_into` call appended, before they reach the accumulator. A Unix peer that bound no path
//! gets nothing. Off, the accepted address is dropped unread.
//!
//! **PROXY protocol.** With [`TcpListener::with_proxy_protocol`] on, every TCP connection must
//! open with a PROXY protocol header (ADR `listener-peer-address`). The connection task reads it
//! off the raw stream through [`read_proxy_origin`], before any TLS accept, under its own
//! `handshake_timeout` budget; that function's doc says how the payload behind the header stays
//! in the socket for the TLS handshake or the [`Framer`]. A missing, malformed, or slow header, or
//! a peer that closes before finishing one, closes the connection with a throttled `proxy_header`
//! diagnostic and counts `logit.input.connections.rejected{reason="proxy_header"}`. An origin is
//! stamped as `client.*` beside any `network.peer.*`, both held in one [`ConnectionAttrs`] built
//! per connection; a header with no origin (`LOCAL`, `UNKNOWN`, `AF_UNSPEC`) stamps nothing. A
//! Unix socket refuses the option at bind.
//!
//! **Reset before the first byte.** A connection reset (`ECONNRESET`) before its first payload
//! byte ends quietly: no `connection_error`, and nothing counted, since no data was lost. A load
//! balancer ends a health check this way: HAProxy's PROXY-aware check sends a `LOCAL` header and
//! then an RST, which reaches [`serve_connection`] while it waits for the first payload byte. A
//! reset after the first byte is a broken connection, diagnosed as `connection_error`.
//!
//! **Idle timeout.** [`TcpListener::with_idle_timeout`] (the operator's `idle_timeout:`) is off
//! unless set, and when set bounds how long a connection may stay quiet before this listener
//! closes it and hands its permit back (`docs/adr/idle-connection-timeout.md`). It shares one
//! next-byte deadline with the first-byte bound: whichever phase the connection is in supplies the
//! deadline, so there is only ever one clock on the read.
//!
//! *What resets it.* The deadline is `last_progress + idle_timeout`, and `last_progress` advances
//! on two things only: bytes read from the peer (stamped after the inner frame loop drains, which
//! also covers an [`absorb_frame`] emit returning), and this connection's own interval flush
//! emitting a batch. A flush tick with nothing to emit does neither, so this process's own timer
//! never re-arms the clock.
//!
//! *Why time blocked downstream never counts.* [`emit`] awaits `Fanout::send`, which awaits a
//! bounded channel's capacity; a connection parked there is waiting on us, not idle. Because
//! `last_progress` is stamped when that await *returns* and the deadline is consulted only while
//! this task is in the read, a full downstream can never make a busy connection look quiet.
//!
//! *Why `Ok(())`.* An idle close is policy, not a fault: it returns `Ok(())`, so it never reaches
//! the accept loop's `connection_error` diagnostic, and is counted
//! `logit.input.connections.closed{reason="idle"}` instead. On the way out, complete accumulated
//! events are flushed [`FlushReason::Closed`] and a buffered *partial* frame is reported through
//! [`report_buffered_tail`], as the shutdown and RST paths do.

use crate::peer::{read_proxy_origin, ConnectionAttrs, PeerAttrs};
use crate::Input;
use bytes::{Bytes, BytesMut};
use logit_core::{Diagnostics, Event, EventBatch, Telemetry};
use logit_pipeline::listen::BindOptions;
use logit_pipeline::sockstat;
use logit_pipeline::{BatchAccumulator, Fanout, FlushReason};
use logit_proto::framing::{FrameError, Framer, FramingMode, MAX_FRAME_BYTES, READ_BUFFER_BYTES};
use logit_proto::Decoder;
use std::future::Future;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::net::TcpListener as TokioTcpListener;
use tokio::net::UnixListener as TokioUnixListener;
use tokio::sync::{watch, OwnedSemaphorePermit};
use tokio_rustls::TlsAcceptor;

/// `logit_pipeline::tls::TlsServerSettings`, re-exported for symmetry with `crate::logit`/`crate::otlp`.
pub use logit_pipeline::tls::TlsServerSettings;

/// How long a connection has, per pre-message phase, before this listener releases its
/// connection-limit permit: the PROXY header under `proxy_protocol:`, the TLS accept when TLS is
/// configured, and on both arms the wait for the first byte. Each phase gets its own budget, so a
/// silent connection with both options on costs three. See this module's "Pre-handshake timeout"
/// doc section.
///
/// The default only: the `handshake_timeout:` field on `syslog_in`/`graphite_in`/`statsd_in`
/// overrides it through [`TcpListener::with_handshake_timeout`]. `logit_config`'s
/// `default_handshake_timeout` mirrors this number by hand (it cannot depend on this crate).
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

// ---- the kernel's accept queue -----------------------------------------------------------------

/// How often [`AcceptQueueSampler::accept`] re-reads the accept queue while waiting for a
/// connection. The same one-second cadence [`crate::udp`]'s receive-buffer sampler uses: frequent
/// enough to be a usable gauge, cheap enough not to need a config knob.
///
/// The cost is one `getsockopt` and three gauge writes per tick **plus one per accepted
/// connection**, since the sample runs at the top of every loop turn and the loop turns on every
/// accept as well as every tick. That is small next to the connection setup it accompanies.
const ACCEPT_QUEUE_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// Gauges the kernel's accept queue for one listening socket: how many completed connections are
/// waiting for an `accept()`, against the backlog ceiling at which the kernel starts refusing them.
///
/// **Why an accept loop cannot see this for itself.** Nothing observable from inside the loop
/// distinguishes "no traffic" from "so far behind that the kernel is dropping SYNs." Only the
/// queue depth does, and it lives in the kernel ([`logit_pipeline::sockstat::listen_queue`]).
///
/// **Sampled before each accept *and* on a fixed interval.** Before each accept, because the depth
/// just before this loop takes one off the queue is what a backlog is made of. On an interval as
/// well, because the accept-time sample fires only when a connection is *taken*, which is what
/// stops happening when the loop is starved of runtime or stuck. Every caller gets both by calling
/// [`Self::accept`] instead of `listener.accept()`.
///
/// Like `crate::udp`'s receive-buffer sampler, this disables itself for good after one failed read
/// and says so once: `TCP_INFO`'s listener aliasing either works on a socket or never will.
pub(crate) struct AcceptQueueSampler {
    /// How the accept queue is read, as a function of the listener [`Self::accept`] was handed.
    ///
    /// **Not a descriptor captured at construction.** A stored `fd` would make the socket
    /// *gauged* and the socket *accepted on* independent: `sampler.accept(&other_listener)` would
    /// compile and gauge one socket while draining another, indistinguishably from correct
    /// output. Taking the descriptor from the `listener` argument at each sample makes them the
    /// same socket by construction. A `BorrowedFd<'_>` field would fix the lifetime but not the
    /// identity, since two listeners can both outlive a sampler.
    ///
    /// A function pointer so a test can substitute a reader that reports nothing or counts its
    /// calls: the disabled path is what every non-Linux build runs and no Linux CI run would
    /// otherwise exercise, and the call count is the only cheap observable for the cadence
    /// [`Self::accept_every`] keeps.
    read_queue: QueueReader,
    telemetry: Telemetry,
    diag: Diagnostics,
    enabled: bool,
    /// The one timer this sampler arms, kept across loop turns *and* across calls.
    ///
    /// `None` until the first enabled [`Self::accept`], because a disabled sampler must arm no
    /// timer, and dropped again when the sampler disables itself. Boxed and pinned so it can live
    /// in a struct and still be polled as a `Pin<&mut Sleep>`.
    ///
    /// **Why one `Sleep` rather than a fresh `sleep(interval)` per turn.** (1) Cost: in tokio
    /// 1.53.1 a `Sleep` registers its `TimerEntry` lazily on first poll (`Sleep::poll_elapsed` ->
    /// `TimerEntry::init` -> `reregister`, which takes the timer driver lock) and cancels it on
    /// drop (`PinnedDrop for TimerEntry` -> `cancel` -> `clear_entry`, which takes that lock
    /// again unconditionally; the `might_be_registered()` check inside only gates the wheel
    /// removal). With `biased;` polling the timer arm first, a fresh `Sleep` per turn pays both
    /// per accepted connection on every stream listener in the process; re-polling a registered
    /// `Sleep` is one `Acquire` load (`StateCell::read_state`). (2) Correctness: a fresh
    /// `sleep(interval)` re-anchors to *now* every turn, so under a steady accept rate faster than
    /// one per interval the tick never fires, starving the sample that exists so a busy listener
    /// still reports. One `Sleep` reset only when it fires keeps the cadence.
    tick: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
}

/// [`AcceptQueueSampler::read_queue`]'s type.
type QueueReader = fn(&TokioTcpListener) -> Result<(u32, u32), sockstat::Unavailable>;

/// The production [`QueueReader`]: this listener's descriptor, into
/// [`logit_pipeline::sockstat::listen_queue`].
fn read_listen_queue(listener: &TokioTcpListener) -> Result<(u32, u32), sockstat::Unavailable> {
    sockstat::fd_of(listener)
        .ok_or(sockstat::Unavailable::NoDescriptor)
        .and_then(sockstat::listen_queue)
}

impl AcceptQueueSampler {
    /// Takes no listener: the socket this gauges is whichever one is handed to [`Self::accept`]
    /// (see [`Self::read_queue`]).
    pub(crate) fn new(telemetry: Telemetry, diag: Diagnostics) -> Self {
        Self { read_queue: read_listen_queue, telemetry, diag, enabled: true, tick: None }
    }

    /// Accepts the next connection on `listener`, gauging the accept queue before the attempt and
    /// once per [`ACCEPT_QUEUE_SAMPLE_INTERVAL`] for as long as the wait lasts.
    pub(crate) async fn accept(
        &mut self,
        listener: &TokioTcpListener,
    ) -> std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)> {
        self.accept_every(listener, ACCEPT_QUEUE_SAMPLE_INTERVAL).await
    }

    /// [`Self::accept`] over an arbitrary interval, split out (as [`crate::udp::sample_while`] is)
    /// so a test can drive the interval tick without sleeping a second.
    ///
    /// **Once the sampler has disabled itself, no timer is armed** and this is `listener.accept()`.
    /// Otherwise every idle listener on a non-Linux build (or a kernel without the counters)
    /// would wake once a second, forever, to call a function that returns immediately.
    ///
    /// **Cancellation-safe**, which this driver's and `logit_in`'s accept loops depend on:
    /// `TcpListener::accept` takes nothing off the queue unless it returns a connection, so
    /// dropping this future when a caller's `select!` loses it to `shutdown` loses at most one
    /// sample, never a connection. The timer is a field of the sampler rather than of this future,
    /// so a cancelled `accept` does not restart the interval either.
    async fn accept_every(
        &mut self,
        listener: &TokioTcpListener,
        interval: Duration,
    ) -> std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)> {
        loop {
            self.sample_once(listener);
            if !self.enabled {
                // Latched off mid-run: give the timer entry back rather than leave it in the
                // wheel for the life of the listener.
                self.tick = None;
                return listener.accept().await;
            }
            // Timer arm first, matching `crate::udp::sample_while`: see
            // `docs/design/pipeline-graph.md`'s "Cancellation points".
            let tick = self.tick.get_or_insert_with(|| Box::pin(tokio::time::sleep(interval)));
            tokio::select! {
                biased;
                () = tick.as_mut() => {
                    // Re-armed from *now* rather than from the old deadline: the cadence promised
                    // is a sample at least every `interval` while waiting, not a fixed schedule to
                    // catch up to after a stall.
                    let next = tokio::time::Instant::now() + interval;
                    tick.as_mut().reset(next);
                }
                accepted = listener.accept() => return accepted,
            }
        }
    }

    /// One `getsockopt`, and the three gauges it feeds.
    ///
    /// `logit.input.accept_queue.limit` is re-emitted every sample although the backlog does not
    /// change after `listen(2)`, as `crate::udp`'s sampler re-emits `receive_buffer.bytes`:
    /// `ComponentBuffer::drain` (`logit_core::telemetry`) `mem::take`s its point map, so a value
    /// written once would appear in one `internal` drain window and vanish. It is its own gauge
    /// because an operator deciding whether to raise `net.core.somaxconn` (or the backlog) needs
    /// the ceiling, and backing it out of `depth / utilization` is undefined at an idle listener's
    /// depth of 0.
    ///
    /// `logit.input.accept_queue.utilization` is skipped when the kernel reports a backlog of 0, so
    /// the gauge's presence is evidence that a ceiling was read. It is **not** clamped to 1.0 and
    /// must not be: `sk_acceptq_is_full` is strictly greater-than, so a `listen(N)` socket settles
    /// at a depth of `N + 1` and a utilization of `(N + 1) / N` when the kernel starts refusing.
    /// `sockstat::listen_queue`'s doc has the kernel citation.
    fn sample_once(&mut self, listener: &TokioTcpListener) {
        if !self.enabled {
            return;
        }
        let (depth, backlog) = match (self.read_queue)(listener) {
            Ok(queue) => queue,
            Err(err) => {
                self.enabled = false;
                // The platform hint only where it holds: `EBADF` or a listener that has left
                // `LISTEN` are about this socket, not about Linux.
                let hint = if err.is_unsupported_option() {
                    " (TCP_INFO's listener fields are Linux-only)"
                } else {
                    ""
                };
                let message = format_args!(
                    "the kernel's accept-queue depth is not available for this listener: \
                     {err}{hint}; logit.input.accept_queue.depth, .limit and .utilization will \
                     not be reported"
                );
                // See `crate::udp::ReceiveBufferSampler::sample_once` for why a non-Linux build
                // gets `debug` and everything else gets `warn`.
                if matches!(
                    err,
                    sockstat::Unavailable::NotLinux | sockstat::Unavailable::NoDescriptor
                ) {
                    self.diag.debug(message);
                } else {
                    self.diag.warn(message);
                }
                return;
            }
        };
        self.telemetry.gauge("logit.input.accept_queue.depth", f64::from(depth), &[]);
        self.telemetry.gauge("logit.input.accept_queue.limit", f64::from(backlog), &[]);
        if backlog > 0 {
            self.telemetry.gauge(
                "logit.input.accept_queue.utilization",
                f64::from(depth) / f64::from(backlog),
                &[],
            );
        }
    }
}

// ---- the listener ----------------------------------------------------------------------------

/// [`TcpListener`]'s runtime knobs: [`crate::udp::UdpListenerConfig`] minus every queue field
/// (this module's "No receive queue" doc section), with the same defaults for the other three.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TcpListenerConfig {
    /// Events to accumulate **per connection** before one `Fanout::send`; `1` means one send per
    /// frame. The listener's worst-case in-flight event count is this times the number of live
    /// connections (this module's "Batching is per connection" doc section).
    pub batch_max_events: usize,
    /// The same bound by estimated heap bytes, also **per connection**.
    pub batch_max_bytes: u64,
    /// `Duration::ZERO` disables the flush timer; the bounds are then the only trigger.
    pub batch_flush_interval: Duration,
}

/// The same numbers as [`crate::udp::UdpListenerConfig::default`]'s corresponding fields
/// (`docs/adr/decoupled-listener-io.md`): a TCP listener has no reason to batch differently.
impl Default for TcpListenerConfig {
    fn default() -> Self {
        Self {
            batch_max_events: 1_000,
            batch_max_bytes: 1024 * 1024,
            batch_flush_interval: Duration::from_millis(100),
        }
    }
}

/// A TCP (optionally TLS) listener that turns each connection's frame stream into batches of
/// decoded events. This module's doc has the accept, cap, handshake, framing, and batching
/// contracts.
pub struct TcpListener<D: Decoder + Clone + Send + 'static> {
    target: StreamTarget,
    decoder: D,
    config: TcpListenerConfig,
    diag: Diagnostics,
    telemetry: Telemetry,
    /// How every connection's messages are delimited, and (below) the bound on one of them:
    /// [`FramingMode::Rfc6587Auto`] and [`MAX_FRAME_BYTES`] unless [`Self::with_framing`] is
    /// called.
    framing: FramingMode,
    max_frame_bytes: usize,
    tls: Option<Arc<rustls::ServerConfig>>,
    /// Set by [`Input::bind`], taken by [`Input::run_until_shutdown`]: the bind pre-pass `otlp_in`
    /// and `logit_in` also use, so `logit run` fails startup on a bind error before anything is
    /// spawned and a test can learn the real address.
    listener: Option<BoundListener>,
    /// See this module's "Connection limit" doc section.
    max_connections: usize,
    handshake_timeout: Duration,
    /// `None` (the default) means no idle timeout. See this module's "Idle timeout" doc section.
    idle_timeout: Option<Duration>,
    /// Whether to stamp each connection's events with its peer (`peer:`), per [`PeerAttrs`].
    peer: bool,
    /// Whether every connection opens with a PROXY protocol header (`proxy_protocol:`). See this
    /// module's "PROXY protocol" doc section.
    proxy_protocol: bool,
    /// Socket options set before the bind (`reuse_port:`).
    bind_options: BindOptions,
}

/// Where a [`TcpListener`] binds: a TCP `host:port`, or a Unix stream socket path.
enum StreamTarget {
    Tcp(String),
    /// `kind` names the component in a bind error; `mode` is the socket file's mode after bind.
    Unix {
        path: PathBuf,
        mode: u32,
        kind: &'static str,
    },
}

/// The bound listening socket, per [`StreamTarget`].
enum BoundListener {
    Tcp(TokioTcpListener),
    Unix(TokioUnixListener),
}

impl<D: Decoder + Clone + Send + 'static> TcpListener<D> {
    pub fn new(bind: impl Into<String>, decoder: D, config: TcpListenerConfig) -> Self {
        Self::with_target(StreamTarget::Tcp(bind.into()), decoder, config)
    }

    /// A listener on a Unix stream socket at `path`, made mode `mode` once bound. `kind` names the
    /// component in a bind error. This module's "A Unix stream socket runs on the same loop" says
    /// what differs from TCP.
    pub fn unix(
        kind: &'static str,
        path: impl Into<PathBuf>,
        mode: u32,
        decoder: D,
        config: TcpListenerConfig,
    ) -> Self {
        Self::with_target(StreamTarget::Unix { path: path.into(), mode, kind }, decoder, config)
    }

    fn with_target(target: StreamTarget, decoder: D, config: TcpListenerConfig) -> Self {
        Self {
            target,
            decoder,
            config,
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
            framing: FramingMode::Rfc6587Auto,
            max_frame_bytes: MAX_FRAME_BYTES,
            tls: None,
            listener: None,
            max_connections: crate::DEFAULT_MAX_CONNECTIONS,
            handshake_timeout: HANDSHAKE_TIMEOUT,
            idle_timeout: None,
            peer: false,
            proxy_protocol: false,
            bind_options: BindOptions::default(),
        }
    }

    /// The address bound, once [`Input::bind`] has run; lets a test learn the OS-assigned port
    /// without a bind-drop-rebind race. `None` on a Unix socket; see [`Self::socket_path`].
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        match self.listener.as_ref()? {
            BoundListener::Tcp(listener) => listener.local_addr().ok(),
            BoundListener::Unix(_) => None,
        }
    }

    /// Replaces a Unix stream listener's socket file mode before [`Input::bind`]; a TCP listener
    /// is left untouched.
    pub fn with_socket_mode(mut self, socket_mode: u32) -> Self {
        if let StreamTarget::Unix { mode, .. } = &mut self.target {
            *mode = socket_mode;
        }
        self
    }

    /// The configured socket path of a Unix stream listener, bound or not; `None` for TCP.
    pub fn socket_path(&self) -> Option<&std::path::Path> {
        match &self.target {
            StreamTarget::Unix { path, .. } => Some(path),
            StreamTarget::Tcp(_) => None,
        }
    }

    /// Sets *this listener's own* diagnostics: the `connection_error`, `bad_frame` and
    /// `framing_error` keys. Does **not** reach the decoder's diagnostics
    /// ([`crate::udp::UdpListener::with_diagnostics`] explains); use [`Self::map_decoder`] for
    /// that.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Applies `f` to the wrapped decoder, so a caller that knows the concrete type can chain its
    /// consuming builder methods, as [`crate::udp::UdpListener::map_decoder`] does.
    pub fn map_decoder(mut self, f: impl FnOnce(D) -> D) -> Self {
        self.decoder = f(self.decoder);
        self
    }

    /// Overrides the batching/shutdown-grace knobs, which a `receive:` config block sets.
    pub fn with_config(mut self, config: TcpListenerConfig) -> Self {
        self.config = config;
        self
    }

    /// The configured batching/shutdown-grace knobs, for test introspection.
    pub fn config(&self) -> TcpListenerConfig {
        self.config
    }

    /// Turns on TLS termination (`tls:` in config), with no ALPN: as with `logit_in`, the
    /// protocol isn't HTTP-shaped, so there's nothing to negotiate. Every path in `settings`
    /// resolves against `base_dir` (the config file's directory). Fails on a Unix socket, which
    /// is always plaintext.
    ///
    /// Registers the files with `reloader` under this listener's diagnostics and telemetry as
    /// they are when this runs, so call it after `with_diagnostics` and `with_telemetry`.
    pub fn with_tls(
        mut self,
        settings: &TlsServerSettings,
        base_dir: &Path,
        reloader: &logit_pipeline::tls::TlsReloader,
    ) -> anyhow::Result<Self> {
        if let StreamTarget::Unix { kind, .. } = &self.target {
            anyhow::bail!(
                "{kind}: 'tls:' needs 'transport: tcp' -- a Unix socket is always plaintext"
            );
        }
        self.tls = Some(Arc::new(logit_pipeline::tls::build_server_config(
            settings,
            base_dir,
            &[],
            reloader,
            &self.diag,
            &self.telemetry,
        )?));
        Ok(self)
    }

    /// This listener's own diagnostics, test-only: a wrapper's `with_diagnostics` has to set both
    /// this and `Self::decoder`'s, and only an accessor on each can prove it did
    /// (`crate::syslog`'s regression test).
    #[cfg(test)]
    pub(crate) fn diag(&self) -> &Diagnostics {
        &self.diag
    }

    /// The wrapped decoder, test-only; the counterpart of `Self::diag`.
    #[cfg(test)]
    pub(crate) fn decoder(&self) -> &D {
        &self.decoder
    }

    /// How this listener's connections are framed, and the largest single frame any of them will
    /// assemble. [`FramingMode::Rfc6587Auto`] with [`MAX_FRAME_BYTES`] (what `syslog_in` wants)
    /// when never called.
    ///
    /// A builder rather than a [`TcpListenerConfig`] field: that struct is the image of the
    /// operator's `receive:` block, and framing is a property of the protocol. See
    /// [`FramingMode`] for why it is explicit rather than sniffed.
    pub fn with_framing(mut self, mode: FramingMode, max_frame_bytes: usize) -> Self {
        self.set_framing(mode, max_frame_bytes);
        self
    }

    /// [`Self::with_framing`] on an already-built listener, for a wrapper that defers the decision
    /// until `bind()`: `graphite_in` applies `max_line_bytes`/`max_frame_bytes` there so its own
    /// builder methods can be called in any order.
    pub(crate) fn set_framing(&mut self, mode: FramingMode, max_frame_bytes: usize) {
        self.framing = mode;
        self.max_frame_bytes = max_frame_bytes;
    }

    /// Overrides [`crate::DEFAULT_MAX_CONNECTIONS`]: the `max_connections:` field of
    /// `syslog_in`/`graphite_in`/`statsd_in`/`lines_in`, through each wrapper's
    /// `with_max_connections`. Graph rule 74 rejects `0` before it gets here.
    pub(crate) fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Overrides [`HANDSHAKE_TIMEOUT`] for every pre-message budget (the PROXY header under
    /// `proxy_protocol:`, the TLS accept when `tls:` is set, then the wait for the first byte):
    /// the `handshake_timeout:` field of `syslog_in`/`graphite_in`/`statsd_in`/`lines_in`,
    /// through each wrapper's `with_handshake_timeout`. Graph rule 45 rejects `0s` before it can
    /// reach here.
    pub fn with_handshake_timeout(mut self, handshake_timeout: Duration) -> Self {
        self.handshake_timeout = handshake_timeout;
        self
    }

    /// Bounds how long a connection may stay quiet once past the first-byte phase: the
    /// `idle_timeout:` field of `syslog_in`/`graphite_in`/`statsd_in`. `None` (the default) means
    /// no idle timeout. See this module's "Idle timeout" doc section. Graph rule 53 rejects
    /// `Some(0s)` (and any value on a UDP listener) before it can reach here.
    ///
    /// Takes the `Option` so every wrapper and `logit-cli`'s `build_spec` can pass the config
    /// value straight through.
    pub fn with_idle_timeout(mut self, idle_timeout: Option<Duration>) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    /// Stamps every event a connection decodes with that connection's peer (the `peer:` field of
    /// `syslog_in`/`graphite_in`/`statsd_in`/`lines_in`), per [`PeerAttrs`]. Off by default.
    pub fn with_peer(mut self, peer: bool) -> Self {
        self.peer = peer;
        self
    }

    /// Requires a PROXY protocol header ahead of every connection's stream and stamps the origin
    /// it names (the `proxy_protocol:` field of `syslog_in`/`graphite_in`/`statsd_in`/`lines_in`).
    /// Off by default. See this module's "PROXY protocol" doc section. [`Input::bind`] refuses it
    /// on a Unix socket; graph rule 79 is what an operator sees.
    pub fn with_proxy_protocol(mut self, proxy_protocol: bool) -> Self {
        self.proxy_protocol = proxy_protocol;
        self
    }

    /// Sets `SO_REUSEPORT` before the bind (the `reuse_port:` field), so another process can bind
    /// the same address at the same time. Off by default. [`Input::bind`] refuses it on a Unix
    /// socket.
    pub fn with_reuse_port(mut self, reuse_port: bool) -> Self {
        self.bind_options.reuse_port = reuse_port;
        self
    }
}

#[async_trait::async_trait]
impl<D: Decoder + Clone + Send + 'static> Input for TcpListener<D> {
    async fn bind(&mut self) -> anyhow::Result<()> {
        if self.listener.is_some() {
            return Ok(()); // idempotent, per `Input::bind`'s contract
        }
        let listener = match &self.target {
            StreamTarget::Tcp(bind) => {
                let listener = logit_pipeline::listen::bind_tcp(bind, self.bind_options).await?;
                self.diag.info("bound", format_args!("listening on {bind}"));
                BoundListener::Tcp(listener)
            }
            StreamTarget::Unix { kind, .. } if self.proxy_protocol => {
                anyhow::bail!(
                    "{kind}: 'proxy_protocol:' needs 'transport: tcp' -- the header comes from a \
                     network proxy"
                );
            }
            StreamTarget::Unix { kind, .. } if self.bind_options.reuse_port => {
                anyhow::bail!(
                    "{kind}: 'reuse_port:' needs 'transport: tcp' -- a Unix socket has no port to \
                     share"
                );
            }
            StreamTarget::Unix { path, mode, kind } => {
                let listener = crate::unix::bind_listener(kind, path, *mode)?;
                self.diag.info("bound", format_args!("listening on {}", path.display()));
                BoundListener::Unix(listener)
            }
        };
        self.listener = Some(listener);
        Ok(())
    }

    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        // Never exercised in production: `run_input` always calls `run_until_shutdown`. The trait
        // requires it.
        let (_tx, rx) = watch::channel(false);
        self.run_until_shutdown(sink, rx).await
    }

    async fn run_until_shutdown(
        &mut self,
        sink: Fanout,
        mut shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        self.bind().await?;
        let listener = self.listener.take().expect("bind() leaves a listener behind");
        let connection_limit = Arc::new(tokio::sync::Semaphore::new(self.max_connections));
        let spawner = ConnectionSpawner {
            // Built once: `TlsAcceptor::from` wraps the `Arc<ServerConfig>`, so cloning it per
            // connection is an `Arc` clone, not a config rebuild.
            tls_acceptor: self.tls.clone().map(TlsAcceptor::from),
            live_connections: crate::listener::LiveConnections::new(self.telemetry.clone()),
            handshake_timeout: self.handshake_timeout,
            idle_timeout: self.idle_timeout,
            config: self.config,
            framing: self.framing,
            max_frame_bytes: self.max_frame_bytes,
            sink,
            // All three diagnostic keys (`connection_error`, `framing_error`, `bad_frame`)
            // throttle listener-wide through the per-connection `Diagnostics` clone
            // `ConnectionSpawner::spawn` takes: a clone shares its original's counts
            // (`logit_core::Diagnostics`' type doc).
            diag: self.diag.clone(),
            telemetry: self.telemetry.clone(),
            shutdown: shutdown.clone(),
            decoder: self.decoder.clone(),
        };
        // A Unix listener has no accept-queue gauges (this module's "A Unix stream socket runs on
        // the same loop"). Both accepts race `shutdown`: see
        // `docs/design/pipeline-graph.md`'s "Cancellation points".
        let mut accept_queue = AcceptQueueSampler::new(self.telemetry.clone(), self.diag.clone());
        let mut accept_diag = self.diag.clone();
        loop {
            let accepted = match &listener {
                BoundListener::Tcp(listener) => tokio::select! {
                    accepted = accept_queue.accept(listener) => accepted.map(|(s, a)| Accepted::Tcp(s, a)),
                    _ = shutdown.wait_for(|&due| due) => return Ok(()),
                },
                BoundListener::Unix(listener) => tokio::select! {
                    accepted = listener.accept() => accepted.map(|(s, a)| Accepted::Unix(s, a)),
                    _ = shutdown.wait_for(|&due| due) => return Ok(()),
                },
            };
            let accepted = match accepted {
                Ok(accepted) => accepted,
                Err(err) => {
                    // `biased`, absorb first: see `docs/design/pipeline-graph.md`'s
                    // "Cancellation points".
                    tokio::select! {
                        biased;
                        absorbed = crate::listener::absorb_accept_error(
                            err,
                            &self.telemetry,
                            &mut accept_diag,
                        ) => absorbed?,
                        _ = shutdown.wait_for(|&due| due) => return Ok(()),
                    }
                    continue;
                }
            };

            // `try_acquire_owned`, not `acquire_owned`: at capacity the connection is closed
            // immediately rather than queued behind a permit that may never come, and before any
            // TLS accept (this module's "Connection limit" doc section says why that differs from
            // `logit_in`).
            let Ok(permit) = connection_limit.clone().try_acquire_owned() else {
                self.telemetry.count(
                    "logit.input.connections.rejected",
                    1.0,
                    &[("reason", "limit")],
                );
                drop(accepted);
                continue;
            };

            // Formatted once per connection, and only when asked for: with `peer: false` the
            // accepted address is dropped unread.
            match accepted {
                Accepted::Tcp(stream, addr) => {
                    let peer = self.peer.then(|| PeerAttrs::from_socket(addr));
                    if self.proxy_protocol {
                        spawner.spawn_proxied(stream, permit, peer);
                    } else {
                        spawner.spawn(stream, permit, peer);
                    }
                }
                Accepted::Unix(stream, addr) => {
                    let peer = if self.peer { PeerAttrs::from_unix(&addr) } else { None };
                    spawner.spawn(stream, permit, peer);
                }
            }
        }
    }
}

/// One accepted connection, per [`BoundListener`].
enum Accepted {
    Tcp(tokio::net::TcpStream, std::net::SocketAddr),
    Unix(tokio::net::UnixStream, tokio::net::unix::SocketAddr),
}

/// Everything a connection task needs from its listener, cloned per connection by
/// [`Self::spawn`]. One value so the TCP and Unix arms of the accept loop spawn identically.
struct ConnectionSpawner<D> {
    tls_acceptor: Option<TlsAcceptor>,
    live_connections: crate::listener::LiveConnections,
    handshake_timeout: Duration,
    idle_timeout: Option<Duration>,
    config: TcpListenerConfig,
    framing: FramingMode,
    max_frame_bytes: usize,
    sink: Fanout,
    diag: Diagnostics,
    telemetry: Telemetry,
    shutdown: watch::Receiver<bool>,
    decoder: D,
}

impl<D: Decoder + Clone + Send + 'static> ConnectionSpawner<D> {
    /// Spawns the task serving `stream`, which holds `permit` for as long as it runs. `peer`, when
    /// present, is stamped on every event the connection decodes.
    fn spawn<S>(&self, stream: S, permit: OwnedSemaphorePermit, peer: Option<PeerAttrs>)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        self.spawn_after(permit, async move { Ok((stream, ConnectionAttrs::new(peer, None))) });
    }

    /// [`Self::spawn`] for a `proxy_protocol: true` listener: the task reads the PROXY header off
    /// the raw stream first, under `handshake_timeout`, and stamps the origin it names beside
    /// `peer`. A missing, malformed, or slow header closes the connection, counted as
    /// `logit.input.connections.rejected{reason="proxy_header"}` (this module's "PROXY protocol"
    /// doc section).
    fn spawn_proxied(
        &self,
        mut stream: tokio::net::TcpStream,
        permit: OwnedSemaphorePermit,
        peer: Option<PeerAttrs>,
    ) {
        let handshake_timeout = self.handshake_timeout;
        self.spawn_after(permit, async move {
            let origin = read_proxy_origin(&mut stream, handshake_timeout).await?;
            let client = PeerAttrs::client(&origin);
            Ok((stream, ConnectionAttrs::new(peer, client)))
        });
    }

    /// Spawns the task that awaits `prelude` and then serves the stream it yields with the
    /// attributes it built. A `prelude` error is a refused PROXY header, the only fallible one.
    fn spawn_after<S, P>(&self, permit: OwnedSemaphorePermit, prelude: P)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        P: Future<Output = anyhow::Result<(S, Option<ConnectionAttrs>)>> + Send + 'static,
    {
        // Every connection task holds a `Fanout` clone, so the shutdown cascade
        // (`docs/adr/service-lifecycle-and-output-retry.md`) completes only once every one has
        // dropped, which `serve_connection`'s shutdown race guarantees.
        let sink = self.sink.clone();
        let mut diag = self.diag.clone();
        let telemetry = self.telemetry.clone();
        let tls_acceptor = self.tls_acceptor.clone();
        let conn_shutdown = self.shutdown.clone();
        let live_connections = self.live_connections.clone();
        let decoder = self.decoder.clone();
        let handshake_timeout = self.handshake_timeout;
        let idle_timeout = self.idle_timeout;
        let config = self.config;
        let framer = Framer::new(self.framing, self.max_frame_bytes);

        tokio::spawn(async move {
            // Held for as long as this task runs: a refused PROXY header or a TLS accept that
            // fails or times out gives the permit back here.
            let _permit = permit;
            // Counted out on drop, so a panic in the connection brings the gauge back down too.
            let live = live_connections.enter();

            let (stream, attrs) = match prelude.await {
                Ok(prepared) => prepared,
                Err(err) => {
                    drop(live);
                    telemetry.count(
                        "logit.input.connections.rejected",
                        1.0,
                        &[("reason", "proxy_header")],
                    );
                    diag.warn_throttled("proxy_header", err);
                    return;
                }
            };

            let result = match tls_acceptor {
                Some(acceptor) => {
                    match tokio::time::timeout(handshake_timeout, acceptor.accept(stream)).await {
                        Ok(Ok(tls_stream)) => {
                            serve_connection(
                                tls_stream,
                                decoder,
                                framer,
                                config,
                                handshake_timeout,
                                idle_timeout,
                                attrs,
                                sink,
                                telemetry.clone(),
                                &mut diag,
                                conn_shutdown,
                            )
                            .await
                        }
                        Ok(Err(err)) => Err(anyhow::anyhow!("TLS handshake failed: {err}")),
                        Err(_elapsed) => Err(anyhow::anyhow!(
                            "TLS handshake did not complete within {handshake_timeout:?}"
                        )),
                    }
                }
                // No TLS to bound, but `serve_connection`'s first-byte deadline still applies
                // (this module's "Pre-handshake timeout" doc section).
                None => {
                    serve_connection(
                        stream,
                        decoder,
                        framer,
                        config,
                        handshake_timeout,
                        idle_timeout,
                        attrs,
                        sink,
                        telemetry.clone(),
                        &mut diag,
                        conn_shutdown,
                    )
                    .await
                }
            };

            drop(live);

            // One connection's error (a peer vanishing mid-frame, a TLS accept that failed or
            // timed out) is never fatal to the listener or its siblings; only an accept failing
            // in the accept loop is.
            if let Err(err) = result {
                diag.warn_throttled("connection_error", err);
            }
        });
    }
}

/// What one `read`-versus-`shutdown` race produced.
enum ReadStep {
    /// Bytes landed in the read buffer.
    Bytes,
    /// The peer closed cleanly.
    Eof,
    /// Shutdown fired before anything else did.
    Shutdown,
    Failed(std::io::Error),
}

/// One read step, raced against `shutdown`.
///
/// Unbiased: see `docs/design/pipeline-graph.md`'s "Cancellation points".
///
/// `shutdown.changed()` needs the caller's `*shutdown.borrow()` check for a shutdown that fired
/// before the iteration. `wait_for` would compile here, since these arms don't await; `changed()`
/// matches `crate::logit`'s `serve_connection`, whose shutdown arm awaits, where a `Ref` kept alive
/// by `select!` would make the future `!Send`.
async fn read_step<S: AsyncRead + Unpin + Send>(
    stream: &mut S,
    buf: &mut BytesMut,
    shutdown: &mut watch::Receiver<bool>,
) -> ReadStep {
    tokio::select! {
        result = stream.read_buf(buf) => match result {
            Ok(0) => ReadStep::Eof,
            Ok(_) => ReadStep::Bytes,
            Err(err) => ReadStep::Failed(err),
        },
        _ = shutdown.changed() => ReadStep::Shutdown,
    }
}

/// Serves one accepted (and, under TLS, handshaken) connection to completion. Generic over the IO
/// type so plaintext (`TcpStream`) and TLS (`tokio_rustls::server::TlsStream<TcpStream>`) share
/// every line.
///
/// Owns its own [`Framer`], [`BatchAccumulator`] and decoder clone; nothing is shared with a
/// sibling connection. Flushes on the accumulator's bounds, on `batch_flush_interval`, on
/// shutdown, and on close (clean or otherwise).
///
/// `diag` is the accept loop's per-connection [`Diagnostics`] clone, borrowed so it is still
/// there for the `connection_error` report on whatever this returns. `framing_error` and
/// `bad_frame` are reported on it and still throttle listener-wide.
///
/// `handshake_timeout` bounds the wait for the *first* byte, TLS or not (this module's
/// "Pre-handshake timeout" doc section). `idle_timeout`, when `Some`, bounds every gap after it
/// (the "Idle timeout" section). The two share one next-byte deadline, since a connection is in
/// one phase at a time.
#[allow(clippy::too_many_arguments)] // one connection's whole context; a params struct would only move it
async fn serve_connection<S, D>(
    mut stream: S,
    mut decoder: D,
    mut framer: Framer,
    config: TcpListenerConfig,
    handshake_timeout: Duration,
    idle_timeout: Option<Duration>,
    attrs: Option<ConnectionAttrs>,
    sink: Fanout,
    telemetry: Telemetry,
    diag: &mut Diagnostics,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    D: Decoder + Send,
{
    // The idle clock's origin, advanced only when bytes are read from the peer and when this
    // connection's interval flush emits (this module's "Idle timeout" doc section).
    //
    // `first_byte_deadline` is absolute, computed once, rather than a budget re-armed per read:
    // the read is re-entered on every `batch_flush_interval` tick (the `Err(_elapsed) => continue`
    // arm), so a per-read budget would be reset by each tick and never fire.
    let mut last_progress = tokio::time::Instant::now();
    let first_byte_deadline = last_progress + handshake_timeout;
    // Cleared, not replaced, between reads so its capacity survives.
    let mut read_buf = BytesMut::with_capacity(READ_BUFFER_BYTES);
    let mut accumulator = BatchAccumulator::new(config.batch_max_events, config.batch_max_bytes);
    // Cleared, not taken, between `decode_into` calls: `BatchAccumulator::absorb`'s doc says why
    // `std::mem::take` would undo the allocation win.
    let mut scratch: Vec<Event> = Vec::new();
    let has_interval = !config.batch_flush_interval.is_zero();
    let mut next_flush =
        has_interval.then(|| tokio::time::Instant::now() + config.batch_flush_interval);

    loop {
        // The interval trigger, using `BatchAccumulator::next_deadline`'s cadence math as
        // `crate::udp::decode_loop` does.
        if let Some(deadline) = next_flush {
            let now_instant = tokio::time::Instant::now();
            if deadline <= now_instant {
                let mut now_instant = now_instant;
                if let Some(batch) = accumulator.take() {
                    emit(&sink, &telemetry, batch, FlushReason::Interval).await;
                    // Stamped after the send returns, so time blocked on a full downstream is
                    // not counted against the peer. A tick with nothing to emit never gets here:
                    // this process's own timer must not keep a silent connection alive.
                    last_progress = tokio::time::Instant::now();
                    // Re-read for the same reason: an `emit` parked past the next deadline would
                    // otherwise leave it already due, and every read after it would flush on
                    // `Interval`.
                    now_instant = last_progress;
                }
                next_flush = Some(BatchAccumulator::next_deadline(
                    deadline,
                    now_instant,
                    config.batch_flush_interval,
                ));
            }
        }

        // Checked explicitly: `read_step`'s `changed()` fires only on a transition this receiver
        // has not observed, so it would miss shutdown already being true. The `Ref` temporary
        // drops at the end of this statement, before any `.await`.
        if *shutdown.borrow() {
            report_buffered_tail(&mut framer, &telemetry, diag);
            if let Some(batch) = accumulator.take() {
                emit(&sink, &telemetry, batch, FlushReason::Shutdown).await;
            }
            return Ok(());
        }

        read_buf.clear();
        // Two deadlines can bound this read: the flush tick (recurring, benign) and the next-byte
        // deadline (ends the connection). Race whichever comes first, then decide which it was;
        // `timeout_at`, not `timeout`, so the next-byte deadline stays absolute across flush
        // ticks.
        //
        // The next-byte deadline is the first-byte deadline before the first byte and
        // `last_progress + idle_timeout` after it. With no `idle_timeout` it is `far_future`, so
        // there is no "is there an idle timeout" branch, only a deadline that may never arrive.
        // `checked_add` because an absurd (but legal) `idle_timeout` can overflow, and rule 53
        // caps nothing above `0s`. `first_byte_seen`, never `framing().is_none()` (this module's
        // "Pre-handshake timeout" doc section).
        let awaiting_first_byte = !framer.first_byte_seen();
        let next_byte_deadline = if awaiting_first_byte {
            first_byte_deadline
        } else {
            idle_timeout.and_then(|idle| last_progress.checked_add(idle)).unwrap_or_else(far_future)
        };
        let read_deadline =
            next_flush.map_or(next_byte_deadline, |flush| flush.min(next_byte_deadline));
        let step = match tokio::time::timeout_at(
            read_deadline,
            read_step(&mut stream, &mut read_buf, &mut shutdown),
        )
        .await
        {
            Ok(step) => step,
            Err(_elapsed) => {
                // Checked against the clock rather than inferred from which deadline was smaller,
                // so a flush tick landing on the same instant can't mask it.
                if tokio::time::Instant::now() >= next_byte_deadline {
                    // No first byte: a fault, returned as `Err` so it reaches the accept loop's
                    // `connection_error` diagnostic.
                    if awaiting_first_byte {
                        return Err(anyhow::anyhow!(
                            "the peer sent no bytes within {handshake_timeout:?}"
                        ));
                    }
                    // Idle: policy, not a fault, so `Ok(())` and no `connection_error` (this
                    // module's "Idle timeout" doc section). A buffered partial frame is counted
                    // `truncated`, as on the shutdown and RST paths.
                    report_buffered_tail(&mut framer, &telemetry, diag);
                    if let Some(batch) = accumulator.take() {
                        emit(&sink, &telemetry, batch, FlushReason::Closed).await;
                    }
                    telemetry.count("logit.input.connections.closed", 1.0, &[("reason", "idle")]);
                    return Ok(());
                }
                // The flush deadline won: back to the interval trigger.
                continue;
            }
        };

        match step {
            ReadStep::Bytes => {}
            ReadStep::Shutdown => {
                report_buffered_tail(&mut framer, &telemetry, diag);
                if let Some(batch) = accumulator.take() {
                    emit(&sink, &telemetry, batch, FlushReason::Shutdown).await;
                }
                return Ok(());
            }
            ReadStep::Eof => {
                // `Framer::finish` decides what a terminator-less remainder is.
                match framer.finish() {
                    Ok(Some(frame)) => {
                        absorb_frame(
                            frame,
                            now_nanos(),
                            &mut decoder,
                            attrs.as_ref(),
                            &mut scratch,
                            &mut accumulator,
                            &sink,
                            &telemetry,
                            diag,
                        )
                        .await;
                    }
                    Ok(None) => {}
                    Err(err) => {
                        report_frame_error(&err, &telemetry, diag);
                    }
                }
                if let Some(batch) = accumulator.take() {
                    emit(&sink, &telemetry, batch, FlushReason::Closed).await;
                }
                return Ok(());
            }
            // A reset before the first byte lost nothing: it's how a load balancer ends a health
            // check (this module's "Reset before the first byte" doc section).
            ReadStep::Failed(err)
                if awaiting_first_byte && err.kind() == std::io::ErrorKind::ConnectionReset =>
            {
                return Ok(());
            }
            ReadStep::Failed(err) => {
                // The connection broke, but what was already decoded is good: deliver it before
                // surfacing the error as `connection_error`.
                report_buffered_tail(&mut framer, &telemetry, diag);
                if let Some(batch) = accumulator.take() {
                    emit(&sink, &telemetry, batch, FlushReason::Closed).await;
                }
                return Err(err.into());
            }
        }

        // `received_at` is when the bytes came off the socket, not when the frame they complete is
        // decoded (`logit_proto::Decoder::decode_into`'s contract).
        let received_at = now_nanos();
        framer.push(&read_buf);
        loop {
            match framer.next_frame() {
                Ok(Some(frame)) => {
                    absorb_frame(
                        frame,
                        received_at,
                        &mut decoder,
                        attrs.as_ref(),
                        &mut scratch,
                        &mut accumulator,
                        &sink,
                        &telemetry,
                        diag,
                    )
                    .await;
                }
                Ok(None) => break,
                Err(err) => {
                    // Its own diagnostic key, not `connection_error`: the cause is the peer's
                    // framing, not I/O.
                    report_frame_error(&err, &telemetry, diag);
                    // A non-fatal error (`OversizeSkipped`, `Drained`) has already resynchronized
                    // the framer: it dropped one line and either consumed its terminator or
                    // latched the drain state that will.
                    if !err.is_fatal() {
                        continue;
                    }
                    // Nothing can resynchronize past a fatal error (see `FrameError`).
                    if let Some(batch) = accumulator.take() {
                        emit(&sink, &telemetry, batch, FlushReason::Closed).await;
                    }
                    return Ok(());
                }
            }
        }

        // One stamp covers both halves of progress: the read, and `absorb_frame`'s `emit` of a
        // full batch having returned. After the loop, not before, so time blocked in that `emit`
        // is not charged to the peer (this module's "Idle timeout" doc section).
        last_progress = tokio::time::Instant::now();
    }
}

/// One complete frame: counted, decoded, and accumulated. A decode error is diagnosed and the
/// frame dropped; the connection stays open, as `crate::udp::decode_loop` does for one bad
/// datagram.
#[allow(clippy::too_many_arguments)] // threaded-through borrows; a params struct would only move them
async fn absorb_frame<D: Decoder + Send>(
    frame: Bytes,
    received_at: i64,
    decoder: &mut D,
    attrs: Option<&ConnectionAttrs>,
    scratch: &mut Vec<Event>,
    accumulator: &mut BatchAccumulator,
    sink: &Fanout,
    telemetry: &Telemetry,
    diag: &mut Diagnostics,
) {
    telemetry.count("logit.input.frames", 1.0, &[]);
    telemetry.count("logit.input.frame.bytes", frame.len() as f64, &[]);
    scratch.clear();
    match decoder.decode_into(frame, received_at, scratch) {
        Ok((resource, scope)) => {
            // After the decoder, so the driver's values replace same-named decoded attributes.
            if let Some(attrs) = attrs {
                attrs.stamp(scratch);
            }
            // `scope` is threaded through rather than hardcoded `None`, as `crate::udp` does.
            if let Some((batch, reason)) = accumulator.absorb(resource, scope, scratch) {
                emit(sink, telemetry, batch, reason).await;
            }
        }
        Err(err) => {
            diag.warn_throttled("bad_frame", err);
        }
    }
}

/// Counts and diagnoses a framing failure, on its own diagnostic key: "my sender's frames are
/// rejected" is a different triage from "a peer's socket broke". Returns whether the diagnostic
/// reported (was not throttled), so a test can assert the listener-wide cadence.
fn report_frame_error(err: &FrameError, telemetry: &Telemetry, diag: &mut Diagnostics) -> bool {
    telemetry.count("logit.input.frames.dropped", 1.0, &[("reason", err.reason())]);
    diag.warn_throttled("framing_error", err)
}

/// Reports a partial frame still held by the [`Framer`] when a connection ends *without* a clean
/// EOF: a peer RST mid-message, a shutdown or idle close before the sender finished one.
///
/// Dropping those bytes is correct, since no complete message was sent, but [`Framer::abandon`]
/// returns them as `logit.input.frames.dropped{reason="truncated"}` so the loss is visible. It
/// agrees with what the same bytes followed by a FIN would do ([`Framer::finish`]), a blank
/// remainder counted by neither; only [`FramingMode::Rfc6587Auto`]'s LF arm instead emits a
/// remainder with content on a FIN, as RFC 6587 permits. A no-op when nothing is buffered, the
/// ordinary case.
fn report_buffered_tail(framer: &mut Framer, telemetry: &Telemetry, diag: &mut Diagnostics) {
    if let Some(err) = framer.abandon() {
        report_frame_error(&err, telemetry, diag);
    }
}

/// Sends one batch. `sink.send` mints a fresh [`logit_pipeline::TraceContext::new_root`] once
/// per *accumulated* batch, not per frame that fed it: the many-to-one attribution gap every
/// accumulating listener shares (`docs/known-gaps/telemetry.md`'s internal-spans entry).
async fn emit(sink: &Fanout, telemetry: &Telemetry, batch: EventBatch, reason: FlushReason) {
    telemetry.count("logit.component.receive.flushed", 1.0, &[("reason", reason.as_str())]);
    sink.send(batch).await;
}

/// Shared with [`crate::udp`] so both drivers stamp `received_at` identically.
fn now_nanos() -> i64 {
    crate::udp::now_nanos()
}

/// A deadline far enough out that it never arrives: the next-byte deadline on a connection with
/// no `idle_timeout` (or on overflow of an absurd one), so [`serve_connection`]'s read races one
/// deadline rather than an `Option` of one.
///
/// Local because tokio's `Instant::far_future` is `pub(crate)` to tokio; the horizon is tokio's
/// (30 years, since 100 overflows on some platforms). The read waits on it only with no flush
/// interval either; otherwise the flush tick is the earlier deadline. `pub(crate)` so the other
/// listeners' idle deadlines (`crate::logit`, `crate::http`) share one definition.
pub(crate) fn far_future() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(86_400 * 365 * 30)
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, Registry, Resource, Value};
    use logit_pipeline::test_util::{
        expect_closed, expect_still_open, recv_batch, wait_until, TelemetryProbe, Totals,
    };
    use logit_proto::framing::Oversize;
    use logit_proto::proxy;
    use logit_proto::CodecError;
    use rustls_pki_types::pem::PemObject;
    use rustls_pki_types::{CertificateDer, PrivateKeyDer};
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;
    use tokio::sync::mpsc;

    // ---- driver: fixtures and harness ---------------------------------------------------------

    /// One frame to one event carrying the raw frame under `"payload"`, except the literal bytes
    /// `b"BAD"`, which are rejected.
    #[derive(Clone)]
    struct TestDecoder {
        resource: Arc<Resource>,
    }

    impl TestDecoder {
        fn new() -> Self {
            Self { resource: Arc::new(Resource::default()) }
        }
    }

    impl Decoder for TestDecoder {
        fn decode_into(
            &mut self,
            bytes: Bytes,
            received_at: i64,
            out: &mut Vec<Event>,
        ) -> Result<(Arc<Resource>, Option<Arc<logit_core::Scope>>), CodecError> {
            if &bytes[..] == b"BAD" {
                return Err(CodecError::Malformed("bad frame".to_string()));
            }
            let mut attrs = AttrMap::new();
            attrs.insert("payload", Value::str(String::from_utf8_lossy(&bytes)));
            out.push(Event::empty(received_at, attrs));
            Ok((Arc::clone(&self.resource), None))
        }
    }

    fn payload(event: &Event) -> String {
        match event.attributes.get("payload") {
            Some(Value::Str(bytes)) => String::from_utf8_lossy(bytes).into_owned(),
            other => panic!("expected a payload attribute, got {other:?}"),
        }
    }

    /// One event per frame and no interval timer, so every delivery is attributable to one frame.
    fn one_per_frame() -> TcpListenerConfig {
        TcpListenerConfig {
            batch_max_events: 1,
            batch_flush_interval: Duration::ZERO,
            ..TcpListenerConfig::default()
        }
    }

    /// Binds an ephemeral port through `Input::bind`, not a bind-and-drop probe socket.
    async fn bound_listener(config: TcpListenerConfig) -> (String, TcpListener<TestDecoder>) {
        let mut listener = TcpListener::new("127.0.0.1:0", TestDecoder::new(), config);
        listener.bind().await.expect("binding an ephemeral port should succeed");
        let addr = listener.local_addr().expect("bind() leaves a real address behind").to_string();
        (addr, listener)
    }

    fn fanout_into_channel(capacity: usize) -> (Fanout, mpsc::Receiver<logit_pipeline::Delivered>) {
        let (tx, rx) = mpsc::channel(capacity);
        (Fanout::new(vec![tx]), rx)
    }

    fn payloads(batch: &EventBatch) -> Vec<String> {
        batch.events.iter().map(payload).collect()
    }

    async fn connect(addr: &str) -> TcpStream {
        TcpStream::connect(addr).await.expect("connecting to the bound listener should succeed")
    }

    fn testdata_dir() -> std::path::PathBuf {
        // The repo root's `testdata/tls` (`testdata/tls/README.md`), two levels up.
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    fn test_tls_settings(client_ca_file: Option<&str>) -> TlsServerSettings {
        TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: client_ca_file.map(str::to_string),
        }
    }

    /// A `tokio-rustls` client trusting only `ca_file` under `testdata/tls` (`other-ca.pem` makes
    /// a real wrong-CA case). `client_cert` is `(cert, key)` file names for mTLS, `None` for none.
    async fn tls_connector(
        ca_file: &str,
        client_cert: Option<(&str, &str)>,
    ) -> tokio_rustls::TlsConnector {
        let dir = testdata_dir();
        let mut roots = rustls::RootCertStore::empty();
        let ca: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(dir.join(ca_file))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        roots.add_parsable_certificates(ca);
        let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots);
        let cfg = match client_cert {
            Some((cert_file, key_file)) => {
                let chain: Vec<CertificateDer<'static>> =
                    CertificateDer::pem_file_iter(dir.join(cert_file))
                        .unwrap()
                        .collect::<Result<_, _>>()
                        .unwrap();
                let key = PrivateKeyDer::from_pem_file(dir.join(key_file)).unwrap();
                builder.with_client_auth_cert(chain, key).unwrap()
            }
            None => builder.with_no_client_auth(),
        };
        tokio_rustls::TlsConnector::from(Arc::new(cfg))
    }

    /// `testdata/tls/server.pem`'s SAN.
    fn server_name() -> rustls_pki_types::ServerName<'static> {
        rustls_pki_types::ServerName::try_from("localhost").unwrap()
    }

    type ClientTls = tokio_rustls::client::TlsStream<TcpStream>;

    async fn tls_connect(connector: &tokio_rustls::TlsConnector, addr: &str) -> ClientTls {
        let stream = connect(addr).await;
        tokio::time::timeout(Duration::from_secs(5), connector.connect(server_name(), stream))
            .await
            .expect("the TLS handshake should complete within 5s")
            .expect("the TLS handshake should succeed")
    }

    // ---- driver: plaintext --------------------------------------------------------------------

    /// After `bind()` the port is live and its address readable before `run`; a second `bind()`
    /// is a no-op.
    #[tokio::test]
    async fn bind_makes_the_port_live_and_local_addr_reports_it_before_run() {
        let mut listener =
            TcpListener::new("127.0.0.1:0", TestDecoder::new(), TcpListenerConfig::default());
        assert_eq!(listener.local_addr(), None, "no address before bind()");

        listener.bind().await.expect("binding an ephemeral port should succeed");
        let addr = listener.local_addr().expect("bind() should leave a real address behind");

        // Nothing is running yet: this connection sits in the accept backlog.
        let _early = TcpStream::connect(addr).await.expect("the bound port should accept");

        listener.bind().await.expect("a second bind should be a harmless no-op");
        assert_eq!(listener.local_addr(), Some(addr), "the address must not change");
    }

    #[tokio::test]
    async fn a_plaintext_connection_round_trips_a_decoded_frame() {
        let (addr, mut listener) = bound_listener(one_per_frame()).await;
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        client.write_all(b"<13>hello\n").await.unwrap();

        let batch = recv_batch(&mut rx).await;
        assert_eq!(payloads(&batch), vec!["<13>hello"]);

        handle.abort();
    }

    #[tokio::test]
    async fn the_accumulator_flushes_on_batch_max_events() {
        let config = TcpListenerConfig {
            batch_max_events: 2,
            batch_flush_interval: Duration::ZERO,
            ..TcpListenerConfig::default()
        };
        let (addr, mut listener) = bound_listener(config).await;
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        // The bound fires on the second frame; with the interval timer off the third stays held.
        client.write_all(b"<13>one\n<13>two\n<13>three\n").await.unwrap();

        let batch = recv_batch(&mut rx).await;
        assert_eq!(payloads(&batch), vec!["<13>one", "<13>two"]);
        assert!(
            tokio::time::timeout(Duration::from_millis(200), rx.recv()).await.is_err(),
            "the third frame must still be accumulating, not delivered"
        );

        handle.abort();
    }

    #[tokio::test]
    async fn the_accumulator_flushes_on_the_batch_flush_interval() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let config = TcpListenerConfig {
            batch_max_events: 1_000,
            batch_flush_interval: Duration::from_millis(50),
            ..TcpListenerConfig::default()
        };
        let (addr, listener) = bound_listener(config).await;
        let mut listener = listener.with_telemetry(telemetry);
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        // Short of `batch_max_events` on an open connection: only the interval timer delivers it.
        client.write_all(b"<13>alone\n").await.unwrap();

        let batch = recv_batch(&mut rx).await;
        assert_eq!(payloads(&batch), vec!["<13>alone"]);

        let events = Totals::of(registry.drain(0));
        assert_eq!(events.sum("logit.component.receive.flushed", &[("reason", "interval")]), 1.0);
        assert_eq!(events.sum("logit.input.frames", &[]), 1.0);
        assert_eq!(events.sum("logit.input.frame.bytes", &[]), 9.0);

        handle.abort();
    }

    #[tokio::test]
    async fn a_clean_close_flushes_whatever_is_accumulated() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let config = TcpListenerConfig {
            batch_max_events: 1_000,
            batch_flush_interval: Duration::ZERO,
            ..TcpListenerConfig::default()
        };
        let (addr, listener) = bound_listener(config).await;
        let mut listener = listener.with_telemetry(telemetry);
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        // The unterminated last message is emitted as a final frame (`Framer::finish`), then the
        // accumulator flushes.
        client.write_all(b"<13>one\n<13>two").await.unwrap();
        client.flush().await.unwrap();
        drop(client);

        let batch = recv_batch(&mut rx).await;
        assert_eq!(payloads(&batch), vec!["<13>one", "<13>two"]);
        assert_eq!(
            Totals::of(registry.drain(0))
                .sum("logit.component.receive.flushed", &[("reason", "closed")]),
            1.0
        );

        handle.abort();
    }

    /// A rejected frame is dropped; the frames either side of it are still served.
    #[tokio::test]
    async fn a_frame_the_decoder_rejects_does_not_close_the_connection() {
        let (addr, mut listener) = bound_listener(one_per_frame()).await;
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        client.write_all(b"<13>good-1\nBAD\n<13>good-2\n").await.unwrap();

        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>good-1"]);
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>good-2"]);

        handle.abort();
    }

    /// Past the cap, a connection is closed rather than queued, and counted.
    #[tokio::test]
    async fn the_connection_cap_drops_a_connection_past_the_limit_and_counts_it() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener = listener.with_telemetry(telemetry).with_max_connections(1);
        let (sink, _rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        // The first connection holds the one permit. The accept loop takes a permit as it accepts
        // a connection, one accept at a time and in the kernel queue's order, so `_first` holds
        // it before `second` is accepted.
        let _first = connect(&addr).await;

        let mut second = connect(&addr).await;
        expect_closed(&mut second, "a past-the-cap connection").await;

        assert_eq!(
            Totals::of(registry.drain(0))
                .sum("logit.input.connections.rejected", &[("reason", "limit")]),
            1.0
        );

        handle.abort();
    }

    /// A connection's `Diagnostics` clone throttles on the listener-wide count.
    #[test]
    fn the_per_frame_diagnostic_throttle_is_shared_not_per_connection() {
        let diag = Diagnostics::new("syslog_in");
        // What the accept loop hands one connection task.
        let mut connection_diag = diag.clone();
        let telemetry = Telemetry::default();
        let err = FrameError::Malformed("an octet count of zero".to_string());

        assert!(
            report_frame_error(&err, &telemetry, &mut connection_diag),
            "the 1st occurrence across the listener reports"
        );
        assert!(
            report_frame_error(&err, &telemetry, &mut connection_diag),
            "the 2nd reports too -- 2 is a power of two"
        );
        assert!(
            !report_frame_error(&err, &telemetry, &mut connection_diag),
            "the 3rd is suppressed, which an unshared per-connection count could never manage"
        );
        assert_eq!(
            diag.occurrences("framing_error"),
            3,
            "and the listener's own value reads all three back"
        );
    }

    /// Three connections' framing errors all count on the one [`Diagnostics`] the listener got.
    #[tokio::test]
    async fn three_connections_report_their_framing_errors_through_one_throttle() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let diag = Diagnostics::new("syslog_in");
        // The occurrence count is the one observable that differs if counts aren't shared: the
        // metrics below reach 3 either way, since `Telemetry` mirrors into one component buffer.
        let listener_diag = diag.clone();
        let listener = listener.with_telemetry(telemetry).with_diagnostics(diag);
        let (sink, _rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut listener = listener;
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        for attempt in 0..3 {
            // A zero octet count: malformed, and fatal to its connection.
            let mut client = connect(&addr).await;
            client.write_all(b"0 nope").await.unwrap();
            expect_closed(&mut client, &format!("connection {attempt} after a malformed count"))
                .await;
        }

        assert_eq!(
            listener_diag.occurrences("framing_error"),
            3,
            "all three connections must count on the one listener-wide Diagnostics -- a clone \
             with counts of its own would leave this at 0, having counted 1 in each throwaway copy"
        );
        // Holds either way; confirms the classification.
        assert_eq!(
            Totals::of(registry.drain(0))
                .sum("logit.input.frames.dropped", &[("reason", "malformed")]),
            3.0
        );

        handle.abort();
    }

    /// An RST with a partial frame buffered (`ReadStep::Failed`) counts it `truncated`.
    #[tokio::test]
    async fn an_abrupt_close_with_a_buffered_partial_frame_counts_it_truncated() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener = listener.with_telemetry(telemetry);
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        client.write_all(b"<13>complete\n").await.unwrap();
        // Awaiting the delivery proves the server is back blocked in the next read, so it
        // consumes the tail below into its framer before the RST (an RST landing on still-queued
        // bytes discards them unread, an uncountable case).
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>complete"]);

        client.write_all(b"<13>unterminated").await.unwrap();
        // Covers the listener's read of the partial frame into its framer. Nothing counts bytes
        // before a frame completes, so there is no observable to wait on instead.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // `SO_LINGER 0` makes the close an RST, not a FIN, so the server's read fails
        // `ECONNRESET` (`ReadStep::Failed`) rather than reaching a clean EOF.
        socket2::SockRef::from(&client)
            .set_linger(Some(Duration::ZERO))
            .expect("SO_LINGER should be settable on a loopback socket");
        drop(client);

        // The count lands on the connection's own task, after its read fails.
        let mut probe = TelemetryProbe::with_registry(registry);
        let totals = probe
            .wait_for("the partial frame counted truncated", |t| {
                t.sum("logit.input.frames.dropped", &[("reason", "truncated")]) >= 1.0
            })
            .await;
        assert_eq!(
            totals.sum("logit.input.frames.dropped", &[("reason", "truncated")]),
            1.0,
            "the partial frame the RST discarded is counted once"
        );

        handle.abort();
    }

    /// Shutdown mid-message counts the partial frame `truncated`.
    #[tokio::test]
    async fn shutdown_mid_message_counts_the_buffered_partial_frame() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener = listener.with_telemetry(telemetry);
        let (sink, mut rx) = fanout_into_channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        client.write_all(b"<13>complete\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>complete"]);

        client.write_all(b"<13>half a mes").await.unwrap();
        // Covers the listener's read of the partial frame into its framer. Nothing counts bytes
        // before a frame completes, so there is no observable to wait on instead.
        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown_tx.send(true).expect("the receiver should still be alive");

        let closed = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
        assert!(
            closed.expect("the fanout should close within 2s").is_none(),
            "nothing complete was pending, so no batch should follow"
        );
        assert_eq!(
            Totals::of(registry.drain(0))
                .sum("logit.input.frames.dropped", &[("reason", "truncated")]),
            1.0
        );

        handle.await.expect("the task should not panic").expect("shutdown should be clean");
    }

    /// A fatal framing error closes its own connection and no sibling.
    #[tokio::test]
    async fn an_oversize_frame_closes_only_that_connection() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener = listener.with_telemetry(telemetry);
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut good = connect(&addr).await;
        let mut bad = connect(&addr).await;
        good.write_all(b"<13>fine\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>fine"]);

        // The write may fail once the server has closed on us, which is the behaviour under test.
        let oversize = vec![b'<'; MAX_FRAME_BYTES + 4_096];
        let _ = tokio::time::timeout(Duration::from_secs(5), bad.write_all(&oversize)).await;
        expect_closed(&mut bad, "the connection that sent an oversize frame").await;

        good.write_all(b"<13>still here\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>still here"]);

        assert_eq!(
            Totals::of(registry.drain(0))
                .sum("logit.input.frames.dropped", &[("reason", "oversize")]),
            1.0
        );

        handle.abort();
    }

    /// Shutdown drops every connection's `Fanout` clone, even an idle connection's.
    #[tokio::test]
    async fn shutdown_returns_promptly_with_an_idle_connection_still_open() {
        let (addr, mut listener) = bound_listener(one_per_frame()).await;
        let (sink, mut rx) = fanout_into_channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        // An idle connection's task drops its clone only because its read races `shutdown`.
        let _idle = connect(&addr).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown_tx.send(true).expect("the receiver should still be alive");

        let closed = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
        assert!(
            closed.expect("the fanout should close within 2s").is_none(),
            "expected every Fanout clone to have dropped"
        );
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("run_until_shutdown should return within 2s")
            .expect("the task should not panic")
            .expect("shutdown should be clean");
    }

    // ---- driver: TLS --------------------------------------------------------------------------

    #[tokio::test]
    async fn a_tls_connection_round_trips_a_decoded_frame() {
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener = listener
            .with_tls(
                &test_tls_settings(None),
                &testdata_dir(),
                &logit_pipeline::tls::TlsReloader::new(),
            )
            .unwrap();
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let connector = tls_connector("ca.pem", None).await;
        let mut client = tls_connect(&connector, &addr).await;
        // Octet counting over TLS: the latch is independent of the transport.
        client.write_all(b"12 <13>over tls").await.unwrap();
        client.flush().await.unwrap();

        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>over tls"]);

        handle.abort();
    }

    #[tokio::test]
    async fn a_client_trusting_the_wrong_ca_is_refused_and_the_listener_keeps_serving() {
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener = listener
            .with_tls(
                &test_tls_settings(None),
                &testdata_dir(),
                &logit_pipeline::tls::TlsReloader::new(),
            )
            .unwrap();
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let wrong = tls_connector("other-ca.pem", None).await;
        let stream = connect(&addr).await;
        let refused =
            tokio::time::timeout(Duration::from_secs(5), wrong.connect(server_name(), stream))
                .await
                .expect("the handshake should resolve within 5s");
        assert!(refused.is_err(), "a client trusting only other-ca.pem must not complete");

        // A failed handshake must not be fatal to the listener or its siblings.
        let connector = tls_connector("ca.pem", None).await;
        let mut client = tls_connect(&connector, &addr).await;
        client.write_all(b"<13>still serving\n").await.unwrap();
        client.flush().await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>still serving"]);

        handle.abort();
    }

    #[tokio::test]
    async fn mutual_tls_accepts_a_client_certificate_and_refuses_a_client_without_one() {
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener = listener
            .with_tls(
                &test_tls_settings(Some("ca.pem")),
                &testdata_dir(),
                &logit_pipeline::tls::TlsReloader::new(),
            )
            .unwrap();
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let with_cert = tls_connector("ca.pem", Some(("client.pem", "client.key"))).await;
        let mut client = tls_connect(&with_cert, &addr).await;
        client.write_all(b"<13>authenticated\n").await.unwrap();
        client.flush().await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>authenticated"]);

        // Under TLS 1.3 the server's "certificate required" alert lands after the client believes
        // the handshake finished, so the failure may surface at connect or on the first write;
        // either way nothing is delivered.
        let without_cert = tls_connector("ca.pem", None).await;
        let stream = connect(&addr).await;
        if let Ok(Ok(mut anonymous)) = tokio::time::timeout(
            Duration::from_secs(5),
            without_cert.connect(server_name(), stream),
        )
        .await
        {
            let _ = anonymous.write_all(b"<13>no certificate\n").await;
            let _ = anonymous.flush().await;
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(500), rx.recv()).await.is_err(),
            "a client presenting no certificate must not deliver events"
        );

        handle.abort();
    }

    /// A silent plaintext connection releases its permit at the first-byte deadline.
    #[tokio::test]
    async fn a_silent_plaintext_connection_releases_its_permit_after_the_handshake_timeout() {
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener =
            listener.with_max_connections(1).with_handshake_timeout(Duration::from_millis(50));
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        // Held (not dropped) past the deadline, so only the deadline can free the permit.
        let mut silent = connect(&addr).await;
        expect_closed(&mut silent, "a plaintext connection that sent no bytes").await;

        let mut client = connect(&addr).await;
        client.write_all(b"<13>permit came back\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>permit came back"]);

        drop(silent);
        handle.abort();
    }

    /// With no `idle_timeout`, a gap past the first-byte budget after the first frame is fine.
    #[tokio::test]
    async fn the_first_byte_deadline_does_not_apply_once_the_framing_has_latched() {
        // The flush timer left on: a tick re-entering the read is what a per-read budget would
        // keep resetting.
        let config = TcpListenerConfig {
            batch_max_events: 1,
            batch_flush_interval: Duration::from_millis(100),
            ..TcpListenerConfig::default()
        };
        let (addr, listener) = bound_listener(config).await;
        let mut listener = listener.with_handshake_timeout(Duration::from_millis(50));
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        client.write_all(b"<13>first\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>first"]);

        // Past the first-byte budget and several flush ticks.
        tokio::time::sleep(Duration::from_millis(300)).await;
        client.write_all(b"<13>much later\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>much later"]);

        handle.abort();
    }

    /// The first-byte deadline fires under every framing mode, not only `Rfc6587Auto`.
    #[tokio::test]
    async fn the_first_byte_deadline_applies_under_every_framing_mode() {
        // `(mode, the wire bytes of one frame)`; each decodes to `<13>hello`.
        let length_prefixed = {
            let mut wire = 9u32.to_be_bytes().to_vec();
            wire.extend_from_slice(b"<13>hello");
            wire
        };
        let cases: Vec<(FramingMode, Vec<u8>)> = vec![
            (FramingMode::Rfc6587Auto, b"<13>hello\n".to_vec()),
            (FramingMode::Lines { oversize: Oversize::Fatal }, b"<13>hello\n".to_vec()),
            (FramingMode::Lines { oversize: Oversize::DrainToNextLine }, b"<13>hello\n".to_vec()),
            (FramingMode::LengthPrefixed, length_prefixed),
        ];

        for (mode, wire) in cases {
            let (addr, listener) = bound_listener(one_per_frame()).await;
            let mut listener = listener
                .with_framing(mode, MAX_FRAME_BYTES)
                .with_max_connections(1)
                .with_handshake_timeout(Duration::from_millis(50));
            let (sink, mut rx) = fanout_into_channel(16);
            let (_shutdown_tx, shutdown_rx) = watch::channel(false);
            let handle =
                tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

            // Held (not dropped) past the deadline, so only the deadline can free the permit.
            let mut silent = connect(&addr).await;
            expect_closed(&mut silent, &format!("a silent connection under {mode:?}")).await;

            let mut client = connect(&addr).await;
            client.write_all(&wire).await.unwrap();
            assert_eq!(
                payloads(&recv_batch(&mut rx).await),
                vec!["<13>hello"],
                "the permit must have come back under {mode:?}"
            );

            drop(silent);
            handle.abort();
        }
    }

    /// A TLS connection that never sends a ClientHello releases its permit at the timeout.
    #[tokio::test]
    async fn a_silent_connection_releases_its_permit_after_the_handshake_timeout() {
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener = listener
            .with_tls(
                &test_tls_settings(None),
                &testdata_dir(),
                &logit_pipeline::tls::TlsReloader::new(),
            )
            .unwrap()
            .with_max_connections(1)
            .with_handshake_timeout(Duration::from_millis(50));
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        // Held (not dropped) past the timeout, so only the timeout can free the permit. The
        // connection's task drops the stream and then its permit with no `.await` between, so on
        // this current-thread runtime the close means the permit is back.
        let mut silent = connect(&addr).await;
        expect_closed(&mut silent, "a TLS connection that sent no ClientHello").await;

        let connector = tls_connector("ca.pem", None).await;
        let mut client = tls_connect(&connector, &addr).await;
        client.write_all(b"<13>permit came back\n").await.unwrap();
        client.flush().await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>permit came back"]);

        drop(silent);
        handle.abort();
    }

    // ---- driver: idle timeout -----------------------------------------------------------------
    //
    // Real durations (50-500ms), never `tokio::time::pause()`: these tests race a timer against
    // a socket read, and paused time would advance past the read. "Closed" assertions have
    // `expect_closed`'s 5s ceiling against deadlines of at most 500ms; "still open" ones assert
    // `timeout(50ms, read) == Err(Elapsed)`, which scheduler lag only makes more true.

    /// An idle connection is closed, counted but not diagnosed, and its permit comes back.
    #[tokio::test]
    async fn an_idle_connection_is_closed_after_the_idle_timeout_and_releases_its_permit() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let diag = Diagnostics::new("syslog_in");
        let listener_diag = diag.clone();
        let mut listener = listener
            .with_telemetry(telemetry)
            .with_diagnostics(diag)
            .with_max_connections(1)
            .with_idle_timeout(Some(Duration::from_millis(100)));
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        // One frame, so only the idle clock can close this; then silence, socket held open.
        let mut quiet = connect(&addr).await;
        quiet.write_all(b"<13>hello\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>hello"]);

        expect_closed(&mut quiet, "a connection quiet past its idle_timeout").await;

        let drained = Totals::of(registry.drain(0));
        assert_eq!(
            drained.sum("logit.input.connections.closed", &[("reason", "idle")]),
            1.0,
            "an idle close is counted"
        );
        assert_eq!(
            listener_diag.occurrences("connection_error"),
            0,
            "and never diagnosed -- an idle close returns Ok(()), so the accept loop's \
             connection_error path must not see it"
        );

        let mut client = connect(&addr).await;
        client.write_all(b"<13>permit came back\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>permit came back"]);

        drop(quiet);
        handle.abort();
    }

    /// Time parked in `Fanout::send` on a full downstream never counts toward the idle clock.
    #[tokio::test]
    async fn a_connection_blocked_on_a_full_downstream_is_not_closed_as_idle() {
        let idle = Duration::from_millis(100);
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener = listener.with_idle_timeout(Some(idle));
        // Capacity 1: the first send is buffered, the second blocks until something receives.
        let (sink, mut rx) = fanout_into_channel(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        client.write_all(b"<13>one\n").await.unwrap();
        client.write_all(b"<13>two\n").await.unwrap();

        // Long enough that a clock running across the blocked send would have fired three times.
        tokio::time::sleep(idle * 3).await;
        expect_still_open(
            &mut client,
            Duration::from_millis(50),
            "a connection blocked on a full downstream",
        )
        .await;

        // Written while the task is parked in `Fanout::send`; these bytes sit in the socket
        // buffer.
        client.write_all(b"<13>three\n").await.unwrap();

        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>one"]);
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>two"]);
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>three"]);

        handle.abort();
    }

    /// An idle close flushes the accumulated batch and counts a buffered partial frame.
    #[tokio::test]
    async fn an_idle_close_flushes_the_accumulated_batch_and_counts_a_buffered_partial_frame_truncated(
    ) {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        // No interval timer and a high bound: only the idle close's flush delivers the frame.
        let config = TcpListenerConfig {
            batch_flush_interval: Duration::ZERO,
            ..TcpListenerConfig::default()
        };
        let (addr, listener) = bound_listener(config).await;
        let mut listener =
            listener.with_telemetry(telemetry).with_idle_timeout(Some(Duration::from_millis(100)));
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        client.write_all(b"<13>complete\n<13>half a mes").await.unwrap();

        assert_eq!(
            payloads(&recv_batch(&mut rx).await),
            vec!["<13>complete"],
            "the accumulated batch is flushed on the way out, not dropped"
        );
        expect_closed(&mut client, "a connection quiet past its idle_timeout").await;

        let drained = Totals::of(registry.drain(0));
        assert_eq!(
            drained.sum("logit.input.frames.dropped", &[("reason", "truncated")]),
            1.0,
            "the partial frame the idle close discarded must still be counted"
        );
        assert_eq!(drained.sum("logit.component.receive.flushed", &[("reason", "closed")]), 1.0);
        assert_eq!(drained.sum("logit.input.connections.closed", &[("reason", "idle")]), 1.0);

        handle.abort();
    }

    /// A lone `CR` after the last `LF` carries no message, so an idle close counts nothing, as a
    /// FIN over the same bytes does (`Framer::abandon`).
    #[tokio::test]
    async fn an_idle_close_with_only_a_bare_cr_buffered_counts_nothing() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let config = TcpListenerConfig {
            batch_flush_interval: Duration::ZERO,
            ..TcpListenerConfig::default()
        };
        let (addr, listener) = bound_listener(config).await;
        let mut listener =
            listener.with_telemetry(telemetry).with_idle_timeout(Some(Duration::from_millis(100)));
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        client.write_all(b"<13>complete\n\r").await.unwrap();

        // Only the idle close's flush delivers the frame, so the `CR` was read by then.
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>complete"]);
        expect_closed(&mut client, "a connection quiet past its idle_timeout").await;

        let drained = Totals::of(registry.drain(0));
        assert_eq!(drained.sum("logit.input.connections.closed", &[("reason", "idle")]), 1.0);
        assert_eq!(
            drained.sum("logit.input.frames.dropped", &[("reason", "truncated")]),
            0.0,
            "a bare CR is not a truncated frame"
        );

        handle.abort();
    }

    /// A flush tick with nothing to emit does not re-arm the idle clock.
    #[tokio::test]
    async fn a_flush_tick_does_not_reset_the_idle_clock() {
        let config = TcpListenerConfig {
            batch_max_events: 1,
            batch_flush_interval: Duration::from_millis(20),
            ..TcpListenerConfig::default()
        };
        let (addr, listener) = bound_listener(config).await;
        let mut listener = listener.with_idle_timeout(Some(Duration::from_millis(100)));
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        client.write_all(b"<13>hello\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>hello"]);

        // Several 20ms ticks land in every 100ms idle window, so a tick-shaped reset would keep
        // this open and `expect_closed` would hit its 2s ceiling.
        expect_closed(&mut client, "a quiet connection under a fast flush interval").await;

        handle.abort();
    }

    /// Progress is bytes, not frames: a sender dribbling a partial frame is not idle.
    #[tokio::test]
    async fn bytes_that_complete_no_frame_still_reset_the_idle_clock() {
        let (addr, listener) = bound_listener(one_per_frame()).await;
        // A 500ms idle timeout against 100ms gaps: 400ms of margin for scheduler lag between one
        // byte and the next, which is all that separates a byte-driven clock from an idle close.
        let mut listener = listener.with_idle_timeout(Some(Duration::from_millis(500)));
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        client.write_all(b"<13>").await.unwrap();
        for byte in b"dribble" {
            tokio::time::sleep(Duration::from_millis(100)).await;
            client.write_all(&[*byte]).await.unwrap();
        }
        // At least 700ms of wall clock has passed on a 500ms idle timeout, with no frame ever
        // completed.
        client.write_all(b"\n").await.unwrap();

        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>dribble"]);

        handle.abort();
    }

    /// The idle timeout fires under every framing mode.
    #[tokio::test]
    async fn the_idle_timeout_applies_under_every_framing_mode() {
        let length_prefixed = {
            let mut wire = 9u32.to_be_bytes().to_vec();
            wire.extend_from_slice(b"<13>hello");
            wire
        };
        let cases: Vec<(FramingMode, Vec<u8>)> = vec![
            (FramingMode::Rfc6587Auto, b"<13>hello\n".to_vec()),
            (FramingMode::Lines { oversize: Oversize::Fatal }, b"<13>hello\n".to_vec()),
            (FramingMode::Lines { oversize: Oversize::DrainToNextLine }, b"<13>hello\n".to_vec()),
            (FramingMode::LengthPrefixed, length_prefixed),
        ];

        for (mode, wire) in cases {
            let (addr, listener) = bound_listener(one_per_frame()).await;
            let mut listener = listener
                .with_framing(mode, MAX_FRAME_BYTES)
                .with_max_connections(1)
                .with_idle_timeout(Some(Duration::from_millis(50)));
            let (sink, mut rx) = fanout_into_channel(16);
            let (_shutdown_tx, shutdown_rx) = watch::channel(false);
            let handle =
                tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

            let mut quiet = connect(&addr).await;
            quiet.write_all(&wire).await.unwrap();
            assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>hello"]);
            expect_closed(&mut quiet, &format!("a quiet connection under {mode:?}")).await;

            let mut client = connect(&addr).await;
            client.write_all(&wire).await.unwrap();
            assert_eq!(
                payloads(&recv_batch(&mut rx).await),
                vec!["<13>hello"],
                "the permit must have come back under {mode:?}"
            );

            drop(quiet);
            handle.abort();
        }
    }

    /// With no `idle_timeout`, a quiet connection is never closed or counted idle.
    #[tokio::test]
    async fn no_idle_timeout_means_a_quiet_connection_is_never_closed() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let config = TcpListenerConfig {
            batch_max_events: 1,
            batch_flush_interval: Duration::from_millis(20),
            ..TcpListenerConfig::default()
        };
        let (addr, listener) = bound_listener(config).await;
        // No `with_idle_timeout` call.
        let mut listener = listener.with_telemetry(telemetry);
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = connect(&addr).await;
        client.write_all(b"<13>first\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>first"]);

        // Fifteen flush ticks of silence.
        tokio::time::sleep(Duration::from_millis(300)).await;
        expect_still_open(
            &mut client,
            Duration::from_millis(50),
            "a quiet connection with no idle_timeout",
        )
        .await;
        client.write_all(b"<13>much later\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>much later"]);

        assert!(
            !Totals::of(registry.drain(0))
                .has("logit.input.connections.closed", &[("reason", "idle")]),
            "nothing was closed as idle, so the counter was never touched"
        );

        handle.abort();
    }

    // ---- the kernel's accept queue (`AcceptQueueSampler`) --------------------------------------

    /// A running listener reports the accept-queue gauges with no configuration.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn the_kernel_accept_queue_gauges_are_reported_for_a_running_listener() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener = listener.with_telemetry(telemetry);
        let (sink, mut rx) = fanout_into_channel(8);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        // A delivered frame proves its accept happened, and so the sample before it.
        let mut client = connect(&addr).await;
        client.write_all(b"<13>hello\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut rx).await), vec!["<13>hello"]);

        let events = Totals::of(registry.drain(0));
        let depth = events
            .gauge("logit.input.accept_queue.depth", &[])
            .expect("the accept-queue depth should be gauged before each accept");
        let limit = events
            .gauge("logit.input.accept_queue.limit", &[])
            .expect("the backlog ceiling should be gauged in its own right, not left implicit");
        let utilization = events
            .gauge("logit.input.accept_queue.utilization", &[])
            .expect("the utilization gauge's presence means the kernel reported a real backlog");
        assert!(depth >= 0.0, "a queue depth is never negative, got {depth}");
        assert!(limit > 0.0, "a listening socket always has a backlog ceiling, got {limit}");
        // No upper bound of 1.0: the ratio can exceed it
        // (`an_over_full_accept_queue_reports_a_utilization_above_one`).
        assert!(utilization >= 0.0, "utilization is depth/backlog, never negative: {utilization}");
        assert!(
            (utilization - depth / limit).abs() < 1e-9,
            "the three must be consistent: utilization is exactly depth/limit"
        );

        handle.abort();
    }

    /// A sampler with nothing to read disables itself on its first call.
    #[tokio::test]
    async fn an_accept_queue_sampler_that_cannot_read_the_queue_disables_itself() {
        let listener = TokioTcpListener::bind("127.0.0.1:0").await.expect("should bind loopback");
        let mut sampler = AcceptQueueSampler::new(Telemetry::default(), Diagnostics::default());
        // The non-Linux shape.
        sampler.read_queue = |_| Err(sockstat::Unavailable::NotLinux);

        assert!(sampler.enabled, "a fresh sampler always tries once");
        sampler.sample_once(&listener);
        assert!(!sampler.enabled, "one failed read is enough -- these fields never appear later");
        sampler.sample_once(&listener); // still a harmless no-op
        assert!(!sampler.enabled);
    }

    /// A disabled sampler still accepts, arms no timer, and records nothing.
    #[tokio::test]
    async fn a_disabled_accept_queue_sampler_still_accepts_and_records_nothing() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let listener = TokioTcpListener::bind("127.0.0.1:0").await.expect("should bind loopback");
        let addr = listener.local_addr().expect("a bound listener has an address");
        let mut sampler = AcceptQueueSampler::new(telemetry, Diagnostics::default());
        sampler.read_queue = |_| Err(sockstat::Unavailable::NotLinux);

        let client = tokio::spawn(async move { TcpStream::connect(addr).await });
        let (_accepted, _peer) = sampler
            .accept(&listener)
            .await
            .expect("a disabled sampler must still accept exactly as a bare accept() would");
        client.await.expect("the connect task should not panic").expect("connect should succeed");
        assert!(sampler.tick.is_none(), "a disabled sampler arms no timer at all");

        let events = Totals::of(registry.drain(0));
        assert_eq!(events.gauge("logit.input.accept_queue.depth", &[]), None);
        assert_eq!(events.gauge("logit.input.accept_queue.limit", &[]), None);
        assert_eq!(events.gauge("logit.input.accept_queue.utilization", &[]), None);
    }

    /// A `listen(1)` socket's queue overshoots its ceiling and the gauge reports it unclamped.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn an_over_full_accept_queue_reports_a_utilization_above_one() {
        use socket2::{Domain, Socket, Type};

        let addr: std::net::SocketAddr = "127.0.0.1:0".parse().expect("a literal address");
        let server = Socket::new(Domain::IPV4, Type::STREAM, None).expect("socket(2)");
        server.bind(&addr.into()).expect("bind to an ephemeral loopback port");
        // `sk_acceptq_is_full` (`include/net/sock.h`, v6.12) is `sk_ack_backlog >
        // sk_max_ack_backlog`, strictly greater (kernel commit 64a146513f8f), and
        // `inet_csk_reqsk_queue_add` increments after that check with no second test. So a
        // `listen(1)` socket admits two connections and settles at depth 2: `limit + 1`. The
        // third connect's handshake is dropped, and nothing ever calls `accept`.
        server.listen(1).expect("listen(2) with a backlog of exactly one");
        server.set_nonblocking(true).expect("tokio requires a nonblocking listener");
        let bound = server
            .local_addr()
            .expect("a bound socket has an address")
            .as_socket()
            .expect("an AF_INET address");

        let mut clients = Vec::new();
        for _ in 0..3 {
            let client = Socket::new(Domain::IPV4, Type::STREAM, None).expect("socket(2)");
            client.set_nonblocking(true).expect("a blocking connect could hang on a full queue");
            // `EINPROGRESS` is the expected answer for all three; the handshake finishes (or does
            // not) in the kernel while this test waits below.
            let _ = client.connect(&bound.into());
            clients.push(client);
        }

        let listener = TokioTcpListener::from_std(std::net::TcpListener::from(server))
            .expect("a nonblocking listening socket is a valid tokio listener");
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let mut sampler = AcceptQueueSampler::new(telemetry, Diagnostics::default());

        // Poll rather than sleep a fixed time. If both handshakes haven't finished in the window,
        // only the `> 1.0` half below is skipped.
        let mut depth = 0;
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            let (d, _limit) = read_listen_queue(&listener).expect("TCP_INFO on a real listener");
            depth = d;
            if depth >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let (queued, limit) = read_listen_queue(&listener).expect("TCP_INFO on a real listener");
        assert_eq!(
            limit, 1,
            "listen(1) is what this socket asked for, and somaxconn cannot \
                              raise it -- only lower it, and never below 1"
        );
        assert!(
            queued <= limit + 1,
            "the kernel admits one connection past its own ceiling and no more, got {queued}"
        );

        sampler.sample_once(&listener);
        let events = Totals::of(registry.drain(0));
        let reported_depth = events
            .gauge("logit.input.accept_queue.depth", &[])
            .expect("the depth should have been gauged");
        let reported_limit = events
            .gauge("logit.input.accept_queue.limit", &[])
            .expect("the ceiling should have been gauged");
        let utilization = events
            .gauge("logit.input.accept_queue.utilization", &[])
            .expect("the utilization should have been gauged");
        assert_eq!(reported_limit, 1.0);
        assert!(
            (utilization - reported_depth / reported_limit).abs() < 1e-9,
            "utilization is exactly depth/limit, unclamped: {utilization} vs \
             {reported_depth}/{reported_limit}"
        );
        if depth >= 2 {
            assert!(
                utilization > 1.0,
                "a listen(1) socket holding two connections is over its ceiling, and the gauge \
                 must say so rather than clamp: {utilization}"
            );
        } else {
            eprintln!(
                "SKIPPED the >1.0 half: this kernel left the accept queue at depth {depth} \
                 within the poll window; the depth bound and the depth/limit identity were still \
                 checked"
            );
        }

        drop(clients);
    }

    /// The queue is sampled **before** the accept, not after it.
    #[tokio::test]
    async fn the_accept_queue_is_sampled_before_the_accept_not_after_it() {
        static SAMPLES: AtomicUsize = AtomicUsize::new(0);
        SAMPLES.store(0, Ordering::SeqCst);

        let listener = TokioTcpListener::bind("127.0.0.1:0").await.expect("should bind loopback");
        let addr = listener.local_addr().expect("a bound listener has an address");
        let mut sampler = AcceptQueueSampler::new(Telemetry::default(), Diagnostics::default());
        sampler.read_queue = |_| {
            SAMPLES.fetch_add(1, Ordering::SeqCst);
            Ok((0, 1))
        };

        let accepting = tokio::spawn(async move {
            sampler.accept_every(&listener, Duration::from_secs(3600)).await
        });

        // Nothing has connected, and an hour's interval cannot tick: a sample here can only be
        // the pre-accept one.
        tokio::time::timeout(Duration::from_secs(5), async {
            while SAMPLES.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the queue must be sampled while accept() is still parked, not after it returns");

        TcpStream::connect(addr).await.expect("loopback connect should succeed");
        accepting
            .await
            .expect("the accept task should not panic")
            .expect("the connection should still be accepted");
    }

    /// An idle listener is still sampled on the interval.
    #[tokio::test]
    async fn an_idle_listener_is_sampled_once_per_interval() {
        static SAMPLES: AtomicUsize = AtomicUsize::new(0);
        SAMPLES.store(0, Ordering::SeqCst);

        let listener = TokioTcpListener::bind("127.0.0.1:0").await.expect("should bind loopback");
        let mut sampler = AcceptQueueSampler::new(Telemetry::default(), Diagnostics::default());
        sampler.read_queue = |_| {
            SAMPLES.fetch_add(1, Ordering::SeqCst);
            Ok((0, 1))
        };

        // Nothing ever connects, so `accept_every` never returns; only the interval can sample.
        tokio::select! {
            accepted = sampler.accept_every(&listener, Duration::from_millis(20)) => {
                panic!("nothing connects, yet accept_every returned {accepted:?}")
            }
            () = wait_until("four samples of an idle listener at a 20ms interval", || {
                SAMPLES.load(Ordering::SeqCst) >= 4
            }) => {}
        }
    }

    /// A steady accept rate faster than the interval does not starve the interval tick
    /// ([`AcceptQueueSampler::tick`]).
    #[tokio::test]
    async fn a_steady_stream_of_accepts_does_not_starve_the_interval_tick() {
        static SAMPLES: AtomicUsize = AtomicUsize::new(0);
        SAMPLES.store(0, Ordering::SeqCst);

        const ACCEPTS: usize = 20;
        const TICK: Duration = Duration::from_millis(20);

        let listener = TokioTcpListener::bind("127.0.0.1:0").await.expect("should bind loopback");
        let addr = listener.local_addr().expect("a bound listener has an address");
        let mut sampler = AcceptQueueSampler::new(Telemetry::default(), Diagnostics::default());
        sampler.read_queue = |_| {
            SAMPLES.fetch_add(1, Ordering::SeqCst);
            Ok((0, 1))
        };

        // Every connection waits in the backlog, so each `accept_every` returns immediately and
        // no single call ever waits a whole interval.
        let mut clients = Vec::new();
        for _ in 0..ACCEPTS {
            clients.push(TcpStream::connect(addr).await.expect("loopback connect"));
        }

        for _ in 0..ACCEPTS {
            let _accepted = sampler
                .accept_every(&listener, TICK)
                .await
                .expect("every queued connection is accepted");
            // Far faster than the interval: a per-turn `sleep` would never come due.
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let samples = SAMPLES.load(Ordering::SeqCst);
        assert!(
            samples > ACCEPTS + 1,
            "{ACCEPTS} accepts spread over ~{}ms must also carry interval ticks -- one sample per \
             accept and no more means the timer was restarted on every loop turn and never came \
             due, got {samples}",
            ACCEPTS * 5
        );
    }

    /// Lowers the process's `RLIMIT_NOFILE` soft limit for as long as it lives and restores the
    /// original on drop.
    #[cfg(target_os = "linux")]
    struct LoweredFdLimit(libc::rlimit);

    #[cfg(target_os = "linux")]
    impl LoweredFdLimit {
        /// Sets the soft limit to the lowest free descriptor number, so every descriptor below it
        /// is in use and the next one the process asks for fails `EMFILE`.
        fn to_the_next_free_descriptor() -> Self {
            use std::os::fd::AsRawFd;
            let next_free = std::fs::File::open("/dev/null").unwrap().as_raw_fd();
            let mut original = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
            // SAFETY: `getrlimit` writes one `rlimit` through a pointer to a live, aligned local.
            assert_eq!(unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut original) }, 0);
            Self::set(libc::rlimit { rlim_cur: next_free as libc::rlim_t, ..original });
            Self(original)
        }

        fn set(limit: libc::rlimit) {
            // SAFETY: `setrlimit` reads one `rlimit` through a pointer to a live, aligned local.
            let rc = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) };
            assert_eq!(rc, 0, "setrlimit(RLIMIT_NOFILE): {}", std::io::Error::last_os_error());
        }
    }

    #[cfg(target_os = "linux")]
    impl Drop for LoweredFdLimit {
        fn drop(&mut self) {
            Self::set(self.0);
        }
    }

    /// A listener that cannot get a descriptor for an accepted connection (`EMFILE`) backs off,
    /// retries, and serves the connection once a descriptor is free again, instead of ending.
    ///
    /// Needs a process to itself: `RLIMIT_NOFILE` is process-wide, so the test runs only under
    /// nextest's process-per-test mode and returns early in libtest's shared process
    /// (`cargo test`, `script/unsafe-check careful`), where lowering the limit would fail other
    /// tests' sockets. `script/unsafe-check`'s `tcp-accept-emfile` scenario covers the same path
    /// through `strace` instead.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_resource_accept_error_backs_off_and_the_listener_keeps_serving() {
        if std::env::var("NEXTEST_EXECUTION_MODE").as_deref() != Ok("process-per-test") {
            eprintln!("skipped: lowers RLIMIT_NOFILE, so it needs nextest's process-per-test mode");
            return;
        }
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let diag = Diagnostics::new("syslog_in");
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener = listener.with_telemetry(telemetry).with_diagnostics(diag.clone());
        let (sink, mut rx) = fanout_into_channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        // Created before the limit drops, so `connect` below needs no new descriptor.
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        let limit = LoweredFdLimit::to_the_next_free_descriptor();
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(sink, shutdown_rx).await });

        let mut client = socket.connect(addr.parse().unwrap()).await.unwrap();
        // Two occurrences: the first `EMFILE`, then the retry after the backoff failing again.
        let started = std::time::Instant::now();
        while diag.occurrences("accept_error") < 2 {
            assert!(
                !handle.is_finished(),
                "the listener ended on a resource accept error: {:?}",
                handle.await
            );
            assert!(started.elapsed() < Duration::from_secs(5), "no retried accept within 5s");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let failures = diag.occurrences("accept_error");
        let backoffs_elapsed =
            started.elapsed().as_millis() / crate::listener::ACCEPT_ERROR_BACKOFF.as_millis() + 2;
        assert!(
            u128::from(failures) <= backoffs_elapsed,
            "{failures} accept failures in {:?}: the loop retried without backing off",
            started.elapsed()
        );
        drop(limit);

        client.write_all(b"<13>after\n").await.unwrap();
        let batch = recv_batch(&mut rx).await;
        assert_eq!(payloads(&batch), vec!["<13>after"]);
        let events = Totals::of(registry.drain(0));
        let resource = events.sum("logit.input.accept.errors", &[("reason", "resource")]);
        assert!(resource >= 2.0, "resource accept errors counted: {resource:?}");
        assert!(!events.has("logit.input.accept.errors", &[("reason", "fatal")]),);

        handle.abort();
    }

    /// An interval `emit` in `serve_connection` that parks on a full downstream past the next
    /// deadline doesn't leave that deadline already due: the read after it resumes must not flush
    /// again at the same instant. `crate::udp`'s
    /// `an_interval_emit_that_parks_past_the_deadline_does_not_flush_once_per_pop_batch` is the
    /// decode-loop twin; this drives the connection over an in-memory duplex stream.
    #[tokio::test(start_paused = true)]
    async fn an_interval_emit_that_parks_past_the_deadline_does_not_flush_once_per_read() {
        const INTERVAL: Duration = Duration::from_millis(100);
        const CYCLES: usize = 20;

        let registry = Registry::new();
        let telemetry = registry.telemetry_for("syslog_in", "syslog_in", "listener");
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let (tx, mut rx) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let connection = tokio::spawn({
            let telemetry = telemetry.clone();
            async move {
                let mut diag = Diagnostics::default();
                serve_connection(
                    server,
                    TestDecoder::new(),
                    Framer::new(FramingMode::Lines { oversize: Oversize::Fatal }, 64 * 1024),
                    TcpListenerConfig {
                        batch_max_events: 10_000,
                        batch_flush_interval: INTERVAL,
                        ..TcpListenerConfig::default()
                    },
                    Duration::from_secs(3600),
                    None,
                    None,
                    Fanout::new(vec![tx]),
                    telemetry,
                    &mut diag,
                    shutdown_rx,
                )
                .await
            }
        });

        let mut flushes = 0.0;
        let mut interval_flushes = |registry: &Registry| {
            flushes += Totals::of(registry.drain(0))
                .sum("logit.component.receive.flushed", &[("reason", "interval")]);
            flushes
        };
        let mut line = 0usize;
        // Off the deadline grid, so a write never shares an instant with a flush.
        tokio::time::sleep(INTERVAL / 2).await;
        for cycle in 0..CYCLES {
            // Five intervals with the consumer stalled: the first flush fills the channel, the
            // next one parks.
            for _ in 0..5 {
                client.write_all(format!("line-{line}\n").as_bytes()).await.unwrap();
                line += 1;
                tokio::time::sleep(INTERVAL).await;
            }
            let before = interval_flushes(&registry);

            tokio::time::timeout(Duration::from_secs(1), rx.recv())
                .await
                .expect("a flush filled the channel during the window")
                .expect("the connection owns the fanout and is still running");
            client.write_all(format!("line-{line}\n").as_bytes()).await.unwrap();
            line += 1;
            // No clock advance: this task stays runnable, so the paused clock stands still.
            for _ in 0..64 {
                tokio::task::yield_now().await;
            }
            assert_eq!(
                interval_flushes(&registry),
                before,
                "cycle {cycle}: the resumed emit must not be followed by another interval flush \
                 at the same instant"
            );
        }
        connection.abort();
    }

    // ---- driver: a reset before the first byte -------------------------------------------------

    /// A one-permit listener, with or without `proxy_protocol:`, and its diagnostics and probe.
    struct OnePermit {
        addr: String,
        rx: mpsc::Receiver<logit_pipeline::Delivered>,
        diag: Diagnostics,
        probe: TelemetryProbe,
        handle: tokio::task::JoinHandle<anyhow::Result<()>>,
    }

    async fn one_permit(proxy_protocol: bool) -> OnePermit {
        let probe = TelemetryProbe::new();
        let diag = Diagnostics::new("lines_in");
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener = listener
            .with_telemetry(probe.telemetry("lines_in", "lines_in", "listener"))
            .with_diagnostics(diag.clone())
            .with_max_connections(1)
            .with_proxy_protocol(proxy_protocol);
        let (sink, rx) = fanout_into_channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            let _shutdown_tx = shutdown_tx;
            listener.run_until_shutdown(sink, shutdown_rx).await
        });
        OnePermit { addr, rx, diag, probe, handle }
    }

    /// Connects, writes `prefix`, and closes with an RST (`SO_LINGER` of zero), as HAProxy ends a
    /// health check.
    async fn connect_and_reset(addr: &str, prefix: &[u8]) {
        let mut client = connect(addr).await;
        client.write_all(prefix).await.unwrap();
        socket2::SockRef::from(&client).set_linger(Some(Duration::ZERO)).unwrap();
        drop(client);
    }

    /// Sends `wire` on new connections until one gets its frame through. With one permit, that
    /// proves every earlier connection's task has finished, diagnostics included: a connection
    /// refused at the cap is closed, and the loop tries again.
    async fn next_connection_delivers(running: &mut OnePermit, wire: &[u8]) -> EventBatch {
        let deadline = tokio::time::Instant::now() + logit_pipeline::test_util::RECV_TIMEOUT;
        loop {
            assert!(tokio::time::Instant::now() < deadline, "no connection got a permit back");
            let mut client = connect(&running.addr).await;
            client.write_all(wire).await.unwrap();
            let mut byte = [0u8; 1];
            tokio::select! {
                delivered = running.rx.recv() => {
                    return logit_pipeline::unwrap_batch(delivered.expect("the listener is running"));
                }
                _ = client.read(&mut byte) => {}
            }
        }
    }

    /// A health check that connects and resets before sending a byte is no fault.
    #[tokio::test]
    async fn a_reset_before_the_first_byte_is_not_a_connection_error() {
        let mut running = one_permit(false).await;
        connect_and_reset(&running.addr, b"").await;
        let batch = next_connection_delivers(&mut running, b"<13>after\n").await;
        assert_eq!(payloads(&batch), vec!["<13>after"]);
        assert_eq!(running.diag.occurrences("connection_error"), 0);
        running.handle.abort();
    }

    /// HAProxy's PROXY-aware health check: an accepted header, then an RST before any payload.
    /// It ends as a reset before the first byte does without the header: no diagnostic, and no
    /// rejection.
    #[tokio::test]
    async fn a_reset_after_an_accepted_header_and_before_the_first_byte_is_not_an_error() {
        for header in
            [v2_header(0x20, 0x00, &[]), b"PROXY TCP4 198.51.100.7 192.0.2.1 1 2\r\n".to_vec()]
        {
            let mut running = one_permit(true).await;
            connect_and_reset(&running.addr, &header).await;
            let batch =
                next_connection_delivers(&mut running, b"PROXY UNKNOWN\r\n<13>after\n").await;
            assert_eq!(payloads(&batch), vec!["<13>after"]);
            assert_eq!(running.diag.occurrences("connection_error"), 0, "after {header:?}");
            assert_eq!(running.diag.occurrences("proxy_header"), 0, "after {header:?}");
            assert_eq!(
                running.probe.sum(PROXY_REJECTED, &[("reason", "proxy_header")]),
                0.0,
                "after {header:?}"
            );
            running.handle.abort();
        }
    }

    /// A reset after payload bytes is a broken connection, diagnosed as before.
    #[tokio::test]
    async fn a_reset_after_payload_bytes_is_still_a_connection_error() {
        for proxy_protocol in [false, true] {
            let mut running = one_permit(proxy_protocol).await;
            let header: &[u8] = if proxy_protocol { b"PROXY UNKNOWN\r\n" } else { b"" };
            // A partial frame, so the reset lands while the connection is mid-message.
            connect_and_reset(&running.addr, &[header, b"<13>partial"].concat()).await;
            let wire = [header, b"<13>after\n"].concat();
            next_connection_delivers(&mut running, &wire).await;
            assert_eq!(
                running.diag.occurrences("connection_error"),
                1,
                "proxy_protocol: {proxy_protocol}"
            );
            running.handle.abort();
        }
    }

    // ---- driver: PROXY protocol ---------------------------------------------------------------

    const PROXY_REJECTED: &str = "logit.input.connections.rejected";

    /// A running `proxy_protocol: true` listener with `peer:` as given, and a probe on its
    /// telemetry.
    struct Proxied {
        addr: String,
        rx: mpsc::Receiver<logit_pipeline::Delivered>,
        probe: TelemetryProbe,
        handle: tokio::task::JoinHandle<anyhow::Result<()>>,
    }

    async fn proxied(
        peer: bool,
        configure: impl FnOnce(TcpListener<TestDecoder>) -> TcpListener<TestDecoder>,
    ) -> Proxied {
        let probe = TelemetryProbe::new();
        let (addr, listener) = bound_listener(one_per_frame()).await;
        let mut listener = configure(
            listener
                .with_telemetry(probe.telemetry("lines_in", "lines_in", "listener"))
                .with_peer(peer)
                .with_proxy_protocol(true),
        );
        let (sink, rx) = fanout_into_channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            let _shutdown_tx = shutdown_tx;
            listener.run_until_shutdown(sink, shutdown_rx).await
        });
        Proxied { addr, rx, probe, handle }
    }

    fn str_attr<'a>(event: &'a Event, key: &str) -> Option<&'a str> {
        event.attributes.get(key).and_then(Value::as_str)
    }

    /// A v2 header with command `ver_cmd`, family and transport `fam`, and `body` after the
    /// fixed part.
    fn v2_header(ver_cmd: u8, fam: u8, body: &[u8]) -> Vec<u8> {
        let mut out = proxy::V2_SIGNATURE.to_vec();
        out.extend_from_slice(&[ver_cmd, fam]);
        out.extend_from_slice(&u16::try_from(body.len()).unwrap().to_be_bytes());
        out.extend_from_slice(body);
        out
    }

    /// A v2 `PROXY` over TCP/IPv4 from 203.0.113.5:41000, with an `AUTHORITY` TLV behind the
    /// address block.
    fn v2_ipv4_with_tlv() -> Vec<u8> {
        let mut body = vec![203, 0, 113, 5, 192, 0, 2, 1];
        body.extend_from_slice(&41000u16.to_be_bytes());
        body.extend_from_slice(&5170u16.to_be_bytes());
        body.extend_from_slice(&[0x02, 0x00, 0x0B]);
        body.extend_from_slice(b"example.com");
        v2_header(0x21, 0x11, &body)
    }

    /// Waits for `n` connections refused for their PROXY header.
    async fn wait_for_proxy_rejections(probe: &mut TelemetryProbe, n: f64) {
        wait_until("the proxy_header rejection count", || {
            probe.sum(PROXY_REJECTED, &[("reason", "proxy_header")]) == n
        })
        .await;
    }

    /// A v1 header and the first frame in one write: the frame reaches the framer intact.
    #[tokio::test]
    async fn a_v1_tcp4_header_stamps_the_origin_and_leaves_the_payload_intact() {
        let mut running = proxied(false, |l| l).await;
        let mut client = connect(&running.addr).await;
        client
            .write_all(b"PROXY TCP4 198.51.100.7 192.0.2.1 40000 5170\r\n<13>first\n<13>second\n")
            .await
            .unwrap();

        let first = recv_batch(&mut running.rx).await;
        assert_eq!(payloads(&first), vec!["<13>first"]);
        let event = &first.events[0];
        assert_eq!(str_attr(event, "client.address"), Some("198.51.100.7"));
        assert_eq!(event.attributes.get("client.port"), Some(&Value::I64(40000)));
        assert_eq!(event.attributes.get("network.peer.address"), None, "peer: is off");
        assert_eq!(payloads(&recv_batch(&mut running.rx).await), vec!["<13>second"]);
        running.handle.abort();
    }

    #[tokio::test]
    async fn a_v1_tcp6_header_stamps_the_origin() {
        let mut running = proxied(false, |l| l).await;
        let mut client = connect(&running.addr).await;
        client.write_all(b"PROXY TCP6 2001:db8::7 2001:db8::1 65535 5170\r\n").await.unwrap();
        client.write_all(b"<13>hello\n").await.unwrap();

        let batch = recv_batch(&mut running.rx).await;
        assert_eq!(payloads(&batch), vec!["<13>hello"]);
        assert_eq!(str_attr(&batch.events[0], "client.address"), Some("2001:db8::7"));
        assert_eq!(batch.events[0].attributes.get("client.port"), Some(&Value::I64(65535)));
        running.handle.abort();
    }

    /// A header split across writes is read in pieces, consuming only header bytes each time.
    #[tokio::test]
    async fn a_v1_header_split_across_writes_is_assembled() {
        let mut running = proxied(false, |l| l).await;
        let mut client = connect(&running.addr).await;
        client.set_nodelay(true).unwrap();
        for piece in
            [&b"PROXY TCP4 198.5"[..], b"1.100.7 192.0.2.1 4", b"0000 5170\r", b"\n<13>a\n"]
        {
            client.write_all(piece).await.unwrap();
        }
        let batch = recv_batch(&mut running.rx).await;
        assert_eq!(payloads(&batch), vec!["<13>a"]);
        assert_eq!(str_attr(&batch.events[0], "client.address"), Some("198.51.100.7"));
        running.handle.abort();
    }

    /// The TLV is skipped and the frame behind it, sent in the same write, decodes.
    #[tokio::test]
    async fn a_v2_ipv4_header_with_a_tlv_stamps_the_origin() {
        let mut running = proxied(false, |l| l).await;
        let mut client = connect(&running.addr).await;
        let mut wire = v2_ipv4_with_tlv();
        wire.extend_from_slice(b"<13>behind a tlv\n");
        client.write_all(&wire).await.unwrap();

        let batch = recv_batch(&mut running.rx).await;
        assert_eq!(payloads(&batch), vec!["<13>behind a tlv"]);
        assert_eq!(str_attr(&batch.events[0], "client.address"), Some("203.0.113.5"));
        assert_eq!(batch.events[0].attributes.get("client.port"), Some(&Value::I64(41000)));
        running.handle.abort();
    }

    /// A proxy's own health check: accepted, and nothing is stamped.
    #[tokio::test]
    async fn a_v2_local_header_is_accepted_with_no_client_attributes() {
        let mut running = proxied(false, |l| l).await;
        let mut client = connect(&running.addr).await;
        let mut wire = v2_header(0x20, 0x00, &[]);
        wire.extend_from_slice(b"<13>health\n");
        client.write_all(&wire).await.unwrap();

        let batch = recv_batch(&mut running.rx).await;
        assert_eq!(payloads(&batch), vec!["<13>health"]);
        assert_eq!(batch.events[0].attributes.get("client.address"), None);
        assert_eq!(batch.events[0].attributes.get("client.port"), None);
        assert_eq!(running.probe.sum(PROXY_REJECTED, &[("reason", "proxy_header")]), 0.0);
        running.handle.abort();
    }

    /// `peer:` reports the proxy, the socket peer; the header names the client.
    #[tokio::test]
    async fn peer_reports_the_proxy_and_the_header_reports_the_client() {
        let mut running = proxied(true, |l| l).await;
        let mut client = connect(&running.addr).await;
        let local_port = client.local_addr().unwrap().port();
        let mut wire = v2_ipv4_with_tlv();
        wire.extend_from_slice(b"<13>both\n");
        client.write_all(&wire).await.unwrap();

        let batch = recv_batch(&mut running.rx).await;
        let event = &batch.events[0];
        assert_eq!(str_attr(event, "network.peer.address"), Some("127.0.0.1"));
        assert_eq!(
            event.attributes.get("network.peer.port"),
            Some(&Value::I64(i64::from(local_port)))
        );
        assert_eq!(str_attr(event, "client.address"), Some("203.0.113.5"));
        assert_eq!(event.attributes.get("client.port"), Some(&Value::I64(41000)));
        running.handle.abort();
    }

    /// No header: the connection is closed and counted, and its bytes are never decoded.
    #[tokio::test]
    async fn a_connection_without_a_header_is_rejected_and_counted() {
        let mut running = proxied(false, |l| l).await;
        let mut bare = connect(&running.addr).await;
        bare.write_all(b"<13>no header\n").await.unwrap();
        expect_closed(&mut bare, "a connection with no PROXY header").await;
        wait_for_proxy_rejections(&mut running.probe, 1.0).await;

        let mut good = connect(&running.addr).await;
        good.write_all(b"PROXY UNKNOWN\r\n<13>after\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut running.rx).await), vec!["<13>after"]);
        running.handle.abort();
    }

    #[tokio::test]
    async fn a_malformed_header_is_rejected_and_counted() {
        let mut running = proxied(false, |l| l).await;
        for wire in [
            b"PROXY TCP4 198.51.100.7 192.0.2.1 99999 5170\r\n<13>x\n".to_vec(),
            v2_header(0x22, 0x11, &[0; 12]),
            v2_header(0x21, 0x11, &[0; 4]),
        ] {
            let mut client = connect(&running.addr).await;
            client.write_all(&wire).await.unwrap();
            expect_closed(&mut client, "a connection with a malformed PROXY header").await;
        }
        wait_for_proxy_rejections(&mut running.probe, 3.0).await;
        running.handle.abort();
    }

    /// A header that stalls partway is cut by `handshake_timeout` and gives its permit back.
    #[tokio::test]
    async fn a_slow_header_is_cut_by_the_handshake_timeout() {
        let mut running = proxied(false, |l| {
            l.with_handshake_timeout(Duration::from_millis(50)).with_max_connections(1)
        })
        .await;
        let mut slow = connect(&running.addr).await;
        slow.write_all(b"PROXY TCP4 198.51").await.unwrap();
        expect_closed(&mut slow, "a connection whose PROXY header stalled").await;
        wait_for_proxy_rejections(&mut running.probe, 1.0).await;

        let mut next = connect(&running.addr).await;
        next.write_all(b"PROXY UNKNOWN\r\n<13>permit came back\n").await.unwrap();
        assert_eq!(payloads(&recv_batch(&mut running.rx).await), vec!["<13>permit came back"]);
        drop(slow);
        running.handle.abort();
    }

    /// The header is read off the raw stream, and the TLS handshake runs on what follows it.
    #[tokio::test]
    async fn tls_runs_behind_the_header() {
        let mut running = proxied(false, |l| {
            l.with_tls(
                &test_tls_settings(None),
                &testdata_dir(),
                &logit_pipeline::tls::TlsReloader::new(),
            )
            .unwrap()
        })
        .await;
        let mut stream = connect(&running.addr).await;
        stream.write_all(&v2_ipv4_with_tlv()).await.unwrap();
        let connector = tls_connector("ca.pem", None).await;
        let mut client =
            tokio::time::timeout(Duration::from_secs(5), connector.connect(server_name(), stream))
                .await
                .expect("the TLS handshake should complete within 5s")
                .expect("the TLS handshake should succeed");
        client.write_all(b"<13>over tls\n").await.unwrap();
        client.flush().await.unwrap();

        let batch = recv_batch(&mut running.rx).await;
        assert_eq!(payloads(&batch), vec!["<13>over tls"]);
        assert_eq!(str_attr(&batch.events[0], "client.address"), Some("203.0.113.5"));
        running.handle.abort();
    }

    /// A Unix socket has no network proxy in front of it; bind refuses the option.
    #[tokio::test]
    async fn a_unix_listener_refuses_proxy_protocol_at_bind() {
        let path = logit_pipeline::test_util::scratch_dir("tcp-proxy-unix").join("s.sock");
        let mut listener =
            TcpListener::unix("lines_in", path, 0o600, TestDecoder::new(), one_per_frame())
                .with_proxy_protocol(true);
        let err = listener.bind().await.expect_err("a Unix socket takes no PROXY header");
        assert!(err.to_string().contains("'proxy_protocol:' needs 'transport: tcp'"), "{err}");
    }
}
