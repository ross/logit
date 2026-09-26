//! The shared UDP listener driver: read/decode decoupling plus datagram-\>batch assembly
//! (`docs/adr/decoupled-listener-io.md`). `statsd_in`, `syslog_in`, `graphite_in`, and
//! `collectd_in` are thin wrappers over [`UdpListener<D>`], which is generic over the one thing
//! they differ in: the decoder.
//!
//! **Crate placement.** The generic queue (`logit_pipeline::BoundedQueue`) and the batch
//! accumulator (`logit_pipeline::BatchAccumulator`) are transport-agnostic and live in
//! `logit-pipeline`. The socket bind, `SO_RCVBUF`, and the receive syscall are protocol-impl
//! code, per `docs/design/pipeline-graph.md`'s crate-layout rule, so `socket2` is a
//! `logit-inputs` dependency only, never `logit-pipeline`'s.
//!
//! **Multicast comes with the driver.** A `bind:` whose address is a multicast group makes
//! [`bind_one`] set `SO_REUSEADDR`, bind the unspecified address on that port, and join the
//! group, so every UDP listener (`collectd_in`'s standard group is `239.192.74.66`) gets it
//! without a field of its own. That function's doc says why each step is needed.
//!
//! **A Unix datagram socket is a datagram socket too.** [`UdpListener::unix`] binds a
//! `SOCK_DGRAM` Unix socket (`statsd_in`'s `transport: unix`, the Datadog Agent's
//! `dogstatsd_socket`) and drives it through the same read loop, queue, and decode loop, over the
//! [`DatagramSocket`] seam. What differs is the bind ([`crate::unix`]) and what the kernel counters
//! mean: on `AF_UNIX` a full receive queue makes the *sender's* `send` block or fail with `EAGAIN`
//! rather than dropping in the kernel, so `logit.input.kernel.drops` stays at zero there and the
//! loss, if any, is the client's to count (`net/unix/af_unix.c`, `unix_dgram_sendmsg`).
//!
//! **Not used by [`crate::internal::InternalInput`].** `internal` has no socket, no datagram, and
//! no `receive:` block; don't generalize this module toward it.

use bytes::Bytes;
use logit_core::{Diagnostics, Event, EventBatch, Telemetry};
use logit_pipeline::sockstat;
use logit_pipeline::{BatchAccumulator, FlushReason, Input};
use logit_pipeline::{
    BoundedQueue, CountedDrain, Fanout, OverflowPolicy, QueueConfig, QueueMetrics, Queued,
};
use logit_proto::Decoder;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

/// One datagram in flight between the read half and the decode half, with the instant the read
/// half took it off the socket -- see [`logit_proto::Decoder::decode_into`]'s `received_at`.
pub struct Datagram {
    pub bytes: Bytes,
    pub received_at: i64,
}

impl Queued for Datagram {
    /// The datagram's payload plus this struct's inline footprint: an admission-control
    /// estimate, not an allocator figure (`docs/design/memory.md` §5).
    fn weight(&self) -> u64 {
        (self.bytes.len() + std::mem::size_of::<Self>()) as u64
    }
    /// Bytes, not "1", so `logit.component.bytes.dropped` reports the size of what was lost, the
    /// unit an operator sizing `receive.max_bytes` reasons in.
    fn units(&self) -> u64 {
        self.bytes.len() as u64
    }
}

pub static RECEIVE_QUEUE_METRICS: QueueMetrics = QueueMetrics {
    depth: "logit.component.receive.datagrams",
    bytes: "logit.component.receive.bytes",
    utilization: "logit.component.receive.utilization",
    push_blocked: "logit.component.receive.push.blocked.duration",
    items_dropped: "logit.component.datagrams.dropped",
    units_dropped: "logit.component.bytes.dropped",
};

pub type ReceiveQueue = BoundedQueue<Datagram>;

/// [`UdpListener`]'s runtime knobs, built from a component's `logit_config::ReceiveConfig`
/// (`docs/adr/decoupled-listener-io.md`) or directly by a test.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UdpListenerConfig {
    pub max_datagrams: usize,
    pub max_bytes: u64,
    pub overflow: OverflowPolicy,
    /// `SO_RCVBUF`, requested at bind. `None` leaves the kernel default alone.
    pub receive_buffer_bytes: Option<u64>,
    /// Events to accumulate across datagrams before one `Fanout::send`. `1` means one send per
    /// datagram (`BatchAccumulator::absorb`).
    pub batch_max_events: usize,
    pub batch_max_bytes: u64,
    /// `Duration::ZERO` disables the flush timer entirely; bounds are then the only trigger.
    pub batch_flush_interval: Duration,
    /// Datagrams one `recvmmsg(2)` call may return on Linux, and how many [`decode_loop`] takes
    /// off the [`ReceiveQueue`] per `pop_many`. `logit_config::ReceiveConfig::read_batch` has the
    /// operator-facing account, including the slab cost and the wider shutdown loss. Clamped into
    /// `1..=MAX_READ_BATCH` by [`UdpListenerConfig::read_batch`]; graph rules 18 and 57 reject
    /// out-of-range values first.
    pub read_batch: usize,
}

/// Matches `logit_config::ReceiveConfig`'s defaults; `docs/adr/decoupled-listener-io.md`
/// justifies the numbers.
impl Default for UdpListenerConfig {
    fn default() -> Self {
        Self {
            max_datagrams: 10_000,
            max_bytes: 32 * 1024 * 1024,
            overflow: OverflowPolicy::DropOldest,
            receive_buffer_bytes: None,
            batch_max_events: 1_000,
            batch_max_bytes: 1024 * 1024,
            batch_flush_interval: Duration::from_millis(100),
            read_batch: 64,
        }
    }
}

impl UdpListenerConfig {
    /// `read_batch`, clamped into the range the read and pop paths can honour.
    ///
    /// Graph rules 18 and 57 reject `0` and anything above [`MAX_READ_BATCH`], so in a real
    /// pipeline this never fires. It guards a directly-built config: `recvmmsg` with a `vlen` of 0
    /// reads nothing, forever, and `pop_many` `debug_assert`s `max > 0`.
    fn read_batch(&self) -> usize {
        self.read_batch.clamp(1, MAX_READ_BATCH)
    }

    fn queue_config(&self) -> QueueConfig {
        QueueConfig {
            max_items: self.max_datagrams,
            max_weight: self.max_bytes,
            overflow: self.overflow,
        }
    }

    fn batching(&self) -> BatchingConfig {
        BatchingConfig {
            max_events: self.batch_max_events,
            max_bytes: self.batch_max_bytes,
            flush_interval: self.batch_flush_interval,
            pop_batch: self.read_batch(),
        }
    }
}

/// The largest payload an **IPv4** UDP datagram can carry: 65,535 minus the 8-byte UDP header.
/// [`BatchReader`] sizes each of its `read_batch` slots to it.
///
/// **It is not the largest payload a UDP datagram can carry.** IPv6's payload-length field
/// excludes the 40-byte header, so an IPv6 UDP datagram may carry up to 65,527 bytes, 20 more than
/// this. The kernel copies such a datagram up to this bound and discards the rest; [`BatchReader`]
/// sees that happen (`MSG_TRUNC` in the returned `msg_flags`) and counts it as
/// `logit.input.datagrams.truncated`. Jumbograms (RFC 2675, past 65,535) are a separate thing, and
/// no mainstream kernel's UDP path supports them.
const MAX_DATAGRAM_BYTES: usize = 65_507;

/// The largest `receive.read_batch` this driver accepts. Graph rule 57 rejects a larger value
/// (`logit_config::MAX_READ_BATCH`, the same number, duplicated because `logit-inputs` doesn't
/// depend on `logit-config`).
///
/// **The number is `UIO_MAXIOV`'s, but the limit is ours, not the kernel's.** `UIO_MAXIOV` does
/// not bound `recvmmsg`'s `vlen`. It bounds `msg_iovlen` within one `msghdr` (`__copy_msghdr` in
/// `net/socket.c` returns `-EMSGSIZE` above it), and [`build_headers`] sets `msg_iovlen` to 1. The
/// receive side has no `vlen` clamp at all: `do_recvmmsg`'s loop is a plain
/// `while (datagrams < vlen)`, and the only `UIO_MAXIOV` clamp on a `vlen` is `__sys_sendmmsg`'s,
/// on the send side. What 1024 bounds is this crate's own two costs: the
/// `vlen * MAX_DATAGRAM_BYTES` slab [`BatchReader::new`] reserves (67 MB of address space at this
/// ceiling), and how many datagrams a cancelled `push_many` can discard on the shutdown path. 1024
/// is a round number past any measured plateau (ADR `udp-intake-batching-and-socket-visibility`'s
/// sweep), not an ABI boundary.
pub const MAX_READ_BATCH: usize = 1024;

/// What `decode_loop` needs to build and drive a [`BatchAccumulator`]; split out from
/// [`UdpListenerConfig`] to keep `decode_loop`'s parameter count down.
#[derive(Debug, Clone, Copy)]
struct BatchingConfig {
    max_events: usize,
    max_bytes: u64,
    flush_interval: Duration,
    /// [`UdpListenerConfig::read_batch`], carried to `decode_loop`'s `pop_many` so one setting
    /// governs both ends of the [`ReceiveQueue`]. Not a `BatchAccumulator` knob.
    pop_batch: usize,
}

/// A tokio datagram socket the read half can drive: [`tokio::net::UdpSocket`], or
/// [`tokio::net::UnixDatagram`] for a Unix datagram listener. The read path needs only readiness
/// plus a raw-fd syscall ([`Self::async_io`], `recvmmsg` on Linux), the descriptor for the kernel
/// counters, and a name for its errors; both tokio types have the first two with identical
/// signatures, so this trait only forwards.
pub(crate) trait DatagramSocket: std::os::fd::AsRawFd + Send + Sync {
    /// Waits for `interest` and calls `f`, retrying on `WouldBlock`: tokio's own `async_io`.
    fn async_io<R: Send>(
        &self,
        interest: tokio::io::Interest,
        f: impl FnMut() -> std::io::Result<R> + Send,
    ) -> impl Future<Output = std::io::Result<R>> + Send;

    /// One datagram into `buf`, for the non-Linux reader.
    #[cfg(not(target_os = "linux"))]
    fn recv(&self, buf: &mut [u8]) -> impl Future<Output = std::io::Result<usize>> + Send;

    /// The bound address or path, for [`describe_read_failure`].
    fn describe_local(&self) -> String;
}

impl DatagramSocket for tokio::net::UdpSocket {
    fn async_io<R: Send>(
        &self,
        interest: tokio::io::Interest,
        f: impl FnMut() -> std::io::Result<R> + Send,
    ) -> impl Future<Output = std::io::Result<R>> + Send {
        tokio::net::UdpSocket::async_io(self, interest, f)
    }

    #[cfg(not(target_os = "linux"))]
    fn recv(&self, buf: &mut [u8]) -> impl Future<Output = std::io::Result<usize>> + Send {
        async move { self.recv_from(buf).await.map(|(n, _peer)| n) }
    }

    fn describe_local(&self) -> String {
        match self.local_addr() {
            Ok(addr) => addr.to_string(),
            Err(_) => "an unknown address".to_string(),
        }
    }
}

impl DatagramSocket for tokio::net::UnixDatagram {
    fn async_io<R: Send>(
        &self,
        interest: tokio::io::Interest,
        f: impl FnMut() -> std::io::Result<R> + Send,
    ) -> impl Future<Output = std::io::Result<R>> + Send {
        tokio::net::UnixDatagram::async_io(self, interest, f)
    }

    #[cfg(not(target_os = "linux"))]
    fn recv(&self, buf: &mut [u8]) -> impl Future<Output = std::io::Result<usize>> + Send {
        tokio::net::UnixDatagram::recv(self, buf)
    }

    fn describe_local(&self) -> String {
        match self.local_addr().ok().and_then(|addr| addr.as_pathname().map(Path::to_path_buf)) {
            Some(path) => path.display().to_string(),
            None => "an unnamed Unix socket".to_string(),
        }
    }
}

/// Where a [`UdpListener`] binds: an IP `host:port`, or a Unix datagram socket path.
enum BindTarget {
    Ip(String),
    /// `kind` names the component in a bind error; `mode` is the socket file's mode after bind.
    Unix {
        path: PathBuf,
        mode: u32,
        kind: &'static str,
    },
}

/// The bound socket, per [`BindTarget`].
enum BoundSocket {
    Udp(tokio::net::UdpSocket),
    Unix(tokio::net::UnixDatagram),
}

/// The read/decode split every datagram listener reduces to (`docs/adr/decoupled-listener-io.md`),
/// generic over the decoder. UDP by default; [`Self::unix`] for a Unix datagram socket.
pub struct UdpListener<D: Decoder + Send> {
    target: BindTarget,
    decoder: D,
    config: UdpListenerConfig,
    diag: Diagnostics,
    telemetry: Telemetry,
    /// Set by [`Input::bind`], taken by [`Input::run_until_shutdown`]. `None` after a run, so a
    /// second run rebinds.
    socket: Option<BoundSocket>,
}

impl<D: Decoder + Send> UdpListener<D> {
    pub fn new(bind: impl Into<String>, decoder: D, config: UdpListenerConfig) -> Self {
        Self::with_target(BindTarget::Ip(bind.into()), decoder, config)
    }

    /// A listener on a Unix datagram socket at `path`, made mode `mode` once bound. `kind` names
    /// the component in a bind error. Path handling is [`crate::unix`]'s.
    pub fn unix(
        kind: &'static str,
        path: impl Into<PathBuf>,
        mode: u32,
        decoder: D,
        config: UdpListenerConfig,
    ) -> Self {
        Self::with_target(BindTarget::Unix { path: path.into(), mode, kind }, decoder, config)
    }

    fn with_target(target: BindTarget, decoder: D, config: UdpListenerConfig) -> Self {
        Self {
            target,
            decoder,
            config,
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
            socket: None,
        }
    }

    /// The bound address once [`Input::bind`] has run, so a test can learn the OS-assigned port
    /// without a bind-drop-rebind race. `None` on a Unix socket; see [`Self::socket_path`].
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        match self.socket.as_ref()? {
            BoundSocket::Udp(socket) => socket.local_addr().ok(),
            BoundSocket::Unix(_) => None,
        }
    }

    /// The configured socket path of a Unix datagram listener, bound or not; `None` for UDP.
    pub fn socket_path(&self) -> Option<&Path> {
        match &self.target {
            BindTarget::Unix { path, .. } => Some(path),
            BindTarget::Ip(_) => None,
        }
    }

    /// Sets this listener's own diagnostics, the ones behind `decode_loop`'s `bad_datagram`
    /// warning.
    ///
    /// Does not reach the decoder's diagnostics (a decoder's finer-grained `bad_line`, say):
    /// [`Decoder`] has no `with_diagnostics` to call. A wrapper that knows its concrete decoder
    /// must also set them through [`Self::map_decoder`].
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Applies `f` to the wrapped decoder, so a wrapper that knows the concrete decoder type can
    /// chain its consuming builder methods (`with_diagnostics`, say).
    pub fn map_decoder(mut self, f: impl FnOnce(D) -> D) -> Self {
        self.decoder = f(self.decoder);
        self
    }

    /// Overrides the queue/batching/shutdown-grace knobs a `receive:` block sets. Defaults to
    /// [`UdpListenerConfig::default`].
    pub fn with_config(mut self, config: UdpListenerConfig) -> Self {
        self.config = config;
        self
    }

    /// The configured knobs, for `logit-cli::pipeline`'s `build_spec` wiring tests.
    pub fn config(&self) -> UdpListenerConfig {
        self.config
    }

    /// This listener's own diagnostics. With [`Self::decoder`], lets a wrapper's test prove its
    /// `with_diagnostics` set both halves.
    #[cfg(test)]
    pub(crate) fn diag(&self) -> &Diagnostics {
        &self.diag
    }

    /// The wrapped decoder; see [`Self::diag`].
    #[cfg(test)]
    pub(crate) fn decoder(&self) -> &D {
        &self.decoder
    }
}

#[async_trait::async_trait]
impl<D: Decoder + Send> Input for UdpListener<D> {
    async fn bind(&mut self) -> anyhow::Result<()> {
        if self.socket.is_some() {
            return Ok(()); // idempotent, per `Input::bind`'s contract
        }
        let bind = match &self.target {
            BindTarget::Ip(bind) => bind,
            BindTarget::Unix { path, mode, kind } => {
                let socket = crate::unix::bind_datagram(kind, path, *mode)?;
                finish_unix_bind(
                    &socket,
                    self.config.receive_buffer_bytes,
                    &self.telemetry,
                    &mut self.diag,
                )?;
                self.diag.info("bound", format_args!("listening on {}", path.display()));
                self.socket = Some(BoundSocket::Unix(socket));
                return Ok(());
            }
        };
        let (socket, multicast_group) =
            bind_socket(bind, self.config.receive_buffer_bytes, &self.telemetry, &mut self.diag)
                .await?;
        match multicast_group {
            Some(group) => self.diag.info(
                "bound",
                format_args!(
                    "listening on {bind} -- joined multicast group {group} on the default interface"
                ),
            ),
            None => self.diag.info("bound", format_args!("listening on {bind}")),
        }
        self.socket = Some(BoundSocket::Udp(socket));
        Ok(())
    }

    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        // Unused in production: `run_input` always calls `run_until_shutdown`. The trait requires
        // it. `_tx` outlives the run, so this shutdown signal never fires.
        let (_tx, rx) = watch::channel(false);
        self.run_until_shutdown(sink, rx).await
    }

    async fn run_until_shutdown(
        &mut self,
        sink: Fanout,
        shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        self.bind().await?;
        let socket = self.socket.take().expect("bind() leaves a socket behind");
        match &socket {
            BoundSocket::Udp(socket) => self.drive(socket, sink, shutdown).await,
            BoundSocket::Unix(socket) => self.drive(socket, sink, shutdown).await,
        }
    }
}

impl<D: Decoder + Send> UdpListener<D> {
    /// [`Input::run_until_shutdown`]'s body over either socket family.
    async fn drive<S: DatagramSocket>(
        &mut self,
        socket: &S,
        sink: Fanout,
        shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        let queue = Arc::new(BoundedQueue::with_metrics(
            self.config.queue_config(),
            &RECEIVE_QUEUE_METRICS,
            self.telemetry.clone(),
        ));
        // Declared before `read` and `decode`, so it drops after both of their futures: locals
        // drop in reverse order.
        let _residual = ResidualOnDrop {
            queue: Arc::clone(&queue),
            telemetry: &self.telemetry,
            diag: &self.diag,
        };

        let mut read = Box::pin(read_loop_sampled(
            socket,
            Arc::clone(&queue),
            self.telemetry.clone(),
            self.diag.clone(),
            shutdown,
            self.config.read_batch(),
        ));
        let mut decode = Box::pin(decode_loop(
            &mut self.decoder,
            Arc::clone(&queue),
            sink,
            self.config.batching(),
            self.telemetry.clone(),
            self.diag.clone(),
        ));

        // Only `read` can finish on its own (a fatal socket error, or `shutdown`), and either way
        // it closes `queue` first (`read_loop`'s doc; `read_loop_sampled` forwards its result
        // unchanged). That lets `decode`'s `pop_many` see "closed and empty" and return, so once
        // `read` is done, `decode` is driven to completion: it drains what `read` queued and
        // flushes its accumulator.
        //
        // The `Option` guards the one case `select!` can't rule out: `decode` finishing first.
        // Only `read_loop` closes `queue` while `drive` runs (the residual guard closes it again on
        // the way out), so today that can't happen, but polling `decode` again
        // after it resolved would be the double-poll hazard `docs/adr/decoupled-listener-io.md`
        // calls out; that branch awaits `read` instead.
        let already_finished = tokio::select! {
            result = &mut read => Some(result),
            () = &mut decode => None,
        };
        match already_finished {
            Some(result) => {
                decode.await;
                result
            }
            None => read.await,
        }
    }
}

/// Counts what a UDP listener's [`ReceiveQueue`] still holds once both halves are gone, as
/// `logit.component.datagrams.dropped`/`bytes.dropped{reason="shutdown"}`, and closes the queue.
/// On a normal return `decode_loop` has drained the queue and this counts nothing; it counts only
/// when `run_input`'s grace backstop drops [`UdpListener::drive`] with datagrams still queued.
///
/// Takes the queue lock from `Drop`, which is safe here: by the time this drops, the `read` and
/// `decode` futures, the queue's only other users, are gone; no lock site holds the mutex across
/// an await; and every lock site swallows poisoning. Silent and lock-only when the queue is empty.
struct ResidualOnDrop<'a> {
    queue: Arc<ReceiveQueue>,
    telemetry: &'a Telemetry,
    diag: &'a Diagnostics,
}

impl Drop for ResidualOnDrop<'_> {
    fn drop(&mut self) {
        self.queue.close();
        let dropped = count_shutdown_drops(self.queue.take_all().into_iter(), self.telemetry);
        if dropped > 0 {
            self.diag.warn(format_args!(
                "{dropped} datagram(s) still in the receive queue when this listener was stopped \
                 at its shutdown grace, undecoded"
            ));
        }
    }
}

/// Resolves `bind` and binds a UDP socket to it, applying `receive_buffer_bytes` if given.
/// Returns the socket and, when `bind` named a multicast group, the group joined (for the `bound`
/// info line; [`bind_one`] says what a multicast bind does differently).
///
/// - **Resolves asynchronously.** `std::net::ToSocketAddrs` makes a synchronous `getaddrinfo`
///   call that would block a tokio worker for as long as a hostname takes to resolve;
///   [`tokio::net::lookup_host`] runs it on the blocking pool.
/// - **Tries every resolved address, not just the first.** A hostname with both an AAAA and an A
///   record must fall through to a later candidate when an earlier one can't bind (its address
///   family disabled, say), `std`/`tokio`'s own `bind` convention.
async fn bind_socket(
    bind: &str,
    receive_buffer_bytes: Option<u64>,
    telemetry: &Telemetry,
    diag: &mut Diagnostics,
) -> anyhow::Result<(tokio::net::UdpSocket, Option<std::net::IpAddr>)> {
    use anyhow::Context;

    let addrs: Vec<std::net::SocketAddr> = tokio::net::lookup_host(bind)
        .await
        .with_context(|| format!("resolving bind address '{bind}'"))?
        .collect();
    bind_first_available(&addrs, receive_buffer_bytes, telemetry, diag)
        .with_context(|| format!("binding to '{bind}'"))
}

/// Tries every address in `addrs` in turn, returning the first successful bind. Split out from
/// [`bind_socket`] so the fallthrough is testable against a hand-built address list, with no
/// hostname that resolves to several addresses.
fn bind_first_available(
    addrs: &[std::net::SocketAddr],
    receive_buffer_bytes: Option<u64>,
    telemetry: &Telemetry,
    diag: &mut Diagnostics,
) -> anyhow::Result<(tokio::net::UdpSocket, Option<std::net::IpAddr>)> {
    let mut last_err: Option<anyhow::Error> = None;
    for &addr in addrs {
        match bind_one(addr, receive_buffer_bytes) {
            Ok(bound) => {
                let socket = finish_bind(bound.socket, receive_buffer_bytes, telemetry, diag)?;
                return Ok((socket, bound.multicast_group));
            }
            Err(err) => last_err = Some(err),
        }
    }
    match last_err {
        Some(err) => Err(err),
        None => anyhow::bail!("resolved to no addresses"),
    }
}

/// One bound socket, plus the multicast group it joined if its address was one.
struct Bound {
    socket: socket2::Socket,
    multicast_group: Option<std::net::IpAddr>,
}

/// Creates and binds one UDP socket to `addr`, the per-candidate half of `bind_socket`'s loop.
/// Synchronous: socket syscalls only, no I/O wait.
///
/// **A multicast `addr` is bound differently.** No interface owns a group address (`224.0.0.0/4`,
/// `ff00::/8`; collectd's defaults are `239.192.74.66` and `ff18::efc0:4a42`), so receiving on one
/// takes three steps:
///
/// 1. `SO_REUSEADDR`, so several processes on the host can subscribe to the same group and port.
/// 2. A bind to the unspecified address on that port: the group isn't bindable everywhere, and
///    binding it wouldn't subscribe to anything.
/// 3. `IP_ADD_MEMBERSHIP`/`IPV6_JOIN_GROUP` on the default interface (`INADDR_ANY`/index `0`), so
///    the kernel's multicast routing picks the interface rather than this listener guessing.
///
/// A failed join is a hard error: a listener that bound but never joined would look healthy and
/// receive nothing, forever.
///
/// **Three things this function never does.** Each is one line away and would break something
/// several files from here.
///
/// - **Never `connect(2)`.** `udp_err` (`net/ipv4/udp.c`; `udpv6_err` in `net/ipv6/udp.c` is
///   identical) gates ICMP error delivery on `if (!inet_test_bit(RECVERR, sk)) { if (!harderr ||
///   sk->sk_state != TCP_ESTABLISHED) goto out; }`, and `sk_state` only becomes `TCP_ESTABLISHED`
///   in `__ip4_datagram_connect` (`net/ipv4/datagram.c`). Unconnected and without `IP_RECVERR`, no
///   ICMP unreachable can set `sk_err` on this socket. That makes
///   `ECONNREFUSED`/`EHOSTUNREACH`/`ENETUNREACH`/`EPROTO`/PMTU `EMSGSIZE` unreachable at
///   [`BatchReader::read_batch`], which is why "every errno but `EAGAIN` is fatal" is the right
///   policy there.
/// - **Never `IP_RECVERR`/`IPV6_RECVERR`.** It's the other way to see ICMP-reported loss, next to
///   the `SO_MEMINFO` counters this listener samples, but it takes the `else` branch of the gate
///   above, which sets `sk_err` with no `TCP_ESTABLISHED` check. Any host that can provoke an ICMP
///   unreachable toward this listener could then kill it: the next `recvmmsg` returns that error,
///   and `read_loop` treats a non-`EAGAIN` errno as fatal.
/// - **Never `shutdown(2)`.** `Ready::READ_CLOSED` is in this listener's wait mask (tokio's
///   `Ready::from_interest` adds it for any readable interest), and `clear_readiness` can never
///   clear it (`runtime/io/scheduled_io.rs` removes it from the clearable mask by name). If it
///   were set while `recvmmsg` kept returning `EAGAIN`, `async_io`'s loop would spin inside a
///   single poll and, because its `WouldBlock` arm restores the coop budget, never yield. That
///   wedges the worker thread: no sampler tick, no shutdown observation, and `run_input`'s
///   backstop can't help because it's in the same task (open tokio issue #6971). It's unreachable
///   only because `EPOLLRDHUP`/`EPOLLHUP` on a UDP socket come from `sk->sk_shutdown`, which
///   nothing sets without a `shutdown(2)` on this fd, and nothing in `logit` makes one.
///
/// Verified against `torvalds/linux` master and tokio tag `tokio-1.53.1`; see ADR
/// `udp-intake-batching-and-socket-visibility`, "Amendment: kernel- and tokio-cited facts behind
/// the UDP read path".
fn bind_one(
    addr: std::net::SocketAddr,
    receive_buffer_bytes: Option<u64>,
) -> anyhow::Result<Bound> {
    use anyhow::Context;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

    let domain = if addr.is_ipv4() { socket2::Domain::IPV4 } else { socket2::Domain::IPV6 };
    let socket = socket2::Socket::new(domain, socket2::Type::DGRAM, Some(socket2::Protocol::UDP))
        .context("creating a UDP socket")?;
    if let Some(requested) = receive_buffer_bytes {
        socket
            .set_recv_buffer_size(requested as usize)
            .with_context(|| format!("setting SO_RCVBUF to {requested} bytes"))?;
    }
    socket.set_nonblocking(true).context("setting the socket non-blocking")?;

    let group = addr.ip();
    if !group.is_multicast() {
        socket.bind(&addr.into())?;
        return Ok(Bound { socket, multicast_group: None });
    }

    socket.set_reuse_address(true).context("setting SO_REUSEADDR, which a multicast bind needs")?;
    let local: SocketAddr = match group {
        IpAddr::V4(_) => (Ipv4Addr::UNSPECIFIED, addr.port()).into(),
        IpAddr::V6(_) => (Ipv6Addr::UNSPECIFIED, addr.port()).into(),
    };
    socket.bind(&local.into())?;
    match group {
        IpAddr::V4(group) => socket
            .join_multicast_v4(&group, &Ipv4Addr::UNSPECIFIED)
            .with_context(|| format!("joining the multicast group {group}")),
        IpAddr::V6(group) => socket
            .join_multicast_v6(&group, 0)
            .with_context(|| format!("joining the multicast group {group}")),
    }?;
    Ok(Bound { socket, multicast_group: Some(group) })
}

/// Gauges the granted `SO_RCVBUF`, warns if the kernel clamped it, and converts to a tokio socket.
/// Runs only once a candidate address has bound.
fn finish_bind(
    socket: socket2::Socket,
    receive_buffer_bytes: Option<u64>,
    telemetry: &Telemetry,
    diag: &mut Diagnostics,
) -> anyhow::Result<tokio::net::UdpSocket> {
    use anyhow::Context;

    report_receive_buffer(&socket, receive_buffer_bytes, telemetry, diag);
    let std_socket: std::net::UdpSocket = socket.into();
    tokio::net::UdpSocket::from_std(std_socket).context("converting to a tokio UdpSocket")
}

/// [`finish_bind`] for a Unix datagram socket, which [`crate::unix`] has already bound:
/// `SO_RCVBUF` (as [`bind_one`] sets it before an IP bind), then the same gauges.
fn finish_unix_bind(
    socket: &tokio::net::UnixDatagram,
    receive_buffer_bytes: Option<u64>,
    telemetry: &Telemetry,
    diag: &mut Diagnostics,
) -> anyhow::Result<()> {
    use anyhow::Context;

    let sock = socket2::SockRef::from(socket);
    if let Some(requested) = receive_buffer_bytes {
        sock.set_recv_buffer_size(requested as usize)
            .with_context(|| format!("setting SO_RCVBUF to {requested} bytes"))?;
    }
    report_receive_buffer(&sock, receive_buffer_bytes, telemetry, diag);
    Ok(())
}

/// Gauges the granted `SO_RCVBUF` and warns if the kernel clamped a requested size.
fn report_receive_buffer(
    socket: &socket2::Socket,
    receive_buffer_bytes: Option<u64>,
    telemetry: &Telemetry,
    diag: &mut Diagnostics,
) {
    // Read once: SO_RCVBUF doesn't change after bind. `ReceiveBufferSampler::sample_once`
    // re-emits this gauge every second (from `SO_MEMINFO`'s `SK_MEMINFO_RCVBUF`, the same
    // `sk_rcvbuf`), because a point written once would survive one `internal` drain window. This
    // emission is the only one a process that fails during startup makes.
    let granted = socket.recv_buffer_size().unwrap_or(0) as f64;
    telemetry.gauge("logit.input.receive_buffer.bytes", granted, &[]);
    if let Some(requested) = receive_buffer_bytes {
        telemetry.gauge("logit.input.receive_buffer.requested.bytes", requested as f64, &[]);
        // Linux doubles the requested value for its own bookkeeping, so a successful request
        // reports back about 2x what was asked, and `granted < requested` would never fire. Warn
        // only when `net.core.rmem_max` clamped the request below that doubled value.
        let effective_minimum =
            if cfg!(target_os = "linux") { requested.saturating_mul(2) } else { requested };
        if (granted as u64) < effective_minimum {
            diag.warn(format_args!(
                "requested a {requested}-byte receive buffer but the kernel granted only \
                 {granted} bytes -- likely clamped by net.core.rmem_max; raise that sysctl to get \
                 the full requested size"
            ));
        }
    }
}

/// Reads datagrams off `socket` into `queue` as fast as the queue's bounds and overflow policy
/// allow, independent of how far behind `decode_loop` is. The default `drop_oldest` (counted)
/// never blocks on downstream backpressure (`docs/adr/decoupled-listener-io.md` says why this
/// differs from a sink queue's `block`); only an operator's `overflow: block` stops reading.
///
/// Races every read and every push against `shutdown`, so shutdown stops this loop at once rather
/// than when the next datagram arrives or (under `block`) downstream makes room. What the loop
/// holds when it stops is counted, never lost silently: a cancelled `push_many` counts its own
/// remainder (`logit_pipeline::CountedDrain`), and [`ReadHalf`] counts a batch `push_many` never
/// took, both as `logit.component.datagrams.dropped{reason="shutdown"}`.
///
/// **Telemetry is per batch, not per datagram.** One [`BatchReader::read_batch`] call is one
/// `logit.input.reads`, one `logit.input.datagrams` of however many it returned, one
/// `logit.input.datagram.bytes` of their total, and, only when nonzero, one
/// `logit.input.datagrams.truncated`. `datagrams / reads` is the mean fill of the syscall batch: a
/// fill pinned at `read_batch` says the knob is the limit; a fill near 1 says the traffic never
/// batches and the knob is irrelevant. Per batch because each count takes `ComponentBuffer`'s
/// mutex, which `decode_loop` contends for from the other side of the same component.
///
/// Closes `queue` on every exit: a return, a fatal socket error, or this future being dropped
/// ([`ReadHalf`]'s `Drop`). That is how `decode_loop`'s `pop_many` sees "closed and empty" and
/// returns.
///
/// A `read_batch` above the queue's `max_datagrams` is legal: `push_many` evicts or blocks per
/// policy, per item, as `push` would, so rejecting it would only refuse a working config.
async fn read_loop<S: DatagramSocket>(
    socket: &S,
    queue: Arc<ReceiveQueue>,
    telemetry: Telemetry,
    diag: Diagnostics,
    mut shutdown: watch::Receiver<bool>,
    read_batch: usize,
) -> anyhow::Result<()> {
    let mut reader = BatchReader::new(read_batch);
    // `batch` is reused and cleared each iteration: `push_many` drains it, so its capacity
    // survives and the steady state allocates only the one right-sized copy per datagram.
    let mut half = ReadHalf {
        queue: &queue,
        batch: Vec::with_capacity(read_batch),
        telemetry: &telemetry,
        diag: &diag,
    };
    loop {
        half.batch.clear();
        let read = tokio::select! {
            read = reader.read_batch(socket, &mut half.batch) => read,
            _ = shutdown.wait_for(|&due| due) => return Ok(()),
        };
        if let Err(err) = read {
            return Err(describe_read_failure(socket, err));
        }
        let bytes: usize = half.batch.iter().map(|datagram| datagram.bytes.len()).sum();
        telemetry.count("logit.input.reads", 1.0, &[]);
        telemetry.count("logit.input.datagrams", half.batch.len() as f64, &[]);
        telemetry.count("logit.input.datagram.bytes", bytes as f64, &[]);
        let truncated = reader.truncated();
        if truncated > 0 {
            telemetry.count("logit.input.datagrams.truncated", truncated as f64, &[]);
        }
        // Unbiased, and `wait_for` is `Ready` on its first poll once shutdown is set, so about
        // half the time `push_many` is never polled and `half.batch` is still full on return.
        tokio::select! {
            () = queue.push_many(&mut half.batch) => {}
            _ = shutdown.wait_for(|&due| due) => return Ok(()),
        }
    }
}

/// [`read_loop`]'s batch and queue, so that every way out of the loop counts what the batch holds
/// and closes the queue: a return, and the future being dropped. The drop case is reachable: under
/// `receive.shutdown_grace: 0s`, `run_input`'s backstop can drop the read future while it yields
/// to the coop budget between the read and the push, with the batch full.
///
/// Disjoint from `push_many`'s own count: a `push_many` that was polled leaves the `Vec` empty
/// (its `CountedDrain` took every item), and one that was never polled took nothing. A datagram in
/// `batch` has already been counted in `logit.input.datagrams`, since the counts above run before
/// any await.
///
/// No allocation and no telemetry call when `batch` is empty, as it is on every exit that follows
/// a completed push.
struct ReadHalf<'a> {
    queue: &'a ReceiveQueue,
    batch: Vec<Datagram>,
    telemetry: &'a Telemetry,
    diag: &'a Diagnostics,
}

impl Drop for ReadHalf<'_> {
    fn drop(&mut self) {
        let dropped = count_shutdown_drops(self.batch.drain(..), self.telemetry);
        if dropped > 0 {
            self.diag.warn(format_args!(
                "{dropped} datagram(s) read off the socket but never queued when this listener \
                 stopped"
            ));
        }
        self.queue.close();
    }
}

/// Counts `datagrams` as `logit.component.datagrams.dropped` and their payload bytes as
/// `logit.component.bytes.dropped`, both `reason="shutdown"` (the `units` of
/// [`RECEIVE_QUEUE_METRICS`]). Returns how many; makes no telemetry call for none.
fn count_shutdown_drops(datagrams: impl Iterator<Item = Datagram>, telemetry: &Telemetry) -> u64 {
    let mut count = 0u64;
    let mut bytes = 0u64;
    for datagram in datagrams {
        count += 1;
        bytes = bytes.saturating_add(datagram.units());
    }
    if count > 0 {
        let tags = [("reason", "shutdown")];
        telemetry.count(RECEIVE_QUEUE_METRICS.items_dropped, count as f64, &tags);
        telemetry.count(RECEIVE_QUEUE_METRICS.units_dropped, bytes as f64, &tags);
    }
    count
}

/// The syscall [`BatchReader::read_batch`] makes, named in the fatal error [`read_loop`] stops on.
#[cfg(target_os = "linux")]
const READ_SYSCALL: &str = "recvmmsg(2)";
#[cfg(not(target_os = "linux"))]
const READ_SYSCALL: &str = "recvfrom(2)";

/// Turns a fatal read error into something an operator can act on.
///
/// Otherwise the only context is `run_input`'s `component '{id}'`, so a sandbox that blocks the
/// syscall reports `component 'statsd_in': Function not implemented (os error 38)`, with no
/// syscall or socket named (the failure quinn#1947 and bun#42678 hit from a seccomp profile
/// refusing `recvmmsg`). `recvmmsg(2)` is unconditional on Linux, so dropping
/// `receive.read_batch` to 1 doesn't help, and the hint says so. There is no runtime fallback
/// from `recvmmsg` to `recvmsg` (quinn#2079's pattern); that's an open decision in
/// `docs/known-gaps.md`.
///
/// The address comes from `getsockname(2)`, not the configured `bind:`: it's the one in use (a
/// `:0` port resolved, or whichever candidate won [`bind_first_available`]).
fn describe_read_failure<S: DatagramSocket>(socket: &S, err: std::io::Error) -> anyhow::Error {
    // Matched on `ErrorKind`, not `libc::E*`: `libc` is a Linux-only dependency and this function
    // also serves the non-Linux `recv_from` path. std's Unix mapping (`decode_error_kind`) is
    // `ENOSYS` -> `Unsupported`, `EPERM`/`EACCES` -> `PermissionDenied`, `ECONNABORTED` ->
    // `ConnectionAborted`.
    let hint = match err.kind() {
        std::io::ErrorKind::Unsupported | std::io::ErrorKind::PermissionDenied => {
            " -- a seccomp or sandbox profile blocking that syscall is the usual cause; \
             `receive.read_batch: 1` does not avoid it, this listener always makes the same call"
        }
        // `udp_abort` (`net/ipv4/udp.c`), reached from a `SOCK_DESTROY` netlink request, is the
        // one externally-triggerable fatal on this socket: it sets `sk_err` and unhashes the
        // socket, which never receives again. A retry would read `EAGAIN` (`sock_error`'s `xchg`
        // clears `sk_err`) and leave a zombie listener, so failing is correct.
        std::io::ErrorKind::ConnectionAborted => {
            " -- the socket was destroyed out from under this listener (an `ss -K`, or another \
             SOCK_DESTROY request naming it); it cannot receive again, so the process exits \
             rather than pretending otherwise"
        }
        _ => "",
    };
    let addr = socket.describe_local();
    anyhow::Error::new(err)
        .context(format!("{READ_SYSCALL} on the listener socket bound to {addr}{hint}"))
}

/// Words of `u64` backing one `mmsghdr`, and one `iovec`, in [`BatchReader`]'s storage (that
/// type's doc says why the element type is `u64`).
///
/// `div_ceil`, not a plain divide: the ABI doesn't promise either size is a multiple of 8 (both
/// are, on every Linux target this builds for), and rounding up can only over-allocate. It does
/// not protect alignment: [`build_headers`] and [`harvest_headers`] stride by `hdrs.add(i)`, that
/// is `size_of::<mmsghdr>()`, not `HDR_WORDS * 8`. Slot 1 stays aligned because Rust guarantees
/// `size_of::<T>()` is a multiple of `align_of::<T>()`, and the const block below asserts it.
#[cfg(target_os = "linux")]
const HDR_WORDS: usize = std::mem::size_of::<libc::mmsghdr>().div_ceil(8);

/// [`HDR_WORDS`]'s `iovec` twin; that constant's doc covers both.
#[cfg(target_os = "linux")]
const IOV_WORDS: usize = std::mem::size_of::<libc::iovec>().div_ceil(8);

/// Every layout fact [`build_headers`], [`recvmmsg_into`] and [`harvest_headers`] rest on, checked
/// against what `libc` says these two structs look like on the target being built.
///
/// A `const` block, so a target whose `mmsghdr` is shaped differently is a compile error, not a
/// runtime surprise: the same tripwire `crates/logit-core/tests/type_sizes.rs` applies to `Event`.
#[cfg(target_os = "linux")]
const _: () = {
    // **Alignment.** `mmsghdr`/`iovec` are at most 8-aligned on every target this compiles for,
    // which makes a `u64` buffer valid storage for them. `Vec<u64>`'s pointer is
    // `align_of::<u64>()`-aligned, and the compiler can't check the cast, so these two asserts are
    // the whole guarantee.
    assert!(std::mem::align_of::<libc::mmsghdr>() <= std::mem::align_of::<u64>());
    assert!(std::mem::align_of::<libc::iovec>() <= std::mem::align_of::<u64>());
    // **Capacity.** The words reserved per slot hold a whole struct, so slot `i` of a
    // `vlen * HDR_WORDS`-word buffer is wholly inside it for every `i < vlen`.
    assert!(HDR_WORDS * 8 >= std::mem::size_of::<libc::mmsghdr>());
    assert!(IOV_WORDS * 8 >= std::mem::size_of::<libc::iovec>());
    // **Stride.** `hdrs.add(i)`/`iovs.add(i)` step by `size_of`, so every slot after the first is
    // aligned only if `size_of` is a multiple of `align_of`. Guaranteed by the language; asserted
    // because `div_ceil` above doesn't protect against it.
    assert!(
        std::mem::size_of::<libc::mmsghdr>().is_multiple_of(std::mem::align_of::<libc::mmsghdr>())
    );
    assert!(std::mem::size_of::<libc::iovec>().is_multiple_of(std::mem::align_of::<libc::iovec>()));
    // **Harvest.** The two fields [`harvest_headers`] reads back lie wholly inside the header (so
    // inside its reserved `HDR_WORDS * 8` bytes) and don't alias each other.
    assert!(
        std::mem::offset_of!(libc::mmsghdr, msg_len) + std::mem::size_of::<libc::c_uint>()
            <= std::mem::size_of::<libc::mmsghdr>()
    );
    assert!(
        std::mem::offset_of!(libc::mmsghdr, msg_hdr.msg_flags) + std::mem::size_of::<libc::c_int>()
            <= std::mem::size_of::<libc::mmsghdr>()
    );
    assert!(
        std::mem::offset_of!(libc::mmsghdr, msg_len)
            != std::mem::offset_of!(libc::mmsghdr, msg_hdr.msg_flags)
    );
};

/// The Linux read half: one `recvmmsg(2)` per [`BatchReader::read_batch`] call, up to
/// `read_batch` datagrams at a time.
///
/// **Why a `libc` call and not a `tokio` one.** `tokio::net::UdpSocket` has no multi-message
/// receive, but [`UdpSocket::async_io`](tokio::net::UdpSocket::async_io) waits for readiness and
/// then hands a closure the syscall. This uses that seam, as `crate::tail::watch` does for
/// `inotify`: `libc` confined to one Linux-gated module, a `// SAFETY:` comment per `unsafe`
/// block, and no raw pointer held across an `.await`.
///
/// **The `mmsghdr`/`iovec` arrays are rebuilt inside the closure on every call, over `Vec<u64>`
/// storage, not `Vec<mmsghdr>`.** `mmsghdr` holds raw pointers, so a `Vec<mmsghdr>` is `!Send`,
/// and this struct lives across `read_batch`'s `.await`. That would make the read future `!Send`,
/// but `UdpListener::run_until_shutdown` is an `#[async_trait]` method that requires `Send`, and
/// ADR `udp-intake-batching-and-socket-visibility` rejects `unsafe impl Send`. `u64` words carry
/// no pointers; the pointers exist only for one synchronous closure call, re-derived from live
/// allocations each time. Rebuilding is a short loop of stores with no allocation.
/// [`assert_batch_read_future_is_send`] pins the property at compile time.
///
/// **`MSG_TRUNC` is detected and counted.** Each slot is [`MAX_DATAGRAM_BYTES`], which covers
/// every IPv4 datagram but not the largest IPv6 one, so on an IPv6 listener the kernel can copy
/// what fits and discard the rest. `msg_len` can't show it: it's the copied length, so it reads
/// `MAX_DATAGRAM_BYTES`, the same as a datagram that fit precisely. The kernel reports it in
/// `msg_hdr.msg_flags` instead. Each one is counted as `logit.input.datagrams.truncated`, and the
/// truncated payload is still delivered. Growing the slots to 65,527 isn't worth 20 bytes x
/// `read_batch` of address space and a constant that stops matching every other 65,507 in the
/// codebase, for a case only a jumbo IPv6 sender produces.
#[cfg(target_os = "linux")]
struct BatchReader {
    /// `vlen` contiguous [`MAX_DATAGRAM_BYTES`] slots, one per `iovec`, allocated once. Its virtual
    /// size is `read_batch * 65,507` bytes, but only pages a datagram is written into are faulted
    /// in, which is why `docs/design/memory.md` records both figures.
    slots: Vec<u8>,
    /// Backing words for the `vlen` `mmsghdr`s and their `vlen` `iovec`s (this struct's doc says
    /// why `u64`). Sized once; re-pointed at `slots` on every call.
    hdr_words: Vec<u64>,
    iov_words: Vec<u64>,
    /// Each returned message's `msg_len`, copied out before the closure returns: the header array
    /// means nothing outside it.
    lens: Vec<u32>,
    /// Each returned message's `msg_hdr.msg_flags`, copied out the same way. Only
    /// [`libc::MSG_TRUNC`] is read; the other flags describe conditions this path can't produce.
    flags: Vec<i32>,
    /// How many datagrams the last `read_batch` call truncated. Reset per call: [`read_loop`]
    /// reports it once per batch.
    truncated: u64,
    vlen: usize,
}

#[cfg(target_os = "linux")]
impl BatchReader {
    fn new(read_batch: usize) -> Self {
        let vlen = read_batch.clamp(1, MAX_READ_BATCH);
        Self {
            slots: vec![0u8; vlen * MAX_DATAGRAM_BYTES],
            hdr_words: vec![0u64; vlen * HDR_WORDS],
            iov_words: vec![0u64; vlen * IOV_WORDS],
            lens: vec![0u32; vlen],
            flags: vec![0i32; vlen],
            truncated: 0,
            vlen,
        }
    }

    /// Waits for the socket to be readable, then takes up to `vlen` datagrams off it in one
    /// `recvmmsg(2)` and appends them to `out`. Returns how many it appended.
    ///
    /// **One `now_nanos()` per syscall, offset by the datagram's index within the batch**, the
    /// named accuracy concession in ADR `udp-intake-batching-and-socket-visibility`. This path
    /// gets no per-message receive instant (that's `SO_TIMESTAMP`, which the same ADR rejects), so
    /// datagram `i` is stamped `base + i` nanoseconds: ordered and distinct, but the spacing is a
    /// placeholder, not a measurement. Stamping at decode time would be worse: it can skew
    /// arbitrarily far behind arrival under backlog.
    ///
    /// **The `+ i` is not cosmetic.** A downstream keyed on (series, timestamp) treats two points
    /// that share both as one point. `influxdb_out`'s line protocol overwrites, and its
    /// `allocate_timestamp` disambiguation resets at the top of every `Encoder::encode`, so it only
    /// covers collisions inside one output batch. A read batch stamped with one instant would
    /// produce same-timestamp, same-series points that straddle an output-batch boundary and reach
    /// the sink undisambiguated. One nanosecond per datagram costs no extra clock read, is strictly
    /// ordered, and is orders of magnitude below the batch's own arrival uncertainty.
    ///
    /// **One right-sized `Bytes::copy_from_slice` per datagram.** Slicing a shared buffer would
    /// save the copy but let one retained event pin a 65 KB slot (`docs/design/memory.md`'s
    /// "considered and rejected", pinned by `datagram_copy_is_one_right_sized_allocation`).
    ///
    /// **A zero-length datagram is legal UDP and is delivered as one**: an empty `Bytes`, counted
    /// like any other. What it means is the decoder's business.
    ///
    /// **A truncated datagram is delivered too**, as far as it was copied, and counted (see
    /// [`BatchReader::truncated`]).
    ///
    /// **Cancellation loses nothing.** `async_io` suspends in two places
    /// (`Registration::async_io`, `tokio/src/runtime/io/registration.rs`, tag `tokio-1.53.1`: the
    /// `self.readiness(interest).await?` and the `poll_fn(coop::poll_proceed).await` after it), and
    /// both are before it calls the closure. Once the closure returns anything but `WouldBlock`,
    /// `async_io` returns in that same poll. The syscall and everything after it run in one poll,
    /// so a `select!` that drops this future drops it before the syscall or not at all: the same
    /// guarantee `recv_from`'s cancel-safety rests on, since `recv_from` is the same `async_io`
    /// call with a different closure.
    async fn read_batch<S: DatagramSocket>(
        &mut self,
        socket: &S,
        out: &mut Vec<Datagram>,
    ) -> std::io::Result<usize> {
        let fd = socket.as_raw_fd();
        let vlen = self.vlen;
        // Borrowed field by field so the closure captures plain-data buffers, not all of `self`.
        let slots = &mut self.slots;
        let hdr_words = &mut self.hdr_words;
        let iov_words = &mut self.iov_words;
        let lens = &mut self.lens;
        let flags = &mut self.flags;

        // `READABLE | ERROR`, as tokio's own `UdpSocket::recv_from` waits on: a socket with only a
        // pending error isn't "readable" to the poller, and an arm that never wakes stalls the
        // listener.
        let received = socket
            .async_io(tokio::io::Interest::READABLE | tokio::io::Interest::ERROR, || {
                // Rebuilt on every call of this `FnMut` (`async_io` may call it more than once), so
                // every pointer the kernel gets is derived from a live allocation within this call,
                // and no raw pointer outlives the closure.
                build_headers(slots, MAX_DATAGRAM_BYTES, iov_words, hdr_words, vlen);
                loop {
                    // SAFETY: `build_headers` immediately above wrote `vlen` fully-initialized
                    // `mmsghdr`s into `hdr_words`, each with a one-entry `iov` describing one
                    // distinct, wholly-owned `MAX_DATAGRAM_BYTES` slot of `slots` -- which is
                    // borrowed exclusively by this closure and not reborrowed anywhere between
                    // that call and this one, so those `iov_base` pointers are still live. That
                    // is exactly `recvmmsg_into`'s stated precondition.
                    let n = unsafe { recvmmsg_into(fd, hdr_words, vlen) };
                    if n >= 0 {
                        harvest_headers(hdr_words, n as usize, lens, flags);
                        return Ok(n as usize);
                    }
                    let err = std::io::Error::last_os_error();
                    if err.raw_os_error() == Some(libc::EINTR) {
                        // Unreachable on this socket, kept as insurance. `__skb_recv_udp`
                        // (`net/ipv4/udp.c`) reaches the sleeping path, the only source of
                        // `sock_intr_errno`'s `EINTR` (`__skb_wait_for_more_packets`,
                        // `net/core/datagram.c`), only through `while (timeo && ...)`, and `timeo`
                        // is zero here twice over: `MSG_DONTWAIT` is passed, and `____sys_recvmsg`
                        // (`net/socket.c`) ORs it in for any `O_NONBLOCK` fd, which a
                        // tokio-registered socket always is. The arm costs one never-taken
                        // comparison and matches `quinn-udp`'s `retry_if_interrupted`, against a
                        // future blocking caller, another socket type, or a kernel change. Only
                        // `strace -e inject=recvmmsg:error=EINTR` can force it; a signal can't.
                        continue;
                    }
                    // `EAGAIN`/`EWOULDBLOCK` arrive as `ErrorKind::WouldBlock`, the signal
                    // `async_io` needs to clear readiness and wait again. Every other errno is
                    // fatal, because every one this socket can produce is permanent:
                    // - The ICMP-derived ones (`ECONNREFUSED`/`EHOSTUNREACH`/`ENETUNREACH`/
                    //   `EPROTO`, PMTU `EMSGSIZE`) are unreachable: `udp_err` (`net/ipv4/udp.c`)
                    //   leaves `sk_err` alone unless `IP_RECVERR` is set or the socket is
                    //   `TCP_ESTABLISHED`, and this one is neither (see `bind_one`).
                    // - `ENOBUFS` never surfaces on receive: `__udp_queue_rcv_skb` charges and
                    //   drops receive-buffer exhaustion in softirq.
                    // - What's left: `EBADF`/`ENOTSOCK`/`EINVAL`/`EFAULT` (caller bugs),
                    //   `EPERM`/`ENOSYS` (seccomp or an LSM), and `ECONNABORTED` from `udp_abort`
                    //   (an `ss -K`/`SOCK_DESTROY`, which unhashes the socket for good).
                    // Retrying is worse than failing: `sock_error`'s `xchg` clears `sk_err`, so a
                    // retry after `udp_abort` reads `EAGAIN` and the listener sits on a dead socket
                    // forever. See ADR `udp-intake-batching-and-socket-visibility`, "Errno
                    // reachability on *this* socket".
                    return Err(err);
                }
            })
            .await?;

        let base = now_nanos();
        self.truncated = 0;
        for i in 0..received {
            // A dead clamp, kept as defence in depth. `udp_recvmsg` (`net/ipv4/udp.c`;
            // `udpv6_recvmsg` identically) computes `err = copied; if (flags & MSG_TRUNC) err =
            // ulen;`: it reports the real datagram length only when `MSG_TRUNC` is an input flag,
            // and `recvmmsg_into` passes `MSG_DONTWAIT` alone. So `msg_len` is always the copied
            // length, `<= iov_len = MAX_DATAGRAM_BYTES`; a longer datagram shows in the output
            // `msg_flags` (below). The clamp means a broken value could only shorten the slice,
            // never index past the slot. The `debug_assert_eq!` keeps the claim honest in every
            // test build; `an_oversized_ipv6_datagram_is_delivered_truncated_and_counted` drives
            // the truncating case through it.
            let len = (self.lens[i] as usize).min(MAX_DATAGRAM_BYTES);
            debug_assert_eq!(
                self.lens[i] as usize, len,
                "recvmmsg reported {} bytes copied into a {MAX_DATAGRAM_BYTES}-byte slot",
                self.lens[i]
            );
            if self.flags[i] & libc::MSG_TRUNC != 0 {
                self.truncated += 1;
            }
            let start = i * MAX_DATAGRAM_BYTES;
            out.push(Datagram {
                bytes: Bytes::copy_from_slice(&self.slots[start..start + len]),
                // Saturating only so the arithmetic is total: `now_nanos()` is ~1.8e18 and `i` is
                // at most 1023, nowhere near `i64::MAX`.
                received_at: base.saturating_add(i as i64),
            });
        }
        Ok(received)
    }
}

#[cfg(target_os = "linux")]
impl BatchReader {
    /// How many datagrams the last [`BatchReader::read_batch`] returned were longer than a slot
    /// and copied only as far as one (IPv6 only; see this type's doc). Per call, not cumulative.
    fn truncated(&self) -> u64 {
        self.truncated
    }
}

/// Writes `vlen` `iovec`s into `iov_words` and `vlen` `mmsghdr`s into `hdr_words`, header `i`
/// describing slot `i` of `slots` (bytes `[i * slot_bytes, (i + 1) * slot_bytes)`) through a
/// one-entry `iov`. Every header is fully re-initialized, overwriting a previous call's kernel
/// writeback.
///
/// **Why this is its own function.** It's the pure half of [`BatchReader::read_batch`]'s closure:
/// no syscall, no fd, only pointer arithmetic over three caller-owned buffers. That makes it
/// reachable under `miri`, which has no shim for `recvmmsg`
/// (`docs/adr/out-of-ci-unsafe-verification.md`). The pointer provenance, stride, slot
/// disjointness, and zero-initialization (inventory entry `NET-01`) all live here where a tool can
/// see them; `script/unsafe-check miri` runs `mod batch_reader_helpers` against it.
///
/// **Why `mem::zeroed()` + two field assignments, not a struct literal.** rust-`libc`'s musl
/// `msghdr` has private `__pad1`/`__pad2` fields a struct literal can't set (rust-lang/libc#2344),
/// and an unzeroed pad is libuv#3419's spurious `EMSGSIZE`. Zeroing also leaves
/// `msg_name`/`msg_namelen`/`msg_control`/`msg_controllen` NULL/0, which tells the kernel to report
/// neither a source address nor ancillary data, and makes the `msg_flags`/`msg_controllen` the
/// kernel writes back per call (`____sys_recvmsg`, `net/socket.c`) inert: they're overwritten here
/// before anything reads them.
///
/// Panics rather than trusting its caller: the three length preconditions would otherwise be
/// undefined behaviour, and checking them costs three comparisons per batch.
#[cfg(target_os = "linux")]
#[inline]
fn build_headers(
    slots: &mut [u8],
    slot_bytes: usize,
    iov_words: &mut [u64],
    hdr_words: &mut [u64],
    vlen: usize,
) {
    assert!(
        vlen.checked_mul(slot_bytes).is_some_and(|need| slots.len() >= need),
        "slots must hold vlen ({vlen}) slots of {slot_bytes} bytes, got {}",
        slots.len()
    );
    assert!(
        vlen.checked_mul(IOV_WORDS).is_some_and(|need| iov_words.len() >= need),
        "iov_words must hold vlen ({vlen}) iovecs of {IOV_WORDS} words, got {}",
        iov_words.len()
    );
    assert!(
        vlen.checked_mul(HDR_WORDS).is_some_and(|need| hdr_words.len() >= need),
        "hdr_words must hold vlen ({vlen}) mmsghdrs of {HDR_WORDS} words, got {}",
        hdr_words.len()
    );

    let iovs = iov_words.as_mut_ptr().cast::<libc::iovec>();
    let hdrs = hdr_words.as_mut_ptr().cast::<libc::mmsghdr>();
    let base = slots.as_mut_ptr();
    for i in 0..vlen {
        // SAFETY: the asserts above establish that `iov_words`/`hdr_words` are live allocations of
        // at least `vlen * IOV_WORDS` / `vlen * HDR_WORDS` `u64`s and that `slots` is at least
        // `vlen * slot_bytes` bytes, so for every `i < vlen`: `iovs.add(i)`/`hdrs.add(i)` are
        // in-bounds and valid for one aligned write of their element type (alignment and the
        // `size_of`-multiple-of-`align_of` stride are both asserted in this module's `const _`
        // block, and `Vec<u64>`/a `&mut [u64]` derived from one is `align_of::<u64>()`-aligned),
        // and `base.add(i * slot_bytes)` is in bounds with `slot_bytes` bytes behind it. The three
        // buffers are borrowed exclusively here, so nothing aliases them for the duration. Writing
        // an `mmsghdr` whose only non-zero fields are a valid `msg_iov` and `msg_iovlen = 1`
        // leaves a fully valid value in every field.
        unsafe {
            iovs.add(i).write(libc::iovec {
                iov_base: base.add(i * slot_bytes).cast::<libc::c_void>(),
                iov_len: slot_bytes,
            });
            let mut hdr: libc::mmsghdr = std::mem::zeroed();
            hdr.msg_hdr.msg_iov = iovs.add(i);
            hdr.msg_hdr.msg_iovlen = 1;
            hdrs.add(i).write(hdr);
        }
    }
}

/// The syscall and nothing else, so [`build_headers`] and [`harvest_headers`] on either side stay
/// pure and `miri`-runnable. Returns `recvmmsg`'s return value: `>= 0` is a datagram count, `-1`
/// means consult `errno`.
///
/// The timeout is NULL ("no timeout"), the only value sound to pass from here: a `timespec` would
/// have to outlive a call this shim can't see the end of. NULL also side-steps `recvmmsg(2)`'s
/// documented timeout bug (BUGS: the timeout is only checked after a datagram arrives).
/// `MSG_WAITFORONE` isn't passed; it's a no-op alongside `MSG_DONTWAIT`.
///
/// # Safety
///
/// `hdr_words` must hold at least `vlen` initialized `mmsghdr`s as [`build_headers`]
/// writes them: each with a valid one-entry `msg_iov` pointing at a live, writable, exclusively
/// owned buffer of at least `iov_len` bytes, and NULL `msg_name`/`msg_control`. The kernel writes
/// through those pointers and into each header's `msg_len`/`msg_flags`, so every one of them must
/// still be live when this is called. `fd` need not be valid -- a bad descriptor is `EBADF`, not
/// undefined behaviour -- but the buffers must be.
#[cfg(target_os = "linux")]
#[inline]
unsafe fn recvmmsg_into(fd: std::os::fd::RawFd, hdr_words: &mut [u64], vlen: usize) -> libc::c_int {
    let hdrs = hdr_words.as_mut_ptr().cast::<libc::mmsghdr>();
    // SAFETY: the caller guarantees `hdrs` points at `vlen` initialized `mmsghdr`s describing live,
    // exclusively-owned buffers (this function's `# Safety` section); `hdr_words` is borrowed
    // exclusively here, so nothing else aliases the array while the kernel writes into it.
    unsafe {
        libc::recvmmsg(fd, hdrs, vlen as libc::c_uint, libc::MSG_DONTWAIT, std::ptr::null_mut())
    }
}

/// Copies the `msg_len` and `msg_hdr.msg_flags` the kernel wrote into the first `n` headers of
/// `hdr_words` out into `lens`/`flags`, touching nothing beyond `n`.
///
/// The headers hold raw pointers, which the `Vec<u64>` storage exists to keep out of a `Send`
/// future, so they mean nothing outside [`BatchReader::read_batch`]'s closure. The two numbers
/// worth keeping go into plain integer buffers, and the headers are rebuilt on the next call.
///
/// Pure, like [`build_headers`]: under `miri` a test plays the kernel's part by writing into the
/// same headers through the same pointer type.
///
/// Panics if `n` exceeds any of the three buffers. The kernel can't return more than the `vlen` it
/// was given, so this is unreachable; an assert rather than a quiet `take(n)` because a count that
/// large would mean the ABI assumption underneath had broken.
#[cfg(target_os = "linux")]
#[inline]
fn harvest_headers(hdr_words: &mut [u64], n: usize, lens: &mut [u32], flags: &mut [i32]) {
    assert!(
        n.checked_mul(HDR_WORDS).is_some_and(|need| hdr_words.len() >= need),
        "hdr_words must hold n ({n}) mmsghdrs of {HDR_WORDS} words, got {}",
        hdr_words.len()
    );
    assert!(
        lens.len() >= n && flags.len() >= n,
        "lens ({}) and flags ({}) must each hold n ({n}) entries",
        lens.len(),
        flags.len()
    );

    let hdrs = hdr_words.as_mut_ptr().cast::<libc::mmsghdr>();
    for (i, (len, flag)) in lens.iter_mut().zip(flags.iter_mut()).take(n).enumerate() {
        // SAFETY: `i < n`, and the assert above establishes `hdr_words` holds at least `n`
        // `mmsghdr`-sized slots, so `hdrs.add(i)` is in bounds and aligned (this module's `const _`
        // block asserts the alignment and the stride). The caller's contract is that the first `n`
        // headers were initialized by `build_headers` and then written by the kernel; both fields
        // read here lie wholly inside one header (also asserted in that block) and are plain
        // integers, so every byte of each is initialized either way.
        unsafe {
            *len = (*hdrs.add(i)).msg_len;
            *flag = (*hdrs.add(i)).msg_hdr.msg_flags;
        }
    }
}

/// Compile-time proof that `read_batch`'s future is `Send` with no `unsafe impl` behind it, the
/// property [`BatchReader`]'s layout exists for.
///
/// `UdpListener::run_until_shutdown`'s `#[async_trait]` already forces this, but a break there
/// surfaces several layers up with an error naming the trait, not the cause; this fails first.
#[cfg(target_os = "linux")]
#[allow(dead_code)] // a type-checked assertion, never called
fn assert_batch_read_future_is_send(
    reader: &mut BatchReader,
    socket: &tokio::net::UdpSocket,
    unix: &tokio::net::UnixDatagram,
    out: &mut Vec<Datagram>,
) {
    fn assert_send<T: Send>(_: &T) {}
    assert_send(&reader.read_batch(socket, out));
    assert_send(&reader.read_batch(unix, out));
}

/// The non-Linux read half: one `recv_from` per datagram, behind the same interface as the
/// `recvmmsg` reader, so [`read_loop`] has one code path.
///
/// `recvmmsg(2)` has no portable equivalent worth a second implementation (FreeBSD's differs
/// enough to be its own port, and `logit` ships no such target). `read_batch` is validated
/// everywhere but only reads in batches on Linux; the decode half's `pop_many` uses it on every
/// target.
#[cfg(not(target_os = "linux"))]
struct BatchReader {
    buf: Vec<u8>,
}

#[cfg(not(target_os = "linux"))]
impl BatchReader {
    fn new(_read_batch: usize) -> Self {
        Self { buf: vec![0u8; MAX_DATAGRAM_BYTES] }
    }

    /// Always `0`: `recv_from` reports only how many bytes it copied, never whether it discarded
    /// any, so `logit.input.datagrams.truncated` is Linux-only.
    fn truncated(&self) -> u64 {
        0
    }

    /// One datagram, appended to `out`; returns `1` on success. Cancel-safe as
    /// `tokio::net::UdpSocket::recv_from` is.
    async fn read_batch<S: DatagramSocket>(
        &mut self,
        socket: &S,
        out: &mut Vec<Datagram>,
    ) -> std::io::Result<usize> {
        let n = socket.recv(&mut self.buf).await?;
        out.push(Datagram {
            bytes: Bytes::copy_from_slice(&self.buf[..n]),
            received_at: now_nanos(),
        });
        Ok(1)
    }
}

/// How often [`read_loop_sampled`] reads the socket's kernel counters: one `getsockopt` a second
/// per UDP listener, cheap enough to need no config knob and frequent enough that
/// `logit.input.receive_buffer.utilization` is a usable gauge. The same cadence whether or not
/// traffic is arriving.
const KERNEL_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// [`read_loop`], plus a sampler that reads the kernel's per-socket counters on a fixed interval
/// while the read loop runs, and once more after it stops.
///
/// **Why the sampler can't live inside `read_loop`.** The moments worth sampling are the ones
/// `read_loop` isn't going round: under `overflow: block` it parks in `queue.push` while the
/// kernel's receive buffer fills and then drops. A sample at the top of each read iteration would
/// go quiet when the numbers matter most. Pinning `read_loop` as one arm of a `select!` against
/// a timer avoids a task, a channel, and a `'static` bound: [`sample_while`] re-polls the same
/// `read_loop` future each time the timer wins, so a blocked `push` resumes where it was.
/// Cancelling and restarting `read_loop` would drop a datagram per tick.
///
/// **The final sample runs on every path [`sample_while`] returns on.** Drops in the last fraction
/// of a second before a fatal socket error or a shutdown are the likeliest to exist, since a
/// listener usually stops because something went wrong. The socket is still open then (owned by
/// `run_until_shutdown`, which outlives this future), so the counters are readable.
///
/// It is **not** unconditional. One path skips it: `run_input`'s grace backstop
/// (`logit_pipeline::runtime`, the `shutdown_grace_expired` arm of its `select!`) drops this
/// future, and a dropped future runs nothing. Production doesn't reach it: `read_loop` races
/// `shutdown` in both its `select!`s and returns within microseconds, and `input_runtime_config`
/// supplies `ReceiveConfig::default()`'s 5 s grace, not `InputRuntimeConfig::default()`'s
/// `Duration::ZERO`. At `Duration::ZERO`, which tests construct, both arms are ready at once and
/// `select!`'s random rotation drops this future about half the time. See ADR
/// `udp-intake-batching-and-socket-visibility`, "The final sample runs on every path
/// `sample_while` returns on".
///
/// **Once the sampler has disabled itself, no timer is armed** and this is [`read_loop`] alone:
/// the other half of `sockstat`'s "report once and stop asking". Otherwise a listener on a
/// non-Linux build (or a kernel without `SO_MEMINFO`) would wake once a second, forever, for
/// nothing. The final sample still runs, as a no-op.
///
/// Sampling is synchronous and inline: one `getsockopt` on an owned fd is a bounded read of kernel
/// memory with no I/O wait, cheaper than a `spawn_blocking` around it.
///
/// The `select!`'s arm ordering matters; see [`sample_while`].
async fn read_loop_sampled<S: DatagramSocket>(
    socket: &S,
    queue: Arc<ReceiveQueue>,
    telemetry: Telemetry,
    diag: Diagnostics,
    shutdown: watch::Receiver<bool>,
    read_batch: usize,
) -> anyhow::Result<()> {
    let sampler = ReceiveBufferSampler::new(socket, telemetry.clone(), diag.clone());
    let read = read_loop(socket, queue, telemetry, diag, shutdown, read_batch);
    sample_while(sampler, read, KERNEL_SAMPLE_INTERVAL).await
}

/// [`read_loop_sampled`]'s loop, over an arbitrary `read` future and interval. Split out so a test
/// can reach two paths production can't on its own: the disabled sampler (the non-Linux shape),
/// and the coop-budget starvation the arm ordering below prevents, which needs a misbehaving
/// `read` future and a short interval.
///
/// **The timer arm comes first.** The intuitive order is the other one (prefer the work, sample
/// while idle), and it's wrong for a reason invisible until measured.
///
/// The mechanism, pinned to **`tokio-1.53.1`**; re-check all four facts on a bump:
///
/// 1. A task's cooperative-scheduling budget starts at **128** units per poll
///    (`task/coop/mod.rs`, `const fn initial() -> Budget { Budget(Some(128)) }`).
/// 2. A **successful** `async_io` spends one (`runtime/io/registration.rs`:
///    `coop.made_progress()` on the success arm). A `WouldBlock` one spends **zero**: it drops
///    the `RestoreOnPending` guard without calling `made_progress`, and that guard's `Drop` writes
///    the pre-decrement budget back. So the budget drains under the flood this sampler exists to
///    report, where every read succeeds and `read_loop` never parks for a real reason: its only
///    way to return `Pending` is running the budget to zero.
/// 3. `Sleep`'s poll consults coop **before** its deadline (`time/sleep.rs`, `poll_elapsed`:
///    `let coop = ready!(crate::task::coop::poll_proceed(cx));` ahead of any use of the deadline
///    or the timer entry). A timer arm polled after the read arm finds a budget of zero and
///    returns `Pending` however far past its deadline it is, and, never having reached the timer
///    driver, isn't registered to be woken by it. The next wake re-polls in the same order with the
///    same result, forever.
/// 4. `select!` gates on the budget before polling **any** arm (`macros/select.rs`:
///    `ready!(poll_budget_available(cx))` is the first thing its generated `poll_fn` does), so the
///    whole `select!` below is subject to (3) as a unit, not just the sleep inside it.
///
/// **A `Pending` caused by the coop budget is not a park, and an arm placed behind one never
/// runs.** With the timer arm first, the budget is intact when `Sleep::poll` runs; it finds the
/// deadline unmet, and its `RestoreOnPending` rolls the decrement back before the read arm burns
/// the budget to zero. The sleep is registered with the timer driver on every poll and the tick
/// lands on time.
///
/// Not part of why the budget drains: a `WouldBlock` `async_io` (see 2), and
/// `watch::Receiver::wait_for`, `read_loop`'s other arm. `wait_for` is wrapped in
/// `cooperative(..)` (`sync/watch.rs`), whose `Coop::poll` (`task/coop/mod.rs`) runs
/// `poll_proceed` first: a `Pending` restores the budget, and only a `Ready`, which ends the loop,
/// spends one unit. The read side's successful `recvmmsg` is what drains it.
///
/// Measured on a release build, eight senders flooding one listener for 10 s at about 90% kernel
/// loss: with the read arm first, 0 of 10 one-second windows carried `kernel.drops` or the buffer
/// gauges; 41-47M drops surfaced as one lump from the final sample after SIGTERM. With the timer
/// arm first, 11 of 11 windows carried them, the final sample still landed separately with a
/// non-zero residual, and throughput didn't measurably move (4.7M datagrams read, against
/// 4.7-5.0M read-arm-first). Measured on the per-datagram `recv_from` reader, before `recvmmsg`;
/// ADR `udp-intake-batching-and-socket-visibility`, "Sampling cadence", records it.
///
/// The cost is one `Sleep::poll` per wake before the read arm: a deadline comparison against a
/// timer that's nearly always not yet due. The final sample is unaffected: the read arm still wins
/// the moment `read_loop` returns, and a tick that beats it only moves the remainder into the
/// final sample.
async fn sample_while<F>(
    mut sampler: ReceiveBufferSampler,
    read: F,
    interval: Duration,
) -> anyhow::Result<()>
where
    F: std::future::Future<Output = anyhow::Result<()>>,
{
    let mut read = std::pin::pin!(read);
    let result = loop {
        sampler.sample_once();
        if !sampler.enabled {
            break (&mut read).await;
        }
        tokio::select! {
            biased;
            () = tokio::time::sleep(interval) => {}
            result = &mut read => break result,
        }
    };
    sampler.sample_once();
    result
}

/// Reads one UDP socket's kernel-side receive counters into telemetry: the drop counter and the
/// receive buffer's fill level, neither visible to the receive syscall.
///
/// Disables itself for good on the first failed read: `SO_MEMINFO` exists for a socket or never
/// will (an older kernel, a non-Linux build), so retrying would be a syscall a second for nothing.
/// One diagnostic says so: a `warn` quoting the OS error when a Linux kernel refused the read, a
/// `debug` when the build has no such counters (non-Linux, or no raw descriptor).
struct ReceiveBufferSampler {
    /// The listener socket's descriptor, captured once. `None` only on a platform with no raw
    /// descriptors, where [`logit_pipeline::sockstat`] reports nothing anyway. A bare fd rather
    /// than a borrow is safe: this sampler lives inside [`read_loop_sampled`], whose `socket`
    /// argument outlives it.
    fd: Option<sockstat::RawFd>,
    drops: sockstat::DropCounter,
    telemetry: Telemetry,
    diag: Diagnostics,
    enabled: bool,
}

impl ReceiveBufferSampler {
    fn new<S: DatagramSocket>(socket: &S, telemetry: Telemetry, diag: Diagnostics) -> Self {
        Self {
            fd: sockstat::fd_of(socket),
            drops: sockstat::DropCounter::new(),
            telemetry,
            diag,
            enabled: true,
        }
    }

    /// One `getsockopt`, and the four metrics it feeds.
    ///
    /// `logit.input.kernel.drops` is reported only when the delta is nonzero, like every other
    /// loss counter here; the gauges alongside it show the sampler is alive.
    ///
    /// `logit.input.receive_buffer.bytes` is re-emitted every sample though it never changes after
    /// bind: `ComponentBuffer::drain` (`logit_core::telemetry`) `mem::take`s its point map, so a
    /// gauge written once appears in one `internal` drain window and then vanishes, leaving the
    /// utilization gauge with no visible denominator. `finish_bind` also emits it, so the value
    /// exists before this loop runs and for a `logit` that fails during startup.
    fn sample_once(&mut self) {
        if !self.enabled {
            return;
        }
        let info = self.fd.ok_or(sockstat::Unavailable::NoDescriptor).and_then(sockstat::meminfo);
        let info = match info {
            Ok(info) => info,
            Err(err) => {
                self.enabled = false;
                // The version hint only where it applies: `EBADF` here would mean a stale or reused
                // descriptor, a bug in logit that must not read as "upgrade your kernel".
                // `Unavailable::is_unsupported_option` draws that line.
                let hint = if err.is_unsupported_option() {
                    " (SO_MEMINFO needs Linux 4.12 or newer)"
                } else {
                    ""
                };
                let message = format_args!(
                    "the kernel's per-socket receive counters are not available for this \
                     listener: {err}{hint}; logit.input.kernel.drops, \
                     logit.input.receive_buffer.used.bytes and .utilization will not be reported"
                );
                // A non-Linux build (or one with no raw descriptors) can't act on this, and
                // `internal`'s `logs:` captures `warn` into the pipeline by default, so a `warn`
                // per listener at every startup would be noise. Anything else is a real failure on
                // a platform that should have worked.
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
        let dropped = self.drops.delta(info.drops);
        if dropped > 0 {
            self.telemetry.count("logit.input.kernel.drops", dropped as f64, &[]);
        }
        self.telemetry.gauge("logit.input.receive_buffer.bytes", f64::from(info.rcvbuf), &[]);
        self.telemetry.gauge(
            "logit.input.receive_buffer.used.bytes",
            f64::from(info.rmem_alloc),
            &[],
        );
        // Both terms come from this one `SO_MEMINFO` read: mixing either with a number from
        // elsewhere (the requested `receive_buffer_bytes`, queued payload bytes) gives a wrong
        // ratio that still looks plausible. `SockMeminfo::receive_utilization`'s doc lists the
        // wrong pairings.
        if let Some(utilization) = info.receive_utilization() {
            self.telemetry.gauge("logit.input.receive_buffer.utilization", utilization, &[]);
        }
    }
}

/// Pops datagrams from `queue`, decodes and accumulates them into batches, and sends each
/// completed batch through `sink`, independent of how fast `read_loop` fills `queue`. Uses
/// [`ReceiveQueue::pop_many`], not `peek`/`commit`: a datagram that fails to decode is diagnosed
/// and dropped, never retried. `pop_many` is cancellation-safe, and this future can be dropped
/// mid-await by `run_input`'s grace backstop.
///
/// **Why `pop_many` rather than `pop`.** Every `pop` refreshes the queue's depth/bytes/utilization
/// gauges, each locking the component's telemetry buffer, which `read_loop` contends for from the
/// other side. Taking up to `read_batch` datagrams per call (`BatchingConfig::pop_batch`, the same
/// knob as the read half) makes that one set of updates per batch. `receive.latency` stays **per
/// datagram**: it says whether event timestamps are trustworthy under load, and a per-batch figure
/// would lose the resolution it exists to report.
///
/// Dropping this future mid-batch (the grace backstop, while an `emit` is parked on a full
/// downstream) discards up to `pop_batch` popped-but-undecoded datagrams. They're counted
/// `logit.component.datagrams.dropped`/`bytes.dropped{reason="shutdown"}` through a
/// `logit_pipeline::CountedDrain`, and logged once (see [`Undecoded`]). The datagram whose `emit`
/// is parked was already yielded and decoded, so it's never counted twice. A panic unwinding
/// through the drain would also count the rest as `shutdown`, since shutdown is the only
/// production canceller.
///
/// Owns `sink` (the `Fanout`): dropping this future closes every downstream inbox, the shutdown
/// cascade in `docs/adr/service-lifecycle-and-output-retry.md`.
///
/// Flushes the accumulator's final contents (`FlushReason::Shutdown`) only once `pop_many` reports
/// closed-and-empty (a return of `0`), after `read_loop` can push nothing new: the same "flush only
/// once nothing can race it" rule as `finish_and_flush` (`logit_pipeline::runtime`). The interval
/// trigger reuses `run_transform`'s deadline race via `BatchAccumulator::next_deadline`.
async fn decode_loop<D: Decoder + Send>(
    decoder: &mut D,
    queue: Arc<ReceiveQueue>,
    sink: Fanout,
    batching: BatchingConfig,
    telemetry: Telemetry,
    mut diag: Diagnostics,
) {
    let mut accumulator = BatchAccumulator::new(batching.max_events, batching.max_bytes);
    // Reused across every `decode_into` call, cleared (not replaced), so its capacity survives;
    // `BatchAccumulator::absorb`'s doc says why a `std::mem::take` here would undo the win.
    let mut scratch: Vec<Event> = Vec::new();
    // Drained (not replaced) by each `pop_many`, so its capacity survives and the steady state
    // allocates nothing.
    let mut popped: Vec<Datagram> = Vec::new();
    // A clone for `Undecoded` to borrow, since `diag` itself is borrowed mutably below.
    let remainder_diag = diag.clone();
    let has_interval = !batching.flush_interval.is_zero();
    let mut next_flush =
        has_interval.then(|| tokio::time::Instant::now() + batching.flush_interval);

    loop {
        // Checked once per popped batch, not per datagram, which can delay an interval flush by
        // the time to decode and absorb up to `read_batch` datagrams: tens of microseconds of CPU
        // against a 100 ms default, bounded by the batch size however deep the backlog. The one
        // unbounded term, an `emit` awaiting a full downstream inbox, delays the flush regardless
        // of batching.
        if let Some(deadline) = next_flush {
            let now_instant = tokio::time::Instant::now();
            if deadline <= now_instant {
                let now_instant = match accumulator.take() {
                    Some(batch) => {
                        emit(&sink, &telemetry, batch, FlushReason::Interval).await;
                        // Re-read: an `emit` parked on a full downstream past the next deadline
                        // would otherwise leave that deadline already due, and every pop batch
                        // after it would flush on `Interval`.
                        tokio::time::Instant::now()
                    }
                    None => now_instant,
                };
                next_flush = Some(BatchAccumulator::next_deadline(
                    deadline,
                    now_instant,
                    batching.flush_interval,
                ));
            }
        }

        popped.clear();
        let count = match next_flush {
            None => queue.pop_many(&mut popped, batching.pop_batch).await,
            Some(deadline) => {
                let wait = deadline.saturating_duration_since(tokio::time::Instant::now());
                match tokio::time::timeout(wait, queue.pop_many(&mut popped, batching.pop_batch))
                    .await
                {
                    Ok(count) => count,
                    Err(_elapsed) => continue,
                }
            }
        };

        if count == 0 {
            // Closed and empty: `read_loop` has stopped for good (shutdown or a fatal socket
            // error), so nothing more can arrive. Flush what's left.
            if let Some(batch) = accumulator.take() {
                emit(&sink, &telemetry, batch, FlushReason::Shutdown).await;
            }
            return;
        }

        // Drained, not iterated by reference: `decode_into` takes each `Bytes` by value, and each
        // is freed as it's consumed rather than at the end of the batch. FIFO order is preserved.
        let undecoded = Undecoded {
            drain: CountedDrain::new(&mut popped, &telemetry, &RECEIVE_QUEUE_METRICS, "shutdown"),
            left: count,
            diag: &remainder_diag,
        };
        for datagram in undecoded {
            let latency_nanos = (now_nanos() - datagram.received_at).max(0) as u64;
            telemetry.timing(
                "logit.component.receive.latency",
                Duration::from_nanos(latency_nanos),
                &[],
            );

            scratch.clear();
            match decoder.decode_into(datagram.bytes, datagram.received_at, &mut scratch) {
                Ok((resource, scope)) => {
                    // `scope` is `None` from every datagram decoder today, but threaded through
                    // rather than hardcoded so a decoder that carries one isn't dropped.
                    if let Some((batch, reason)) = accumulator.absorb(resource, scope, &mut scratch)
                    {
                        emit(&sink, &telemetry, batch, reason).await;
                    }
                }
                Err(err) => {
                    // A malformed datagram from one client shouldn't take the whole listener down.
                    diag.warn_throttled("bad_datagram", err);
                }
            }
        }
    }
}

/// [`decode_loop`]'s popped batch: a `CountedDrain` that also logs, when dropped with datagrams
/// never yielded, how many it counted. The telemetry count is the drain's; this adds the self-log
/// line, since `internal`'s final drain has already run by the time the grace backstop drops the
/// loop.
struct Undecoded<'a> {
    drain: CountedDrain<'a, Datagram>,
    /// Datagrams not yet yielded.
    left: usize,
    diag: &'a Diagnostics,
}

impl Iterator for Undecoded<'_> {
    type Item = Datagram;

    fn next(&mut self) -> Option<Datagram> {
        let datagram = self.drain.next()?;
        self.left -= 1;
        Some(datagram)
    }
}

impl Drop for Undecoded<'_> {
    fn drop(&mut self) {
        if self.left > 0 {
            self.diag.warn(format_args!(
                "{} datagram(s) taken off the receive queue but not decoded when this listener \
                 was stopped at its shutdown grace",
                self.left
            ));
        }
    }
}

/// Counts the flush by `reason` and sends `batch`.
///
/// `sink.send` mints a fresh [`logit_pipeline::TraceContext::new_root`] per accumulated batch, not
/// per datagram, so a `batch_max_events` above 1 puts independently-arrived datagrams under one
/// shared root. It's the same many-to-one gap as a stateful transform's `flush()`, tracked in
/// `docs/known-gaps.md`'s internal-spans entry.
async fn emit(sink: &Fanout, telemetry: &Telemetry, batch: EventBatch, reason: FlushReason) {
    telemetry.count("logit.component.receive.flushed", 1.0, &[("reason", reason.as_str())]);
    sink.send(batch).await;
}

/// Wall-clock nanoseconds since the Unix epoch, the `received_at` stamp. Shared with
/// [`crate::tcp`]'s connection loop so the two listeners read one clock.
pub(crate) fn now_nanos() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, Resource};
    use logit_pipeline::unwrap_batch;
    use logit_proto::CodecError;
    use tokio::net::UdpSocket;
    use tokio::sync::mpsc;

    /// The `read_batch` tests pass when the value doesn't matter: `UdpListenerConfig::default`'s
    /// 64, spelled out so a test asserting across batch boundaries names its number.
    const TEST_POP_BATCH: usize = 64;

    /// One datagram -> one event, except the literal bytes `b"BAD"`, which are rejected. Each
    /// event carries the raw datagram under `"payload"` and its `received_at` as `timestamp`, so a
    /// test can tell which datagram produced which event.
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
                return Err(CodecError::Malformed("bad datagram".to_string()));
            }
            let mut attrs = AttrMap::new();
            attrs.insert("payload", logit_core::Value::str(String::from_utf8_lossy(&bytes)));
            out.push(Event::empty(received_at, attrs));
            Ok((Arc::clone(&self.resource), None))
        }
    }

    fn payload(event: &Event) -> String {
        match event.attributes.get("payload") {
            Some(logit_core::Value::Str(bytes)) => String::from_utf8_lossy(bytes).into_owned(),
            other => panic!("expected a payload attribute, got {other:?}"),
        }
    }

    async fn bind_ephemeral() -> UdpSocket {
        UdpSocket::bind("127.0.0.1:0").await.expect("should bind an ephemeral port")
    }

    async fn send_datagram(target: std::net::SocketAddr, payload: &[u8]) {
        let sender = bind_ephemeral().await;
        sender.send_to(payload, target).await.expect("send_to should succeed on loopback");
    }

    fn recording_fanout(capacity: usize) -> (Fanout, mpsc::Receiver<logit_pipeline::Delivered>) {
        let (tx, rx) = mpsc::channel(capacity);
        (Fanout::new(vec![tx]), rx)
    }

    /// A receive queue emitting into `telemetry`: a `Registry`'s handle for a test that reads the
    /// queue's drop counts, since a default handle records nothing.
    fn test_queue(
        overflow: OverflowPolicy,
        max_datagrams: usize,
        telemetry: &Telemetry,
    ) -> Arc<ReceiveQueue> {
        Arc::new(BoundedQueue::with_metrics(
            QueueConfig { max_items: max_datagrams, max_weight: u64::MAX, overflow },
            &RECEIVE_QUEUE_METRICS,
            telemetry.clone(),
        ))
    }

    /// A stalled downstream `Fanout` never stops the read half from draining the socket.
    ///
    /// Deterministic, not timed: the one consumer has capacity 1 and is never read, so
    /// `decode_loop` blocks on its second send and pops nothing more. Everything `read_loop`
    /// pushes afterward fills the 4-item queue and then evicts under `drop_oldest`, so the queue
    /// must end holding 4 items; a backpressured reader would leave 0.
    ///
    /// The loops are raced as unspawned futures: `spawn` needs `'static`, and `decode_loop` never
    /// returns here, so it must be raced away from, not awaited.
    #[tokio::test]
    async fn the_reader_keeps_reading_while_the_downstream_fanout_is_never_drained() {
        let socket = bind_ephemeral().await;
        let addr = socket.local_addr().unwrap();
        let queue = test_queue(OverflowPolicy::DropOldest, 4, &Telemetry::default());
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (fanout, _rx) = recording_fanout(1);
        let telemetry = Telemetry::default();
        let mut decoder = TestDecoder::new();

        tokio::pin! {
            let read_fut = read_loop(
                &socket,
                Arc::clone(&queue),
                telemetry.clone(),
                Diagnostics::default(),
                shutdown_rx.clone(),
                TEST_POP_BATCH,
            );
            let decode_fut = decode_loop(
                &mut decoder,
                Arc::clone(&queue),
                fanout,
                BatchingConfig {
                    max_events: 1,
                    max_bytes: u64::MAX,
                    flush_interval: Duration::ZERO,
                    pop_batch: TEST_POP_BATCH,
                },
                telemetry,
                Diagnostics::default(),
            );
            // More datagrams than the queue's depth (4): a backpressured reader would leave some in
            // the OS receive buffer.
            let driver = async {
                for i in 0..20u32 {
                    send_datagram(addr, format!("msg-{i}").as_bytes()).await;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            };
        }

        tokio::select! {
            _ = &mut read_fut => panic!("read_loop must not exit during this test"),
            _ = &mut decode_fut => panic!("decode_loop must not exit during this test"),
            () = &mut driver => {}
        }
        // Neither loop is polled again, so nothing else touches the queue below.

        let mut drained = 0;
        while tokio::time::timeout(Duration::from_millis(10), queue.pop()).await.is_ok() {
            drained += 1;
        }
        assert_eq!(
            drained, 4,
            "the queue should hold exactly its configured depth (4 items), not 0 -- 0 would mean \
             the reader stopped accepting datagrams once downstream stalled"
        );
    }

    /// `run_until_shutdown` with nothing ever sent shuts down cleanly within its grace and
    /// delivers nothing. `a_backlog_queued_before_shutdown_is_still_decoded_and_delivered` covers
    /// the queued-backlog case against `read_loop`/`decode_loop` directly.
    #[tokio::test]
    async fn shutdown_with_an_empty_queue_finishes_within_grace_and_delivers_nothing() {
        let mut listener = UdpListener::new(
            "127.0.0.1:0",
            TestDecoder::new(),
            UdpListenerConfig {
                batch_max_events: 1,
                batch_flush_interval: Duration::ZERO,
                ..UdpListenerConfig::default()
            },
        );

        let (fanout, mut rx) = recording_fanout(8);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(fanout, shutdown_rx).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        shutdown_tx.send(true).expect("receiver should still be alive");
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("should shut down within its grace")
            .expect("task should not panic")
            .expect("should shut down without error");
        assert!(rx.try_recv().is_err(), "nothing was ever sent, so nothing should be delivered");
    }

    // -- `Input::bind`/`local_addr` --

    /// Bind, learn the address via `local_addr`, then a real datagram sent to it is delivered by
    /// a `run_until_shutdown` that reuses the bound socket.
    #[tokio::test]
    async fn bind_then_run_delivers_a_real_datagram() {
        let mut listener =
            UdpListener::new("127.0.0.1:0", TestDecoder::new(), UdpListenerConfig::default());
        assert_eq!(listener.local_addr(), None, "no address before bind()");

        listener.bind().await.expect("binding an ephemeral port should succeed");
        let addr = listener.local_addr().expect("bind() should leave a real address behind");

        let (fanout, mut rx) = recording_fanout(8);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(fanout, shutdown_rx).await });

        send_datagram(addr, b"hello").await;
        let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("a datagram sent to the bound address should be delivered")
            .expect("the channel should not have closed");
        let batch = unwrap_batch(delivered);
        assert_eq!(payload(&batch.events[0]), "hello");

        handle.abort();
    }

    /// A second `bind()` is a no-op ([`logit_pipeline::Input::bind`]'s contract), not a rebind
    /// that fails with "address in use".
    #[tokio::test]
    async fn a_second_bind_is_a_no_op() {
        let mut listener =
            UdpListener::new("127.0.0.1:0", TestDecoder::new(), UdpListenerConfig::default());
        listener.bind().await.expect("first bind should succeed");
        let addr = listener.local_addr().expect("bind() should leave a real address behind");
        listener.bind().await.expect("second bind should be a harmless no-op");
        assert_eq!(listener.local_addr(), Some(addr), "the address must not change");
    }

    /// `bind()` reports an unbindable address as an error. Uses an already-held port, not a
    /// privileged one: the tests run as root in the dev container
    /// (`docs/adr/containerized-development.md`), where a low port binds fine.
    #[tokio::test]
    async fn bind_reports_an_unbindable_address() {
        let held = bind_ephemeral().await;
        let addr = held.local_addr().unwrap().to_string();
        let mut listener = UdpListener::new(addr, TestDecoder::new(), UdpListenerConfig::default());
        assert!(listener.bind().await.is_err(), "binding an already-held address should fail");
    }

    /// `run_until_shutdown` binds on its own when `bind()` wasn't called first
    /// ([`logit_pipeline::Input::bind`]'s lazy fallback).
    #[tokio::test]
    async fn run_until_shutdown_binds_when_the_caller_did_not() {
        let mut listener =
            UdpListener::new("127.0.0.1:0", TestDecoder::new(), UdpListenerConfig::default());
        assert_eq!(listener.local_addr(), None);
        let (fanout, _rx) = recording_fanout(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(fanout, shutdown_rx).await });
        // Only proves the task didn't error out at bind; the listener moved into the task, so
        // there's nothing else to synchronize on.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!handle.is_finished(), "run_until_shutdown should have bound and now be listening");
        handle.abort();
    }

    /// Datagrams already queued when shutdown fires still reach the `Fanout`.
    #[tokio::test]
    async fn a_backlog_queued_before_shutdown_is_still_decoded_and_delivered() {
        let socket = bind_ephemeral().await;
        let queue = test_queue(OverflowPolicy::DropOldest, 100, &Telemetry::default());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (fanout, mut rx) = recording_fanout(100);
        let telemetry = Telemetry::default();
        let mut decoder = TestDecoder::new();

        // Queued directly, for determinism. `shutdown.wait_for` checks the current value on its
        // first poll, so signalling before either loop runs is equivalent to mid-run.
        for i in 0..3u32 {
            queue
                .push(Datagram { bytes: Bytes::from(format!("msg-{i}")), received_at: i as i64 })
                .await;
        }
        shutdown_tx.send(true).expect("receiver should still be alive");

        // `join!`, not `spawn`: both loops terminate here, and nothing need be `'static`.
        let (read_result, ()) = tokio::join!(
            read_loop(
                &socket,
                Arc::clone(&queue),
                telemetry.clone(),
                Diagnostics::default(),
                shutdown_rx,
                TEST_POP_BATCH
            ),
            decode_loop(
                &mut decoder,
                Arc::clone(&queue),
                fanout,
                BatchingConfig {
                    max_events: 1,
                    max_bytes: u64::MAX,
                    flush_interval: Duration::ZERO,
                    pop_batch: TEST_POP_BATCH,
                },
                telemetry,
                Diagnostics::default(),
            )
        );
        read_result.expect("should shut down cleanly");

        let mut payloads = Vec::new();
        while let Ok(delivered) = rx.try_recv() {
            payloads.push(payload(&unwrap_batch(delivered).events[0]));
        }
        payloads.sort();
        assert_eq!(payloads, vec!["msg-0", "msg-1", "msg-2"]);
    }

    /// A backlog several pop batches deep is fully decoded, in arrival order, across batch
    /// boundaries. Unsorted: the channel is FIFO and `batch_max_events: 1` makes one delivery per
    /// datagram, so the received sequence is the decode order.
    #[tokio::test]
    async fn a_backlog_deeper_than_the_pop_batch_is_fully_decoded_in_arrival_order() {
        const BACKLOG: usize = TEST_POP_BATCH * 3 + 7;

        let socket = bind_ephemeral().await;
        let queue = test_queue(OverflowPolicy::DropOldest, BACKLOG * 2, &Telemetry::default());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (fanout, mut rx) = recording_fanout(BACKLOG * 2);
        let telemetry = Telemetry::default();
        let mut decoder = TestDecoder::new();

        for i in 0..BACKLOG {
            queue
                .push(Datagram { bytes: Bytes::from(format!("msg-{i}")), received_at: i as i64 })
                .await;
        }
        shutdown_tx.send(true).expect("receiver should still be alive");

        let (read_result, ()) = tokio::join!(
            read_loop(
                &socket,
                Arc::clone(&queue),
                telemetry.clone(),
                Diagnostics::default(),
                shutdown_rx,
                TEST_POP_BATCH
            ),
            decode_loop(
                &mut decoder,
                Arc::clone(&queue),
                fanout,
                BatchingConfig {
                    max_events: 1,
                    max_bytes: u64::MAX,
                    flush_interval: Duration::ZERO,
                    pop_batch: TEST_POP_BATCH,
                },
                telemetry,
                Diagnostics::default(),
            )
        );
        read_result.expect("should shut down cleanly");

        let mut payloads = Vec::new();
        while let Ok(delivered) = rx.try_recv() {
            payloads.push(payload(&unwrap_batch(delivered).events[0]));
        }
        let expected: Vec<String> = (0..BACKLOG).map(|i| format!("msg-{i}")).collect();
        assert_eq!(
            payloads, expected,
            "every datagram in a backlog {BACKLOG} deep (pop batch {TEST_POP_BATCH}) should be \
             decoded exactly once, in arrival order"
        );
    }

    /// A malformed datagram is diagnosed and skipped without stopping the decode loop.
    #[tokio::test]
    async fn a_malformed_datagram_is_skipped_without_stopping_the_decode_loop() {
        let socket = bind_ephemeral().await;
        let addr = socket.local_addr().unwrap();
        let queue = test_queue(OverflowPolicy::DropOldest, 10, &Telemetry::default());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (fanout, mut rx) = recording_fanout(10);
        let telemetry = Telemetry::default();
        let mut decoder = TestDecoder::new();

        // A third joined future, so both loops are running while the datagrams are sent.
        let driver = async {
            send_datagram(addr, b"good-1").await;
            send_datagram(addr, b"BAD").await;
            send_datagram(addr, b"good-2").await;
            tokio::time::sleep(Duration::from_millis(50)).await;
            shutdown_tx.send(true).expect("receiver should still be alive");
        };

        let (read_result, (), ()) = tokio::join!(
            read_loop(
                &socket,
                Arc::clone(&queue),
                telemetry.clone(),
                Diagnostics::default(),
                shutdown_rx,
                TEST_POP_BATCH
            ),
            decode_loop(
                &mut decoder,
                Arc::clone(&queue),
                fanout,
                BatchingConfig {
                    max_events: 1,
                    max_bytes: u64::MAX,
                    flush_interval: Duration::ZERO,
                    pop_batch: TEST_POP_BATCH,
                },
                telemetry,
                Diagnostics::default(),
            ),
            driver,
        );
        read_result.expect("should shut down cleanly");

        let mut payloads = Vec::new();
        while let Ok(delivered) = rx.try_recv() {
            payloads.push(payload(&unwrap_batch(delivered).events[0]));
        }
        payloads.sort();
        assert_eq!(
            payloads,
            vec!["good-1", "good-2"],
            "the malformed datagram must be skipped, not stop the good ones either side of it"
        );
    }

    /// With no `receive_buffer_bytes`, `bind_socket` still gauges the kernel's default grant, and
    /// reports no requested size.
    #[tokio::test]
    async fn bind_socket_reports_the_granted_receive_buffer_even_when_unset() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        let mut diag = Diagnostics::default();
        let socket = bind_socket("127.0.0.1:0", None, &telemetry, &mut diag)
            .await
            .expect("binding with no explicit receive_buffer_bytes should succeed");
        drop(socket);

        let events = registry.drain(0);
        let granted = gauge(&events, "logit.input.receive_buffer.bytes")
            .expect("the granted receive buffer should be gauged even when none was requested");
        assert!(granted > 0.0, "the kernel always grants a nonzero default, got {granted}");
        assert_eq!(
            gauge(&events, "logit.input.receive_buffer.requested.bytes"),
            None,
            "nothing was requested, so no requested size should be reported"
        );
    }

    /// A `bind:` resolving to several candidates falls through past one that can't bind. The
    /// first candidate fails because another socket holds it, not through DNS.
    #[tokio::test]
    async fn bind_first_available_falls_through_to_a_later_candidate() {
        let occupied = bind_ephemeral().await;
        let occupied_addr = occupied.local_addr().unwrap();
        let free_addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();

        let telemetry = Telemetry::default();
        let mut diag = Diagnostics::default();
        let (socket, group) =
            bind_first_available(&[occupied_addr, free_addr], None, &telemetry, &mut diag)
                .expect("should fall through to the second, unoccupied candidate");

        assert_ne!(
            socket.local_addr().unwrap(),
            occupied_addr,
            "must not have somehow bound the already-occupied address"
        );
        assert_eq!(group, None, "a unicast bind joins no group");
        drop(occupied); // held until here, so the port stays occupied throughout
    }

    /// The multicast path of [`bind_one`]: a group address binds the unspecified address on that
    /// port with `SO_REUSEADDR` and joins the group, and a datagram sent to the group from an
    /// ordinary socket arrives.
    ///
    /// **Skips rather than fails when the environment has no multicast route**: a container with
    /// only a bridged `eth0` and no `224.0.0.0/4` route fails the join itself. Production doesn't
    /// skip; `bind_one` fails startup, because a listener that joined nothing receives nothing.
    ///
    /// Port 0 isn't usable (senders must know the port), so the port comes from binding and
    /// dropping an ordinary socket first, with a small race against anything else claiming it.
    #[tokio::test]
    async fn a_multicast_bind_joins_the_group_and_receives_a_datagram_sent_to_it() {
        // collectd's default IPv4 group (`network` plugin).
        const GROUP: &str = "239.192.74.66";
        let port = {
            let probe = UdpSocket::bind("0.0.0.0:0").await.expect("should bind an ephemeral port");
            probe.local_addr().unwrap().port()
        };
        let addr: std::net::SocketAddr = format!("{GROUP}:{port}").parse().unwrap();

        let telemetry = Telemetry::default();
        let mut diag = Diagnostics::default();
        let (socket, group) = match bind_first_available(&[addr], None, &telemetry, &mut diag) {
            Ok(bound) => bound,
            Err(err) if is_no_multicast_route(&err) => {
                println!("skipping: this environment has no multicast route ({err:#})");
                return;
            }
            Err(err) => panic!("binding the multicast group failed: {err:#}"),
        };
        assert_eq!(group, Some(addr.ip()), "the joined group is reported for the `bound` line");
        assert_eq!(
            socket.local_addr().unwrap(),
            format!("0.0.0.0:{port}").parse::<std::net::SocketAddr>().unwrap(),
            "a multicast listener binds the unspecified address, not the group itself"
        );

        // Not `127.0.0.1`: the sender's source address picks the interface a multicast datagram
        // leaves by, and loopback would never reach a group joined on the default interface.
        let sender = UdpSocket::bind("0.0.0.0:0").await.expect("should bind an ephemeral port");
        if let Err(err) = sender.send_to(b"hello group", addr).await {
            // The same missing route, surfacing at send time instead of join time.
            println!("skipping: cannot send to a multicast group here ({err})");
            return;
        }

        let mut buf = [0u8; 64];
        let (len, _from) = tokio::time::timeout(Duration::from_secs(5), socket.recv_from(&mut buf))
            .await
            .expect("a datagram sent to a joined group must arrive")
            .expect("recv_from should succeed");
        assert_eq!(&buf[..len], b"hello group");
    }

    /// Whether `err` is the "this host has no multicast route" family the test above skips on.
    /// Linux errno numbers (`EPERM`, `ENODEV`, `EADDRNOTAVAIL`, `ENETUNREACH`); a non-Linux host
    /// fails the test rather than skipping it.
    fn is_no_multicast_route(err: &anyhow::Error) -> bool {
        const SKIP_ERRNOS: [i32; 4] = [1, 19, 99, 101];
        err.chain()
            .filter_map(|cause| cause.downcast_ref::<std::io::Error>())
            .any(|io| io.raw_os_error().is_some_and(|code| SKIP_ERRNOS.contains(&code)))
    }

    // -- per-socket kernel visibility (`ReceiveBufferSampler`, `read_loop_sampled`) --------------

    /// How many datagrams a kernel-overrun test blasts at a tiny receive buffer. At 8 KiB Linux
    /// grants 16 KiB and charges each datagram several hundred bytes of `skb->truesize`, so a
    /// couple of dozen fit: a margin of nearly two orders of magnitude, so "more than zero" drops
    /// can't flake.
    #[cfg(target_os = "linux")]
    const OVERRUN_DATAGRAMS: usize = 2_000;

    /// Requested, not assumed: Linux doubles it and `net.core.rmem_max` may clamp it, and nothing
    /// below depends on the granted figure.
    #[cfg(target_os = "linux")]
    const TINY_RECEIVE_BUFFER: u64 = 8 * 1024;

    /// Every `logit.input.kernel.drops` delta in `events`, summed.
    #[cfg(target_os = "linux")]
    fn kernel_drops(events: &[Event]) -> f64 {
        events
            .iter()
            .flat_map(|event| &event.metrics)
            .filter(|m| logit_core::interner::resolve(m.name) == "logit.input.kernel.drops")
            .map(|m| match &m.kind {
                logit_core::MetricKind::Sum(sum) => sum.value,
                other => panic!("kernel.drops must be a counter, got {other:?}"),
            })
            .sum()
    }

    /// The single value of gauge `name` in `events`, or `None` if it was never recorded. A gauge
    /// is last-write-wins per drain, so there is at most one point per name here.
    fn gauge(events: &[Event], name: &str) -> Option<f64> {
        events
            .iter()
            .flat_map(|event| &event.metrics)
            .filter(|m| logit_core::interner::resolve(m.name) == name)
            .map(|m| match &m.kind {
                logit_core::MetricKind::Gauge(v) => *v,
                other => panic!("{name} must be a gauge, got {other:?}"),
            })
            .next()
    }

    #[cfg(target_os = "linux")]
    async fn blast(target: std::net::SocketAddr, datagrams: usize) {
        let sender = bind_ephemeral().await;
        for _ in 0..datagrams {
            // Errors ignored: a loopback send into a full receive buffer still succeeds (the
            // packet is discarded later, in softirq, and charged to the receiver's `sk_drops`),
            // and a send that did fail isn't part of the overrun.
            let _ = sender.send_to(b"overrun", target).await;
        }
    }

    /// Datagrams the kernel discards before the read loop returns them are counted, and a
    /// listener that ran reports the receive buffer's gauges alongside them.
    ///
    /// **No sleeps.** The overrun happens while the listener is bound but not yet running, and
    /// `shutdown` is already signalled. The sampler's first read comes at the top of
    /// `read_loop_sampled`, before `read_loop` is polled, and drops are a count, so every sample's
    /// delta adds to the asserted total.
    ///
    /// This is the case `DropCounter`'s first-sample-is-absolute rule exists for: every drop here
    /// happened before the first sample, and a baseline-only first sample would report zero.
    ///
    /// **The fill gauges are asserted present, not nonzero.** A gauge is last-write-wins per
    /// drain, so this sees the final sample, and one `recvmmsg` with `vlen = 64` empties a 16 KiB
    /// buffer (about 21 of these datagrams) in one syscall, so it reads empty by then.
    /// `a_full_receive_buffer_is_reported_as_used_bytes_and_a_utilization_ratio` asserts the
    /// nonzero fill deterministically.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn the_kernels_own_drops_and_receive_buffer_fill_are_reported() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        let mut listener = UdpListener::new(
            "127.0.0.1:0",
            TestDecoder::new(),
            UdpListenerConfig {
                receive_buffer_bytes: Some(TINY_RECEIVE_BUFFER),
                ..UdpListenerConfig::default()
            },
        )
        .with_telemetry(telemetry);
        listener.bind().await.expect("binding an ephemeral port should succeed");
        let addr = listener.local_addr().expect("bind() leaves a real address behind");

        blast(addr, OVERRUN_DATAGRAMS).await;

        // Already signalled: `read_loop` stops almost at once, after `read_loop_sampled`'s first
        // sample of a still-full socket.
        let (_shutdown_tx, shutdown_rx) = watch::channel(true);
        let (fanout, _rx) = recording_fanout(8);
        tokio::time::timeout(
            Duration::from_secs(5),
            listener.run_until_shutdown(fanout, shutdown_rx),
        )
        .await
        .expect("shutdown was already signalled, so this must return promptly")
        .expect("should shut down without error");

        let events = registry.drain(0);
        assert!(
            kernel_drops(&events) > 0.0,
            "{OVERRUN_DATAGRAMS} datagrams into a {TINY_RECEIVE_BUFFER}-byte buffer nothing was \
             reading must leave the kernel's own drop counter nonzero"
        );
        let used = gauge(&events, "logit.input.receive_buffer.used.bytes")
            .expect("the receive-buffer fill gauge should have been sampled");
        let granted = gauge(&events, "logit.input.receive_buffer.bytes").expect(
            "the granted-buffer gauge should be re-emitted by the sampler, not only at bind",
        );
        let utilization = gauge(&events, "logit.input.receive_buffer.utilization")
            .expect("the utilization gauge should have been sampled");
        assert!(granted > 0.0, "a live socket always has a receive-buffer ceiling");
        assert!(used >= 0.0, "a fill level is never negative, got {used}");
        // Both terms come from one `SO_MEMINFO` read, so the ratio is the one the kernel tests.
        // No `<= 1.0` bound: the kernel admits a datagram while the already-charged total is at
        // or below the ceiling, then charges its whole `truesize` on top, so a saturated queue
        // reads up to `(rcvbuf + truesize) / rcvbuf` (1.17 observed under a real flood). See
        // `SockMeminfo::receive_utilization`.
        assert!(
            (utilization - used / granted).abs() < 1e-9,
            "utilization must be `used.bytes / receive_buffer.bytes`, not a ratio against the \
             requested size or against queued payload bytes"
        );
    }

    /// A full, unread receive buffer, sampled once, reports nonzero fill and utilization. No read
    /// loop runs: one `recvmmsg` would empty the buffer first.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_full_receive_buffer_is_reported_as_used_bytes_and_a_utilization_ratio() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        let mut diag = Diagnostics::default();
        let (socket, _group) =
            bind_socket("127.0.0.1:0", Some(TINY_RECEIVE_BUFFER), &telemetry, &mut diag)
                .await
                .expect("binding an ephemeral port should succeed");
        let addr = socket.local_addr().expect("a bound socket has an address");

        blast(addr, OVERRUN_DATAGRAMS).await;

        let mut sampler =
            ReceiveBufferSampler::new(&socket, telemetry.clone(), Diagnostics::default());
        sampler.sample_once();
        assert!(sampler.enabled, "SO_MEMINFO is available on this kernel -- test premise");

        let events = registry.drain(0);
        let used = gauge(&events, "logit.input.receive_buffer.used.bytes")
            .expect("the receive-buffer fill gauge should have been sampled");
        let granted = gauge(&events, "logit.input.receive_buffer.bytes")
            .expect("the granted-buffer gauge should be re-emitted by the sampler");
        let utilization = gauge(&events, "logit.input.receive_buffer.utilization")
            .expect("the utilization gauge should have been sampled");
        assert!(used > 0.0, "the buffer is full and unread, got {used} bytes");
        assert!(utilization > 0.0, "a full buffer's utilization is nonzero, got {utilization}");
        assert!(
            (utilization - used / granted).abs() < 1e-9,
            "utilization must be `used.bytes / receive_buffer.bytes`"
        );
    }

    /// **`logit.input.kernel.drops` equals `/proc/net/udp`'s `drops` column**, to the packet, on
    /// the same socket at the same moment.
    ///
    /// Both read the same field: `sk_get_meminfo` (`net/core/sock.c`) does
    /// `mem[SK_MEMINFO_DROPS] = atomic_read(&sk->sk_drops)`, and `udp4_format_sock`
    /// (`net/ipv4/udp.c`) prints `atomic_read(&sp->sk_drops)` as its last column (both verified at
    /// v6.12), so this is an equality, not a threshold.
    ///
    /// The only test that pins the `SK_MEMINFO_DROPS` index against something other than itself:
    /// reading `SK_MEMINFO_BACKLOG` (7) or `SK_MEMINFO_OPTMEM` (6) instead would pass every
    /// nonzero-drops assertion elsewhere.
    ///
    /// **Ordering.** The blast finishes before anything is read and nothing else sends here, so
    /// `sk_drops` is frozen. The socket stays open (procfs lists only live sockets), and the row is
    /// found by inode (`/proc/self/fd/<fd>` reads `socket:[<inode>]`), not address, so another
    /// test's loopback socket can't be mistaken for it.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn the_kernels_drop_counter_agrees_with_proc_net_udp_to_the_packet() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        let mut diag = Diagnostics::default();
        let (socket, _group) =
            bind_socket("127.0.0.1:0", Some(TINY_RECEIVE_BUFFER), &telemetry, &mut diag)
                .await
                .expect("binding an ephemeral port should succeed");
        let addr = socket.local_addr().expect("a bound socket has an address");

        // The blaster is awaited to completion, so nothing is still in flight below.
        blast(addr, OVERRUN_DATAGRAMS).await;

        let Some(procfs_drops) = proc_net_udp_drops(&socket) else {
            println!("skipping: /proc/net/udp is not readable in this environment");
            return;
        };

        let mut sampler =
            ReceiveBufferSampler::new(&socket, telemetry.clone(), Diagnostics::default());
        sampler.sample_once();
        assert!(sampler.enabled, "SO_MEMINFO is available on this kernel -- test premise");

        // A third reading, straight off the fd, bypassing the sampler.
        let direct = logit_pipeline::sockstat::meminfo(
            logit_pipeline::sockstat::fd_of(&socket).expect("a unix socket has a descriptor"),
        )
        .expect("SO_MEMINFO on a socket this process just opened");

        let reported = kernel_drops(&registry.drain(0));
        assert!(
            procfs_drops > 0,
            "the flood must have overrun a {TINY_RECEIVE_BUFFER}-byte buffer"
        );
        assert_eq!(
            reported, procfs_drops as f64,
            "logit.input.kernel.drops must equal /proc/net/udp's drops column for this socket \
             exactly -- it is literally the same sk_drops field"
        );
        assert_eq!(
            u64::from(direct.drops),
            procfs_drops,
            "and so must a direct SO_MEMINFO read, which is what rules out the counter's \
             first-sample arithmetic hiding an index mistake"
        );
    }

    /// This socket's `drops` column in `/proc/net/udp[6]`, found by inode. `None` if procfs is
    /// unreadable or the row isn't there.
    ///
    /// The row layout is fixed by `udp4_format_sock`'s `seq_printf` (`net/ipv4/udp.c`; udp6's
    /// `__ip6_dgram_sock_seq_show` in `net/ipv6/datagram.c` prints the same columns with wider
    /// addresses): whitespace-separated, `inode` is field 9 and `drops` is field 12 and last.
    /// `tx_queue:rx_queue` and `tr:tm->when` are each one colon-joined field, so a row has 13
    /// fields, not 15.
    #[cfg(target_os = "linux")]
    fn proc_net_udp_drops(socket: &UdpSocket) -> Option<u64> {
        use std::os::fd::AsRawFd;

        // `/proc/self/fd/<fd>` reads back as `socket:[<inode>]`, the key procfs's socket tables
        // use; no `fstat`, no `unsafe`.
        let link = std::fs::read_link(format!("/proc/self/fd/{}", socket.as_raw_fd())).ok()?;
        let link = link.to_str()?;
        let inode = link.strip_prefix("socket:[")?.strip_suffix(']')?;

        for table in ["/proc/net/udp", "/proc/net/udp6"] {
            let Ok(contents) = std::fs::read_to_string(table) else {
                continue;
            };
            for row in contents.lines().skip(1) {
                let fields: Vec<&str> = row.split_whitespace().collect();
                if fields.len() < 13 || fields[9] != inode {
                    continue;
                }
                return fields[12].parse().ok();
            }
        }
        None
    }

    /// Drops in the last fraction of a second before the reader stops are still reported.
    ///
    /// The future is polled once up front (the first sample, of a socket nothing has sent to,
    /// parking `read_loop`) and then not at all during the overrun, since it's a local future, not
    /// a spawned task. So every drop is taken while the only sample so far saw zero.
    ///
    /// On resume, [`sample_while`]'s `select!` is `biased` toward the timer, so if the blast
    /// outlasted `KERNEL_SAMPLE_INTERVAL`, a due tick reports some drops and the final sample the
    /// rest. The assertion is on the total.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn the_final_sample_reports_drops_that_happened_just_before_shutdown() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        let mut diag = Diagnostics::default();
        let (socket, _group) =
            bind_socket("127.0.0.1:0", Some(TINY_RECEIVE_BUFFER), &telemetry, &mut diag)
                .await
                .expect("binding an ephemeral port should succeed");
        let addr = socket.local_addr().expect("a bound socket has an address");
        // Depth 1 under `block`, nothing popping: the reader parks in `queue.push` for good, the
        // state this wrapper exists to keep sampling through.
        let queue = test_queue(OverflowPolicy::Block, 1, &Telemetry::default());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let sampled = read_loop_sampled(
            &socket,
            Arc::clone(&queue),
            telemetry.clone(),
            Diagnostics::default(),
            shutdown_rx,
            TEST_POP_BATCH,
        );
        tokio::pin!(sampled);

        // One poll: `select!` polls every arm on its first pass, so the first `sample_once` has
        // run by the time `yield_now` resolves.
        tokio::select! {
            _ = &mut sampled => panic!("the read loop must not finish before shutdown"),
            () = tokio::task::yield_now() => {}
        }
        assert_eq!(
            kernel_drops(&registry.drain(0)),
            0.0,
            "the premise: nothing has been sent yet, so the first sample saw no drops at all"
        );

        // Nothing polls `sampled` until the `await` below, so no one drains the buffer.
        blast(addr, OVERRUN_DATAGRAMS).await;
        shutdown_tx.send(true).expect("receiver should still be alive");
        tokio::time::timeout(Duration::from_secs(5), sampled)
            .await
            .expect("shutdown should stop the read loop promptly")
            .expect("should shut down without error");

        assert!(
            kernel_drops(&registry.drain(0)) > 0.0,
            "drops taken after the last interval sample must still be reported -- that is what \
             the guaranteed final sample is for"
        );
    }

    /// A sampler with nothing to read latches itself off on its first call, so [`sample_while`]
    /// arms no timer.
    #[test]
    fn a_sampler_that_cannot_read_the_counters_disables_itself_on_the_first_sample() {
        let mut sampler = ReceiveBufferSampler {
            // The non-Linux shape: `sockstat::fd_of` has no descriptor to hand back.
            fd: None,
            drops: sockstat::DropCounter::new(),
            telemetry: Telemetry::default(),
            diag: Diagnostics::default(),
            enabled: true,
        };
        assert!(sampler.enabled, "a fresh sampler always tries once");
        sampler.sample_once();
        assert!(!sampler.enabled, "one failed read is enough -- these counters never appear later");
        sampler.sample_once(); // a no-op, which the final sample relies on
        assert!(!sampler.enabled);
    }

    /// With the sampler disabled (every non-Linux build's shape), [`sample_while`] still forwards
    /// the `read` future's result and leaves the queue closed.
    #[tokio::test]
    async fn a_disabled_sampler_still_reads_and_closes_the_queue() {
        let socket = bind_ephemeral().await;
        let queue = test_queue(OverflowPolicy::DropOldest, 10, &Telemetry::default());
        // Already signalled, so `read_loop` returns on its first poll.
        let (_shutdown_tx, shutdown_rx) = watch::channel(true);
        let sampler = ReceiveBufferSampler {
            fd: None,
            drops: sockstat::DropCounter::new(),
            telemetry: Telemetry::default(),
            diag: Diagnostics::default(),
            enabled: true,
        };

        sample_while(
            sampler,
            read_loop(
                &socket,
                Arc::clone(&queue),
                Telemetry::default(),
                Diagnostics::default(),
                shutdown_rx,
                TEST_POP_BATCH,
            ),
            KERNEL_SAMPLE_INTERVAL,
        )
        .await
        .expect("a disabled sampler must not change how the read loop reports its result");

        assert!(
            queue.pop().await.is_none(),
            "the queue must still be closed on the way out -- that is what lets decode_loop finish"
        );
    }

    /// Never finishes, and returns `Pending` only by exhausting its task's coop budget,
    /// self-waking each time: `read_loop`'s shape under a flood. `consume_budget` spends one unit
    /// per call and yields once the 128-unit budget is gone.
    #[cfg(target_os = "linux")]
    async fn burns_its_whole_coop_budget_forever() -> anyhow::Result<()> {
        loop {
            tokio::task::consume_budget().await;
        }
    }

    /// A read future that only yields on coop-budget exhaustion doesn't silence the sampler.
    ///
    /// **Fails with the arms swapped** (read first): the read arm spends all 128 units, the timer
    /// arm's `coop::poll_proceed` returns `Pending` however far past its deadline, and `ticks`
    /// stays at 0.
    ///
    /// Real time, not a paused clock: the task never idles, so auto-advance never engages. The
    /// sampler gets its own spawned task because a `timeout` around it in the test's task would
    /// have its `Sleep` starved by the same budget exhaustion.
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_sampler_keeps_ticking_while_the_read_future_burns_its_whole_coop_budget() {
        const INTERVAL: Duration = Duration::from_millis(10);
        const WINDOW: Duration = Duration::from_millis(60);
        const WINDOWS: usize = 6;

        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        // A real socket, so the sampler stays enabled.
        let socket = bind_ephemeral().await;
        let sampler = ReceiveBufferSampler::new(&socket, telemetry, Diagnostics::default());
        let sampling =
            tokio::spawn(sample_while(sampler, burns_its_whole_coop_budget_forever(), INTERVAL));

        // Discard the opening `sample_once`: it runs before the `select!`, under either ordering.
        tokio::time::sleep(WINDOW).await;
        registry.drain(0);

        let mut ticks = 0;
        for _ in 0..WINDOWS {
            tokio::time::sleep(WINDOW).await;
            // A gauge is last-write-wins per drain: present means at least one sample ran.
            if gauge(&registry.drain(0), "logit.input.receive_buffer.used.bytes").is_some() {
                ticks += 1;
            }
        }
        sampling.abort();

        assert!(
            ticks >= 3,
            "the sampler must keep reporting while the read side is saturated -- that is the only \
             time its numbers matter. Saw samples in {ticks} of {WINDOWS} windows of {WINDOW:?}, \
             at an interval of {INTERVAL:?}; 0 means the timer arm is starved behind the read arm \
             by the coop budget"
        );
    }

    // -- the pure halves of the `recvmmsg` closure (`miri`'s only way in) ------------------------

    /// Everything about [`build_headers`]/[`harvest_headers`] a pointer-provenance checker can
    /// see, with no syscall in reach (inventory entry `NET-01`).
    ///
    /// `miri` has no shim for `recvmmsg(2)` or any socket call, so [`BatchReader::read_batch`] as a
    /// whole is out of its reach (`docs/adr/out-of-ci-unsafe-verification.md`). The build /
    /// syscall / harvest split puts every pointer decision (the `u64`-storage cast, the
    /// `size_of`-strided `add(i)`, the slot arithmetic, the re-initialization, and the read-back of
    /// two kernel-written fields) on the sides `miri` can execute. `script/unsafe-check miri` runs
    /// this module; the tests are ordinary tests too, so a regression fails in CI.
    ///
    /// **The tests play the kernel**, writing through a `*mut libc::mmsghdr` derived from the same
    /// `&mut [u64]` the syscall shim derives its pointer from: the aliasing question production
    /// relies on under Stacked/Tree Borrows.
    #[cfg(target_os = "linux")]
    mod batch_reader_helpers {
        use super::super::{build_headers, harvest_headers, HDR_WORDS, IOV_WORDS};
        use super::MAX_DATAGRAM_BYTES;

        /// Every `vlen` worth checking: both ends of [`BatchReader::new`]'s clamp (`1` and
        /// [`MAX_READ_BATCH`]), the default 64 and 63 (so an off-by-one in the loop bound shows as
        /// a missing or extra header), and `2`, the smallest `vlen` where slot disjointness means
        /// anything.
        const VLENS: [usize; 5] = [1, 2, 63, 64, 1024];

        /// A small slot size: `1024 * MAX_DATAGRAM_BYTES` is a 67 MB zeroed slab, cheap for a
        /// release binary but slow for an interpreter tracking every byte's initialization.
        /// [`build_headers`] takes the size as a parameter and isn't sensitive to its value;
        /// `slot_bytes_matching_production` covers the real stride, just not crossed with the
        /// largest `vlen` under `miri`.
        const SMALL_SLOT: usize = 64;

        /// One header's fields, read back the way the kernel sees them: through a
        /// `*mut libc::mmsghdr` derived from `hdr_words`, following `msg_iov` into the `iovec`
        /// array rather than re-deriving that pointer from `iov_words`.
        ///
        /// Following the stored pointer is the point: it's the read that fails if the provenance
        /// `build_headers` hands the kernel has been invalidated. So nothing here may touch
        /// `iov_words` or `slots` again; a reborrow would pop the tag under test.
        struct HeaderView {
            iov_base: usize,
            iov_len: usize,
            msg_iovlen: usize,
            msg_name: usize,
            msg_namelen: u32,
            msg_control: usize,
            msg_controllen: usize,
            msg_len: u32,
            msg_flags: i32,
        }

        // `msg_iovlen`/`msg_controllen` are `size_t` in rust-`libc`'s `linux-gnu` `msghdr` and
        // narrower on some other targets; the casts are that portability, even where clippy sees
        // through them on this target.
        #[allow(clippy::unnecessary_cast)]
        fn view(hdr_words: &mut [u64], vlen: usize) -> Vec<HeaderView> {
            let hdrs = hdr_words.as_mut_ptr().cast::<libc::mmsghdr>();
            (0..vlen)
                .map(|i| {
                    // SAFETY: `i < vlen` and `hdr_words` holds `vlen * HDR_WORDS` words, so
                    // `hdrs.add(i)` is in bounds and aligned. `build_headers` initialized every
                    // one of these headers, and its `msg_iov` points into a live `iovec` array
                    // that this function deliberately does not reborrow.
                    unsafe {
                        let hdr = &*hdrs.add(i);
                        let iov = &*hdr.msg_hdr.msg_iov;
                        HeaderView {
                            iov_base: iov.iov_base as usize,
                            iov_len: iov.iov_len,
                            msg_iovlen: hdr.msg_hdr.msg_iovlen as usize,
                            msg_name: hdr.msg_hdr.msg_name as usize,
                            msg_namelen: hdr.msg_hdr.msg_namelen,
                            msg_control: hdr.msg_hdr.msg_control as usize,
                            msg_controllen: hdr.msg_hdr.msg_controllen as usize,
                            msg_len: hdr.msg_len,
                            msg_flags: hdr.msg_hdr.msg_flags,
                        }
                    }
                })
                .collect()
        }

        /// What `recvmmsg(2)` writes back into the first `n` headers: `msg_len` and
        /// `msg_hdr.msg_flags`, which [`harvest_headers`] reads, and `msg_namelen`/
        /// `msg_controllen`, which `____sys_recvmsg` (`net/socket.c`) writes on every call. Those
        /// two get nonsense values, so a [`build_headers`] that failed to re-zero a header fails
        /// the re-initialization test.
        fn play_kernel(hdr_words: &mut [u64], n: usize, lens: &[u32], flags: &[i32]) {
            let hdrs = hdr_words.as_mut_ptr().cast::<libc::mmsghdr>();
            for i in 0..n {
                // SAFETY: `i < n <= vlen` and `hdr_words` holds `vlen * HDR_WORDS` words, so
                // `hdrs.add(i)` is in bounds and aligned; `build_headers` initialized it, so every
                // field is a live, valid value to assign over. This is the same pointer type,
                // derived from the same borrow, that `recvmmsg_into` hands the kernel.
                unsafe {
                    (*hdrs.add(i)).msg_len = lens[i];
                    (*hdrs.add(i)).msg_hdr.msg_flags = flags[i];
                    (*hdrs.add(i)).msg_hdr.msg_namelen = 0xDEAD;
                    (*hdrs.add(i)).msg_hdr.msg_controllen = 0xBEEF;
                }
            }
        }

        /// Fresh backing buffers for one `vlen`, sized as `BatchReader::new` sizes them.
        fn buffers(vlen: usize, slot_bytes: usize) -> (Vec<u8>, Vec<u64>, Vec<u64>) {
            (
                vec![0u8; vlen * slot_bytes],
                vec![0u64; vlen * IOV_WORDS],
                vec![0u64; vlen * HDR_WORDS],
            )
        }

        /// A freshly-built header array: one `iov` per header, pointing at its own slot, every
        /// slot inside the slab and `slot_bytes` long, and no address, control buffer, or
        /// returned length carried over.
        fn assert_freshly_built(views: &[HeaderView], slab: (usize, usize), slot_bytes: usize) {
            let (slab_start, slab_len) = slab;
            let mut seen: Vec<(usize, usize)> = Vec::with_capacity(views.len());
            for (i, v) in views.iter().enumerate() {
                assert_eq!(v.msg_iovlen, 1, "header {i} must describe exactly one iovec");
                assert_eq!(v.iov_len, slot_bytes, "header {i}'s slot must be a whole slot long");
                assert_eq!(
                    v.iov_base,
                    slab_start + i * slot_bytes,
                    "header {i} must point at slot {i}, not at some other slot"
                );
                assert!(
                    v.iov_base >= slab_start && v.iov_base + v.iov_len <= slab_start + slab_len,
                    "header {i}'s slot [{}, {}) must be wholly inside the slab [{slab_start}, {})",
                    v.iov_base,
                    v.iov_base + v.iov_len,
                    slab_start + slab_len
                );
                assert_eq!(v.msg_name, 0, "header {i} must ask for no source address");
                assert_eq!(v.msg_namelen, 0, "header {i} must ask for no source address");
                assert_eq!(v.msg_control, 0, "header {i} must ask for no ancillary data");
                assert_eq!(v.msg_controllen, 0, "header {i} must ask for no ancillary data");
                assert_eq!(v.msg_len, 0, "header {i} must start with no returned length");
                assert_eq!(v.msg_flags, 0, "header {i} must start with no returned flags");
                seen.push((v.iov_base, v.iov_base + v.iov_len));
            }
            for (i, a) in seen.iter().enumerate() {
                for (j, b) in seen.iter().enumerate().skip(i + 1) {
                    assert!(
                        a.1 <= b.0 || b.1 <= a.0,
                        "slots {i} [{}, {}) and {j} [{}, {}) overlap",
                        a.0,
                        a.1,
                        b.0,
                        b.1
                    );
                }
            }
        }

        /// The construction half, at every `vlen` the clamp can produce.
        #[test]
        fn every_header_describes_its_own_slot_and_asks_for_nothing_else() {
            for vlen in VLENS {
                let (mut slots, mut iov_words, mut hdr_words) = buffers(vlen, SMALL_SLOT);
                let slab = (slots.as_ptr() as usize, slots.len());

                build_headers(&mut slots, SMALL_SLOT, &mut iov_words, &mut hdr_words, vlen);

                let views = view(&mut hdr_words, vlen);
                assert_eq!(views.len(), vlen);
                assert_freshly_built(&views, slab, SMALL_SLOT);
            }
        }

        /// The same construction at the production stride, `MAX_DATAGRAM_BYTES`.
        ///
        /// `1024` is excluded under `miri` only, for cost; the test above covers 1024 at a
        /// smaller stride.
        #[test]
        fn slot_bytes_matching_production() {
            #[cfg(miri)]
            let vlens: &[usize] = &[1, 2, 63, 64];
            #[cfg(not(miri))]
            let vlens: &[usize] = &VLENS;

            for &vlen in vlens {
                let (mut slots, mut iov_words, mut hdr_words) = buffers(vlen, MAX_DATAGRAM_BYTES);
                let slab = (slots.as_ptr() as usize, slots.len());

                build_headers(&mut slots, MAX_DATAGRAM_BYTES, &mut iov_words, &mut hdr_words, vlen);

                let views = view(&mut hdr_words, vlen);
                assert_freshly_built(&views, slab, MAX_DATAGRAM_BYTES);
            }
        }

        /// Whatever the kernel left in a header is gone after the next [`build_headers`], so
        /// `____sys_recvmsg`'s writeback can't be read as the next call's.
        #[test]
        fn a_rebuild_after_a_kernel_writeback_fully_reinitialises_every_header() {
            for vlen in VLENS {
                let (mut slots, mut iov_words, mut hdr_words) = buffers(vlen, SMALL_SLOT);
                let slab = (slots.as_ptr() as usize, slots.len());

                build_headers(&mut slots, SMALL_SLOT, &mut iov_words, &mut hdr_words, vlen);
                let lens: Vec<u32> = (0..vlen).map(|i| (i as u32) + 7).collect();
                let flags: Vec<i32> = (0..vlen).map(|_| libc::MSG_TRUNC).collect();
                play_kernel(&mut hdr_words, vlen, &lens, &flags);

                // As the closure's next `FnMut` call does: same buffers, no clearing in between.
                build_headers(&mut slots, SMALL_SLOT, &mut iov_words, &mut hdr_words, vlen);

                let views = view(&mut hdr_words, vlen);
                assert_freshly_built(&views, slab, SMALL_SLOT);
            }
        }

        /// Harvest copies the first `n` headers' values and touches nothing past `n`.
        #[test]
        fn harvest_copies_the_first_n_headers_and_nothing_past_them() {
            const UNTOUCHED_LEN: u32 = 0xA5A5_A5A5;
            const UNTOUCHED_FLAG: i32 = 0x5A5A_5A5A;

            for vlen in VLENS {
                // None, one, one short of the batch, and the whole batch.
                for n in [0, 1, vlen.saturating_sub(1), vlen] {
                    let (mut slots, mut iov_words, mut hdr_words) = buffers(vlen, SMALL_SLOT);
                    build_headers(&mut slots, SMALL_SLOT, &mut iov_words, &mut hdr_words, vlen);

                    let written_lens: Vec<u32> =
                        (0..vlen).map(|i| ((i * 13) % SMALL_SLOT) as u32).collect();
                    // Alternating, so reading the wrong header's flags can't pass by accident.
                    let written_flags: Vec<i32> =
                        (0..vlen).map(|i| if i % 2 == 0 { libc::MSG_TRUNC } else { 0 }).collect();
                    play_kernel(&mut hdr_words, n, &written_lens, &written_flags);

                    let mut lens = vec![UNTOUCHED_LEN; vlen];
                    let mut flags = vec![UNTOUCHED_FLAG; vlen];
                    harvest_headers(&mut hdr_words, n, &mut lens, &mut flags);

                    for i in 0..n {
                        assert_eq!(
                            lens[i], written_lens[i],
                            "vlen {vlen}, n {n}: msg_len {i} must come back exactly"
                        );
                        assert_eq!(
                            flags[i], written_flags[i],
                            "vlen {vlen}, n {n}: msg_flags {i} must come back exactly"
                        );
                    }
                    for i in n..vlen {
                        assert_eq!(
                            lens[i], UNTOUCHED_LEN,
                            "vlen {vlen}, n {n}: entry {i} is past the batch and must be untouched"
                        );
                        assert_eq!(
                            flags[i], UNTOUCHED_FLAG,
                            "vlen {vlen}, n {n}: entry {i} is past the batch and must be untouched"
                        );
                    }
                }
            }
        }

        /// The full provenance chain in production's order: build, write into each slot through
        /// the header's own `iov_base` (as the kernel does), harvest, then read the slab back
        /// through an ordinary borrow.
        ///
        /// Under Stacked/Tree Borrows this fails if the slab pointer's tag is invalidated between
        /// construction and use, which is why nothing touches `slots` or `iov_words` before the
        /// last write. The later fresh borrow of `slots` pops those tags after the "kernel" is
        /// done.
        #[test]
        fn writing_through_each_headers_own_iov_lands_in_that_headers_own_slot() {
            for vlen in VLENS {
                let (mut slots, mut iov_words, mut hdr_words) = buffers(vlen, SMALL_SLOT);
                build_headers(&mut slots, SMALL_SLOT, &mut iov_words, &mut hdr_words, vlen);

                let hdrs = hdr_words.as_mut_ptr().cast::<libc::mmsghdr>();
                for i in 0..vlen {
                    // SAFETY: `i < vlen`, so `hdrs.add(i)` is an in-bounds, aligned, initialized
                    // header. Its `msg_iov` points at a live `iovec` whose `iov_base` addresses
                    // `iov_len` writable bytes of the slab -- the exact contract `build_headers`
                    // establishes for the kernel, exercised here by the one writer that can
                    // actually be observed under `miri`. One byte per slot is enough to pin the
                    // slot the pointer resolves to; writing the whole slot would only be slower.
                    unsafe {
                        let iov = *(*hdrs.add(i)).msg_hdr.msg_iov;
                        assert_eq!(iov.iov_len, SMALL_SLOT);
                        iov.iov_base.cast::<u8>().write((i % 251) as u8 + 1);
                    }
                }
                let lens: Vec<u32> = vec![1; vlen];
                let flags: Vec<i32> = vec![0; vlen];
                play_kernel(&mut hdr_words, vlen, &lens, &flags);

                let mut harvested_lens = vec![0u32; vlen];
                let mut harvested_flags = vec![0i32; vlen];
                harvest_headers(&mut hdr_words, vlen, &mut harvested_lens, &mut harvested_flags);
                assert!(harvested_lens.iter().all(|&len| len == 1));

                for i in 0..vlen {
                    let start = i * SMALL_SLOT;
                    assert_eq!(
                        slots[start],
                        (i % 251) as u8 + 1,
                        "the byte written through header {i}'s iov must land at the head of slot {i}"
                    );
                    assert!(
                        slots[start + 1..start + SMALL_SLOT].iter().all(|&b| b == 0),
                        "and nothing else in slot {i} may be disturbed"
                    );
                }
            }
        }

        /// Both helpers panic on a buffer too small for their `vlen`/`n` rather than overrunning
        /// it.
        #[test]
        fn a_buffer_too_small_for_the_batch_panics_rather_than_overrunning() {
            fn must_panic(name: &str, case: impl FnOnce()) {
                let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(case));
                assert!(caught.is_err(), "{name}: an undersized buffer must panic");
            }

            must_panic("slots", || {
                let (mut s, mut i, mut h) = buffers(4, SMALL_SLOT);
                s.truncate(4 * SMALL_SLOT - 1);
                build_headers(&mut s, SMALL_SLOT, &mut i, &mut h, 4);
            });
            must_panic("iov_words", || {
                let (mut s, mut i, mut h) = buffers(4, SMALL_SLOT);
                i.truncate(4 * IOV_WORDS - 1);
                build_headers(&mut s, SMALL_SLOT, &mut i, &mut h, 4);
            });
            must_panic("hdr_words", || {
                let (mut s, mut i, mut h) = buffers(4, SMALL_SLOT);
                h.truncate(4 * HDR_WORDS - 1);
                build_headers(&mut s, SMALL_SLOT, &mut i, &mut h, 4);
            });
            must_panic("harvest past the headers", || {
                let (mut s, mut i, mut h) = buffers(4, SMALL_SLOT);
                build_headers(&mut s, SMALL_SLOT, &mut i, &mut h, 4);
                let (mut lens, mut flags) = (vec![0u32; 8], vec![0i32; 8]);
                harvest_headers(&mut h, 5, &mut lens, &mut flags);
            });
            must_panic("harvest past the outputs", || {
                let (mut s, mut i, mut h) = buffers(8, SMALL_SLOT);
                build_headers(&mut s, SMALL_SLOT, &mut i, &mut h, 8);
                let (mut lens, mut flags) = (vec![0u32; 4], vec![0i32; 4]);
                harvest_headers(&mut h, 8, &mut lens, &mut flags);
            });
        }
    }

    // -- batched reads (`BatchReader`, `read_batch`) --------------------------------------------

    /// Every counter point named `name` in `events`, summed.
    fn counter(events: &[Event], name: &str) -> f64 {
        events
            .iter()
            .flat_map(|event| &event.metrics)
            .filter(|m| logit_core::interner::resolve(m.name) == name)
            .map(|m| match &m.kind {
                logit_core::MetricKind::Sum(sum) => sum.value,
                other => panic!("{name} must be a counter, got {other:?}"),
            })
            .sum()
    }

    /// Runs one listener over a real loopback socket at `read_batch`, sends `payloads` in order
    /// from one sender socket, and returns the delivered datagram bytes in delivery order (the
    /// listener's telemetry lands in `registry`).
    ///
    /// **No sleeps and no unbounded waits.** The driver waits for `payloads.len()` deliveries,
    /// each under a `timeout`, then signals shutdown. `batch_max_events: 1` makes one delivery per
    /// datagram and the channel is FIFO, so the received sequence is the decode order.
    ///
    /// One sender socket makes the ordering assertion meaningful: the kernel preserves the order
    /// of datagrams from one socket to one loopback peer, but not across senders.
    async fn deliver_burst(
        read_batch: usize,
        payloads: &[Vec<u8>],
        registry: &logit_core::Registry,
    ) -> Vec<Vec<u8>> {
        let telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        let mut diag = Diagnostics::default();
        // A megabyte requested (doubled, maybe clamped by `net.core.rmem_max`), so a burst sent
        // before anything drains it can't overrun the socket.
        let (socket, _group) =
            bind_socket("127.0.0.1:0", Some(1024 * 1024), &telemetry, &mut diag).await.unwrap();
        let addr = socket.local_addr().expect("a bound socket has an address");
        let queue = test_queue(OverflowPolicy::DropOldest, payloads.len() * 2, &telemetry);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (fanout, mut rx) = recording_fanout(8);
        let mut decoder = TestDecoder::new();

        let driver = async {
            let sender = bind_ephemeral().await;
            for payload in payloads {
                sender.send_to(payload, addr).await.expect("loopback send should succeed");
            }
            let mut received = Vec::with_capacity(payloads.len());
            for i in 0..payloads.len() {
                // Bounded, so a lost datagram (most plausibly a `rmem_max` clamp) fails with a
                // count instead of hanging.
                let delivered = tokio::time::timeout(Duration::from_secs(10), rx.recv())
                    .await
                    .unwrap_or_else(|_| {
                        panic!(
                            "only {i} of {} datagrams were delivered within 10s -- one was lost \
                             between the sender and the fanout",
                            payloads.len()
                        )
                    })
                    .expect("the fanout channel should stay open");
                let batch = unwrap_batch(delivered);
                assert_eq!(batch.events.len(), 1, "batch_max_events: 1 means one event per send");
                received.push(payload_bytes(&batch.events[0]));
            }
            shutdown_tx.send(true).expect("receiver should still be alive");
            received
        };

        let (read_result, (), received) = tokio::join!(
            read_loop(
                &socket,
                Arc::clone(&queue),
                telemetry.clone(),
                Diagnostics::default(),
                shutdown_rx,
                read_batch
            ),
            decode_loop(
                &mut decoder,
                Arc::clone(&queue),
                fanout,
                BatchingConfig {
                    max_events: 1,
                    max_bytes: u64::MAX,
                    flush_interval: Duration::ZERO,
                    pop_batch: read_batch,
                },
                telemetry,
                Diagnostics::default(),
            ),
            driver,
        );
        read_result.expect("should shut down cleanly");
        received
    }

    /// [`payload`]'s byte-exact twin: `String::from_utf8_lossy` would rewrite invalid UTF-8.
    fn payload_bytes(event: &Event) -> Vec<u8> {
        match event.attributes.get("payload") {
            Some(logit_core::Value::Str(bytes)) => bytes.to_vec(),
            other => panic!("expected a payload attribute, got {other:?}"),
        }
    }

    fn numbered_payloads(count: usize) -> Vec<Vec<u8>> {
        (0..count).map(|i| format!("msg-{i}").into_bytes()).collect()
    }

    /// A burst several batches deep arrives complete and in send order, through the batch `Vec`,
    /// `push_many`, `pop_many`, and the decode iteration.
    #[tokio::test]
    async fn a_two_hundred_datagram_burst_is_delivered_complete_and_in_order() {
        let payloads = numbered_payloads(200);
        let registry = logit_core::Registry::new();
        let received = deliver_burst(64, &payloads, &registry).await;
        assert_eq!(received, payloads, "every datagram, exactly once, in the order it was sent");
    }

    /// `read_batch: 1` (`vlen = 1`, not a second code path) and 64 yield the same event stream.
    #[tokio::test]
    async fn read_batch_one_and_sixty_four_yield_identical_event_streams() {
        let payloads = numbered_payloads(150);
        let registry = logit_core::Registry::new();
        let one = deliver_burst(1, &payloads, &registry).await;
        let sixty_four = deliver_burst(64, &payloads, &registry).await;
        assert_eq!(one, payloads, "read_batch: 1 must deliver the whole burst in order");
        assert_eq!(sixty_four, one, "the batch size must not be observable in the event stream");
    }

    /// Byte-exact across the legal size range in one batch: zero-length, small, and one at the
    /// 65,507-byte maximum, which proves each slot holds a whole datagram.
    #[tokio::test]
    async fn datagrams_of_mixed_sizes_including_empty_and_near_maximum_survive_byte_exact() {
        let payloads: Vec<Vec<u8>> = vec![
            Vec::new(),
            b"x".to_vec(),
            vec![b'a'; 1500],
            Vec::new(),
            vec![b'b'; 60 * 1024],
            b"tail".to_vec(),
            vec![b'c'; MAX_DATAGRAM_BYTES],
        ];
        let registry = logit_core::Registry::new();
        let received = deliver_burst(64, &payloads, &registry).await;
        let sizes: Vec<usize> = received.iter().map(Vec::len).collect();
        let expected_sizes: Vec<usize> = payloads.iter().map(Vec::len).collect();
        assert_eq!(sizes, expected_sizes, "every datagram's length must survive the batch read");
        assert_eq!(received, payloads, "and so must every byte of it");
    }

    /// For a known burst, `logit.input.datagrams` and `.datagram.bytes` are exact and
    /// `logit.input.reads` never exceeds datagrams (a read returns at least one).
    #[tokio::test]
    async fn the_read_counter_never_exceeds_the_datagram_counter_and_both_are_exact() {
        const BURST: usize = 200;
        let payloads = numbered_payloads(BURST);
        let registry = logit_core::Registry::new();
        let received = deliver_burst(64, &payloads, &registry).await;
        assert_eq!(received.len(), BURST);

        let events = registry.drain(0);
        let datagrams = counter(&events, "logit.input.datagrams");
        let reads = counter(&events, "logit.input.reads");
        let bytes = counter(&events, "logit.input.datagram.bytes");
        let expected_bytes: usize = payloads.iter().map(Vec::len).sum();
        assert_eq!(datagrams, BURST as f64, "every datagram read is counted exactly once");
        assert_eq!(bytes, expected_bytes as f64, "and so is every byte of it");
        assert!(reads >= 1.0, "reading {BURST} datagrams takes at least one syscall, got {reads}");
        assert!(
            reads <= datagrams,
            "a read that returned nothing is not a read: {reads} reads for {datagrams} datagrams"
        );
    }

    /// Every datagram gets its own, strictly increasing `received_at` (why it matters: the
    /// "`+ i` is not cosmetic" note on `BatchReader::read_batch`).
    ///
    /// **What the assertion rests on.** Within one batch, strict increase holds by construction:
    /// `base + i` from one clock read. Across batches it doesn't: `now_nanos()` is the wall clock
    /// (`received_at` is a wall-clock timestamp), which can step backwards by more than the `+ i`
    /// offset between two batches. Tracked in `docs/known-gaps.md`.
    ///
    /// Not a flake worth tightening: it takes a backwards step inside this burst's
    /// sub-millisecond window, and narrowing to within-batch windows would lose the cross-batch
    /// coverage that catches a one-stamp-per-syscall regression.
    #[tokio::test]
    async fn every_datagram_in_a_batch_gets_its_own_received_at() {
        const BURST: usize = 200;

        let telemetry = Telemetry::default();
        let mut diag = Diagnostics::default();
        let (socket, _group) =
            bind_socket("127.0.0.1:0", Some(1024 * 1024), &telemetry, &mut diag).await.unwrap();
        let addr = socket.local_addr().expect("a bound socket has an address");
        let queue = test_queue(OverflowPolicy::DropOldest, BURST * 2, &Telemetry::default());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let sender = bind_ephemeral().await;
        for i in 0..BURST {
            sender.send_to(format!("msg-{i}").as_bytes(), addr).await.expect("loopback send");
        }

        // Pop until the whole burst is out; the data is the synchronization.
        let read = read_loop(
            &socket,
            Arc::clone(&queue),
            telemetry,
            Diagnostics::default(),
            shutdown_rx,
            64,
        );
        tokio::pin!(read);
        let mut stamps: Vec<i64> = Vec::with_capacity(BURST);
        while stamps.len() < BURST {
            tokio::select! {
                _ = &mut read => panic!("the read loop must not finish before shutdown"),
                datagram = queue.pop() => {
                    stamps.push(datagram.expect("the queue is open").received_at);
                }
            }
        }
        shutdown_tx.send(true).expect("receiver should still be alive");
        tokio::time::timeout(Duration::from_secs(5), read)
            .await
            .expect("shutdown should stop the read loop promptly")
            .expect("should shut down without error");

        for pair in stamps.windows(2) {
            assert!(
                pair[1] > pair[0],
                "received_at must strictly increase in arrival order, got {} then {} somewhere in \
                 {BURST} datagrams",
                pair[0],
                pair[1]
            );
        }
    }

    /// A 65,527-byte IPv6 datagram is delivered truncated to a slot and counted as
    /// `logit.input.datagrams.truncated`.
    ///
    /// **Skips rather than fails where IPv6 loopback isn't usable**: a container with no `::1`, or
    /// whose loopback won't carry a fragmented 65 KB datagram.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn an_oversized_ipv6_datagram_is_delivered_truncated_and_counted() {
        /// The largest IPv6 payload, 20 bytes past a slot.
        const IPV6_MAX_PAYLOAD: usize = 65_527;

        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        let mut diag = Diagnostics::default();
        let bound = bind_socket("[::1]:0", Some(4 * 1024 * 1024), &telemetry, &mut diag).await;
        let Ok((socket, _group)) = bound else {
            println!("skipping: this environment has no usable IPv6 loopback");
            return;
        };
        let addr = socket.local_addr().expect("a bound socket has an address");

        let sender = match UdpSocket::bind("[::1]:0").await {
            Ok(sender) => sender,
            Err(err) => {
                println!("skipping: cannot bind an IPv6 sender here ({err})");
                return;
            }
        };
        let payload = vec![b'z'; IPV6_MAX_PAYLOAD];
        if let Err(err) = sender.send_to(&payload, addr).await {
            // EMSGSIZE, or a loopback that won't fragment: nothing to do with this code path.
            println!("skipping: cannot send a {IPV6_MAX_PAYLOAD}-byte datagram here ({err})");
            return;
        }

        let queue = test_queue(OverflowPolicy::DropOldest, 8, &Telemetry::default());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let read = read_loop(
            &socket,
            Arc::clone(&queue),
            telemetry,
            Diagnostics::default(),
            shutdown_rx,
            64,
        );
        tokio::pin!(read);
        let datagram = tokio::select! {
            _ = &mut read => panic!("the read loop must not finish before shutdown"),
            datagram = queue.pop() => datagram.expect("the queue is open"),
        };
        shutdown_tx.send(true).expect("receiver should still be alive");
        tokio::time::timeout(Duration::from_secs(5), read)
            .await
            .expect("shutdown should stop the read loop promptly")
            .expect("should shut down without error");

        assert_eq!(
            datagram.bytes.len(),
            MAX_DATAGRAM_BYTES,
            "a datagram longer than a slot is delivered as far as the slot holds"
        );
        assert!(
            datagram.bytes.iter().all(|&b| b == b'z'),
            "and every byte of what was delivered is the payload's own"
        );
        let events = registry.drain(0);
        assert_eq!(
            counter(&events, "logit.input.datagrams.truncated"),
            1.0,
            "the 20 bytes the slot could not hold must be counted, not silently dropped"
        );
        assert_eq!(
            counter(&events, "logit.input.datagrams"),
            1.0,
            "a truncated datagram is still a datagram -- it is delivered, so it is counted as one"
        );
    }

    /// A fatal read error ends `read_loop`, closes the queue, and names the syscall and socket.
    ///
    /// **A real, deterministic, unprivileged non-`EAGAIN` error, with no fault injection.** Every
    /// errno an unconnected UDP socket can produce on receive is unreachable (`bind_one`'s doc),
    /// needs privilege (`ss -K` -> `ECONNABORTED`), or needs `strace -e inject=`. A pipe with a
    /// byte in it is readable but not a socket: `async_io` runs the closure on the first poll, and
    /// `recvmmsg(2)` returns `ENOTSOCK` every time.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_fatal_read_error_closes_the_queue_and_names_the_syscall_and_the_socket() {
        use std::os::fd::FromRawFd;

        let mut fds = [0 as libc::c_int; 2];
        // SAFETY: `pipe2(2)` writes two descriptors into the two-element array it is handed, and
        // `fds` is exactly that. `O_NONBLOCK` is set here rather than with a second `fcntl`
        // because tokio requires a non-blocking descriptor for `from_std`.
        let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK) };
        assert_eq!(rc, 0, "pipe2(2) failed: {}", std::io::Error::last_os_error());
        let (read_fd, write_fd) = (fds[0], fds[1]);

        // One byte, so the read end is readable and `async_io` goes straight to the closure.
        // SAFETY: `write_fd` is a live descriptor from the `pipe2` above; the source is a
        // one-byte buffer this frame owns and the length matches it exactly.
        let written = unsafe { libc::write(write_fd, c"x".as_ptr().cast(), 1) };
        assert_eq!(written, 1, "writing to the pipe failed: {}", std::io::Error::last_os_error());

        // SAFETY: `read_fd` is a live, owned descriptor that this test never uses again by number
        // -- `UdpSocket` takes sole ownership of it here and closes it exactly once, on drop. It
        // is deliberately *not* a socket; that is the condition under test, and passing a
        // non-socket descriptor is an `ENOTSOCK` at the syscall, not undefined behaviour.
        let not_a_socket = unsafe { std::net::UdpSocket::from_raw_fd(read_fd) };
        let socket = tokio::net::UdpSocket::from_std(not_a_socket)
            .expect("tokio registers any pollable non-blocking descriptor");

        let queue = test_queue(OverflowPolicy::DropOldest, 10, &Telemetry::default());
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let err = tokio::time::timeout(
            Duration::from_secs(5),
            read_loop(
                &socket,
                Arc::clone(&queue),
                Telemetry::default(),
                Diagnostics::default(),
                shutdown_rx,
                64,
            ),
        )
        .await
        .expect("a fatal read error must end the loop, not hang it")
        .expect_err("recvmmsg(2) on a pipe is ENOTSOCK, which is fatal to the listener");

        // SAFETY: `write_fd` is still the live descriptor `pipe2` returned; nothing else owns it
        // and it is not used again after this.
        unsafe { libc::close(write_fd) };

        let rendered = format!("{err:#}");
        assert!(
            rendered.contains(READ_SYSCALL),
            "a fatal read must name the syscall an operator has to go looking for, got: {rendered}"
        );
        assert!(
            rendered.contains("listener socket"),
            "and say which socket it was reading, got: {rendered}"
        );
        assert_eq!(
            err.downcast_ref::<std::io::Error>().and_then(std::io::Error::raw_os_error),
            Some(libc::ENOTSOCK),
            "the original errno must survive as the error's cause, not be flattened into a string"
        );
        assert!(
            queue.pop().await.is_none(),
            "a fatal read must still close the queue on the way out -- that is the only signal \
             decode_loop has that nothing more will arrive"
        );
    }

    /// `ENOSYS`/`EPERM` (a seccomp profile refusing the syscall) get the syscall, the socket, and
    /// the `read_batch: 1` hint; other errnos get no guessed cause. Tested at the seam: forcing the
    /// real syscall needs `script/unsafe-check inject`, which is out of CI.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_sandbox_blocked_syscall_is_named_along_with_why_read_batch_one_would_not_help() {
        let socket = bind_ephemeral().await;
        let addr = socket.local_addr().expect("a bound socket has an address");

        for errno in [libc::ENOSYS, libc::EPERM] {
            let err = describe_read_failure(&socket, std::io::Error::from_raw_os_error(errno))
                .to_string();
            assert!(err.contains(READ_SYSCALL), "errno {errno}: must name the syscall, got: {err}");
            assert!(
                err.contains(&addr.to_string()),
                "errno {errno}: must name the bound socket, got: {err}"
            );
            assert!(
                err.contains("seccomp") && err.contains("read_batch: 1"),
                "errno {errno}: must say what blocks the call and that the obvious config \
                 workaround is not one, got: {err}"
            );
        }

        // Any other errno gets the syscall and the address only.
        let plain = describe_read_failure(&socket, std::io::Error::from_raw_os_error(libc::EBADF))
            .to_string();
        assert!(plain.contains(READ_SYSCALL) && plain.contains(&addr.to_string()));
        assert!(
            !plain.contains("seccomp"),
            "an unrelated errno must not be guessed at, got: {plain}"
        );
    }

    /// Shutdown during a blocked `push_many` exits promptly and closes the queue, and the rest of
    /// the batch is counted `datagrams.dropped{reason="shutdown"}`: every datagram read is either
    /// queued or counted, in datagrams and in bytes.
    ///
    /// No sleep: the `Block` queue of 4 is pre-filled to 3, so the read half places one datagram
    /// and parks. The loop polls until `logit.input.reads` shows a batch was read (bounded, so a
    /// regression fails rather than hangs).
    #[tokio::test]
    async fn shutdown_while_a_batch_is_mid_push_exits_promptly_and_closes_the_queue() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        let mut diag = Diagnostics::default();
        let (socket, _group) =
            bind_socket("127.0.0.1:0", Some(1024 * 1024), &telemetry, &mut diag).await.unwrap();
        let addr = socket.local_addr().expect("a bound socket has an address");
        let queue = test_queue(OverflowPolicy::Block, 4, &telemetry);
        for i in 0..3u32 {
            queue.push(Datagram { bytes: Bytes::from(format!("pre-{i}")), received_at: 0 }).await;
        }

        // More than a batch, so the read half holds a remainder it can't place.
        let sender = bind_ephemeral().await;
        for i in 0..200u32 {
            sender.send_to(format!("msg-{i}").as_bytes(), addr).await.expect("loopback send");
        }

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let read = read_loop(
            &socket,
            Arc::clone(&queue),
            telemetry.clone(),
            Diagnostics::default(),
            shutdown_rx,
            64,
        );
        tokio::pin!(read);

        let mut events = Vec::new();
        for _ in 0..10_000 {
            tokio::select! {
                _ = &mut read => panic!("the read loop must not finish before shutdown"),
                () = tokio::task::yield_now() => {}
            }
            events.extend(registry.drain(0));
            if counter(&events, "logit.input.reads") > 0.0 {
                break;
            }
        }
        assert!(
            counter(&events, "logit.input.reads") > 0.0,
            "the read half never got a batch off the socket -- test premise"
        );

        shutdown_tx.send(true).expect("receiver should still be alive");
        tokio::time::timeout(Duration::from_secs(5), read)
            .await
            .expect("a blocked push_many must be cancelled by shutdown, not waited out")
            .expect("should shut down without error");

        let mut placed = Vec::new();
        while let Some(datagram) = queue.pop().await {
            if !datagram.bytes.starts_with(b"pre-") {
                placed.push(datagram);
            }
        }
        assert_eq!(placed.len(), 1, "the read half placed one datagram before the queue was full");
        assert!(
            queue.pop().await.is_none(),
            "the queue must be closed on the way out -- that is what lets decode_loop finish"
        );

        events.extend(registry.drain(0));
        let placed_bytes: u64 = placed.iter().map(|datagram| datagram.bytes.len() as u64).sum();
        let read = counter(&events, "logit.input.datagrams");
        let dropped = shutdown_drops(&events);
        assert!(dropped.0 > 0.0, "the rest of the batch must be counted, got {dropped:?}");
        assert_eq!(read, placed.len() as f64 + dropped.0, "datagrams read = queued + dropped");
        assert_eq!(
            counter(&events, "logit.input.datagram.bytes"),
            placed_bytes as f64 + dropped.1,
            "bytes read = bytes queued + bytes dropped"
        );
    }

    /// With `overflow: block`, `max_datagrams` below `read_batch` (one `push_many` can't fit even
    /// an empty queue), and no flush timer, every datagram is still delivered and the run ends.
    ///
    /// A `push_many` that notified `not_empty` only after its whole batch landed would deadlock
    /// here: the reader waiting for room, the decoder waiting for an item already in the queue.
    /// The combination is legal config (graph rule 57's comment says why), so it's pinned end to
    /// end.
    #[tokio::test]
    async fn a_block_queue_smaller_than_the_read_batch_still_delivers_every_datagram() {
        const BURST: usize = 200;

        let mut listener = UdpListener::new(
            "127.0.0.1:0",
            TestDecoder::new(),
            UdpListenerConfig {
                max_datagrams: 4,
                read_batch: 64,
                overflow: OverflowPolicy::Block,
                batch_max_events: 1,
                batch_flush_interval: Duration::ZERO,
                receive_buffer_bytes: Some(1024 * 1024),
                ..UdpListenerConfig::default()
            },
        );
        listener.bind().await.expect("binding an ephemeral port should succeed");
        let addr = listener.local_addr().expect("bind() leaves a real address behind");

        let (fanout, mut rx) = recording_fanout(8);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(fanout, shutdown_rx).await });

        let payloads = numbered_payloads(BURST);
        let sender = bind_ephemeral().await;
        for payload in &payloads {
            sender.send_to(payload, addr).await.expect("loopback send should succeed");
        }

        let mut received = Vec::with_capacity(BURST);
        for _ in 0..BURST {
            let delivered = tokio::time::timeout(Duration::from_secs(10), rx.recv())
                .await
                .expect(
                    "every datagram must be delivered -- a stall here is the reader waiting for \
                     room while the decoder waits for an item it was never told about",
                )
                .expect("the fanout channel should stay open");
            received.push(payload_bytes(&unwrap_batch(delivered).events[0]));
        }
        assert_eq!(received, payloads);

        shutdown_tx.send(true).expect("receiver should still be alive");
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("should shut down within its grace")
            .expect("task should not panic")
            .expect("should shut down without error");
    }

    #[tokio::test]
    async fn bind_first_available_with_every_candidate_failing_reports_the_last_error() {
        let occupied = bind_ephemeral().await;
        let occupied_addr = occupied.local_addr().unwrap();

        let telemetry = Telemetry::default();
        let mut diag = Diagnostics::default();
        let err = bind_first_available(&[occupied_addr], None, &telemetry, &mut diag)
            .expect_err("the only candidate is already occupied -- must fail, not hang or panic");
        assert!(!err.to_string().is_empty());
        drop(occupied);
    }

    // -- shutdown accounting (ADR `shutdown-accounting-and-cancellation-safety`, decision 4) ----

    /// Every counter point named `name` tagged `reason=reason`, summed.
    fn counter_with_reason(events: &[Event], name: &str, reason: &str) -> f64 {
        let tagged: Vec<Event> = events
            .iter()
            .filter(|event| {
                event.attributes.get("reason").and_then(|value| value.as_str()) == Some(reason)
            })
            .cloned()
            .collect();
        counter(&tagged, name)
    }

    /// `(datagrams, bytes)` counted `datagrams.dropped`/`bytes.dropped{reason="shutdown"}`.
    fn shutdown_drops(events: &[Event]) -> (f64, f64) {
        (
            counter_with_reason(events, RECEIVE_QUEUE_METRICS.items_dropped, "shutdown"),
            counter_with_reason(events, RECEIVE_QUEUE_METRICS.units_dropped, "shutdown"),
        )
    }

    /// Samples in `logit.component.receive.latency`: one per datagram yielded to the decoder.
    fn latency_samples(events: &[Event]) -> f64 {
        events
            .iter()
            .flat_map(|event| &event.metrics)
            .filter(|m| logit_core::interner::resolve(m.name) == "logit.component.receive.latency")
            .map(|m| match &m.kind {
                logit_core::MetricKind::Distribution(sketch) => sketch.count() as f64,
                other => panic!("receive.latency must be a timing, got {other:?}"),
            })
            .sum()
    }

    /// The per-listener contract: every datagram read was yielded to the decoder (one
    /// `receive.latency` sample each) or counted dropped under some reason, and the same in
    /// bytes, against `decoded_bytes` from a [`CountingDecoder`].
    fn assert_datagram_contract(events: &[Event], decoded_bytes: u64, context: &str) {
        let read = counter(events, "logit.input.datagrams");
        let decoded = latency_samples(events);
        let dropped = counter(events, RECEIVE_QUEUE_METRICS.items_dropped);
        assert_eq!(
            read,
            decoded + dropped,
            "{context}: {read} datagram(s) read, {decoded} decoded, {dropped} dropped"
        );
        let read_bytes = counter(events, "logit.input.datagram.bytes");
        let dropped_bytes = counter(events, RECEIVE_QUEUE_METRICS.units_dropped);
        assert_eq!(
            read_bytes,
            decoded_bytes as f64 + dropped_bytes,
            "{context}: {read_bytes} byte(s) read, {decoded_bytes} decoded, {dropped_bytes} dropped"
        );
    }

    /// [`TestDecoder`], also summing the bytes of every datagram it's handed, rejected or not.
    struct CountingDecoder {
        inner: TestDecoder,
        bytes: Arc<std::sync::atomic::AtomicU64>,
    }

    impl CountingDecoder {
        fn new(bytes: &Arc<std::sync::atomic::AtomicU64>) -> Self {
            Self { inner: TestDecoder::new(), bytes: Arc::clone(bytes) }
        }
    }

    impl Decoder for CountingDecoder {
        fn decode_into(
            &mut self,
            bytes: Bytes,
            received_at: i64,
            out: &mut Vec<Event>,
        ) -> Result<(Arc<Resource>, Option<Arc<logit_core::Scope>>), CodecError> {
            self.bytes.fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
            self.inner.decode_into(bytes, received_at, out)
        }
    }

    /// A listener over an ephemeral port whose telemetry lands in `registry` and whose decoder sums
    /// decoded bytes into `decoded_bytes`: a `Block` queue, one event per send, no flush timer.
    async fn accounting_listener(
        registry: &logit_core::Registry,
        decoded_bytes: &Arc<std::sync::atomic::AtomicU64>,
        max_datagrams: usize,
        read_batch: usize,
    ) -> (UdpListener<CountingDecoder>, std::net::SocketAddr) {
        let mut listener = UdpListener::new(
            "127.0.0.1:0",
            CountingDecoder::new(decoded_bytes),
            UdpListenerConfig {
                max_datagrams,
                read_batch,
                overflow: OverflowPolicy::Block,
                batch_max_events: 1,
                batch_flush_interval: Duration::ZERO,
                receive_buffer_bytes: Some(1024 * 1024),
                ..UdpListenerConfig::default()
            },
        )
        .with_telemetry(registry.telemetry_for("statsd_in", "statsd_in", "listener"));
        listener.bind().await.expect("binding an ephemeral port should succeed");
        let addr = listener.local_addr().expect("bind() leaves a real address behind");
        (listener, addr)
    }

    /// A `read_loop` that reads a batch and then loses the unbiased push-or-shutdown race without
    /// ever polling `push_many` counts the whole batch as `shutdown` drops.
    ///
    /// Shutdown is already set, so `select!`'s random branch order gives each trial one of three
    /// outcomes: no read, a read whose batch is queued, or a read whose `push_many` is never
    /// polled. Every trial must satisfy "read = queued + dropped"; trials repeat until the third
    /// outcome shows up (about 1 in 4 each), bounded so a regression fails.
    #[tokio::test]
    async fn a_read_loop_whose_push_many_was_never_polled_before_shutdown_counts_its_whole_batch() {
        const DATAGRAMS: usize = 8;
        let mut seen = false;
        for trial in 0..400 {
            let registry = logit_core::Registry::new();
            let telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
            let socket = bind_ephemeral().await;
            let addr = socket.local_addr().unwrap();
            let sender = bind_ephemeral().await;
            for i in 0..DATAGRAMS {
                sender.send_to(format!("msg-{i}").as_bytes(), addr).await.expect("loopback send");
            }
            // Readiness known before the race, so the read arm can win it on its first poll.
            socket.readable().await.expect("the datagrams are already in the socket");
            let queue = test_queue(OverflowPolicy::DropOldest, 64, &telemetry);
            let (_shutdown_tx, shutdown_rx) = watch::channel(true);

            tokio::time::timeout(
                Duration::from_secs(5),
                read_loop(
                    &socket,
                    Arc::clone(&queue),
                    telemetry.clone(),
                    Diagnostics::default(),
                    shutdown_rx,
                    64,
                ),
            )
            .await
            .expect("shutdown is already set, so the loop returns at once")
            .expect("no read error on a loopback socket");

            let queued = queue.take_all();
            assert!(queue.pop().await.is_none(), "trial {trial}: the queue must be closed");
            let queued_bytes: u64 = queued.iter().map(|datagram| datagram.bytes.len() as u64).sum();
            let events = registry.drain(0);
            let read = counter(&events, "logit.input.datagrams");
            let dropped = shutdown_drops(&events);
            assert_eq!(
                read,
                queued.len() as f64 + dropped.0,
                "trial {trial}: read = queued + dropped"
            );
            assert_eq!(
                counter(&events, "logit.input.datagram.bytes"),
                queued_bytes as f64 + dropped.1,
                "trial {trial}: bytes read = bytes queued + bytes dropped"
            );
            if read > 0.0 && queued.is_empty() {
                assert_eq!(dropped.0, read, "trial {trial}: a never-polled push drops its batch");
                seen = true;
                break;
            }
        }
        assert!(seen, "no trial read a batch and then left push_many unpolled in 400 tries");
    }

    /// A `read_loop` future dropped between its read and its push, with the batch still in hand,
    /// counts that batch as `shutdown` drops and closes the queue.
    ///
    /// The drop point is the coop-budget check in front of the push `select!`: each poll here
    /// leaves the read one unit of budget, so a successful read spends the last unit and the push
    /// `select!` yields before polling any arm. That's the state `run_input`'s backstop can drop
    /// under `receive.shutdown_grace: 0s`.
    #[tokio::test]
    async fn a_read_loop_dropped_mid_iteration_counts_what_its_batch_held_and_closes_the_queue() {
        use std::task::Poll;

        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        let socket = bind_ephemeral().await;
        let addr = socket.local_addr().unwrap();
        let sender = bind_ephemeral().await;
        for i in 0..5u32 {
            sender.send_to(format!("msg-{i}").as_bytes(), addr).await.expect("loopback send");
        }
        let queue = test_queue(OverflowPolicy::DropOldest, 64, &telemetry);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut read = Box::pin(read_loop(
            &socket,
            Arc::clone(&queue),
            telemetry.clone(),
            Diagnostics::default(),
            shutdown_rx,
            64,
        ));

        let mut events = Vec::new();
        for _ in 0..1_000 {
            // A fresh poll of this task starts with the whole 128-unit budget.
            tokio::task::yield_now().await;
            let pending = std::future::poll_fn(|cx| {
                for _ in 0..127 {
                    let burn = std::pin::pin!(tokio::task::consume_budget());
                    assert!(burn.poll(cx).is_ready(), "a fresh task poll has 128 units of budget");
                }
                Poll::Ready(read.as_mut().poll(cx).is_pending())
            })
            .await;
            assert!(pending, "the read loop must not finish before shutdown");
            events.extend(registry.drain(0));
            if counter(&events, "logit.input.reads") > 0.0 {
                break;
            }
        }
        let read_datagrams = counter(&events, "logit.input.datagrams");
        let read_bytes = counter(&events, "logit.input.datagram.bytes");
        assert!(read_datagrams > 0.0, "the read half never got a batch off the socket -- premise");

        drop(read);
        events.extend(registry.drain(0));
        assert_eq!(
            shutdown_drops(&events),
            (read_datagrams, read_bytes),
            "the push select! had no budget, so the whole batch was still in hand"
        );
        let mut out = Vec::new();
        let popped = tokio::time::timeout(Duration::from_secs(1), queue.pop_many(&mut out, 1))
            .await
            .expect("a closed, empty queue answers at once");
        assert_eq!(popped, 0, "nothing was queued and the queue is closed");
    }

    /// A `read_loop` future dropped while parked on its read still closes the queue, so a decode
    /// loop sharing it sees "closed and empty" rather than waiting forever.
    #[tokio::test(start_paused = true)]
    async fn read_loop_closes_the_queue_even_when_its_future_is_dropped() {
        use std::task::Poll;

        let socket = bind_ephemeral().await;
        let queue = test_queue(OverflowPolicy::DropOldest, 4, &Telemetry::default());
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut read = Box::pin(read_loop(
            &socket,
            Arc::clone(&queue),
            Telemetry::default(),
            Diagnostics::default(),
            shutdown_rx,
            64,
        ));
        let pending =
            std::future::poll_fn(|cx| Poll::Ready(read.as_mut().poll(cx).is_pending())).await;
        assert!(pending, "nothing was sent and shutdown isn't set, so the read parks");
        drop(read);

        let mut out = Vec::new();
        let popped = tokio::time::timeout(Duration::from_secs(1), queue.pop_many(&mut out, 1))
            .await
            .expect("a closed, empty queue answers at once");
        assert_eq!(popped, 0);
    }

    /// `decode_loop` dropped while its second `emit` is parked counts the three datagrams it
    /// popped but never decoded, with their own bytes, and nothing else.
    #[tokio::test(start_paused = true)]
    async fn a_decode_loop_dropped_mid_batch_counts_every_popped_but_undecoded_datagram() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        let queue = test_queue(OverflowPolicy::Block, 5, &telemetry);
        for payload in ["a", "bb", "ccc", "dddd", "eeeee"] {
            queue
                .push(Datagram { bytes: Bytes::from_static(payload.as_bytes()), received_at: 0 })
                .await;
        }
        let (fanout, mut rx) = recording_fanout(1);
        let mut decoder = TestDecoder::new();
        let mut decode = Box::pin(decode_loop(
            &mut decoder,
            Arc::clone(&queue),
            fanout,
            BatchingConfig {
                max_events: 1,
                max_bytes: u64::MAX,
                flush_interval: Duration::ZERO,
                pop_batch: 5,
            },
            telemetry.clone(),
            Diagnostics::default(),
        ));
        tokio::time::timeout(Duration::from_millis(10), &mut decode)
            .await
            .expect_err("the second emit parks on the full, unread consumer");
        let mut events = registry.drain(0);
        assert_eq!(
            latency_samples(&events),
            2.0,
            "two datagrams decoded: the one delivered and the one whose emit is parked"
        );

        drop(decode);
        events.extend(registry.drain(0));
        assert_eq!(
            shutdown_drops(&events),
            (3.0, 12.0),
            "datagrams 3 to 5 (3 + 4 + 5 bytes); the parked one was decoded and isn't counted"
        );
        let first = unwrap_batch(rx.try_recv().expect("the first emit landed"));
        assert_eq!(payload(&first.events[0]), "a");
        assert!(queue.take_all().is_empty(), "all five were popped in one batch");
    }

    /// A listener whose downstream is wedged, cut off by a grace backstop after shutdown, counts
    /// everything it still held: the read half's remainder, the decode half's undecoded batch,
    /// and the receive queue's residual. The datagram contract holds.
    #[tokio::test]
    async fn a_udp_listener_cancelled_by_the_grace_backstop_counts_what_its_queue_still_held() {
        let registry = logit_core::Registry::new();
        let decoded_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (mut listener, addr) = accounting_listener(&registry, &decoded_bytes, 16, 64).await;
        let (fanout, _rx) = recording_fanout(1); // never read
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut run = Box::pin(listener.run_until_shutdown(fanout, shutdown_rx));

        let sender = bind_ephemeral().await;
        for i in 0..100u32 {
            sender.send_to(format!("msg-{i}").as_bytes(), addr).await.expect("loopback send");
        }
        // Run until the decode half is parked on the unread consumer (two decoded: one
        // delivered, one parked) and the read half has read more than the queue holds.
        let mut events = Vec::new();
        let give_up = tokio::time::Instant::now() + Duration::from_secs(10);
        while latency_samples(&events) < 2.0 || counter(&events, "logit.input.datagrams") < 20.0 {
            tokio::select! {
                result = &mut run => panic!("the listener must not finish before shutdown: {result:?}"),
                () = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
            events.extend(registry.drain(0));
            assert!(tokio::time::Instant::now() < give_up, "the listener never wedged");
        }
        // A little longer, so the read half refills the queue behind the parked decode half.
        tokio::select! {
            result = &mut run => panic!("the listener must not finish before shutdown: {result:?}"),
            () = tokio::time::sleep(Duration::from_millis(50)) => {}
        }

        shutdown_tx.send(true).expect("receiver should still be alive");
        tokio::time::timeout(Duration::from_millis(200), &mut run).await.expect_err(
            "the decode half is parked on the unread consumer; only the backstop ends it",
        );
        drop(run);

        events.extend(registry.drain(0));
        assert!(shutdown_drops(&events).0 > 0.0, "the backstop dropped queued datagrams");
        assert_datagram_contract(
            &events,
            decoded_bytes.load(std::sync::atomic::Ordering::Relaxed),
            "grace backstop",
        );
    }

    /// Under `receive.shutdown_grace: 0s`, `run_input`'s backstop drops the listener at an
    /// arbitrary point after the signal. Wherever that lands, every datagram read is decoded or
    /// counted. `run_input` is private to `logit_pipeline`, so this reproduces its `select!`:
    /// biased, input first, the backstop `unconstrained`.
    #[tokio::test]
    async fn a_zero_shutdown_grace_never_breaks_the_datagram_contract() {
        for iteration in 0..50usize {
            let registry = logit_core::Registry::new();
            let decoded_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let (mut listener, addr) = accounting_listener(&registry, &decoded_bytes, 4, 8).await;
            let (fanout, _rx) = recording_fanout(1); // never read
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let mut backstop = shutdown_rx.clone();

            let run = async {
                tokio::select! {
                    biased;
                    result = listener.run_until_shutdown(fanout, shutdown_rx) => result,
                    () = tokio::task::unconstrained(async move {
                        let _ = backstop.wait_for(|&due| due).await;
                        tokio::time::sleep(Duration::ZERO).await;
                    }) => Ok(()),
                }
            };
            // Varies how much traffic and how many scheduler turns precede the signal, so the
            // drop lands in a different place from one iteration to the next.
            let driver = async {
                let sender = bind_ephemeral().await;
                for i in 0..(iteration % 20 + 1) {
                    sender.send_to(format!("msg-{i}").as_bytes(), addr).await.expect("send");
                }
                for _ in 0..(iteration % 7) {
                    tokio::task::yield_now().await;
                }
                shutdown_tx.send(true).expect("receiver should still be alive");
            };
            let (result, ()) = tokio::join!(run, driver);
            result.expect("should shut down without error");

            let events = registry.drain(0);
            assert_datagram_contract(
                &events,
                decoded_bytes.load(std::sync::atomic::Ordering::Relaxed),
                &format!("iteration {iteration}"),
            );
        }
    }

    /// An interval `emit` that parks on a full downstream past the next deadline doesn't leave that
    /// deadline already due: the pop batch after it resumes must not flush again at the same
    /// instant.
    ///
    /// The consumer takes one `Delivered` every five intervals. Each window, the first flush fills
    /// the one-slot channel and the next parks; the receive resumes it. A next deadline computed
    /// from a clock reading taken before the parked `emit` would already be past, so the next pop
    /// batch would flush at once and park again.
    #[tokio::test(start_paused = true)]
    async fn an_interval_emit_that_parks_past_the_deadline_does_not_flush_once_per_pop_batch() {
        const INTERVAL: Duration = Duration::from_millis(100);
        const CYCLES: usize = 20;

        fn interval_flushes(events: &[Event]) -> f64 {
            counter_with_reason(events, "logit.component.receive.flushed", "interval")
        }

        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        let queue = test_queue(OverflowPolicy::DropOldest, 1024, &telemetry);
        let (fanout, mut rx) = recording_fanout(1);
        let decode = tokio::spawn({
            let queue = Arc::clone(&queue);
            let telemetry = telemetry.clone();
            async move {
                let mut decoder = TestDecoder::new();
                decode_loop(
                    &mut decoder,
                    queue,
                    fanout,
                    BatchingConfig {
                        max_events: 10_000,
                        max_bytes: u64::MAX,
                        flush_interval: INTERVAL,
                        pop_batch: 1,
                    },
                    telemetry,
                    Diagnostics::default(),
                )
                .await;
            }
        });
        let push = |i: usize| {
            queue.push(Datagram { bytes: Bytes::from(format!("msg-{i}")), received_at: 0 })
        };

        let start = tokio::time::Instant::now();
        let mut events = Vec::new();
        let mut sent = 0usize;
        // Off the deadline grid, so a push never shares an instant with a flush.
        tokio::time::sleep(INTERVAL / 2).await;
        for cycle in 0..CYCLES {
            for _ in 0..5 {
                push(sent).await;
                sent += 1;
                tokio::time::sleep(INTERVAL).await;
            }
            events.extend(registry.drain(0));
            let before = interval_flushes(&events);

            tokio::time::timeout(Duration::from_secs(1), rx.recv())
                .await
                .expect("a flush filled the channel during the window")
                .expect("the decode loop owns the fanout and is still running");
            push(sent).await;
            sent += 1;
            // No clock advance: this task stays runnable, so the paused clock stands still.
            for _ in 0..64 {
                tokio::task::yield_now().await;
            }
            events.extend(registry.drain(0));
            assert_eq!(
                interval_flushes(&events),
                before,
                "cycle {cycle}: the resumed emit must not be followed by another interval flush \
                 at the same instant"
            );
        }

        let intervals = (start.elapsed().as_nanos() / INTERVAL.as_nanos()) as f64;
        assert!(
            interval_flushes(&events) <= intervals + CYCLES as f64 + 1.0,
            "{} interval flushes over {intervals} intervals and {CYCLES} resumed emits",
            interval_flushes(&events)
        );
        decode.abort();
    }

    /// A batch whose fan-out is cut off mid-`Fanout::deliver` reaches a prefix of the consumers,
    /// and is counted `sent` and `receive.flushed` but never dropped. This pins the gap
    /// `docs/known-gaps.md` records for the grace backstop.
    #[tokio::test(start_paused = true)]
    async fn a_batch_cut_off_mid_fan_out_reaches_a_prefix_of_consumers() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        let queue = test_queue(OverflowPolicy::DropOldest, 4, &telemetry);
        queue.push(Datagram { bytes: Bytes::from_static(b"only"), received_at: 0 }).await;

        let (first_tx, mut first_rx) = mpsc::channel(8);
        let (second_tx, mut second_rx) = mpsc::channel(1);
        // Fill the second consumer, so the fan-out parks on it after sending to the first.
        let filler =
            EventBatch { resource: Arc::new(Resource::default()), scope: None, events: Vec::new() };
        Fanout::new(vec![second_tx.clone()]).send(filler).await;
        let fanout = Fanout::new(vec![first_tx, second_tx]).with_telemetry(telemetry.clone());

        let mut decoder = TestDecoder::new();
        let mut decode = Box::pin(decode_loop(
            &mut decoder,
            Arc::clone(&queue),
            fanout,
            BatchingConfig {
                max_events: 1,
                max_bytes: u64::MAX,
                flush_interval: Duration::ZERO,
                pop_batch: 4,
            },
            telemetry.clone(),
            Diagnostics::default(),
        ));
        tokio::time::timeout(Duration::from_millis(10), &mut decode)
            .await
            .expect_err("the fan-out parks on the full second consumer");
        drop(decode);

        let first = unwrap_batch(first_rx.try_recv().expect("the first consumer got the batch"));
        assert_eq!(payload(&first.events[0]), "only");
        let filler = unwrap_batch(second_rx.try_recv().expect("the filler is still there"));
        assert!(filler.events.is_empty());
        assert!(second_rx.try_recv().is_err(), "the second consumer never got the batch");

        let events = registry.drain(0);
        assert_eq!(counter(&events, "logit.component.batches.sent"), 1.0);
        assert_eq!(counter(&events, "logit.component.receive.flushed"), 1.0);
        assert_eq!(counter(&events, "logit.component.events.dropped"), 0.0);
        assert_eq!(shutdown_drops(&events), (0.0, 0.0), "the datagram was decoded, not dropped");
    }
}
