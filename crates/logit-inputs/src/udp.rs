//! The shared UDP listener driver: read/decode decoupling plus datagram-\>batch assembly
//! (`docs/adr/decoupled-listener-io.md`). `StatsdInput` and `SyslogInput` are both thin
//! wrappers over [`UdpListener<D>`] -- their `run` loops used to be byte-for-byte identical apart
//! from the decoder type, which is exactly what this generalizes over.
//!
//! **Crate placement.** The generic queue (`logit_pipeline::BoundedQueue`) and the batch
//! accumulator (`logit_pipeline::BatchAccumulator`) are transport-agnostic and live in
//! `logit-pipeline`, alongside `SinkQueue`. A UDP socket bind, an `SO_RCVBUF` setsockopt, and a
//! `recv_from` loop are unambiguously protocol-*impl* shaped, per
//! `docs/design/pipeline-graph.md`'s crate-layout rule ("`logit-inputs`... hold only impls") --
//! `socket2` is therefore a `logit-inputs` dependency only, never `logit-pipeline`'s.
//!
//! **Multicast comes free with the driver.** A `bind:` whose address is a multicast group makes
//! [`bind_one`] set `SO_REUSEADDR`, bind the unspecified address on that port and join the group,
//! rather than binding the group address directly -- so `collectd_in` (whose protocol has a
//! standard group, `239.192.74.66`), `statsd_in` and `syslog_in` all get it without a field of
//! their own. See that function's doc for why each of the three steps is needed.
//!
//! **Not used by [`crate::internal::InternalInput`].** `internal` has no socket, no datagram, and
//! no `receive:` block -- its own `Input::run_until_shutdown` override is a single final
//! `Registry` drain, nothing queue-shaped. Don't generalize this module toward it.

use bytes::Bytes;
use logit_core::{Diagnostics, Event, EventBatch, Telemetry};
use logit_pipeline::sockstat;
use logit_pipeline::{BatchAccumulator, FlushReason, Input};
use logit_pipeline::{BoundedQueue, Fanout, OverflowPolicy, QueueConfig, QueueMetrics, Queued};
use logit_proto::Decoder;
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
    /// The datagram's own right-sized allocation plus this struct's inline footprint. Not an
    /// allocator figure -- an admission-control estimate, the same discipline as
    /// `EventBatch::estimated_heap_bytes` (`docs/design/memory.md` §5).
    fn weight(&self) -> u64 {
        (self.bytes.len() + std::mem::size_of::<Self>()) as u64
    }
    /// Bytes, not "1" -- so `logit.component.bytes.dropped` reports the size of what was lost,
    /// the unit an operator sizing `receive.max_bytes` actually reasons in.
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

/// [`UdpListener`]'s runtime knobs. Workstream F (`docs/adr/decoupled-listener-io.md`) builds
/// this from a component's `logit_config::ReceiveConfig`; a test can build it directly.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UdpListenerConfig {
    pub max_datagrams: usize,
    pub max_bytes: u64,
    pub overflow: OverflowPolicy,
    /// `SO_RCVBUF`, requested at bind. `None` leaves the kernel default alone.
    pub receive_buffer_bytes: Option<u64>,
    /// Events to accumulate across datagrams before one `Fanout::send`. `1` means one send per
    /// datagram -- the pre-ADR `decoupled-listener-io` behaviour, exactly (`BatchAccumulator::absorb`'s doc comment).
    pub batch_max_events: usize,
    pub batch_max_bytes: u64,
    /// `Duration::ZERO` disables the flush timer entirely; bounds are then the only trigger.
    pub batch_flush_interval: Duration,
    /// How long [`UdpListener::run_until_shutdown`] keeps draining after shutdown fires before
    /// [`logit_pipeline::runtime::run_input`]'s grace backstop cancels it by drop.
    pub shutdown_grace: Duration,
    /// Datagrams one `recvmmsg(2)` call may return on Linux, and -- the same number -- how many
    /// [`decode_loop`] takes off the [`ReceiveQueue`] per `pop_many`. See
    /// `logit_config::ReceiveConfig::read_batch` for the operator-facing account of both halves,
    /// the slab cost and the widened shutdown loss. Clamped into `1..=MAX_READ_BATCH` by
    /// [`UdpListenerConfig::read_batch`]; graph rules 18 and 57 reject the out-of-range values
    /// before a config ever gets here.
    pub read_batch: usize,
}

/// Matches `docs/adr/decoupled-listener-io.md`'s `ReceiveConfig` defaults exactly -- see that
/// ADR for the numbers' justification against the field's own tuning figures (Telegraf, gostatsd,
/// DogStatsD, rsyslog, syslog-ng).
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
            shutdown_grace: Duration::from_secs(5),
            read_batch: 64,
        }
    }
}

impl UdpListenerConfig {
    /// `read_batch`, clamped into the range the read and pop paths can actually honour.
    ///
    /// Graph rules 18 and 57 already reject `0` and anything above [`MAX_READ_BATCH`] at
    /// validation time, so in a real pipeline this clamp never fires; it exists because
    /// `UdpListenerConfig` is also built directly by tests and by anything embedding the driver,
    /// and neither `recvmmsg` (`vlen` of 0 reads nothing, forever) nor `pop_many` (whose own
    /// `debug_assert` catches `max == 0`) has a sensible answer for a zero here.
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
/// Every UDP listener in this codebase has always sized its receive buffer to exactly this, and
/// [`BatchReader`] sizes *each* of its `read_batch` slots to it.
///
/// **It is not the largest payload a UDP datagram can carry, and the difference is real.** IPv6's
/// own payload-length field excludes the 40-byte header, so an IPv6 UDP datagram may carry up to
/// 65,527 bytes -- 20 more than this. A datagram that big arriving on an IPv6 listener is copied as
/// far as this bound and the remainder is discarded by the kernel, which is exactly what the
/// `recv_from` loop this replaced did with its own 65,507-byte buffer. What is new is that
/// [`BatchReader`] can *see* it happen (`MSG_TRUNC` in the returned `msg_flags`) and counts it as
/// `logit.input.datagrams.truncated` instead of losing the bytes silently. (Jumbograms --
/// RFC 2675's payload-length-zero extension, past 65,535 -- are a separate thing again, and nothing
/// in this codebase or in any mainstream kernel's UDP path supports them.)
const MAX_DATAGRAM_BYTES: usize = 65_507;

/// The largest `receive.read_batch` this driver accepts. Graph rule 57 rejects a larger value at
/// config-validation time (`logit_config::MAX_READ_BATCH`, the same number -- `logit-inputs`
/// deliberately does not depend on `logit-config`, the same duplication
/// [`UdpListenerConfig::default`] already carries against `ReceiveConfig::default`).
///
/// **The number is `UIO_MAXIOV`'s, but the limit is ours, not the kernel's.** It is tempting to
/// write that `UIO_MAXIOV` bounds `recvmmsg`'s `vlen`; it does not. `UIO_MAXIOV` bounds
/// `msg_iovlen` *within one* `msghdr` -- `__copy_msghdr` (`net/socket.c`) returns `-EMSGSIZE`
/// above it -- and [`build_headers`] sets `msg_iovlen` to 1. There is no `vlen` clamp on the
/// receive side at all: `do_recvmmsg`'s loop is a plain `while (datagrams < vlen)`, and the only
/// `UIO_MAXIOV` clamp on a `vlen` anywhere is `__sys_sendmmsg`'s (`if (vlen > UIO_MAXIOV) vlen =
/// UIO_MAXIOV;`), on the *send* side. What 1024 actually bounds is this crate's own two costs: the
/// `vlen * MAX_DATAGRAM_BYTES` slab [`BatchReader::new`] reserves (67 MB of address space at this
/// ceiling), and how many datagrams a cancelled `push_many` can discard on the shutdown path. Both
/// are ours to choose; 1024 is a round number comfortably past any measured plateau
/// (ADR `udp-intake-batching-and-socket-visibility`'s sweep) rather than an ABI boundary.
pub const MAX_READ_BATCH: usize = 1024;

/// The three `decode_loop` needs to build and drive a [`BatchAccumulator`] -- split out from
/// [`UdpListenerConfig`] purely to keep `decode_loop`'s own parameter count down.
#[derive(Debug, Clone, Copy)]
struct BatchingConfig {
    max_events: usize,
    max_bytes: u64,
    flush_interval: Duration,
    /// [`UdpListenerConfig::read_batch`], carried through to `decode_loop`'s `pop_many` so one
    /// setting governs both ends of the [`ReceiveQueue`]. Not a `BatchAccumulator` knob like the
    /// three above -- it rides along here only to keep `decode_loop`'s parameter count down, which
    /// is what this struct exists for.
    pop_batch: usize,
}

/// The read/decode split every UDP listener reduces to
/// (`docs/adr/decoupled-listener-io.md`) -- generic over the decoder because that is the
/// *only* thing `StatsdInput`/`SyslogInput` ever differed in.
pub struct UdpListener<D: Decoder + Send> {
    bind: String,
    decoder: D,
    config: UdpListenerConfig,
    diag: Diagnostics,
    telemetry: Telemetry,
    /// Set by [`Input::bind`], taken back out by [`Input::run_until_shutdown`]
    /// (`docs/plans/operator-surface.md`, workstream B). `None` after a run, so a second run
    /// rebinds, same as before this field existed.
    socket: Option<tokio::net::UdpSocket>,
}

impl<D: Decoder + Send> UdpListener<D> {
    pub fn new(bind: impl Into<String>, decoder: D, config: UdpListenerConfig) -> Self {
        Self {
            bind: bind.into(),
            decoder,
            config,
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
            socket: None,
        }
    }

    /// The address actually bound, once [`Input::bind`] has run -- lets a test learn the
    /// OS-assigned port without a bind-drop-rebind race.
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        self.socket.as_ref().and_then(|s| s.local_addr().ok())
    }

    /// Sets *this listener's own* diagnostics -- the top-level `bad_datagram` diagnostic
    /// `decode_loop` reports when a whole datagram fails to decode (`udp.rs`'s own
    /// `diag.warn_throttled("bad_datagram", ...)` call). Does **not** reach `self.decoder`'s own
    /// diagnostics field, if it has one (`StatsdDecoder`/`SyslogDecoder` each track their own,
    /// used for the finer-grained `bad_line` diagnostic a malformed line inside an otherwise-valid
    /// datagram reports) -- `UdpListener` is generic over `D: Decoder`, which has no
    /// `with_diagnostics` method of its own to call here. `StatsdInput`/`SyslogInput`'s own
    /// `with_diagnostics` (which know their concrete decoder type) use [`Self::map_decoder`] to
    /// propagate the same value into the decoder as well -- callers going through this method
    /// directly on a bare `UdpListener` must do the same if the decoder needs to know it too.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Applies `f` to the wrapped decoder -- lets a caller that knows the concrete decoder type
    /// (`StatsdInput`/`SyslogInput`, generic `UdpListener` itself never can) chain the decoder's
    /// own consuming builder methods, e.g. `with_diagnostics`, through `UdpListener`'s own
    /// builder-style API.
    pub fn map_decoder(mut self, f: impl FnOnce(D) -> D) -> Self {
        self.decoder = f(self.decoder);
        self
    }

    /// Overrides the queue/batching/shutdown-grace knobs -- what a `receive:` config block sets
    /// (`docs/adr/decoupled-listener-io.md`). Defaults to [`UdpListenerConfig::default`]
    /// when never called.
    pub fn with_config(mut self, config: UdpListenerConfig) -> Self {
        self.config = config;
        self
    }

    /// The currently-configured queue/batching/shutdown-grace knobs -- for test introspection
    /// (`logit-cli::pipeline`'s `build_spec` wiring tests), mirroring how `NodeSpec::Output`'s
    /// `SinkQueueConfig`/`WriteLoopConfig` are directly inspectable after `build_spec` runs.
    pub fn config(&self) -> UdpListenerConfig {
        self.config
    }

    /// Test-only: lets `StatsdInput`/`SyslogInput`'s own tests confirm a `with_diagnostics` call
    /// actually reached the wrapped decoder, not just `UdpListener`'s own `diag` field.
    /// This listener's own diagnostics -- test-only, the driver-half counterpart of
    /// [`Self::decoder`]: a wrapper's `with_diagnostics` has to set both, and only an accessor on
    /// each can prove it did (`crate::syslog`'s own regression test).
    #[cfg(test)]
    pub(crate) fn diag(&self) -> &Diagnostics {
        &self.diag
    }

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
        let (socket, multicast_group) = bind_socket(
            &self.bind,
            self.config.receive_buffer_bytes,
            &self.telemetry,
            &mut self.diag,
        )
        .await?;
        match multicast_group {
            Some(group) => self.diag.info(
                "bound",
                format_args!(
                    "listening on {} -- joined multicast group {group} on the default interface",
                    self.bind
                ),
            ),
            None => self.diag.info("bound", format_args!("listening on {}", self.bind)),
        }
        self.socket = Some(socket);
        Ok(())
    }

    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        // Never exercised in production -- `run_input` always calls `run_until_shutdown`. Present
        // because the trait requires it, mirroring how `logit_pipeline::run` passes
        // `std::future::pending()` as `run_with_shutdown`'s never-firing signal.
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
        let queue = Arc::new(BoundedQueue::with_metrics(
            self.config.queue_config(),
            &RECEIVE_QUEUE_METRICS,
            self.telemetry.clone(),
        ));

        let mut read = Box::pin(read_loop_sampled(
            &socket,
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

        // `read` is the only side that can finish on its own initiative -- a fatal socket error,
        // or `shutdown` firing -- and whichever way it finishes, it always closes `queue` first
        // (see `read_loop`'s own doc comment; `read_loop_sampled` only wraps it, adding the
        // kernel-counter sampler and forwarding its result unchanged), which is what lets
        // `decode`'s `pop_many` discover
        // "closed and empty" and return on its own. `decode` therefore never needs to be raced
        // away from early the way `run_output`'s `write`/`drain` dance does: once `read` is done,
        // simply drive `decode` to completion so it drains whatever `read` already queued and
        // flushes its accumulator.
        //
        // The `Option` indirection (rather than unconditionally `decode.await`ing after the
        // `select!`) exists only to guard the one edge case `select!` itself can't rule out:
        // `decode` finishing *before* `read` does. Nothing in today's `read_loop`/`decode_loop`
        // makes that possible (only `read_loop` ever closes `queue`), but if it somehow happened,
        // polling `decode` again after it already resolved would be exactly the double-poll
        // hazard `docs/adr/decoupled-listener-io.md` calls out -- so this still awaits `read`
        // in that branch instead, and never touches `decode` again once it's the side that fired.
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

/// Resolves `bind` and binds a UDP socket to it, applying `receive_buffer_bytes` if given.
/// Returns the socket and, when `bind` named a multicast group, the group that was joined (for
/// the `bound` info line -- see [`bind_one`] for what a multicast bind actually does differently).
///
/// Two properties this must have, both regressions an earlier version of this function had
/// relative to the `tokio::net::UdpSocket::bind` it replaced:
///
/// - **Resolves asynchronously.** `std::net::ToSocketAddrs::to_socket_addrs` performs a
///   synchronous (and, for a real hostname rather than a bare IP literal, potentially slow)
///   `getaddrinfo` call; calling it directly here would block whichever tokio worker thread is
///   running this listener's startup for as long as resolution takes.
///   [`tokio::net::lookup_host`] does the same resolution off tokio's own blocking thread pool.
/// - **Tries every resolved address, not just the first.** A `bind:` value that resolves to more
///   than one candidate (a hostname yielding both an AAAA and an A record, say) must fall through
///   to a later candidate if an earlier one can't be bound (its address family disabled, that
///   specific address unavailable) -- exactly `std`/`tokio`'s own `bind` convention for a
///   multi-address `ToSocketAddrs` target.
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

/// Tries every address in `addrs` in turn, returning the first successful bind -- split out from
/// [`bind_socket`] specifically so this fallback behavior (and its regression, an earlier version
/// of `bind_socket` tried only the first candidate) is directly unit-testable against a hand-built
/// address list, without needing a real hostname that resolves to more than one address.
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

/// Creates and binds one UDP socket to `addr` -- the per-candidate half of `bind_socket`'s
/// try-every-resolved-address loop. Synchronous and cheap (socket syscalls only, no I/O wait),
/// unlike the DNS resolution `bind_socket` itself awaits before ever calling this.
///
/// **A multicast `addr` is bound differently.** A group address (`224.0.0.0/4`, `ff00::/8` --
/// collectd's own defaults are `239.192.74.66` and `ff18::efc0:4a42`, and statsd/syslog senders use
/// groups too) is not an address any interface owns, so receiving on one takes three steps rather
/// than one: `SO_REUSEADDR`, so several processes on the host can subscribe to the same group and
/// port; a bind to the *unspecified* address on that port, since the group itself is not bindable
/// everywhere and binding it would still not subscribe to anything; and an explicit
/// `IP_ADD_MEMBERSHIP`/`IPV6_JOIN_GROUP` on the default interface (`INADDR_ANY` / interface index
/// `0` -- the kernel's own multicast routing decides which interface that is, rather than this
/// listener guessing at one). A failed join is a hard error, not a warning: a listener that bound
/// but never joined would sit there looking healthy and receive nothing forever.
///
/// A unicast `addr` binds exactly as it always has.
///
/// **Three things this function deliberately never does, all of them load-bearing elsewhere.**
/// They are negatives, so nothing in the code says them; they are recorded here because each one
/// is a single line away and each would break something several files from this one.
///
/// - **Never `connect(2)`.** `udp_err` (`net/ipv4/udp.c`; `udpv6_err`, `net/ipv6/udp.c`, is
///   identical) gates ICMP error delivery on `if (!inet_test_bit(RECVERR, sk)) { if (!harderr ||
///   sk->sk_state != TCP_ESTABLISHED) goto out; }`, and `sk_state` only becomes `TCP_ESTABLISHED`
///   in `__ip4_datagram_connect` (`net/ipv4/datagram.c`). Unconnected and without `IP_RECVERR`,
///   therefore, no ICMP unreachable can ever set `sk_err` on this socket -- which is what makes
///   `ECONNREFUSED`/`EHOSTUNREACH`/`ENETUNREACH`/`EPROTO`/PMTU `EMSGSIZE` unreachable at
///   [`BatchReader::read_batch`], and so what makes "every errno but `EAGAIN` is fatal" the right
///   policy there rather than a hasty one.
/// - **Never `IP_RECVERR`/`IPV6_RECVERR`.** It is the obvious idea -- it is the *other* way to see
///   ICMP-reported loss, next to the `SO_MEMINFO` counters this listener does sample -- and it
///   takes the `else` branch of the gate quoted above, which sets `sk_err` with **no**
///   `TCP_ESTABLISHED` check. Enabling it would make this listener killable by any host that can
///   provoke an ICMP unreachable toward it, since the next `recvmmsg` would return that error and
///   `read_loop` treats a non-`EAGAIN` errno as fatal. Any future change here needs an answer for
///   that first.
/// - **Never `shutdown(2)`.** `Ready::READ_CLOSED` is in this listener's wait mask (tokio's
///   `Ready::from_interest` adds it whenever the interest is readable) and `clear_readiness` can
///   never clear it (`runtime/io/scheduled_io.rs` subtracts it from the clearable mask by name).
///   If it were ever set while `recvmmsg` kept returning `EAGAIN`, `async_io`'s loop would spin
///   *inside a single poll* -- and, because its `WouldBlock` arm restores the coop budget, never
///   yield -- wedging the worker thread: no sampler tick, no shutdown observation, and
///   `run_input`'s backstop unable to help because it is in the same task. That is open tokio
///   issue #6971. It is unreachable here only because `EPOLLRDHUP`/`EPOLLHUP` on a UDP socket come
///   from `sk->sk_shutdown`, which nothing sets without a `shutdown(2)` call on this fd, and
///   nothing in `logit` makes one on a listener socket.
///
/// Verified against `torvalds/linux` master and tokio tag `tokio-1.53.1`, 2026-09-21; see ADR
/// `udp-intake-batching-and-socket-visibility`'s amendment of that date.
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

/// The granted-`SO_RCVBUF` gauging/warning and the final conversion to a tokio socket, run only
/// once some candidate address has actually bound successfully.
fn finish_bind(
    socket: socket2::Socket,
    receive_buffer_bytes: Option<u64>,
    telemetry: &Telemetry,
    diag: &mut Diagnostics,
) -> anyhow::Result<tokio::net::UdpSocket> {
    use anyhow::Context;

    // Read once at bind, not per datagram -- SO_RCVBUF doesn't change after bind. The gauge
    // itself is re-emitted every second by `ReceiveBufferSampler::sample_once` (from
    // `SO_MEMINFO`'s `SK_MEMINFO_RCVBUF`, the same `sk_rcvbuf` this getsockopt returns), because a
    // point written once here would survive exactly one `internal` drain window. This emission
    // still earns its keep: it is the only one a process that fails during startup ever makes.
    let granted = socket.recv_buffer_size().unwrap_or(0) as f64;
    telemetry.gauge("logit.input.receive_buffer.bytes", granted, &[]);
    if let Some(requested) = receive_buffer_bytes {
        telemetry.gauge("logit.input.receive_buffer.requested.bytes", requested as f64, &[]);
        // Linux doubles the requested value for its own bookkeeping, so a successful request
        // routinely reports back roughly 2x what was asked -- a plain `granted < requested` check
        // would never fire there. Warn only when the kernel's own `net.core.rmem_max` ceiling
        // actually clamped the request below what was asked.
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

    let std_socket: std::net::UdpSocket = socket.into();
    tokio::net::UdpSocket::from_std(std_socket).context("converting to a tokio UdpSocket")
}

/// Reads datagrams off `socket` into `queue` as fast as `queue.push` (governed by its own bounds/
/// overflow policy) allows -- entirely independent of how far behind `decode_loop`'s current
/// decode is running. Never blocks on downstream backpressure by default (`drop_oldest`, counted --
/// `docs/adr/decoupled-listener-io.md`'s core argument for why this differs from a sink
/// queue's `block` default); `overflow: block` is the one configuration under which this
/// genuinely does stop reading, by explicit operator choice.
///
/// Races every read *and* every push against `shutdown`, so a graceful shutdown stops this loop
/// immediately rather than only once the next datagram happens to arrive, or (under `block`) only
/// once downstream makes room. Cancelling a blocked `push_many` this way drops whatever the reader
/// was still holding, uncounted -- bounded by `read_batch`, widened from the exactly-one ADR
/// `service-lifecycle-and-output-retry` accepted for "a datagram in flight when the signal lands"
/// and named as such in ADR `udp-intake-batching-and-socket-visibility`.
///
/// **Telemetry is per batch, not per datagram.** One [`BatchReader::read_batch`] call is one
/// `logit.input.reads`, one `logit.input.datagrams` of however many it returned, one
/// `logit.input.datagram.bytes` of their total, and -- only when it is nonzero, like every other
/// loss counter here -- one `logit.input.datagrams.truncated`. `datagrams / reads` is the mean fill of the
/// syscall batch -- a fill pinned at `read_batch` says the knob is the limit, a fill near 1 says
/// the traffic never batches and the knob is irrelevant. Deliberately three counts per batch
/// rather than three per datagram: each one takes `ComponentBuffer`'s mutex, which `decode_loop`
/// contends with from the other side of the same component.
///
/// Closes `queue` in every exit path -- shutdown, or a fatal socket error -- which is what lets
/// `decode_loop`'s `pop_many` discover "closed and empty" and return; no separate close-detection
/// signal is needed on that side.
///
/// `read_batch` larger than the queue's own `max_datagrams` is legal and deliberately not rejected
/// at config time: `push_many` already has a defined answer for a batch that cannot fit at all
/// (evict or block per policy, per item, exactly as `push` would), so the only thing an extra
/// validation rule would buy is refusing a configuration that works.
async fn read_loop(
    socket: &tokio::net::UdpSocket,
    queue: Arc<ReceiveQueue>,
    telemetry: Telemetry,
    mut shutdown: watch::Receiver<bool>,
    read_batch: usize,
) -> anyhow::Result<()> {
    let mut reader = BatchReader::new(read_batch);
    // Reused across every iteration, cleared (not replaced) each time round, for the same reason
    // `decode_loop`'s `popped` is: `push_many` drains it, so its capacity survives and the steady
    // state allocates nothing beyond the one right-sized copy per datagram.
    let mut batch: Vec<Datagram> = Vec::with_capacity(read_batch);
    let result = loop {
        batch.clear();
        let read = tokio::select! {
            read = reader.read_batch(socket, &mut batch) => read,
            _ = shutdown.wait_for(|&due| due) => break Ok(()),
        };
        if let Err(err) = read {
            break Err(describe_read_failure(socket, err));
        }
        let bytes: usize = batch.iter().map(|datagram| datagram.bytes.len()).sum();
        telemetry.count("logit.input.reads", 1.0, &[]);
        telemetry.count("logit.input.datagrams", batch.len() as f64, &[]);
        telemetry.count("logit.input.datagram.bytes", bytes as f64, &[]);
        let truncated = reader.truncated();
        if truncated > 0 {
            telemetry.count("logit.input.datagrams.truncated", truncated as f64, &[]);
        }
        tokio::select! {
            () = queue.push_many(&mut batch) => {}
            _ = shutdown.wait_for(|&due| due) => break Ok(()),
        }
    };
    queue.close();
    result
}

/// The syscall [`BatchReader::read_batch`] takes datagrams off the socket with, by name -- for the
/// one message an operator ever sees it in, the fatal error [`read_loop`] stops on.
#[cfg(target_os = "linux")]
const READ_SYSCALL: &str = "recvmmsg(2)";
#[cfg(not(target_os = "linux"))]
const READ_SYSCALL: &str = "recvfrom(2)";

/// Turns a fatal read error into something an operator can act on.
///
/// Without this the only context added on the way out is `run_input`'s
/// `.with_context(|| format!("component '{id}'"))` (`logit_pipeline::runtime`), so a sandbox that
/// blocks the syscall reports `component 'statsd_in': Function not implemented (os error 38)` --
/// no syscall named, no socket named, nothing to search for. That is the same failure quinn#1947
/// and bun#42678 both hit (a seccomp profile refusing `recvmmsg`); `logit` at least fails cleanly
/// rather than spinning, which is the part bun had to fix, but the message was no more useful than
/// theirs was. `recvmmsg(2)` is unconditional on Linux, so the obvious operator response -- drop
/// `receive.read_batch` to 1 -- does not help, and the hint says so rather than leaving it to be
/// discovered. There is deliberately no runtime `recvmmsg` -> `recvmsg` fallback latch
/// (quinn#2079's pattern); that is an open design decision, not an oversight -- see
/// `docs/known-gaps.md`.
///
/// The bound address comes from `getsockname(2)` via `local_addr`, not from the configured `bind:`
/// string: it is the address actually in use (a `:0` port resolved, or whichever candidate won
/// [`bind_first_available`]'s fallthrough), and it costs one syscall on a path that is about to
/// terminate the listener anyway.
fn describe_read_failure(socket: &tokio::net::UdpSocket, err: std::io::Error) -> anyhow::Error {
    // Matched on `ErrorKind`, not `libc::E*`: `libc` is a Linux-only dependency of this crate and
    // this function is shared with the `recv_from` twin, so it has to build without it. std's Unix
    // mapping (`sys/pal/unix/mod.rs`, `decode_error_kind`) is `ENOSYS` -> `Unsupported`,
    // `EPERM`/`EACCES` -> `PermissionDenied`, `ECONNABORTED` -> `ConnectionAborted`.
    let hint = match err.kind() {
        std::io::ErrorKind::Unsupported | std::io::ErrorKind::PermissionDenied => {
            " -- a seccomp or sandbox profile blocking that syscall is the usual cause; \
             `receive.read_batch: 1` does not avoid it, this listener always makes the same call"
        }
        // `udp_abort` (`net/ipv4/udp.c`) is the one externally-triggerable fatal on this socket:
        // it sets `sk_err` and `__udp_disconnect`s, reached from a `SOCK_DESTROY` netlink request.
        // The socket really is gone -- unhashed, never to receive again -- so failing is correct,
        // and a retry would read `EAGAIN` (`sock_error`'s `xchg` clears `sk_err`) and leave a
        // silent zombie listener behind. Naming the cause is all that is left to do.
        std::io::ErrorKind::ConnectionAborted => {
            " -- the socket was destroyed out from under this listener (an `ss -K`, or another \
             SOCK_DESTROY request naming it); it cannot receive again, so the process exits \
             rather than pretending otherwise"
        }
        _ => "",
    };
    let addr = match socket.local_addr() {
        Ok(addr) => addr.to_string(),
        Err(_) => "an unknown address".to_string(),
    };
    anyhow::Error::new(err)
        .context(format!("{READ_SYSCALL} on the listener socket bound to {addr}{hint}"))
}

/// Words of `u64` backing one `mmsghdr`, and one `iovec`, in [`BatchReader`]'s storage --
/// see that type's doc for why the element type is `u64` rather than the C struct itself.
///
/// `div_ceil`, not a plain divide: nothing in the ABI *promises* either size is a multiple of 8
/// (both are, on every Linux target this builds for), and rounding up can only ever over-allocate.
/// Note what this does **not** buy, since it is easy to misread as buying it: the stride
/// [`build_headers`] and [`harvest_headers`] walk is `hdrs.add(i)`, i.e. `size_of::<mmsghdr>()`,
/// *not* the `HDR_WORDS * 8` bytes reserved per slot, so a size that were not a multiple of the
/// alignment would misalign slot 1 however much was allocated. That case is impossible for a
/// language-level reason rather than an ABI one -- Rust guarantees `size_of::<T>()` is a multiple
/// of `align_of::<T>()` -- and the const block below asserts it anyway.
#[cfg(target_os = "linux")]
const HDR_WORDS: usize = std::mem::size_of::<libc::mmsghdr>().div_ceil(8);

/// [`HDR_WORDS`]'s `iovec` twin; that constant's doc covers both.
#[cfg(target_os = "linux")]
const IOV_WORDS: usize = std::mem::size_of::<libc::iovec>().div_ceil(8);

/// Every layout fact [`build_headers`], [`recvmmsg_into`] and [`harvest_headers`] rest on, checked
/// against whatever `libc` says these two structs look like on the target actually being built.
///
/// A `const` block, so each of these is a hard compile error rather than a runtime surprise on a
/// target whose `mmsghdr` is shaped differently than this code assumes -- the same tripwire
/// discipline `crates/logit-core/tests/type_sizes.rs` applies to `Event`, and the reason
/// `libc`'s own CI validates these structs against real headers with `ctest`.
#[cfg(target_os = "linux")]
const _: () = {
    // **Alignment.** `mmsghdr`/`iovec` are 8-aligned on every target this compiles for, which is
    // what makes a `u64` buffer valid storage for them. `Vec<u64>` allocates through
    // `Layout::array::<u64>()`, whose alignment is `align_of::<u64>()`, so `as_mut_ptr()` is
    // exactly that aligned -- and these two asserts are the whole of the guarantee, since the
    // point of the `u64` element type is that the compiler cannot check the cast for us.
    assert!(std::mem::align_of::<libc::mmsghdr>() <= std::mem::align_of::<u64>());
    assert!(std::mem::align_of::<libc::iovec>() <= std::mem::align_of::<u64>());
    // **Capacity.** The words reserved per slot hold a whole struct, so slot `i` of a
    // `vlen * HDR_WORDS`-word buffer is wholly inside it for every `i < vlen`.
    assert!(HDR_WORDS * 8 >= std::mem::size_of::<libc::mmsghdr>());
    assert!(IOV_WORDS * 8 >= std::mem::size_of::<libc::iovec>());
    // **Stride.** `hdrs.add(i)`/`iovs.add(i)` step by `size_of`, so every slot after the first is
    // aligned only if `size_of` is a multiple of `align_of`. Guaranteed by the language; asserted
    // because it is the one thing `div_ceil` above does *not* protect against.
    assert!(
        std::mem::size_of::<libc::mmsghdr>().is_multiple_of(std::mem::align_of::<libc::mmsghdr>())
    );
    assert!(std::mem::size_of::<libc::iovec>().is_multiple_of(std::mem::align_of::<libc::iovec>()));
    // **Harvest.** The two fields [`harvest_headers`] reads back out of a header the kernel wrote
    // lie wholly inside that header (and so inside its reserved `HDR_WORDS * 8` bytes), and are
    // two distinct fields rather than one aliased under two names.
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
/// **Why this is a `libc` call and not a `tokio` one.** `tokio::net::UdpSocket` exposes no
/// vectored multi-message receive; what it does expose is
/// [`UdpSocket::async_io`](tokio::net::UdpSocket::async_io), which waits for readiness and then
/// hands control to a closure that makes the syscall itself. That is the seam this uses, and it is
/// the same shape `crates/logit-inputs/src/tail/watch.rs`'s `inotify` backend already uses for
/// `read(2)` off an inotify fd: `libc` confined to one Linux-gated module, a `// SAFETY:` comment
/// per `unsafe` block, and no raw pointer held anywhere a future could carry it across an `.await`.
///
/// **The `mmsghdr`/`iovec` arrays are rebuilt inside the closure on every call, and the storage
/// that backs them is `Vec<u64>`, not `Vec<mmsghdr>`.** Both halves of that matter, for the same
/// reason: `mmsghdr` contains raw pointers, so a `Vec<mmsghdr>` is `!Send`, and this struct lives
/// across the `.await` inside `read_batch`. Holding one would make the whole read future `!Send`
/// -- and `UdpListener::run_until_shutdown` is an `#[async_trait]` method, which requires `Send`
/// -- leaving `unsafe impl Send` as the only way out, which this codebase does not do and ADR
/// `udp-intake-batching-and-socket-visibility` explicitly rejects. Plain `u64` words carry no
/// pointers, so the struct stays ordinarily `Send`; the pointers exist only for the duration of
/// one synchronous closure call, re-derived from live allocations each time. Rebuilding them is a
/// short loop of stores and allocates nothing, which is why reuse was never worth the hazard.
/// [`assert_batch_read_future_is_send`] pins the property at compile time.
///
/// **`MSG_TRUNC` is detected and counted, not assumed away.** Each slot is
/// [`MAX_DATAGRAM_BYTES`], which covers every IPv4 datagram and all but the last 20 bytes of the
/// largest possible IPv6 one -- so on an IPv6 listener a datagram *can* arrive longer than the
/// `iovec` it is being written into, and the kernel copies what fits and discards the rest. It
/// cannot be caught by looking at `msg_len`, which is the *copied* length and so reads exactly
/// `MAX_DATAGRAM_BYTES` in that case, indistinguishable from a datagram that fit precisely; the
/// kernel reports it in `msg_hdr.msg_flags` instead, which this reads back out of the same header
/// it already reads `msg_len` from. Each one is counted as `logit.input.datagrams.truncated` and
/// the truncated payload is still delivered -- the same bytes the `recv_from` loop this replaced
/// would have delivered, now with the loss visible rather than silent. Growing the slots to 65,527
/// was the alternative and is not worth 20 bytes x `read_batch` of address space plus a constant
/// that stops matching every other 65,507 in this codebase, to avoid a case only a
/// deliberately-jumbo IPv6 sender produces.
#[cfg(target_os = "linux")]
struct BatchReader {
    /// `vlen` contiguous [`MAX_DATAGRAM_BYTES`] slots, one per `iovec`. Allocated once at
    /// construction and never resized. Its *virtual* size is `read_batch * 65,507` bytes; only the
    /// pages a datagram is actually written into are ever faulted in, which is why
    /// `docs/design/memory.md` records both figures for this row and not just the first.
    slots: Vec<u8>,
    /// Backing words for the `vlen` `mmsghdr`s the syscall takes, and for their `vlen` `iovec`s --
    /// see this struct's own doc for why the element type is `u64` rather than the C structs
    /// themselves. Sized once; re-pointed at `slots` on every call.
    hdr_words: Vec<u64>,
    iov_words: Vec<u64>,
    /// Each returned message's `msg_len`, copied out of the `mmsghdr` array before the closure
    /// returns -- the header array's contents are meaningless to anything outside the closure, so
    /// the numbers worth keeping are lifted into plain integer buffers instead.
    lens: Vec<u32>,
    /// Each returned message's `msg_hdr.msg_flags`, lifted out of the same header for the same
    /// reason. Only [`libc::MSG_TRUNC`] is read from it (see [`BatchReader::truncated`]); the rest
    /// of the flag set describes conditions this receive path cannot produce.
    flags: Vec<i32>,
    /// How many datagrams the **last** `read_batch` call had truncated. Reset per call, not
    /// cumulative: [`read_loop`] reports it once per batch alongside the batch's other counts.
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
    /// **One `now_nanos()` per syscall, offset by the datagram's index within the batch** -- the
    /// named accuracy concession in ADR `udp-intake-batching-and-socket-visibility`. The kernel does
    /// not report a per-message receive instant through this path (that is `SO_TIMESTAMP`, a
    /// per-message cmsg mechanism the same ADR rejects), so datagram `i` of a batch is stamped
    /// `base + i` nanoseconds: ordered and distinct, but with a spacing that is a placeholder rather
    /// than a measurement. Still strictly better than stamping at decode time, which can skew
    /// arbitrarily far behind arrival under backlog.
    ///
    /// **The `+ i` is not cosmetic.** A downstream keyed on (series, timestamp) treats two points
    /// that share both as *one* point -- `influxdb_out`'s line protocol overwrites, and its
    /// `allocate_timestamp` disambiguation is cleared at the top of every `Encoder::encode`, so it
    /// only ever covers collisions *inside* one output batch. A whole read batch stamped with one
    /// instant would produce same-timestamp same-series points that straddle an output-batch
    /// boundary and so reach the sink undisambiguated. One nanosecond per datagram is free (no
    /// extra clock read), strictly ordered, and orders of magnitude below the batch's own arrival
    /// uncertainty.
    ///
    /// **One right-sized `Bytes::copy_from_slice` per datagram**, exactly as the `recv_from` loop
    /// this replaces did: reading straight into a shared buffer and slicing it would save the copy
    /// but let one retained event pin a 65 KB slot (`docs/design/memory.md`'s "considered and
    /// rejected", pinned by `datagram_copy_is_one_right_sized_allocation`).
    ///
    /// **A zero-length datagram is legal UDP and is delivered as one**, the same as `recv_from`
    /// returning `n == 0` did: an empty `Bytes` goes on `out` and is counted like any other, and
    /// what to make of it is the decoder's business, not the reader's.
    ///
    /// **A truncated datagram is delivered too**, as far as it was copied, and counted -- see
    /// [`BatchReader::truncated`] and this type's own doc.
    ///
    /// **Cancellation loses nothing.** `async_io` suspends in exactly two places
    /// (`Registration::async_io`, `tokio/src/runtime/io/registration.rs`, tag `tokio-1.53.1`: the
    /// `self.readiness(interest).await?` and the `poll_fn(coop::poll_proceed).await` that follows
    /// it), and **both are strictly before** it calls the closure; once the closure returns
    /// anything but `WouldBlock`, `async_io` returns in that same poll without suspending again.
    /// The syscall and everything after it therefore run synchronously in one poll, so a `select!`
    /// that drops this future either drops it before the syscall or not at all -- the same
    /// guarantee `recv_from`'s own cancel-safety rests on, and for the same reason, since
    /// `recv_from` is the same `async_io` call with a different closure.
    async fn read_batch(
        &mut self,
        socket: &tokio::net::UdpSocket,
        out: &mut Vec<Datagram>,
    ) -> std::io::Result<usize> {
        use std::os::fd::AsRawFd;

        let fd = socket.as_raw_fd();
        let vlen = self.vlen;
        // Captured field by field (Rust 2021 disjoint closure capture) so the closure borrows
        // three plain-data buffers rather than all of `self`.
        let slots = &mut self.slots;
        let hdr_words = &mut self.hdr_words;
        let iov_words = &mut self.iov_words;
        let lens = &mut self.lens;
        let flags = &mut self.flags;

        // `READABLE | ERROR`, matching what `tokio`'s own `UdpSocket::recv_from` waits on rather
        // than readability alone: a socket with only a pending error queued is not "readable" to
        // the poller, and an arm that never wakes is how that turns into a stalled listener.
        let received = socket
            .async_io(tokio::io::Interest::READABLE | tokio::io::Interest::ERROR, || {
                // Rebuilt on every call of this `FnMut` (`async_io` may call it more than once,
                // clearing readiness between `WouldBlock`s), so every pointer the kernel is handed
                // is derived fresh from a live allocation within this same call -- there is no
                // stale-provenance window across calls, and no raw pointer outlives the closure.
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
                        // Unreachable on this socket, kept deliberately. `__skb_recv_udp`
                        // (`net/ipv4/udp.c`) only reaches the sleeping path -- the sole source of
                        // `sock_intr_errno`'s `EINTR` (`__skb_wait_for_more_packets`,
                        // `net/core/datagram.c`) -- through `while (timeo && ...)`, and `timeo`
                        // is zero here twice over: `MSG_DONTWAIT` is passed explicitly, and
                        // `____sys_recvmsg` (`net/socket.c`) ORs it in anyway for any `O_NONBLOCK`
                        // fd, which a tokio-registered socket always is. So this arm costs one
                        // never-taken comparison and buys the same defence `quinn-udp`'s
                        // `retry_if_interrupted` keeps for the same call -- cheap insurance
                        // against a future blocking caller, a different socket type, or a kernel
                        // that stops short-circuiting. Forcing it takes
                        // `strace -e inject=recvmmsg:error=EINTR`; a signal cannot produce it.
                        continue;
                    }
                    // `EAGAIN`/`EWOULDBLOCK` already arrive as `ErrorKind::WouldBlock`, which is
                    // exactly the signal `async_io` needs to clear readiness and wait again.
                    // Every other errno is fatal to the listener. That is the right call because
                    // every errno this socket can actually produce is permanent, not transient:
                    // the ICMP-derived ones everyone reaches for a retry over
                    // (`ECONNREFUSED`/`EHOSTUNREACH`/`ENETUNREACH`/`EPROTO`, PMTU `EMSGSIZE`) are
                    // unreachable here at all -- `udp_err` (`net/ipv4/udp.c`) leaves `sk_err`
                    // alone unless `IP_RECVERR` is set or the socket is `TCP_ESTABLISHED`, and
                    // this one is neither (see `bind_one`) -- and `ENOBUFS` never surfaces on the
                    // receive path, since receive-buffer exhaustion is charged and dropped in
                    // softirq by `__udp_queue_rcv_skb`. What is left is `EBADF`/`ENOTSOCK`/
                    // `EINVAL`/`EFAULT` (caller bugs), `EPERM`/`ENOSYS` (seccomp or an LSM), and
                    // `ECONNABORTED` from `udp_abort` -- an `ss -K`/`SOCK_DESTROY` on this socket,
                    // which unhashes it for good. Retrying any of them is worse than failing:
                    // `sock_error`'s `xchg` clears `sk_err`, so a retry after `udp_abort` reads
                    // `EAGAIN` and the listener sits on a dead socket forever, silently. See
                    // ADR `udp-intake-batching-and-socket-visibility`'s 2026-09-21 amendment.
                    return Err(err);
                }
            })
            .await?;

        let base = now_nanos();
        self.truncated = 0;
        for i in 0..received {
            // Dead code, on purpose. `udp_recvmsg` (`net/ipv4/udp.c`, and `udpv6_recvmsg`
            // identically) computes `err = copied; if (flags & MSG_TRUNC) err = ulen;` -- it
            // reports the *real* datagram length only when `MSG_TRUNC` is passed as an **input**
            // flag, which `recvmmsg_into` does not do (it passes `MSG_DONTWAIT` alone). So
            // `msg_len` is always the copied length, `<= iov_len = MAX_DATAGRAM_BYTES`, and a
            // longer datagram is signalled through the *output* `msg_flags` instead (below).
            // Kept as defence in depth so a hostile or broken value could only ever shorten the
            // slice, never index past the slot; the `debug_assert_eq!` below is what keeps the
            // claim honest, and it runs in every test build --
            // `an_oversized_ipv6_datagram_is_delivered_truncated_and_counted` drives exactly the
            // truncating case through it.
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
                // `base + i`, saturating only so the arithmetic is total -- `now_nanos()` is ~1.8e18
                // and `i` is at most 1023, so the addition is nowhere near `i64::MAX` in any year
                // this code will run in.
                received_at: base.saturating_add(i as i64),
            });
        }
        Ok(received)
    }
}

#[cfg(target_os = "linux")]
impl BatchReader {
    /// How many of the datagrams the last [`BatchReader::read_batch`] call returned arrived longer
    /// than a slot and were copied only as far as one -- an IPv6-only case, see this type's doc.
    /// Per call, not cumulative.
    fn truncated(&self) -> u64 {
        self.truncated
    }
}

/// Writes `vlen` `iovec`s into `iov_words` and `vlen` `mmsghdr`s into `hdr_words`, header `i`
/// describing slot `i` of `slots` -- the `[i * slot_bytes, (i + 1) * slot_bytes)` byte range --
/// through a one-entry `iov`. Every header is fully re-initialized, so a previous call's kernel
/// writeback is overwritten rather than carried forward.
///
/// **Why this is its own function.** It is the half of [`BatchReader::read_batch`]'s closure that
/// is pure: no syscall, no fd, nothing but pointer arithmetic over three caller-owned buffers. That
/// is what makes it reachable under `miri`, which has no shim for `recvmmsg` and so can never
/// execute the closure as a whole (`docs/adr/out-of-ci-unsafe-verification.md`). The pointer
/// provenance, the stride, the slot disjointness and the zero-initialization are the whole of what
/// `NET-01` is a P0 for, and all of it lives here where a tool can see it. `script/unsafe-check
/// miri` runs `mod batch_reader_helpers` against it.
///
/// **Why `mem::zeroed()` + two field assignments, not a struct literal.** rust-`libc`'s musl
/// `msghdr` carries private `__pad1`/`__pad2` fields a struct literal cannot set
/// (rust-lang/libc#2344), and an unzeroed pad is libuv#3419's spurious `EMSGSIZE`. Zeroing also
/// leaves `msg_name`/`msg_namelen`/`msg_control`/`msg_controllen` NULL/0, which is what tells the
/// kernel to report neither a source address nor any ancillary data -- and what makes the
/// `msg_flags`/`msg_controllen` the kernel writes back per call (`____sys_recvmsg`,
/// `net/socket.c`) inert, since they are overwritten here before anything could read them.
///
/// Panics rather than trusting its caller: the three length preconditions are the ones that would
/// otherwise be undefined behaviour, and checking them costs three comparisons per *batch*.
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

/// The syscall, and nothing else -- a thin shim between [`build_headers`] and
/// [`harvest_headers`] so that the two halves either side of it stay pure and therefore
/// `miri`-runnable. Returns `recvmmsg`'s own return value: `>= 0` is a datagram count, `-1` means
/// consult `errno`.
///
/// The timeout argument is NULL ("no timeout"), which is the only value sound to pass from here:
/// a `timespec` would have to outlive a call this shim cannot see the end of. It is also the value
/// that side-steps `recvmmsg(2)`'s documented timeout bug (BUGS: the timeout is only checked
/// *after* a datagram arrives), and `MSG_WAITFORONE` is deliberately not passed -- it is a no-op
/// alongside `MSG_DONTWAIT`.
///
/// # Safety
///
/// `hdr_words` must hold at least `vlen` initialized `mmsghdr`s exactly as [`build_headers`]
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

/// Lifts the `msg_len` and `msg_hdr.msg_flags` the kernel wrote into the first `n` headers of
/// `hdr_words` out into `lens`/`flags`, and touches nothing beyond `n`.
///
/// The header array's contents are meaningless to anything outside [`BatchReader::read_batch`]'s
/// closure -- they hold raw pointers, which is exactly what the `Vec<u64>` storage exists to keep
/// out of a `Send` future -- so the two numbers worth keeping are copied into plain integer
/// buffers here, and the headers are then free to be rebuilt from scratch on the next call.
///
/// Pure, for the same reason [`build_headers`] is: under `miri` a test plays the kernel's part by
/// writing into the same headers through the same pointer type, and this reads back exactly what
/// was written.
///
/// Panics if `n` exceeds any of the three buffers. The kernel cannot return more than the `vlen`
/// it was given, so this is unreachable; it is an assert rather than a silent `take(n)` because a
/// count that large would mean the ABI assumption underneath all of this had broken, and that
/// should be loud.
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

/// Compile-time proof of the property [`BatchReader`]'s doc comment rests on: the future returned
/// by `read_batch` is `Send`, with no `unsafe impl` anywhere behind it.
///
/// `UdpListener::run_until_shutdown` being an `#[async_trait]` method already forces this
/// indirectly -- a `!Send` read future would fail to compile there, several layers up, with an
/// error naming the trait rather than the cause. This says it once, next to the code whose layout
/// decisions (`Vec<u64>` storage, arrays rebuilt inside the closure) exist for no other reason, so
/// a change that breaks it fails here first.
#[cfg(target_os = "linux")]
#[allow(dead_code)] // a type-checked assertion, never called
fn assert_batch_read_future_is_send(
    reader: &mut BatchReader,
    socket: &tokio::net::UdpSocket,
    out: &mut Vec<Datagram>,
) {
    fn assert_send<T: Send>(_: &T) {}
    assert_send(&reader.read_batch(socket, out));
}

/// The non-Linux read half: today's one-`recv_from`-per-datagram loop, behind the same interface
/// its `recvmmsg` counterpart above presents, so [`read_loop`] is one code path rather than two.
///
/// `recvmmsg(2)` is a Linux syscall; there is no portable equivalent worth the second
/// implementation (`sendmmsg`/`recvmmsg` exist on FreeBSD but with a different enough surface to
/// be its own port, and `logit` ships no such target today). `read_batch` is therefore parsed,
/// validated and documented everywhere, but only *reads* in batches on Linux -- the decode half's
/// `pop_many` uses the same value on every target. Mirrors how `logit_pipeline::sockstat`'s
/// non-Linux twins keep one call site rather than two.
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
    /// any, so this target has nothing to count. The `logit.input.datagrams.truncated` counter is
    /// Linux-only for that reason, and is documented as such.
    fn truncated(&self) -> u64 {
        0
    }

    /// One datagram, appended to `out`; always returns `1` on success. Cancel-safe exactly as
    /// `tokio::net::UdpSocket::recv_from` is.
    async fn read_batch(
        &mut self,
        socket: &tokio::net::UdpSocket,
        out: &mut Vec<Datagram>,
    ) -> std::io::Result<usize> {
        let (n, _peer) = socket.recv_from(&mut self.buf).await?;
        out.push(Datagram {
            bytes: Bytes::copy_from_slice(&self.buf[..n]),
            received_at: now_nanos(),
        });
        Ok(1)
    }
}

/// How often [`read_loop_sampled`] reads the socket's kernel counters. One `getsockopt` a second
/// per UDP listener -- small enough not to need a config knob, frequent enough that
/// `logit.input.receive_buffer.utilization` is a usable gauge rather than a coarse average, and
/// deliberately the same cadence whether or not traffic is arriving (see the wrapper's doc).
const KERNEL_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// [`read_loop`], plus a sampler that reads the kernel's own per-socket counters on a fixed
/// interval for as long as the read loop is running, and once more after it stops.
///
/// **Why the sampler cannot live inside `read_loop`.** The moments worth sampling are exactly the
/// moments `read_loop` is not going round: under `overflow: block` it parks in `queue.push` until
/// downstream makes room, and the kernel's receive buffer -- which cannot wait -- is filling and
/// then dropping the whole time. A sample taken at the top of each read iteration would therefore
/// go quiet precisely when the numbers start mattering. Pinning `read_loop` as one arm of a
/// `select!` against a timer solves that without a task, a channel or a `'static` bound: the loop
/// below re-polls the *same* `read_loop` future each time the timer wins, so a blocked `push`
/// resumes exactly where it was and nothing is cancelled. (Cancelling and restarting `read_loop`
/// here would drop a datagram per tick; it is never dropped and re-created.)
///
/// **The final sample runs on every path [`sample_while`] itself returns on.** Drops in the last
/// fraction of a second before a fatal socket error or a shutdown are as real as any other, and
/// with a one-second interval they are the likeliest ones to exist at all -- a listener usually
/// stops *because* something went wrong. So the sampler runs once more after `read_loop` has
/// returned, before this function forwards that result. The socket is still open at that point (it
/// is owned by `run_until_shutdown`, which outlives this future), so the counters are still
/// readable.
///
/// It is **not** unconditional, and the difference is worth stating precisely rather than leaving
/// "guaranteed" to be read as more than it is. There is exactly one path that skips it: a future
/// that is *dropped* runs nothing, and `run_input`'s grace backstop
/// (`logit_pipeline::runtime`, the `shutdown_grace_expired` arm of its `select!`) drops this whole
/// future when the grace expires. Production never reaches it -- `read_loop` races `shutdown` in
/// both of its own `select!`s, so it returns within microseconds of the signal and the final
/// sample lands long before the grace could, and `input_runtime_config` supplies
/// `ReceiveConfig::default()`'s 5 s rather than `InputRuntimeConfig::default()`'s
/// `Duration::ZERO`. At `Duration::ZERO`, which tests do construct, both arms are ready at once
/// and `select!`'s random rotation drops this future roughly half the time. See ADR
/// `udp-intake-batching-and-socket-visibility`'s 2026-09-21 amendment.
///
/// **Once the sampler has disabled itself, no timer is armed at all** and this is exactly
/// [`read_loop`]. That is the other half of `sockstat`'s "report once and stop asking": a listener
/// on a non-Linux build (or a kernel without `SO_MEMINFO`) would otherwise wake once a second,
/// forever, to call a function that returns immediately -- a cost this would have added to every
/// idle listener on those platforms in exchange for nothing. The final sample still runs in that
/// state, where it is a no-op.
///
/// Sampling is synchronous and inline -- one `getsockopt` on an fd this process owns, which is a
/// bounded read of kernel memory with no I/O wait, so `spawn_blocking` would cost more than the
/// call it wrapped.
///
/// The `select!`'s arm ordering is not incidental -- see [`sample_while`], which holds the loop.
async fn read_loop_sampled(
    socket: &tokio::net::UdpSocket,
    queue: Arc<ReceiveQueue>,
    telemetry: Telemetry,
    diag: Diagnostics,
    shutdown: watch::Receiver<bool>,
    read_batch: usize,
) -> anyhow::Result<()> {
    let sampler = ReceiveBufferSampler::new(socket, telemetry.clone(), diag);
    let read = read_loop(socket, queue, telemetry, shutdown, read_batch);
    sample_while(sampler, read, KERNEL_SAMPLE_INTERVAL).await
}

/// [`read_loop_sampled`]'s loop, over an arbitrary `read` future and an arbitrary interval --
/// split out for exactly the reason [`bind_first_available`] is, to make two paths reachable from
/// a test that production never reaches on its own: the *disabled* sampler (the non-Linux shape,
/// unreachable on the Linux this is tested on), and the coop-budget starvation the arm ordering
/// below exists to prevent, which needs a `read` future that misbehaves in a specific way and an
/// interval short enough to observe in a test.
///
/// **The timer arm comes first, and that ordering is load-bearing.** The intuitive order is the
/// other one -- prefer the work, treat the sample as something to do while idle -- and it is
/// wrong, for a reason that is invisible until you measure it.
///
/// The mechanism, stated against the version this is pinned to -- **`tokio-1.53.1`**, and every
/// one of the four facts below has to be re-checked on a bump, which is what this paragraph is
/// for:
///
/// 1. A task's cooperative-scheduling budget starts at **128** units per poll
///    (`task/coop/mod.rs`, `const fn initial() -> Budget { Budget(Some(128)) }`).
/// 2. A **successful** `async_io` spends one of them (`runtime/io/registration.rs`:
///    `coop.made_progress()` on the success arm). A `WouldBlock` one spends **zero** -- it drops
///    the `RestoreOnPending` guard without calling `made_progress`, and that guard's `Drop` writes
///    the pre-decrement budget back. So the budget drains exactly under the flood this sampler
///    exists to report, where every read succeeds and `read_loop` never parks for a real reason:
///    the only way it returns `Pending` is by running the budget to zero.
/// 3. `Sleep`'s poll consults coop **before** its deadline (`time/sleep.rs`, `poll_elapsed`:
///    `let coop = ready!(crate::task::coop::poll_proceed(cx));` ahead of any use of the deadline
///    or the timer entry). So a timer arm polled *after* the read arm finds a budget of zero and
///    returns `Pending` however far past its deadline it is -- and, never having reached the timer
///    driver, is not even registered to be woken by it. The next wake re-polls in the same order
///    with the same result, forever.
/// 4. `select!` itself gates on the budget before polling **any** arm (`macros/select.rs`:
///    `ready!(poll_budget_available(cx))` is the first thing its generated `poll_fn` does), so the
///    whole `select!` below is subject to (3) as a unit, not just the sleep inside it.
///
/// **A `Pending` caused by the coop budget is not a park, and an arm placed behind one never
/// runs.** With the timer arm first the budget is intact when `Sleep::poll` runs, it finds the
/// deadline unmet, and its own `RestoreOnPending` rolls the decrement back before the read arm
/// burns the budget to zero -- so the sleep is registered with the timer driver on every poll and
/// the tick lands on time.
///
/// Two things the earlier wording of this comment got wrong, recorded so they are not
/// reintroduced: a `WouldBlock` `async_io` does **not** spend a unit (see 2), and
/// `watch::Receiver::wait_for` -- `read_loop`'s other arm -- has no coop call on its path at all,
/// so neither is part of why the budget drains. The read side's *successful* `recvmmsg` is.
///
/// Measured on a release build, eight senders flooding one listener for 10 s at roughly 90% kernel
/// loss: with the read arm first, 0 of 10 one-second windows carried `kernel.drops` or the buffer
/// gauges at all -- 41-47M drops surfaced as a single lump from the final sample after SIGTERM,
/// which is precisely the "you cannot see it while it is happening" this work set out to fix. With
/// the timer arm first, 11 of 11 windows carried them, the final sample still landed separately
/// with a non-zero residual, and throughput did not measurably move (4.7M datagrams read, against
/// 4.7-5.0M).
///
/// The cost of this ordering is one `Sleep::poll` per wake before the read arm is polled -- a
/// deadline comparison against a timer that is nearly always not yet due. The guaranteed final
/// sample is unaffected: the read arm still wins the moment `read_loop` actually returns, and a
/// tick that beats it to the punch only means the remainder is what the final sample reports.
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

/// Reads one UDP socket's kernel-side receive counters into telemetry -- the drop counter and the
/// receive buffer's fill level, both of which are invisible to `recv_from` itself.
///
/// Disables itself for good on the first failed read: `SO_MEMINFO` either exists for a socket or
/// it never will (an older kernel, a non-Linux build), so retrying it every second would be a
/// syscall per second forever in exchange for nothing. One `warn` says so, once.
struct ReceiveBufferSampler {
    /// The listener socket's descriptor, captured once. `None` only on a platform with no raw
    /// descriptors at all, where [`logit_pipeline::sockstat`] reports nothing anyway. Safe to
    /// hold as a bare fd rather than a borrow: this sampler is created and dropped inside
    /// [`read_loop_sampled`], whose `socket` argument outlives it.
    fd: Option<sockstat::RawFd>,
    drops: sockstat::DropCounter,
    telemetry: Telemetry,
    diag: Diagnostics,
    enabled: bool,
}

impl ReceiveBufferSampler {
    fn new(socket: &tokio::net::UdpSocket, telemetry: Telemetry, diag: Diagnostics) -> Self {
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
    /// `logit.input.kernel.drops` is reported only when the delta is nonzero, matching how every
    /// other loss counter here behaves (`logit.component.datagrams.dropped` does not emit a zero
    /// either) -- the two gauges alongside it are what tell an operator the sampler is alive.
    ///
    /// `logit.input.receive_buffer.bytes` is re-emitted on every sample even though the value
    /// never changes after bind. `ComponentBuffer::drain` (`logit_core::telemetry`) `mem::take`s
    /// its point map, so a gauge written once at bind time appears in exactly one `internal` drain
    /// window and then vanishes from the series forever -- which would leave the utilization gauge
    /// below with no visible denominator a minute into the process's life. `finish_bind` still
    /// emits it (and still warns about an `rmem_max` clamp) so the value is there before this loop
    /// ever runs, and for a `logit` that fails during startup.
    fn sample_once(&mut self) {
        if !self.enabled {
            return;
        }
        let info = self.fd.ok_or(sockstat::Unavailable::NoDescriptor).and_then(sockstat::meminfo);
        let info = match info {
            Ok(info) => info,
            Err(err) => {
                self.enabled = false;
                // What the kernel actually said, and the version hint *only* where it applies:
                // `EBADF` here would mean a stale or reused descriptor -- a bug in logit, and the
                // one cause an operator most needs to not see dressed up as "upgrade your kernel".
                // `Unavailable::is_unsupported_option` is what draws that line.
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
                // A build that is simply not Linux (or has no raw descriptors at all) can do
                // nothing about this, and `internal`'s `logs:` setting captures `warn` into the
                // pipeline by default -- so one line per listener at every startup would be noise
                // an operator cannot act on. Everything else is a real failure on a platform that
                // should have worked, and stays at `warn`.
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
        // Both terms come from this one `SO_MEMINFO` read, deliberately: they are the kernel's own
        // comparable pair, and mixing either with a number from anywhere else (the operator's
        // requested `receive_buffer_bytes`, the queued payload bytes) gets the ratio wrong in a
        // way that still looks plausible. `SockMeminfo::receive_utilization`'s doc has the three
        // wrong pairings spelt out.
        if let Some(utilization) = info.receive_utilization() {
            self.telemetry.gauge("logit.input.receive_buffer.utilization", utilization, &[]);
        }
    }
}

/// Pops datagrams from `queue`, decodes and accumulates them into batches, and sends each
/// completed batch through `sink` -- entirely independent of how fast `read_loop` is filling
/// `queue`. Uses [`ReceiveQueue::pop_many`] (not `peek`/`commit`): a datagram that fails to decode
/// is diagnosed and dropped, never retried, and `pop_many` is cancellation-safe
/// (`logit_pipeline::queue::BoundedQueue::pop_many`'s own doc comment) -- this whole future can be
/// dropped mid-await by `run_input`'s grace backstop.
///
/// **Why `pop_many` rather than `pop`.** Every `pop` refreshes the queue's three depth/bytes/
/// utilization gauges, each of which locks the component's telemetry buffer, and `read_loop`
/// (pushing) contends on that same lock from the other side; taking up to `read_batch`
/// (`BatchingConfig::pop_batch`, the same value the read half's `recvmmsg` uses -- one knob, both
/// ends of one queue) datagrams per call collapses that to one set of updates per batch.
/// `receive.latency` stays
/// **per datagram** -- it is the number that says whether event timestamps are trustworthy under
/// load, and a per-batch figure would lose exactly the resolution it exists to report.
///
/// One consequence to name, since it widens an already-accepted loss: this future being dropped
/// mid-batch (the grace backstop) now discards up to `read_batch` popped-but-not-yet-decoded
/// datagrams instead of the one `pop` held, uncounted, on the shutdown path only -- the decode-side
/// twin of the `push_many` cancellation the ADR names.
///
/// Owns `sink` (the `Fanout`) -- dropping this future is what closes every downstream inbox, the
/// shutdown cascade `docs/adr/service-lifecycle-and-output-retry.md` established.
///
/// Flushes the accumulator's final contents (`FlushReason::Shutdown`) only once `pop_many` reports
/// closed-and-empty (a return of `0`) -- i.e. only after `read_loop` can no longer push anything
/// new, the same
/// "flush only once nothing can race it" reasoning `finish_and_flush`
/// (`logit_pipeline::runtime`) uses on the sink side. Reuses `run_transform`'s deadline-race
/// pattern for the interval trigger via `BatchAccumulator::next_deadline`, rather than a second
/// copy of that cadence math.
async fn decode_loop<D: Decoder + Send>(
    decoder: &mut D,
    queue: Arc<ReceiveQueue>,
    sink: Fanout,
    batching: BatchingConfig,
    telemetry: Telemetry,
    mut diag: Diagnostics,
) {
    let mut accumulator = BatchAccumulator::new(batching.max_events, batching.max_bytes);
    // Reused across every `decode_into` call, cleared (not replaced) between them, so its
    // allocated capacity survives from one datagram to the next -- `BatchAccumulator::absorb`'s
    // own doc comment explains why this is what actually realizes the allocation win, and why
    // `std::mem::take` anywhere in this loop would silently undo it.
    let mut scratch: Vec<Event> = Vec::new();
    // Reused across every `pop_many` call for the same reason `scratch` is reused across every
    // `decode_into` call: drained (not replaced) each time round, so its capacity survives and the
    // steady state allocates nothing.
    let mut popped: Vec<Datagram> = Vec::new();
    let has_interval = !batching.flush_interval.is_zero();
    let mut next_flush =
        has_interval.then(|| tokio::time::Instant::now() + batching.flush_interval);

    loop {
        // The interval trigger is checked once per *popped batch* rather than once per datagram
        // now, which can only ever delay an interval flush by however long it takes to decode and
        // absorb up to `read_batch` datagrams -- tens of microseconds of pure CPU against a
        // 100 ms default interval, and bounded by the batch size regardless of how deep the backlog
        // is. The one unbounded term in that span, an `emit` awaiting a full downstream inbox, is
        // not new: a single datagram's `emit` could already park here for as long as downstream is
        // stalled, and a stalled downstream delays the interval flush either way.
        if let Some(deadline) = next_flush {
            let now_instant = tokio::time::Instant::now();
            if deadline <= now_instant {
                if let Some(batch) = accumulator.take() {
                    emit(&sink, &telemetry, batch, FlushReason::Interval).await;
                }
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
            // error). Flush whatever's left -- nothing more will ever arrive either way.
            if let Some(batch) = accumulator.take() {
                emit(&sink, &telemetry, batch, FlushReason::Shutdown).await;
            }
            return;
        }

        // Drained, not iterated by reference: each datagram's `Bytes` is handed to `decode_into` by
        // value, and draining also frees each one as it is consumed rather than at the end of the
        // batch. In arrival order -- `pop_many` appends in FIFO order and this preserves it.
        for datagram in popped.drain(..) {
            let latency_nanos = (now_nanos() - datagram.received_at).max(0) as u64;
            telemetry.timing(
                "logit.component.receive.latency",
                Duration::from_nanos(latency_nanos),
                &[],
            );

            scratch.clear();
            match decoder.decode_into(datagram.bytes, datagram.received_at, &mut scratch) {
                Ok((resource, scope)) => {
                    // `scope` is whatever `decoder.decode_into` returned -- `None` for every
                    // decoder this loop drives today (statsd/syslog datagrams have no OTLP
                    // instrumentation-scope concept), but threaded through rather than hardcoded so
                    // a future `Decoder` on this same loop that does carry one isn't silently
                    // dropped.
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

/// `sink.send` mints a fresh [`logit_pipeline::TraceContext::new_root`] here -- once per
/// *accumulated* batch, not once per datagram that fed it. Before ADR `decoupled-listener-io`, one datagram was one
/// `Fanout::send`, so every datagram got its own root; now a `batch_max_events` greater than 1
/// deliberately correlates however many datagrams the accumulator happened to merge under one
/// shared root, even though they arrived independently and share no other relationship. This is
/// not a new hazard class, just a new place `TraceContext`'s own doc comment's already-tracked gap
/// shows up: a stateful transform's `flush()` has minted one root per flush (covering however many
/// batches contributed to it) since before this PR, for the identical reason -- no single parent
/// to attribute a many-to-one emission to. `docs/known-gaps.md`'s internal-spans entry is the one
/// place this is tracked; not duplicated here.
async fn emit(sink: &Fanout, telemetry: &Telemetry, batch: EventBatch, reason: FlushReason) {
    telemetry.count("logit.component.receive.flushed", 1.0, &[("reason", reason.as_str())]);
    sink.send(batch).await;
}

/// `pub(crate)` rather than private: [`crate::tcp`]'s connection loop stamps `received_at` the
/// same way, and one shared clock reader is better than two copies that could drift apart.
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

    /// The `read_batch` the tests below that don't care about the value itself pass to
    /// [`read_loop`]/[`BatchingConfig`] -- `UdpListenerConfig::default`'s own 64, spelled here so
    /// a test asserting across batch boundaries (`a_backlog_deeper_than_the_pop_batch_...`) says
    /// which number it is reasoning about rather than reaching into the config for it.
    const TEST_POP_BATCH: usize = 64;

    /// A trivial `Decoder`: one datagram -> one event, except the literal bytes `b"BAD"`, which
    /// are rejected -- enough to exercise decode-error handling without pulling in statsd/syslog
    /// grammar specifics. Every decoded event's `attributes` carries the raw datagram under
    /// `"payload"`, and its `timestamp` is exactly the `received_at` it was handed -- both of
    /// which these tests use to identify which datagram produced which event.
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

    fn test_queue(overflow: OverflowPolicy, max_datagrams: usize) -> Arc<ReceiveQueue> {
        Arc::new(BoundedQueue::with_metrics(
            QueueConfig { max_items: max_datagrams, max_weight: u64::MAX, overflow },
            &RECEIVE_QUEUE_METRICS,
            Telemetry::default(),
        ))
    }

    /// The central property this whole workstream exists for: a stalled downstream `Fanout`
    /// consumer must never stop the read half from keeping the socket drained -- unlike the
    /// pre-ADR `decoupled-listener-io` loop, where `recv_from` and `Fanout::send` shared one path.
    ///
    /// Proven by direct construction rather than a timing guess: the `Fanout`'s one consumer has
    /// channel capacity 1 and is never `.recv()`d, so `decode_loop` blocks forever the moment its
    /// *second* `Fanout::send` is attempted (the first fits in the empty channel) -- deterministic
    /// regardless of scheduling, since a blocked send means no further `queue.pop()` calls happen
    /// either. Exactly two datagrams are ever removed from `queue` this way; everything `read_loop`
    /// pushes afterward either grows the queue or (once at its 4-item bound) evicts under
    /// `drop_oldest` -- so once every send has landed, draining `queue` directly must find exactly
    /// 4 items still sitting in it, never 0 (which is what a backpressured reader would leave).
    ///
    /// `read_loop`/`decode_loop` run as plain (unspawned) futures raced via `select!` against the
    /// test's own driver, not `tokio::spawn`/`spawn_local` -- both require `'static`, which a
    /// stack-local `socket`/`&mut decoder` can't satisfy, and `decode_loop` here never returns on
    /// its own (that's the scenario under test), so it must be raced away from, not awaited.
    #[tokio::test]
    async fn the_reader_keeps_reading_while_the_downstream_fanout_is_never_drained() {
        let socket = bind_ephemeral().await;
        let addr = socket.local_addr().unwrap();
        let queue = test_queue(OverflowPolicy::DropOldest, 4);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (fanout, _rx) = recording_fanout(1);
        let telemetry = Telemetry::default();
        let mut decoder = TestDecoder::new();

        tokio::pin! {
            let read_fut = read_loop(
                &socket,
                Arc::clone(&queue),
                telemetry.clone(),
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
            // Send more datagrams than the queue's own depth (4) -- if the reader ever stopped
            // reading because of downstream backpressure, some of these sends would pile up in
            // the OS receive buffer instead of ever reaching `queue`; instead `drop_oldest` just
            // evicts, and the reader keeps consuming every one.
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
        // Neither loop future is polled again after the `select!` above returns -- simply
        // letting `read_fut`/`decode_fut` fall out of scope at the end of this function is what
        // stops them, safely, mid-poll -- and it's what makes the queue below inspectable with
        // nothing else concurrently touching it.

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

    /// On shutdown, whatever the read half already queued must still be decoded and delivered --
    /// not silently dropped -- within the grace `run_until_shutdown` is given.
    #[tokio::test]
    async fn shutdown_drains_the_queue_and_delivers_every_already_queued_datagram() {
        let mut listener = UdpListener::new(
            "127.0.0.1:0",
            TestDecoder::new(),
            UdpListenerConfig {
                batch_max_events: 1,
                batch_flush_interval: Duration::ZERO,
                shutdown_grace: Duration::from_secs(5),
                ..UdpListenerConfig::default()
            },
        );

        // `Input::bind`/`UdpListener::local_addr` (docs/plans/operator-surface.md, workstream B)
        // now make the OS-assigned port observable before `run` -- see
        // `bind_then_run_delivers_a_real_datagram` below for the test that actually exercises a
        // real socket round trip. This test still proves the *shutdown* contract specifically
        // through `UdpListener::run_until_shutdown` end to end: shut down almost immediately, and
        // since nothing was sent, this only proves a clean, prompt shutdown with nothing queued.
        // The queued-backlog case is covered directly against `read_loop`/`decode_loop` below.
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

    // -- workstream B: `Input::bind`/`local_addr` (docs/plans/operator-surface.md) --

    /// The primitive `shutdown_drains_the_queue_...` above says was impossible before this
    /// workstream: bind first, learn the real address via `local_addr`, *then* send a real
    /// datagram to it and see it delivered -- with `run_until_shutdown` never having called
    /// `bind()` itself.
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

    /// A second `bind()` call is a no-op, per [`logit_pipeline::Input::bind`]'s idempotency
    /// contract -- it must not try to rebind (and fail with "address in use") against the socket
    /// it already holds.
    #[tokio::test]
    async fn a_second_bind_is_a_no_op() {
        let mut listener =
            UdpListener::new("127.0.0.1:0", TestDecoder::new(), UdpListenerConfig::default());
        listener.bind().await.expect("first bind should succeed");
        let addr = listener.local_addr().expect("bind() should leave a real address behind");
        listener.bind().await.expect("second bind should be a harmless no-op");
        assert_eq!(listener.local_addr(), Some(addr), "the address must not change");
    }

    /// `bind()` surfaces a genuinely unbindable address as an error, same as `run` did before this
    /// method existed (`bind_socket`'s own error path, unchanged). A privileged low port is the
    /// usual way to force this, but this test runs as root in CI's containerized environment
    /// (`docs/adr/containerized-development.md`), where that fails to fail -- occupying a
    /// specific already-bound ephemeral port instead works regardless of privilege.
    #[tokio::test]
    async fn bind_reports_an_unbindable_address() {
        let held = bind_ephemeral().await;
        let addr = held.local_addr().unwrap().to_string();
        let mut listener = UdpListener::new(addr, TestDecoder::new(), UdpListenerConfig::default());
        assert!(listener.bind().await.is_err(), "binding an already-held address should fail");
    }

    /// `run_until_shutdown` still binds on its own when the caller never called `bind()` first --
    /// [`logit_pipeline::Input::bind`]'s documented lazy fallback, and the reason no existing
    /// direct-`run`/`run_until_shutdown` test in this module needed to change for this workstream.
    #[tokio::test]
    async fn run_until_shutdown_binds_when_the_caller_did_not() {
        let mut listener =
            UdpListener::new("127.0.0.1:0", TestDecoder::new(), UdpListenerConfig::default());
        assert_eq!(listener.local_addr(), None);
        let (fanout, _rx) = recording_fanout(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { listener.run_until_shutdown(fanout, shutdown_rx).await });
        // Give the spawned task a chance to reach its own internal `self.bind().await?` -- there
        // is nothing else to synchronize on here since the listener itself was moved into the
        // task, but this is only proving the task didn't immediately error out, not timing a
        // real race.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!handle.is_finished(), "run_until_shutdown should have bound and now be listening");
        handle.abort();
    }

    /// The backlog case `shutdown_drains_the_queue_...` above deferred: datagrams already sitting
    /// in the queue when shutdown fires must still reach the `Fanout`, not be silently dropped.
    #[tokio::test]
    async fn a_backlog_queued_before_shutdown_is_still_decoded_and_delivered() {
        let socket = bind_ephemeral().await;
        let queue = test_queue(OverflowPolicy::DropOldest, 100);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (fanout, mut rx) = recording_fanout(100);
        let telemetry = Telemetry::default();
        let mut decoder = TestDecoder::new();

        // Queue three datagrams directly (bypassing the socket, for determinism), then signal
        // shutdown before either loop starts running -- `shutdown.wait_for` checks the current
        // value on its very first poll, so this ordering is equivalent to shutting down mid-run.
        for i in 0..3u32 {
            queue
                .push(Datagram { bytes: Bytes::from(format!("msg-{i}")), received_at: i as i64 })
                .await;
        }
        shutdown_tx.send(true).expect("receiver should still be alive");

        // `tokio::join!`, not `spawn`/`spawn_local`: both loops genuinely terminate here (unlike
        // the stalled-downstream test above), so waiting for both to finish concurrently is
        // exactly right, and neither `socket` nor `&mut decoder` need to satisfy `'static`.
        let (read_result, ()) = tokio::join!(
            read_loop(&socket, Arc::clone(&queue), telemetry.clone(), shutdown_rx, TEST_POP_BATCH),
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

    /// The same drain, over a backlog several times deeper than the pop batch: `decode_loop`
    /// takes datagrams off the queue a batch at a time now, so "every queued datagram is decoded"
    /// has to hold across batch boundaries, and arrival order has to survive both the batched pop
    /// and the iteration over what it popped. Nothing is sorted here, unlike the test above --
    /// `Fanout`'s channel is FIFO and `batch_max_events: 1` makes one delivery per datagram, so the
    /// received sequence is the decode order exactly.
    #[tokio::test]
    async fn a_backlog_deeper_than_the_pop_batch_is_fully_decoded_in_arrival_order() {
        const BACKLOG: usize = TEST_POP_BATCH * 3 + 7;

        let socket = bind_ephemeral().await;
        let queue = test_queue(OverflowPolicy::DropOldest, BACKLOG * 2);
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
            read_loop(&socket, Arc::clone(&queue), telemetry.clone(), shutdown_rx, TEST_POP_BATCH),
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

    /// A malformed datagram is diagnosed and skipped -- it must not stop the decode loop from
    /// processing whatever comes after it.
    #[tokio::test]
    async fn a_malformed_datagram_is_skipped_without_stopping_the_decode_loop() {
        let socket = bind_ephemeral().await;
        let addr = socket.local_addr().unwrap();
        let queue = test_queue(OverflowPolicy::DropOldest, 10);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (fanout, mut rx) = recording_fanout(10);
        let telemetry = Telemetry::default();
        let mut decoder = TestDecoder::new();

        // A third concurrent future (alongside `read_loop`/`decode_loop`, joined below) since
        // this test needs both loops genuinely *running* while the datagrams are sent -- unlike
        // the backlog test above, where shutdown was already signalled before either loop started.
        let driver = async {
            send_datagram(addr, b"good-1").await;
            send_datagram(addr, b"BAD").await;
            send_datagram(addr, b"good-2").await;
            tokio::time::sleep(Duration::from_millis(50)).await;
            shutdown_tx.send(true).expect("receiver should still be alive");
        };

        let (read_result, (), ()) = tokio::join!(
            read_loop(&socket, Arc::clone(&queue), telemetry.clone(), shutdown_rx, TEST_POP_BATCH),
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

    /// `SO_RCVBUF` reporting: the granted-buffer gauge fires even when `receive_buffer_bytes` was
    /// never set -- an operator should always be able to see the kernel default, not just an
    /// explicit override.
    #[tokio::test]
    async fn bind_socket_reports_the_granted_receive_buffer_even_when_unset() {
        let telemetry = Telemetry::default();
        let mut diag = Diagnostics::default();
        let socket = bind_socket("127.0.0.1:0", None, &telemetry, &mut diag)
            .await
            .expect("binding with no explicit receive_buffer_bytes should succeed");
        // `Telemetry::default()` is the disabled no-op handle, so there's nothing to read the
        // gauge back out of here -- this test's real assertion is simply that `bind_socket`
        // completes and yields a usable socket with no explicit `receive_buffer_bytes`, which is
        // the common (unset) case every other test in this module already relies on implicitly.
        drop(socket);
    }

    /// The regression `bind_first_available` exists to prevent: a `bind:` target resolving to
    /// more than one candidate address must fall through to a later one if an earlier one can't
    /// be bound, not fail outright on the first. Forces a deterministic first-candidate failure
    /// by occupying a real address with another socket first, rather than relying on a specific
    /// hostname's DNS records (unavailable/unpredictable in a test environment).
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
        drop(occupied); // keep alive until here, so the port stays genuinely occupied throughout
    }

    /// The multicast path of [`bind_one`]: a group address binds the unspecified address on that
    /// port with `SO_REUSEADDR` and joins the group, and a datagram sent to the group from an
    /// ordinary socket arrives.
    ///
    /// **Skips rather than fails when the environment has no multicast route** -- a container with
    /// only a bridged `eth0` and no `224.0.0.0/4` route makes the join itself fail, which says
    /// nothing about this code. Production deliberately does *not* skip: `bind_one` returns the
    /// error and startup fails loudly, because a collectd listener that silently joined nothing
    /// would receive nothing forever.
    ///
    /// Port 0 is not usable here (a multicast bind must name the port senders use, and an
    /// OS-assigned one is not knowable to a sender), so the port comes from binding and dropping an
    /// ordinary socket first -- a small race with anything else on the host claiming it in between,
    /// and the reason `SO_REUSEADDR` is set rather than the reason it is.
    #[tokio::test]
    async fn a_multicast_bind_joins_the_group_and_receives_a_datagram_sent_to_it() {
        // collectd's own default IPv4 group (`network` plugin), which is also what
        // `docs/plans/collectd-binary-relay.md` names.
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

        // Bound to the unspecified address, not `127.0.0.1`: the source address a sender binds
        // picks the interface a multicast datagram leaves by, and one pinned to loopback would
        // never reach a group joined on the default (routed) interface.
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
    /// Linux errno numbers (`ENODEV`, `EADDRNOTAVAIL`, `ENETUNREACH`, `EPERM`) -- CI and the dev
    /// container are both Linux, and a non-Linux host simply gets the stricter behaviour of
    /// failing the test rather than skipping it.
    fn is_no_multicast_route(err: &anyhow::Error) -> bool {
        const SKIP_ERRNOS: [i32; 4] = [1, 19, 99, 101];
        err.chain()
            .filter_map(|cause| cause.downcast_ref::<std::io::Error>())
            .any(|io| io.raw_os_error().is_some_and(|code| SKIP_ERRNOS.contains(&code)))
    }

    // -- per-socket kernel visibility (`ReceiveBufferSampler`, `read_loop_sampled`) --------------

    /// How many datagrams a kernel-overrun test blasts at a deliberately tiny receive buffer. At
    /// `receive_buffer_bytes: 8 KiB` Linux grants 16 KiB (it doubles the request) and charges each
    /// of these datagrams several hundred bytes of `skb->truesize`, so a couple of dozen fit and
    /// the rest have nowhere to go -- a margin of nearly two orders of magnitude, which is what
    /// keeps the assertion "more than zero" rather than a number that could flake.
    #[cfg(target_os = "linux")]
    const OVERRUN_DATAGRAMS: usize = 2_000;

    /// Deliberately tiny, and deliberately *requested* rather than assumed: Linux doubles it and
    /// `net.core.rmem_max` may clamp it, and nothing below depends on the exact granted figure.
    #[cfg(target_os = "linux")]
    const TINY_RECEIVE_BUFFER: u64 = 8 * 1024;

    /// Every `logit.input.kernel.drops` delta in `events`, summed -- the counter is a delta `Sum`
    /// per drain, so a total is what an operator's backend would show.
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
    #[cfg(target_os = "linux")]
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
            // Errors ignored on purpose: a loopback send into a full receive buffer still
            // *succeeds* (the packet is discarded later, in softirq, and charged to the receiving
            // socket's `sk_drops`), and any send that did fail simply isn't part of the overrun.
            let _ = sender.send_to(b"overrun", target).await;
        }
    }

    /// The gap `docs/known-gaps.md` used to record: a datagram the kernel discards before the read
    /// loop can return it is now counted and attributable, and the receive buffer's own gauges are
    /// reported alongside it by a listener that actually ran.
    ///
    /// **No sleeps, and no dependence on how fast the reader runs.** The overrun happens *before*
    /// anything reads the socket -- the listener is bound (so the socket, and its tiny buffer,
    /// exist) but not yet running -- and `shutdown` is already signalled when the run starts. The
    /// sampler's first read happens at the top of `read_loop_sampled`, before `read_loop` is
    /// polled even once, so the drop counter is observed at a point in time this test fully
    /// controls, and it is a *count*: every sample's delta adds to the total this asserts on.
    ///
    /// It is also the case `DropCounter`'s first-sample-is-absolute rule exists for: every one of
    /// these drops happened before the first sample, and a counter that treated that sample as a
    /// mere baseline would report zero here -- which is the answer `logit` gave before this work.
    ///
    /// **The *fill* gauges are asserted present here, not nonzero, and that is a consequence of
    /// the batched read rather than a weakening.** A gauge is last-write-wins per drain, so what
    /// this test can see is whatever the *final* sample read -- and one `recvmmsg` with
    /// `vlen = 64` empties a 16 KiB receive buffer (about 21 of these datagrams, at several
    /// hundred bytes of `skb->truesize` each) in a single syscall, so by the time the read loop
    /// exits the buffer legitimately reads empty. Before W4 the same run took 21 `recv_from` calls
    /// and usually lost the race to the already-signalled shutdown arm first.
    /// `a_full_receive_buffer_is_reported_as_used_bytes_and_a_utilization_ratio` below asserts the
    /// nonzero fill and the `used / granted` pairing directly against a sampler on a full socket,
    /// where it is deterministic.
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

        // Already signalled: `read_loop` stops almost immediately, but not before
        // `read_loop_sampled` has taken its first sample of a socket that is still full.
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
        // The pairing the metric's whole meaning depends on: both terms come from the same
        // `SO_MEMINFO` read, so the ratio is exactly the one the kernel itself tests. No upper
        // bound of 1.0 on it, deliberately -- the kernel admits a datagram whenever the
        // *already-charged* total is at or below the ceiling and then charges the whole of its
        // `truesize` on top, so a saturated queue settles at up to `rcvbuf + truesize` and reads
        // over 1.0 for as long as it stays there (observed at 1.17 against a real flood).
        // Asserting `<= 1.0` would be a flake waiting to happen. See
        // `SockMeminfo::receive_utilization`, which has both kernel generations' spelling of that
        // same admission rule.
        assert!(
            (utilization - used / granted).abs() < 1e-9,
            "utilization must be `used.bytes / receive_buffer.bytes`, not a ratio against the \
             requested size or against queued payload bytes"
        );
    }

    /// The nonzero half of the pair above, asserted where it is deterministic: a socket whose
    /// receive buffer is full *right now*, sampled once, with nothing in between that could have
    /// drained it. This is the reading an operator watching a listener under load actually sees,
    /// and the one a batched read makes hard to catch from the outside -- one `recvmmsg` empties a
    /// small buffer, so a test that lets the read loop run at all is racing it.
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

    /// This module's headline claim, checked against the number an operator would check it
    /// against: **`logit.input.kernel.drops` is the same counter `/proc/net/udp`'s `drops` column
    /// prints**, to the packet, on the same socket at the same moment.
    ///
    /// `logit_pipeline::sockstat`'s module doc asserts this identity ("`SK_MEMINFO_DROPS` is the
    /// same `sk->sk_drops` that procfs's `drops` column prints, so the two agree by
    /// construction") and `docs/plans/udp-intake.md` recorded checking it as a manual step. It is
    /// neither manual nor approximate: both sides read the same field --
    /// `sk_get_meminfo` (`net/core/sock.c`) does `mem[SK_MEMINFO_DROPS] = atomic_read(&sk->sk_drops)`
    /// and `udp4_format_sock` (`net/ipv4/udp.c`) prints `atomic_read(&sp->sk_drops)` as its last
    /// column, both verified at v6.12 -- so this is an equality, not a threshold.
    ///
    /// It is also the only test in the tree that pins the `SK_MEMINFO_DROPS` *index* against
    /// something other than itself. A mutant that read `SK_MEMINFO_BACKLOG` (7) or
    /// `SK_MEMINFO_OPTMEM` (6) instead still reports "some number" and still passes every
    /// nonzero-drops assertion elsewhere; only an equality against an independently-produced
    /// figure catches it.
    ///
    /// **Ordering.** The blast finishes before anything is read, and nothing else sends to this
    /// socket, so `sk_drops` is frozen by the time the comparison runs. The socket is still open
    /// throughout -- procfs only lists live sockets, and the row is found by the socket's own
    /// inode (`/proc/self/fd/<fd>` reads back as `socket:[<inode>]`), not by address, which is
    /// what makes it unambiguous even with another test's loopback socket bound nearby.
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

        // A third, independent reading: straight off the fd, bypassing the sampler entirely.
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
    /// unreadable or the row is not there (neither is something this code could be blamed for).
    ///
    /// The row layout is fixed by `udp4_format_sock`'s `seq_printf` (`net/ipv4/udp.c`, and
    /// `__ip6_dgram_sock_seq_show` in `net/ipv6/datagram.c` for udp6, which prints the same
    /// columns with wider addresses): whitespace-separated, `sl` is field 0, `inode` is field 9
    /// and `drops` is field 12 and last. `tx_queue:rx_queue` and `tr:tm->when` are each one
    /// colon-joined field, which is what makes the count come out at 13 rather than 15.
    #[cfg(target_os = "linux")]
    fn proc_net_udp_drops(socket: &UdpSocket) -> Option<u64> {
        use std::os::fd::AsRawFd;

        // `/proc/self/fd/<fd>` is a symlink that reads back as `socket:[<inode>]` -- the inode
        // procfs's socket tables key on. Cheaper and safer than an `fstat`, and no `unsafe`.
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

    /// The guarantee `read_loop_sampled` adds on top of its interval: drops that happen in the
    /// last fraction of a second before the reader stops are still reported.
    ///
    /// One guarantee holds the test up, and it is not the arm ordering. The future is polled
    /// exactly once up front -- taking the first sample, of a socket nothing has sent to yet, and
    /// parking `read_loop` -- and is then not polled *at all* while the overrun happens, because it
    /// is a plain local future rather than a spawned task. So no interval sample can occur during
    /// the blast, and every drop below is taken while the only sample that has ever run saw zero.
    ///
    /// On the resume after `shutdown` fires, [`sample_while`]'s `select!` is `biased` toward the
    /// *timer*, for the reason that function's doc gives -- so if the blast happened to outlast
    /// `KERNEL_SAMPLE_INTERVAL`, a due tick wins that poll, reports what it sees, and the final
    /// sample reports the remainder. Either way both samples are part of the same total, and the
    /// assertion is on the total. The final sample's guarantee is that nothing is left behind when
    /// the loop exits, not that it is the only sample to have run.
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
        // Depth 1 under `block`, with nothing popping: the reader takes one datagram and then
        // parks in `queue.push` for good -- the state this whole wrapper exists to keep sampling
        // through.
        let queue = test_queue(OverflowPolicy::Block, 1);
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

        // One poll: `select!` polls every arm on its first pass, so the wrapper's first
        // `sample_once` has definitely run by the time `yield_now` resolves.
        tokio::select! {
            _ = &mut sampled => panic!("the read loop must not finish before shutdown"),
            () = tokio::task::yield_now() => {}
        }
        assert_eq!(
            kernel_drops(&registry.drain(0)),
            0.0,
            "the premise: nothing has been sent yet, so the first sample saw no drops at all"
        );

        // Nothing polls `sampled` between here and the `await` below, so the reader is frozen and
        // every one of these datagrams arrives at a buffer that is not being drained.
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

    /// A sampler with nothing to read latches itself off on its very first call -- the state
    /// [`sample_while`] checks before arming its interval timer, and the reason a listener on a
    /// platform without these counters goes back to parking instead of waking once a second
    /// forever.
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
        sampler.sample_once(); // still a harmless no-op, which is what the final sample relies on
        assert!(!sampler.enabled);
    }

    /// The disabled path end to end: with no timer armed at all, [`sample_while`] is exactly its
    /// `read` future, and must still forward that future's result and leave the queue closed
    /// behind it -- the shape every non-Linux build runs, and one no Linux CI run would otherwise
    /// exercise.
    #[tokio::test]
    async fn a_disabled_sampler_still_reads_and_closes_the_queue() {
        let socket = bind_ephemeral().await;
        let queue = test_queue(OverflowPolicy::DropOldest, 10);
        // Already signalled, so `read_loop` returns on its first poll and this test needs no timer
        // of its own -- which is also what would hang it if a disabled sampler still armed one and
        // this assertion depended on the clock. It doesn't; the point is the result and the close.
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

    /// A future that never finishes and returns `Pending` **only** by exhausting its task's
    /// cooperative-scheduling budget, self-waking each time -- the exact shape `read_loop` takes
    /// under a flood, where there is always another datagram and it never parks for a real reason.
    /// `tokio::task::consume_budget` spends one unit per call and yields once the whole 128-unit
    /// budget is gone, which is all it takes to reproduce the condition.
    #[cfg(target_os = "linux")]
    async fn burns_its_whole_coop_budget_forever() -> anyhow::Result<()> {
        loop {
            tokio::task::consume_budget().await;
        }
    }

    /// The regression the `select!`'s arm ordering in [`sample_while`] exists to prevent: a read
    /// future that only ever yields on coop-budget exhaustion must not silence the sampler.
    ///
    /// **This test fails with the arms swapped back** (read first): the read arm spends all 128
    /// units, the timer arm is then polled with a budget of zero, `Sleep`'s own
    /// `coop::poll_proceed` returns `Pending` regardless of how far past its deadline it is, and
    /// the next wake repeats it forever -- so `ticks` below stays at 0 instead of reaching the
    /// interval's own cadence.
    ///
    /// Real time, not a paused clock: the task under test is never idle (it self-wakes
    /// continuously), so tokio's auto-advance would never engage. The sampler runs on its own
    /// spawned task for the same reason -- a `tokio::time::timeout` wrapped around this future in
    /// the test's own task would have its own `Sleep` starved by the very budget exhaustion under
    /// test, and would never fire.
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_sampler_keeps_ticking_while_the_read_future_burns_its_whole_coop_budget() {
        const INTERVAL: Duration = Duration::from_millis(10);
        const WINDOW: Duration = Duration::from_millis(60);
        const WINDOWS: usize = 6;

        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        // A real socket, so the sampler stays enabled and actually has counters to write.
        let socket = bind_ephemeral().await;
        let sampler = ReceiveBufferSampler::new(&socket, telemetry, Diagnostics::default());
        let sampling =
            tokio::spawn(sample_while(sampler, burns_its_whole_coop_budget_forever(), INTERVAL));

        // Let the loop's opening `sample_once` land and throw it away: that one runs before the
        // `select!` is ever reached, so it happens under either arm ordering and proves nothing.
        tokio::time::sleep(WINDOW).await;
        registry.drain(0);

        let mut ticks = 0;
        for _ in 0..WINDOWS {
            tokio::time::sleep(WINDOW).await;
            // A gauge is last-write-wins per drain, so its presence means "at least one sample ran
            // in this window" -- which is the question, not how many.
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

    /// `NET-01`'s P0 artifact: everything about [`build_headers`]/[`harvest_headers`] that a
    /// pointer-provenance checker can see, exercised with no syscall anywhere in reach.
    ///
    /// **Why this module exists at all.** `miri` has no shim for `recvmmsg(2)` and none for any
    /// socket call, so [`BatchReader::read_batch`] as a whole is permanently out of its reach
    /// (`docs/adr/out-of-ci-unsafe-verification.md`). Splitting the closure into build / syscall /
    /// harvest puts every one of the pointer decisions -- the `u64`-storage cast, the
    /// `size_of`-strided `add(i)`, the slot arithmetic, the re-initialization, and the read-back of
    /// two fields the kernel wrote through a pointer derived from the same borrow -- on the two
    /// sides `miri` *can* execute. `script/unsafe-check miri` runs exactly this module; the tests
    /// below are ordinary `cargo test` tests too, so a regression fails in CI even for someone who
    /// never runs the nightly harness.
    ///
    /// **The kernel's part is played by the tests themselves**, writing through a `*mut
    /// libc::mmsghdr` derived from the same `&mut [u64]` the syscall shim would derive its own
    /// from -- which is the aliasing question worth asking under Stacked/Tree Borrows, and the one
    /// production actually relies on.
    #[cfg(target_os = "linux")]
    mod batch_reader_helpers {
        use super::super::{build_headers, harvest_headers, HDR_WORDS, IOV_WORDS};
        use super::MAX_DATAGRAM_BYTES;

        /// Every `vlen` worth checking: both ends of the clamp [`BatchReader::new`] applies
        /// (`1` and [`MAX_READ_BATCH`]), the default `read_batch` of 64 and the value just below
        /// it (so an off-by-one in the loop bound shows up as a missing or extra header rather
        /// than as a boundary case that happens to line up), and `2` as the smallest `vlen` where
        /// slot disjointness means anything at all.
        const VLENS: [usize; 5] = [1, 2, 63, 64, 1024];

        /// The per-slot size the helper tests use where the *real* one would only make them slow:
        /// `1024 * MAX_DATAGRAM_BYTES` is a 67 MB zeroed slab, which is nothing to a release
        /// binary that `mmap`s it once and faults in a few pages, and a great deal to an
        /// interpreter tracking every byte's initialization state. Nothing in
        /// [`build_headers`] is sensitive to the *value*: it is a stride and a length, passed as a
        /// parameter precisely so a test can vary it. `slot_bytes_matching_production` below runs
        /// the same construction at the real `MAX_DATAGRAM_BYTES`, so the production stride is
        /// covered too, just not crossed with the largest `vlen` under `miri`.
        const SMALL_SLOT: usize = 64;

        /// One header's fields, read back out of the `u64` storage exactly the way the kernel
        /// would see them -- through a `*mut libc::mmsghdr` derived from `hdr_words`, following
        /// `msg_iov` into the separate `iovec` array rather than re-deriving that array's pointer
        /// from `iov_words`.
        ///
        /// Following the stored pointer, rather than recomputing where it *should* point, is the
        /// point: it is the one read that fails if the provenance `build_headers` hands the kernel
        /// has been invalidated by the time the kernel would use it. Which is also why nothing
        /// here may touch `iov_words` or `slots` again -- a reborrow of either would pop exactly
        /// the tag under test, in the test rather than in production.
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
        // narrower integers in some of its other target definitions -- which is the whole reason
        // this file never builds an `msghdr` by struct literal. The casts below are that
        // portability, not redundancy, even where clippy can see through them on *this* target.
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

        /// What `recvmmsg(2)` writes back into the first `n` headers, and nothing this code ever
        /// looks at: `msg_len` and `msg_hdr.msg_flags` are what [`harvest_headers`] reads, and
        /// `msg_namelen`/`msg_controllen` are written back by `____sys_recvmsg` (`net/socket.c`)
        /// on every call -- deliberately given nonsense values here so that a
        /// [`build_headers`] that failed to re-zero a header would be caught by the
        /// re-initialization test rather than silently agreeing.
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

        /// Fresh, correctly-sized backing buffers for one `vlen`, exactly as `BatchReader::new`
        /// allocates them.
        fn buffers(vlen: usize, slot_bytes: usize) -> (Vec<u8>, Vec<u64>, Vec<u64>) {
            (
                vec![0u8; vlen * slot_bytes],
                vec![0u64; vlen * IOV_WORDS],
                vec![0u64; vlen * HDR_WORDS],
            )
        }

        /// Asserts the whole of what a freshly-built header array must look like: a one-entry
        /// `iov` per header, pointing at that header's own slot and no other, every slot wholly
        /// inside the slab, every slot exactly `slot_bytes` long, and no address, control buffer
        /// or returned length carried over from anywhere.
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

        /// The same construction at the real per-slot size, so the production stride
        /// (`MAX_DATAGRAM_BYTES`, not [`SMALL_SLOT`]) is covered rather than only the parameter.
        ///
        /// `1024` is excluded under `miri` alone, and only for cost: a 67 MB zeroed slab is a
        /// single `mmap` to a real binary and per-byte bookkeeping to an interpreter. Nothing
        /// about the arithmetic differs between 64 slots and 1024 of them that
        /// [`every_header_describes_its_own_slot_and_asks_for_nothing_else`] does not already
        /// cover at 1024 with a smaller stride.
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

        /// The property the whole "rebuild the arrays on every call" design rests on: whatever the
        /// kernel left behind in a header is gone after the next [`build_headers`], so the
        /// writeback `____sys_recvmsg` performs on `msg_flags`/`msg_controllen` can never be read
        /// as if it belonged to the *next* call's datagram.
        #[test]
        fn a_rebuild_after_a_kernel_writeback_fully_reinitialises_every_header() {
            for vlen in VLENS {
                let (mut slots, mut iov_words, mut hdr_words) = buffers(vlen, SMALL_SLOT);
                let slab = (slots.as_ptr() as usize, slots.len());

                build_headers(&mut slots, SMALL_SLOT, &mut iov_words, &mut hdr_words, vlen);
                let lens: Vec<u32> = (0..vlen).map(|i| (i as u32) + 7).collect();
                let flags: Vec<i32> = (0..vlen).map(|_| libc::MSG_TRUNC).collect();
                play_kernel(&mut hdr_words, vlen, &lens, &flags);

                // Exactly what the closure does on its next `FnMut` call: same buffers, same
                // arguments, no clearing in between.
                build_headers(&mut slots, SMALL_SLOT, &mut iov_words, &mut hdr_words, vlen);

                let views = view(&mut hdr_words, vlen);
                assert_freshly_built(&views, slab, SMALL_SLOT);
            }
        }

        /// The harvest half: exactly the first `n` entries are written, with exactly the values
        /// the kernel put in the headers, and nothing past `n` is touched.
        #[test]
        fn harvest_copies_the_first_n_headers_and_nothing_past_them() {
            const UNTOUCHED_LEN: u32 = 0xA5A5_A5A5;
            const UNTOUCHED_FLAG: i32 = 0x5A5A_5A5A;

            for vlen in VLENS {
                // Every interesting `n` for this `vlen`: none at all, one, one short of the whole
                // batch, and the whole batch.
                for n in [0, 1, vlen.saturating_sub(1), vlen] {
                    let (mut slots, mut iov_words, mut hdr_words) = buffers(vlen, SMALL_SLOT);
                    build_headers(&mut slots, SMALL_SLOT, &mut iov_words, &mut hdr_words, vlen);

                    let written_lens: Vec<u32> =
                        (0..vlen).map(|i| ((i * 13) % SMALL_SLOT) as u32).collect();
                    // Alternating, so a harvest that read the wrong header's flags (or the same
                    // header's twice) cannot pass by accident.
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

        /// The full provenance chain, end to end and in production's own order: build the headers,
        /// then write into each slot *through the `iov_base` pointer the header carries* (which is
        /// what the kernel does, and the only thing in this file that depends on that pointer
        /// still being usable after [`build_headers`] has returned), harvest the lengths, and only
        /// then read the slab back through an ordinary borrow.
        ///
        /// Under Stacked/Tree Borrows this is the test that fails if the slab pointer's tag is
        /// invalidated between construction and use -- which is precisely why nothing between the
        /// `build_headers` call and the last write touches `slots` or `iov_words` at all. Reading
        /// `slots` afterwards is a fresh borrow and pops those tags, which is fine: by then the
        /// "kernel" is done.
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

        /// Both helpers refuse a buffer too small for the `vlen`/`n` they are handed, rather than
        /// writing or reading past it. Unreachable from [`BatchReader::read_batch`], whose buffers
        /// are sized once from the same `vlen`, and cheap enough (three comparisons per batch, not
        /// per datagram) to keep as the thing that makes these two functions safe to call at all.
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

    /// Every counter point named `name` in `events`, summed -- counts drain as deltas, so a total
    /// is what an operator's backend would show and what these tests assert on.
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

    /// Runs one listener over a real loopback socket at the given `read_batch`, sends `payloads`
    /// in order from a single sender socket, and returns what came out the other end -- the
    /// datagram bytes, in delivery order, plus the listener's own telemetry.
    ///
    /// **No sleeps anywhere, and no unbounded waits.** The driver waits for exactly
    /// `payloads.len()` deliveries and only then signals shutdown, so the test's synchronization is
    /// the data itself; each of those waits is wrapped in a `timeout` so a lost datagram is a
    /// failure with a count in it rather than a hung test process. `batch_max_events:
    /// 1` makes that one delivery per datagram, and a `Fanout` channel is FIFO, so the received
    /// sequence *is* the decode order.
    ///
    /// A single sender socket is what makes the ordering assertion meaningful: the kernel
    /// preserves the order of datagrams sent from one socket to one loopback peer. Two senders
    /// would have no such guarantee, and a test asserting one would be asserting a coincidence.
    async fn deliver_burst(
        read_batch: usize,
        payloads: &[Vec<u8>],
        registry: &logit_core::Registry,
    ) -> Vec<Vec<u8>> {
        let telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        let mut diag = Diagnostics::default();
        // A megabyte requested (the kernel doubles it, and `net.core.rmem_max` may clamp it):
        // enough that a burst sent before anything drains it cannot overrun the socket, which is
        // the one way this test could lose a datagram for a reason that is not a bug.
        let (socket, _group) =
            bind_socket("127.0.0.1:0", Some(1024 * 1024), &telemetry, &mut diag).await.unwrap();
        let addr = socket.local_addr().expect("a bound socket has an address");
        let queue = test_queue(OverflowPolicy::DropOldest, payloads.len() * 2);
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
                // Bounded, so a datagram lost anywhere (an `SO_RCVBUF` request the container's
                // `rmem_max` clamped below what this burst needs, most plausibly) fails the test
                // with a count instead of hanging the process forever waiting for it.
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
            read_loop(&socket, Arc::clone(&queue), telemetry.clone(), shutdown_rx, read_batch),
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

    /// [`payload`]'s byte-exact twin -- `String::from_utf8_lossy` would quietly rewrite any byte
    /// that isn't valid UTF-8, which is exactly what a byte-exactness test must not do.
    fn payload_bytes(event: &Event) -> Vec<u8> {
        match event.attributes.get("payload") {
            Some(logit_core::Value::Str(bytes)) => bytes.to_vec(),
            other => panic!("expected a payload attribute, got {other:?}"),
        }
    }

    fn numbered_payloads(count: usize) -> Vec<Vec<u8>> {
        (0..count).map(|i| format!("msg-{i}").into_bytes()).collect()
    }

    /// The headline property of the batched read: a burst several batches deep arrives complete
    /// and in the order it was sent. `recvmmsg` fills its `mmsghdr` array in arrival order and
    /// this loop must preserve that through the batch `Vec`, `push_many`, `pop_many` and the
    /// decode iteration -- four places an off-by-one or a reversed drain would show up.
    #[tokio::test]
    async fn a_two_hundred_datagram_burst_is_delivered_complete_and_in_order() {
        let payloads = numbered_payloads(200);
        let registry = logit_core::Registry::new();
        let received = deliver_burst(64, &payloads, &registry).await;
        assert_eq!(received, payloads, "every datagram, exactly once, in the order it was sent");
    }

    /// `read_batch: 1` is not a second code path -- it is `vlen = 1`, one datagram per syscall,
    /// which is what ADR `udp-intake-batching-and-socket-visibility` rejected a special case for.
    /// The proof it owes is this one: identical input, identical event stream, either way.
    #[tokio::test]
    async fn read_batch_one_and_sixty_four_yield_identical_event_streams() {
        let payloads = numbered_payloads(150);
        let registry = logit_core::Registry::new();
        let one = deliver_burst(1, &payloads, &registry).await;
        let sixty_four = deliver_burst(64, &payloads, &registry).await;
        assert_eq!(one, payloads, "read_batch: 1 must deliver the whole burst in order");
        assert_eq!(sixty_four, one, "the batch size must not be observable in the event stream");
    }

    /// Byte-exactness across the whole legal size range, in one batch: a zero-length datagram (a
    /// perfectly legal UDP payload, and what `recv_from` used to report as `n == 0`), ordinary
    /// small ones, and one near the 65,507-byte maximum. The large one is what proves each slot
    /// really is a whole datagram's worth -- a slab sized per *batch* rather than per *slot* would
    /// silently truncate here, which is the failure `MSG_TRUNC` would otherwise have to be
    /// handled for.
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

    /// `logit.input.reads` is the syscall count and `logit.input.datagrams` the datagram count, so
    /// `datagrams / reads` is the mean fill an operator reads the `read_batch` knob against. The
    /// invariant that makes it meaningful at all: a read returns at least one datagram, so reads
    /// can never exceed datagrams, and both have to add up exactly for a known burst.
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

    /// Two datagrams that arrive in one `recvmmsg` must not end up carrying the same
    /// `received_at`. Anything downstream keyed on (series, timestamp) -- `influxdb_out`'s line
    /// protocol is the live case -- treats two points sharing both as *one* point, and its own
    /// same-timestamp disambiguation (`allocate_timestamp`) is reset at the top of every
    /// `Encoder::encode`, so it cannot help across an output-batch boundary a read batch straddles.
    /// `base + i` is what keeps them distinct, for the cost of no extra clock read at all.
    ///
    /// **What is guaranteed, and what this assertion actually rests on.** *Within* one batch,
    /// strict increase holds by construction: every datagram is stamped `base + i` from one clock
    /// read, so the ordering cannot depend on the clock at all. *Across* batches it does not hold
    /// by construction, because `now_nanos()` is `SystemTime::now()` -- the **wall** clock, which
    /// is the right choice here (`received_at` is the event's wall-clock timestamp, and a
    /// monotonic instant could not be one) but which can step backwards. A `clock_settime`
    /// correction, chrony's `makestep`, or a VM suspend/restore between two batches can move it
    /// back by far more than the `<= read_batch` nanoseconds of offset the `+ i` adds, and two
    /// datagrams in consecutive batches could then share a `received_at` -- the very collision the
    /// offset exists to prevent. Tracked in `docs/known-gaps.md`; not closable from here.
    ///
    /// The assertion below is nonetheless not flaky in any way worth tightening. It would take a
    /// backwards step landing inside the sub-millisecond window this 200-datagram loopback burst
    /// occupies, on the machine running the test suite; a forward step cannot break it at all.
    /// Narrowing the assertion to within-batch windows would cost the cross-batch coverage --
    /// which is what caught the "one stamp per syscall" shape in the first place -- to insure
    /// against something that has never been observed here, so the claim is stated honestly rather
    /// than the test weakened.
    #[tokio::test]
    async fn every_datagram_in_a_batch_gets_its_own_received_at() {
        const BURST: usize = 200;

        let telemetry = Telemetry::default();
        let mut diag = Diagnostics::default();
        let (socket, _group) =
            bind_socket("127.0.0.1:0", Some(1024 * 1024), &telemetry, &mut diag).await.unwrap();
        let addr = socket.local_addr().expect("a bound socket has an address");
        let queue = test_queue(OverflowPolicy::DropOldest, BURST * 2);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let sender = bind_ephemeral().await;
        for i in 0..BURST {
            sender.send_to(format!("msg-{i}").as_bytes(), addr).await.expect("loopback send");
        }

        // Read until the queue holds the whole burst, then stop -- the queue's own depth is the
        // synchronization, so there is nothing to sleep on.
        let read = read_loop(&socket, Arc::clone(&queue), telemetry, shutdown_rx, 64);
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

    /// The one case a 65,507-byte slot cannot hold: an IPv6 datagram may carry up to 65,527 bytes,
    /// so the last 20 are copied nowhere. The bytes were lost the same way before this work -- the
    /// `recv_from` loop had a 65,507-byte buffer too -- what is new is that the loss is *counted*
    /// rather than silent, from `MSG_TRUNC` in the header the reader already reads `msg_len` from.
    ///
    /// **Skips rather than fails where IPv6 loopback isn't usable.** A container with no `::1`, or
    /// one whose loopback MTU won't carry a fragmented 65 KB datagram, says nothing about this code;
    /// the assertion below is only meaningful once the datagram has actually arrived.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn an_oversized_ipv6_datagram_is_delivered_truncated_and_counted() {
        /// 20 bytes past what a slot holds -- the largest payload IPv6 permits.
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

        let queue = test_queue(OverflowPolicy::DropOldest, 8);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let read = read_loop(&socket, Arc::clone(&queue), telemetry, shutdown_rx, 64);
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

    /// The fatal-error path, which before this test nothing covered at all: `read_loop`'s
    /// `Err` break, its `queue.close()` on the way out, and the message an operator is left with.
    ///
    /// **A real, deterministic, unprivileged non-`EAGAIN` error, with no fault injection.** That
    /// is harder than it sounds: every errno an unconnected UDP socket can produce on the receive
    /// path is either unreachable (`bind_one`'s doc has the kernel citations) or needs privilege
    /// (`ss -K`, i.e. `SOCK_DESTROY` → `ECONNABORTED`) or `strace -e inject=`. What does work is a
    /// descriptor that is *readable but is not a socket*: a pipe whose write end has a byte in it
    /// is immediately `EPOLLIN`, so `async_io` hands control to the closure on the first poll, and
    /// `recvmmsg(2)` on it returns `ENOTSOCK` -- a genuine kernel error, first call, every time.
    ///
    /// This is also why the message must name the syscall rather than lean on `io::Error`'s own
    /// text. Without [`describe_read_failure`] the whole report is `component 'statsd_in': Socket
    /// operation on non-socket (os error 88)` -- and for the case that actually happens in the
    /// field, a sandbox refusing the syscall, `Function not implemented (os error 38)`.
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

        // One byte, so the read end is readable and `async_io` proceeds straight to the closure
        // instead of parking on a readiness that would never arrive.
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

        let queue = test_queue(OverflowPolicy::DropOldest, 10);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let err = tokio::time::timeout(
            Duration::from_secs(5),
            read_loop(&socket, Arc::clone(&queue), Telemetry::default(), shutdown_rx, 64),
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

    /// The seccomp case, at the seam rather than through a sandbox: `ENOSYS`/`EPERM` is what
    /// quinn#1947 and bun#42678 both hit, and the default message for it (`Function not
    /// implemented (os error 38)`) tells an operator nothing at all. Forcing the real syscall to
    /// return it needs `strace -e inject=recvmmsg:error=ENOSYS:when=1` (`script/unsafe-check
    /// inject`, which is out of CI by design); what belongs *in* CI is that the mapping from that
    /// errno to a message worth reading does not quietly disappear.
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

        // Every other errno gets the syscall and the address and nothing invented on top.
        let plain = describe_read_failure(&socket, std::io::Error::from_raw_os_error(libc::EBADF))
            .to_string();
        assert!(plain.contains(READ_SYSCALL) && plain.contains(&addr.to_string()));
        assert!(
            !plain.contains("seccomp"),
            "an unrelated errno must not be guessed at, got: {plain}"
        );
    }

    /// Shutdown landing while the read half is parked inside a blocked `push_many` -- the one
    /// place ADR `udp-intake-batching-and-socket-visibility` widens an accepted loss, from the
    /// single datagram `push` held to at most `read_batch`. What must still hold is everything
    /// around it: the loop exits promptly rather than waiting for room that will never come, and
    /// it closes the queue on the way out so `decode_loop` can finish.
    ///
    /// Deterministic without a sleep. The queue is `Block` with a bound of 4 and is pre-filled to
    /// 3, so the read half can accept exactly one datagram of whatever batch it reads and must
    /// then park; the loop below polls it until `logit.input.reads` proves a batch has actually
    /// been read (bounded, and asserted afterwards, so a regression fails rather than hangs).
    #[tokio::test]
    async fn shutdown_while_a_batch_is_mid_push_exits_promptly_and_closes_the_queue() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        let mut diag = Diagnostics::default();
        let (socket, _group) =
            bind_socket("127.0.0.1:0", Some(1024 * 1024), &telemetry, &mut diag).await.unwrap();
        let addr = socket.local_addr().expect("a bound socket has an address");
        let queue = test_queue(OverflowPolicy::Block, 4);
        for i in 0..3u32 {
            queue.push(Datagram { bytes: Bytes::from(format!("pre-{i}")), received_at: 0 }).await;
        }

        // More than one batch's worth, so the read half is certainly holding a remainder it can
        // never place once the fourth slot is taken.
        let sender = bind_ephemeral().await;
        for i in 0..200u32 {
            sender.send_to(format!("msg-{i}").as_bytes(), addr).await.expect("loopback send");
        }

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let read = read_loop(&socket, Arc::clone(&queue), telemetry.clone(), shutdown_rx, 64);
        tokio::pin!(read);

        let mut reads = 0.0;
        for _ in 0..10_000 {
            tokio::select! {
                _ = &mut read => panic!("the read loop must not finish before shutdown"),
                () = tokio::task::yield_now() => {}
            }
            reads += counter(&registry.drain(0), "logit.input.reads");
            if reads > 0.0 {
                break;
            }
        }
        assert!(reads > 0.0, "the read half never got a batch off the socket -- test premise");

        shutdown_tx.send(true).expect("receiver should still be alive");
        tokio::time::timeout(Duration::from_secs(5), read)
            .await
            .expect("a blocked push_many must be cancelled by shutdown, not waited out")
            .expect("should shut down without error");

        let mut drained = 0;
        while queue.pop().await.is_some() {
            drained += 1;
        }
        assert_eq!(
            drained, 4,
            "the three pre-filled datagrams plus the one the read half managed to place -- the \
             rest of its batch is the bounded, uncounted shutdown loss the ADR names"
        );
        assert!(
            queue.pop().await.is_none(),
            "the queue must be closed on the way out -- that is what lets decode_loop finish"
        );
    }

    /// The whole listener, end to end, in the configuration where a batched read is hardest on the
    /// queue underneath it: `overflow: block` with a `max_datagrams` *smaller* than `read_batch`,
    /// so a single `push_many` call cannot fit even against a completely empty queue, and
    /// `batch_flush_interval: 0s`, so `decode_loop` waits on `pop_many` with no timer to rescue it.
    ///
    /// Every datagram must still be delivered, and the run must finish. A `push_many` that
    /// notified `not_empty` only after its whole batch landed would deadlock exactly here -- the
    /// reader parked waiting for room, the decoder parked waiting for an item, and the item that
    /// would have woken it already sitting in the queue. That combination is legal configuration
    /// (graph rule 57's own comment says why it is not rejected), so it is pinned end to end
    /// rather than left to the queue's own unit tests.
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
}
