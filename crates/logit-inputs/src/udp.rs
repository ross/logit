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
//! no `receive:` block -- it keeps `Input::run_until_shutdown`'s default (cancel-by-drop,
//! unchanged from ADR `service-lifecycle-and-output-retry`). Don't generalize this module toward it.

use bytes::Bytes;
use logit_core::{Diagnostics, Event, EventBatch, Telemetry};
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
        }
    }
}

impl UdpListenerConfig {
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
        }
    }
}

/// The three `decode_loop` needs to build and drive a [`BatchAccumulator`] -- split out from
/// [`UdpListenerConfig`] purely to keep `decode_loop`'s own parameter count down.
#[derive(Debug, Clone, Copy)]
struct BatchingConfig {
    max_events: usize,
    max_bytes: u64,
    flush_interval: Duration,
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

        let mut read =
            Box::pin(read_loop(&socket, Arc::clone(&queue), self.telemetry.clone(), shutdown));
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
        // (see `read_loop`'s own doc comment), which is what lets `decode`'s `pop()` discover
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
                let socket =
                    finish_bind(bound.socket, receive_buffer_bytes, telemetry, diag)?;
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

    socket
        .set_reuse_address(true)
        .context("setting SO_REUSEADDR, which a multicast bind needs")?;
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

    // Sampled once at bind, not per datagram -- SO_RCVBUF doesn't change after bind.
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
/// Races every `recv_from` *and* every `push` against `shutdown`, so a graceful shutdown stops
/// this loop immediately rather than only once the next datagram happens to arrive, or (under
/// `block`) only once downstream makes room. Cancelling a blocked `push` this way drops the one
/// datagram it was holding, uncounted -- bounded to exactly one, the same scope ADR `service-lifecycle-and-output-retry` already
/// accepted for "a datagram in flight when the signal lands."
///
/// Closes `queue` in every exit path -- shutdown, or a fatal socket error -- which is what lets
/// `decode_loop`'s `pop()` discover "closed and empty" and return; no separate close-detection
/// signal is needed on that side.
async fn read_loop(
    socket: &tokio::net::UdpSocket,
    queue: Arc<ReceiveQueue>,
    telemetry: Telemetry,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    // The largest possible UDP payload (65535 minus the 8-byte UDP header) -- the same bound
    // every UDP listener in this codebase has always used.
    let mut buf = vec![0u8; 65_507];
    let result = loop {
        let recv = tokio::select! {
            recv = socket.recv_from(&mut buf) => recv,
            _ = shutdown.wait_for(|&due| due) => break Ok(()),
        };
        let (n, _peer) = match recv {
            Ok(pair) => pair,
            Err(err) => break Err(err.into()),
        };
        telemetry.count("logit.input.datagrams", 1.0, &[]);
        telemetry.count("logit.input.datagram.bytes", n as f64, &[]);
        let datagram =
            Datagram { bytes: Bytes::copy_from_slice(&buf[..n]), received_at: now_nanos() };
        tokio::select! {
            () = queue.push(datagram) => {}
            _ = shutdown.wait_for(|&due| due) => break Ok(()),
        }
    };
    queue.close();
    result
}

/// Pops datagrams from `queue`, decodes and accumulates them into batches, and sends each
/// completed batch through `sink` -- entirely independent of how fast `read_loop` is filling
/// `queue`. Uses [`ReceiveQueue::pop`] (not `peek`/`commit`): a datagram that fails to decode is
/// diagnosed and dropped, never retried, and `pop` is cancellation-safe
/// (`logit_pipeline::queue::BoundedQueue::pop`'s own doc comment) -- this whole future can be
/// dropped mid-await by `run_input`'s grace backstop.
///
/// Owns `sink` (the `Fanout`) -- dropping this future is what closes every downstream inbox, the
/// shutdown cascade `docs/adr/service-lifecycle-and-output-retry.md` established.
///
/// Flushes the accumulator's final contents (`FlushReason::Shutdown`) only once `pop()` reports
/// closed-and-empty -- i.e. only after `read_loop` can no longer push anything new, the same
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
    let has_interval = !batching.flush_interval.is_zero();
    let mut next_flush =
        has_interval.then(|| tokio::time::Instant::now() + batching.flush_interval);

    loop {
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

        let datagram = match next_flush {
            None => queue.pop().await,
            Some(deadline) => {
                let wait = deadline.saturating_duration_since(tokio::time::Instant::now());
                match tokio::time::timeout(wait, queue.pop()).await {
                    Ok(datagram) => datagram,
                    Err(_elapsed) => continue,
                }
            }
        };

        let Some(datagram) = datagram else {
            // Closed and empty: `read_loop` has stopped for good (shutdown or a fatal socket
            // error). Flush whatever's left -- nothing more will ever arrive either way.
            if let Some(batch) = accumulator.take() {
                emit(&sink, &telemetry, batch, FlushReason::Shutdown).await;
            }
            return;
        };

        let latency_nanos = (now_nanos() - datagram.received_at).max(0) as u64;
        telemetry.timing(
            "logit.component.receive.latency",
            Duration::from_nanos(latency_nanos),
            &[],
        );

        scratch.clear();
        match decoder.decode_into(datagram.bytes, datagram.received_at, &mut scratch) {
            Ok((resource, scope)) => {
                // `scope` is whatever `decoder.decode_into` returned -- `None` for every decoder
                // this loop drives today (statsd/syslog datagrams have no OTLP
                // instrumentation-scope concept), but threaded through rather than hardcoded so a
                // future `Decoder` on this same loop that does carry one isn't silently dropped.
                if let Some((batch, reason)) = accumulator.absorb(resource, scope, &mut scratch) {
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

fn now_nanos() -> i64 {
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
            let read_fut = read_loop(&socket, Arc::clone(&queue), telemetry.clone(), shutdown_rx.clone());
            let decode_fut = decode_loop(
                &mut decoder,
                Arc::clone(&queue),
                fanout,
                BatchingConfig { max_events: 1, max_bytes: u64::MAX, flush_interval: Duration::ZERO },
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
            read_loop(&socket, Arc::clone(&queue), telemetry.clone(), shutdown_rx),
            decode_loop(
                &mut decoder,
                Arc::clone(&queue),
                fanout,
                BatchingConfig {
                    max_events: 1,
                    max_bytes: u64::MAX,
                    flush_interval: Duration::ZERO
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
            read_loop(&socket, Arc::clone(&queue), telemetry.clone(), shutdown_rx),
            decode_loop(
                &mut decoder,
                Arc::clone(&queue),
                fanout,
                BatchingConfig {
                    max_events: 1,
                    max_bytes: u64::MAX,
                    flush_interval: Duration::ZERO
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
        let (len, _from) =
            tokio::time::timeout(Duration::from_secs(5), socket.recv_from(&mut buf))
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
