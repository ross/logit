//! `internal`: `logit` observing itself. On each `interval` it drains every component's
//! per-component self-telemetry buffer ([`logit_core::telemetry`]) and sends the result as one
//! ordinary batch, so any downstream component works on it (`docs/design/internal-telemetry.md`,
//! `docs/adr/internal-telemetry-as-pipeline-events.md`). Graph rule 13 allows at most one
//! `internal`: a second would split the one process-wide buffer set between them.
//!
//! A drain yields three kinds of event: metric points (timestamped at the drain), spans (at their
//! own start), and captured logs (at capture). Logs arrive only when `logs:` isn't `off`:
//! `logit-cli` activates `logit_core::TelemetryLayer` at that threshold (`warn`, the default, or
//! `error`).
//!
//! `interval` is also the sampling tick for the `logit.process.*` metrics (resident memory, CPU
//! time, threads, open files, interner size, uptime), which no occurrence would ever push. They
//! ride on this component's own `Telemetry` handle; see `ProcessSampler` and
//! `docs/design/internal-telemetry.md`'s "Process-level metrics".

use crate::procstat::{self, CpuTicks, Status, Unavailable};
use crate::Input;
use logit_core::{
    interner, AttrMap, Diagnostics, EventBatch, Registry, Resource, Scope, Telemetry,
};
use logit_pipeline::Fanout;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;

pub struct InternalInput {
    interval: Duration,
    registry: Arc<Registry>,
    /// `service.name = logit`, built once and `Arc`-shared by every batch. `internal` is the one
    /// input that stamps it: its data is `logit`'s own, where another listener's belongs to
    /// other services.
    resource: Arc<Resource>,
    /// `{ name: "logit", version: CARGO_PKG_VERSION }`, built once and `Arc`-shared like
    /// `resource`. Downstream identifies `logit`'s own points, spans, and logs by it; no other
    /// input stamps it, and no codec invents it.
    scope: Arc<Scope>,
    telemetry: Telemetry,
    diag: Diagnostics,
    process: ProcessSampler,
}

impl InternalInput {
    pub fn new(interval: Duration, registry: Arc<Registry>) -> Self {
        let mut attributes = AttrMap::new();
        attributes.insert("service.name", "logit");
        Self {
            interval,
            registry,
            resource: Arc::new(Resource { attributes, ..Default::default() }),
            scope: Arc::new(Scope {
                name: bytes::Bytes::from_static(b"logit"),
                version: bytes::Bytes::from_static(env!("CARGO_PKG_VERSION").as_bytes()),
                ..Default::default()
            }),
            telemetry: Telemetry::default(),
            diag: Diagnostics::default(),
            process: ProcessSampler::new(),
        }
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    /// This component's own handle, registered in the `Registry` it drains, so its
    /// `logit.process.*`/`logit.internal.*` points ride along in the next drain. Without one, no
    /// process-level metric is sampled and no `/proc` file is read.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// The allocator's live heap size in bytes, gauged as `logit.process.memory.allocated.bytes`
    /// on every tick it returns `Some`. A hook because this crate can't see the global allocator.
    pub fn with_heap_stats(mut self, f: fn() -> Option<u64>) -> Self {
        self.process.heap = Some(f);
        self
    }
}

#[async_trait::async_trait]
impl Input for InternalInput {
    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        // The runtime's `run_input` always calls `run_until_shutdown`; this shares its loop
        // through a never-firing `watch`. Keep `_tx` bound: a dropped sender makes `wait_for`
        // resolve at once, turning `run` into "drain once, then exit".
        let (_tx, rx) = watch::channel(false);
        self.run_until_shutdown(sink, rx).await
    }

    /// The drain loop, plus **one final drain when `shutdown` fires**, which is why this input
    /// overrides the trait's default (cancel-by-drop).
    ///
    /// **Why.** Up to one whole `interval` of self-telemetry is always buffered, and
    /// cancel-by-drop would discard it: a 25s run at the default 10s `interval` would report two
    /// intervals and lose the third. `logit-perf`'s attribution mode (`docs/design/performance.md`)
    /// SIGTERMs a short-lived process and reads these points back, so it needs that last slice.
    ///
    /// **Why the final batch is delivered.** Downstream inboxes stay open while this future
    /// holds `sink`, so no downstream close-time flush starts until this returns
    /// (`run_with_telemetry`'s shutdown cascade). `run_input` bounds the wait by
    /// `InputRuntimeConfig::shutdown_grace`, which for `internal` is always
    /// `ReceiveConfig::default()`'s 5s (graph rule 17 rejects a `receive:` block on it). One drain
    /// and one `Fanout::send` fit well inside that; the grace cuts short a send blocked on
    /// downstream backpressure, its only unbounded wait.
    ///
    /// **Residual.** The final drain's own `logit.internal.{points,spans,logs}.emitted` and
    /// `logit.internal.drain.duration` are recorded after it, so with no tick left they're never
    /// emitted: the last instance of "a drain can't count itself" (see `tick`).
    async fn run_until_shutdown(
        &mut self,
        sink: Fanout,
        mut shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        let started = Instant::now();
        let mut ticker = tokio::time::interval(self.interval);
        // Skip `interval`'s immediate first tick, so the first drain is one interval in.
        ticker.tick().await;
        loop {
            // Both arm bodies await a send: see `docs/design/pipeline-graph.md`'s
            // "Cancellation points".
            tokio::select! {
                _ = ticker.tick() => self.tick(started, &sink).await,
                () = shutdown_due(&mut shutdown) => {
                    self.tick(started, &sink).await;
                    return Ok(());
                }
            }
        }
    }
}

impl InternalInput {
    async fn tick(&mut self, started: Instant, sink: &Fanout) {
        // Sampled here because nothing else has an occasion to push them. `interner.strings` is
        // the process-wide interner's size, which never shrinks (`interner::len`).
        self.telemetry.gauge("logit.process.interner.strings", interner::len() as f64, &[]);
        self.telemetry.gauge("logit.process.uptime", started.elapsed().as_secs_f64(), &[]);
        self.process.sample(&self.telemetry, &self.diag);

        let drain_timer = self.telemetry.timer("logit.internal.drain.duration");
        let events = self.registry.drain(now_nanos());
        drop(drain_timer);
        if events.is_empty() {
            return;
        }
        // These counts are recorded after the drain that produced them, so they ride in the next
        // drain, one tick behind: a drain can't include a count of itself.
        //
        // Counted per kind, since one drain mixes points, spans, and logs. `event.log` is
        // checked first: a log event carries neither `metrics` nor `span`, so it would otherwise
        // fall through as a point.
        let (points_emitted, spans_emitted, logs_emitted) =
            events.iter().fold((0u64, 0u64, 0u64), |(points, spans, logs), event| {
                if event.log.is_some() {
                    (points, spans, logs + 1)
                } else if event.span.is_some() {
                    (points, spans + 1, logs)
                } else {
                    (points + 1, spans, logs)
                }
            });
        if points_emitted > 0 {
            self.telemetry.count("logit.internal.points.emitted", points_emitted as f64, &[]);
        }
        if spans_emitted > 0 {
            self.telemetry.count("logit.internal.spans.emitted", spans_emitted as f64, &[]);
        }
        if logs_emitted > 0 {
            self.telemetry.count("logit.internal.logs.emitted", logs_emitted as f64, &[]);
        }

        sink.send(EventBatch {
            resource: self.resource.clone(),
            scope: Some(self.scope.clone()),
            events,
        })
        .await;
    }
}

/// A `/proc/self` reader; a field of [`ProcessSampler`] so a test can inject one.
type Reader<T> = fn() -> Result<T, Unavailable>;

/// Samples the procfs- and allocator-backed `logit.process.*` metrics once per `internal` tick
/// (`docs/adr/process-level-metrics.md`).
///
/// Each of the four procfs sources latches off on its own first failure, with one diagnostic, as
/// `crate::tcp::AcceptQueueSampler` does: a sandbox that hides one file must not silence the
/// others. The heap hook is independent of all four.
struct ProcessSampler {
    read_status: Reader<Status>,
    read_cpu: Reader<CpuTicks>,
    read_fds: Reader<u64>,
    read_fds_limit: Reader<Option<u64>>,
    status_enabled: bool,
    cpu_enabled: bool,
    fds_enabled: bool,
    fds_limit_enabled: bool,
    /// The previous `cpu_ticks` reading. Zero at start, so the first delta is CPU time since
    /// process start and a cumulative total downstream equals the kernel's counter.
    last_cpu: CpuTicks,
    heap: Option<fn() -> Option<u64>>,
}

impl ProcessSampler {
    fn new() -> Self {
        Self {
            read_status: procstat::status,
            read_cpu: procstat::cpu_ticks,
            read_fds: procstat::open_fds,
            read_fds_limit: procstat::open_files_limit,
            status_enabled: true,
            cpu_enabled: true,
            fds_enabled: true,
            fds_limit_enabled: true,
            last_cpu: CpuTicks::default(),
            heap: None,
        }
    }

    /// Gauges are re-emitted every tick because a drain takes the point map. The CPU counter is
    /// emitted every tick too, a zero delta included: a cumulative `aggregate` evicts a series
    /// idle for `series_retention` windows, and the restarted total reads downstream as a counter
    /// reset.
    fn sample(&mut self, telemetry: &Telemetry, diag: &Diagnostics) {
        if !telemetry.is_enabled() {
            return;
        }
        if self.status_enabled {
            match (self.read_status)() {
                Ok(status) => {
                    telemetry.gauge(
                        "logit.process.memory.resident.bytes",
                        status.resident_bytes as f64,
                        &[],
                    );
                    telemetry.gauge("logit.process.threads", status.threads as f64, &[]);
                }
                Err(err) => {
                    self.status_enabled = false;
                    report(
                        diag,
                        &err,
                        "/proc/self/status",
                        "logit.process.memory.resident.bytes and logit.process.threads",
                    );
                }
            }
        }
        if self.cpu_enabled {
            match (self.read_cpu)() {
                Ok(now) => {
                    let hz = procstat::USER_HZ as f64;
                    let user = now.user.saturating_sub(self.last_cpu.user) as f64 / hz;
                    let system = now.system.saturating_sub(self.last_cpu.system) as f64 / hz;
                    self.last_cpu = now;
                    telemetry.count("logit.process.cpu.seconds", user, &[("mode", "user")]);
                    telemetry.count("logit.process.cpu.seconds", system, &[("mode", "system")]);
                }
                Err(err) => {
                    self.cpu_enabled = false;
                    report(diag, &err, "/proc/self/stat", "logit.process.cpu.seconds");
                }
            }
        }
        if self.fds_enabled {
            match (self.read_fds)() {
                Ok(fds) => telemetry.gauge("logit.process.fds", fds as f64, &[]),
                Err(err) => {
                    self.fds_enabled = false;
                    report(diag, &err, "/proc/self/fd", "logit.process.fds");
                }
            }
        }
        if self.fds_limit_enabled {
            match (self.read_fds_limit)() {
                Ok(Some(limit)) => telemetry.gauge("logit.process.fds.limit", limit as f64, &[]),
                Ok(None) => {}
                Err(err) => {
                    self.fds_limit_enabled = false;
                    report(diag, &err, "/proc/self/limits", "logit.process.fds.limit");
                }
            }
        }
        if let Some(allocated) = self.heap.and_then(|heap| heap()) {
            telemetry.gauge("logit.process.memory.allocated.bytes", allocated as f64, &[]);
        }
    }
}

/// The one diagnostic a source gets when it latches off: `debug` on a non-Linux build, where the
/// absence is expected, and `warn` for a failed read on Linux.
fn report(diag: &Diagnostics, err: &Unavailable, source: &str, metrics: &str) {
    let message = format_args!("{source} is not available: {err}; {metrics} will not be reported");
    if matches!(err, Unavailable::NotLinux) {
        diag.debug(message);
    } else {
        diag.warn(message);
    }
}

/// Resolves once `shutdown` holds `true`, yielding nothing.
///
/// Exists to keep the `select!` above `Send`, as `#[async_trait]` requires: `wait_for` resolves to
/// a `watch::Ref` holding a non-`Send` read guard, and `select!` keeps a branch's value alive
/// across its handler, which here awaits a drain. Returning `()` drops the guard first.
///
/// The `Result` is discarded: `Err` means the sender dropped, which happens only in the same
/// teardown, and "drain once more, then stop" is right either way.
async fn shutdown_due(shutdown: &mut watch::Receiver<bool>) {
    let _ = shutdown.wait_for(|&due| due).await;
}

fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::MetricKind;
    use logit_pipeline::test_util::Totals;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn a_tick_with_nothing_buffered_sends_nothing() {
        let registry = Registry::new();
        let mut input = InternalInput::new(Duration::from_millis(1), registry);
        let (tx, mut rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx]);

        input.tick(Instant::now(), &fanout).await;

        assert!(rx.try_recv().is_err(), "an empty drain should send no batch");
    }

    #[tokio::test]
    async fn a_tick_drains_a_registered_components_buffered_points_into_one_batch() {
        let registry = Registry::new();
        let component_telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        component_telemetry.count("logit.input.datagrams", 1.0, &[]);

        let mut input = InternalInput::new(Duration::from_millis(1), registry);
        let (tx, mut rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx]);

        input.tick(Instant::now(), &fanout).await;

        let delivered = rx.try_recv().expect("should have sent a batch");
        let batch = match delivered {
            logit_pipeline::Delivered::Owned(batch, _ctx) => batch,
            logit_pipeline::Delivered::Shared(shared, _ctx) => (*shared).clone(),
        };
        assert_eq!(batch.events.len(), 1);
        assert_eq!(
            batch.events[0].attributes.get("component").and_then(|v| v.as_str()),
            Some("statsd_in")
        );
    }

    #[tokio::test]
    async fn a_tick_samples_its_own_process_level_gauges() {
        let registry = Registry::new();
        let own_telemetry = registry.telemetry_for("self", "internal", "listener");
        let mut input =
            InternalInput::new(Duration::from_millis(1), registry).with_telemetry(own_telemetry);
        let (tx, mut rx) = mpsc::channel(2);
        let fanout = Fanout::new(vec![tx]);

        // Sampled just before the drain, so they appear in this same tick's batch.
        input.tick(Instant::now(), &fanout).await;

        let delivered = rx.try_recv().expect("should have sent a batch");
        let batch = match delivered {
            logit_pipeline::Delivered::Owned(batch, _ctx) => batch,
            logit_pipeline::Delivered::Shared(shared, _ctx) => (*shared).clone(),
        };
        let names: Vec<&str> = batch
            .events
            .iter()
            .flat_map(|e| e.metrics.iter().map(|m| logit_core::interner::resolve(m.name)))
            .collect();
        assert!(names.contains(&"logit.process.interner.strings"));
        assert!(names.contains(&"logit.process.uptime"));
        #[cfg(target_os = "linux")]
        {
            let totals = Totals::of(batch.events.clone());
            for name in [
                "logit.process.memory.resident.bytes",
                "logit.process.threads",
                "logit.process.fds",
            ] {
                assert!(totals.gauge(name, &[]).is_some_and(|v| v > 0.0), "{name}");
            }
            assert!(totals.has("logit.process.cpu.seconds", &[("mode", "user")]));
            assert!(totals.has("logit.process.cpu.seconds", &[("mode", "system")]));
            let limit = procstat::open_files_limit().expect("limits should read on Linux");
            assert_eq!(totals.gauge("logit.process.fds.limit", &[]).is_some(), limit.is_some());
        }
        let _ = rx.try_recv(); // drain any second batch, unasserted
    }

    fn fixed_status() -> Result<Status, Unavailable> {
        Ok(Status { resident_bytes: 4096, threads: 3 })
    }

    fn fixed_cpu() -> Result<CpuTicks, Unavailable> {
        Ok(CpuTicks::default())
    }

    fn fixed_fds() -> Result<u64, Unavailable> {
        Ok(10)
    }

    fn fixed_fds_limit() -> Result<Option<u64>, Unavailable> {
        Ok(Some(1024))
    }

    /// A sampler over fixed readers, so a test's values don't depend on the host's `/proc`.
    fn fixed_sampler() -> ProcessSampler {
        ProcessSampler {
            read_status: fixed_status,
            read_cpu: fixed_cpu,
            read_fds: fixed_fds,
            read_fds_limit: fixed_fds_limit,
            ..ProcessSampler::new()
        }
    }

    #[test]
    fn cpu_seconds_are_the_delta_between_ticks_and_a_zero_delta_is_still_emitted() {
        static CALLS: AtomicUsize = AtomicUsize::new(0);
        fn two_readings() -> Result<CpuTicks, Unavailable> {
            Ok(match CALLS.fetch_add(1, Ordering::Relaxed) {
                0 => CpuTicks { user: 150, system: 20 },
                _ => CpuTicks { user: 170, system: 20 },
            })
        }
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("self", "internal", "listener");
        let diag = Diagnostics::default();
        let mut sampler = ProcessSampler { read_cpu: two_readings, ..fixed_sampler() };

        sampler.sample(&telemetry, &diag);
        let first = Totals::of(registry.drain(0));
        assert_eq!(first.sum("logit.process.cpu.seconds", &[("mode", "user")]), 1.5);
        assert_eq!(first.sum("logit.process.cpu.seconds", &[("mode", "system")]), 0.2);

        sampler.sample(&telemetry, &diag);
        let second = Totals::of(registry.drain(0));
        assert!((second.sum("logit.process.cpu.seconds", &[("mode", "user")]) - 0.2).abs() < 1e-9);
        assert!(
            second.has("logit.process.cpu.seconds", &[("mode", "system")]),
            "an idle mode still emits its zero delta"
        );
        assert_eq!(second.sum("logit.process.cpu.seconds", &[("mode", "system")]), 0.0);
    }

    #[test]
    fn a_failing_source_latches_off_alone_and_is_not_read_again() {
        static CALLS: AtomicUsize = AtomicUsize::new(0);
        fn failing_fds() -> Result<u64, Unavailable> {
            CALLS.fetch_add(1, Ordering::Relaxed);
            Err(Unavailable::Io(std::io::Error::from(std::io::ErrorKind::PermissionDenied)))
        }
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("self", "internal", "listener");
        let diag = Diagnostics::default();
        let mut sampler =
            ProcessSampler { read_fds: failing_fds, heap: Some(|| Some(7)), ..fixed_sampler() };

        for tick in 0..2 {
            sampler.sample(&telemetry, &diag);
            let totals = Totals::of(registry.drain(0));
            assert_eq!(totals.gauge("logit.process.fds", &[]), None, "tick {tick}");
            assert_eq!(
                totals.gauge("logit.process.memory.resident.bytes", &[]),
                Some(4096.0),
                "tick {tick}"
            );
            assert_eq!(totals.gauge("logit.process.threads", &[]), Some(3.0), "tick {tick}");
            assert_eq!(totals.gauge("logit.process.fds.limit", &[]), Some(1024.0), "tick {tick}");
            assert!(totals.has("logit.process.cpu.seconds", &[("mode", "user")]), "tick {tick}");
            assert_eq!(
                totals.gauge("logit.process.memory.allocated.bytes", &[]),
                Some(7.0),
                "tick {tick}"
            );
        }
        assert_eq!(CALLS.load(Ordering::Relaxed), 1, "a latched source is never read again");
    }

    #[tokio::test]
    async fn a_disabled_telemetry_handle_reads_no_process_source() {
        static CALLS: AtomicUsize = AtomicUsize::new(0);
        fn counted_status() -> Result<Status, Unavailable> {
            CALLS.fetch_add(1, Ordering::Relaxed);
            fixed_status()
        }
        fn counted_cpu() -> Result<CpuTicks, Unavailable> {
            CALLS.fetch_add(1, Ordering::Relaxed);
            fixed_cpu()
        }
        fn counted_fds() -> Result<u64, Unavailable> {
            CALLS.fetch_add(1, Ordering::Relaxed);
            fixed_fds()
        }
        fn counted_fds_limit() -> Result<Option<u64>, Unavailable> {
            CALLS.fetch_add(1, Ordering::Relaxed);
            fixed_fds_limit()
        }
        fn counted_heap() -> Option<u64> {
            CALLS.fetch_add(1, Ordering::Relaxed);
            Some(1)
        }
        let mut input = InternalInput::new(Duration::from_millis(1), Registry::new())
            .with_heap_stats(counted_heap);
        input.process.read_status = counted_status;
        input.process.read_cpu = counted_cpu;
        input.process.read_fds = counted_fds;
        input.process.read_fds_limit = counted_fds_limit;
        let (tx, _rx) = mpsc::channel(1);

        input.tick(Instant::now(), &Fanout::new(vec![tx])).await;

        assert_eq!(CALLS.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn the_heap_hook_gauges_allocated_bytes_only_when_it_answers() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("self", "internal", "listener");
        let diag = Diagnostics::default();

        let mut answering = fixed_sampler();
        answering.heap = InternalInput::new(Duration::from_millis(1), Registry::new())
            .with_heap_stats(|| Some(42))
            .process
            .heap;
        answering.sample(&telemetry, &diag);
        let totals = Totals::of(registry.drain(0));
        assert_eq!(totals.gauge("logit.process.memory.allocated.bytes", &[]), Some(42.0));

        let mut silent = ProcessSampler { heap: Some(|| None), ..fixed_sampler() };
        silent.sample(&telemetry, &diag);
        let totals = Totals::of(registry.drain(0));
        assert!(!totals.has("logit.process.memory.allocated.bytes", &[]));
        assert_eq!(totals.gauge("logit.process.threads", &[]), Some(3.0));
    }

    /// OTLP backends read a trace's service name off the root span's resource; with an empty one,
    /// Grafana's Traces Drilldown shows `<root span not yet received>`.
    #[tokio::test]
    async fn every_batch_carries_service_name_on_its_resource() {
        let registry = Registry::new();
        let component_telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        component_telemetry.count("logit.input.datagrams", 1.0, &[]);

        let mut input = InternalInput::new(Duration::from_millis(1), registry);
        let (tx, mut rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx]);

        input.tick(Instant::now(), &fanout).await;

        let delivered = rx.try_recv().expect("should have sent a batch");
        let batch = match delivered {
            logit_pipeline::Delivered::Owned(batch, _ctx) => batch,
            logit_pipeline::Delivered::Shared(shared, _ctx) => (*shared).clone(),
        };
        assert_eq!(
            batch.resource.attributes.get("service.name").and_then(|v| v.as_str()),
            Some("logit")
        );
    }

    #[tokio::test]
    async fn every_batch_carries_the_logit_scope() {
        let registry = Registry::new();
        let component_telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        component_telemetry.count("logit.input.datagrams", 1.0, &[]);

        let mut input = InternalInput::new(Duration::from_millis(1), registry);
        let (tx, mut rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx]);

        input.tick(Instant::now(), &fanout).await;

        let delivered = rx.try_recv().expect("should have sent a batch");
        let batch = match delivered {
            logit_pipeline::Delivered::Owned(batch, _ctx) => batch,
            logit_pipeline::Delivered::Shared(shared, _ctx) => (*shared).clone(),
        };
        let scope = batch.scope.expect("internal should stamp a Scope on every batch");
        assert_eq!(&scope.name[..], b"logit");
        assert_eq!(&scope.version[..], env!("CARGO_PKG_VERSION").as_bytes());
    }

    #[tokio::test]
    async fn points_emitted_is_counted_for_the_following_drain() {
        let registry = Registry::new();
        let own_telemetry = registry.telemetry_for("self", "internal", "listener");
        let component_telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        component_telemetry.count("logit.input.datagrams", 1.0, &[]);

        let mut input =
            InternalInput::new(Duration::from_millis(1), registry).with_telemetry(own_telemetry);
        let (tx, mut rx) = mpsc::channel(4);
        let fanout = Fanout::new(vec![tx]);

        input.tick(Instant::now(), &fanout).await; // drains statsd_in's point; buffers the emitted-count
        input.tick(Instant::now(), &fanout).await; // now drains the emitted-count from the first tick

        let mut found = false;
        while let Ok(delivered) = rx.try_recv() {
            let batch = match delivered {
                logit_pipeline::Delivered::Owned(batch, _ctx) => batch,
                logit_pipeline::Delivered::Shared(shared, _ctx) => (*shared).clone(),
            };
            for event in &batch.events {
                for metric in &event.metrics {
                    if logit_core::interner::resolve(metric.name) == "logit.internal.points.emitted"
                    {
                        found = true;
                        if let MetricKind::Sum(sum) = metric.kind {
                            assert!(sum.value >= 1.0);
                        }
                    }
                }
            }
        }
        assert!(found, "the second drain should carry the first drain's emitted-count");
    }

    /// A drain mixing spans and points counts each under its own counter.
    #[tokio::test]
    async fn a_drain_carrying_a_span_reports_spans_emitted_separately_from_points_emitted() {
        let registry = logit_core::Registry::with_span_sampling(1.0);
        let own_telemetry = registry.telemetry_for("self", "internal", "listener");
        let component_telemetry = registry.telemetry_for("agg", "aggregate", "transform");
        component_telemetry.count("logit.transform.series.active", 1.0, &[]);
        drop(component_telemetry.span(
            "flush",
            logit_core::SpanKind::Internal,
            [1; 16],
            [1; 8],
            None,
        ));

        let mut input =
            InternalInput::new(Duration::from_millis(1), registry).with_telemetry(own_telemetry);
        let (tx, mut rx) = mpsc::channel(4);
        let fanout = Fanout::new(vec![tx]);

        input.tick(Instant::now(), &fanout).await; // drains the point + the span; buffers both counts
        input.tick(Instant::now(), &fanout).await; // now drains the emitted-counts from the first tick

        let (mut found_points, mut found_spans) = (false, false);
        while let Ok(delivered) = rx.try_recv() {
            let batch = match delivered {
                logit_pipeline::Delivered::Owned(batch, _ctx) => batch,
                logit_pipeline::Delivered::Shared(shared, _ctx) => (*shared).clone(),
            };
            for event in &batch.events {
                for metric in &event.metrics {
                    match logit_core::interner::resolve(metric.name) {
                        "logit.internal.points.emitted" => found_points = true,
                        "logit.internal.spans.emitted" => found_spans = true,
                        _ => {}
                    }
                }
            }
        }
        assert!(found_points, "a points.emitted counter should still be recorded");
        assert!(found_spans, "a spans.emitted counter should also be recorded, separately");
    }

    /// A drained log event is counted as a log, not a point, though it carries no `metrics`.
    #[tokio::test]
    async fn a_drain_carrying_a_log_reports_logs_emitted_separately_from_points_emitted() {
        let registry = logit_core::Registry::new();
        let own_telemetry = registry.telemetry_for("self", "internal", "listener");
        let component_telemetry = registry.telemetry_for("stat", "statsd_in", "listener");
        component_telemetry.count("logit.input.datagrams", 1.0, &[]);

        // Captured through the public `TelemetryLayer` path, not `logit_core`'s private plumbing.
        use tracing_subscriber::layer::SubscriberExt;
        let layer = logit_core::TelemetryLayer::new();
        layer.activate(registry.clone(), logit_core::Severity::Warn, "self");
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(target: "logit", component = "stat", key = "bad_datagram", "malformed");
        });

        let mut input =
            InternalInput::new(Duration::from_millis(1), registry).with_telemetry(own_telemetry);
        let (tx, mut rx) = mpsc::channel(4);
        let fanout = Fanout::new(vec![tx]);

        input.tick(Instant::now(), &fanout).await; // drains the point + the log; buffers both counts
        input.tick(Instant::now(), &fanout).await; // now drains the emitted-counts from the first tick

        let (mut found_points, mut found_logs) = (false, false);
        while let Ok(delivered) = rx.try_recv() {
            let batch = match delivered {
                logit_pipeline::Delivered::Owned(batch, _ctx) => batch,
                logit_pipeline::Delivered::Shared(shared, _ctx) => (*shared).clone(),
            };
            for event in &batch.events {
                for metric in &event.metrics {
                    match logit_core::interner::resolve(metric.name) {
                        "logit.internal.points.emitted" => found_points = true,
                        "logit.internal.logs.emitted" => found_logs = true,
                        _ => {}
                    }
                }
            }
        }
        assert!(found_points, "a points.emitted counter should still be recorded");
        assert!(found_logs, "a logs.emitted counter should also be recorded, separately");
    }

    fn batch_of(delivered: logit_pipeline::Delivered) -> logit_core::EventBatch {
        match delivered {
            logit_pipeline::Delivered::Owned(batch, _ctx) => batch,
            logit_pipeline::Delivered::Shared(shared, _ctx) => (*shared).clone(),
        }
    }

    fn metric_names(batch: &logit_core::EventBatch) -> Vec<&'static str> {
        batch
            .events
            .iter()
            .flat_map(|e| e.metrics.iter().map(|m| logit_core::interner::resolve(m.name)))
            .collect()
    }

    /// A shutdown mid-interval gets one final drain of the buffered point, then `Ok(())`.
    #[tokio::test(start_paused = true)]
    async fn a_shutdown_mid_interval_drains_once_more_before_returning() {
        let registry = Registry::new();
        let component_telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        component_telemetry.count("logit.input.datagrams", 1.0, &[]);

        let mut input = InternalInput::new(Duration::from_secs(60), registry);
        let (tx, mut rx) = mpsc::channel(4);
        let fanout = Fanout::new(vec![tx]);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let handle =
            tokio::spawn(async move { input.run_until_shutdown(fanout, shutdown_rx).await });

        // One second into a sixty-second interval: the next tick is 59s away.
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(rx.try_recv().is_err(), "no interval tick is due yet");
        shutdown_tx.send(true).expect("the run task holds a receiver");

        handle.await.expect("the drain task should not panic").expect("should return Ok(())");

        let batch = batch_of(rx.try_recv().expect("the final drain should have sent a batch"));
        assert_eq!(
            batch.events[0].attributes.get("component").and_then(|v| v.as_str()),
            Some("statsd_in")
        );
        assert_eq!(metric_names(&batch), vec!["logit.input.datagrams"]);
        assert!(rx.try_recv().is_err(), "the final drain should send exactly one batch");
    }

    /// After a full tick, the shutdown drain carries only what was buffered since, not a replay.
    #[tokio::test(start_paused = true)]
    async fn a_shutdown_after_a_tick_still_drains_the_partial_interval() {
        let registry = Registry::new();
        let component_telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        component_telemetry.count("logit.input.datagrams", 1.0, &[]);

        let mut input = InternalInput::new(Duration::from_secs(60), registry);
        let (tx, mut rx) = mpsc::channel(4);
        let fanout = Fanout::new(vec![tx]);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let handle =
            tokio::spawn(async move { input.run_until_shutdown(fanout, shutdown_rx).await });

        tokio::task::yield_now().await; // let the loop consume interval's immediate first tick
        tokio::time::advance(Duration::from_secs(60)).await;
        tokio::task::yield_now().await; // ...and let the tick it just made due actually drain
        let first = batch_of(rx.try_recv().expect("the interval tick should have sent a batch"));
        assert_eq!(metric_names(&first), vec!["logit.input.datagrams"]);

        component_telemetry.count("logit.input.decode.errors", 1.0, &[]);
        shutdown_tx.send(true).expect("the run task holds a receiver");

        handle.await.expect("the drain task should not panic").expect("should return Ok(())");

        let second = batch_of(rx.try_recv().expect("the final drain should have sent a batch"));
        assert_eq!(
            metric_names(&second),
            vec!["logit.input.decode.errors"],
            "the final drain carries only what was buffered since the last tick"
        );
        assert!(rx.try_recv().is_err(), "exactly two batches for two drains");
    }

    /// `run` drains on every interval and never returns on its own.
    #[tokio::test(start_paused = true)]
    async fn run_keeps_draining_on_every_interval() {
        let registry = Registry::new();
        let component_telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        component_telemetry.count("logit.input.datagrams", 1.0, &[]);

        let mut input = InternalInput::new(Duration::from_secs(60), registry);
        let (tx, mut rx) = mpsc::channel(4);
        let fanout = Fanout::new(vec![tx]);

        let handle = tokio::spawn(async move { input.run(fanout).await });

        tokio::task::yield_now().await; // let the loop consume interval's immediate first tick
        tokio::time::advance(Duration::from_secs(60)).await;
        tokio::task::yield_now().await;
        let first = batch_of(rx.try_recv().expect("the first interval tick should have drained"));
        assert_eq!(metric_names(&first), vec!["logit.input.datagrams"]);

        component_telemetry.count("logit.input.decode.errors", 1.0, &[]);
        tokio::time::advance(Duration::from_secs(60)).await;
        tokio::task::yield_now().await;
        let second = batch_of(rx.try_recv().expect("the second interval tick should have drained"));
        assert_eq!(metric_names(&second), vec!["logit.input.decode.errors"]);

        assert!(!handle.is_finished(), "run should keep ticking, not return on its own");
        handle.abort();
    }
}
