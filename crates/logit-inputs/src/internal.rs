//! `internal`: `logit` talking about itself. Drains every component's buffered self-telemetry
//! points ([`logit_core::telemetry`]) on `interval` and emits them into the graph as ordinary
//! events, exactly like any other listener -- so every existing downstream tool (`aggregate`,
//! `keep`, `lua`, any sink) already works on them, with nothing new to build. See
//! `docs/design/internal-telemetry.md` and `docs/adr/internal-telemetry-as-pipeline-events.md`.
//!
//! `interval` serves double duty: the drain cadence for every component's buffered points, and
//! the sampling tick for this component's own process-level gauges (interner size, uptime) --
//! tied to no occurrence, so nothing else would ever push them.

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
    /// Built once here and `Arc`-shared by every batch this input ever sends -- the resource is
    /// batch-level and identical on every tick, so rebuilding it per drain would re-intern
    /// `service.name` and reallocate an `AttrMap` for a value that cannot change. `internal` is
    /// the one input allowed to stamp `service.name = logit`: this is `logit`'s own telemetry,
    /// unlike `syslog_in`/`statsd_in`, whose ingested data belongs to other services and would be
    /// misidentified by the same stamp. See `docs/design/internal-telemetry.md`.
    resource: Arc<Resource>,
    /// The OTLP instrumentation scope every batch this input sends carries -- `{ name: "logit",
    /// version: env!("CARGO_PKG_VERSION") }`, built once here and `Arc`-shared across every
    /// batch the same way `resource` is. `internal` is the one input allowed to stamp this: it's
    /// the identity an earlier codec revision used to *invent* on decode for any OTLP-sourced
    /// batch with no wire scope (`crates/logit-proto/src/otlp/common.rs`'s `pb_to_scope`), which
    /// W4 retired everywhere except here, where it belongs to the one real producer of it --
    /// `logit`'s own self-telemetry. `docs/design/internal-telemetry.md` relies on this scope
    /// existing to identify `logit`'s own points/spans/logs downstream.
    scope: Arc<Scope>,
    telemetry: Telemetry,
    diag: Diagnostics,
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
        }
    }

    /// Attaches this component's own telemetry handle -- `internal` is a component like any
    /// other, registered in the same `Registry` it drains, so its own points (`logit.process.*`,
    /// `logit.internal.*`) ride along in the very next drain rather than needing a special path.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

#[async_trait::async_trait]
impl Input for InternalInput {
    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        // Never exercised in production -- `run_input` (`crates/logit-pipeline/src/runtime.rs`)
        // always calls `run_until_shutdown`. Present because the trait requires it, and shaped
        // exactly like `crate::udp::UdpListener::run`: a never-firing `watch` channel, so the two
        // entry points share one loop rather than drifting apart. The `_tx` binding is
        // load-bearing -- drop the sender and `wait_for` below resolves immediately with
        // `RecvError`, which would turn every `run` into "drain once, then exit".
        let (_tx, rx) = watch::channel(false);
        self.run_until_shutdown(sink, rx).await
    }

    /// The drain loop, plus **one final drain when `shutdown` fires** -- the whole reason this
    /// input overrides the trait's default (which just drops `run`'s future, ADR
    /// `decoupled-listener-io`).
    ///
    /// **Why.** Points land in `logit_core::telemetry`'s per-component buffers continuously but
    /// only leave them on a drain tick, so at the instant a SIGTERM arrives there is always up to
    /// one whole `interval` of buffered self-telemetry sitting there. Cancel-by-drop threw all of
    /// it away, silently: a process running the default 10s `interval` for 25s reported two
    /// intervals and lost the third. That's wrong for any operator watching `logit`'s own
    /// counters across a restart, and it's load-bearing for `logit-perf`'s attribution mode
    /// (`docs/design/performance.md`), which reads exactly these points back out of a short-lived
    /// process it SIGTERMs on purpose -- without this drain the last, and for a short run the
    /// most interesting, slice of every node's `process.duration` never reaches the dump.
    ///
    /// **Why the final drain's batch actually gets delivered.** `sink` is a [`Fanout`] owned by
    /// this future, and every downstream node's inbox stays open for as long as *some* sender
    /// exists -- so nothing downstream can begin its own close-time flush until this function
    /// returns and drops it (`run_with_telemetry`'s shutdown cascade,
    /// `crates/logit-pipeline/src/runtime.rs`). `run_input` bounds that wait by
    /// `logit_pipeline::InputRuntimeConfig`'s `shutdown_grace`, which for `internal` is
    /// `ReceiveConfig::default()`'s 5s (`logit_cli::pipeline::input_runtime_config`) -- one
    /// `Registry::drain` plus one `Fanout::send` fits inside that with room to spare, and
    /// `Fanout::send`'s only unbounded wait is downstream backpressure, which the grace backstop
    /// is there to cut short anyway.
    ///
    /// **Residual, by design.** The final drain's own `logit.internal.points.emitted` (and the
    /// `spans`/`logs` counters, and `logit.internal.drain.duration`) are recorded *after* the
    /// drain that produced them, so they sit in `internal`'s buffer one tick behind and, with no
    /// tick left to come, are never emitted. That's the same "a drain can't include a count of
    /// itself" property every one of these self-counts already has (`InternalInput::tick`'s own
    /// comment, `docs/design/internal-telemetry.md`) -- not a new gap, just its last instance.
    async fn run_until_shutdown(
        &mut self,
        sink: Fanout,
        mut shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        let started = Instant::now();
        let mut ticker = tokio::time::interval(self.interval);
        // `tokio::time::interval` fires its first tick immediately -- consumed here and skipped,
        // so the first real drain happens after one full interval has actually elapsed rather
        // than at t=0 against buffers nothing has had time to populate.
        ticker.tick().await;
        loop {
            // Both arms are cancellation-safe: `Interval::tick` guarantees no tick is consumed
            // when another branch wins, and `wait_for` re-checks the current value on its next
            // call, so neither a tick nor the shutdown edge can be lost to the loser of a race.
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
    async fn tick(&self, started: Instant, sink: &Fanout) {
        // Process-level facts sampled here rather than pushed by anything else, since nothing
        // else has an occasion to push them -- closes the `interner::len()` observability hook
        // `docs/known-gaps.md` names as "nearly free... and would make this observable rather
        // than silent" once something reads it.
        self.telemetry.gauge("logit.process.interner.strings", interner::len() as f64, &[]);
        self.telemetry.gauge("logit.process.uptime", started.elapsed().as_secs_f64(), &[]);

        let drain_timer = self.telemetry.timer("logit.internal.drain.duration");
        let events = self.registry.drain(now_nanos());
        drop(drain_timer);
        if events.is_empty() {
            return;
        }
        // Recorded via `self.telemetry` after the drain that produced this count -- like every
        // other point here, it rides along in the *next* drain, one tick behind. Every mature
        // statsd client's own self-telemetry (packets sent/dropped) works the same way, for the
        // same reason: a drain can't include a count of itself.
        //
        // Split by shape, not just totalled: `drain` now returns metric-point, span-carrying,
        // and (workstream D) log-carrying events in one flat list
        // (`docs/design/internal-telemetry.md`'s "Spans" and "Logs" sections), and
        // `logit.internal.points.emitted` naming *points* specifically would become wrong the
        // moment a span or a log rode along inside its count uncounted-for. `event.log` is
        // checked *before* falling through to "point" -- a log event carries neither `metrics`
        // nor `span`, so without this check it would be miscounted as a point.
        // `logit.internal.spans.emitted`/`logs.emitted` are the symmetric counters for the other
        // two shapes.
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

        // A real Scope, deliberately: this is `logit` observing itself, the one producer
        // `docs/design/internal-telemetry.md` names as allowed to stamp that identity on purpose
        // (see `scope`'s own doc comment on this struct).
        sink.send(EventBatch {
            resource: self.resource.clone(),
            scope: Some(self.scope.clone()),
            events,
        })
        .await;
    }
}

/// Resolves once `shutdown` holds `true`, yielding nothing.
///
/// The wrapper exists to make the `select!` above `Send`, which `Input`'s `#[async_trait]`
/// requires: `watch::Receiver::wait_for` resolves to a `watch::Ref` holding an
/// `RwLockReadGuard`, which isn't `Send`, and `select!` keeps each branch's resolved value alive
/// across the *other* branch's handler -- which here `.await`s a drain. Returning `()` drops the
/// guard before the macro ever stores it. (`Input::run_until_shutdown`'s default body can inline
/// the same call because neither of its handlers awaits anything.)
///
/// The `Result` is discarded for the same reason that default body discards it: `Err` means the
/// sender was dropped, which in this process only happens as part of the same teardown, and
/// "drain once more, then stop" is the right answer either way.
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
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn a_tick_with_nothing_buffered_sends_nothing() {
        let registry = Registry::new();
        let input = InternalInput::new(Duration::from_millis(1), registry);
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

        let input = InternalInput::new(Duration::from_millis(1), registry);
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
        let input =
            InternalInput::new(Duration::from_millis(1), registry).with_telemetry(own_telemetry);
        let (tx, mut rx) = mpsc::channel(2);
        let fanout = Fanout::new(vec![tx]);

        // Nothing else buffered, so this drain contains only `internal`'s own process-level
        // gauges -- sampled just before the drain inside the same `tick` call.
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
        let _ = rx.try_recv(); // drain any second batch, unasserted
    }

    /// Tempo (and every other OTLP backend) reads the *root span's resource* for a trace's
    /// service name -- an empty `Resource` is why Grafana's Traces Drilldown showed
    /// `<root span not yet received>` against traces whose root span had plainly been received.
    /// `internal`'s telemetry is `logit`'s own, so unlike `syslog_in`/`statsd_in` (whose data
    /// belongs to *other* services) it is the one input that can honestly name itself here.
    #[tokio::test]
    async fn every_batch_carries_service_name_on_its_resource() {
        let registry = Registry::new();
        let component_telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        component_telemetry.count("logit.input.datagrams", 1.0, &[]);

        let input = InternalInput::new(Duration::from_millis(1), registry);
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

    /// The scope identity `otlp_out` used to have `otlp_in`'s decoder *invent* for any
    /// OTLP-sourced batch with no wire scope -- W4 retired that everywhere except here, where it
    /// belongs to the one real producer of it (`InternalInput::scope`'s own doc comment).
    #[tokio::test]
    async fn every_batch_carries_the_logit_scope() {
        let registry = Registry::new();
        let component_telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        component_telemetry.count("logit.input.datagrams", 1.0, &[]);

        let input = InternalInput::new(Duration::from_millis(1), registry);
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

        let input =
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

    /// The counting half of `docs/design/internal-telemetry.md`'s "Spans" section: a drain that
    /// mixes span and metric events reports each kind under its own counter, not one merged
    /// `points.emitted` that would misdescribe a span as a point.
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

        let input =
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

    /// The same property as the span/point split above, for workstream D's log events
    /// (`docs/plans/operator-surface.md`): a log event carries neither `metrics` nor `span`, so
    /// without the `event.log.is_some()` check landing *before* the "point" fallback, it would be
    /// miscounted as a point.
    #[tokio::test]
    async fn a_drain_carrying_a_log_reports_logs_emitted_separately_from_points_emitted() {
        let registry = logit_core::Registry::new();
        let own_telemetry = registry.telemetry_for("self", "internal", "listener");
        let component_telemetry = registry.telemetry_for("stat", "statsd_in", "listener");
        component_telemetry.count("logit.input.datagrams", 1.0, &[]);

        // Through the public path -- `TelemetryLayer`, activated, capturing a real `tracing`
        // event -- rather than reaching into `logit_core::telemetry`'s own private plumbing.
        use tracing_subscriber::layer::SubscriberExt;
        let layer = logit_core::TelemetryLayer::new();
        layer.activate(registry.clone(), logit_core::Severity::Warn, "self");
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(target: "logit", component = "stat", key = "bad_datagram", "malformed");
        });

        let input =
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

    /// Pulls the `EventBatch` out of a `Delivered`, the same two-arm match every assertion in
    /// this module does inline; the shutdown tests below read several batches each, which is
    /// where repeating it stops being cheaper than naming it.
    fn batch_of(delivered: logit_pipeline::Delivered) -> logit_core::EventBatch {
        match delivered {
            logit_pipeline::Delivered::Owned(batch, _ctx) => batch,
            logit_pipeline::Delivered::Shared(shared, _ctx) => (*shared).clone(),
        }
    }

    /// Every metric name in a batch, resolved -- what the shutdown tests assert the *contents* of
    /// a drain with, rather than just its arrival.
    fn metric_names(batch: &logit_core::EventBatch) -> Vec<&'static str> {
        batch
            .events
            .iter()
            .flat_map(|e| e.metrics.iter().map(|m| logit_core::interner::resolve(m.name)))
            .collect()
    }

    /// The point of the `run_until_shutdown` override: a SIGTERM arriving partway through an
    /// interval used to drop that interval's buffered points on the floor (cancel-by-drop), so
    /// with a 60s interval and a shutdown one second in, *nothing* was ever emitted. Now the
    /// buffered point gets exactly one final drain, and the function returns `Ok(())` rather
    /// than being cancelled.
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

        // One second into a sixty-second interval: the loop is parked on a tick that is 59s away
        // from firing, which is precisely the window the old cancel-by-drop lost.
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

    /// The same property one interval later, which is the case that proves the final drain is a
    /// drain of the *partial* interval and not just a replay: a full tick emits the first point,
    /// a second point is then recorded mid-interval, and shutdown emits that one on its own.
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

    /// `run` is what the `Input` trait contract requires to work standalone, and it now reaches
    /// the same loop through a never-firing `watch` channel -- so the thing worth pinning is that
    /// it still drains on every interval and never returns on its own.
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
