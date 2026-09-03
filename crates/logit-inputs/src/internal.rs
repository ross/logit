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
use logit_core::{interner, AttrMap, Diagnostics, EventBatch, Registry, Resource, Telemetry};
use logit_pipeline::Fanout;
use std::sync::Arc;
use std::time::{Duration, Instant};

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
            resource: Arc::new(Resource { attributes }),
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
        let started = Instant::now();
        let mut ticker = tokio::time::interval(self.interval);
        // `tokio::time::interval` fires its first tick immediately -- consumed here and skipped,
        // so the first real drain happens after one full interval has actually elapsed rather
        // than at t=0 against buffers nothing has had time to populate.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            self.tick(started, &sink).await;
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
        // Split by shape, not just totalled: `drain` now returns metric-point events and
        // span-carrying events in one flat list (`docs/design/internal-telemetry.md`'s "Spans"
        // section), and `logit.internal.points.emitted` naming *points* specifically would become
        // wrong the moment a span rode along inside its count uncounted-for.
        // `logit.internal.spans.emitted` is the symmetric counter for the other half.
        let (points_emitted, spans_emitted) =
            events.iter().fold((0u64, 0u64), |(points, spans), event| {
                if event.span.is_some() {
                    (points, spans + 1)
                } else {
                    (points + 1, spans)
                }
            });
        if points_emitted > 0 {
            self.telemetry.count("logit.internal.points.emitted", points_emitted as f64, &[]);
        }
        if spans_emitted > 0 {
            self.telemetry.count("logit.internal.spans.emitted", spans_emitted as f64, &[]);
        }

        sink.send(EventBatch { resource: self.resource.clone(), events }).await;
    }
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
                        if let MetricKind::Counter(v) = metric.kind {
                            assert!(v >= 1.0);
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
}
