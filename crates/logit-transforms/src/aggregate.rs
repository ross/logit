//! The built-in `aggregate` transform: a stateful, tumbling-window metric aggregator.
//!
//! Windowing/merge semantics are recorded in `docs/adr/0008-aggregation-window-semantics.md` and
//! come from `docs/design/data-model.md`'s mergeable-metric-kinds design: `Counter` sums, `Gauge`
//! keeps the value with the latest source timestamp, `Distribution` merges via `DdSketch::merge`
//! (this is that method's first real caller anywhere in the codebase). `Set`/`Histogram`/`Summary`
//! have no defined merge rule here yet (`Set` specifically is blocked on `HyperLogLog` still being a
//! method-less stub) and pass through untouched rather than being dropped -- this project's
//! consistent stance on data it doesn't know how to handle correctly. Since an event can now carry
//! a log and/or a span alongside its metrics (docs/adr/0012-multi-payload-events.md), pass-through
//! is per *metric*, not per *event*: this stage absorbs every mergeable metric off an event and
//! forwards whatever's left -- the unmergeable metrics, plus any log/span -- rather than treating
//! "can't merge one metric" as a reason to forward the whole event untouched.

use logit_core::interner::Symbol;
use logit_core::{AttrMap, Diagnostics, Event, MetricKind, MetricRecord, Resource, Value};
use logit_pipeline::Transform;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

/// One tumbling-window aggregator, owned by one pipeline stage. `process` accumulates what it can
/// and passes everything else straight through; `flush` drains every window it's holding, resetting
/// each to empty -- state does not carry across flushes (tumbling, not sliding).
pub struct Aggregator {
    interval: Duration,
    groups: Vec<ResourceGroup>,
    diag: Diagnostics,
}

struct ResourceGroup {
    resource: Arc<Resource>,
    series: HashMap<SeriesKey, Accumulator>,
}

enum Accumulator {
    Counter(f64),
    /// `at` is the source event's timestamp, used to pick the last-write-wins value -- not the
    /// window's timestamp, which doesn't exist until flush.
    Gauge {
        value: f64,
        at: i64,
    },
    Distribution(logit_core::DdSketch),
}

impl Aggregator {
    pub fn new(interval: Duration) -> Self {
        Self { interval, groups: Vec::new(), diag: Diagnostics::default() }
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Absorbs every mergeable metric (`Counter`/`Gauge`/`Distribution`) off `event` into this
    /// aggregator's window state, and forwards whatever's left -- unmergeable metric kinds, a
    /// kind conflict with an already-accumulating series, and/or a log or span, if the event
    /// carries any (docs/adr/0012-multi-payload-events.md). `None` only when nothing at all
    /// remains on the event; a pure log/span event (no metrics at all) never touches window
    /// state, matching the zero-cost pass-through this had before an event could carry more than
    /// one payload.
    ///
    /// Grouped by `resource` *value*, not `Arc` identity: two inputs that each build their own
    /// `Arc::new(Resource::default())` describe the same (empty) origin and should aggregate
    /// together, not be split into separate windows because they happen to be different
    /// allocations. One input's batches do share one `Arc` in practice (see
    /// `crates/logit-inputs/src/statsd.rs`), so the common case is a single linear-scan group.
    pub fn process(&mut self, resource: &Arc<Resource>, mut event: Event) -> Option<Event> {
        if event.metrics.is_empty() {
            return Some(event);
        }

        // Taken as an owned list, not filtered in place with `retain`: a `retain` closure would
        // need `&event.attributes`/`&event.timestamp` at the same time `self.group_for(resource)`
        // needs `&mut self`, and those two borrows can't coexist. Owning the metrics up front
        // makes them independent of `event` for the rest of this loop; anything not absorbed is
        // pushed back at the end, in its original relative order.
        let metrics = std::mem::take(&mut event.metrics);
        for record in metrics {
            // No merge rule defined for these (docs/design/data-model.md) -- leave them on the
            // event rather than absorbing or dropping them.
            if matches!(
                record.kind,
                MetricKind::Set(_) | MetricKind::Histogram { .. } | MetricKind::Summary { .. }
            ) {
                event.metrics.push(record);
                continue;
            }

            let key = SeriesKey {
                name: record.name,
                unit: record.unit,
                attributes: event.attributes.clone(),
            };
            let group = self.group_for(resource);
            let acc = group.series.entry(key).or_insert_with(|| Accumulator::new_for(&record.kind));
            let accumulated = match (acc, &record.kind) {
                (Accumulator::Counter(sum), MetricKind::Counter(v)) => {
                    *sum += v;
                    true
                }
                (Accumulator::Gauge { value, at }, MetricKind::Gauge(v)) => {
                    // Last-write-wins by timestamp (docs/design/data-model.md): a later-or-equal
                    // source timestamp replaces the held value. Equal timestamps favor whichever
                    // arrives second -- arbitrary but deterministic given actual processing
                    // order. Two gauges of the same series inside one event share `event
                    // .timestamp`, so this tiebreak is what decides between them too.
                    if event.timestamp >= *at {
                        *value = *v;
                        *at = event.timestamp;
                    }
                    true
                }
                (Accumulator::Distribution(sketch), MetricKind::Distribution(incoming)) => {
                    sketch.merge(incoming);
                    true
                }
                // A series already accumulating under one kind (e.g. it started as a counter)
                // just saw a metric of a different kind under the same name/unit/tags (e.g. a
                // gauge). No correct merge exists for that -- leave this one metric on the event
                // rather than silently dropping it or corrupting the existing accumulator with a
                // type-punned value. Per-metric now, not per-event: a sibling metric on the same
                // event that *does* merge cleanly is still absorbed.
                _ => false,
            };
            if !accumulated {
                self.diag.warn_throttled(
                    "kind_conflict",
                    format_args!(
                        "metric '{}' has a kind that conflicts with an already-accumulating \
                         series under the same name/unit/tags -- forwarding it untouched",
                        logit_core::interner::resolve(record.name)
                    ),
                );
                event.metrics.push(record);
            }
        }

        if event.metrics.is_empty() && event.log.is_none() && event.span.is_none() {
            None
        } else {
            Some(event)
        }
    }

    fn group_for(&mut self, resource: &Arc<Resource>) -> &mut ResourceGroup {
        if let Some(i) = self.groups.iter().position(|g| g.resource.as_ref() == resource.as_ref()) {
            &mut self.groups[i]
        } else {
            self.groups.push(ResourceGroup { resource: resource.clone(), series: HashMap::new() });
            self.groups.last_mut().expect("just pushed")
        }
    }

    /// Drains every window: one `Vec<Event>` per resource group that had any series, each series
    /// becoming one emitted event stamped with `now`. Resets all state -- the next window starts
    /// empty, per the tumbling design in ADR 0008.
    pub fn flush(&mut self, now: i64) -> Vec<(Arc<Resource>, Vec<Event>)> {
        self.groups
            .drain(..)
            .filter(|g| !g.series.is_empty())
            .map(|g| {
                let events = g
                    .series
                    .into_iter()
                    .map(|(key, acc)| {
                        Event::metric(
                            now,
                            key.attributes,
                            MetricRecord { name: key.name, kind: acc.into_kind(), unit: key.unit },
                        )
                    })
                    .collect();
                (g.resource, events)
            })
            .collect()
    }
}

/// `Aggregator`'s existing inherent methods already match `Transform`'s contract exactly (a
/// deliberate match, not a coincidence -- see `crate::Transform`'s doc comment): this impl is
/// pure delegation, no reshaping needed.
impl Transform for Aggregator {
    fn process(&mut self, resource: &Arc<Resource>, event: Event) -> Option<Event> {
        Aggregator::process(self, resource, event)
    }

    fn flush_interval(&self) -> Option<Duration> {
        Some(self.interval())
    }

    fn flush(&mut self, now: i64) -> Vec<(Arc<Resource>, Vec<Event>)> {
        Aggregator::flush(self, now)
    }
}

impl Accumulator {
    fn new_for(kind: &MetricKind) -> Self {
        match kind {
            MetricKind::Counter(_) => Accumulator::Counter(0.0),
            MetricKind::Gauge(_) => Accumulator::Gauge { value: 0.0, at: i64::MIN },
            MetricKind::Distribution(_) => Accumulator::Distribution(logit_core::DdSketch::new()),
            MetricKind::Set(_) | MetricKind::Histogram { .. } | MetricKind::Summary { .. } => {
                unreachable!("process() never creates an accumulator for a pass-through kind")
            }
        }
    }

    fn into_kind(self) -> MetricKind {
        match self {
            Accumulator::Counter(sum) => MetricKind::Counter(sum),
            Accumulator::Gauge { value, .. } => MetricKind::Gauge(value),
            Accumulator::Distribution(sketch) => MetricKind::Distribution(Box::new(sketch)),
        }
    }
}

/// A metric series' identity: name, unit, and attribute set. Used as a `HashMap` key, which
/// `AttrMap`/`Value` can't be directly -- neither implements `Eq`/`Hash` (`Value::F64` has no
/// total order). Projects `f64` through `to_bits()` for both comparison and hashing instead, so
/// `NaN` keys consistently with itself (`Eq`'s reflexivity requires `a == a`) rather than the
/// IEEE-754 "NaN != NaN" that would otherwise make an aggregation key grow without bound.
struct SeriesKey {
    name: Symbol,
    unit: Option<Symbol>,
    attributes: AttrMap,
}

impl PartialEq for SeriesKey {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.unit == other.unit
            && self.attributes.len() == other.attributes.len()
            && self
                .attributes
                .iter()
                .zip(other.attributes.iter())
                .all(|((k1, v1), (k2, v2))| k1 == k2 && value_key_eq(v1, v2))
    }
}

impl Eq for SeriesKey {}

impl Hash for SeriesKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.unit.hash(state);
        // `AttrMap::iter()` yields sorted-by-`Symbol` order (see its doc comment), so this is
        // stable regardless of insertion order -- two events with the same tags added in a
        // different order still hash and key identically.
        for (k, v) in self.attributes.iter() {
            k.hash(state);
            hash_value(v, state);
        }
    }
}

fn value_key_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::I64(a), Value::I64(b)) => a == b,
        (Value::U64(a), Value::U64(b)) => a == b,
        (Value::F64(a), Value::F64(b)) => a.to_bits() == b.to_bits(),
        (Value::Bytes(a), Value::Bytes(b)) => a == b,
        (Value::Str(a), Value::Str(b)) => a == b,
        (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| value_key_eq(a, b))
        }
        (Value::Map(a), Value::Map(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .all(|((k1, v1), (k2, v2))| k1 == k2 && value_key_eq(v1, v2))
        }
        _ => false,
    }
}

fn hash_value<H: Hasher>(v: &Value, state: &mut H) {
    // Hash the variant first (by discriminant-ish tag) so e.g. an empty `Array` and an empty
    // `Map` don't collide.
    match v {
        Value::Null => 0u8.hash(state),
        Value::Bool(b) => {
            1u8.hash(state);
            b.hash(state);
        }
        Value::I64(n) => {
            2u8.hash(state);
            n.hash(state);
        }
        Value::U64(n) => {
            3u8.hash(state);
            n.hash(state);
        }
        Value::F64(n) => {
            4u8.hash(state);
            n.to_bits().hash(state);
        }
        Value::Bytes(b) => {
            5u8.hash(state);
            b.hash(state);
        }
        Value::Str(s) => {
            6u8.hash(state);
            s.hash(state);
        }
        Value::Timestamp(t) => {
            7u8.hash(state);
            t.hash(state);
        }
        Value::Array(items) => {
            8u8.hash(state);
            items.len().hash(state);
            for item in items {
                hash_value(item, state);
            }
        }
        Value::Map(map) => {
            9u8.hash(state);
            map.len().hash(state);
            for (k, v) in map.iter() {
                k.hash(state);
                hash_value(v, state);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner::intern;

    fn metric_event(name: &str, kind: MetricKind, timestamp: i64) -> Event {
        Event::metric(
            timestamp,
            AttrMap::new(),
            MetricRecord { name: intern(name), kind, unit: None },
        )
    }

    fn metric_event_with_tags(
        name: &str,
        kind: MetricKind,
        timestamp: i64,
        tags: &[(&str, &str)],
    ) -> Event {
        let mut event = metric_event(name, kind, timestamp);
        for (k, v) in tags {
            event.attributes.insert(k, *v);
        }
        event
    }

    fn default_resource() -> Arc<Resource> {
        Arc::new(Resource::default())
    }

    fn kind_of(event: &Event) -> &MetricKind {
        &event
            .metrics
            .first()
            .unwrap_or_else(|| panic!("expected a metric on event {event:?}"))
            .kind
    }

    fn counter_value(kind: &MetricKind) -> f64 {
        match kind {
            MetricKind::Counter(v) => *v,
            other => panic!("expected Counter, got {other:?}"),
        }
    }

    #[test]
    fn counters_sum_within_a_window() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        assert!(agg
            .process(&resource, metric_event("hits", MetricKind::Counter(1.0), 0))
            .is_none());
        assert!(agg
            .process(&resource, metric_event("hits", MetricKind::Counter(2.0), 1))
            .is_none());
        assert!(agg
            .process(&resource, metric_event("hits", MetricKind::Counter(3.0), 2))
            .is_none());

        let flushed = agg.flush(100);
        assert_eq!(flushed.len(), 1);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        assert_eq!(counter_value(kind_of(&events[0])), 6.0);
        assert_eq!(events[0].timestamp, 100);
    }

    #[test]
    fn gauge_keeps_the_value_with_the_latest_source_timestamp() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        // Deliberately out of arrival order: the later-timestamped value (5) arrives first.
        agg.process(&resource, metric_event("temp", MetricKind::Gauge(5.0), 50));
        agg.process(&resource, metric_event("temp", MetricKind::Gauge(1.0), 10));

        let (_, events) = &agg.flush(100)[0];
        assert_eq!(events.len(), 1);
        match kind_of(&events[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, 5.0, "should keep the value stamped at t=50"),
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    #[test]
    fn distributions_merge_via_ddsketch() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        for v in [10.0, 20.0, 30.0, 40.0, 50.0] {
            let mut sketch = logit_core::DdSketch::new();
            sketch.add(v);
            agg.process(
                &resource,
                metric_event("latency", MetricKind::Distribution(Box::new(sketch)), 0),
            );
        }

        let (_, events) = &agg.flush(100)[0];
        assert_eq!(events.len(), 1);
        match kind_of(&events[0]) {
            MetricKind::Distribution(sketch) => {
                assert_eq!(sketch.count(), 5);
                let median = sketch.quantile(0.5).expect("merged sketch has a median");
                assert!((median - 30.0).abs() < 5.0, "median should be near 30, got {median}");
            }
            other => panic!("expected Distribution, got {other:?}"),
        }
    }

    #[test]
    fn a_second_flush_after_the_first_emits_nothing() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        agg.process(&resource, metric_event("hits", MetricKind::Counter(1.0), 0));

        assert_eq!(agg.flush(100).len(), 1, "first flush should emit the window");
        assert!(agg.flush(200).is_empty(), "tumbling: state resets, second flush is empty");
    }

    #[test]
    fn logs_and_spans_pass_through_untouched() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let log = Event::log(
            0,
            AttrMap::new(),
            logit_core::LogRecord {
                message: Value::str("hello"),
                severity: None,
                body_format: logit_core::BodyFormat::Raw,
            },
        );
        let passed = agg.process(&resource, log);
        assert!(passed.is_some(), "a log event should pass through, not be absorbed");
        assert!(agg.flush(100).is_empty(), "nothing should have been accumulated");
    }

    #[test]
    fn set_histogram_and_summary_pass_through_untouched() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        for kind in [
            MetricKind::Set(logit_core::HyperLogLog::default()),
            MetricKind::Histogram { buckets: vec![(10.0, 1)] },
            MetricKind::Summary { quantiles: vec![(0.5, 1.0)] },
        ] {
            let event = metric_event("m", kind, 0);
            assert!(
                agg.process(&resource, event).is_some(),
                "a kind with no defined merge rule should pass through"
            );
        }
        assert!(agg.flush(100).is_empty());
    }

    #[test]
    fn distinct_tag_sets_stay_distinct_series() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        agg.process(
            &resource,
            metric_event_with_tags("hits", MetricKind::Counter(1.0), 0, &[("host", "a")]),
        );
        agg.process(
            &resource,
            metric_event_with_tags("hits", MetricKind::Counter(1.0), 0, &[("host", "b")]),
        );

        let (_, events) = &agg.flush(100)[0];
        assert_eq!(events.len(), 2, "different tag values should be different series");
    }

    #[test]
    fn same_tags_in_different_insertion_order_collide_into_one_series() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        agg.process(
            &resource,
            metric_event_with_tags(
                "hits",
                MetricKind::Counter(1.0),
                0,
                &[("host", "a"), ("env", "prod")],
            ),
        );
        agg.process(
            &resource,
            metric_event_with_tags(
                "hits",
                MetricKind::Counter(1.0),
                0,
                &[("env", "prod"), ("host", "a")],
            ),
        );

        let (_, events) = &agg.flush(100)[0];
        assert_eq!(events.len(), 1, "AttrMap keeps sorted order regardless of insertion order");
        assert_eq!(counter_value(kind_of(&events[0])), 2.0);
    }

    #[test]
    fn different_resources_do_not_fold_together() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let mut resource_a = Resource::default();
        resource_a.attributes.insert("host", "a");
        let mut resource_b = Resource::default();
        resource_b.attributes.insert("host", "b");

        agg.process(&Arc::new(resource_a), metric_event("hits", MetricKind::Counter(1.0), 0));
        agg.process(&Arc::new(resource_b), metric_event("hits", MetricKind::Counter(1.0), 0));

        let flushed = agg.flush(100);
        assert_eq!(flushed.len(), 2, "distinct resources should produce distinct batches");
    }

    #[test]
    fn nan_attribute_value_keys_stably_across_events() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let mut e1 = metric_event("hits", MetricKind::Counter(1.0), 0);
        e1.attributes.insert("score", f64::NAN);
        let mut e2 = metric_event("hits", MetricKind::Counter(1.0), 0);
        e2.attributes.insert("score", f64::NAN);

        agg.process(&resource, e1);
        agg.process(&resource, e2);

        let (_, events) = &agg.flush(100)[0];
        assert_eq!(events.len(), 1, "two NaN-tagged events should key into the same series");
        assert_eq!(counter_value(kind_of(&events[0])), 2.0);
    }

    #[test]
    fn a_kind_conflict_on_one_series_is_forwarded_not_dropped_or_a_panic() {
        // Same name, same (empty) tags, different kinds -- e.g. a misconfigured statsd source
        // sending both `foo:1|c` and `foo:1|g`. There's no correct merge, so this must forward
        // the conflicting event rather than panic (this exact shape used to hit an `unreachable!`)
        // or silently corrupt the counter accumulator already in progress.
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        assert!(agg.process(&resource, metric_event("m", MetricKind::Counter(1.0), 0)).is_none());
        let conflicting = metric_event("m", MetricKind::Gauge(5.0), 0);
        let passed = agg.process(&resource, conflicting);
        assert!(passed.is_some(), "the conflicting event should be forwarded, not absorbed");

        // The counter accumulator should be untouched by the conflicting event.
        let (_, events) = &agg.flush(100)[0];
        assert_eq!(events.len(), 1);
        assert_eq!(counter_value(kind_of(&events[0])), 1.0);
    }

    /// The headline test for the multi-payload model (docs/adr/0012-multi-payload-events.md): an
    /// event carrying both a log and a counter has the counter absorbed into window state while
    /// the log is still forwarded -- pass-through is per metric now, not per event.
    #[test]
    fn a_log_event_carrying_a_counter_has_the_counter_absorbed_and_the_log_forwarded() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let mut event = Event::log(
            0,
            AttrMap::new(),
            logit_core::LogRecord {
                message: Value::str("hello"),
                severity: None,
                body_format: logit_core::BodyFormat::Raw,
            },
        );
        event.metrics.push(MetricRecord {
            name: intern("hits"),
            kind: MetricKind::Counter(1.0),
            unit: None,
        });

        let passed = agg.process(&resource, event).expect("the log half should be forwarded");
        assert!(passed.metrics.is_empty(), "the counter should have been absorbed");
        assert_eq!(
            passed.log.as_ref().expect("the log should still be present").message,
            Value::str("hello")
        );

        let (_, events) = &agg.flush(100)[0];
        assert_eq!(events.len(), 1);
        assert_eq!(counter_value(kind_of(&events[0])), 1.0);
    }

    #[test]
    fn a_mixed_metric_event_absorbs_the_mergeable_ones_and_keeps_the_rest() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let mut event = metric_event("hits", MetricKind::Counter(1.0), 0);
        event.metrics.push(MetricRecord {
            name: intern("sizes"),
            kind: MetricKind::Histogram { buckets: vec![(10.0, 1)] },
            unit: None,
        });

        let passed =
            agg.process(&resource, event).expect("the histogram should survive as the remainder");
        assert_eq!(passed.metrics.len(), 1, "only the unmergeable histogram should remain");
        assert!(matches!(passed.metrics[0].kind, MetricKind::Histogram { .. }));

        let (_, events) = &agg.flush(100)[0];
        assert_eq!(events.len(), 1);
        assert_eq!(counter_value(kind_of(&events[0])), 1.0);
    }

    #[test]
    fn a_metric_only_event_fully_absorbed_returns_none() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let mut event = metric_event("a", MetricKind::Counter(1.0), 0);
        event.metrics.push(MetricRecord {
            name: intern("b"),
            kind: MetricKind::Counter(2.0),
            unit: None,
        });

        assert!(
            agg.process(&resource, event).is_none(),
            "an event with nothing left to forward should still return None"
        );
    }

    #[test]
    fn two_metrics_of_the_same_series_on_one_event_sum_into_one_flushed_series() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let mut event = metric_event("hits", MetricKind::Counter(1.0), 0);
        event.metrics.push(MetricRecord {
            name: intern("hits"),
            kind: MetricKind::Counter(2.0),
            unit: None,
        });

        assert!(agg.process(&resource, event).is_none());

        let (_, events) = &agg.flush(100)[0];
        assert_eq!(events.len(), 1, "both metrics should key into the same series");
        assert_eq!(counter_value(kind_of(&events[0])), 3.0);
    }

    #[test]
    fn a_kind_conflict_leaves_only_the_conflicting_metric_on_the_event() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        assert!(agg.process(&resource, metric_event("m", MetricKind::Counter(1.0), 0)).is_none());

        let mut event = metric_event("m", MetricKind::Counter(1.0), 0);
        event.metrics.push(MetricRecord {
            name: intern("m"),
            kind: MetricKind::Gauge(5.0),
            unit: None,
        });

        let passed =
            agg.process(&resource, event).expect("the conflicting gauge should be forwarded");
        assert_eq!(passed.metrics.len(), 1, "the absorbed counter should not also be forwarded");
        assert!(matches!(passed.metrics[0].kind, MetricKind::Gauge(v) if v == 5.0));

        let (_, events) = &agg.flush(100)[0];
        assert_eq!(events.len(), 1);
        assert_eq!(counter_value(kind_of(&events[0])), 2.0, "both counters should have merged");
    }
}
