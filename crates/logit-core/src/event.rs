use crate::interner::Symbol;
use crate::{AttrMap, LogRecord, MetricKind, MetricRecord, Resource, SpanRecord, Value};
use smallvec::SmallVec;
use std::sync::Arc;

/// A batch of events sharing one [`Resource`]. Events always travel in batches -- per-event
/// channel sends and allocation would dominate the profile at any interesting throughput. See
/// `docs/design/data-model.md`.
#[derive(Debug, Clone)]
pub struct EventBatch {
    pub resource: Arc<Resource>,
    pub events: Vec<Event>,
}

impl EventBatch {
    /// Approximate heap bytes held by this batch: attribute keys/values, log bodies, metric
    /// records, and the batch's resource. Deliberately approximate -- an O(events) walk for
    /// admission control (bounding an in-memory delivery buffer, see
    /// `docs/adr/0020-buffered-sink-delivery.md`), NOT an allocator-accounting figure. Exempt from
    /// this crate's exact-size/exact-allocation-count discipline (`tests/type_sizes.rs`,
    /// `crates/logit-bench/tests/allocations.rs`) on purpose -- don't add this to either of those.
    ///
    /// What gets counted, per event: every attribute key's and value's byte length
    /// (`value_heap_bytes` below), the log body's `Value` the same way if `event.log` is `Some`,
    /// and a per-metric-record contribution for `event.metrics` (`metric_record_heap_bytes`
    /// below). `Null`/`Bool`/`I64`/`U64`/`F64`/`Timestamp` values are stored inline in `Value`
    /// with no heap component of their own (see `tests/type_sizes.rs`'s
    /// `value_is_bytes_plus_a_discriminant_word`), so they contribute nothing -- only
    /// `Bytes`/`Str`/`Array`/`Map` do. The batch's `resource` is counted once, not once per event
    /// -- it's `Arc`-shared across every event in the batch, not copied per event.
    pub fn estimated_heap_bytes(&self) -> u64 {
        let mut total = attr_map_heap_bytes(&self.resource.attributes);
        for event in &self.events {
            total += attr_map_heap_bytes(&event.attributes);
            if let Some(log) = &event.log {
                total += value_heap_bytes(&log.message);
            }
            for metric in &event.metrics {
                total += metric_record_heap_bytes(metric);
            }
        }
        total
    }
}

/// A rough per-record stand-in for `MetricKind::Distribution`'s inlined `DDSketch`. The sketch
/// doesn't expose its live bin count cheaply, and walking its internal `Store`s would make this
/// method's cost depend on how populated each sketch is rather than staying a flat O(events) walk
/// -- exactly the kind of precision this estimate deliberately isn't after. Picked as a plausible
/// "typically a few hundred bins across both of `DDSketch`'s `Store`s" guess, not a measurement.
const ESTIMATED_DISTRIBUTION_HEAP_BYTES: u64 = 512;

fn symbol_heap_bytes(symbol: Symbol) -> u64 {
    crate::interner::resolve(symbol).len() as u64
}

fn attr_map_heap_bytes(attrs: &AttrMap) -> u64 {
    attrs.iter().map(|(key, value)| symbol_heap_bytes(key) + value_heap_bytes(value)).sum()
}

/// Heap bytes owned by one `Value`. `Array`/`Map` recurse into their elements -- still an O(the
/// value's own size) walk, not an O(events) one, since a single event's attributes can't nest
/// arbitrarily many other events inside them.
fn value_heap_bytes(value: &Value) -> u64 {
    match value {
        Value::Null
        | Value::Bool(_)
        | Value::I64(_)
        | Value::U64(_)
        | Value::F64(_)
        | Value::Timestamp(_) => 0,
        Value::Bytes(b) | Value::Str(b) => b.len() as u64,
        Value::Array(items) => items.iter().map(value_heap_bytes).sum(),
        Value::Map(map) => attr_map_heap_bytes(map),
    }
}

/// Heap bytes owned by one `MetricRecord`: its name/unit symbols, plus a kind-dependent
/// contribution. `Counter`/`Gauge`/`Set` are effectively free (no heap payload of their own --
/// `HyperLogLog` is still a stub, see `metric.rs`); `Histogram`/`Summary` count their actual
/// bucket/quantile `Vec`s exactly, since those are plain `Vec`s and doing so costs nothing extra;
/// `Distribution` uses [`ESTIMATED_DISTRIBUTION_HEAP_BYTES`] rather than walking the sketch.
fn metric_record_heap_bytes(record: &MetricRecord) -> u64 {
    let symbols = symbol_heap_bytes(record.name) + record.unit.map(symbol_heap_bytes).unwrap_or(0);
    let kind = match &record.kind {
        MetricKind::Counter(_) | MetricKind::Gauge(_) | MetricKind::Set(_) => 0,
        MetricKind::Distribution(_) => ESTIMATED_DISTRIBUTION_HEAP_BYTES,
        MetricKind::Histogram { buckets } => {
            (buckets.len() * std::mem::size_of::<(f64, u64)>()) as u64
        }
        MetricKind::Summary { quantiles } => {
            (quantiles.len() * std::mem::size_of::<(f64, f64)>()) as u64
        }
    };
    symbols + kind
}

/// The metric list on an [`Event`]. Inline capacity 1: the overwhelmingly common shape is a
/// single metric (statsd) or none at all (a log line), and `kv_metrics` -- the first real
/// multi-metric producer -- spills to the heap only past the first.
pub type MetricList = SmallVec<[MetricRecord; 1]>;

/// One event moving through the pipeline. An event is *whatever it carries* -- a log, some
/// metrics, a span, several of those at once, or (legally) none at all -- not a tagged one-of.
/// The same access log line is both a log and, once a transform like `kv_metrics` has run, a
/// source of several derived metrics; a sink emits whatever it finds. Two logs on one event is
/// unrepresentable by construction, since `log` is a single `Option`, not a list. See
/// `docs/adr/0012-multi-payload-events.md` and `docs/design/data-model.md`.
#[derive(Debug, Clone)]
pub struct Event {
    /// Unix nanoseconds.
    pub timestamp: i64,
    pub attributes: AttrMap,
    pub log: Option<LogRecord>,
    pub metrics: MetricList,
    pub span: Option<SpanRecord>,
}

impl Event {
    /// An event carrying one metric and nothing else -- the shape every metrics-only input
    /// (statsd today) produces.
    pub fn metric(timestamp: i64, attributes: AttrMap, record: MetricRecord) -> Self {
        Event {
            timestamp,
            attributes,
            log: None,
            metrics: MetricList::from_iter([record]),
            span: None,
        }
    }

    /// An event carrying a log body and nothing else -- the shape every log-only input
    /// (`syslog_in`, once implemented) produces.
    pub fn log(timestamp: i64, attributes: AttrMap, record: LogRecord) -> Self {
        Event { timestamp, attributes, log: Some(record), metrics: MetricList::new(), span: None }
    }

    /// An event carrying a span and nothing else.
    pub fn span(timestamp: i64, attributes: AttrMap, record: SpanRecord) -> Self {
        Event { timestamp, attributes, log: None, metrics: MetricList::new(), span: Some(record) }
    }

    /// An event carrying no payload at all -- legal and representable, unlike under the old
    /// one-of model. The base for building a multi-payload event by hand:
    /// `let mut e = Event::empty(ts, attrs); e.metrics.push(record); e.log = Some(log);`
    pub fn empty(timestamp: i64, attributes: AttrMap) -> Self {
        Event { timestamp, attributes, log: None, metrics: MetricList::new(), span: None }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BodyFormat, DdSketch};

    fn batch(resource: Arc<Resource>, events: Vec<Event>) -> EventBatch {
        EventBatch { resource, events }
    }

    fn default_batch(events: Vec<Event>) -> EventBatch {
        batch(Arc::new(Resource::default()), events)
    }

    fn attrs_with(pairs: &[(&str, Value)]) -> AttrMap {
        let mut attrs = AttrMap::new();
        for (key, value) in pairs {
            attrs.insert(key, value.clone());
        }
        attrs
    }

    #[test]
    fn empty_batch_returns_zero() {
        assert_eq!(default_batch(vec![]).estimated_heap_bytes(), 0);
    }

    #[test]
    fn attribute_only_event_is_nonzero_and_plausible() {
        let attrs = attrs_with(&[("host", Value::str("web-1")), ("env", Value::str("prod"))]);
        let bytes = default_batch(vec![Event::empty(0, attrs)]).estimated_heap_bytes();

        // "host" + "web-1" + "env" + "prod" is 16 bytes of actual string data; the estimate
        // should at least cover that and stay in the same ballpark, not blow up to kilobytes.
        assert!(bytes >= 16, "estimate should cover the raw string bytes: {bytes}");
        assert!(bytes < 1024, "estimate should stay roughly proportional to the input: {bytes}");
    }

    #[test]
    fn log_only_event_is_nonzero() {
        let log = LogRecord {
            message: Value::str("a reasonably long log line for the estimator to see"),
            severity: None,
            body_format: BodyFormat::Raw,
        };
        let bytes = default_batch(vec![Event::log(0, AttrMap::new(), log)]).estimated_heap_bytes();
        assert!(bytes > 0);
    }

    #[test]
    fn metrics_only_event_is_nonzero() {
        let record = MetricRecord {
            name: crate::interner::intern("estimated_heap_bytes_test_counter"),
            kind: MetricKind::Counter(1.0),
            unit: None,
        };
        let bytes =
            default_batch(vec![Event::metric(0, AttrMap::new(), record)]).estimated_heap_bytes();
        // A `Counter` has no heap payload of its own, but the metric's name symbol still counts.
        assert!(bytes > 0);
    }

    #[test]
    fn distribution_metric_costs_more_than_a_counter_with_the_same_name_length() {
        let counter = MetricRecord {
            name: crate::interner::intern("estimated_heap_bytes_test_metric_a"),
            kind: MetricKind::Counter(1.0),
            unit: None,
        };
        let mut sketch = DdSketch::new();
        sketch.add(1.0);
        let distribution = MetricRecord {
            name: crate::interner::intern("estimated_heap_bytes_test_metric_b"),
            kind: MetricKind::Distribution(sketch),
            unit: None,
        };

        let counter_bytes =
            default_batch(vec![Event::metric(0, AttrMap::new(), counter)]).estimated_heap_bytes();
        let distribution_bytes =
            default_batch(vec![Event::metric(0, AttrMap::new(), distribution)])
                .estimated_heap_bytes();

        assert!(distribution_bytes > counter_bytes);
    }

    #[test]
    fn growth_is_monotonic_as_events_are_added() {
        let mut b = default_batch(vec![]);
        let mut previous = b.estimated_heap_bytes();
        for i in 0..5 {
            let attrs = attrs_with(&[("i", Value::from(i))]);
            let log = LogRecord {
                message: Value::str("padding so each added event is not entirely free"),
                severity: None,
                body_format: BodyFormat::Raw,
            };
            b.events.push(Event::log(i, attrs, log));

            let next = b.estimated_heap_bytes();
            assert!(next >= previous, "adding an event should never decrease the estimate");
            assert!(next > previous, "this event carries real payload, so it should add weight");
            previous = next;
        }
    }

    #[test]
    fn resource_contributes_once_per_batch_not_once_per_event() {
        let resource = Arc::new(Resource {
            attributes: attrs_with(&[("datacenter", Value::str("us-east-1"))]),
        });
        let resource_only_bytes = attr_map_heap_bytes(&resource.attributes);

        let one_event = batch(resource.clone(), vec![Event::empty(0, AttrMap::new())]);
        let two_events = batch(
            resource.clone(),
            vec![Event::empty(0, AttrMap::new()), Event::empty(0, AttrMap::new())],
        );

        assert!(resource_only_bytes > 0, "fixture should carry a nonzero resource cost");
        // An empty event with no attributes/log/metrics adds nothing of its own, so both batches
        // should equal exactly the resource's contribution, counted once -- not twice.
        assert_eq!(one_event.estimated_heap_bytes(), resource_only_bytes);
        assert_eq!(two_events.estimated_heap_bytes(), resource_only_bytes);
    }
}
