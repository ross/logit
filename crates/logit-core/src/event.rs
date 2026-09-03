use crate::interner::Symbol;
use crate::{
    AttrMap, LogRecord, MetricKind, MetricRecord, Resource, SpanEvent, SpanLink, SpanRecord, Value,
};
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
    /// Approximate heap bytes held by this batch: the `Vec<Event>` backing allocation itself,
    /// attribute keys/values, log bodies, span-owned data, metric records, and the batch's
    /// resource. Deliberately approximate -- an O(events) walk for admission control (bounding an
    /// in-memory delivery buffer, see `docs/adr/0021-buffered-sink-delivery.md`), NOT an
    /// allocator-accounting figure. Exempt from this crate's exact-size/exact-allocation-count
    /// discipline (`tests/type_sizes.rs`, `crates/logit-bench/tests/allocations.rs`) on purpose --
    /// don't add this to either of those.
    ///
    /// What gets counted: the dominant term first -- `events.capacity() * size_of::<Event>()`,
    /// the batch's own backing storage, which every event pays (776 bytes each, `memory.md` §1)
    /// *before* any nested heap payload -- a batch of numeric-only metrics with no string
    /// attributes would otherwise estimate close to zero despite genuinely holding hundreds of
    /// bytes per event. Then, per event: every attribute key's and value's byte length
    /// (`value_heap_bytes` below), the log body's `Value` the same way if `event.log` is `Some`,
    /// every span-owned `Value`/`AttrMap`/nested `Vec` if `event.span` is `Some`
    /// (`span_heap_bytes` below), and a per-metric-record contribution for `event.metrics`
    /// (`metric_record_heap_bytes` below). `Null`/`Bool`/`I64`/`U64`/`F64`/`Timestamp` values are
    /// stored inline in `Value` with no heap component of their own (see `tests/type_sizes.rs`'s
    /// `value_is_bytes_plus_a_discriminant_word`), so they contribute nothing -- only
    /// `Bytes`/`Str`/`Array`/`Map` do. The batch's `resource` is counted once, not once per event
    /// -- it's `Arc`-shared across every event in the batch, not copied per event.
    pub fn estimated_heap_bytes(&self) -> u64 {
        let mut total = self.resource.estimated_heap_bytes()
            + (self.events.capacity() * std::mem::size_of::<Event>()) as u64;
        for event in &self.events {
            total += event.estimated_heap_bytes();
        }
        total
    }
}

/// Heap bytes owned by one `SpanRecord` beyond its own inline 136 bytes (`memory.md` §1): its
/// `name`, plus every `SpanEvent`/`SpanLink`'s own `Vec` backing storage and owned data. Unlike
/// `Event`'s outer `Vec` (which every batch has regardless of shape), a span's `events`/`links`
/// are typically empty, so their `capacity() * size_of::<T>()` terms are usually zero in
/// practice -- included anyway since a span that *does* carry several span-events (a common OTLP
/// shape) would otherwise be undercounted the same way the outer batch was before this fix.
fn span_heap_bytes(span: &SpanRecord) -> u64 {
    let name = value_heap_bytes(&span.name);
    let events = (span.events.capacity() * std::mem::size_of::<SpanEvent>()) as u64
        + span
            .events
            .iter()
            .map(|e| value_heap_bytes(&e.name) + attr_map_heap_bytes(&e.attributes))
            .sum::<u64>();
    let links = (span.links.capacity() * std::mem::size_of::<SpanLink>()) as u64
        + span.links.iter().map(|l| attr_map_heap_bytes(&l.attributes)).sum::<u64>();
    name + events + links
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

pub(crate) fn attr_map_heap_bytes(attrs: &AttrMap) -> u64 {
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
/// contribution. `Counter`/`Gauge`/`GaugeDelta`/`Set` are effectively free (no heap payload of
/// their own -- `HyperLogLog` is still a stub, see `metric.rs`); `Histogram`/`Summary` count their
/// actual bucket/quantile `Vec`s exactly, since those are plain `Vec`s and doing so costs nothing
/// extra; `Distribution` uses [`ESTIMATED_DISTRIBUTION_HEAP_BYTES`] rather than walking the sketch.
fn metric_record_heap_bytes(record: &MetricRecord) -> u64 {
    let symbols = symbol_heap_bytes(record.name) + record.unit.map(symbol_heap_bytes).unwrap_or(0);
    let kind = match &record.kind {
        MetricKind::Counter(_)
        | MetricKind::Gauge(_)
        | MetricKind::GaugeDelta(_)
        | MetricKind::Set(_) => 0,
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

    /// This event's own contribution to [`EventBatch::estimated_heap_bytes`] -- everything that
    /// formula counts *per event* (attributes, log body, span-owned data, metric records), minus
    /// the batch-level terms (the resource, counted once via [`Resource::estimated_heap_bytes`],
    /// and the `Vec<Event>` backing storage itself). Exposed so a caller accumulating events
    /// incrementally (`logit_pipeline::BatchAccumulator`) can track a running total in O(1) per
    /// event as they arrive, rather than re-walking every event held so far on every call --
    /// summing this over a set of events and adding the batch-level terms once reproduces
    /// `estimated_heap_bytes` exactly, by construction.
    pub fn estimated_heap_bytes(&self) -> u64 {
        let mut total = attr_map_heap_bytes(&self.attributes);
        if let Some(log) = &self.log {
            total += value_heap_bytes(&log.message);
        }
        if let Some(span) = &self.span {
            total += span_heap_bytes(span);
        }
        for metric in &self.metrics {
            total += metric_record_heap_bytes(metric);
        }
        total
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

        // "host" + "web-1" + "env" + "prod" is 16 bytes of actual string data, on top of the one
        // event's own Vec<Event> backing-storage floor (size_of::<Event>()) every event pays
        // regardless of shape -- the estimate should cover at least both, and stay in the same
        // ballpark rather than blowing up to kilobytes.
        let floor = std::mem::size_of::<Event>() as u64;
        assert!(bytes >= floor + 16, "estimate should cover the per-event floor plus the raw string bytes: {bytes} (floor {floor})");
        assert!(
            bytes < floor + 1024,
            "estimate should stay roughly proportional to the input beyond the floor: {bytes}"
        );
    }

    #[test]
    fn a_purely_numeric_metric_event_is_not_undercounted_to_near_zero() {
        // The exact scenario a review finding named: a numeric-only event (no strings anywhere)
        // used to estimate at only a few bytes (the metric name symbol's length) despite Event
        // itself costing 776 bytes before any nested allocation -- the dominant term for a
        // metrics-heavy batch. Must now be at least one Event's worth of backing storage.
        let record = MetricRecord {
            name: crate::interner::intern("numeric_only_test_counter"),
            kind: MetricKind::Counter(1.0),
            unit: None,
        };
        let bytes =
            default_batch(vec![Event::metric(0, AttrMap::new(), record)]).estimated_heap_bytes();
        let floor = std::mem::size_of::<Event>() as u64;
        assert!(bytes >= floor, "a numeric-only event must count at least its own Event-sized backing storage, got {bytes}, floor {floor}");
    }

    #[test]
    fn a_spans_own_data_contributes_to_the_estimate() {
        let span_event = SpanEvent {
            timestamp: 0,
            name: Value::str("a span event name long enough to matter"),
            attributes: attrs_with(&[("k", Value::str("a fairly long attribute value here"))]),
        };
        let span = SpanRecord {
            trace_id: [0; 16],
            span_id: [0; 8],
            parent_span_id: None,
            name: Value::str("span name"),
            kind: crate::SpanKind::Internal,
            status: crate::SpanStatus::Unset,
            events: vec![span_event],
            links: vec![],
            end_timestamp: 0,
        };
        let without_span =
            default_batch(vec![Event::empty(0, AttrMap::new())]).estimated_heap_bytes();
        let with_span =
            default_batch(vec![Event::span(0, AttrMap::new(), span)]).estimated_heap_bytes();
        assert!(with_span > without_span, "a span's own name/events/links must add weight, got with_span={with_span} without_span={without_span}");
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
        // An empty event with no attributes/log/metrics adds nothing of its own beyond the
        // Vec<Event> backing storage every event pays regardless of shape -- so the marginal cost
        // of a second event is exactly one more Event-sized slot, never the resource's cost
        // repeated (that's what "counted once, not twice" actually means once the per-event floor
        // exists: the resource term is the same additive constant in both expressions below,
        // not something that scales with event count the way the floor deliberately does).
        let event_size = std::mem::size_of::<Event>() as u64;
        assert_eq!(one_event.estimated_heap_bytes(), resource_only_bytes + event_size);
        assert_eq!(two_events.estimated_heap_bytes(), resource_only_bytes + 2 * event_size);
    }
}
