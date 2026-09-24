use crate::{
    AttrMap, ExpHistogram, LogRecord, MetricKind, MetricRecord, Resource, Scope, SpanEvent,
    SpanExt, SpanLink, SpanRecord, Value,
};
use smallvec::SmallVec;
use std::sync::Arc;

/// A batch of events sharing one [`Resource`] and, optionally, one [`Scope`]. Events always travel
/// in batches: per-event channel sends and allocation would dominate the profile at any
/// interesting throughput. See `docs/design/data-model.md`.
#[derive(Debug, Clone, PartialEq)]
pub struct EventBatch {
    pub resource: Arc<Resource>,
    /// The OTLP instrumentation scope the events were reported through, if any.
    pub scope: Option<Arc<Scope>>,
    pub events: Vec<Event>,
}

impl EventBatch {
    /// Approximate heap bytes held by this batch, for admission control of an in-memory delivery
    /// buffer (`docs/adr/buffered-sink-delivery.md`), not allocator accounting.
    ///
    /// An O(events) walk. It's exempt from the exact-size and exact-allocation-count tests
    /// (`tests/type_sizes.rs`, `crates/logit-bench/tests/allocations.rs`); don't add it to them.
    ///
    /// Counts `events.capacity() * size_of::<Event>()`, the dominant term (without it a
    /// numeric-only batch would estimate near zero); then, per event, the heap payload of
    /// attribute values, the log body, span-owned data, and metric records; and the `Arc`-shared
    /// resource and scope once per batch. Scalar `Value`s are inline and count nothing.
    ///
    /// Doesn't count interned [`crate::Symbol`]s (keys, metric names, `event_name`): their bytes
    /// live in the process-wide interner, not in the batch. Resolving them also cost an interner
    /// probe per key per event on every queue push (`docs/design/memory.md` §5 has the measured
    /// cost).
    pub fn estimated_heap_bytes(&self) -> u64 {
        let mut total = self.resource.estimated_heap_bytes()
            + self.scope.as_ref().map(|s| s.estimated_heap_bytes()).unwrap_or(0)
            + (self.events.capacity() * std::mem::size_of::<Event>()) as u64;
        for event in &self.events {
            total += event.estimated_heap_bytes();
        }
        total
    }
}

/// Heap bytes owned by one `SpanRecord` beyond its inline size: `name`, the `events`/`links`
/// backing storage and owned data, and a boxed [`SpanExt`]. The backing-storage terms are usually
/// zero, but a span carrying several span events (a common OTLP shape) needs them.
fn span_heap_bytes(span: &SpanRecord) -> u64 {
    let name = value_heap_bytes(&span.name);
    let events = (span.events.capacity() * std::mem::size_of::<SpanEvent>()) as u64
        + span
            .events
            .iter()
            .map(|e| value_heap_bytes(&e.name) + attr_map_heap_bytes(&e.attributes))
            .sum::<u64>();
    let links = (span.links.capacity() * std::mem::size_of::<SpanLink>()) as u64
        + span
            .links
            .iter()
            .map(|l| {
                attr_map_heap_bytes(&l.attributes)
                    + l.trace_state.as_ref().map(|s| s.len() as u64).unwrap_or(0)
            })
            .sum::<u64>();
    let ext = span
        .ext
        .as_ref()
        .map(|ext| (std::mem::size_of::<SpanExt>() as u64) + span_ext_bytes_heap_bytes(ext))
        .unwrap_or(0);
    name + events + links + ext
}

fn span_ext_bytes_heap_bytes(ext: &SpanExt) -> u64 {
    ext.status_message.as_ref().map(|s| s.len() as u64).unwrap_or(0)
        + ext.trace_state.as_ref().map(|s| s.len() as u64).unwrap_or(0)
}

/// A flat guess (a few hundred bins), not a measurement, for a `Distribution`'s sketch heap.
/// `DDSketch` doesn't expose its bin count cheaply, and walking its stores would make the estimate
/// cost scale with sketch population.
const ESTIMATED_DISTRIBUTION_HEAP_BYTES: u64 = 512;

/// Values only: keys are interned [`crate::Symbol`]s and count nothing (see
/// [`EventBatch::estimated_heap_bytes`]).
pub(crate) fn attr_map_heap_bytes(attrs: &AttrMap) -> u64 {
    attrs.iter().map(|(_, value)| value_heap_bytes(value)).sum()
}

/// Heap bytes owned by one `Value`; `Array`/`Map` recurse into their elements.
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

/// Heap bytes owned by one `MetricRecord`: exemplars plus the kind's payload. Symbols count
/// nothing; `Samples` counts only once spilled past its inline capacity; `Distribution` uses
/// [`ESTIMATED_DISTRIBUTION_HEAP_BYTES`].
fn metric_record_heap_bytes(record: &MetricRecord) -> u64 {
    let exemplars = if record.exemplars.is_empty() {
        0
    } else {
        (record.exemplars.capacity() * std::mem::size_of::<crate::Exemplar>()) as u64
            + record
                .exemplars
                .iter()
                .map(|e| attr_map_heap_bytes(&e.filtered_attributes))
                .sum::<u64>()
    };
    let kind = match &record.kind {
        MetricKind::Sum(_) | MetricKind::Gauge(_) | MetricKind::GaugeDelta(_) => 0,
        MetricKind::Set(hll) => hll.heap_bytes(),
        MetricKind::Samples(s) => {
            if s.values.spilled() {
                (s.values.capacity() * std::mem::size_of::<f64>()) as u64
            } else {
                0
            }
        }
        MetricKind::Distribution(_) => ESTIMATED_DISTRIBUTION_HEAP_BYTES,
        MetricKind::SetMembers(members) => members.iter().map(|m| m.len() as u64).sum(),
        MetricKind::Histogram(h) => (h.buckets.len() * std::mem::size_of::<(f64, u64)>()) as u64,
        MetricKind::ExponentialHistogram(e) => exp_histogram_heap_bytes(e),
        MetricKind::Summary(s) => (s.quantiles.len() * std::mem::size_of::<(f64, f64)>()) as u64,
    };
    exemplars + kind
}

fn exp_histogram_heap_bytes(e: &ExpHistogram) -> u64 {
    let positive = (e.positive.1.capacity() * std::mem::size_of::<u64>()) as u64;
    let negative = (e.negative.1.capacity() * std::mem::size_of::<u64>()) as u64;
    positive + negative
}

/// The metric list on an [`Event`]. Inline capacity 1: the common shape is one metric or none (a
/// log line); multi-metric producers (`kv_metrics`, `collectd_in`) spill past the first.
pub type MetricList = SmallVec<[MetricRecord; 1]>;

/// One event moving through the pipeline: whatever it carries (a log, some metrics, a span,
/// several at once, or none), not a tagged one-of. An access log line is a log and, after
/// `kv_metrics`, a source of derived metrics; a sink emits whatever it finds. At most one log and
/// one span per event. See `docs/adr/multi-payload-events.md` and `docs/design/data-model.md`.
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    /// Unix nanoseconds.
    pub timestamp: i64,
    pub attributes: AttrMap,
    pub log: Option<LogRecord>,
    pub metrics: MetricList,
    pub span: Option<SpanRecord>,
}

impl Event {
    /// An event carrying one metric and nothing else.
    pub fn metric(timestamp: i64, attributes: AttrMap, record: MetricRecord) -> Self {
        Event {
            timestamp,
            attributes,
            log: None,
            metrics: MetricList::from_iter([record]),
            span: None,
        }
    }

    /// An event carrying a log and nothing else.
    pub fn log(timestamp: i64, attributes: AttrMap, record: LogRecord) -> Self {
        Event { timestamp, attributes, log: Some(record), metrics: MetricList::new(), span: None }
    }

    /// An event carrying a span and nothing else.
    pub fn span(timestamp: i64, attributes: AttrMap, record: SpanRecord) -> Self {
        Event { timestamp, attributes, log: None, metrics: MetricList::new(), span: Some(record) }
    }

    /// An event carrying no payload, legal on its own and the base for building a multi-payload
    /// event: `let mut e = Event::empty(ts, attrs); e.metrics.push(record); e.log = Some(log);`
    pub fn empty(timestamp: i64, attributes: AttrMap) -> Self {
        Event { timestamp, attributes, log: None, metrics: MetricList::new(), span: None }
    }

    /// This event's per-event share of [`EventBatch::estimated_heap_bytes`], excluding the
    /// batch-level terms (resource, scope, `Vec<Event>` storage). Summing it and adding those
    /// terms reproduces the batch figure exactly, so `logit_pipeline::BatchAccumulator` can keep a
    /// running total in O(1) per event.
    pub fn estimated_heap_bytes(&self) -> u64 {
        let mut total = attr_map_heap_bytes(&self.attributes);
        if let Some(log) = &self.log {
            // `event_name` is an interned symbol: not counted.
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
        EventBatch { resource, scope: None, events }
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

    fn log_record(message: &str) -> LogRecord {
        LogRecord {
            message: Value::str(message),
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        }
    }

    fn span_record() -> SpanRecord {
        SpanRecord {
            trace_id: [0; 16],
            span_id: [0; 8],
            parent_span_id: None,
            name: Value::str("span name"),
            kind: crate::SpanKind::Internal,
            status: crate::SpanStatus::Unset,
            events: vec![],
            links: vec![],
            end_timestamp: 0,
            flags: 0,
            ext: None,
        }
    }

    #[test]
    fn empty_batch_returns_zero() {
        assert_eq!(default_batch(vec![]).estimated_heap_bytes(), 0);
    }

    #[test]
    fn attribute_only_event_is_nonzero_and_plausible() {
        let attrs = attrs_with(&[("host", Value::str("web-1")), ("env", Value::str("prod"))]);
        let bytes = default_batch(vec![Event::empty(0, attrs)]).estimated_heap_bytes();

        // 9 bytes of value data (keys are interned and count nothing) over the per-event floor.
        let floor = std::mem::size_of::<Event>() as u64;
        assert!(bytes >= floor + 9, "estimate should cover the per-event floor plus the raw string bytes: {bytes} (floor {floor})");
        assert!(
            bytes < floor + 1024,
            "estimate should stay roughly proportional to the input beyond the floor: {bytes}"
        );
    }

    #[test]
    fn a_purely_numeric_metric_event_is_not_undercounted_to_near_zero() {
        // No strings anywhere, but the event still costs its own backing storage.
        let record = MetricRecord::new(
            crate::interner::intern("numeric_only_test_counter"),
            MetricKind::counter(1.0),
        );
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
            dropped_attributes_count: 0,
        };
        let mut span = span_record();
        span.events = vec![span_event];
        let without_span =
            default_batch(vec![Event::empty(0, AttrMap::new())]).estimated_heap_bytes();
        let with_span =
            default_batch(vec![Event::span(0, AttrMap::new(), span)]).estimated_heap_bytes();
        assert!(with_span > without_span, "a span's own name/events/links must add weight, got with_span={with_span} without_span={without_span}");
    }

    #[test]
    fn a_spans_ext_contributes_to_the_estimate() {
        let mut span = span_record();
        let without_ext = default_batch(vec![Event::span(0, AttrMap::new(), span.clone())])
            .estimated_heap_bytes();
        span.ext = Some(Box::new(SpanExt {
            status_message: Some(bytes::Bytes::from_static(b"a status message")),
            trace_state: Some(bytes::Bytes::from_static(b"vendor=value")),
            dropped_attributes_count: 0,
            dropped_events_count: 0,
            dropped_links_count: 0,
        }));
        let with_ext =
            default_batch(vec![Event::span(0, AttrMap::new(), span)]).estimated_heap_bytes();
        assert!(with_ext > without_ext, "a populated SpanExt must add weight");
    }

    #[test]
    fn log_only_event_is_nonzero() {
        let log = log_record("a reasonably long log line for the estimator to see");
        let bytes = default_batch(vec![Event::log(0, AttrMap::new(), log)]).estimated_heap_bytes();
        assert!(bytes > 0);
    }

    #[test]
    fn metrics_only_event_is_nonzero() {
        let record = MetricRecord::new(
            crate::interner::intern("estimated_heap_bytes_test_counter"),
            MetricKind::counter(1.0),
        );
        let bytes =
            default_batch(vec![Event::metric(0, AttrMap::new(), record)]).estimated_heap_bytes();
        // A `Sum` has no heap payload and the name counts nothing: only the per-event floor.
        assert!(bytes > 0);
        assert_eq!(bytes, std::mem::size_of::<Event>() as u64);
    }

    #[test]
    fn distribution_metric_costs_more_than_a_sum_with_the_same_name_length() {
        let counter = MetricRecord::new(
            crate::interner::intern("estimated_heap_bytes_test_metric_a"),
            MetricKind::counter(1.0),
        );
        let mut sketch = DdSketch::new();
        sketch.add(1.0);
        let distribution = MetricRecord::new(
            crate::interner::intern("estimated_heap_bytes_test_metric_b"),
            MetricKind::Distribution(sketch),
        );

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
            let log = log_record("padding so each added event is not entirely free");
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
            ..Resource::default()
        });
        let resource_only_bytes = attr_map_heap_bytes(&resource.attributes);

        let one_event = batch(resource.clone(), vec![Event::empty(0, AttrMap::new())]);
        let two_events = batch(
            resource.clone(),
            vec![Event::empty(0, AttrMap::new()), Event::empty(0, AttrMap::new())],
        );

        assert!(resource_only_bytes > 0, "fixture should carry a nonzero resource cost");
        // A second empty event adds one `Event`-sized slot, never the resource again.
        let event_size = std::mem::size_of::<Event>() as u64;
        assert_eq!(one_event.estimated_heap_bytes(), resource_only_bytes + event_size);
        assert_eq!(two_events.estimated_heap_bytes(), resource_only_bytes + 2 * event_size);
    }

    #[test]
    fn scope_contributes_once_per_batch_not_once_per_event() {
        let scope = Arc::new(Scope {
            name: bytes::Bytes::from_static(b"nginx-otel-module"),
            version: bytes::Bytes::from_static(b"1.0.0"),
            ..Scope::default()
        });
        let scope_only_bytes = scope.estimated_heap_bytes();
        assert!(scope_only_bytes > 0, "fixture should carry a nonzero scope cost");

        let one_event = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: Some(scope.clone()),
            events: vec![Event::empty(0, AttrMap::new())],
        };
        let two_events = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: Some(scope.clone()),
            events: vec![Event::empty(0, AttrMap::new()), Event::empty(0, AttrMap::new())],
        };
        let event_size = std::mem::size_of::<Event>() as u64;
        assert_eq!(one_event.estimated_heap_bytes(), scope_only_bytes + event_size);
        assert_eq!(two_events.estimated_heap_bytes(), scope_only_bytes + 2 * event_size);
    }
}
