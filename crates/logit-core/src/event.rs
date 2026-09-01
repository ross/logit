use crate::{AttrMap, LogRecord, MetricRecord, Resource, SpanRecord};
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
