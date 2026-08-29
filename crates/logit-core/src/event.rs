use crate::{AttrMap, LogRecord, MetricRecord, Resource, SpanRecord};
use std::sync::Arc;

/// A batch of events sharing one [`Resource`]. Events always travel in batches -- per-event
/// channel sends and allocation would dominate the profile at any interesting throughput. See
/// `docs/design/data-model.md`.
#[derive(Debug, Clone)]
pub struct EventBatch {
    pub resource: Arc<Resource>,
    pub events: Vec<Event>,
}

#[derive(Debug, Clone)]
pub struct Event {
    /// Unix nanoseconds.
    pub timestamp: i64,
    pub attributes: AttrMap,
    pub payload: Payload,
}

#[derive(Debug, Clone)]
pub enum Payload {
    Log(LogRecord),
    Metric(MetricRecord),
    Span(SpanRecord),
}
