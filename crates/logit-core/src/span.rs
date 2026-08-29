use crate::{AttrMap, Value};

#[derive(Debug, Clone)]
pub struct SpanRecord {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub name: Value,
    pub kind: SpanKind,
    pub status: SpanStatus,
    pub events: Vec<SpanEvent>,
    pub links: Vec<SpanLink>,
    /// Unix nanoseconds. The span's start time is `Event::timestamp`.
    pub end_timestamp: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanKind {
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanStatus {
    Unset,
    Ok,
    Error,
}

#[derive(Debug, Clone)]
pub struct SpanEvent {
    pub timestamp: i64,
    pub name: Value,
    pub attributes: AttrMap,
}

#[derive(Debug, Clone)]
pub struct SpanLink {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub attributes: AttrMap,
}
