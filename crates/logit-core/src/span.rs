use crate::{AttrMap, Value};
use bytes::Bytes;

#[derive(Debug, Clone, PartialEq)]
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
    /// W3C trace flags (low 8 bits of OTLP's own `Span.flags`); bit 0 is the `SAMPLED` flag. `0`
    /// means unset.
    pub flags: u32,
    /// Boxed: populated only on an error span or one carrying a `trace_state`/dropped counts, so
    /// the overwhelmingly common span (no status message, no `tracestate`) doesn't pay for these
    /// fields inline.
    pub ext: Option<Box<SpanExt>>,
}

/// The rarely-populated half of a span's OTLP fidelity -- boxed out of [`SpanRecord`] itself, see
/// its `ext` field's doc comment.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SpanExt {
    pub status_message: Option<Bytes>,
    pub trace_state: Option<Bytes>,
    pub dropped_attributes_count: u32,
    pub dropped_events_count: u32,
    pub dropped_links_count: u32,
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

#[derive(Debug, Clone, PartialEq)]
pub struct SpanEvent {
    pub timestamp: i64,
    pub name: Value,
    pub attributes: AttrMap,
    pub dropped_attributes_count: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SpanLink {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub attributes: AttrMap,
    pub flags: u32,
    pub trace_state: Option<Bytes>,
    pub dropped_attributes_count: u32,
}
