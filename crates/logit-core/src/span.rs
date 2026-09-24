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
    /// OTLP's `Span.flags`, carried verbatim: the low 8 bits are the W3C trace flags (bit 0 =
    /// `SAMPLED`), the bits above them OTLP's own (`CONTEXT_HAS_IS_REMOTE`/`CONTEXT_IS_REMOTE`).
    /// `0` means unset.
    pub flags: u32,
    /// Boxed: only a span with a status message, `trace_state`, or dropped counts populates it, so
    /// the common span doesn't pay for these fields inline.
    pub ext: Option<Box<SpanExt>>,
}

/// The rarely populated OTLP span fields, boxed out of [`SpanRecord`].
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

impl SpanKind {
    /// Every variant's lowercase name, in variant order -- the names the Lua API and `stdio_out`
    /// render and accept.
    pub const NAMES: [&'static str; 5] = ["internal", "server", "client", "producer", "consumer"];

    /// The lowercase name the Lua API and `stdio_out` render this kind as.
    pub fn as_str(self) -> &'static str {
        match self {
            SpanKind::Internal => "internal",
            SpanKind::Server => "server",
            SpanKind::Client => "client",
            SpanKind::Producer => "producer",
            SpanKind::Consumer => "consumer",
        }
    }

    /// The inverse of [`SpanKind::as_str`]: an exact lowercase match, no case folding or aliases.
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "internal" => SpanKind::Internal,
            "server" => SpanKind::Server,
            "client" => SpanKind::Client,
            "producer" => SpanKind::Producer,
            "consumer" => SpanKind::Consumer,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanStatus {
    Unset,
    Ok,
    Error,
}

impl SpanStatus {
    /// Every variant's lowercase name, in variant order -- the names the Lua API and `stdio_out`
    /// render and accept.
    pub const NAMES: [&'static str; 3] = ["unset", "ok", "error"];

    /// The lowercase name the Lua API and `stdio_out` render this status as.
    pub fn as_str(self) -> &'static str {
        match self {
            SpanStatus::Unset => "unset",
            SpanStatus::Ok => "ok",
            SpanStatus::Error => "error",
        }
    }

    /// The inverse of [`SpanStatus::as_str`]: an exact lowercase match, no case folding or
    /// aliases.
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "unset" => SpanStatus::Unset,
            "ok" => SpanStatus::Ok,
            "error" => SpanStatus::Error,
            _ => return None,
        })
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn span_kind_names_round_trip_in_variant_order() {
        let variants = [
            SpanKind::Internal,
            SpanKind::Server,
            SpanKind::Client,
            SpanKind::Producer,
            SpanKind::Consumer,
        ];
        for (name, v) in SpanKind::NAMES.iter().zip(variants) {
            assert_eq!(v.as_str(), *name);
            assert_eq!(SpanKind::from_name(v.as_str()), Some(v));
        }
        assert_eq!(SpanKind::from_name("Server"), None);
    }

    #[test]
    fn span_status_names_round_trip_in_variant_order() {
        let variants = [SpanStatus::Unset, SpanStatus::Ok, SpanStatus::Error];
        for (name, v) in SpanStatus::NAMES.iter().zip(variants) {
            assert_eq!(v.as_str(), *name);
            assert_eq!(SpanStatus::from_name(v.as_str()), Some(v));
        }
        assert_eq!(SpanStatus::from_name("Ok"), None);
    }
}
