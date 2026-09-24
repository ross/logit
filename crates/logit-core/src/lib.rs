//! The internal event model shared by every input, transform, and output in `logit`.
//!
//! See `docs/design/data-model.md` for the design rationale. This crate defines the *shape*
//! only -- no I/O, no pipeline, no protocol codecs live here.

pub mod diag;
pub mod interner;
pub mod provenance;
/// Consistent sampling's compare and frozen key hash. Not re-exported: `sampling::keep` reads
/// better at a call site than a bare `keep`.
pub mod sampling;
pub mod telemetry;
/// `{name}` placeholder templates. Not re-exported: the names are generic enough that
/// `template::Template` reads better than `Template`.
pub mod template;
pub mod time;
pub mod value;

pub mod attrs;
mod event;
mod metric;
mod resource;
mod span;
pub mod trace;

pub use attrs::AttrMap;
pub use diag::Diagnostics;
pub use event::{Event, EventBatch, MetricList};
pub use interner::Symbol;
pub use metric::{
    DdSketch, Exemplar, ExpHistogram, Histogram, HllDecodeError, HyperLogLog, MetricKind,
    MetricRecord, Samples, Sum, Summary, Temporality, SAMPLES_INLINE,
};
pub use provenance::Provenance;
pub use resource::{Resource, Scope};
pub use span::{SpanEvent, SpanExt, SpanKind, SpanLink, SpanRecord, SpanStatus};
pub use telemetry::{
    trace_is_sampled, Registry, SpanGuard, Tag, Telemetry, TelemetryLayer, DEFAULT_SPAN_SAMPLE_RATE,
};
pub use time::{format_rfc3339_utc, parse_decimal_nanos, parse_rfc3339_to_nanos, TimestampError};
pub use trace::{parse_traceparent, random_id_bytes, TraceRef};
pub use value::Value;

/// A normalized, syslog-flavored log severity. Codecs map their native levels onto this.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
}

impl Severity {
    /// Every variant's lowercase name, in variant order: what the Lua API and `stdio_out` render
    /// and accept.
    pub const NAMES: [&'static str; 6] = ["trace", "debug", "info", "warn", "error", "fatal"];

    /// The lowercase name the Lua API and `stdio_out` render this severity as.
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Trace => "trace",
            Severity::Debug => "debug",
            Severity::Info => "info",
            Severity::Warn => "warn",
            Severity::Error => "error",
            Severity::Fatal => "fatal",
        }
    }

    /// The inverse of [`Severity::as_str`]: lowercase only, no case folding or aliases.
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "trace" => Severity::Trace,
            "debug" => Severity::Debug,
            "info" => Severity::Info,
            "warn" => Severity::Warn,
            "error" => Severity::Error,
            "fatal" => Severity::Fatal,
            _ => return None,
        })
    }
}

/// How a log record's body was found; a hint to downstream parsers/transforms, not a guarantee.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyFormat {
    Raw,
    Json,
    Structured,
}

impl BodyFormat {
    /// Every variant's lowercase name, in variant order: what the Lua API and `stdio_out` render
    /// and accept.
    pub const NAMES: [&'static str; 3] = ["raw", "json", "structured"];

    /// The lowercase name the Lua API and `stdio_out` render this format as.
    pub fn as_str(self) -> &'static str {
        match self {
            BodyFormat::Raw => "raw",
            BodyFormat::Json => "json",
            BodyFormat::Structured => "structured",
        }
    }

    /// The inverse of [`BodyFormat::as_str`]: lowercase only, no case folding or aliases.
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "raw" => BodyFormat::Raw,
            "json" => BodyFormat::Json,
            "structured" => BodyFormat::Structured,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogRecord {
    pub message: Value,
    pub severity: Option<Severity>,
    pub body_format: BodyFormat,
    /// The application trace/span this log was emitted under, not `logit`'s pipeline trace. Only
    /// a codec or an operator's config/script sets it (`trace_context`, Lua's
    /// `event.log.trace_id`), never `logit` on its own. See `docs/adr/log-record-trace-context.md`.
    pub trace: Option<TraceRef>,
    /// OTLP's `LogRecord.event_name`: a stable identifier for the kind of event, distinct from
    /// `message`.
    pub event_name: Option<Symbol>,
    /// Unix nanoseconds when a collector observed this log, as distinct from `Event::timestamp`
    /// (when it was generated): OTLP's `observed_time_unix_nano`. `0` means unset.
    pub observed_timestamp: i64,
    pub dropped_attributes_count: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn severity_names_round_trip_in_variant_order() {
        let variants = [
            Severity::Trace,
            Severity::Debug,
            Severity::Info,
            Severity::Warn,
            Severity::Error,
            Severity::Fatal,
        ];
        for (name, v) in Severity::NAMES.iter().zip(variants) {
            assert_eq!(v.as_str(), *name);
            assert_eq!(Severity::from_name(v.as_str()), Some(v));
        }
        assert_eq!(Severity::from_name("Warn"), None);
    }

    #[test]
    fn body_format_names_round_trip_in_variant_order() {
        let variants = [BodyFormat::Raw, BodyFormat::Json, BodyFormat::Structured];
        for (name, v) in BodyFormat::NAMES.iter().zip(variants) {
            assert_eq!(v.as_str(), *name);
            assert_eq!(BodyFormat::from_name(v.as_str()), Some(v));
        }
        assert_eq!(BodyFormat::from_name("Json"), None);
    }
}
