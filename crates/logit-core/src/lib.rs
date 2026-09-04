//! The internal event model shared by every input, transform, and output in `logit`.
//!
//! See `docs/design/data-model.md` for the design rationale. This crate defines the *shape*
//! only -- no I/O, no pipeline, no protocol codecs live here.

pub mod diag;
pub mod interner;
pub mod telemetry;
pub mod time;
pub mod value;

mod attrs;
mod event;
mod metric;
mod resource;
mod span;
pub mod trace;

pub use attrs::AttrMap;
pub use diag::Diagnostics;
pub use event::{Event, EventBatch, MetricList};
pub use interner::Symbol;
pub use metric::{DdSketch, HyperLogLog, MetricKind, MetricRecord};
pub use resource::Resource;
pub use span::{SpanEvent, SpanKind, SpanLink, SpanRecord, SpanStatus};
pub use telemetry::{
    trace_is_sampled, Registry, SpanGuard, Tag, Telemetry, DEFAULT_SPAN_SAMPLE_RATE,
};
pub use time::format_rfc3339_utc;
pub use trace::TraceRef;
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

/// How a log record's body was found; a hint to downstream parsers/transforms, not a guarantee.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyFormat {
    Raw,
    Json,
    Structured,
}

#[derive(Debug, Clone)]
pub struct LogRecord {
    pub message: Value,
    pub severity: Option<Severity>,
    pub body_format: BodyFormat,
    /// The application trace/span this log line was emitted under, if any -- distinct from
    /// `logit`'s own pipeline trace context. `logit`'s code never sets this on its own; it only
    /// ever carries what a codec decoded off the wire or what an operator's config/script
    /// explicitly set (`ComponentKind::TraceContext`, `event.log.trace_id` in Lua). See
    /// `docs/adr/log-record-trace-context.md`.
    pub trace: Option<TraceRef>,
}
