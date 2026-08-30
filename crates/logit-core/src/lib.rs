//! The internal event model shared by every input, transform, and output in `logit`.
//!
//! See `docs/design/data-model.md` for the design rationale. This crate defines the *shape*
//! only -- no I/O, no pipeline, no protocol codecs live here.

pub mod diag;
pub mod interner;
pub mod value;

mod attrs;
mod event;
mod metric;
mod resource;
mod span;

pub use attrs::AttrMap;
pub use diag::Diagnostics;
pub use event::{Event, EventBatch, MetricList};
pub use interner::Symbol;
pub use metric::{DdSketch, HyperLogLog, MetricKind, MetricRecord};
pub use resource::Resource;
pub use span::{SpanEvent, SpanKind, SpanLink, SpanRecord, SpanStatus};
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
}
