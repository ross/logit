//! Per-protocol input implementations. The v0.1 vertical slice target is `statsd`
//! (`docs/OVERVIEW.md`); other protocols implement the same trait incrementally.
//!
//! The `Input` trait itself lives in `logit-pipeline`
//! (`docs/design/pipeline-graph.md`'s "Crate layout" section) -- this crate depends on that one
//! for the trait, not the other way around, so the pipeline runtime never has to know about any
//! concrete protocol.

pub mod internal;
pub mod otlp;
pub mod statsd;
pub mod syslog;
pub mod udp;

pub use logit_pipeline::Input;
