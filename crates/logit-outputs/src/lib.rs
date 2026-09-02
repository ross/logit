//! Per-protocol output sinks. The v0.1 vertical slice target is `influxdb`
//! (`docs/OVERVIEW.md`); other protocols implement the same trait incrementally.
//!
//! The `Output` trait itself lives in `logit-pipeline`
//! (`docs/design/pipeline-graph.md`'s "Crate layout" section) -- see `logit-inputs`'s crate doc
//! comment for the same reasoning.

pub mod influxdb;
pub mod stdio;
pub mod syslog;

pub use logit_pipeline::Output;
