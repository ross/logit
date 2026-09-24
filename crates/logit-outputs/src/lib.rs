//! Per-protocol output sinks, each implementing `logit_pipeline::Output`.
//!
//! The trait lives in `logit-pipeline`, not here, so the runtime never depends on a concrete
//! protocol (`docs/design/pipeline-graph.md`'s "Crate layout").

mod attrs;
pub mod collectd;
pub mod datadog;
pub mod file;
pub mod graphite;
mod http;
pub mod influxdb;
pub mod logit;
pub mod null;
pub mod otlp;
pub mod prometheus;
pub mod statsd;
pub mod stdio;
pub mod syslog;
mod tls;

pub use logit_pipeline::Output;
