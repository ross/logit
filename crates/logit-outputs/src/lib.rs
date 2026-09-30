//! Per-protocol output sinks, each implementing `logit_pipeline::Output`.
//!
//! The trait lives in `logit-pipeline`, not here, so the runtime never depends on a concrete
//! protocol (`docs/design/pipeline-graph.md`'s "Crate layout").

mod accounting;
mod attrs;
pub mod collectd;
pub mod datadog;
pub mod datadog_trace;
mod datagram;
pub mod file;
pub mod graphite;
mod http;
pub mod human;
pub mod influxdb;
pub mod logit;
pub mod ndjson;
pub mod null;
pub mod otlp;
pub mod prometheus;
pub mod splunk;
pub mod statsd;
pub mod stdio;
mod stream;
#[cfg(test)]
mod stream_pins;
pub mod syslog;
#[cfg(test)]
pub(crate) mod test_support;
mod tls;

pub use logit_pipeline::Output;

/// Counts `logit.output.requests` for one returned attempt, tagged with its fault class
/// (`ok|clean|ambiguous|permanent`; ADR `sink-send-path-and-attempt-accounting`, decision 4).
/// The pooled-stream driver, `logit_out`, and the datagram sinks all count through it, so every
/// transport of every one of them has one vocabulary.
pub(crate) fn count_request<T>(telemetry: &logit_core::Telemetry, result: &anyhow::Result<T>) {
    let class = match result {
        Ok(_) => "ok",
        Err(err) => match logit_pipeline::classify(err) {
            logit_pipeline::Fault::Clean => "clean",
            logit_pipeline::Fault::Ambiguous => "ambiguous",
            logit_pipeline::Fault::Permanent => "permanent",
        },
    };
    telemetry.count("logit.output.requests", 1.0, &[("class", class)]);
}
