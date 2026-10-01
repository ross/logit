//! Per-protocol input implementations: the `Input` behind every `*_in` component kind, and
//! `internal`.
//!
//! The `Input` trait lives in `logit-pipeline` (`docs/design/pipeline-graph.md`, "Crate layout"):
//! this crate depends on that one, not the other way around, so the pipeline runtime never knows
//! about a concrete protocol.

// This crate holds two of the codebase's three raw-`libc` production `unsafe` call sites
// (`udp.rs`'s `recvmmsg`, `tail/watch.rs`'s inotify; `docs/adr/out-of-ci-unsafe-verification.md`).
// Denied per crate, not in `[workspace.lints]`, because a workspace-wide deny would fire on
// `logit-bench`'s and `logit-perf`'s dev-only `unsafe`, which don't hold this bar (a `SAFETY:`
// comment before every unsafe operation).
#![deny(unsafe_op_in_unsafe_fn)]
#![deny(clippy::undocumented_unsafe_blocks)]

pub mod collectd;
pub mod datadog;
pub mod datadog_trace;
pub mod docker;
#[cfg(test)]
mod docker_verification;
pub mod generate;
pub mod graphite;
mod http;
pub mod internal;
mod listener;
pub mod logit;
pub mod otlp;
mod procstat;
pub mod prometheus;
pub mod splunk;
pub mod statsd;
pub mod syslog;
pub mod tail;
pub mod tcp;
mod tls;
pub mod udp;
mod unix;
mod zstd;

pub use logit_pipeline::Input;

/// The builder default for every stream listener's connection cap: the TCP driver's listeners
/// (`syslog_in`, `graphite_in`, `statsd_in`), `logit_in`, and the HTTP listeners. A connection
/// arriving past the cap is rejected, never queued. `logit_config::default_max_connections`
/// mirrors this number by hand.
pub(crate) const DEFAULT_MAX_CONNECTIONS: usize = 1024;

/// This process's soft `Max open files` limit, or `None` when it is unlimited or can't be read
/// (off Linux, or `/proc/self/limits` unreadable). The ceiling every listener's connection cap
/// shares, since each open connection holds a file descriptor.
pub fn open_files_limit() -> Option<u64> {
    procstat::open_files_limit().ok().flatten()
}
