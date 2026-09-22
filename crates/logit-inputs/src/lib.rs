//! Per-protocol input implementations. The v0.1 vertical slice target is `statsd`
//! (`docs/OVERVIEW.md`); other protocols implement the same trait incrementally.
//!
//! The `Input` trait itself lives in `logit-pipeline`
//! (`docs/design/pipeline-graph.md`'s "Crate layout" section) -- this crate depends on that one
//! for the trait, not the other way around, so the pipeline runtime never has to know about any
//! concrete protocol.

// This crate carries two of the codebase's three raw-`libc` production `unsafe` call sites
// (`udp.rs`'s `recvmmsg`, `tail/watch.rs`'s hand-rolled inotify) -- see
// `docs/adr/out-of-ci-unsafe-verification.md`. Denied at the crate level rather than promoted to
// `[workspace.lints]`: a workspace-wide `unsafe_op_in_unsafe_fn` deny has real fallout in
// `logit-bench`'s and `logit-perf`'s own `unsafe` (dev-only crates with no stake in this
// cluster's review bar, and no `SAFETY:` comment immediately preceding every unsafe operation
// today), so the bar is drawn narrowly around the two crates that actually hold this cluster's
// code instead.
#![deny(unsafe_op_in_unsafe_fn)]
#![deny(clippy::undocumented_unsafe_blocks)]

pub mod collectd;
pub mod docker;
pub mod generate;
pub mod graphite;
mod http;
pub mod internal;
pub mod logit;
pub mod otlp;
pub mod prometheus;
pub mod statsd;
pub mod syslog;
pub mod tail;
pub mod tcp;
mod tls;
pub mod udp;

pub use logit_pipeline::Input;
