//! Parsers of an event's log message: text a producer wrote.
//!
//! Each module is the parse core of one transform in `logit-transforms`, which keeps its
//! all-or-nothing merge into `event.attributes`, its diagnostics, and its telemetry, except the
//! per-pair counters `logfmt` and `kv` count inside the parse
//! ([ADR `parsers-live-in-logit-proto`](../../../../docs/adr/parsers-live-in-logit-proto.md)):
//!
//! - [`json`]: the `json` transform (`crates/logit-transforms/src/json.rs`).
//! - [`csv`]: the `csv` transform (`crates/logit-transforms/src/csv.rs`).
//! - [`logfmt`]: the `logfmt` and `kv` transforms (`crates/logit-transforms/src/logfmt.rs`).

pub mod csv;
pub mod json;
pub mod logfmt;
