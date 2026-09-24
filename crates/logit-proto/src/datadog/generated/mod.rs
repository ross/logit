//! Hand-written module tree wrapping the single `prost-build` output file
//! (`datadog.agentpayload.rs`) -- never regenerated itself, `script/protogen` only ever
//! (re)writes that one file. See
//! [ADR `committed-pregenerated-otlp-protobuf`](../../../../../docs/adr/committed-pregenerated-otlp-protobuf.md)
//! (this crate's OTLP family is where that ADR is written up; this third family follows the same
//! scheme) and `crates/logit-proto/proto/README.md`'s "Vendored Datadog agent-payload" section for
//! the pinned tag/commit.
//!
//! Unlike OTLP's five interdependent packages and Prometheus's two sibling files, the vendored
//! `agent_payload.proto` is one file declaring one package (`datadog.agentpayload`), so there is
//! no cross-file package merging and no `super::`-relative path to preserve by nesting -- a single
//! flat `agentpayload` module is enough (`crate::datadog::generated::agentpayload::MetricPayload`,
//! `agentpayload::SketchPayload`, ...).
//!
//! `#[path = ...] mod ...;` (a real file-module), not `mod ... { include!(...); }` -- inner
//! attributes (`#![allow(clippy::all)]` etc., at the top of the generated file) are only valid at
//! the true start of a file module; `include!`'s textual splice doesn't count as one, and rustc
//! rejects them there.
//!
//! `#[rustfmt::skip]` on the generated file's module declaration -- not `#![rustfmt::skip]` inside
//! the generated file itself -- is what keeps `script/format`/`script/format --check` off this
//! generated code: rustfmt honors `#[rustfmt::skip]` on a module *declaration* by skipping the
//! file it names entirely, and the outer form is stable (the inner `#![rustfmt::skip]` form the
//! generated file would otherwise want at its own top is nightly-only, rust-lang/rust#54726).

/// The Agent metrics-intake payload types (`MetricPayload`/`metric_payload::MetricSeries`,
/// `SketchPayload`/`sketch_payload::Sketch`, `EventsPayload`, `CommonMetadata`, `Metadata`,
/// `Origin`, ...), from `agent_payload.proto`.
#[rustfmt::skip]
#[path = "datadog.agentpayload.rs"]
pub mod agentpayload;
