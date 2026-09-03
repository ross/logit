---
created: 2026-09-02
updated: 2026-09-02
---

# Committed, pre-generated OTLP protobuf types; no `protoc` in any build path

## Status
Accepted

## Context

[ADR `native-wire-format-with-otlp-bridge`](native-wire-format-with-otlp-bridge.md) settled OTLP as a first-class ingest/egress
codec, not the internal transport. Building that codec means getting real, correct Rust types for
OTLP's `common`/`resource`/`logs`/`metrics`/`trace` protobuf messages — roughly 30 message types,
several with nested `oneof`s and packed-varint repeated fields (`metrics.proto`'s
`ExponentialHistogramDataPoint` alone). [ADR `containerized-development`](containerized-development.md) says nothing
beyond Docker/Compose is assumed on the host, and — more specifically — that a build should not
need a toolchain component (like `protoc`) contributors and CI don't already carry for every other
crate in this workspace.

Three ways to get those types, all considered:

1. Depend on the `opentelemetry-proto` crate.
2. Generate them at build time via a `build.rs` calling `prost-build` (which itself can shell out
   to `protoc`, or use `prost-build`'s `protoc-bin-vendored`-style bundled binary).
3. Hand-write them.

## Decision

**Generate once with `prost-build`, offline, in a throwaway container carrying `protoc`
(`tools/protogen/`, driven by `script/protogen`), and commit the generated `.rs` output.**
`crates/logit-proto/src/otlp/generated/*.v1.rs` is prost-build's direct output (never hand-edited),
committed alongside the `.proto` sources it came from
(`crates/logit-proto/proto/opentelemetry/proto/...`, vendored verbatim from
`open-telemetry/opentelemetry-proto` at a pinned tag — see `crates/logit-proto/proto/README.md` for
the exact tag/commit). `script/protogen` is a new `script/*` entry (ADR `scripts-to-rule-them-all`'s convention) that
re-runs the generator and overwrites those files; it is **not** part of `script/cibuild` — running
it is a deliberate, reviewed act (regenerate, read the diff, commit), not a CI-enforced drift check,
because a drift check would mean putting `protoc` in the CI image, exactly what this ADR avoids.

`tools/protogen/` is its own empty `[workspace]` (`tools/protogen/Cargo.toml`), never a member of
the root workspace and never a dependency of any shipped crate — `cargo build --workspace` from the
repo root never touches it, and neither `prost-build` nor `protoc` ever enter the release image's or
CI's dependency graph. The only new *runtime* dependency this PR adds is `prost = "0.14"` (added to
`[workspace.dependencies]` and to `crates/logit-proto/Cargo.toml`) — OTLP's protos use no
`google.protobuf` well-known types, so `prost-types` isn't needed either.

Each generated file gets `#![allow(clippy::all)]`/`#![allow(rustdoc::all)]` at its own top (both
valid there: `allow` is a builtin attribute, just given a tool-lint-path argument) so
`script/lint`/`script/format --check` don't fight vendored code they didn't write. Formatting is
skipped differently: `#![rustfmt::skip]` as an *inner* attribute is nightly-only
(rust-lang/rust#54726), so `crates/logit-proto/src/otlp/generated/mod.rs` — the hand-written module
tree wrapping the five generated files, matching the OTLP proto package hierarchy so prost's
`super::`-relative cross-package field types resolve — carries the equivalent *outer*
`#[rustfmt::skip]` on each file's `pub mod v1;` declaration instead, which rustfmt honors by
skipping the whole file it names. `crates/logit-proto`'s `Cargo.toml` also sets `[lib] doctest =
false`: vendored upstream comments include prose examples (e.g. `Span.attributes`'s doc comment)
that rustdoc reads as runnable code blocks and tries to execute as doctests; `#![allow(rustdoc::all)]`
silences the *lint* but not doctest *execution*, and this crate has no hand-written doctest to lose.

Messages only — no service stubs. `logit` encodes/decodes the plain `TracesData`/`LogsData`/
`MetricsData` top-level messages (`{ repeated Resource*Signal* = 1; }`), not
`Export*ServiceRequest`/`Response`; the two are wire-identical for every field this codec touches,
so PR3's hand-rolled gRPC/HTTP transport (its own ADR) can send/receive these bytes as real OTLP
requests without this crate ever generating, or depending on, the three `collector/*_service.proto`
files (vendored anyway, for provenance and for PR3 to reference directly).

## Alternatives considered

- **The `opentelemetry-proto` crate.** Verified against this workspace: its `Cargo.toml` declares
  `opentelemetry` and `opentelemetry_sdk` as **non-optional**, even under `default-features = false,
  features = ["gen-tonic-messages"]` — pulling it in would drag in the OTel SDK (and `tracing`) this
  repo has deliberately deferred taking a dependency on, pin this workspace to whatever `prost`/
  `tonic` versions that crate's release cadence chose rather than one `logit` controls, and its own
  optional `schemars` dependency is 1.0 against this workspace's 0.8 — a `[bans] multiple-versions`
  finding for a crate this project doesn't otherwise need at all. Rejected.
- **`build.rs` + `prost-build` at build time.** Puts `protoc` on every build path, including the
  production release image (`Dockerfile`, not just `Dockerfile.dev`) — precisely what ADR `containerized-development`
  rules out generally and this ADR rules out specifically for OTLP. Rejected.
- **Hand-rolled protobuf encode/decode.** ~30 message types, several with nested `oneof`s and
  packed-varint repeated fields, is exactly the class of surface AGENTS.md's `HyperLogLog` stance
  argues against re-implementing by hand to move faster — real complexity with an existing,
  well-tested generator available, not a case for a from-scratch implementation. Rejected.

## Consequences

- The vendored `.proto` sources carry a version (tag + commit hash, `proto/README.md`) that has to
  be bumped deliberately — regeneration is opt-in, not automatic, so a newer upstream OTLP release
  is adopted only when someone runs `script/protogen` and reviews the diff.
- `prost` (only) joins the dependency set every shipped crate ultimately builds against.
- Generated files are permanently excluded from lint/format scrutiny by attribute, and from
  doctests by the crate-level `doctest = false` — reviewers checking `crates/logit-proto/src/otlp/`
  changes should expect `generated/*.v1.rs` to appear only as whole-file rewrites after a deliberate
  `script/protogen` run, never as a hand-edited diff.
- **This is the ADR `native-wire-format-with-otlp-bridge` qualification its own Consequences section flagged as possible but didn't
  yet have an example of:** "the internal event model must be a superset of what OTLP can express,
  or the OTLP codec becomes lossy" assumed the loss, if any, would run *into* `logit`'s model from
  OTLP's. `crates/logit-proto/src/otlp/metrics.rs`'s `Distribution`→`Summary` and `Set`→skip
  mappings are the opposite direction: here it's `logit`'s own model (a mergeable `DDSketch`, a
  `HyperLogLog` stub with no cardinality to read) that cannot be losslessly re-expressed *as* OTLP.
  Both are counted (`logit.output.metrics.degraded`/`.skipped{metric_kind}`) and documented in
  `docs/known-gaps.md`'s "Cross-protocol semantic gaps" entry rather than silently contradicting
  ADR `native-wire-format-with-otlp-bridge`'s claim.
