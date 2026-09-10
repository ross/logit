---
created: 2026-09-09
updated: 2026-09-09
---

# `tracing` for self-logging, with `Diagnostics` as its producer

## Status
Accepted

## Context

`logit_core::diag::Diagnostics` prefixes a component id and throttles by occurrence count, and
that's all: no severity, no structured fields, no filtering, no timestamps, no lifecycle messages
(startup, bound, shutdown, sink degraded) at all — every diagnostic is a bare `eprintln!`.
`docs/known-gaps.md` filed the `tracing` migration as "deliberately kept as separate, later
work," and ADR `internal-telemetry-as-pipeline-events` left the door open for exactly this: "a
future `tracing` subscriber could itself feed `Diagnostics`/`Telemetry`, same as any other
producer." This is that work.

## Decision

**Adopt `tracing`, not a hand-rolled leveled-logging facade.** `tracing` was already resolving
transitively in `Cargo.lock` (via `hyper`/`tokio`'s own instrumentation) at 0.1.44 before this
decision — promoting it to a direct dependency adds no new crate version to the graph.
`Diagnostics` keeps its existing API and throttling shape (callers don't change) but its `warn`/
`warn_throttled` now emit through `tracing::warn!` instead of `eprintln!`, carrying `component`
and (for `warn_throttled`) `key` as structured fields; new `info`/`error` methods cover the rare,
unthrottled lifecycle messages a component owns directly (a listener's bound address, a sink
recovering).

**`tracing_subscriber`'s `fmt` layer, not a bespoke formatter.** `--log-level`/`LOGIT_LOG`
(`EnvFilter` syntax) and `--log-format text|json` are the only two knobs `logit run` exposes;
`schema`/`validate`/`graph` stay print-only, since they run once and exit, with nothing ongoing to
log leveled. `tracing-subscriber` is a genuinely new dependency subtree (unlike `tracing` itself)
but pulls in nothing that needed a build-time C toolchain or changed `deny.toml`'s license
allowlist.

**The suppressed `warn_throttled` path must stay allocation-free and clock-free.** The existing
throttle (telemetry count, then a `HashMap::entry` bump, then a power-of-two check) returns before
ever calling into `tracing` on a suppressed occurrence — `crates/logit-bench/tests/
allocations.rs`'s exact-equality assertions are the proof, not the claim, and stayed unchanged
across this migration.

**Lifecycle events get stable, `&'static str` names, not free-text messages.** `starting`,
`ready`, `shutdown signal received`, `drain complete`, `degraded`/`recovered`, `exiting` are the
same strings every time, specifically so an operator can build a log-based alert ("no `ready`
within N seconds of `starting`") without parsing prose. This is the same reasoning
`Diagnostics::warn_throttled`'s `&'static str` key already followed for *why* something happened;
lifecycle events extend it to *what phase the process is in*.

## Alternatives considered

- **The `log` crate (`log::warn!`/`env_logger`) instead of `tracing`.** Rejected: `log`'s facade
  has no structured fields — every diagnostic would still have to encode `component`/`key` into
  the message string by hand, exactly the limitation this migration exists to remove. `tracing`
  also gives workstream D's `TelemetryLayer` a `Subscriber`/`Layer` API to hook into; `log` has no
  equivalent.
- **Keep `eprintln!`, hand-roll JSON output for `--log-format json`.** Rejected: this reproduces a
  subset of what `tracing_subscriber::fmt`'s `json()` feature already does, with no filtering
  (`EnvFilter`) and no path to workstream D's internal-logs capture without inventing a second,
  parallel event bus.
- **`tracing` spans for pipeline stages (`process`, `deliver`, `flush`).** Rejected here — not
  because it's a bad idea, but because it already exists, under a different name: `internal`'s own
  spans (`logit_core::telemetry::Telemetry::span`, ADR
  `internal-span-emission-and-deterministic-sampling`) are the pipeline's own tracing, sampled
  deterministically per trace and drained as ordinary events. A second, parallel `tracing`-span
  layer over the same node visits would duplicate that mechanism rather than extend it.

## Consequences

- Every existing `Diagnostics` call site (30+ across `logit-inputs`, `logit-outputs`,
  `logit-transforms`, and `logit-pipeline`) gets
  leveled, filterable, structured output for free, with no call-site changes.
- `grep -rn 'eprintln!' crates/*/src` names only `logit-cli/src/main.rs`: `Command::Run`'s
  exit-error printer, `Command::Graph`'s validation warning, and `Command::Ready`'s probe failure
  — a CLI's own stderr on its own error paths, not a running service's self-log.
- Workstream D's `TelemetryLayer` (see `docs/design/internal-telemetry.md`'s "Logs" section) is
  the producer ADR `internal-telemetry-as-pipeline-events` predicted: a `tracing_subscriber::Layer`
  feeding `Registry` the same way any other component's `Telemetry` handle does.
- `docs/known-gaps.md`'s `eprintln!` entry is closed.
