# 0013 — Service lifecycle: signal-driven shutdown and bounded output retry

## Status
Accepted

## Context
`docs/plans/0002-nginx-integration.md`'s workstream B works backward from one property of the
target deployment: `logit` runs unattended, behind a restart policy, with no operator watching it.
Two gaps in v0.1 make that combination actively harmful rather than merely incomplete:

- **No installed signal handler.** Ctrl-C/SIGTERM falls through to the OS default (immediate
  termination). The close-time flush that protects an in-flight `aggregate` window already exists
  (`crates/logit-pipeline/src/runtime.rs`'s per-node flush when its inbox closes *normally*) — but
  nothing today closes a listener's inbox that way on its own, since every listener loops forever.
  An unattended restart (a container orchestrator sending SIGTERM before SIGKILL) loses whatever
  window was in flight, every time.
- **One InfluxDB 5xx ends the process.** `InfluxDbOutput::send` propagates any transport error or
  non-2xx response straight up through `run_output` to `run`'s `result??`, ending `logit run`. Under
  a restart policy that's not "fail fast and let the supervisor recover" — a single 5xx blip
  becomes an unbounded crash loop, worse than tolerating the blip would have been.

Both are scoped to "production packaging and service lifecycle," not to any nginx-specific
behavior — this ADR and the workstream it documents apply equally to the existing statsd/InfluxDB
path.

## Decision

### Shutdown: cancel the listener future, don't touch `Input`
`crates/logit-pipeline/src/runtime.rs` gains `run_with_shutdown(graph, specs, shutdown)`, which
`run(graph, specs)` now calls with `std::future::pending()` as its (never-resolving) `shutdown` —
so every existing caller and test is unaffected.

`shutdown` is raced against each listener with `tokio::select!`:

```rust
tokio::select! {
    result = input.run(fanout) => result.with_context(|| format!("component '{id}'")),
    _ = shutdown.wait_for(|&due| due) => Ok(()),
}
```

`Input::run(&mut self, sink: Fanout)` takes its `Fanout` by *value* — so when `shutdown` wins the
race, dropping `input.run`'s future drops the `Fanout` inside it, which drops the last `Sender`
into every one of that listener's downstream inboxes. Those inboxes then observe every sender gone
and close, precisely the way they already do today when a listener finishes on its own
(`FiniteInput` in `runtime.rs`'s own tests proves that cascade). The existing close-time flush
(`run_transform`/`run_lua`, `runtime.rs:175-185` and `:268-273`) needs no change at all.

**No `Input` trait change, and no cooperation required from any listener implementation** — this is
the decision this ADR is actually making, over the alternative of widening `Input::run` with a
cancellation parameter (see Alternatives). `syslog_in` (a later workstream) inherits shutdown for
free the moment it exists.

`crates/logit-cli/src/pipeline.rs::run_pipelines` installs the real signal handler:
`SignalKind::terminate()`/`SignalKind::interrupt()` under `#[cfg(unix)]`, `tokio::signal::ctrl_c()`
otherwise. A second signal before the drain finishes calls `std::process::exit(130)` immediately —
a wedged drain must stay killable by the same signal that started it, which matters specifically
once a restart policy, not a person at a terminal, is what's waiting on this process to exit.

**Accepted cost:** a datagram mid-`recv_from`/decode when the signal lands is dropped along with
the cancelled future. This is the right trade — UDP is lossy by contract already; the aggregation
window this change protects is not — but it is a real, deliberate behavior, not an oversight.
Recorded in `docs/known-gaps.md`.

### Retry: a tight wall-clock budget, not an attempt count

> **Revised by [ADR 0020](0020-buffered-sink-delivery.md).** The ~5s default budget below is
> deliberately tight *because* delivery isn't decoupled from the drain loop at this point — see
> that ADR's "Retry moves behind the queue boundary and its budget widens" section. Once a sink
> holds its own queue, the same reasoning argues for a much larger default. This section's
> narrative and the retryable/permanent classification it establishes are otherwise unaffected and
> still describe how `InfluxDbOutput` classifies a failure; only the budget's size and the retry
> loop's location move.
`InfluxDbOutput::send` (`crates/logit-outputs/src/influxdb.rs`) runs *inline* in `run_output`'s
drain loop — one `send` per batch, awaited directly, nothing decoupling delivery from the pipeline
that feeds it. That shape rules out an attempt-count-only retry policy: five attempts at up to a
10s request timeout each, plus backoff between them, could stall the drain loop for close to a
minute. During that stall the output's 64-batch inbox fills, backpressure reaches `aggregate` and
then the statsd listener, which stops calling `recv_from` — and the kernel starts dropping UDP
datagrams with no signal anywhere in `logit` that it happened. `docs/design/pipeline-graph.md`'s
backpressure section already names this as a general property (a stalled sink backs up every
branch sharing its upstream); retry without a bound turns it from a latent property into an active
one.

`InfluxDbOutput` gains a `RetryPolicy { total_budget, base_delay, max_delay, attempt_timeout }`,
set via a `with_retry(policy)` builder (the same idiom as the existing `with_timeout`). The default
`total_budget` is **~5 seconds** — comfortably inside the 10s aggregation window, so a retrying
output never stalls the pipeline past one window's worth of backpressure. The deadline is checked
before starting each attempt and before each backoff sleep, so the budget is a hard ceiling
regardless of how many attempts fit inside it. Each attempt's own per-request timeout is
`min(with_timeout's setting, retry.attempt_timeout, time remaining in the budget)` — `with_timeout`
keeps its existing meaning (a hard per-request cap) rather than being silently overridden by retry's
own pacing, and the remaining-budget clamp is what stops a single slow attempt from ever consuming
the whole budget on its own.

Retried: a transport error (including a request timeout) and any 5xx. **429 also retries** —
InfluxDB's own rate-limit response, and genuinely transient — a deliberate narrow deviation from
"a 4xx stays a hard failure." Every other 4xx bails on the first attempt: it's a config error
(bad org/bucket/token), and retrying would only delay a diagnosis that's already available.
Backoff is `base_delay * 2^(attempt-1)`, capped at `max_delay` and clamped to whatever budget
remains — no jitter, since there's exactly one writer per `InfluxDbOutput`, not a fleet
thundering-herding a shared endpoint.

**What this does and does not buy:** a ~5s budget rides out a blip or an isolated 5xx. It does
*not* ride out a real outage — an InfluxDB restart routinely takes longer than 5s, and `logit run`
will still exit for the supervisor to restart, exactly as it does today without retry. The
difference retry makes is narrower than "survives an InfluxDB outage": it's "a single transient
failure no longer ends the process." Riding out an actual outage without dropping intake needs
delivery decoupled from the drain loop (a real `Buffer` implementation) — out of scope here, and
tracked as a new `docs/known-gaps.md` entry, not solved by widening the retry budget.

### Diagnostics: attribution via a builder, throttling by count
Nine `eprintln!` sites exist; six carry no component id (`InfluxDbOutput`/`InfluxLineEncoder`,
`StatsdInput`/`StatsdDecoder`, `Aggregator`, `JsonParser` — none of these structs has an id field
today). A new `logit_core::diag::Diagnostics { component_id, counts }` is added to each via a
`with_diagnostics(diag)` builder, mirroring `with_timeout`'s existing shape rather than changing
any constructor — `influxdb.rs` alone has 19 tests, `statsd.rs` 17, `json.rs` 15, `aggregate.rs`
11; a constructor signature change would churn all of them for no behavioral gain. Only
`logit-cli`'s `build_spec` calls `with_diagnostics`, using the same `id` its loop already threads
through `with_context`.

`Diagnostics::warn_throttled` limits report volume **by occurrence count, not by time window**:
the 1st, 2nd, 4th, 8th, … occurrence of a given key reports, each naming the running total. A
time-window limiter was the more obvious shape but was rejected: `Transform::process` has no clock
injected, and threading one through the trait purely to rate-limit diagnostics would make an
unrelated interface non-deterministic to test. Count-based throttling needs no clock, is
trivially deterministic to unit test, and is bounded the same way in practice — a million
malformed lines produce on the order of twenty stderr lines, not a rate that happens to look
bounded under typical load.

## Alternatives considered
- **Widen `Input::run` with a cancellation token or `select!`-aware signature**, so shutdown is
  cooperative rather than cancel-by-drop. Rejected: every current and future `Input` implementation
  would have to reimplement shutdown correctly on its own, for a benefit — slightly more graceful
  handling of "the exact byte in flight" — that this project doesn't need, since dropping a
  UDP-listener future mid-datagram is already an accepted loss. Cancel-by-drop needs zero
  cooperation from any implementation and gets `syslog_in` shutdown for free later.
- **An attempt-count-only retry policy** (e.g. "5 attempts, exponential backoff, no total-time
  cap"). Rejected outright once the concurrency shape was examined: it cannot bound the stall it
  imposes on the shared drain loop, and an unbounded stall silently drops UDP intake at the kernel
  buffer — a worse failure than the one retry exists to fix.
- **A generous retry budget (~60s), to ride out a real InfluxDB restart.** Rejected for the same
  reason: without decoupling delivery from the drain loop, a generous budget just relocates the
  data loss from "the write that failed" to "everything the pipeline couldn't accept while the
  write was retrying" — worse, not better, and it still doesn't survive an outage longer than the
  budget.
- **Implement `Buffer` now, to decouple delivery from the drain loop properly.** This is the actual
  fix for the retry-vs-backpressure tension, and is likely the right next step, but it's real new
  design (an in-memory ring buffer or similar, an overflow policy, retry moving behind that
  boundary) that expands this workstream well past "packaging and lifecycle." Deferred, and
  tracked as its own `docs/known-gaps.md` entry rather than folded into this ADR's scope.
- **A time-window rate limiter for diagnostics**, matching how most logging frameworks throttle.
  Rejected here specifically because `Transform::process` has no clock to hand it without a trait
  change that buys nothing else this workstream needs; count-based throttling meets the actual
  goal (bounded stderr volume under a flood of malformed input) without one.

## Consequences
- `crates/logit-pipeline/src/runtime.rs`: new `run_with_shutdown`, re-exported from `lib.rs`
  alongside `run`. `run_input` gains a `watch::Receiver<bool>` parameter.
- `crates/logit-cli/src/pipeline.rs`: `run_pipelines` installs the signal handler and a
  second-signal kill switch; `run_config` (the test-only helper) is now `#[cfg(test)]`, since
  `run_pipelines` no longer calls through it in production.
- Workspace `Cargo.toml`: `tokio`'s `signal` feature is now enabled.
- `crates/logit-outputs/src/influxdb.rs`: `InfluxDbOutput` gains `RetryPolicy`/`with_retry`;
  `tokio` moves from `logit-outputs`' dev-dependencies to real dependencies (`tokio::time::sleep`).
- `crates/logit-core/src/diag.rs` (new): the shared `Diagnostics` helper. Not a `tracing`
  migration — that stays separate, deliberately deferred work; this closes specifically the
  "unattributed, unbounded stderr" hazard the plan named.
- `StatsdInput`, `StatsdDecoder`, `Aggregator`, `JsonParser`, `InfluxDbOutput`, and its inner
  `InfluxLineEncoder` each gain a `with_diagnostics(Diagnostics)` builder, mirroring
  `InfluxDbOutput::with_timeout`'s existing idiom rather than changing any constructor -- these six
  types carry ~90 existing tests between them, and the builder approach churns none of them.
  `logit-cli` gains a direct `logit-core` dependency to construct `Diagnostics::new(id)` in
  `build_spec`, which now takes the component's `id` as a parameter for exactly that.
- `docs/known-gaps.md`: the "no graceful shutdown" entry closes, replaced by the two named residual
  gaps (dropped in-flight datagram, no `Output` close hook); the `eprintln!` entry narrows to "the
  `tracing` migration is still outstanding"; the output-buffering entry gains a note that bounded
  retry now exists but `Buffer`/at-least-once delivery remain unimplemented; a new entry records
  that delivery IO is not decoupled from event processing within a node, which is what makes the
  retry budget above tight rather than generous.
