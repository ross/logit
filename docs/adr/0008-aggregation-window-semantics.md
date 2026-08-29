# 0008 — `aggregate` transform: tumbling windows, pass-through, and the flush-tick contract

## Status
Accepted

## Context
`BuiltinTransformConfig::Aggregate { interval: Duration }` (`crates/logit-config/src/lib.rs`) has
existed as a config variant since PR #8, rejected at both `logit validate` and `logit run` as "not
implemented yet." It's the last piece of the v0.1 vertical slice
([`tmp/0.1-status.md`](../../tmp/0.1-status.md)), and the first real consumer of two things that
existed only in prose or as unused code before this: `DdSketch::merge`
([`docs/design/data-model.md`](../design/data-model.md)'s mergeable-metric-kinds design) and
`ScriptWorker::flush()` (`crates/logit-script/src/lib.rs`, implemented and tested since the Lua
engine landed, but never invoked by the pipeline).

`docs/design/lua-api.md` fixes some of this transform's shape in advance — a stage-local `interval`
config key, `flush()` runs on that interval and returns events to emit, `aggregate` sits ahead of
user Lua in the chain — but leaves the actual windowing semantics open: tumbling vs. sliding, window
alignment, late/out-of-order data, what an emitted aggregate's timestamp and resource are, and what
happens to events the aggregator can't accumulate. Per `AGENTS.md`'s "a new design decision worth
remembering gets an ADR," those are what this record settles.

## Decision

**Tumbling windows, reset on flush.** Each interval accumulates from empty; a flush drains every
window into one emitted event per series and discards the accumulator. A counter's emitted value is
therefore that window's *sum* (a delta), matching what a raw InfluxDB counter field expects, not a
running total — `docs/design/data-model.md`'s "Counter/Gauge merge trivially (sum / last-write-wins
by timestamp)" describes merging *within* one window, not across window boundaries.

**Per-kind merge, exactly as `data-model.md` specifies, nothing invented:** `Counter` sums; `Gauge`
keeps the value with the latest *source* timestamp (ties favor whichever event is processed second —
arbitrary but deterministic, since processing order is itself deterministic per pipeline);
`Distribution` merges via `DdSketch::merge`. `Set` has no merge implemented here because
`HyperLogLog` (`crates/logit-core/src/metric.rs`) is still a method-less stub — a design gap, not a
decision this ADR is re-litigating. `Histogram`/`Summary` have no merge rule specified anywhere in
this codebase's design docs.

**Pass through, never drop.** Logs, spans, and any metric kind with no defined merge rule here
(`Set`, `Histogram`, `Summary`) are forwarded to the next stage untouched. So is an event whose kind
*conflicts* with a series already accumulating under the same name/unit/attributes (a counter and a
gauge sharing identical tags, say) — there's no correct merge for that either, so it's forwarded
rather than silently corrupting the existing accumulator or being dropped. This means aggregated
output can arrive out of order relative to passed-through events from the same batch (the aggregated
version only appears at the next flush tick) — inherent to windowed aggregation, not a bug.

**Grouped by resource value, not by which pipeline/batch produced it.** Two batches whose
`Arc<Resource>` are different allocations but equal content describe the same origin and aggregate
together; `Resource` is `PartialEq`, not `Hash`, so grouping is a linear scan (one group in practice
today — statsd always uses `Resource::default()`).

**Emitted timestamp is flush wall-clock**, i.e. when the window closed — not a source event's
timestamp, and not the gauge's internally-tracked "latest write" timestamp (used only to pick the
winner, then discarded).

**Windows are wall-clock-driven, not event-time.** A flush fires when the pipeline's clock reaches
the deadline, regardless of what timestamps the accumulated events actually carry. There is no
watermark, no late-data grace period, no reordering buffer: an event that arrives after its window's
flush has already fired lands in the *next* window, full stop. This matches every other timing
decision already made in this codebase (statsd stamps one wall-clock timestamp per datagram) and
keeps the aggregator's state bounded and simple; a real event-time model is a larger design question
for if/when it's actually needed.

**A flush is not exempt from the rest of the chain.** Events an `Aggregate` stage emits at a flush
tick run through every later stage exactly like a normal batch would (`flush_stage`,
`crates/logit-cli/src/pipeline.rs`). A downstream `Aggregate` stage therefore re-accumulates a
flushed event rather than passing it straight through — intentional (a chain of two aggregators is a
two-stage window), not a special case.

**The same flush-tick timer drives Lua's `flush()` too**, via an optional `interval` on the `lua`/
`lua_file` transform config variants (`crates/logit-config/src/lib.rs`). Omitted, the stage never
ticks — the same as a script defining no `flush()` at all, which was already legal and is now
finally reachable in practice. A Lua stage's `flush()` has no batch of its own to take a resource
from (unlike `Aggregate`, which tracks its own per-resource windows); it's stamped with whichever
resource the worker most recently saw on a real batch, or a fresh default if none has arrived yet.
This is a real, narrow gap — documented here rather than left silent — that matters only once a
pipeline has more than one resource feeding it, which nothing in v0.1 does.

## Alternatives considered
- **Sliding windows.** Rejected for v0.1: no consumer needs overlap between windows, and a sliding
  window needs either a ring buffer of sub-windows or re-processing overlapping ranges, real added
  complexity with no concrete requirement driving it yet.
- **Event-time windows with a watermark/grace period.** Rejected for the same reason as sliding
  windows — genuinely more correct for out-of-order data, but no input in this codebase produces
  meaningfully out-of-order data today, and it would need a new "how late is too late" config
  surface with no user asking for it yet.
- **Cumulative (never-reset) counters**, matching OTLP/Prometheus cumulative temporality. Rejected:
  it means state grows unbounded with series cardinality and a process restart resets every series
  to zero with no way to detect that from the emitted stream, whereas tumbling-with-reset makes each
  emitted value self-contained.
- **Drop pass-through-ineligible events instead of forwarding them.** Rejected outright: this is
  exactly the "reports healthy while silently losing telemetry" failure class this project's review
  process has repeatedly flagged and fixed elsewhere (see PR #8's review rounds). An `aggregate`
  stage that silently dropped every log line in a mixed pipeline would be a much worse defect than
  one that merely reorders relative to aggregated metrics.
- **Reject at config time any pipeline where `aggregate` might see a non-metric event.** Not
  implementable today: nothing in `InputConfig` declares which payload types an input kind produces,
  so there's no data to reject on.

## Consequences
- A counter/gauge/distribution series that a misconfigured source sends as two different kinds under
  identical tags is *reported* (`eprintln!`, pending a real diagnostics facility — the same
  known gap as every other stage) and forwarded, not silently merged into a nonsensical value or
  dropped.
- `logit validate`/`logit run` reject a zero flush interval on either an `aggregate` stage or a Lua
  stage's `interval` (`require_implemented_transform`,
  `crates/logit-cli/src/pipeline.rs`) — the hand-rolled humantime codec in `logit-config` accepts
  `0s` structurally, but a zero interval would make the worker's flush schedule perpetually due.
- The published schema (ADR 0003) gets `interval` as an additive, optional field on the Lua
  variants; `BuiltinTransformConfig::Aggregate`'s `interval` was already required.
- No graceful shutdown yet (Ctrl-C still falls through to the OS default), but the worker thread
  now flushes every flush-bearing stage once when its inbound channel closes normally, so a
  pipeline that reaches a clean end doesn't silently lose its last in-flight window.
