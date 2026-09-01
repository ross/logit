# 0017. Minimize allocations over event size, when the two conflict

## Status

Accepted.

## Context

Wave 2 of the memory-analysis effort (`docs/design/memory.md` §8 items 9 and 10) boxed
`SpanRecord` and `DdSketch` inside `Event`/`MetricKind`. Both trades have the identical shape: a
smaller, universal per-event byte footprint (paid by every event, on every hop, whether or not it
carries that payload), in exchange for one added heap allocation on construction and one more on
`Clone` — but *only* for an event that actually carries the payload in question.

Measured on the project's own reference config (`examples/nginx-to-influxdb.yaml`, whose event
carries 2 distribution metrics out of 4), boxing `DdSketch` alone moved the headline ingest number
`docs/design/memory.md` tracks from 5 to 7 allocations per line — a real, present-day cost, not a
theoretical one. That prompted the question this ADR settles: when a design choice must trade
`Event`'s (or a payload's) static size against allocation count, which axis wins?

Two things make the answer clear-cut for this project specifically:

- **`logit`'s deployments are not memory-footprint constrained** in the sense that matters here.
  The byte counts in play are hundreds of bytes per event, not a scaling concern against realistic
  in-flight event volume on the hardware `logit` targets.
- **Allocations are the more expensive resource at this scale, and by a wide margin.** Copying a
  few hundred extra bytes is close to free — a modern CPU moves dozens of bytes per cycle, so the
  difference is single-digit nanoseconds, often absorbed by cache effects already happening. A
  heap allocation, even on a fast path (jemalloc, thread-local cache hit, no contention), does real
  work — size-class lookup, freelist manipulation, metadata bookkeeping — commonly tens of
  nanoseconds in the best case, and considerably worse under contention, fragmentation, or a
  syscall fallback. No isolated, controlled benchmark of this exact ratio exists yet in this
  codebase (the `divan`/`CountingAlloc` harness in `crates/logit-bench` could produce one cheaply
  if a decision ever turns on the precise number); the direction is well-established regardless.

## Decision

**When a design choice trades `Event`'s (or a payload type's) static size against allocation
count, minimize allocations.** Concretely: don't box (or otherwise move to the heap) a field
inlined in `Event`/`MetricKind`/or any type on the hot path, if doing so adds an allocation on
construction or `Clone` for a payload that will be commonly populated in a real, intended
deployment.

**Evaluate "commonly populated" against the workload a payload type is designed to serve at
maturity, not against what today's input coverage happens to support.** `logit` targets logs,
metrics, and traces as equally first-class (`docs/OVERVIEW.md`); a trace-heavy deployment will
have most events carrying a span, the same way the metrics-heavy nginx reference config already
has most metrics as distributions. That an OTLP (or other span-producing) input doesn't exist yet
is a `v0.1` gap (`docs/known-gaps.md`), not a property of the workload — it is exactly the kind of
"current implementation state" this ADR says not to design against. A payload type is either
expected to be common once its input matures, or it isn't; there is no third position where it's
temporarily exempt because nothing populates it yet.

This reverses both of Wave 2's boxing decisions:

- **`MetricKind::Distribution(Box<DdSketch>)` → back to `Distribution(DdSketch)`.** Distributions
  are a shipping, present-day feature (statsd's `ms`/`h`/`d`, `kv_metrics` distributions) and
  commonly populated in exactly the configs this project uses as its own reference.
- **`Event.span: Option<Box<SpanRecord>>` → back to `Option<SpanRecord>`.** No OTLP input exists
  yet, but per this ADR that's not a reason to treat spans as rare — a trace-focused deployment
  will populate this on most events, the same way a metrics-focused one populates `metrics`.

`docs/design/memory.md`'s §1 and §8 are corrected to match, with `Event`'s size reverting to
reflect both unboxed fields again (see that document for the updated numbers).

## What this doesn't change

- **Item 14 (smallvec's `union` feature)** stands — it's not a trade at all, just recovered
  padding, free in both bytes and allocations.
- **Item 8 (`AttrMap`'s inline capacity)** stands unchanged, and this ADR reinforces its existing
  conclusion rather than revising it: the measured evidence already said don't shrink capacity
  8 → 4, specifically *because* doing so would cost an allocation (the logs-only shape) without
  ever saving one. That conclusion was already choosing fewer allocations over a smaller `AttrMap`
  — this ADR makes explicit, as a general policy, the reasoning that decision already applied in
  one specific instance.
- **This does not mandate maximizing inline capacity everywhere unconditionally.** The general
  direction is "prefer fewer allocations," not "inline everything regardless of size" — a change
  that trades allocations for size is still a `size_of` cost real enough to measure and weigh
  (`crates/logit-core/tests/type_sizes.rs` keeps asserting it exactly, on purpose), just not the
  *deciding* factor when the two are genuinely in tension. Whether other inline-capacity questions
  (e.g., `MetricList`'s current 1-slot capacity, which spills for any multi-metric event) are worth
  revisiting under this policy is a separate, open question — not decided here.

## Alternatives

- **Keep evaluating size-vs-allocation trades case by case, weighing both axes with no stated
  priority.** This is what Wave 2 did, and it produced an inconsistent outcome across items 9 and
  10 — one payload type was implicitly given a pass on the "how common will this be" question
  because its input doesn't exist yet, the other wasn't, for no principled reason. Rejected in
  favor of one stated, reusable priority.
- **Prioritize `Event` size over allocations**, on the reasoning that a smaller `Event` helps
  every hop, every fan-out clone, and every batch move uniformly, where an allocation cost is
  confined to the specific payload that's present. Rejected: at logit's target scale and hardware,
  the footprint difference is not the constraint that matters, and the allocation cost the
  fixtures actually measure (tens of nanoseconds and real CPU work per occurrence) is the more
  expensive resource by a wide margin at the byte counts in play here.
- **A compact, non-`Box` representation for `SpanRecord`/`DdSketch`** (e.g., a small-buffer
  optimization tuned to their actual common size, or shrinking `DDSketch` itself via a smaller
  `Store` configuration). Not pursued: this would mean surgery on `DDSketch`'s own internals (out
  of this project's control) or a bespoke inline/heap hybrid built from scratch, materially more
  design work than a policy decision warrants, and no evidence yet that the byte cost this ADR
  accepts is actually a live problem worth that investment.

## Consequences

- `Event` is somewhat larger again than Wave 2's boxed version, in exchange for zero added
  allocations on constructing or cloning a span- or distribution-carrying event, for any workload
  where those are the common case.
- Future decisions of this same shape (a hot-path type inlining vs. boxing a field) should cite
  this ADR rather than re-litigate the size-vs-allocations priority from scratch — only the
  "how common will this payload actually be at maturity" question needs fresh evidence each time.
- `crates/logit-core/tests/type_sizes.rs` and `crates/logit-bench/tests/allocations.rs` revert to
  asserting the unboxed numbers (adjusted for item 14's still-applied `union` feature savings).
