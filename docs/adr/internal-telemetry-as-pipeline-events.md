---
created: 2026-08-31
updated: 2026-08-31
---

# Internal telemetry as ordinary pipeline events, drained from a component-level buffer

## Status
Accepted

## Context

`logit` has no way to say anything about itself. `Diagnostics` (`crates/logit-core/src/diag.rs`)
prints throttled stderr lines and keeps its counts private; `interner::len()` exists as an
explicit "observability hook" with nothing wired to it (`docs/known-gaps.md`); how many events a
component sourced, what it dropped, how long a sink's writes take, whether a node is stalled on
backpressure — all invisible. Operating or debugging a running pipeline means guessing.

What's needed first is not a specific set of counters — that's genuinely unknown and best learned
by running the thing — but the framework: a mechanism that makes adding a new point cheap, keeps
its cost honest, and costs nothing at all when nobody asked for it.

Two decisions had to be made before any code: what a telemetry point *is* (as data), and how it
gets from "a component observed something" to "an operator can see it."

## Decision

**Internal telemetry is `logit`'s own event model, emitted through an ordinary graph component.**
A new `internal` listener component (`crates/logit-inputs/src/internal.rs`) drains a
process-wide buffer of points and emits them as regular `Event`s carrying `MetricRecord`s — the
exact type every other input already produces. This is not a new subsystem bolted onto the
pipeline; it is the pipeline, pointed at itself. Every existing downstream tool (`aggregate`,
`keep`, `lua`, any sink) already works on the result with no new code, because there is nothing
new for it to know about.

**Points are buffered and coalesced between drains, never aggregated over time by the telemetry
layer itself.** `logit_core::telemetry::Telemetry` (a handle every component can hold, mirroring
`Diagnostics`'s shape) records counts, gauges, and timings into a per-component buffer, keyed by
`(name, tags)`. A repeat at the same key merges into the pending point — sum for counts,
last-write-wins for gauges, one merged `DdSketch` for timings — using exactly the merges
`logit-transforms::Aggregator` already performs on real events. `internal`'s `interval` only
controls how often the buffer is drained into the graph; any real time-windowed aggregation is the
operator's to attach downstream by pointing an `aggregate` component at `internal`, the same way
they would at any other metrics source. The telemetry layer doesn't get its own windowing model
to keep in sync with the one that already exists.

**One `internal` source, not one per signal, and not named after what it emits today.** Every
self-observed fact — component counters and process-level facts (interner size, uptime) alike —
comes out of one component, distinguished by a dotted name prefix (`logit.component.*` vs.
`logit.process.*`). Splitting them by signal type or origin is a `filter` component's job once one
exists (already a declared-but-unimplemented `ComponentKind`); building a second listener now
would pre-empt a mechanism that's coming anyway. The name `internal` — not `internal_metrics` —
is deliberate for the same reason: this is "`logit` talking about itself," free to grow logs
(routing `Diagnostics` output into the graph) and spans (once a batch's identity can be threaded
through `Delivered`, ADR `minimize-allocations-over-event-size`-gated) without a rename.

**Zero cost when unconfigured, structurally, not by convention.** `Telemetry::default()` — what
every component starts with — wraps `Option<Arc<ComponentBuffer>>` as `None`; every method on it
is an immediate return, no allocation, no clock read, not even for `Telemetry::timer`, whose guard
only calls `Instant::now()` when the handle is live. A `Registry` (the buffer-of-buffers) is built
at all only when `logit-cli::pipeline::prepare` sees an `internal` component in the resolved
graph; otherwise every component gets the disabled handle, indistinguishable in cost from this
feature not existing. Pinned by `crates/logit-bench/tests/allocations.rs`'s existing exact-equality
assertions staying unchanged with no `internal` component present — the proof, not just the claim.

**The runtime instruments the uniform metric set itself; components add only what only they
know.** `Fanout::send`/`send_blocking` (`crates/logit-pipeline/src/fanout.rs`) is the one choke
point every producer — a listener, a `Transform`, a Lua component — sends through regardless of
kind, so instrumenting it there (batches/events sent, send-blocked duration, events dropped on a
closed consumer) yields the uniform per-component picture for every node with zero code in any
individual component. `run_transform`/`run_output`/`run_lua`
(`crates/logit-pipeline/src/runtime.rs`) add the consumer-side half (received counts, process
duration, absorbed/errored counts) from their own loops, which already see every event. A
component only reaches for its own `Telemetry` handle to add detail the runtime structurally
can't see — `statsd_in`'s datagram/byte counts, `influxdb_out`'s response class and retry count —
via the same `with_telemetry` builder idiom `with_diagnostics`/`with_timeout`/`with_retry`
already established.

**`Diagnostics` carries a `Telemetry` handle, so every existing throttled diagnostic becomes a
metric for free.** `warn_throttled`'s `&'static str` key already names *why* something happened
(`bad_datagram`, `parse_failure`, a retry reason); mirroring every occurrence — not just the ones
that make it past stderr's throttling — into `logit.component.diagnostics{key=...}` costs one
call site, not one per existing diagnostic. A flood invisible on a throttled terminal becomes a
visible rate the moment telemetry is live.

**Statsd client precedent, and the one place `logit` can't follow it.** Mature DogStatsD clients
(datadog-go, java-dogstatsd-client) aggregate counts/gauges/sets client-side by default for the
same reason this buffer coalesces: bound volume at the source without changing what the numbers
mean. They deliberately do *not* collapse timings/histograms/distributions the same way by
default — their own aggregator's comment is "we only pack them in one message instead of
aggregating them," because the statsd wire can carry raw samples cheaply and the server does the
distribution math; collapsing early is opt-in (`WithExtendedClientSideAggregation`) and needs a
newer Agent. `logit`'s `MetricKind` has no raw-sample representation at all — only mergeable ones
(`Counter`, `Gauge`, `Distribution` via `DdSketch`) — so "pack the raw values" isn't an option
here; building one sketch per drain is the only way to get one point out. That's taken as the
default here in a way it isn't for DogStatsD, because `logit`'s own downstream `aggregate` merges
sketches natively rather than needing raw values, and it bounds memory for free: a `DdSketch` is
bounded by its error config, not sample count, so no per-context sample cap is needed either.

**Cardinality is bounded and the overflow is counted, not silently grown.** Every mature statsd
client bounds its own queues and drops on overflow rather than growing unbounded, counting the
drop in its own telemetry (`datadog.dogstatsd.client.packets_dropped*`). A component's buffer caps
distinct `(name, tags)` keys at 1024; beyond that, a new key is dropped and the drop counted as
`logit.internal.points.dropped{reason="cardinality"}` under that component's own identity — a
misbehaving component (one that ignores the tag-cardinality convention: `&'static str` tag values
only, since the process-wide interner never evicts, per `docs/known-gaps.md`) becomes visible
instead of quietly leaking.

## Alternatives considered

- **A pull-based `/metrics` scrape endpoint (Prometheus-style).** Rejected for this codebase
  specifically: it would mean a second representation of a metric (Prometheus text format) kept in
  sync with `logit_core::MetricKind`, a second serving mechanism (an HTTP listener with nothing to
  do with the pipeline), and no way to reuse `aggregate`/`keep`/any sink on the result without
  first re-ingesting it through some other input. Emitting through the existing model and graph
  gets all of that for free.
- **A generic `tracing`/`metrics`-crate integration, with `logit` as just another exporter
  target.** This is real, valuable, future work (`docs/known-gaps.md` already names a `tracing`
  migration as separate, later work) but answers a different question — how `logit`'s *own*
  process-level logging matures — not how an operator gets a live, per-component picture of the
  pipeline it's running today. The two aren't in tension: a future `tracing` subscriber could
  itself feed `Diagnostics`/`Telemetry`, same as any other producer.
- **Aggregating internal points into real time windows inside the buffer itself**, rather than
  leaving that to a downstream `aggregate` component. Rejected: it would duplicate
  `logit-transforms::Aggregator`'s merge logic in a second place, and — more importantly — remove
  the operator's ability to choose their own window, sink, and enrichment for internal telemetry
  the same way they already do for real data. The buffer's coalescing looks similar to windowing
  but is deliberately not that: it has no fixed period, no flush semantics beyond "whatever's
  pending when `internal` next ticks," and exists purely to bound volume, not to answer "what
  happened in the last 10 seconds."
- **A `Registry` per graph, addressable by the CLI outside the pipeline (e.g. for a future
  `logit stats` command).** Not ruled out for later, but not built now: nothing today needs to read
  telemetry except by consuming it as events, and adding a second read path before the first one
  has real data flowing through it would be speculative.
- **Extended client-side aggregation for timings, opt-in like DogStatsD's.** Rejected as unneeded
  complexity here: DogStatsD's opt-in exists because the *non*-aggregated path (raw samples over
  the wire) is a real, well-supported alternative for its server. `logit` has no such alternative
  — there's nothing to opt out *to* — so a toggle would guard a path that can't actually be taken.

## Consequences

- Adding a new internal metric is a `telemetry.count(...)`/`.gauge(...)`/`.timing(...)` call at
  the point that already knows the fact, no new type or wiring elsewhere.
- The uniform per-component metric set (`crates/logit-pipeline/src/runtime.rs`,
  `crates/logit-pipeline/src/fanout.rs`) never needs updating when a new component kind lands —
  every kind gets it automatically, the same way arity and thread-vs-task dispatch already follow
  from `ComponentKind` alone.
- `docs/design/internal-telemetry.md` records the emit API, the merge table, the naming scheme,
  and the tag-cardinality convention this ADR assumes; keep the two in sync.
- A future `internal_spans`/log-carrying extension is additive to the same `internal` component —
  no rename, no second listener kind — once its own gating question (trace context in `Delivered`,
  ADR `minimize-allocations-over-event-size`) is answered on its own evidence.
- `docs/known-gaps.md`'s `interner::len()` note is closed: `internal` samples it on every tick.
