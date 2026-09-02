# Internal telemetry

How `logit` observes its own behavior: the emit API, the buffer it feeds, the `internal` source
that drains it into the graph, and the naming/tagging conventions every component follows.
Decision record: [ADR 0018](../adr/0018-internal-telemetry-as-pipeline-events.md). This document
is load-bearing per `AGENTS.md` — read it before adding a new internal metric or touching
`logit_core::telemetry`.

## Why this exists

Before this, `logit` couldn't say anything about itself: how many events a component sourced,
what it dropped, how long a sink's writes take, whether a node is stalled on backpressure — all
invisible. `Diagnostics` (`crates/logit-core/src/diag.rs`) prints throttled stderr lines; nothing
else exists. This is the framework for closing that gap — deliberately more about *the mechanism*
than about any specific counter, since which counters actually matter in operation is something to
learn by running this, not to guess up front.

## Shape: `logit`'s own event model, through an ordinary component

Internal telemetry is not a new subsystem. A point a component records is buffered, then drained
by the `internal` component (`crates/logit-inputs/src/internal.rs`) into ordinary
`Event`s carrying `MetricRecord`s — the same type `statsd_in` produces. It flows through the graph
exactly like any other source:

```yaml
components:
  self:
    type: internal
    interval: 10s

  window:
    type: aggregate
    sources: [self]
    interval: 60s

  influx:
    type: influxdb_out
    sources: [window]
    url: !env INFLUXDB_URL
    org: logit
    bucket: internal
    token: !env INFLUXDB_TOKEN
```

Nothing downstream needs to know it's looking at telemetry rather than user data. `keep`, `lua`,
any sink — all already work.

## The emit API

`logit_core::telemetry::Telemetry` is a component's handle, mirroring `Diagnostics`'s shape:

```rust
telemetry.count(name: &'static str, n: f64, tags: &[Tag]);
telemetry.gauge(name: &'static str, v: f64, tags: &[Tag]);
telemetry.timing(name: &'static str, d: Duration, tags: &[Tag]);
let timer = telemetry.timer(name: &'static str);   // records on Drop, or explicitly via .stop(tags)
```

`Telemetry::default()` is the disabled handle every component starts with. Every method on it is
an immediate no-op — no allocation, no lock, and (via `Timer`) no clock read. A live handle only
ever reaches a component through `Registry::telemetry_for` (below), which only ever exists when a
config's `internal` component asked for one. This is what makes "no `internal` component in
config" cost nothing: not "close to nothing," a branch on `Option::None`.

**Tags are `(&'static str, &'static str)` pairs by convention.** Both halves must be
compile-time-constant strings: `("class", "5xx")`, never a raw path, peer address, or anything
else derived from traffic. This isn't enforced by the type system beyond requiring `'static` — it
is enforced by review and by the fact that every shipped component follows it. It matters more
here than it does for ordinary event attributes: the process-wide interner never evicts
(`docs/known-gaps.md`), so a runtime-derived tag *value* leaks for the life of the process, same
as a metric name embedding a request id would.

## The buffer: coalesce between drains, using the merges `aggregate` already performs

Points are kept in a per-component `ComponentBuffer`, keyed by `(name, tags)`. A repeat of the
same key merges into the pending point rather than queueing a second one:

| Kind | Coalesced by | Emitted as |
|---|---|---|
| count | sum | `MetricKind::Counter` |
| gauge | last write wins | `MetricKind::Gauge` |
| timing | samples merged into one sketch | `MetricKind::Distribution(DdSketch)` |

These are exactly `logit-transforms::Aggregator`'s own merge rules
(`Accumulator::Counter` sums, `Gauge` is last-write-wins, `Distribution` merges sketches). That
identity is load-bearing, not incidental: it's what makes attaching a real `aggregate` component
downstream of `internal` extend this to any actual time window *correctly* — the merges compose,
because they're the same merges. The buffer itself has no notion of a time window; it holds
whatever has accumulated since the last drain and nothing more. See ADR 0018 for why this can't
take DogStatsD's "pack raw samples, let the server aggregate" option for timings —
`logit_core::MetricKind` has no raw-sample representation, only mergeable ones.

**Cardinality is capped, not unbounded.** A component's buffer holds at most 1024 distinct
`(name, tags)` keys (`telemetry::MAX_KEYS_PER_COMPONENT`); a new key beyond the cap is dropped and
counted as `logit.internal.points.dropped{reason="cardinality"}` under that component's own
`component`/`kind`/`role` attributes — bound-and-count-the-drop, the same convention every mature
statsd client uses for its own send failures, so a component that violates the tag convention
above becomes visible instead of silently growing the interner. Under the tag convention, this
never fires in practice: cardinality is bounded by how many distinct metric names and
compile-time-constant tag values a component's code contains, not by traffic.

## `internal`: the drain

```rust
ComponentKind::Internal { interval: Duration }
```

A listener (`Role::Listener` — no `sources`, needs ≥1 consumer), like `statsd_in`. `interval`
serves two purposes:

1. **Drain cadence.** Every registered component's buffer is drained and its points emitted as one
   batch.
2. **Sampling tick for process-level gauges** — facts tied to no occurrence, so nothing else has a
   reason to push them: `logit.process.interner.strings` (`interner::len()`, closing the
   observability hook `docs/known-gaps.md` names) and `logit.process.uptime`.

At most one `internal` component per config (graph validation rule 13,
`crates/logit-pipeline/src/graph.rs`) — two would each drain, and so split, the same process-wide
`Registry`.

**Pick `interval` so it divides evenly into any downstream `aggregate` interval.** `internal`'s
own drain boundary and `aggregate`'s window boundary are two independent clocks; if they don't
divide evenly, a real window ends up straddling two drains in a way that isn't reproducible run to
run. The same rule DogStatsD documents for its own aggregation-interval-vs.-Agent-flush-interval
relationship, for the same reason.

`internal`'s own points (`logit.internal.points.emitted`, `logit.internal.drain.duration`) are
recorded via its own `Telemetry` handle, registered in the same `Registry` it drains — they ride
along in the *next* drain, one tick behind, since a drain can't include a count of itself. Every
mature statsd client's own self-telemetry (packets sent/dropped) works the same way.

## Naming

Dotted, lowercase, namespaced by where it comes from:

- `logit.component.*` — the uniform set every component gets from the runtime (below).
- `logit.<kind-family>.*` — component-specific detail, e.g. `logit.input.datagrams`,
  `logit.output.requests`.
- `logit.process.*` — facts about the running process, not any one component.
- `logit.internal.*` — facts about the `internal` component itself, including
  `logit.internal.points.dropped` (which names the *offending* component via its `component`
  attribute, not via the metric name).

No event type in the metric name (`logit.component.events_in`, say) — deliberate: `internal` may
grow logs and spans later without every existing name having promised "this is a metrics-only
source."

## Two layers of instrumentation, one buffer

**Layer 2: the runtime instruments itself, uniformly, with no component code.**
`Fanout::send`/`send_blocking` (`crates/logit-pipeline/src/fanout.rs`) is the one choke point
every producer sends through — a listener, a `Transform`, a Lua component — so instrumenting there
gives every one of them the send-side numbers for free:

| Name | Kind | Meaning |
|---|---|---|
| `logit.component.batches.sent` | count | one per `Fanout::send` call, regardless of fan-out width |
| `logit.component.events.sent` | count | events in that batch |
| `logit.component.send.blocked.duration` | timing | time spent inside one `Fanout::send` call (all consumers) |
| `logit.component.events.dropped{reason="closed_consumer"}` | count | a consumer's channel was already closed |

`run_transform`/`run_output`/`run_lua` (`crates/logit-pipeline/src/runtime.rs`) add the
receive/processing side from their own loops, which already see every batch and event:

| Name | Kind | Recorded in |
|---|---|---|
| `logit.component.batches.received` / `.events.received` | count | `run_transform`, `run_output`, `run_lua` |
| `logit.component.process.duration` | timing | `run_transform`, `run_lua` (whole batch) |
| `logit.component.events.dropped{reason="absorbed"}` | count | `Transform::process` returned `None` |
| `logit.component.events.dropped{reason="script_drop"}` | count | Lua `ProcessOutcome::Drop` |
| `logit.component.flush.events` / `.flush.duration` | count / timing | a flush-bearing node's `flush()` |
| `logit.component.send.duration` | timing | one delivery attempt, `deliver_with_retry` (`write_loop`) |
| `logit.component.retries` | count | a retried delivery attempt, `deliver_with_retry` (`write_loop`) |
| `logit.component.errors` | count | `Output::send` failed (any attempt), or a Lua script error |
| `logit.component.diagnostics{key=...}` | count | every `Diagnostics::warn_throttled` occurrence, throttled or not |

**Every sink also gets a `SinkQueue`** (`crates/logit-pipeline/src/sink_queue.rs`,
`docs/adr/0019-buffered-sink-delivery.md`) sitting between its inbox drain and delivery — its own
uniform layer, same reasoning as `Fanout`'s: instrumenting the one choke point every sink's batches
pass through gives every sink these for free, no per-sink code:

| Name | Kind | Meaning |
|---|---|---|
| `logit.component.buffer.batches` | gauge | batches currently queued, sampled on every push/commit |
| `logit.component.buffer.bytes` | gauge | `EventBatch::estimated_heap_bytes` summed over what's queued |
| `logit.component.buffer.utilization` | gauge | `max(batches ratio, bytes ratio)` against the two configured bounds |
| `logit.component.buffer.push.blocked.duration` | timing | how long a `Block`-policy push waited for room; only recorded when a push actually had to wait |
| `logit.component.batches.dropped{reason=...}` / `.events.dropped{reason=...}` | count | `reason` one of `overflow_oldest`/`overflow_newest` (`SinkQueue` eviction), `send_failed` (`write_loop`: not retryable, or retryable but the budget ran out), `shutdown` (`write_loop`: shutdown grace expired with the queue still non-empty) |

Two metrics named in this doc's original design were not built in the pass that shipped
`SinkQueue`: a per-batch `buffer.wait.duration` (push-to-commit latency) and an
`outcome`-tagged `send.attempts{outcome="ok"|"retryable"|"permanent"}` breakdown. `buffer.batches`/
`.bytes`/`.utilization` already answer "is this sink's queue backing up," which was the operative
question; the finer breakdowns are a plausible future addition, not a gap blocking anything today.

This set never needs updating when a new component kind lands — it comes from `ComponentKind`'s
role and the node runtime alone, the same way arity and thread-vs-task dispatch already do.

**Layer 3: a component adds only what only it knows**, via the same `with_telemetry` builder
idiom `with_diagnostics`/`with_timeout`/`with_retry` already established
(`crates/logit-cli/src/pipeline.rs::build_spec`). Both layers write into the *same*
`ComponentBuffer` — `build_spec` computes one `Telemetry` handle per component and hands it both to
the component itself and to the runtime's per-node instrumentation, so a drain sees one coherent
picture per component, not two.

Two worked examples ship as the pattern to follow:

- `statsd_in` (`crates/logit-inputs/src/statsd.rs`): `logit.input.datagrams`,
  `logit.input.datagram.bytes` — per-datagram detail `Fanout`'s per-batch view can't see, plus
  decode failures free via the `Diagnostics` bridge.
- `influxdb_out` (`crates/logit-outputs/src/influxdb.rs`): `logit.output.requests{class="2xx|
  4xx|5xx|network_error"}`, `logit.output.request.duration` (per attempt), `logit.output.batch.bytes`
  — the encode/HTTP-response detail a generic `send.duration` timer can't distinguish. **Not**
  `logit.output.retries` — retry moved out of this sink entirely
  (`docs/adr/0019-buffered-sink-delivery.md`) into the generic `deliver_with_retry` every sink now
  shares, so retry counting is a Layer 2 metric (`logit.component.retries`, above), not something
  each sink tracks for itself.

## Adding a new internal metric

1. Decide which layer it belongs to. Uniform across every component of a kind? It probably
   belongs in `runtime.rs`/`fanout.rs`, not in one component. Specific to what one component knows
   internally? It belongs on that component, via its own `Telemetry` handle.
2. Pick a name following the scheme above, and tags that are `&'static str` constants only.
3. Call `count`/`gauge`/`timing`/`timer` at the point that already knows the fact. No new type, no
   registration step, no schema change.
4. If it's genuinely new ground (a new signal type, a new source of process-level facts), read
   ADR 0018's "Alternatives considered" first — several shapes that look like natural extensions
   were deliberately not built yet, for stated reasons.

## What this is not

- **Not a time-series aggregation engine.** The buffer coalesces to bound volume between drains;
  any real windowed aggregation is `aggregate`, attached downstream like any other consumer.
- **Not a scrape endpoint.** There is no pull path and no plan for one — see ADR 0018's
  alternatives for why.
- **Not the `tracing` migration.** `Diagnostics`'s stderr output and this telemetry layer are both
  still separate from the deferred `tracing` migration `docs/known-gaps.md` names; that migration,
  when it lands, is a plausible future *producer* into this same buffer, not a replacement for it.
