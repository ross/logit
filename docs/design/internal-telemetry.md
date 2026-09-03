# Internal telemetry

How `logit` observes its own behavior: the emit API, the buffer it feeds, the `internal` source
that drains it into the graph, and the naming/tagging conventions every component follows.
Decision record: [ADR `internal-telemetry-as-pipeline-events`](../adr/internal-telemetry-as-pipeline-events.md). This document
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

## Resource identity

Every batch `internal` sends carries `service.name = logit` on its `Arc<Resource>`
(`crates/logit-inputs/src/internal.rs`), built once in `InternalInput::new` since the resource is
batch-level and identical on every tick. This is what lets an OTLP backend (Tempo, in the demo)
resolve a root span's service — without it, a trace's root span still arrives, but with no
`service.name` to show, which is a *different* failure than a missing span and easy to mistake for
one: Grafana's Traces Drilldown renders it as `<root span not yet received>` either way.

`internal` is the one input allowed to make this claim. `service.name` names *the producer* of the
telemetry, not the source of the data: `internal`'s telemetry genuinely is `logit`'s own, so it can
honestly say so. `syslog_in`/`statsd_in`, by contrast, always use `Resource::default()` — data they
ingest belongs to whatever service sent it (one statsd listener may well serve several), so
stamping `logit` there would misattribute it. `otlp_in` gets this for free: it preserves whatever
resource the sender attached, rather than manufacturing one.

`influx_out` also sources `self` in the demo, and its encoder folds resource attributes into
InfluxDB tags (`crates/logit-outputs/src/influxdb.rs`'s `render_tag_suffix`) — so this attribute is
also a tag on every `logit.*` series. That's why it's `service.name` alone and not
`service.version`: a constant tag is a one-time, harmless addition to series identity, but a
version would re-key every series on each release. `otel.scope.version` on the OTLP instrumentation
scope (`crates/logit-proto/src/otlp/common.rs`'s `logit_scope`) already carries that information on
the trace side without that cost.

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
whatever has accumulated since the last drain and nothing more. See ADR `internal-telemetry-as-pipeline-events` for why this can't
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

## Spans

Closes the emission half of what was, until [ADR `internal-span-emission-and-deterministic-sampling`](../adr/internal-span-emission-and-deterministic-sampling.md),
an open item: [ADR `trace-context-propagation-on-delivered`](../adr/trace-context-propagation-on-delivered.md) put a real
`TraceContext` on every `Delivered` and gave the two unambiguous node kinds a real parent to
propagate; a follow-up gave `Transform::flush` a bounded `Vec<SpanLink>` per emitted event. Neither
emitted a `SpanRecord`. This section is that emission, plus the sampling knob span volume needs
that metric volume never did.

### One span is one node's minted `TraceContext`

Not "one node's processing of one batch" — the two differ for `Transform::flush` (an *n*-to-1
emission, no single incoming batch) and for a flush spanning several resource groups (which used
to mint several unrelated roots for what is really one unit of work). The runtime mints a context
exactly once per unit of work and uses that *same* context both as the span's identity and as what
the emission is sent under:

| Node | `trace_id` | `span_id` | `parent_span_id` | `SpanKind` | Recorded in | Window measured |
|---|---|---|---|---|---|---|
| Listener | fresh root | that root's | none | `Producer` | `Fanout::send`/`send_blocking` | the `send` call only |
| `Transform::process` | inherited | `parent.child()`, minted in `run_transform` | incoming | `Internal` | `run_transform` | `process_batch` + send |
| Lua `process` | inherited | same, minted in `run_lua` | incoming | `Internal` | `run_lua` | `process()` + blocking send |
| `Transform::flush` | fresh root | that root's | none | `Internal` | `run_flush` | `flush()` + every group's send |
| Lua `flush()` | fresh root | that root's | none | `Internal` | `run_lua`'s `flush_now` | `flush()` + send |
| `run_output` | inherited | `ctx.child()`, minted then discarded | incoming | `Client` | `write_loop` | the whole `deliver_with_retry` |

A fan-out (one batch, several downstream consumers) still records exactly one span: it's one
`send_with_own_context` call by one node, and the *N* consumers each mint their own child later, on
their own visit — the span belongs to the emission, not the edge.

### The emit API

Mirrors `Telemetry::timer`'s shape:

```rust
let mut span = telemetry.span(op, kind, trace_id, span_id, parent_span_id);
span.events(n);            // how many events this emission carries
span.link(link);            // or .links(iter) -- bounded, see below
span.tag("fault", "ambiguous");
span.error();                // or .ok() -- defaults to Ok
```                           // dropped, or .finish(), to record it

`op` is one of `"process"|"flush"|"send"|"deliver"` — half of the drained span's `name`, joined
with this component's own `kind` at drain time (`"aggregate process"`, `"influxdb_out deliver"`).
The sample decision (below) is made *inside* `span`, before any span-shaped state exists — an
unsampled trace gets the same disabled `SpanGuard` a disabled handle's `timer()` returns: every
method an immediate no-op, no allocation, no clock read beyond the one sampling comparison.

### The sampler: deterministic on `trace_id`

```rust
pub fn trace_is_sampled(trace_id: &[u8; 16], rate: f64) -> bool
```

Every node — and every `logit` process in a split-collection topology (`docs/OVERVIEW.md`) —
computes the same keep/drop verdict independently, from `trace_id` alone: a kept trace is kept at
*every* hop, a dropped one dropped at every hop, with no propagated bit and no extra bytes on
`TraceContext`/`Delivered`. Same shape as OTel's `TraceIdRatioBased` sampler (the top 53 bits of
the low 8 `trace_id` bytes — `f64`'s exact-integer range — compared against `rate`).

The rate lives on `Registry` (`Registry::with_span_sampling(rate)`, process-wide — graph rule 13
already guarantees at most one `internal` component) and is copied into each `ComponentBuffer` at
construction, so a live span never needs a second lock. Config:

```yaml
self:
  type: internal
  interval: 10s
  span_sample_rate: 0.1   # the default; 1.0 keeps everything, 0.0 turns spans off
```

Below `1.0` by default (`DEFAULT_SPAN_SAMPLE_RATE = 0.1`): span volume is a different shape than
metric volume — one span per node-visit per batch, where a metric point coalesces between drains.
Named `span_sample_rate`, not `sample_rate` — there is already a `ComponentKind::Sample` transform,
and `internal` may grow other sampling knobs later. Graph validation rule 16 rejects a non-finite
or out-of-`[0, 1]` value as a config error, since `trace_is_sampled` treats NaN as "keep
everything" — a surprising result to get from a typo rather than a deliberate choice.

### The bound: a plain `Vec`, not a keyed map

A point's `PointKey` map coalesces repeats at the same `(name, tags)` key; two spans never share an
identity to coalesce on, so `ComponentBuffer` holds spans in a separate, unkeyed `Vec`, capped at
`MAX_SPANS_PER_COMPONENT` (512) — a volume bound, not a cardinality one, since nothing else bounds
how many can accumulate except drain interval × sample rate. `SpanGuard::link`/`links` additionally
cap each individual span's own link list at `MAX_LINKS_PER_SPAN` (32). Both drop-and-count, never
silently grow: `logit.internal.spans.dropped{reason="buffer_full"}` on the buffer (drained
alongside `points.dropped`), `logit.internal.span.links.dropped{reason="cardinality"}` recorded
immediately on the guard.

### Span versus point, side by side

| | Point | Span |
|---|---|---|
| Storage | `HashMap<PointKey, Pending>`, keyed | `Vec<PendingSpan>`, unkeyed |
| Coalesces? | Yes, by `(name, tags)` | No — every visit is distinct |
| Cap | `MAX_KEYS_PER_COMPONENT` (1024 distinct keys) | `MAX_SPANS_PER_COMPONENT` (512 total) |
| Drop counter | `logit.internal.points.dropped{reason="cardinality"}` | `logit.internal.spans.dropped{reason="buffer_full"}` |
| Drained `Event::timestamp` | the drain time, `now` | the span's own `start` — **never** `now` |
| Emitted-count counter | `logit.internal.points.emitted` | `logit.internal.spans.emitted` |

The timestamp row is the one place `ComponentBuffer::drain(now)` must ignore its own `now`
argument for spans: `Event::timestamp` *is* the span's start (`SpanRecord`'s own doc comment), so
stamping it with the drain time would make every span drift later than reality by however long it
sat in the buffer.

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
| `logit.script.vm.memory` | gauge | `run_lua`, once per batch — the strongest signal a stateful script is leaking Lua-side state |
| `logit.script.events.emitted{outcome="emit"\|"emit_many"}` | count | `run_lua`, per `ProcessOutcome` — distinguishes a 1:1 script from a fan-out one |

**Every sink also gets a `SinkQueue`** (`crates/logit-pipeline/src/queue.rs`,
`docs/adr/buffered-sink-delivery.md`) sitting between its inbox drain and delivery — its own
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
role and the node runtime alone, the same way arity and thread-vs-task dispatch already do. The
last two rows are Lua-specific (recorded in `run_lua`, not shared with `run_transform`/`run_output`)
because only a Lua node has a VM to sample or a script return value to classify — everything else
in this table applies uniformly across every component kind.

**Every UDP listener also gets a `ReceiveQueue`** (`logit-inputs::udp`, an instance of the same
generic `BoundedQueue<T: Queued>` `SinkQueue` is, `docs/adr/decoupled-listener-io.md`) sitting
between the socket read and decode — the listener-side mirror of the sink block above, one choke
point every datagram passes through:

| Name | Kind | Meaning |
|---|---|---|
| `logit.component.receive.datagrams` | gauge | datagrams currently queued, sampled on every push/pop |
| `logit.component.receive.bytes` | gauge | undecoded datagram bytes summed over what's queued |
| `logit.component.receive.utilization` | gauge | `max(datagram ratio, byte ratio)` against the two configured bounds |
| `logit.component.receive.push.blocked.duration` | timing | only under `overflow: block`, only when a push actually waited |
| `logit.component.receive.latency` | timing | arrival (`Datagram::received_at`) → dequeue, per datagram — the number that says whether event timestamps are trustworthy under load |
| `logit.component.datagrams.dropped{reason=...}` / `.bytes.dropped{reason=...}` | count | `reason` one of `overflow_oldest`/`overflow_newest` (`ReceiveQueue` eviction) |
| `logit.component.receive.flushed{reason=...}` | count | `reason` one of `max_events`/`max_bytes`/`interval`/`resource_change`/`shutdown` — a `BatchAccumulator` emission |
| `logit.input.receive_buffer.bytes` / `.requested.bytes` | gauge | granted `SO_RCVBUF` after any kernel clamp, and what was actually requested (absent when unset) — sampled once at bind |

Three naming choices worth calling out, since the obvious names collide with existing ones: drops
are `logit.component.*`, not `logit.input.*` — they're emitted by the same generic `BoundedQueue`
code as the sink side's `batches.dropped`, and an operator alerting on data loss shouldn't have to
union two namespaces (the pre-existing `logit.input.datagrams`/`.datagram.bytes` *arrival* counters
stay under `logit.input.*`, since nothing in the runtime can see a datagram boundary — those remain
genuinely impl-known); accumulator emissions are `receive.flushed`, not an unqualified
`batches.flushed`, because `logit.component.flush.events`/`.flush.duration` already mean "a
stateful transform's window flush," and a bare `batches.flushed` next to those would read as the
same concept. `overflow: block` (never the receive-queue default — see the ADR) is the one
configuration under which `push.blocked.duration` records anything at all.

**Layer 3: a component adds only what only it knows**, via the same `with_telemetry` builder
idiom `with_diagnostics`/`with_timeout`/`with_retry` already established
(`crates/logit-cli/src/pipeline.rs::build_spec`). Both layers write into the *same*
`ComponentBuffer` — `build_spec` computes one `Telemetry` handle per component and hands it both to
the component itself and to the runtime's per-node instrumentation, so a drain sees one coherent
picture per component, not two.

Worked examples, one per shipped component:

- `statsd_in` (`crates/logit-inputs/src/statsd.rs`): `logit.input.datagrams`,
  `logit.input.datagram.bytes` — per-datagram detail `Fanout`'s per-batch view can't see, plus
  decode failures free via the `Diagnostics` bridge. Both listeners are now thin wrappers over
  `logit-inputs::udp::UdpListener` (`docs/adr/decoupled-listener-io.md`), which is where the
  `ReceiveQueue`/`receive_buffer.*` table above actually gets recorded — free for both, no
  per-listener code. A sampled `ms`/`h`/`d` line whose `@<rate>` implied a weight above
  `MAX_SAMPLE_WEIGHT` (the decode-time sample-rate extrapolation's bound on how far one value can
  inflate a `Distribution`'s `count()`) clamps rather than extrapolating unboundedly, reported via
  that same `Diagnostics` bridge as `logit.component.diagnostics{key="sample_rate_clamped"}` — no
  separate counter needed, since the bridge already mirrors every occurrence.
- `syslog_in` (`crates/logit-inputs/src/syslog.rs`): the same pair, `logit.input.datagrams`/
  `.datagram.bytes` — direct parity with `statsd_in`, the other UDP listener.
- `aggregate` (`crates/logit-transforms/src/aggregate.rs`): `logit.transform.series.active` and
  `logit.transform.resource.groups`, sampled at the top of `flush` before it touches its own state
  — the peak-of-window series count, which is the visible signal for the cardinality blow-up
  `crate::keep`'s own module doc already warns `aggregate` is exposed to. Gauge retention across
  the window boundary (`docs/adr/aggregation-window-semantics.md`'s amendment) adds three
  more: `logit.transform.series.retained` (gauge — the idle-but-carried population; `.active`
  itself keeps its original "series updated this window" meaning, not silently widened to include
  these), `logit.transform.series.evicted{reason="idle"|"cardinality"}` (count — a TTL expiry vs.
  the hard `max_retained_gauge_series` cap; a non-zero `cardinality` count means a later delta is
  about to resolve against 0.0), and `logit.transform.gauge.delta.unseeded` (count — a
  `GaugeDelta` opened a brand-new series and resolved against 0.0, statsd's own rule for an
  unseeded gauge, but indistinguishable from a real 0.0 without this). The last two also each fire
  a throttled `logit.component.diagnostics{key="gauge_retention_full"|"gauge_delta_unseeded"}`
  point via the `Diagnostics` bridge.
- `kv_metrics` (`crates/logit-transforms/src/kv_metrics.rs`): `logit.transform.derived{metric_
  kind}` / `.derived.skipped{metric_kind}` — makes the documented silent-skip path (a missing or
  non-numeric field, deliberately never a diagnostic) visible as a rate instead of invisible.
  `metric_kind`, not `kind` — `kind` is reserved for a point's own component-kind identity (see
  below), and this is the first component to actually need a tag that would have collided with it.
- `keep`/`remove` (`crates/logit-transforms/src/keep.rs`): `logit.transform.attributes.kept` /
  `.dropped` — the other half of `aggregate`'s cardinality story: how much `keep` is actually
  suppressing before events reach it. No `Diagnostics` on either (pure attribute filtering has
  nothing to warn about), so `Telemetry` is attached directly rather than through the
  `Diagnostics` bridge.
- `set` (`crates/logit-transforms/src/set.rs`): `logit.transform.set.resource.rebuilt` (count) —
  fires only on a `map_resource` cache miss (a batch whose incoming resource `Arc` isn't the one
  cached from the last call), so a config that defeats the one-entry cache (a listener minting a
  fresh `Arc` per batch, `otlp_in` chief among them) is visible as a rate rather than invisible.
  Absent entirely when `set` has no `resource:` configured (`map_resource` returns before touching
  telemetry) — see [ADR `operator-declared-resource-attributes`](../adr/operator-declared-resource-attributes.md).
- `trace_context` (`crates/logit-transforms/src/trace_context.rs`): `logit.transform.trace_context.lifted`
  (count) on a successful lift; `.skipped{reason="missing"|"invalid"}` otherwise — `missing` when
  the configured `trace_id` attribute isn't present at all, `invalid` when it (or a configured
  `span_id`/`flags`) is present but doesn't parse. The `kv_metrics` `.derived`/`.derived.skipped`
  pattern, applied to lifting a trace context instead of deriving a metric — see
  [ADR `log-record-trace-context`](../adr/log-record-trace-context.md).
- `stdio_out` (`crates/logit-outputs/src/stdio.rs`): `logit.output.batch.bytes` — direct parity
  with `influxdb_out`'s own batch-bytes metric. Also has no `Diagnostics` (a write error
  propagates as a hard failure today, with no `warn_throttled` call site to bridge).
- `lua`/`lua_file` (`crates/logit-script`, `crates/logit-pipeline/src/runtime.rs::run_lua`):
  `logit.script.vm.memory` (the Lua VM's own `used_memory()`, the strongest single signal of a
  leaking stateful script) and `logit.script.events.emitted{outcome}`, both from the Rust side —
  plus, uniquely among all these, a **script-facing** `telemetry` global a script itself can call
  (`telemetry.count(...)`/`.gauge(...)`), for domain facts only the script knows. See "Metrics from
  Lua scripts" below and `docs/design/lua-api.md`.
- `influxdb_out` (`crates/logit-outputs/src/influxdb.rs`): `logit.output.requests{class="2xx|
  4xx|5xx|network_error"}`, `logit.output.request.duration` (per attempt), `logit.output.batch.bytes`
  — the encode/HTTP-response detail a generic `send.duration` timer can't distinguish. A
  `MetricKind::GaugeDelta` reaching this encoder unresolved (`docs/adr/relative-gauge-adjustments.md`
  — means the pipeline is missing an `aggregate` component) reports under its own
  `logit.component.diagnostics{key="gauge_delta_unresolved"}`, not the generic `encode_error` every
  other unrepresentable kind uses, specifically so it's greppable on its own. **Not**
  `logit.output.retries` — retry moved out of this sink entirely
  (`docs/adr/buffered-sink-delivery.md`) into the generic `deliver_with_retry` every sink now
  shares, so retry counting is a Layer 2 metric (`logit.component.retries`, above), not something
  each sink tracks for itself.
- `syslog_out` (`crates/logit-outputs/src/syslog.rs`): `logit.output.batch.bytes`,
  `logit.output.request.duration`, `logit.output.requests{class="ok"|"error"}` — the same shape as
  `influxdb_out`'s, minus the HTTP-specific status classes, since there's no response to classify.
  Plus detail neither of the other two sinks needs: `logit.output.events.skipped` (events with no
  `log` record — nothing to render as a syslog message, ADR `multi-payload-events`), `logit.output.messages.
  truncated` and `logit.output.messages.dropped{reason="oversize_header"|"oversize_datagram"}`
  (per-message size handling, `docs/adr/syslog-output.md`'s "Sizing" section). Retry stays a
  Layer 2 metric here too, for the same reason as `influxdb_out`.

## Metrics from Lua scripts

`telemetry.count(name, n, tags?)` / `telemetry.gauge(name, v, tags?)` are callable from a script's
`process()`/`flush()` (`crates/logit-script/src/telemetry.rs`, wired in via
`ScriptWorker::with_telemetry` — a builder, not a constructor parameter, so it doesn't touch
`logit-script`'s existing `ScriptWorker::new(script)` call sites). Points a script emits go
through the exact same buffer, `internal` component, and downstream tools as everything else —
there's no separate script-telemetry pipeline to configure.

**Cardinality here is convention-enforced, not type-system-enforced.** Every Rust `Telemetry` call
takes `&'static str` names/tags specifically so cardinality is bounded by code the type system
checks. A Lua-provided string can't satisfy that at compile time, so it's round-tripped through
the process's own interner (`interner::resolve(interner::intern(s))`, which genuinely returns
`&'static str`) — reusing existing, already-accepted infrastructure rather than a new leak
mechanism, at the cost that a script author (not the compiler) is now the one responsible for not
building a metric name or tag value out of per-event data. Full reasoning, including the
alternatives considered: [ADR `lua-authored-telemetry-cardinality`](../adr/lua-authored-telemetry-cardinality.md). See
`docs/design/lua-api.md`'s "Emitting telemetry from a script" for the script-author-facing version
of this same warning.

No `timing()` for scripts: the sandboxed stdlib exposes no clock (`table`/`string`/`math` only),
so there's no way for a script to produce a duration to hand it.

**Two more boundaries [`crates/logit-script/src/telemetry.rs`] holds, both because a script's
input is less constrained than a Rust call site's:**

- **Checked before anything else, on every call: `Telemetry::is_enabled()`.** Reading a Lua
  argument, converting it, and interning it are all real work — a disabled handle (no `internal`
  component configured) has to skip every bit of that, not just the eventual `Telemetry::count`
  call, or a pipeline with telemetry "off" would still permanently intern whatever a script passes
  it. This is what makes the zero-cost-when-disabled guarantee hold all the way to the Lua
  boundary, not just at the Rust one.
- **The `logit.` prefix is reserved.** A `(name, tags)` key in a component's buffer carries no
  notion of which caller wrote to it — a script calling `telemetry.count("logit.component.
  events.received", 1)` would coalesce into (and corrupt) the exact key the runtime itself writes
  to, since `count` and `gauge` on the same key silently convert one into the other. Rejected with
  a clear Lua error naming the reserved namespace, not a silent collision.

Also worth knowing, since a Lua tag key is script-chosen rather than fixed at a Rust call site:
`component`, `kind`, and `role` are reserved for a point's own identity and can never become part
of a tag, at two levels. `PointKey::new` (`crates/logit-core/src/telemetry.rs`) filters a reserved
key out *before* a point's cardinality key is built — not just at drain time — because overwriting
the label alone would still leave two differently-tagged calls (`{kind = "a"}` vs. `{kind = "b"}`)
occupying two distinct, wasted key slots that drain to externally indistinguishable points instead
of coalescing into one. That's a framework-level guarantee, holding for any caller. The Lua binding
additionally *rejects* a reserved tag key outright (`crates/logit-script/src/telemetry.rs`) rather
than silently relying on that filter — a script that set one probably meant something by it, so a
clear error surfaces the mistake instead of a silent no-op.

## Adding a new internal metric

1. Decide which layer it belongs to. Uniform across every component of a kind? It probably
   belongs in `runtime.rs`/`fanout.rs`, not in one component. Specific to what one component knows
   internally? It belongs on that component, via its own `Telemetry` handle.
2. Pick a name following the scheme above, and tags that are `&'static str` constants only.
3. Call `count`/`gauge`/`timing`/`timer` at the point that already knows the fact. No new type, no
   registration step, no schema change.
4. If it's genuinely new ground (a new signal type, a new source of process-level facts), read
   ADR `internal-telemetry-as-pipeline-events`'s "Alternatives considered" first — several shapes that look like natural extensions
   were deliberately not built yet, for stated reasons.

## What this is not

- **Not a time-series aggregation engine.** The buffer coalesces to bound volume between drains;
  any real windowed aggregation is `aggregate`, attached downstream like any other consumer.
- **Not a scrape endpoint.** There is no pull path and no plan for one — see ADR `internal-telemetry-as-pipeline-events`'s
  alternatives for why.
- **Not the `tracing` migration.** `Diagnostics`'s stderr output and this telemetry layer are both
  still separate from the deferred `tracing` migration `docs/known-gaps.md` names; that migration,
  when it lands, is a plausible future *producer* into this same buffer, not a replacement for it.
