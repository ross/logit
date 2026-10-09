# Internal telemetry

How `logit` observes its own behavior: the emit API, the buffer it feeds, the `internal` source
that drains that buffer into the graph, and the naming and tagging conventions every component
follows. Decision record: [ADR `internal-telemetry-as-pipeline-events`](../adr/internal-telemetry-as-pipeline-events.md).
This document is load-bearing per `AGENTS.md`: read it before adding a new internal metric or
touching `logit_core::telemetry`.

To find what a `logit.*` name means, see [Naming](#naming) for the namespaces, then
[Two layers of instrumentation, one buffer](#two-layers-of-instrumentation-one-buffer): layer 2
lists the metrics every component gets from the runtime, and layer 3 has one subsection per
component kind.

## Why this exists

Without it, `logit` can't say anything about itself: how many events a component sourced, what it
dropped, how long a sink's writes take, or whether a node is stalled on backpressure.
`Diagnostics` (`crates/logit-core/src/diag.rs`) only prints throttled stderr lines. This document
is more about *the mechanism* than about any specific counter, because which counters matter in
operation is something to learn by running `logit`, not to guess up front.

## Shape: `logit`'s own event model, through an ordinary component

Internal telemetry is not a new subsystem. A component records a point into a buffer, and the
`internal` component (`crates/logit-inputs/src/internal.rs`) drains the buffer into ordinary
`Event`s carrying `MetricRecord`s, the same type `statsd_in` produces. Those events flow through
the graph like any other source's:

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

Nothing downstream needs to know it's handling telemetry rather than user data. `keep`, `lua`, and
every sink already work.

## Resource identity

Every batch `internal` sends carries `service.name = logit` on its `Arc<Resource>`
(`crates/logit-inputs/src/internal.rs`), built once in `InternalInput::new` because the resource is
batch-level and identical on every tick. This lets an OTLP backend (Tempo, in the demo) resolve a
root span's service. Without it, the root span still arrives but has no `service.name`, and
Grafana's Traces Drilldown renders that as `<root span not yet received>`, the same text it shows
for a genuinely missing span. Don't mistake one failure for the other.

`internal` is the only input allowed to make this claim. `service.name` names *the producer* of
the telemetry, not the source of the data, and `internal`'s telemetry genuinely is `logit`'s own.
Inputs fall into three categories:

- **No claim.** `syslog_in`/`statsd_in`/`lines_in` always use `Resource::default()`. The data they ingest
  belongs to whatever service sent it (one statsd listener may serve several), so stamping `logit`
  would misattribute it. `otlp_in` makes no claim either: it preserves whatever resource the sender
  attached rather than manufacturing one.
- **Genuine self-claim.** `internal`.
- **Discovered facts.** `docker_in` stamps `container.*` (id, name, image, opt-in labels), read
  locally off the container's own `config.v2.json`. These are facts about where the data came
  from, with the same standing `syslog_in`'s parsed hostname has, not an assertion `logit` makes up.
  `docker_in` still never claims `service.name`/`service.namespace` on the operator's behalf. That
  stays a `set` transform's job downstream
  ([ADR `operator-declared-resource-attributes`](../adr/operator-declared-resource-attributes.md)),
  which is what the demo's `nginx_identity` does. `set`'s `map_resource` overlays onto whatever
  resource it's handed, so `container.*` survives downstream of it untouched.

**Why `service.name` alone, and not `service.version`.** In the demo, `victoria_out` also sources
`self`, and `prometheus_out` folds resource attributes into labels (`logit_proto::prometheus`'s
"Encode" table), as `influxdb_out` folds them into tags, so this attribute is a label on every
`logit.*` series. A constant tag is a one-time, harmless addition to series identity; a version tag
would re-key every series on each release. The OTLP instrumentation scope carries the version
instead, without that cost: `internal` stamps
`Scope { name: "logit", version: env!("CARGO_PKG_VERSION") }` on every batch it sends
(`crates/logit-inputs/src/internal.rs`'s `InternalInput::scope`), `Arc`-shared across the batch
like its `resource`.

`internal` is the only real producer of that scope. No codec invents it: a batch with
`scope: None` encodes as an empty `InstrumentationScope` (empty name), never a fabricated
`"logit"`/version (`crates/logit-proto/src/otlp/common.rs`'s `scope_to_pb`/`pb_to_scope`,
[ADR `metrics-model-v2`](../adr/metrics-model-v2.md)). So `internal` stamps the scope itself rather
than relying on a codec default.

## The emit API

`logit_core::telemetry::Telemetry` is a component's handle, mirroring `Diagnostics`'s shape:

```rust
telemetry.count(name: &'static str, n: f64, tags: &[Tag]);
telemetry.gauge(name: &'static str, v: f64, tags: &[Tag]);
telemetry.timing(name: &'static str, d: Duration, tags: &[Tag]);
let timer = telemetry.timer(name: &'static str);   // records on Drop, or explicitly via .stop(tags)
```

`Telemetry::default()` is the disabled handle every component starts with. Every method on it is
an immediate no-op: no allocation, no lock, and (via `Timer`) no clock read. A live handle only
reaches a component through `Registry::telemetry_for` (below), which exists only when the config
has an `internal` component. So a config with no `internal` component pays nothing beyond a branch
on `Option::None`.

**Tags are `(&'static str, &'static str)` pairs by convention.** Both halves must be
compile-time-constant strings: `("class", "5xx")`, never a raw path, peer address, or anything else
derived from traffic. The type system enforces only `'static`; review enforces the rest, and every
shipped component follows it. It matters more here than for ordinary event attributes: the
process-wide interner never evicts (`docs/known-gaps/runtime.md`), so a runtime-derived tag *value*
leaks for the life of the process, as a metric name embedding a request id would.

## The buffer: coalesce between drains, using the merges `aggregate` already performs

Points are kept in a per-component `ComponentBuffer`, keyed by `(name, tags)`. A repeat of the
same key merges into the pending point rather than queueing a second one:

| Kind | Coalesced by | Emitted as |
|---|---|---|
| count | sum | `MetricKind::Sum` (produced via `MetricKind::counter(v)`) |
| gauge | last write wins | `MetricKind::Gauge` |
| timing | samples merged into one sketch | `MetricKind::Distribution(DdSketch)` |

These are `logit-transforms::Aggregator`'s own merge rules (`Accumulator::Sum` sums, `Gauge` is
last-write-wins, `Distribution` merges sketches). The identity is load-bearing: because the merges
are the same, an `aggregate` component attached downstream of `internal` extends them to any real
time window *correctly*. The buffer itself has no notion of a time window; it holds whatever has
accumulated since the last drain.

The buffer sketches timings into one running `DdSketch` between drains rather than retaining raw
per-timing values, even though `MetricKind::Samples` exists. ADR
`internal-telemetry-as-pipeline-events` explains why a mergeable running sketch is the right shape
for internally generated points.

**Cardinality is capped.** A component's buffer holds at most 1024 distinct `(name, tags)` keys
(`telemetry::MAX_KEYS_PER_COMPONENT`). A new key beyond the cap is dropped and counted as
`logit.internal.points.dropped{reason="cardinality"}` under that component's own
`component`/`kind`/`role` attributes. This is the bound-and-count-the-drop convention every mature
statsd client uses for its own send failures, and it makes a component that violates the tag
convention visible instead of silently growing the interner. Under the tag convention the cap never
fires in practice, because cardinality is bounded by the metric names and constant tag values in a
component's code, not by traffic.

## Spans

A span records one node's visit to one unit of work.
[ADR `trace-context-propagation-on-delivered`](../adr/trace-context-propagation-on-delivered.md)
put a real `TraceContext` on every `Delivered`, and `Transform::flush` carries a bounded
`Vec<SpanLink>` per emitted event. [ADR `internal-span-emission-and-deterministic-sampling`](../adr/internal-span-emission-and-deterministic-sampling.md)
turns that context into an emitted `SpanRecord` and adds the sampling knob that span volume needs
and metric volume never did.

### One span is one node's minted `TraceContext`

A span is not "one node's processing of one batch". The two differ for `Transform::flush` (an
*n*-to-1 emission with no single incoming batch) and for a flush spanning several resource groups,
which is one unit of work and must not mint several unrelated roots. The runtime mints a context
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

A fan-out (one batch, several downstream consumers) records exactly one span. It's one
`send_with_own_context` call by one node, and each of the *N* consumers mints its own child later,
on its own visit. The span belongs to the emission, not the edge.

### The emit API

Mirrors `Telemetry::timer`'s shape:

```rust
let mut span = telemetry.span(op, kind, trace_id, span_id, parent_span_id);
span.events(n);            // how many events this emission carries
span.link(link);            // or .links(iter) -- bounded, see below
span.tag("fault", "ambiguous");
span.error();                // or .ok() -- defaults to Ok
                             // dropped, or .finish(), to record it
```

`op` is one of `"process"|"flush"|"send"|"deliver"`. At drain time it's joined with the
component's own `kind` to form the span's `name` (`"aggregate process"`, `"influxdb_out deliver"`).
The sample decision (below) is made *inside* `span`, before any span-shaped state exists. An
unsampled trace gets the same disabled `SpanGuard` a disabled handle's `timer()` returns: every
method is an immediate no-op, with no allocation and no clock read beyond the one sampling
comparison.

### The sampler: deterministic on `trace_id`

```rust
pub fn trace_is_sampled(trace_id: &[u8; 16], rate: f64) -> bool
```

Every node, and every `logit` process in a split-collection topology (`docs/OVERVIEW.md`),
computes the same keep/drop verdict independently from `trace_id` alone. A kept trace is kept at
*every* hop and a dropped one is dropped at every hop, with no propagated bit and no extra bytes on
`TraceContext`/`Delivered`. It has the same shape as OTel's `TraceIdRatioBased` sampler: the top 53
bits of the low 8 `trace_id` bytes (`f64`'s exact-integer range) compared against `rate`.

The rate lives on `Registry` (`Registry::with_span_sampling(rate)`) and is process-wide; graph
rule 13 already guarantees at most one `internal` component. It's copied into each
`ComponentBuffer` at construction, so a live span never needs a second lock. Config:

```yaml
self:
  type: internal
  interval: 10s
  span_sample_rate: 0.1   # the default; 1.0 keeps everything, 0.0 turns spans off
```

The default is below `1.0` (`DEFAULT_SPAN_SAMPLE_RATE = 0.1`) because span volume has a different
shape from metric volume: one span per node visit per batch, where a metric point coalesces
between drains. The field is `span_sample_rate`, not `sample_rate`, because a `ComponentKind::Sample`
transform already exists. Graph validation rule 16 rejects a non-finite or out-of-`[0, 1]` value:
`trace_is_sampled` treats NaN as "keep everything", which is a surprising result to get from a typo.

### The bound: a plain `Vec`, not a keyed map

A point's `PointKey` map coalesces repeats at the same `(name, tags)` key. Two spans never share an
identity to coalesce on, so `ComponentBuffer` holds spans in a separate, unkeyed `Vec`, capped at
`MAX_SPANS_PER_COMPONENT` (512). That's a volume bound, not a cardinality one: nothing else bounds
how many spans accumulate except drain interval × sample rate. `SpanGuard::link`/`links` also cap
each span's own link list at `MAX_LINKS_PER_SPAN` (32). Both drop and count rather than grow:
`logit.internal.spans.dropped{reason="buffer_full"}` on the buffer (drained alongside
`points.dropped`), and `logit.internal.span.links.dropped{reason="cardinality"}`, recorded
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

The timestamp row is the one place `ComponentBuffer::drain(now)` ignores its own `now` argument.
`Event::timestamp` *is* the span's start (`SpanRecord`'s own doc comment), so stamping it with the
drain time would make every span look later than it was by however long it sat in the buffer.

## Logs

`TelemetryLayer` (`crates/logit-core/src/telemetry.rs`) is a `tracing_subscriber::Layer` that
captures every `logit`-targeted event at or above a threshold into the same per-component buffer
that points and spans drain from. It's the producer ADR `internal-telemetry-as-pipeline-events`
predicted: "a future `tracing` subscriber could itself feed `Diagnostics`/`Telemetry`, same as any
other producer." ADR `tracing-for-self-logging` covers why `logit` adopted `tracing`; this section
covers only the capture into the buffer.

### The emit API

Logs have no per-component emit method the way `Telemetry::span`/`.count`/`.gauge` do. A component
never records a log by calling its own `Telemetry` handle. Instead, `TelemetryLayer::on_event`
captures centrally whatever `tracing::warn!`/`Diagnostics::warn` already emits: it reads the
event's `component`/`key` fields and rendered message, then calls the same
`Registry::push_log(component_id, log)` a component-level method would have:

```rust
let layer = TelemetryLayer::new();               // starts inactive: every event a no-op
layer.activate(registry, Severity::Warn, "self"); // "self" = the internal component's own id
```

The layer starts inactive on purpose. `logit-cli::main` installs it inside the global `tracing`
subscriber *before* the config is loaded, because there's no stable API to add a layer to an
already-installed subscriber. It `activate`s the layer once the config's `internal` component (if
any) and its `logs:` threshold are known. A config with no `internal` component, or with
`logs: off`, never activates it: the same zero-cost-when-unconfigured shape `Telemetry::default`
has for points and spans.

Where an event lands:

- **With a `component` field:** that component's own buffer, under its `key` field if present, or
  under the placeholder `"log"` if not (`Diagnostics::warn` never sets one).
- **Without a `component` field** (a runtime lifecycle event like `ready` or
  `shutdown signal received`): the `internal` component's own buffer, under the stable key
  `"process"`.

### The bound: a plain `Vec`, capped like spans

`ComponentBuffer` holds captured logs in an unkeyed `Vec<PendingLog>`, capped at
`MAX_LOGS_PER_COMPONENT` (256). It uses the same volume-bound, drop-and-count shape as spans
(`logit.internal.logs.dropped{reason="buffer_full"}`), for the same reason: nothing else bounds how
many accumulate except drain interval × how chatty a component's diagnostics are. The drained
`Event::timestamp` is capture time, not drain time, as for spans: a log line that looks later than
it was would be actively misleading.

## `internal`: the drain

```rust
ComponentKind::Internal { interval: Duration, span_sample_rate: f64, logs: InternalLogs }
```

`internal` is a listener (`Role::Listener`: no `sources`, needs ≥1 consumer), like `statsd_in`.
`interval` serves two purposes:

1. **Drain cadence.** Every registered component's buffer is drained and its points emitted as one
   batch.
2. **Sampling tick for process-level metrics**, facts tied to no occurrence, so nothing else has a
   reason to push them: resident memory, CPU time, threads, open files, interner size, and uptime.
   See [Process-level metrics](#process-level-metrics).

A config can have at most one `internal` component (graph validation rule 13,
`crates/logit-pipeline/src/graph.rs`). Two would each drain, and so split, the same process-wide
`Registry`.

**Pick an `interval` that divides evenly into any downstream `aggregate` interval.** `internal`'s
drain boundary and `aggregate`'s window boundary are independent clocks. If they don't divide
evenly, a window straddles two drains in a way that isn't reproducible from run to run. DogStatsD
documents the same rule for its aggregation interval against the Agent's flush interval, for the
same reason.

**Shutdown drains once more.** `InternalInput` overrides `Input::run_until_shutdown`
([ADR `decoupled-listener-io`](../adr/decoupled-listener-io.md)) to run one final drain when the
shutdown signal fires, instead of being cancelled by drop. Otherwise every SIGTERM would lose
everything buffered since the last tick, up to a whole `interval`. That future owns the `Fanout`, so
no downstream node starts its close-time flush until the final batch is sent, and `run_input`'s
`shutdown_grace` (5s for `internal`) bounds the wait. The final drain can't emit its own
`points.emitted`/`drain.duration`, which are always recorded a tick behind and have no tick left.

`internal` records its own points (`logit.internal.points.emitted`,
`logit.internal.spans.emitted`, `logit.internal.logs.emitted`, `logit.internal.drain.duration`)
through its own `Telemetry` handle, registered in the same `Registry` it drains. They ride along in
the *next* drain, one tick behind, because a drain can't include a count of itself. Mature statsd
clients count their own packets sent/dropped the same way. `logs.emitted` is counted separately from
`points.emitted` for the same reason `spans.emitted` is: a log event carries neither `metrics` nor
`span`, so it would otherwise be miscounted as a point
(`crates/logit-inputs/src/internal.rs::tick`'s fold checks `event.log.is_some()` first).

### Process-level metrics

`internal` samples these on every tick, before its drain, so they arrive in that same drain
([ADR `process-level-metrics`](../adr/process-level-metrics.md)). Each rides on `internal`'s own
`Telemetry` handle and so carries `internal`'s `component`/`kind`/`role`, like every other point.
Without that handle, nothing is sampled.

| Name | Kind | Meaning and source |
|---|---|---|
| `logit.process.memory.resident.bytes` | gauge | Resident set size: `/proc/self/status` `VmRSS:`, kB × 1024. It can lag slightly, because the kernel batches per-thread RSS counters. |
| `logit.process.memory.allocated.bytes` | gauge | Bytes live in the Rust global allocator: jemalloc's `stats.allocated`, when built with the `jemalloc` feature. |
| `logit.process.threads` | gauge | Threads in the process: `/proc/self/status` `Threads:`. |
| `logit.process.fds` | gauge | Open file descriptors: the entries in `/proc/self/fd`, less the one the listing holds. |
| `logit.process.fds.limit` | gauge | The soft `Max open files` limit from `/proc/self/limits`. Not emitted when it's `unlimited`. |
| `logit.process.cpu.seconds{mode=user\|system}` | delta counter | CPU seconds since the previous tick: `/proc/self/stat` `utime`/`stime`, over `USER_HZ` (100). |
| `logit.process.interner.strings` | gauge | The process-wide interner's size (`interner::len()`), which never shrinks: the observability hook `docs/known-gaps/runtime.md` names. |
| `logit.process.uptime` | gauge | Seconds since `internal` started. |

**The procfs points are Linux-only**, and each of their four sources (`status`, `stat`, `fd`,
`limits`) latches off on its own first failed read, with one diagnostic: `debug` on a non-Linux
build, `warn` for a failed read on Linux. A sandbox that hides one file silences only the points
that file feeds. The allocator gauge doesn't depend on procfs.

**CPU is a delta, like every other `internal` counter.** The first tick's delta is CPU time since
process start, so a cumulative total downstream equals the kernel's own counter. Every tick emits
both modes, a zero delta included: a cumulative `aggregate` evicts a series idle for
`series_retention` windows, and the restarted total would read as a counter reset. What an operator
sees depends on the leg:

- **Prometheus behind `aggregate` with `temporality: cumulative`:**
  `logit_process_cpu_seconds_total{mode}`, where `rate()` is cores in use per `mode` (sum over
  `mode` for the total).
- **`prometheus_out` without a cumulative `aggregate`:** it skips a delta `Sum` and counts it as
  `logit.output.metrics.skipped{metric_kind="delta_sum"}`.
- **InfluxDB behind a delta `aggregate`:** CPU seconds per window per mode, so the value divided by
  the window length is utilization. The first window includes the CPU spent starting up.

**Resident memory is more than the allocator's heap.** A gap between the two isn't a leak by itself:

```text
resident ≈ allocated
         + jemalloc overhead and fragmentation
         + LuaJIT machine code
         + the binary's mapped pages and thread stacks
         + the UDP read slab
```

mlua creates each Lua VM with an allocator over the Rust global allocator, so LuaJIT's GC heaps
count in `allocated` as well as `resident`. A script that retains memory raises both, and
`logit.script.vm.memory` attributes that share per VM. Only LuaJIT's machine-code areas, mapped
directly with `mmap`, sit outside `allocated`.

### Reading an attribution dump

`script/perf attribute --scenario NAME` (`crates/logit-perf/src/attribute.rs`,
[ADR `load-test-harness`](../adr/load-test-harness.md)) reads these points back mechanically, and
it's a useful debugging tool in its own right. It copies a `perf/scenarios/*.yaml` to a temporary
directory, appends an `internal` component and a `file_out` sink with `format: native`, runs it,
and sends SIGTERM after the generator finishes. It then decodes the output file with `logit_proto`'s
own frame reader and native decoder.

Grouping the decoded points by their `component` attribute turns the tables below into a per-node
table: events in/out, Σ `process.duration`, Σ `send.blocked.duration`, Σ `send.duration`, peak
`buffer.utilization`, and drops by `reason`. A one-line verdict names the node with the largest Σ
process time, plus each node that spent time blocked in `send`. For a blocked node, the constraint
is that node's *consumer*, not the node reporting the time. A consumer's
`inbox.blocked.duration` names that constraint directly, so read it before tracing the graph by
hand.

The tool depends on exactly two things from this document: the `component`/`kind`/`role` identity
on every point, and the shutdown drain above. Without the final tick, a short scenario loses its
last partial `interval`, which on a five-second run is a fifth of the measurement. A scenario that
already has its own `internal` component is refused rather than rewritten, because graph rule 13
allows at most one. See `docs/design/performance.md` for the surrounding methodology.

## Naming

Dotted, lowercase, and namespaced by where the metric comes from:

| Namespace | What it covers |
|---|---|
| `logit.component.*` | The uniform set every component gets from the runtime (layer 2, below). |
| `logit.<kind-family>.*` | Component-specific detail (layer 3), for example `logit.input.datagrams` and `logit.output.requests`. |
| `logit.tls.*` | A TLS component's certificate files: reloads and the expiry of the certificate it serves. See [TLS certificate reload](#tls-certificate-reload). |
| `logit.process.*` | Facts about the running process, not any one component. They ride on `internal`'s own `component`/`kind`/`role`; see [Process-level metrics](#process-level-metrics). |
| `logit.internal.*` | Facts about the `internal` component itself, including `logit.internal.points.dropped`, which names the *offending* component through its `component` attribute, not through the metric name. |

Metric names never include an event type (`logit.component.events_in`, say), because `internal`
carries logs and spans as well as metrics.

Every point also carries its component's identity as `component`, `kind`, and `role` attributes.
Those three tag keys are reserved; see [Metrics from Lua scripts](#metrics-from-lua-scripts).

## Two layers of instrumentation, one buffer

**Layer 2** is the runtime instrumenting itself, uniformly, with no component code. **Layer 3** is
what a component adds because only it knows. Both layers write into the *same* `ComponentBuffer`:
`build_spec` (`crates/logit-cli/src/pipeline.rs::build_spec`) computes one `Telemetry` handle per
component and hands it to both the component and the runtime's per-node instrumentation, so a
drain sees one coherent picture per component, not two.

### Layer 2: the runtime

Layer 2 comes from `ComponentKind`'s role and the node runtime alone, the same way arity and
thread-vs-task dispatch do, so it never needs updating when a new component kind lands.

#### Send side: `Fanout`

`Fanout::send`/`send_blocking` (`crates/logit-pipeline/src/fanout.rs`) is the one choke point every
producer sends through (a listener, a `Transform`, a Lua component), so instrumenting it gives
every producer the send-side numbers for free:

| Name | Kind | Meaning |
|---|---|---|
| `logit.component.batches.sent` | count | one per `Fanout::send` call, regardless of fan-out width. A `Fanout::send_with_deadline` (`datadog_in`) that times out sends nothing and counts nothing, span and `send.blocked.duration` included |
| `logit.component.events.sent` | count | events in that batch |
| `logit.component.send.blocked.duration` | timing | time spent inside one `Fanout::send` call (all consumers) |
| `logit.component.events.dropped{reason="closed_consumer"}` | count | a consumer's channel was already closed |

#### Inbox side: recorded under the consumer

`send.blocked.duration` is the producer's total and can't say which consumer it waited on. The
inbox metrics are recorded under the *consumer's* component id by the same send paths
(`Fanout::send*`, the blocking Lua path, and `Fanout::send_with_deadline`), so the consumer that
constrains its producer is the one that carries them:

| Name | Kind | Meaning |
|---|---|---|
| `logit.component.inbox.full` | count | a send found this inbox full, on any send path |
| `logit.component.inbox.blocked.duration` | timing | time a send waited for room in this inbox. Recorded only when the send waited, whereas the producer's `send.blocked.duration` records a sample on every send |
| `logit.component.inbox.batches` | gauge | the inbox's depth, sampled after each receive (`run_transform`, `run_router`, `run_lua`, and a sink's `drain_inbox`). A consumer parked in a full downstream send, or a sink parked on its full store, receives nothing and so reports no new depth; its `inbox.full` and `inbox.blocked.duration` are the signal then |

A send tries the inbox first. On a full inbox it counts `inbox.full`, starts the consumer's blocked
timer, and waits as before. A `Fanout::send_with_deadline` that times out sends nothing, but an
`inbox.full` it observed stays counted, because that observes the consumer's state and isn't a
send. The consumer's blocked timer, dropped by the timeout, records the time to cancel.

A sink's own `buffer.overflow: drop_newest | drop_oldest` keeps its inbox draining, so such a sink
shows `inbox.full` only briefly. A sink under the default `overflow: block` shows sustained
`inbox.full` once its buffer fills (`buffer.utilization` at 1.0).

#### Receive and processing side: the node loops

`run_transform`/`run_output`/`run_lua` (`crates/logit-pipeline/src/runtime.rs`) add the
receive and processing side from their own loops, which already see every batch and event:

| Name | Kind | Recorded in |
|---|---|---|
| `logit.component.batches.received` / `.events.received` | count | `run_transform`, `run_output`, `run_lua`, `run_router`. For `run_output`, this includes every batch its shutdown sweep takes from the inbox |
| `logit.component.batches.delivered` / `.events.delivered` | count | `write_loop`, once per batch `Output::send` delivered. At shutdown a sink's `batches.received` equals `batches.delivered` plus `batches.dropped` under every reason plus what a disk spool still holds, and the same for events |
| `logit.component.process.duration` | timing | `run_transform`, `run_lua`, `run_router` (whole batch — for a router this spans `route_batch`'s partition, not any one destination's send) |
| `logit.component.events.dropped{reason="absorbed"}` | count | `Transform::process` returned `false` |
| `logit.component.events.dropped{reason="script_drop"}` | count | Lua `ProcessOutcome::Drop` |
| `logit.component.events.dropped{reason="unrouted"}` | count | `run_router` or `run_lua`: events no route or `event:to(..)` claimed, at a node with targets and no ordinary consumers. See below. |
| `logit.component.flush.events` / `.flush.duration` | count / timing | a flush-bearing node's `flush()` |
| `logit.component.send.duration` | timing | one delivery attempt, `deliver_with_retry` (`write_loop`); for a sink with a send window above 1, one windowed round (the fill and the head's acknowledgment wait), `deliver_window` |
| `logit.component.retries` | count | a retried delivery attempt or windowed round, `deliver_with_retry` or `deliver_window` (`write_loop`) |
| `logit.component.errors` | count | `Output::send` failed (any attempt), or a Lua script error |
| `logit.component.diagnostics{key=...}` | count | every `Diagnostics::warn_throttled` occurrence, throttled or not |
| `logit.script.vm.memory` | gauge | `run_lua`, once per batch — the strongest signal a stateful script is leaking Lua-side state |
| `logit.script.events.emitted{outcome="emit"\|"emit_many"}` | count | `run_lua`, per `ProcessOutcome` — distinguishes a 1:1 script from a fan-out one |
| `logit.script.vm.gc.forced` / `.gc.duration` | count / timing | `run_lua`, per `max_memory` verdict: the full collections forced by a VM over its cap, rate-limited to about one a second. A steady count means the cap sits too close to the working set |

The last three rows are Lua-specific (recorded in `run_lua`, not shared with
`run_transform`/`run_output`), because only a Lua node has a VM to sample or a script return value
to classify. Every other row applies uniformly across component kinds.

**A Lua node's watcher adds two diagnostic keys and no metric**
([ADR `lua-runaway-script-bounds`](../adr/lua-runaway-script-bounds.md)). `watch_lua_thread`
reads the thread's heartbeat and reports `script_stalled` through `Diagnostics::warn_throttled`
once per stall, when the thread has sat inside one `process()`/`flush()` call with no progress
for its `stall_after` (10 s), so a stall also counts
`logit.component.diagnostics{key="script_stalled"}`.
It reports `script_resumed` through `Diagnostics::info` when progress returns, which counts
nothing. The node's `/readyz` state (`stalled`) is the durable signal; the diagnostic is the
alert. A node wedged at shutdown fails the run with an error naming it, not a diagnostic key.
A script looping over `Event.new` forever, constructing events, advances the heartbeat and is
never stalled or wedged (a loop of refused calls does not advance it): telling it from a large
`flush()` would take a time limit, which the ADR declines.
Events it produced after its channels were revoked count as
`events.dropped{reason="closed_consumer"}` under its own id, and the batches still waiting in its
inbox, which it never read, count as `batches.dropped`/`events.dropped{reason="shutdown"}` under
its own id, as a sink's abandoned inbox does.

**A node over its `max_memory` logs `memory_limit_exceeded`** through `Diagnostics::error`, which
counts nothing, naming the bytes held, the collections run, and the cap, then fails the run like
a wedge, with an error naming the node. The batch that crossed the cap has already been sent; the
batches still waiting in its inbox count as `batches.dropped`/`events.dropped{reason="shutdown"}`
under its own id. A memory failure is not a panic, so it never logs `thread_panicked`.

**`unrouted` is counted explicitly** ([ADR `target-components`](../adr/target-components.md)).
`Fanout` returns early on zero consumers and counts nothing, and the ADR's rule is that unrouted
events are dropped and counted, never silently. A router *with* ordinary consumers never emits
this reason, because its unrouted events go to those consumers. A `lua`/`lua_file` component with
`targets:` and no ordinary consumers follows the same rule, on both its batch path and its
`flush()`.

**`absorbed` is imprecise for filters.** `process_batch` records
`logit.component.events.dropped{reason="absorbed"}` whenever a transform returns `false`, including
every drop by the signal, attribute, and provenance filters and by `sample`. Those components' own
layer-3 counters say why.

**A `target` emits the layer-2 *producer* set and nothing else.** It has no task, no inbox, and no
receive side: it's one `Fanout` carrying the target's own id and telemetry handle
([ADR `target-components`](../adr/target-components.md)). So
`batches.sent`/`events.sent`/`send.blocked.duration`/`events.dropped{reason="closed_consumer"}`
appear under the target's id (per-stream volume, with no new metric), and none of the
`*.received`/`process.duration` rows ever do. The batches a target "sends" were counted `received`
by its routers, not by it.

#### Sinks: `SinkStore`

Every sink gets a `SinkStore` (`crates/logit-pipeline/src/queue.rs`,
`docs/adr/buffered-sink-delivery.md`) between its inbox drain and delivery. Like `Fanout`, it's one
choke point every sink's batches pass through, so every sink gets these metrics with no per-sink
code. The in-memory queue (`SinkQueue`, the default) and the disk queue (`DiskQueue`, opt-in via
`buffer.disk:`, `docs/adr/disk-backed-sink-buffer.md`) emit the same first four rows with the same
meanings, except that `buffer.bytes` is on-disk bytes rather than `estimated_heap_bytes` for a
disk-backed sink:

| Name | Kind | Meaning |
|---|---|---|
| `logit.component.buffer.batches` | gauge | batches currently queued, sampled on every push/commit. For a disk-backed sink, skipping a corrupt region leaves it unchanged: corruption present at `DiskQueue::open` was never counted, and a record corrupted after its push over-counts by one until the next `open` re-derives the count |
| `logit.component.buffer.bytes` | gauge | `EventBatch::estimated_heap_bytes` summed over what's queued (in-memory), or on-disk segment bytes (disk-backed) |
| `logit.component.buffer.utilization` | gauge | `max(batches ratio, bytes ratio)` against the two configured bounds |
| `logit.component.buffer.push.blocked.duration` | timing | how long a `Block`-policy push waited for room; only recorded when a push actually had to wait |
| `logit.component.retrying` | gauge | `1` from the held head's first retryable failure while a retry is pending or in flight, `0` once that head is delivered or dropped and when the write loop exits. A hold drops nothing, so `batches.dropped` stays flat while this reads `1` |
| `logit.component.batches.dropped{reason=...}` / `.events.dropped{reason=...}` | count | `reason` one of `overflow_oldest`/`overflow_newest` (queue eviction), `rejected` (`write_loop`: the destination refused this batch for its own content, or the sink attached no fault), `ambiguous_at_most_once` (`write_loop`: a send of unknown outcome under `delivery: at_most_once`), `shutdown` (`run_output` stopped with an in-memory queue still non-empty, or with batches that never reached the queue: left in the inbox, or held by a push abandoned at shutdown. A disk-backed sink spools those, so it emits `shutdown` only under `delivery: at_most_once`, for a send the shutdown grace cut off mid-flight, which the destination may have taken and which isn't replayed; its sink span carries `fault=ambiguous`), `frame_too_large`/`disk_corrupt`/`disk_full`/`disk_io_error` (disk-backed only, see below) |

`write_loop` also writes four `Diagnostics` keys. `retrying` and `send_failed` carry the fault
class and the destination's error text:

- `retrying` (error): the held head's first retryable failure, then at most once a minute while
  it keeps failing, however long the hold lasts. It names how long the head has been held, the
  failure count, and the queued batch count.
- `send_failed` (warn, throttled to the 1st, 2nd, 4th, ... occurrence): each dropped batch, with
  the time since the last success.
- `degraded` (warn): the first drop since the sink was last healthy.
- `recovered` (info): the first success after a drop or a retried failure.

No send outcome ends the process: a sink that can't deliver holds or drops, and the diagnostics and
the `retrying` gauge are how it shows.

Disk-backed sinks (`DiskQueue`) also emit:

| Name | Kind | Meaning |
|---|---|---|
| `logit.component.buffer.disk.segments` | gauge | segment files currently on disk |
| `logit.component.buffer.disk.replayed` | count | records found between the resume point and the end of all segments, at `DiskQueue::open` |
| `logit.component.buffer.disk.truncated` | count | a torn tail found and truncated at `DiskQueue::open` |
| `logit.component.buffer.disk.errors{op=...}` | count | a failed spool filesystem operation, `op` one of `cursor` (a `cursor.json` write), `flush`, `fsync` (a segment or the spool directory), `create` (a rotation's new segment), `truncate` (the torn-tail repair), `unlink` (a consumed segment). Only `truncate` drops a batch: the push that attempted the repair, also counted `batches.dropped{reason="disk_full"\|"disk_io_error"}` |

Each `disk.errors` point is also diagnosed: `op="cursor"` under
`logit.component.diagnostics{key="cursor_error"}` (the key `DiskQueue::open` already uses for an
unreadable or stale cursor), every other `op` under `key="disk_fs_error"`.

`batches.dropped{reason="disk_corrupt"}` counts spooled bytes that don't parse as a record, in
two places. `DiskQueue::open` counts each corrupt region it resyncs past, or skips to the end of a
segment, from the resume point on. The delivery read path counts one when it resyncs past a
corrupt region to the next record, or skips a corrupt region that runs to the end of its segment
(advancing the cursor as a commit would). The same region can count once at open and again when
delivery reaches it. A region counts once however many records it spanned, so the count is a lower
bound, and a skipped region to the end of a segment counts zero `events.dropped`: how many events
undecodable bytes held is unknowable.

Two metrics from this document's original design were never built: a per-batch
`buffer.wait.duration` (push-to-commit latency) and an `outcome`-tagged `send.attempts`
breakdown. `buffer.batches`/`.bytes`/`.utilization` already answer whether a sink's queue is
backing up, which is the operative question, and `retrying` says whether its head is held.

#### UDP listeners: `ReceiveQueue` and the kernel socket

Every UDP listener gets a `ReceiveQueue` (`logit-inputs::udp`, `docs/adr/decoupled-listener-io.md`)
between the socket read and decode. It's an instance of the same generic `BoundedQueue<T: Queued>`
that `SinkQueue` is: the listener-side mirror of the sink block above, and one choke point every
datagram passes through.

| Name | Kind | Meaning |
|---|---|---|
| `logit.component.receive.datagrams` | gauge | datagrams currently queued, sampled once per *pushed batch* and once per *popped batch* (`BoundedQueue::push_many`/`pop_many`, ADR `udp-intake-batching-and-socket-visibility` — a batch is one `recvmmsg(2)` read on the push side, up to `receive.read_batch` datagrams, and one `pop_many` of the same bound on the pop side) |
| `logit.component.receive.bytes` | gauge | undecoded datagram bytes summed over what's queued |
| `logit.component.receive.utilization` | gauge | `max(datagram ratio, byte ratio)` against the two configured bounds |
| `logit.component.receive.push.blocked.duration` | timing | only under `overflow: block`, only when a push actually waited |
| `logit.component.receive.latency` | timing | arrival (`Datagram::received_at`) → dequeue, per datagram — the number that says whether event timestamps are trustworthy under load |
| `logit.component.datagrams.dropped{reason=...}` / `.bytes.dropped{reason=...}` | count | `reason` one of `overflow_oldest`/`overflow_newest` (`ReceiveQueue` eviction) or `shutdown`: datagrams read but never decoded because the listener stopped (a batch the read half never queued, a popped batch the decode half never decoded, or what the receive queue still held when the grace backstop dropped the listener). With these, `logit.input.datagrams` equals the `receive.latency` sample count plus every reason's drops. `shutdown` counts land after `internal`'s final drain, so no exported pipeline sees them; the listener also logs each at `warn` (see `docs/known-gaps/intake.md`) |
| `logit.component.receive.flushed{reason=...}` | count | `reason` one of `max_events`/`max_bytes`/`interval`/`resource_change`/`shutdown`/`closed` — a `BatchAccumulator` emission. `closed` is **a single tracked file or connection** ending and flushing its own accumulator on the way out: a `tail_in`/`docker_in` file that rotated away or was removed, or a `graphite_in` TCP connection the client closed or reset, or that was dropped for an oversize frame — in every case while the listener itself keeps running. `shutdown` is the whole component stopping. An ordinary client disconnect is `closed`, never `shutdown`. |
| `logit.input.datagrams.truncated` | count | datagrams that arrived longer than the 65,507-byte receive slot and were delivered only as far as it holds, with the remainder discarded by the kernel. **IPv6-only, and Linux-only:** 65,507 is IPv4's maximum payload, IPv6 permits 65,527, and `MSG_TRUNC` in `recvmmsg`'s returned flags is what makes the loss visible rather than silent — a `recv_from` build has no way to see it and never reports this. Not emitted when it is zero, like every other loss counter here |
| `logit.input.reads` | count | read syscalls the listener made — one per `recvmmsg(2)` batch on Linux, one per `recv_from` elsewhere. Exists to be a denominator: `logit.input.datagrams / logit.input.reads` is the **mean fill** of the syscall batch, the only number that says whether `receive.read_batch` is doing anything. A fill pinned at `read_batch` means the knob is the limit and raising it may help; a fill near 1 means datagrams arrive one at a time and the knob is irrelevant at any setting |
| `logit.input.receive_buffer.bytes` | gauge | granted `SO_RCVBUF` after any kernel clamp — the kernel's `sk_rcvbuf`, which on Linux is double what was requested. Emitted at bind *and* re-emitted on every kernel sample below (see "Why a constant is re-emitted") |
| `logit.input.receive_buffer.requested.bytes` | gauge | what `receive.receive_buffer_bytes` asked for, absent when unset — sampled once at bind, and genuinely bind-only: it is config, not a kernel reading |
| `logit.input.receive_buffer.used.bytes` | gauge | `SO_MEMINFO`'s `SK_MEMINFO_RMEM_ALLOC`: bytes the kernel currently charges this socket's receive queue. **Not** queued payload bytes — each packet is charged its `skb->truesize`, several hundred bytes above its own length |
| `logit.input.receive_buffer.utilization` | gauge | `used.bytes / receive_buffer.bytes`, both from the same `SO_MEMINFO` read. 1.0 is not "nearly full" — it is where the kernel begins dropping. Readings *above* 1.0 are normal under load and must never be clamped: the kernel admits a datagram whenever the already-charged total is at or below the ceiling and then charges its whole `truesize` on top, so a saturated queue settles at up to `rcvbuf + truesize` |
| `logit.input.kernel.drops` | count | datagrams the kernel discarded before `recv_from` could return them (`SO_MEMINFO`'s `SK_MEMINFO_DROPS`, the same number `/proc/net/udp`'s `drops` column shows for this socket). A delta between samples; not emitted when it is zero |

The last three rows are Linux-only (`logit_pipeline::sockstat`, `getsockopt(SO_MEMINFO)`, Linux
4.12+) and absent elsewhere. The first failed read logs one diagnostic saying so: a `warn` quoting
the OS error on a Linux kernel that refused the read, or `debug` on a non-Linux build, where there
was never anything to read. After that, the listener stops sampling and stops arming the interval
timer for the rest of its run. Otherwise they're sampled once a second while the read loop runs,
plus **once more after it stops**: a listener usually stops *because* something went wrong, and the
drops in its last second are the ones most worth having.

Three naming choices, because the obvious names collide with existing ones:

- **Drops are `logit.component.*`, not `logit.input.*`.** The generic `BoundedQueue` code emits
  them, as it emits the sink side's `batches.dropped`, and an operator alerting on data loss
  shouldn't have to union two namespaces. The `logit.input.datagrams`/`.datagram.bytes` *arrival*
  counters and `logit.input.reads` stay under `logit.input.*`, because nothing in the runtime can
  see a datagram boundary or a syscall; only the listener knows them.
- **Accumulator emissions are `receive.flushed`, not a bare `batches.flushed`.**
  `logit.component.flush.events`/`.flush.duration` already mean a stateful transform's window
  flush, and `batches.flushed` beside them would read as the same concept.
- **`push.blocked.duration` records only under `overflow: block`**, which is never the receive
  queue's default (see the ADR).

#### TCP listeners: the kernel accept queue

Every TCP listener (`logit-inputs::tcp`, plus the three inputs with their own accept loops:
`logit_in`, `otlp_in`, and `prometheus_in`'s remote-write receiver) gets the stream-side
counterpart, also Linux-only, from `getsockopt(TCP_INFO)` on the listening socket:

| Name | Kind | Meaning |
|---|---|---|
| `logit.input.accept_queue.depth` | gauge | connections that have completed their handshake and are waiting to be accepted (`tcpi_unacked`, which the kernel aliases onto `sk_ack_backlog` for a socket in `LISTEN`) |
| `logit.input.accept_queue.limit` | gauge | the backlog ceiling itself (`tcpi_sacked`, aliased onto `sk_max_ack_backlog`) — what `listen(2)` was given, after `net.core.somaxconn` clamped it. Re-emitted each sample, same reason as `receive_buffer.bytes` |
| `logit.input.accept_queue.utilization` | gauge | that depth against that ceiling. Like `receive_buffer.utilization`, readings *above* 1.0 are legitimate and never clamped: `sk_acceptq_is_full` is strictly greater-than (`include/net/sock.h`) and the queue is incremented after that check, so a `listen(N)` socket settles at `N + 1` and refusal begins just *above* 1.0, not at it |

These are sampled before each `accept()` *and* on the same one-second interval. The per-accept
sample is the depth at the instant that matters; the interval sample keeps a listener that's
blocked in `accept()`, or starved of runtime with a growing queue, from reporting nothing.

**Why a constant is re-emitted.** `logit.input.receive_buffer.bytes` doesn't change after bind, yet
the sampler writes it every second. `ComponentBuffer::drain` (`crates/logit-core/src/telemetry.rs`)
`mem::take`s its point map, so a point written once at bind appears in exactly one `internal` drain
window and then vanishes from the series for the life of the process. That would leave
`receive_buffer.utilization` with no visible denominator a minute in. The bind-time emission stays
too, because it's the only one a process that fails during startup ever makes.
`.requested.bytes` is *not* re-emitted: it's what the operator asked for, which the config already
says, not a reading of anything.

**Where `kernel` appears in a name, and where it doesn't.** Only on the drops counter, where it
says *whose loss this was*. `logit.component.datagrams.dropped` is a drop `logit` chose and can be
sized out of; `logit.input.kernel.drops` is one the kernel took before `logit` had any say. The
remedies differ (see `docs/deploying.md`'s "What to watch for listener intake"), so an operator
needs to tell them apart at a glance. The gauges need no qualifier: `used.bytes` and
`utilization` extend the `logit.input.receive_buffer.*` family that
`receive_buffer.bytes`/`.requested.bytes` established, where "the receive buffer" has only ever
meant the kernel's, so `receive_buffer.kernel.used.bytes` would say it twice. `accept_queue.*`
has no `kernel` segment for the same reason: a listener has no accept queue of its own to confuse
it with.

`utilization` is deliberately the same last segment as `logit.component.receive.utilization` and
`logit.component.buffer.utilization`: three different buffers, one convention (a 0-to-1 fill ratio
against whatever bound that buffer has), so an operator who can read one can read all three.
`accept_queue.limit` is reported in its own right rather than left implicit in the ratio: an
operator deciding whether to raise `net.core.somaxconn` needs the ceiling itself, and backing it
out of `depth / utilization` is undefined at the depth of 0 an idle listener always reports.

### Layer 3: what only a component knows

A component adds its own points through the same `with_telemetry` builder idiom
`with_diagnostics`/`with_timeout`/`with_retry` established
(`crates/logit-cli/src/pipeline.rs::build_spec`). Most components also report `Diagnostics` keys,
which the `Diagnostics` bridge mirrors as `logit.component.diagnostics{key}` (layer 2, above); the
subsections below list both.

The shared TCP stream driver's metrics recur across several listeners, so they're listed once here
and referenced below. A listener on `logit-inputs::tcp::TcpListener` records:

| Name | Kind | Meaning |
|---|---|---|
| `logit.input.connections` | gauge | connections holding a permit, sampled on every connect and disconnect. Published by a drop guard (`crates/logit-inputs/src/listener.rs`), so a connection task that panics still counts itself out |
| `logit.input.connections.rejected{reason="limit"}` | count | a connection closed at the connection cap, before any TLS handshake |
| `logit.input.connections.rejected{reason="proxy_header"}` | count | a connection under `proxy_protocol: true` whose PROXY header was missing, malformed, cut short by the peer closing, or not complete within `handshake_timeout`; read before any TLS handshake |
| `logit.input.accept.errors{reason="connection"\|"resource"\|"fatal"\|"other"}` | count | an `accept()` that failed, by class (`crates/logit-inputs/src/listener.rs`'s `classify_accept_error` has the table). `connection` retries at once; `resource` (fd exhaustion, realistically) and `other` back off 100 ms and continue; `fatal` ends the listener |
| `logit.input.connections.closed{reason="idle"}` | count | an operator-configured `idle_timeout:` closed the connection. Policy, not a fault: counted, never diagnosed, and only possible when the field is set. A close at shutdown is not counted: there is no `reason="shutdown"`, because the runtime would record it after `internal`'s final drain |
| `logit.input.frames` / `logit.input.frame.bytes` | count/sum | frames received, at the protocol's own unit |
| `logit.input.frames.dropped{reason="oversize"\|"drained"\|"malformed"\|"truncated"}` | count | the same per-reason shape `logit.proto.errors{reason}` uses. `drained` is a line-framed listener's line that crossed its bound before its `LF` arrived: counted once, after which the connection discards input until its next `LF` |

The driver's `Diagnostics` keys: `framing_error` (any `frames.dropped` reason), `bad_frame` (a
decoder that rejects a whole frame), `connection_error` (I/O, a TLS handshake that failed or
timed out, or a connection that sent no first byte inside `handshake_timeout` and so gave its
permit back; never an idle close, nor a reset before the connection's first payload byte), `proxy_header` (any `connections.rejected{reason="proxy_header"}`
refusal), and `accept_error` (any `accept.errors` reason). These keys
and the decoder's own `bad_line` throttle listener-wide rather than per connection, because a `Diagnostics` clone shares its original's
counts ([ADR `service-lifecycle-and-output-retry`](../adr/service-lifecycle-and-output-retry.md)'s
2026-09-14 amendment). A peer looping connect / bad frame / close is throttled like any other
repeated failure instead of warning once per TCP handshake. Apart from the certificate reload's
points ([TLS certificate reload](#tls-certificate-reload)), no TCP listener has a TLS-specific
metric: a handshake failure surfaces as `connection_error`.

`logit.input.accept.errors{reason}` and the `accept_error` key are not the driver's alone. Every
connection-oriented listener records them from one shared helper, on each of its accept loops:
this driver's TCP and Unix sockets, `logit_in`, `otlp_in`, `prometheus_in`'s bind mode,
`datadog_in`, `datadog_trace_in`'s TCP and Unix sockets, and `splunk_hec_in`.

There's no stream counterpart of `logit.input.reads`: a stream listener's reads aren't
message-aligned, so a read count over a frame count wouldn't be a fill ratio of anything.

#### TLS certificate reload

Every listener with a `tls:` block (`bind_tls:` on `prometheus_in`) registers its files with the
process's one `TlsReloader` (`crates/logit-pipeline/src/tls.rs`), and so does every client: each
sink's `tls:` block (`endpoint_tls:` on `prometheus_out`) and `prometheus_in`'s `scrape_tls:`. A
client registers its `ca_file` and its `cert_file`/`key_file` pair, whichever are set, and nothing
when neither is. The reloader records these under the owning component:

| Name | Kind | Meaning |
|---|---|---|
| `logit.tls.reloads{outcome="reloaded"\|"failed"}` | count | a check found new content in the component's files and loaded it, or failed to and kept using the previous files. The same failed content counts once, not once per check |
| `logit.tls.certificate.not_after{side="server"\|"client"}` | gauge | the `notAfter` of the leaf certificate in use, in unix seconds: a listener's `cert_file` under `server`, a client's under `client`. A client with no `cert_file` has none. Emitted at startup and after a reload, and re-emitted every second from a cached value, as [`receive_buffer.bytes` is](#tcp-listeners-the-kernel-accept-queue), so it never drops out of the series. Left out if the certificate's validity can't be read. Alert on it approaching now: it's what catches a renewal that never reached the files |

The tag is `side`, not `role`: `role` is reserved for the component's own identity.

Its `Diagnostics` keys: `tls_reloaded` (at `info`, unthrottled, naming the files and the new
certificate's expiry) and `tls_reload_failed` (throttled, naming the file and why it failed).

#### Inputs

##### `statsd_in`

`crates/logit-inputs/src/statsd.rs`.

**Under `transport: udp`:** `logit.input.datagrams`, `logit.input.datagram.bytes`, and
`logit.input.reads`, the per-datagram and per-syscall detail `Fanout`'s per-batch view can't see.
Like `syslog_in`, `statsd_in` is a thin wrapper over `logit-inputs::udp::UdpListener` on this
transport (`docs/adr/decoupled-listener-io.md`), which records the `ReceiveQueue`/`receive_buffer.*`
table with no per-listener code.

**Under `transport: tcp`:** it runs on `logit-inputs::tcp::TcpListener`, as a TCP
`syslog_in`/`graphite_in` does, and records that driver's stream set in place of the datagram
pair, with nothing statsd-specific: `logit.input.accept_queue.depth` / `.utilization`, the
connection metrics, `logit.input.frames` / `logit.input.frame.bytes` (one *frame* is one
LF-delimited statsd line), and `logit.input.frames.dropped{reason}`. Only three reasons can occur:

- **`oversize`:** a line past the driver's 64 KiB bound. Dropped and counted once; the connection
  stays open and the next line still decodes. A statsd listener frames `Lines{DrainToNextLine}` and
  never RFC 6587's octet counting, because a statsd line may legally begin with a digit.
- **`drained`:** an `oversize` line that crossed the bound before its `LF` arrived, counted in its
  place. The connection then discards input until its next `LF`, so a sender that never sends one
  shows as one `drained` and no further frames.
- **`truncated`:** a partial line left buffered when a connection ends: abruptly, on shutdown
  mid-message, **or on a clean close with the final line unterminated**. That last case is where
  statsd parts company with `syslog_in`: RFC 6587 §3.4.2 explicitly permits a terminator-less final
  message and statsd does not, and emitting a half-line would turn a sender dying mid-write into a
  plausible-looking metric. A whitespace-only remainder isn't counted, because nothing was lost.

`malformed` can't occur: it's an octet count RFC 6587's grammar doesn't permit, and this listener
never reads one.

**Under `transport: unix`:** the `udp` set, from the same driver on a Unix datagram socket. The
`SO_MEMINFO` sampler reads it as it reads a UDP socket, but `AF_UNIX` makes a full receive queue
block or refuse the *sender* rather than drop, so `logit.input.kernel.drops` stays at zero there.

**Under `transport: unix_stream`:** the `tcp` set, less `logit.input.accept_queue.*` (a Unix
listener has no `TCP_INFO`). One frame is one length-prefixed packet, which may hold several lines,
and `oversize` is a packet declaring more than 64 KiB, which closes the connection.

**On either transport:** a line that *parses* badly isn't a framing error. It's the decoder's own
`bad_line`, throttled per listener because every connection's decoder clone shares one set of
counts. The driver's `bad_frame` key fires only for the single whole-frame failure
`StatsdDecoder::decode_into` can return: a frame that isn't valid UTF-8. A sampled `ms`/`h`/`d`
line whose `@<rate>` implies a weight above `MAX_SAMPLE_WEIGHT` (the bound on how far decode-time
sample-rate extrapolation can inflate a `Distribution`'s `count()`) is clamped rather than
extrapolated without bound, and reported as
`logit.component.diagnostics{key="sample_rate_clamped"}`. No separate counter is needed, because
the bridge already mirrors every occurrence.

##### `syslog_in`

`crates/logit-inputs/src/syslog.rs`.

**Under `transport: udp`:** the same trio as `statsd_in`, `logit.input.datagrams`/
`.datagram.bytes`/`.reads`.

**Under `transport: tcp`:** it runs on `logit-inputs::tcp::TcpListener`
([ADR `syslog-tcp-ingress-and-tls`](../adr/syslog-tcp-ingress-and-tls.md)) and records the whole
stream set in the table above, where one frame is an RFC 6587 frame. The connection metrics are
`logit_in`'s, reused verbatim. A connection past the cap is closed before any TLS handshake,
because syslog has no in-band reject message to spend one on.

`frames.dropped` reasons:

- **`oversize`:** a frame over the 64 KiB ceiling. Ends the connection.
- **`malformed`:** an octet count RFC 6587 §3.4.1's grammar doesn't permit. Ends the connection;
  neither framing can resynchronize past one.
- **`truncated`:** every way a partial frame gets dropped instead of emitted: a clean EOF mid-frame
  under octet counting, and, on **either** framing, a connection that ended without one at all (a
  peer RST mid-message, or this listener shutting down before the sender finished). Under
  non-transparent framing a clean EOF is *not* truncation: a terminator-less remainder is an
  ordinary final message and is emitted.

A frame that *parses* badly isn't a framing error. `SyslogDecoder::decode_into` is infallible, so a
rejected syslog message reports as the decoder's own `bad_line` on either transport, and the
driver's `bad_frame` key stays unused here. There's no `ReceiveQueue` on this path, so none of the
`receive_buffer.*` table: the connection's own flow control is the queue (graph rule 17).

##### `collectd_in`

`crates/logit-inputs/src/collectd.rs`, [ADR `collectd-binary-relay`](../adr/collectd-binary-relay.md).

**No layer-3 counters of its own.** The shared `UdpListener` driver records
`logit.input.datagrams`/`.datagram.bytes`/`.reads` and the whole `ReceiveQueue`/`receive_buffer.*`
table, and collectd's binary framing gives this listener nothing further only it can see.
`Diagnostics` keys:

| Key | Meaning |
|---|---|
| `bad_datagram` | The driver's own: the datagram's *first* part is malformed, so nothing was salvaged. |
| `bad_part` | A malformed part behind at least one decoded value list. The earlier lists are kept and the rest of the datagram is abandoned. |
| `incomplete_identity` | A value list with an empty host, plugin, or type, which collectd's own receiver rejects too. |
| `encrypted_packet_dropped` | A `SecurityLevel Encrypt` datagram. This codec holds no keys. |
| `types_db_mismatch` | The configured `types_db` defines the list's type with a different data-source count or kinds than arrived, so its records fall back to index naming. |
| `notification_dropped` | A `0x0100`/`0x0101` notification with an out-of-set severity, an empty message, or no host set: the notification counterpart of `incomplete_identity`. |

A type simply *missing* from `types_db` is deliberately not reported, because that's routine, not a
misconfiguration.

##### `graphite_in`

`crates/logit-inputs/src/graphite/`, [ADR `graphite-carbon-relay`](../adr/graphite-carbon-relay.md).

**What it reports depends on its `transport:`**, because the two transports run different drivers.

- **`transport: udp`:** `collectd_in`'s shape exactly. No layer-3 counters of its own;
  `logit.input.datagrams`/`.datagram.bytes`/`.reads`, the `ReceiveQueue` table, and
  `receive_buffer.*` all come from the shared `UdpListener`.
- **`transport: tcp`:** no counters of its own either. It runs on the shared `TcpListener`
  (`docs/adr/graphite-carbon-relay.md`'s 2026-09-14 amendment) and reports exactly what a TCP
  `syslog_in` reports. `logit.input.connections.rejected{reason="limit"}` is the connection cap
  (`max_connections`, 1024 by default) binding; the listener rejects rather than queues because
  carbon's wire has no way to say "try later". Under `proxy_protocol: true`,
  `logit.input.connections.rejected{reason="proxy_header"}` counts a connection refused for its
  PROXY header. `logit.input.frames` / `.frame.bytes` count under
  **both** protocols, where one frame is one plaintext line or one pickle payload, counted at the
  size the decoder was handed (a pickle frame's 4-byte length prefix is stripped first, so the
  count is the payload, not the wire framing).
  `logit.input.frames.dropped{reason="oversize"|"drained"|"malformed"|"truncated"}` and
  `logit.component.receive.flushed{reason}` from the per-connection `BatchAccumulator` (the same
  layer-2 point a datagram listener's shared `decode_loop` records) complete the set. There's no
  receive queue on this transport, because TCP's own flow control is the backpressure, so none of
  the `ReceiveQueue` table appears.

Both TCP framing failures land on `frames.dropped{reason="oversize"}`. They differ in whether the
connection survives: a plaintext line past `max_line_bytes` is dropped and the reader
resynchronizes at the next newline, while a pickle frame declaring more than `max_frame_bytes`
closes the connection, because a length-framed stream has no resync point. A plaintext line that
crossed `max_line_bytes` before its newline arrived counts `reason="drained"` instead, because the
connection then discards input until its next newline. None is `metrics.skipped`: one vocabulary
per driver.

The codec adds its own counters under both transports:
`logit.input.metrics.skipped{reason="bad_line"|"bad_tag"|"bad_timestamp"|"non_finite_value"|
"bad_shape"}` and `logit.input.tags.normalized{reason="duplicate_key"}` (a repeated carbon tag key
collapsing to its last value, which is what carbon's own `TaggedSeries.parse` does). Its
per-connection accumulator flushes as `receive.flushed{reason="closed"}` when a client hangs up and
`{reason="shutdown"}` only when the component itself is stopping.

`Diagnostics` keys: `bound`; the codec's
`bad_line`/`bad_tag`/`bad_timestamp`/`non_finite_value`/`duplicate_tag_key`/`bad_pickle`; and the
driver's `framing_error` (either oversize case above, or a partial frame discarded by an abrupt
close), `bad_frame` (a framed payload the decoder rejected outright, pickle only, because the
plaintext path isolates every failure per line), `connection_error`, and `proxy_header` under
`proxy_protocol: true`. A `connection_error` is
never fatal to the listener or its sibling connections.

##### `lines_in`

`crates/logit-inputs/src/lines.rs`, [ADR `plain-lines-listener`](../adr/plain-lines-listener.md).

It has no layer-3 counters of its own. It runs on the same shared drivers as `statsd_in`, so what
it reports depends on its `transport:`:

- **`udp` and `unix`:** the datagram set `statsd_in` records, from the shared `UdpListener`.
- **`tcp` and `unix_stream`:** the stream set from the shared `TcpListener`: the connection
  metrics, `logit.input.frames` / `.frame.bytes` (one frame is one LF-delimited line), and
  `logit.input.frames.dropped{reason="oversize"|"drained"|"truncated"}`.

The stream framer enforces `max_line_bytes` on `tcp` and `unix_stream`, and the decoder enforces
it on `udp` and `unix`. Both count a dropped line as `logit.input.frames.dropped{reason="oversize"}`,
so the series means the same on every transport. On a datagram transport it's the only series of
that name, and the rest of the datagram still decodes. On a stream transport, a line that crossed
the bound before its `LF` arrived counts `reason="drained"` in place of `oversize`, because the
connection then discards input until its next `LF`.

`Diagnostics` keys: `bound`, the driver's keys, and the datagram decoder's throttled
`oversize_line`.

##### `otlp_in`

`crates/logit-inputs/src/otlp.rs`,
[ADR `otlp-tls-and-pooled-grpc-client`](../adr/otlp-tls-and-pooled-grpc-client.md).

**The connection metrics, and one codec counter.** `logit.input.connections`,
`logit.input.connections.rejected{reason="limit"}` (the connection cap, `max_connections`, 1024
by default, binding), `logit.input.connections.rejected{reason="proxy_header"}` (under
`proxy_protocol: true`, a PROXY header that was missing, malformed, cut short, or late, read
before any TLS accept), and `logit.input.connections.closed{reason="idle"}`. The first three are
the points `logit_in` and the shared TCP driver record, and the shared TCP driver alone records
the `proxy_header` reason, for the same reason: this accept loop rejects at the
cap rather than queueing behind a permit, so there's a refusal to count, and the gauge counts
permit holders only. A connection past the cap is dropped before any TLS accept (OTLP has no
in-band "try later" to spend a handshake delivering), so a rejection is never also a handshake. An
idle close is
`graceful_shutdown()`, a bounded grace, then drop, the same close a stalled request body's
`408`/`grpc-status: 4` reaches.

There's no frame or request counter. This input's unit of arrival is an HTTP request or a gRPC
call, and `Fanout` already sees one batch per accepted request, so a counter would only restate it.

`logit.input.batches.dropped{reason="closed_consumer"}` (count): every batch of a request that no
consumer took, the refused one included, because every consumer of the listener has closed. The
request is answered HTTP `503` with `Retry-After: 1` or gRPC status 14 (`UNAVAILABLE`), never a
`partial_success`. Not disjoint from `logit.component.batches.sent`: the refused batch is also
counted there and once per consumer in `logit.component.events.dropped{reason="closed_consumer"}`;
batches after it that were never offered appear only here.

The OTLP codec counts a metric with no data as
`logit.input.metrics.skipped{metric_kind="unknown", reason="no_data"}`
(`crates/logit-proto/src/otlp/metrics.rs`).

`Diagnostics` keys: `bound`, `connection_error` (one connection's I/O failing, a TLS accept
that failed or timed out, or a plaintext connection held open past `handshake_timeout` without a
first byte, which then gave its permit back; never an idle close), `proxy_header` (any
`connections.rejected{reason="proxy_header"}` refusal), and `forwarded` (under `forwarded:`, a request whose named header names no usable client; the header name and the reason appear in the message text, never the value; a request without the header isn't counted). A plaintext peer that *closes
cleanly or resets* before sending a request byte, after a complete PROXY header under
`proxy_protocol: true`, is not counted: that's what a TCP health check looks like, and counting it
would add one point per probe interval to this key forever.

##### `prometheus_in`

`crates/logit-inputs/src/prometheus.rs`, codec in `crates/logit-proto/src/prometheus/`,
[ADR `prometheus-scrape-and-exposition`](../adr/prometheus-scrape-and-exposition.md) and
[ADR `prometheus-remote-write`](../adr/prometheus-remote-write.md).

**Two modes, two request-level pairs, one shared sample counter.**

| Mode | Name | Kind | Meaning |
|---|---|---|---|
| scrape | `logit.input.scrapes{class="2xx"\|"4xx"\|"5xx"\|"other"\|"network_error"\|"timeout"\|"parse_error"\|"oversize"}` | count | one per target per tick: the HTTP classes plus three ways a scrape fails before or after a status (`parse_error` is a 2xx body that wouldn't decode) |
| scrape | `logit.input.scrape.duration` | timing | one per target per tick, recorded regardless of outcome |
| bind | `logit.input.writes{class="ok"\|"not_found"\|"method"\|"unsupported"\|"oversize"\|"timeout"\|"bad_request"\|"closed_consumer"}`, plus `encoding="snappy"\|"zstd"` on `class="ok"` | count | one per request, one class per row of the module doc's routes table. See below. |
| bind | `logit.input.write.duration` | timing | one per request, every exit included, which is why the count and the timer live in one wrapper around the routing itself |
| both | `logit.input.samples` | count | a different unit in each mode. See below. |

These are this component's own spellings, not `otlp_in`'s, which has no request-level counters to
mirror. In `logit.input.writes`:

- `ok` carries the body's `encoding`, so a vmagent that stayed on its default zstd wire, rather
  than downgrading to Snappy, reads as `encoding="zstd"`.
- `unsupported` is a `415` on `Content-Encoding` *or* `Content-Type`.
- `oversize` is a `413` from either the compressed body or its decompressed size: Snappy's
  declared length, or for zstd a declared content size, a window, or a streaming decode past
  the cap.
- `timeout` is a `408` from a body that stopped arriving. It's **only reachable where
  `idle_timeout:` is set**, because the per-frame stall bound is derived from it and it's off by
  default. On a default `bind:` this class never fires, and a half-uploaded request holds its
  connection permit instead.
- `closed_consumer` is a write no consumer took, answered `503`. Its samples aren't counted in
  `logit.input.samples`, and `logit.input.batches.dropped{reason="closed_consumer"}` counts the
  batch. Not disjoint from `logit.component.batches.sent`: the refused batch is also counted there
  and once per consumer in `logit.component.events.dropped{reason="closed_consumer"}`.

`logit.input.samples` counts the series a scrape decoded (one event per series) in scrape mode. In
bind mode it counts the **wire samples** of a delivered write: every decoded series' samples,
minus those of any series the model mapping then dropped. A write no consumer took counts none.
That's the number the 2.0 `X-Prometheus-Remote-Write-Samples-Written` header reports for that
request, by design: a counter and a header disagreeing about one request would be a puzzle with no
right answer.
`docs/known-gaps/prometheus.md` tracks the unit difference as its own row.

**The bind-mode metadata cache** is the one piece of cross-request state on this kind, and it
reports itself:

| Name | Kind | Meaning |
|---|---|---|
| `logit.input.metadata_cache.size` | gauge | families currently remembered, published whenever the table changes: a transition, like `logit.input.connections`, rather than a per-request restatement |
| `logit.input.metadata_cache.evicted{reason="expired"\|"cardinality"}` | count | a family whose `ttl` ran out, versus one pushed out of `max_families` by a newer one: the same two reasons and least-recently-used shape `prometheus_out`'s `max_series:` uses |
| `logit.input.metadata_cache.replaced` | count | one per family a request retyped. Watch it when a sender's model kinds look wrong: a healthy fleet retypes almost nothing, and a steady stream means two senders disagree about one family name |
| `logit.input.metadata_cache.truncated` | count | one per `# HELP` or `# UNIT` string cut to `MAX_METADATA_TEXT_BYTES` on its way into the table. What's remembered outlives the request that carried it, and the request's size cap doesn't bound a table that keeps entries. The *type* is always remembered exactly, so this bounds what one entry costs, not what it types |

The connection metrics are `otlp_in`'s spelling verbatim, because bind mode runs the same accept
loop and the same shared idle tracker (`crates/logit-inputs/src/http.rs`):
`logit.input.connections` (gauge), `logit.input.connections.rejected{reason="limit"}`,
`logit.input.connections.rejected{reason="proxy_header"}` (under `proxy_protocol: true`, a PROXY
header that was missing, malformed, cut short, or late, read before any TLS accept), and
`logit.input.connections.closed{reason="idle"}` (count). Scrape mode has none of them: it's a
client, with no socket of its own.

**The codec's counters**, on the decode side of both modes, are
`logit.input.metrics.skipped{reason=…}` and `logit.input.metrics.degraded{reason=…}`:

- **Text assembler reasons**, shared by both modes because remote-write decodes through the same
  `assemble::Assembler`: `malformed_line`, `malformed_metadata`, `duplicate_label`,
  `duplicate_series`, `duplicate_type`, `duplicate_metadata`, `unknown_suffix`,
  `incomplete_series`, `empty_histogram`, `non_monotonic_buckets`, plus
  `degraded{reason="histogram_count_mismatch"}`.
- **`skipped{reason="invalid_labels"}`** (remote-write): a series with no `__name__`, an empty label
  name or value, or a label set that isn't strictly ascending by byte order. Both specs forbid a
  sender from producing these, and none is worth failing the whole request over.
- **`skipped{reason="native_histogram"}`** (remote-write): one `histograms[]` entry. See
  `docs/known-gaps/mappings.md`; this is also why a 2.0 response's `Histograms-Written` is always
  `0`.
- **`degraded{reason="exemplar_dropped"}`** (remote-write): an exemplar whose series has no sample
  anywhere in the request, or whose series was itself skipped. This reason is also an encoder
  reason on the output side. It's not additive with `invalid_labels`: a series with bad labels and
  three exemplars raises one `invalid_labels` and three `exemplar_dropped`, because they answer
  different questions.
- **`degraded{reason="seed_mismatch"}`** (metadata cache): a *remembered* type would have made the
  assembler throw a sample away, so it gives way instead and the sample opens an implicit family of
  its own (`crates/logit-proto/src/prometheus/assemble.rs`'s "A seeded type is advisory" table). A
  declaration the request itself carried is a statement about the samples in front of it; one from
  the cache is a memory of what some other message said. It never appears with an empty cache, and
  a steady stream of it means the table and the senders disagree about a family's shape.

`Diagnostics` keys: `bound` (bind mode's listener), `scrape_failed` (scrape mode; the failing
target's redacted URL appears in the message text only, never a tag), `write_rejected` (bind mode,
every `400`/`408`/`413`/`415`; the peer address appears in the message text only, for the same
tag-cardinality reason), `closed_consumer` (bind mode, a `503` for a write no consumer took),
`connection_error` (never an idle close, nor a plaintext peer that closes cleanly or resets
before sending a request byte, after a complete PROXY header under `proxy_protocol: true`; on a
TLS listener that close fails the TLS accept and is counted), `proxy_header` (bind mode, any
`connections.rejected{reason="proxy_header"}` refusal), and `forwarded` (bind mode, under `forwarded:`, a request whose named header names no usable client; the header name and the reason appear in the message text, never the value; a request without the header isn't counted).

##### `datadog_in`

`crates/logit-inputs/src/datadog.rs`, codec in `crates/logit-proto/src/datadog/`,
[ADR `datadog-agent-and-intake-relay`](../adr/datadog-agent-and-intake-relay.md).

**The connection metrics are `otlp_in`'s verbatim**, because this listener runs the same accept loop
and the same shared idle tracker (`crates/logit-inputs/src/http.rs`): `logit.input.connections`
(gauge), `logit.input.connections.rejected{reason="limit"}`,
`logit.input.connections.closed{reason="idle"}`, and the accept-queue gauges. Under
`proxy_protocol: true`, `logit.input.connections.rejected{reason="proxy_header"}` counts a
connection refused for its PROXY header, as on the shared TCP stream driver.

**Unlike `otlp_in`, it counts requests.** A Datadog Agent posts to about a dozen routes, some of
which this listener only acknowledges, so the `Fanout`'s batch count can't say which routes are
arriving or which were refused.

| Name | Kind | Meaning |
|---|---|---|
| `logit.input.requests{route, class}` | count | one per request, every exit included. `class` is `ok`, `rejected`, `busy`, or `closed_consumer` (a `503` for a request no consumer took); `route` is `series_v2`, `series_v1`, `distribution_points`, `sketches`, `service_checks`, `events`, `intake`, `logs`, `traces`, `stats`, `validate` (both validate paths), `health`, one of the acknowledged routes below, or `unknown` for a path this listener doesn't serve |
| `logit.input.request.duration` | timing | one per request, every exit included, time spent waiting on a busy downstream too |
| `logit.input.request.bytes` | count | the compressed body size, once the body has been read |
| `logit.input.requests.rejected{reason}` | count | one per `4xx`: `unknown_route` (`404`), `method` (`405`), `auth` (`403`), `encoding` (`415`), `oversize` (`413`, compressed or decompressed), `stalled` (`408`, only with `idle_timeout:` set), `body_read` (`413` for a body that failed for another reason, such as a client disconnecting mid-upload), `malformed_encoding` (`400`, a stream that doesn't decompress), or `malformed` (`400`, a payload the codec rejects whole) |
| `logit.input.requests.acknowledged{route}` | count | a payload answered `2xx` and never sent: `host_metadata`, `metadata`, `collector`, `container`, and `orch` on every request, and `intake` for host metadata posted to `/intake/` |
| `logit.input.batches.dropped{reason="busy"}` | count | batches a `503` left undelivered, disjoint from `logit.component.batches.sent`: a batch is one or the other. See below |
| `logit.input.batches.dropped{reason="closed_consumer"}` | count | every batch of the request that no consumer took, the refused one included, answered `503`. Not disjoint from `logit.component.batches.sent`: the refused batch is also counted there and once per consumer in `logit.component.events.dropped{reason="closed_consumer"}`; batches after it that were never offered appear only here. |

**A busy request is not a lost one.** When the pipeline doesn't accept a request's batches within
5 seconds, the request gets `503` with `Retry-After: 1`, counted `class="busy"`, and its
undelivered batches are counted `batches.dropped{reason="busy"}`. The Agent keeps the payload and
retries it, so "dropped" here means "not delivered by this request", not "lost". Read a steady busy
rate as a pipeline that can't keep up with its Agents: the Agent's retry queue is absorbing the
difference and drops payloads only once it fills.

Each batch reaches every downstream consumer or none (`Fanout::send_with_deadline`), so the batch
that timed out is counted only under `batches.dropped{reason="busy"}`, never under
`logit.component.batches.sent`, and no consumer holds it. A traces or stats request that decodes to
several batches can still be answered `503` after some of them were fully delivered; those count as
`batches.sent`, and the Agent's retry delivers them again (the module doc's "Backpressure" section,
[ADR `datadog-agent-and-intake-relay`](../adr/datadog-agent-and-intake-relay.md) decision 5).

A timed-out `send_with_deadline` counts nothing sent, but an `inbox.full` it observed stays counted
under the consumer, and the consumer's `inbox.blocked.duration` records the time until the timeout
cancelled the wait.

The codec's own counters (a series, sketch, log, event, check, span, or stats group dropped while
the rest of a request decodes) are in the [`datadog` codec section](#datadog), under this
component's id.

`Diagnostics` keys: `bound`, `connection_error` (never an idle close, nor a plaintext peer that
closes cleanly or resets before sending a request byte, after a complete PROXY header under
`proxy_protocol: true`; on a TLS listener that close fails the TLS accept and is counted),
`proxy_header` (any `connections.rejected{reason="proxy_header"}` refusal),
`forwarded` (under `forwarded:`, a request whose named header names no usable client; the header name and the reason appear in the message text, never the value; a request without the header isn't counted),
`request_rejected` (every rejection except `404` and `405`; the peer address appears in the
message text only, never a tag, and an API key never appears at all), `busy` (a `503`), and
`closed_consumer` (a `503`).

##### `datadog_trace_in`

`crates/logit-inputs/src/datadog_trace.rs`, codec in `crates/logit-proto/src/datadog/`,
[ADR `datadog-agent-and-intake-relay`](../adr/datadog-agent-and-intake-relay.md).

**The connection metrics are `datadog_in`'s, except the accept-queue gauges cover the TCP
listener only.** `logit.input.connections` (gauge), `logit.input.connections.rejected{reason="limit"}`,
and `logit.input.connections.closed{reason="idle"}` count the TCP listener and the Unix socket
together, under one cap. The accept-queue gauges read the kernel's `TCP_INFO`, which a Unix socket
has no counterpart for, so a `socket:`-only listener has none. Under `proxy_protocol: true`,
`logit.input.connections.rejected{reason="proxy_header"}` counts a TCP connection refused for its
PROXY header; the Unix socket never reads one.

| Name | Kind | Meaning |
|---|---|---|
| `logit.input.requests{route, class}` | count | one per request, every exit included. `class` is `ok`, `rejected`, `busy`, or `closed_consumer` (a `503` for a request no consumer took); `route` is `traces_v03`, `traces_v04`, `traces_v05`, `traces_v07`, `stats_v06`, `info`, one of the `404` or stub routes below, or `unknown` |
| `logit.input.request.duration` | timing | one per request, every exit included |
| `logit.input.request.bytes` | count | the body size as sent, once read |
| `logit.input.requests.rejected{reason}` | count | one per `4xx`: `unknown_route` (`404`), `unsupported_route` (`404`, also tagged `route`: `traces_v01`, `traces_v02`, `traces_v10`, `pipeline_stats`, `telemetry_proxy`, `remote_config`), `method` (`405`), `encoding` (`415`, anything but identity or gzip), `json_traces` (`415`, a JSON v0.3/v0.4 body), `oversize` (`413`), `stalled` (`408`), `body_read` (`413`), `malformed_encoding` (`400`), or `malformed` (`400`) |
| `logit.input.requests.acknowledged{route}` | count | a stub's upload, answered `200` and discarded: `evp_proxy_v1`–`v4`, `profiling`, `debugger_v1_input`, `debugger_v1_diagnostics`, `debugger_v2_input`, `symdb`, `dogstatsd_v1_proxy`, `dogstatsd_v2_proxy`, `tracer_flare`, `openlineage` |
| `logit.input.batches.dropped{reason="busy"}` | count | batches a `503` left undelivered. See below |
| `logit.input.batches.dropped{reason="closed_consumer"}` | count | every batch of the request that no consumer took, the refused one included, answered `503`. Not disjoint from `logit.component.batches.sent`: the refused batch is also counted there and once per consumer in `logit.component.events.dropped{reason="closed_consumer"}`; batches after it that were never offered appear only here. |
| `logit.input.spans` | count | spans delivered, counted once the batch is accepted |

**A busy request is soon a lost one.** The wait is 2 seconds, not `datadog_in`'s 5, and a dd-trace
tracer retries a `503` a few times and then drops the payload, over the window
[ADR `datadog-agent-and-intake-relay`](../adr/datadog-agent-and-intake-relay.md)'s decision 11
derives. So `batches.dropped{reason="busy"}` here counts batches a retry may still deliver during
a shorter stall and lost ones during a longer one, and the counter can't tell them apart. Any sustained rate calls for more downstream capacity, such as a `buffer:` on the sinks.

The codec's own counters are in the [`datadog` codec section](#datadog), under this component's id.

`Diagnostics` keys: `bound`, `connection_error` (never an idle close, nor a connect-and-close
probe on the Unix socket, nor a plaintext TCP peer that closes cleanly or resets before sending
a request byte, after a complete PROXY header under `proxy_protocol: true`), `proxy_header` (any
`connections.rejected{reason="proxy_header"}` refusal),
`forwarded` (under `forwarded:`, a request whose named header names no usable client; the header name and the reason appear in the message text, never the value; a request without the header isn't counted),
`request_rejected` (every rejection except `404` and `405`; the peer address or socket path
appears in the message text only), `busy` (a `503`), `closed_consumer` (a `503`), `trace_count_mismatch` (an
`X-Datadog-Trace-Count` header that disagrees with the traces on the wire; the request is still
served), and `bad_header` (a `Datadog-Client-Dropped-P0-*` header that isn't an unsigned integer,
left out of the resource).

##### `splunk_hec_in`

`crates/logit-inputs/src/splunk.rs`, codec in `crates/logit-proto/src/splunk/`,
[ADR `splunk-hec-relay`](../adr/splunk-hec-relay.md).

**The connection metrics are `datadog_in`'s verbatim**, from the same accept loop and shared idle
tracker: `logit.input.connections` (gauge, published by the same drop guard),
`logit.input.connections.rejected{reason="limit"}`, `logit.input.connections.closed{reason="idle"}`,
and the accept-queue gauges. Under `proxy_protocol: true`,
`logit.input.connections.rejected{reason="proxy_header"}` counts a connection refused for its
PROXY header, as on the shared TCP stream driver.

| Name | Kind | Meaning |
|---|---|---|
| `logit.input.requests{route, class}` | count | one per request, every exit included. `class` is `ok`, `rejected`, `busy` (a `503`: code 9 on a post, code 18 on `/health`), or `closed_consumer` (no consumer took a batch: `503` code 9 for the first batch of a body, `500` code 8 for a later one); `route` is `event` (`/services/collector`, `/event`, `/event/1.0`), `raw` (`/raw`, `/raw/1.0`), `ack`, `health` (`/health`, `/health/1.0`), or `unknown` for a path this listener doesn't serve |
| `logit.input.request.duration` | timing | one per request, every exit included, time spent waiting on a busy downstream too |
| `logit.input.request.bytes` | count | the body size as sent (before gzip decompression), once the body has been read |
| `logit.input.requests.rejected{reason}` | count | one per `4xx`: `unknown_route` (`404`), `method` (`405`), `query_token` (`400` code 16, a token in the query string), `auth` (`401` code 2 or 3, `403` code 4), `encoding` (`415`, anything but identity or gzip), `oversize` (`413`, as sent or decompressed), `stalled` (`408`, only with `idle_timeout:` set), `body_read` (`413` for a body that failed for another reason, such as a client disconnecting mid-upload), `malformed_encoding` (`400` code 6, a gzip stream that doesn't decompress), `no_data` (`400` code 5, an empty body or a `/raw` body with no non-empty line), `malformed` (`400` code 6: a `/event` object the codec can't parse, after the objects before it were delivered, or an `/ack` body that isn't `{"acks":[…]}`), or `no_channel` (`400` code 10, an `/ack` request that names no channel) |
| `logit.input.batches.dropped{reason="busy"}` | count | batches a `503` left undelivered, disjoint from `logit.component.batches.sent`. See below |
| `logit.input.batches.dropped{reason="closed_consumer"}` | count | every batch of the request that no consumer took, the refused one included, answered `503` code 9 for the first batch of a body and `500` code 8 for a later one. Not disjoint from `logit.component.batches.sent`: the refused batch is also counted there and once per consumer in `logit.component.events.dropped{reason="closed_consumer"}`; batches after it that were never offered appear only here. |
| `logit.input.acks.issued` | count | `ackId`s issued, one per `200`, or code 6 after a delivered prefix, to a request that names a channel |
| `logit.input.acks.polled{result}` | count | ids asked about on `/ack`: `acked` (answered `true`), or `unknown` (answered `false`: already reported, never issued on that channel, or dropped) |
| `logit.input.acks.dropped{reason}` | count | ids issued and never reported: `expired` (more than `max_pending_acks` newer ids issued on the channel) or `evicted` (with its channel) |
| `logit.input.ack_channels.evicted` | count | channels evicted, least recently used first, once `max_ack_channels` are held |

**A busy request is not a lost one**, as on `datadog_in`: after 5 seconds without the pipeline
taking a request's first batch, the request gets `503` code 9 with `Retry-After: 1`, counted
`class="busy"`, with nothing of it delivered, and every HEC client retries it. A `/event` body
that carries several envelopes decodes to one batch per resource; once the first is delivered,
the rest wait without a deadline and the request gets `200` (or, for a body cut short, its code
6), so `logit.input.request.duration` can run past 5 seconds while `busy` stays flat (the module
doc's "Backpressure" section). From a busy answer until a later request's data is taken, for at
most 5 seconds, `/health` answers `503` code 18, counted
`logit.input.requests{route="health", class="busy"}`.

The codec's own counters (an object skipped for a missing or blank `event`, an unknown envelope
key, a bad `time` or `fields`, a span that fell back to a log) are in the tables of
`crates/logit-proto/src/splunk/mod.rs`'s module doc and its `logs`, `metrics`, and `spans`
submodules, under this component's id.

`Diagnostics` keys: `bound`, `connection_error` (never an idle close, nor a plaintext peer that
closes cleanly or resets before sending a request byte, after a complete PROXY header under
`proxy_protocol: true`; on a TLS listener that close fails the TLS accept and is counted),
`proxy_header` (any `connections.rejected{reason="proxy_header"}` refusal),
`forwarded` (under `forwarded:`, a request whose named header names no usable client; the header name and the reason appear in the message text, never the value; a request without the header isn't counted),
`request_rejected` (every rejection except `404` and `405`; the peer address appears in the
message text only, never a tag, and a token never appears at all), `busy` (a `503`), and
`closed_consumer` (a `503` code 9 or `500` code 8).

##### `tail_in` and `docker_in`

`crates/logit-inputs/src/tail/driver.rs`, `docker.rs`: one shared `Tailer<D, F>` driver.
[ADR `file-tailing-and-docker-json-logs`](../adr/file-tailing-and-docker-json-logs.md) and
[ADR `docker-container-identity-and-minimal-watches`](../adr/docker-container-identity-and-minimal-watches.md).

A tailed file has no `ReceiveQueue` for the layer-2 table to instrument, so the driver records its
own read-side counters:

| Name | Kind | Meaning |
|---|---|---|
| `logit.input.lines` / `.line.bytes` | count | the read-side counterpart of `statsd_in`'s per-datagram pair, at line granularity: every line offered to the decoder, a rejected one and the unterminated line emitted at close included, and not a line dropped for `max_line_bytes` |
| `logit.input.files.open` | gauge | sampled after every `scan` |
| `.files.rotated` / `.files.truncated` | count | a known path now naming another inode (each such path in a scan, so a rotation chain under a wildcard counts every renamed name), or the same inode shrinking |
| `.scan.errors{op="read_dir"\|"stat"}` | count | the number of failed listings, or failed `stat`s, in one `scan`, recorded once per scan per operation that had one. A failure retires no tracked file it may name: `read_dir` is a pattern's directory, `stat` a matched path (or `docker_in`'s container directory) that failed with anything but `NotFound` or `NotADirectory`. A missing directory is not an error. The rule's canonical table is `crates/logit-inputs/src/tail/pattern.rs`'s module doc. See [ADR `tail-discovery-failure-and-resume-identity`](../adr/tail-discovery-failure-and-resume-identity.md), decision 1 |
| `.files.resume_rejected` | count | a resume refused, so the file starts at `0` instead: the file is shorter than the recorded head, its head bytes hash differently, or the offset is past its length. The fingerprint comes from a checkpoint entry or a de-selection retention. Decision 2 of the same ADR |
| `logit.input.batches.dropped{reason="closed_consumer"}` | count | every batch no consumer took, the refused one included. The driver then stops writing the checkpoint, which stays at the last one written (at or before the last line a consumer took), and stops. Not disjoint from `logit.component.batches.sent`: the refused batch is also counted there and once per consumer in `logit.component.events.dropped{reason="closed_consumer"}`; batches after it that were never offered appear only here. |
| `.checkpoint.writes` | count | only on an actual write; `checkpoint_interval` ticks that find nothing dirty record nothing |
| `.checkpoint.errors{op="load"\|"write"}` | count | `load`: a checkpoint present but unusable at startup (unreadable, malformed, empty, wrong version, or missing beside a stray `.tmp`), after which every file present starts at its beginning; `write`: a failed durable write, retried on the next tick |
| `.watch.wakes{source="inotify"\|"poll"}` | count | which wake source fired |
| `.watch.overflows` | count | the `inotify` queue overflowing into a full rescan |
| `.watch.watches` | gauge | sampled alongside `.files.open`. See below. |
| `.files.identity_changed` | count | `docker_in` only: `config.v2.json`'s own stat changed and the rebuilt resource differs in value, so the decoder's `Arc` was swapped |
| `.files.deselected` | count | `docker_in` only: a tracked container renamed out of `containers:`, closed rather than kept flowing |

`.watch.watches` counts the watched directory plus one entry per currently-open file. It reports
the driver's *intended* watch set rather than live kernel descriptors: under `watch: poll` both
halves are no-ops with nothing registered, so the count still reports what would be watched under
`inotify`, not zero. It's proportional to what's tailed, not to what's running on the host, which is
the property the minimal-watch-set design is for.

`Diagnostics` keys:

| Key | Meaning |
|---|---|
| `bad_line` / `long_line` / `invalid_utf8` | A line that wouldn't decode, exceeded `max_line_bytes`, or needed a lossy UTF-8 conversion. |
| `open_error` / `read_error` | A file this driver is trying to track. |
| `renamed` | A same-inode rebind following a *file* rename. Not the same as `docker_in`'s `container_renamed`, which is the same file with a new identity. |
| `scan_error` | A `scan`'s `read_dir` or `stat` failed, carrying the first failing path, its error, and how many failed in that scan. One per `.scan.errors` point. |
| `resume_rejected` | A resume refused (a short file, a head mismatch, or an offset past the end), naming the path and the offset it would have sought to. One per `.files.resume_rejected` point. |
| `truncated` | A tracked file whose length fell below the tracked offset, so it restarts at `0` with its splitter and decoder state reset. One per `.files.truncated` point; the message carries the pre-truncation offset. |
| `closed_consumer` | No consumer took a batch. The driver stops and its node finishes; a restart resumes at the frozen checkpoint. |
| `checkpoint_error` | Loading or writing the checkpoint file itself, one per `.checkpoint.errors` point. A write failure names the step that failed. |
| `watch_error` | The one-shot cases: `auto` falling back to polling; a *file* watch that failed, which isn't retried (the file is still tailed, at `poll_interval`); or the `inotify` wake source itself becoming unusable, after which the listener runs poll-only. |
| `watch_dir_error` | A directory watch that failed, carrying the errno. Its own key because it's retried, and so re-counted, on every later `scan` while the directory is missing, and `warn_throttled` logs a key only at powers of two of its count. Sharing a key would silence the one-shot cases above. |
| `metadata_error` | `docker_in` only: `config.v2.json` missing or unparseable. Degrades to a `container.id`-only resource rather than refusing to tail. A missing file is retried on every poll tick; one that exists but won't parse is retried on its next stat change, because the stat cache caches a failed read the same way it caches a successful one. Diagnosed again only once it recovers or the stat changes, not once per tick. |
| `bad_time` | `docker_in` only: the envelope's own `time` field didn't parse. Falls back to read time. |
| `container_renamed` | `docker_in` only, `Diagnostics::info` rather than `warn_throttled`, because a rename is normal operation: the container's identity changed and the decoder's resource was swapped. |
| `container_deselected` | `docker_in` only, `Diagnostics::info`: a tracked container renamed out of the configured selection stopped flowing. Its offset is retained in memory only, not across a restart. |

##### `logit_in`

`crates/logit-inputs/src/logit.rs`,
[ADR `native-transport-handshake-and-ack`](../adr/native-transport-handshake-and-ack.md),
[ADR `native-hop-identity-and-sequence`](../adr/native-hop-identity-and-sequence.md),
[ADR `native-hop-named-acks`](../adr/native-hop-named-acks.md),
[ADR `native-hop-ack-status`](../adr/native-hop-ack-status.md).

- `logit.proto.frames{direction="in",compression}` and `logit.proto.frame.bytes`: per-frame
  detail at the transport's own unit, as `statsd_in`'s per-datagram pair is.
- `logit.input.acks` (count): `Ack`s written, one per run of handled frames
  ([ADR `native-hop-named-acks`](../adr/native-hop-named-acks.md), decision 2), plus one per
  frame refused by name. Against `logit.proto.frames{direction="in"}` it shows the coalescing
  ratio.
- `logit.proto.errors{reason="magic"|"version"|"malformed"|"crc"|"truncated_header"|"truncated"|"too_large"|"codec"|"handshake"|"decode_budget"|"ack_write_stalled"|"reject_write_stalled"}`
  (count): every way a frame or a handshake can be rejected, each its own reason so a version
  mismatch doesn't hide behind a generic "bad frame" tag. `magic` is a frame header whose magic
  isn't `LGIT`, and `version` one whose frame version this reader doesn't understand.
  `malformed` is any other frame or hop batch that fails to parse: an unknown compression byte,
  bad lengths, a body that doesn't decompress, a payload that doesn't open with a complete sender
  identity and sequence pair (the connection ends), or a corrupt batch body or trailer after a
  valid pair (answered `Ack{rejected(malformed)}`, the connection kept). `truncated_header` is a peer that closed,
  or a read that failed, part-way through a frame header, and `truncated` the same part-way through
  a body. A close between frames is the ordinary end of a connection and isn't counted, including
  a TLS peer gone without `close_notify`. `too_large` is a header that declared a
  payload over `max_frame_bytes`: with a `compressed_len` within `frame::compressed_bound` of it,
  the frame is read and answered `Ack{rejected(too_large)}`, the connection kept; past that bound,
  `Reject{FRAME_TOO_LARGE}` and a close. `handshake` is any handshake that ends without a
  `HelloAck`: no `Hello` within `handshake_timeout`, stray bytes, a header over the control-message
  cap, a `Hello` that fails to decode (a missing, repeated, or unknown field, or a `window` of 0),
  or a version or codec `Reject`. `decode_budget` is a well-formed batch that would decode
  past its per-frame budget (`native::DecodeBudget`), a batch too large for the frame cap it
  arrived under rather than corrupt bytes, answered `Ack{rejected(decode_budget)}` with the
  connection kept. The two
  `_write_stalled` reasons count a control write to a peer that stopped reading, abandoned after
  `handshake_timeout`: an `Ack` (the connection ends; for the `Ack` written before a `Reject`, the
  `Reject` is left out and the close ends as it would have) or a `Reject` (the connection was
  closing anyway).
- `logit.input.connections` (gauge, sampled on every connect/disconnect) and
  `logit.input.connections.rejected{reason="limit"}` (count, the connection cap,
  `max_connections`, 1024 by default, binding). `otlp_in` and a TCP
  `syslog_in`/`graphite_in`/`statsd_in`/`lines_in` on the shared driver record the same pair; all
  six reject at the cap rather than queueing behind a permit. Here a connection this
  listener closed still counts, and holds its permit, while it lingers: after its last answer it
  reads and discards until the peer closes or for `handshake_timeout`.
- `logit.input.connections.closed{reason="idle"}` (count), the third point all six share. Here the
  idle time is measured from the last frame handled rather than from bytes read, because a peer
  waiting on a delayed ack isn't idle. The close writes `Reject{GOING_AWAY, "idle for <dur>"}`, the
  same signal an ordinary shutdown sends, and returns `Ok(())`: it's never
  `logit.proto.errors{reason="handshake"}` or any other diagnostic.

- `logit.input.batches.dropped{reason="closed_consumer"}` (count): the frame no consumer took,
  answered `Reject{GOING_AWAY, "no consumer took the batch"}` before the connection closes, and
  never acknowledged. Not disjoint from `logit.component.batches.sent`: the refused batch is also
  counted there and once per consumer in `logit.component.events.dropped{reason="closed_consumer"}`.
- `logit.input.batches.dropped{reason="rejected"}` (count): a frame refused by name with a
  rejected `Ack` ([ADR `native-hop-ack-status`](../adr/native-hop-ack-status.md)), also counted
  under its `logit.proto.errors` reason (`decode_budget`, `malformed`, or `too_large`). Its
  sender's mark is raised, and the sender drops the batch as `rejected`.
- `logit.input.batches.resends` (count): frames at or below their sender identity's mark,
  acknowledged and not forwarded
  ([ADR `native-hop-identity-and-sequence`](../adr/native-hop-identity-and-sequence.md)). These
  are the first replays `logit` can count
  ([ADR `delivery-semantics`](../adr/delivery-semantics.md), item 11).
- `logit.input.senders` (gauge): identities in the component's table, published when the count
  changes.
- `logit.input.senders.evicted` (count): identities evicted from a full table. A sender evicted
  and then resending has its resend forwarded, a duplicate.

`Diagnostics` keys: `bound`, `decode_budget` (a batch refused by its decode budget, naming the
budget and `max_frame_bytes`), `frame_rejected` (any other frame refused by name: too large, or a
body that doesn't decode), and `connection_error` (any other connection failing; never an
idle close, and never a close between frames).

##### `generate_in`

`crates/logit-inputs/src/generate.rs`, [ADR `load-test-harness`](../adr/load-test-harness.md).

**Layer 2 only.** The runtime's own `logit.component.events.sent` on this node's fanout edge
already *is* the generated count, so a counter here would only restate it.

One `Diagnostics` key: `rate_behind`, reported once the generator falls a whole second's worth of
events behind its configured `rate`. It's the signal that a rate-limited scenario has quietly
become a throughput one, which nothing else can distinguish.

Separately, and not telemetry: on finishing its `count`, it logs one `generation complete` line at
`info` carrying `events`/`batches`/`elapsed`. The perf harness reads wall time and the events/s
divisor from that line (`docs/plans/load-test-harness.md`). It's the one component that emits a
structured `tracing` event directly rather than through `Diagnostics`, because the harness needs
those values as *fields*, not as a rendered message.

#### Transforms

##### `route`

`crates/logit-transforms/src/route.rs`, [ADR `target-components`](../adr/target-components.md).

**Layer 2 only**, for the same reason as `generate_in`. `run_router`/`route_batch` already record
`batches.received`/`events.received`/`process.duration`, and every destination's own `Fanout`
counts what it sent, so a native equality match has nothing further worth a counter. An event
`route` can't place lands on its router's `Forward` partition, which is what
`events.dropped{reason="unrouted"}` counts when that router has no ordinary consumers.

##### `aggregate`

`crates/logit-transforms/src/aggregate.rs`.

| Name | Kind | Meaning |
|---|---|---|
| `logit.transform.series.active` | gauge | series updated this window, sampled at the top of `flush` before it touches its own state: the peak-of-window series count, and the visible signal for the cardinality blow-up `crate::keep`'s module doc warns `aggregate` is exposed to |
| `logit.transform.resource.groups` | gauge | resource groups, sampled with `.series.active` |
| `logit.transform.series.retained` | gauge | the idle-but-carried population. `.active` keeps its "series updated this window" meaning and doesn't include these |
| `logit.transform.series.evicted{reason="idle"\|"cardinality"}` | count | a TTL expiry versus the hard `max_retained_series` cap. A non-zero `cardinality` count means a later delta is about to resolve against 0.0, or a cumulative series is about to restart from zero with a new `start_timestamp`. `cardinality` also carries `state="active"\|"idle"`: `active` counts series updated this window, emitted and then dropped, so more active series arrive per window than the cap holds |
| `logit.transform.gauge.delta.unseeded` | count | a `GaugeDelta` opened a brand-new series and resolved against 0.0 (statsd's own rule for an unseeded gauge), indistinguishable from a real 0.0 without this |
| `logit.transform.samples.fallback{reason="rate_mismatch"\|"cap"}` | count | a `samples`-mode series gave up raw retention and became a sketch |
| `logit.transform.set_members.fallback{reason="cap"}` | count | a `members`-mode series gave up raw retention and became an estimate |
| `logit.transform.samples.weight_clamped` | count | a record's sample rate implied more than `Samples::MAX_WEIGHT` observations per value (`Samples::is_clamped`), counted when its weight is applied: on absorb into a sketch, or when a raw `distributions: samples` series falls back |
| `logit.transform.samples.non_finite_dropped` | count | `NaN` or infinite `Samples` values dropped while sketching, since they have no bin |
| `logit.transform.metrics.absorbed` | count | metric records merged into a series. With `.passed_through` it accounts for every record `process` receives: `metrics_in == absorbed + passed_through` summed over every reason |
| `logit.transform.metrics.passed_through{reason="no_recorded_value"}` | count | an OTLP `NO_RECORDED_VALUE`-flagged record forwarded unmerged, because it has no reading to fold into a series |
| `logit.transform.metrics.passed_through{reason="no_merge_rule"}` | count | a record of a kind this stage doesn't merge, forwarded: a cumulative `Sum`, an `ExponentialHistogram`, a `Summary`, and a `Histogram` outside `temporality: cumulative` or already cumulative |
| `logit.transform.metrics.passed_through{reason="kind_conflict"}` | count | a record forwarded because its series holds another kind |
| `logit.transform.metrics.passed_through{reason="histogram_bounds_mismatch"}` | count | a histogram forwarded because its bucket bounds differ from its series' |
| `logit.transform.metrics.passed_through{reason="non_finite"}` | count | a delta `Sum` whose value is `NaN` or infinite, forwarded so a cumulative total stays finite |
| `logit.transform.links.dropped{reason="contexts"}` | count | contributing span contexts past the fixed per-series cap (`MAX_CONTRIBUTING_CONTEXTS_PER_SERIES`, 8) that a flushed event's links can't carry, one per merged record whose context the full series doesn't link, so a context seen in two batches counts twice. Unrelated to `max_retained_series`: no series is lost |

Series retention across the window boundary comes from
`docs/adr/aggregation-window-semantics.md`'s gauge-retention amendment and its cumulative
amendment, which reuses the same two bounds and counters for a `temporality: cumulative`
`Sum`/`Histogram`. Absorbing raw kinds comes from the same ADR's "raw samples and set members"
amendment.

`Diagnostics` keys: `series_retention_full` and `gauge_delta_unseeded` (mirroring the eviction and
unseeded counters); `samples_rate_mismatch`, `samples_cap_exceeded`, `set_members_cap_exceeded`,
and `sample_rate_clamped` (mirroring the raw-retention counters); `kind_conflict` (a metric whose
kind conflicts with an already-accumulating series under the same name/unit/tags, forwarded
untouched); `histogram_bounds_mismatch` (a histogram whose bucket bounds differ from the
accumulating series', also forwarded untouched, under its own key so an operator knows it's a
producer that re-bucketed rather than two kinds colliding); and `sum_non_finite` (a delta `Sum`
whose value is `NaN` or infinite, forwarded untouched).

##### `kv_metrics`

`crates/logit-transforms/src/kv_metrics.rs`.

`logit.transform.derived{metric_kind}` / `.derived.skipped{metric_kind}` make the documented
silent-skip path (a missing or non-numeric field, deliberately never a diagnostic) visible as a
rate. The tag is `metric_kind`, not `kind`, because `kind` is reserved for a point's own
component-kind identity (see [Naming](#naming)). One `Diagnostics` key, `distribution_no_field`,
is defense in depth only: graph validation rejects a fieldless distribution before a real config
reaches it.

##### `scale`

`crates/logit-transforms/src/scale.rs`, [ADR `scale-transform`](../adr/scale-transform.md).

`logit.transform.scaled` / `.scaled.skipped`, once per configured field per event: the same
pattern as `kv_metrics`'s `.derived`/`.derived.skipped`, making the documented silent-skip path (a
missing, non-numeric, or non-finite result) visible. No `Diagnostics`.

##### `regex`

`crates/logit-transforms/src/regex.rs`, [ADR `regex-transform`](../adr/regex-transform.md).

`logit.transform.matched` / `.matched.skipped`: exactly one of the two per event, regardless of
how many attributes a match contributed. `.matched.skipped` covers every silent skip: no log, a
non-string message or `field`, non-UTF-8 bytes, or no match. No `Diagnostics`.

##### `logfmt` and `kv`

`crates/logit-transforms/src/logfmt.rs` (the pair counters in
`crates/logit-proto/src/message/logfmt.rs`),
[ADR `logfmt-and-kv-parsing`](../adr/logfmt-and-kv-parsing.md).

`logit.transform.pairs.parsed` / `.pairs.skipped`, once per pair: `.skipped` counts a pair the
parser couldn't use, such as an empty key, an empty segment, or a bare key with `bare_keys` off. `Diagnostics` keys:
`parse_failure` (the message didn't parse; the event passes through) and `invalid_utf8` (the
message isn't valid UTF-8; the event passes through unparsed).

##### `csv`

`crates/logit-transforms/src/csv.rs`,
[ADR `csv-positional-columns`](../adr/csv-positional-columns.md).

`logit.transform.rows.parsed` per parsed row, and `logit.transform.rows.skipped{reason="empty"}`
for an empty message, a routine skip with no diagnostic. `Diagnostics` keys, each passing the event
through unparsed: `invalid_utf8`, `header_row` (the message is the configured header row),
`parse_failure` (a malformed row), and `field_count` (a row with a different number of fields than
configured columns).

##### `keep` and `remove`

`crates/logit-transforms/src/keep.rs`.

`logit.transform.attributes.kept` / `.dropped`: the other half of `aggregate`'s cardinality story,
showing how much `keep` suppresses before events reach `aggregate`. Neither kind has `Diagnostics`,
because pure attribute filtering has nothing to warn about, so `Telemetry` is attached directly
rather than through the `Diagnostics` bridge.

##### `set`

`crates/logit-transforms/src/set.rs`.

`logit.transform.set.resource.rebuilt` (count) fires only on a `map_resource` cache miss: a batch
whose incoming resource `Arc` isn't the one cached from the last call. It makes a config that
defeats the one-entry cache (a listener minting a fresh `Arc` per batch, `otlp_in` chief among
them) visible as a rate. It's absent when `set` has no `resource:` configured, because
`map_resource` returns before touching telemetry. See
[ADR `operator-declared-resource-attributes`](../adr/operator-declared-resource-attributes.md).

##### `trace_context`

`crates/logit-transforms/src/trace_context.rs`,
[ADR `log-record-trace-context`](../adr/log-record-trace-context.md) and
[ADR `trace-context-span-lifting`](../adr/trace-context-span-lifting.md).

`logit.transform.trace_context.lifted` (count) on a successful lift, and `.skipped{reason}`
otherwise, with `reason` one of:

- `missing`: no trace id at all, neither the configured attribute nor a `traceparent`.
- `invalid`: something present didn't parse: an id, the flags, a `traceparent`, a
  `span.kind`/`span.status` name, a timing value, or two forms of one timing quantity at once.
- With a `span:` block only: `span_id` (no span id of its own and `mint_id` off), `timing` (the
  timing attributes can't determine a start and an end, or determine an impossible span), or `skew`
  (start or end further from receipt time than `max_skew`).

With a `span:` block, `.spans{id="present"|"minted"}` (count) alongside `.lifted` says whether the
minted `SpanRecord`'s id came from the line or from `mint_id`. This is the `kv_metrics`
`.derived`/`.derived.skipped` pattern applied to lifting a trace context.

##### Filters: `has_signal`, `keep_signals`, `drop_signals`, `has_attributes`, `drop_attributes`, `has_provenance`, `drop_provenance`

`crates/logit-transforms/src/signals.rs`, `crates/logit-transforms/src/attributes.rs`,
`crates/logit-transforms/src/provenance.rs`;
[ADR `attribute-filtering-components`](../adr/attribute-filtering-components.md),
[ADR `provenance-filtering-components`](../adr/provenance-filtering-components.md).

All seven share `logit.transform.events.filtered`, `1.0` per dropped event. `has_signal`,
`has_attributes`/`drop_attributes`, and `has_provenance`/`drop_provenance` also record `0.0` on the
forward path, which registers the series rather than leaving it absent; `keep_signals`/
`drop_signals` record it only when stripping leaves an event with no payload. Those two also
record `logit.transform.payloads.stripped{signal}` (count), one per payload slot actually cleared;
no other filter mutates a forwarded event, so none has a `.payloads.stripped`-style counter. None has `Diagnostics`, because matching a fixed set or clearing a fixed signal set can't
fail.

`has_attributes`/`drop_attributes` deliberately have **no cache-miss counter** for their
resource-match cache (`Matcher`, `Set::map_resource`'s `Arc::ptr_eq` idiom applied to a read).
Unlike `set`'s miss, which rebuilds an `AttrMap`/`Resource`/`Arc` and so is worth a rate, a miss
here only re-evaluates `AttrMap::get_sym` against the `Arc` already in hand, so a counter would
advertise a cost that isn't there.

##### `keep_values`

`crates/logit-transforms/src/keep_values.rs`,
[ADR `value-allowlist-cardinality-clamp`](../adr/value-allowlist-cardinality-clamp.md).

`logit.transform.values.allowed`/`.clamped`, tagged `field`: the value-side counterpart to
`keep`'s `.attributes.kept`/`.dropped`, read per configured field rather than in aggregate, because
two fields on one component can clamp at very different rates. `.values.normalized`, also tagged
`field`, fires only when a `normalize:` step changed the value; counting the common
already-conforming case would make the rate unreadable. `field` is safe as a tag because the fields
are config-declared. No `Diagnostics`: clamping to a fixed allow-list can't fail.

##### `shape`

`crates/logit-transforms/src/shape.rs`,
[ADR `shape-observer-component`](../adr/shape-observer-component.md).

Three drop counters and nothing else: `logit.transform.batches.dropped` (a flush window's per-batch
table hit its 4096-batch cap), and `logit.transform.keys.untracked` and
`logit.transform.keysets.untracked` (an observation the cumulative table's `max_tracked_*` cap
turned away, emitted once per flush rather than once per event). All three are **counts of things
not recorded**, which a bounded table must never drop silently. All three are untagged on purpose:
`shape`'s defining property is that nothing it emits names an observed key or value, and that
applies to its own telemetry as much as to its metrics (`logit.shape.*`) and tags
(`signal`/`source`/`tap`).

The component's *measurements* aren't telemetry points: they're ordinary events on its own
outbound edge, so an `aggregate` downstream summarizes them like any other traffic. No
`Diagnostics`, because counting a shape can't fail.

##### `flatten`

`crates/logit-transforms/src/flatten.rs`, [ADR `flatten-transform`](../adr/flatten-transform.md).

`logit.transform.values.flattened` (count, one per leaf attribute written) and
`logit.transform.values.unflattened{reason="max_depth"}` (count, a source value the internal
recursion-depth wall refused, written back whole and unexpanded). Both are **untagged**, unlike
`keep_values`' `field`-tagged pair: under the default `attributes: all`, the source attribute name
is data the operator doesn't control, and tagging by it would mint an unbounded telemetry series
from key-position data, the property `shape` is built to avoid. No `Diagnostics`: flattening an
already-decoded value can't fail.

##### `http_access`

`crates/logit-transforms/src/http_access.rs`,
[ADR `http-access-normalization`](../adr/http-access-normalization.md),
[`docs/http-access-logs.md`](../http-access-logs.md).

Seven counters. Every tag comes from a closed, `&'static` table, never an observed value, and
`field` is always the canonical dotted name.

| Name | Meaning |
|---|---|
| `logit.transform.http_access.normalized{field}` | a field rewritten into its conformant form: a dashed alias renamed, a composite decomposed, a numeric coerced, a duration converted, a method or version normalized, a leading `?` stripped |
| `.derived{field}` | a field written from config or a built-in table: `user_agent.class`, `user_agent.synthetic.type`, `http.route`, `error.type`, `span.name`, `span.status`, `span.duration_s`, `http.request.method_original`, and `client.address` and `client.port` under `forwarded`. Every derived attribute but those two is fill-only, so one the producer already sent is honoured and not counted |
| `.truncated{field}` | a value cut to its `max_length` |
| `.cleaned{field}` | a control byte replaced by `_` |
| `.invalid{field}` | present but unparseable, left as it arrived; under `forwarded`, the header's attribute when it names no usable client |
| `.redacted` | untagged; one per sensitive `url.query` value replaced |
| `.routed{outcome="rule"\|"builtin"\|"other"\|"none"\|"kept"}` | once per event with a `url.path`, or with a producer-sent `http.route`, which is `kept` (honoured, never re-matched). The tag is the outcome, never the route value, which is operator-declared and unbounded in number |

Four throttled `Diagnostics` keys, for genuine producer malformation only: `bad_request_line`
(`http.request.line` isn't `METHOD TARGET PROTOCOL`), `bad_status`, `bad_duration`, and
`forwarded` (the header `forwarded:` names gives no usable client address, so `client.address`
and `client.port` stay as logged). An absent
field, an unknown method, an unclassifiable user agent, and an unrouted path are normal traffic and
get counters only.

##### `sample`

`crates/logit-transforms/src/sample.rs`,
[ADR `consistent-sampling-component`](../adr/consistent-sampling-component.md).

- `logit.transform.events.filtered`, shared with the filters above: the batch's dropped count,
  emitted even at `0` so the series registers.
- `logit.transform.sample.decisions{outcome="kept"|"dropped",
  by="key"|"random"|"override"|"missing"}` (count, non-zero cells only). `override` is an
  `always_keep` hit, `key` a hashed verdict, and `random` a keyless sampler's draw. `missing` is an
  event whose configured key was absent, whatever `missing:` then did with it (a random draw
  included), so that cell counts exactly the events the key didn't cover.

Unlike the filters, both are **tallied in plain integers per event and emitted once per batch from
`end_batch`** (`kv_metrics`' pattern): a sampler sits on every event of the high-volume streams it
exists for, where a `Telemetry::count` per event is the cost. No `Diagnostics`: nothing here can
fail.

##### `timestamp`

`crates/logit-transforms/src/timestamp.rs`,
[ADR `timestamp-transform`](../adr/timestamp-transform.md).

- `logit.transform.timestamp.resolved` (count): events whose `event.timestamp` was rewritten.
- `logit.transform.timestamp.skipped{reason="missing"|"invalid"|"skew_past"|"skew_future"|"span"|"start"}`
  (count, non-zero cells only): events forwarded untouched, by why. `missing` is an absent, null,
  empty, or `-` source; `invalid` a value that doesn't parse under `format`; `skew_past` and
  `skew_future` a resolved instant further than `max_skew` before or after the event's current
  timestamp; `span` an event carrying a span; `start` a metric whose non-zero
  `start_timestamp` is later than the resolved instant.

Both are **tallied in plain integers per event and emitted once per batch from `end_batch`**, as
`sample` does. No `Diagnostics`: a skip is expected traffic during a backlog drain, not a fault.

##### `json`

`crates/logit-transforms/src/json.rs`,
[ADR `json-parsing-into-attributes`](../adr/json-parsing-into-attributes.md).

No counters. Three throttled `Diagnostics` keys:

- `parse_failure`: the message isn't a JSON object. The event passes through with its attributes
  untouched.
- `no_brace`: `skip_to_brace: true` and no `{` anywhere.
- `invalid_utf8`: only under `invalid_utf8: replace`. A parse that failed on invalid UTF-8
  succeeded on the lossy retry: the line was rescued, not lost. A retry that also fails reports
  `parse_failure` instead.

##### `lua` and `lua_file`

`crates/logit-script`, `crates/logit-pipeline/src/runtime.rs::run_lua`.

`logit.script.vm.memory` (the Lua VM's own `used_memory()`, the strongest single signal of a
leaking stateful script) and `logit.script.events.emitted{outcome}`, both from the Rust side (see
layer 2). Uniquely, a script can also call a **script-facing** `telemetry` global
(`telemetry.count(...)`/`.gauge(...)`) for domain facts only the script knows. See
[Metrics from Lua scripts](#metrics-from-lua-scripts) and `docs/design/lua-api.md`.

The runtime's stall watcher adds the `script_stalled` (`warn_throttled`, so counted in
`logit.component.diagnostics{key}`) and `script_resumed` (`info`) diagnostic keys, and
`max_memory` adds `memory_limit_exceeded` (`error`) and the `logit.script.vm.gc.forced` count and
`.gc.duration` timing; see layer 2's
[receive and processing side](#receive-and-processing-side-the-node-loops).

A script's `print(...)` is a self-log line, never stdout: `print: <arguments, tab-joined>` at
`info`, target `logit`, with the component's `component` field (`<unset>` for top-level code that
runs before the id is known; `crates/logit-script/src/print.rs`). At `info` it sits below the
lowest `logs:` threshold `internal` accepts, so it reaches the process log but never the pipeline.

#### Outputs

Every sink's retry counting is layer 2 (`logit.component.retries`), not something each sink tracks
itself: retry lives in the generic `deliver_with_retry` every sink shares
(`docs/adr/buffered-sink-delivery.md`). No sink has a `logit.output.retries` metric.

Several sinks share a `*.normalized` counter family. A `normalized` reason normally means a
*different but equivalent* wire form (batching, reordering, a dialect substitution). **`multi_value`
is the exception, and it's lossy wherever it appears** (`influxdb_out`, `graphite_out`,
`prometheus_out`): a multi-valued attribute (an `Array`, for example from a relayed, repeated
DogStatsD tag key) renders as its last representable element, and every other element is dropped.
It's `normalized` rather than `dropped` because the tag or label itself survives, but read it as
data loss.

The datagram sinks count an oversize drop at one of two points, and in each sink's own unit:

| Counter | Reason | Where | Unit |
|---|---|---|---|
| `logit.output.messages.dropped` | `oversize_datagram` | the kernel refused a datagram with `EMSGSIZE`, or the packer skipped an entry over the cap | `statsd_out`: entries (lines); `graphite_out`: datapoints; `collectd_out`: value lists (a notification counts one); `syslog_out`: messages |
| `logit.output.messages.dropped` | `oversize_line` | `statsd_out`'s encoder: one line over `max_packet_bytes` | lines |
| `logit.output.metrics.skipped` | `oversize_line` | `graphite_out`'s encoder: one plaintext line over `max_packet_bytes` | lines |
| `logit.output.metrics.skipped` | `oversize_value_list`, `oversize_notification` | `collectd_out`'s encoder: one value list or notification over `max_packet_bytes` | value lists, notifications |

A sink counter falls in one of three classes
([ADR `sink-send-path-and-attempt-accounting`](../adr/sink-send-path-and-attempt-accounting.md),
decision 1). For `statsd_out`, `syslog_out`, `graphite_out`, `collectd_out`, `influxdb_out`,
`stdio_out`, `file_out`, `otlp_out`, `prometheus_out`'s remote-write mode, `splunk_hec_out`,
`datadog_out`, and `datadog_trace_out`:

| Class | Counts | Counters |
|---|---|---|
| Encode-side | once per batch, however many attempts the runtime makes | `batch.bytes`; `messages.dropped` for a reason the encoder decided; `tags.dropped`, `tags.normalized`, `messages.normalized`, `messages.truncated`, `events.skipped`, `structured_data.dropped`; the codec counters of `statsd_out`, `graphite_out`, `collectd_out`, `otlp_out`, `prometheus_out`, `splunk_hec_out`, `datadog_out`, and `datadog_trace_out` (`metrics.skipped`, `metrics.degraded`, `metrics.normalized`, `identity.sanitized`, `labels.dropped`, `labels.normalized`, `spans.degraded`, `stats.skipped`, `stats.degraded`); `splunk_hec_out`'s `records.dropped{reason="oversize"}` for an object over `max_body_bytes`; `datadog_out`'s `records.dropped{reason="stale"\|"too_many_tags"\|"needs_agent_processing"\|"not_datadog_origin"}` and its `too_many_tags` diagnostic; both Datadog sinks' `records.dropped{reason="oversize"}` for an event too large to send alone; `file.rotations` and `file.reopens`; and every `logit.component.diagnostics` count an encoder emits, `datadog_trace_out`'s `bad_header` included |
| Transport | once per attempt | `requests`, `request.duration`, `request.bytes`, `reconnects`, and what the attempt sent or had accepted: `messages`, `datagrams`, `datapoints`, `samples`, `records` |
| Kernel or destination verdict, and packer backstop | once per attempt that meets it | `messages.dropped{reason="oversize_datagram"}`, from `EMSGSIZE` or from the packer's skip of an entry over the cap; `splunk_hec_out`'s `records.dropped{reason="invalid_event"}`, its `records.dropped{reason="oversize"}` from Splunk Cloud's oversize answer, `requests.rejected`, and `acks`; `otlp_out`'s `records.rejected`; `datadog_out`'s `records.rejected{route="series"}` from a series `202`'s body; the Datadog sinks' `records.dropped{reason="oversize"}` for a request answered `413`; `otlp_out`'s `records.dropped{signal, reason="rejected"\|"ambiguous_at_most_once"}` and the Datadog sinks' `records.dropped{route, reason="rejected"}` for a request answered with a verdict that names it (counted even when the send ends `Ok`); and the diagnostics a verdict raises (`remote_write_rejected`, `otlp_partial_success`, `signal_rejected`, `invalid_event`, `token_rejected`, `request_refused`, `request_rejected`, `api_key_rejected`, `series_rejected`, `ack_unsupported`, `ack_timeout`, and `oversize` for Splunk Cloud's answer). `otlp_out` meets each of its verdicts at most once per batch, because a retry never resends a signal the destination settled |

The sinks with encoders count the first class through a gate `Output::observe_batch` arms
(`crates/logit-outputs/src/accounting.rs`); `stdio_out` and `file_out` count after the write that
delivered the batch. A caller that sends without `observe_batch` counts every `send`, unless an
earlier batch whose last attempt failed left the gate armed (`docs/known-gaps/datadog.md`).
`prometheus_out`'s registry mode has no gate: its `send` never fails, so nothing repeats. No sink
counts an encode-side counter per attempt.

The two Datadog sinks encode a batch in units: `datadog_out`'s plan (the stale filter, the tag
limit, and the readiness gate) and each of its routes, and `datadog_trace_out`'s trace and stats
routes. Each unit counts on the batch's first encode of it, so a route an earlier attempt never reached counts
on the attempt that first encodes it. A request over a route's byte limit is bisected and each
half re-encoded; the re-encodes count nothing, so a codec counter for a record counts once per
record, at the record's first encode, however deep the bisection goes, on a single attempt as on
a retried batch. A record the codec degraded and the sink then dropped as `oversize`
counts under both counters.

A few Datadog codec counters describe a request body, not a record, and count once per body the
codec encodes before any bisection, that is once per count-capped request:
`spans.degraded{reason="no_wire_form"}` for a batch-resource carrier the form can't carry,
`spans.degraded{reason="json_text"}` for a batch-resource carrier sent as JSON text,
`tags.dropped{reason="no_wire_form"\|"unrepresentable"}` for a stats payload's resource
attributes, and `stats.degraded{reason="negative_timestamp"}`, once per bucket. `datadog_out`
never cuts a traces or stats request by count, so these count once per batch there;
`datadog_trace_out` counts them once per 1,000 traces or stats groups. "Once" is per bucket for
`negative_timestamp`: on `datadog_out` it counts once for each distinct negative bucket in the
batch, on `datadog_trace_out` once for each such bucket in each request.

`splunk_hec_out`'s `records.dropped{reason="oversize"}` mixes two classes: an object over
`max_body_bytes` is dropped before any request and counts once per batch, and a lone object Splunk
Cloud answered as over its cap counts on each attempt that gets that answer. The Datadog sinks'
`oversize` mixes the same two, an event too large alone and a request answered `413`; a `413` is
`Fault::Rejected`, so its batch isn't retried.

`oversize_datagram` counts per attempt for both of its causes, the kernel's `EMSGSIZE` and the
packer's skip of an entry over the cap (a backstop no encoder reaches), so a batch retried after a
later failure can count it again. `crates/logit-outputs/src/datagram.rs`'s module doc has the send
side's rules.

The same four sinks, and `statsd_out` under `transport: unix`, count `logit.output.messages`,
`logit.output.datagrams`, and `graphite_out`'s `logit.output.datapoints` for every datagram the
kernel took, on an attempt that then failed as well as on one that succeeded: those datagrams are on
the wire either way. A cancelled attempt counts none of them (`docs/known-gaps/intake.md`). On a
stream transport the same counters count only a delivered frame.

An attempt the shutdown grace cancels counts no `requests` and
none of the counts a dropped future never reaches, and `logit.component.errors` covers it. Its
`request.duration` timer still records, because a timer records when it drops.

`logit.output.requests` has two vocabularies. The stream and datagram sinks and `logit_out` tag
one count per attempt with `class=ok|clean|ambiguous|rejected|refused`, the `Fault` taxonomy. The HTTP
sinks tag one count per request with its status class or `network_error`
(`docs/known-gaps/telemetry.md`).

##### `influxdb_out`

`crates/logit-outputs/src/influxdb.rs`.

- `logit.output.requests{class="2xx|4xx|5xx|network_error"}`, `logit.output.request.duration` (per
  attempt), and `logit.output.batch.bytes`: the encode and HTTP-response detail a generic
  `send.duration` timer can't distinguish.
- `logit.output.tags.normalized{reason="multi_value"}`: a multi-valued tag (from a relayed,
  repeated DogStatsD tag key, `docs/adr/statsd-output.md`'s amendment) rendered as its last
  representable element, once per attribute. Line protocol has no multi-value tag, so this is the
  fallback. Lossy; see above.
- A `MetricKind::GaugeDelta` reaching this encoder unresolved means the pipeline is missing an
  `aggregate` component (`docs/adr/relative-gauge-adjustments.md`). It reports under its own
  `logit.component.diagnostics{key="gauge_delta_unresolved"}`, not the generic `encode_error` every
  other unrepresentable kind uses, so it's greppable on its own.

##### `stdio_out` and `file_out`

`StreamOutput`, `crates/logit-outputs/src/stdio.rs`. Both are built on the same sink (ADR
`rotating-file-output`).

- `logit.output.batch.bytes`, matching `influxdb_out`'s, counted once the batch is written, so a
  retried batch counts once. A write error propagates as a hard failure, with no `warn_throttled`
  call site to bridge.
- `file_out` rotation only: `logit.output.file.rotations` (count, one per rotation whose rename
  committed, including one whose re-open then failed and was retried) and,
  through `Diagnostics::warn_throttled`,
  `logit.component.diagnostics{key="rotate_failure"|"retention_failure"}`
  (`crates/logit-outputs/src/file.rs::FileTarget::rotate`). `rotate_failure` means the rotation
  didn't happen: renaming the active file to its `.rotating` staging path failed, or, under
  `max_files: 1`, truncating it in place failed. Either way nothing on disk changed, writing
  continues to the current file, and the next write retries the rotation. `retention_failure`
  means a retained file's own delete or rename in the cascade, or the staged file's promotion to
  `.1`, failed and was skipped. Neither can fire for
  a `stdio_out` target or an unrotated `file_out` (`RotatePolicy::never()`), because
  `should_rotate` never returns `true` under that policy.
- Any file target, `stdio_out`'s or `file_out`'s: `logit.output.file.reopens` (count, one per
  successful reopen of the target's path after a SIGHUP, ADR `signal-handling`) and, through
  `Diagnostics::warn_throttled`, `logit.component.diagnostics{key="reopen_failure"}` when that
  reopen's open fails (`crates/logit-outputs/src/file.rs::FileTarget::reopen`). A failed reopen
  also fails the batch as `Clean`, and the retry's write opens the file again. A `stdout` or
  `stderr` target never counts either.

##### `syslog_out`

`crates/logit-outputs/src/syslog.rs`.

- `logit.output.batch.bytes` and `logit.output.request.duration`: `influxdb_out`'s shape.
- `logit.output.requests{class="ok"|"clean"|"ambiguous"|"rejected"|"refused"}` (count): one per attempt
  that returns, on both transports, tagged with the attempt's `Fault` class
  ([ADR `sink-send-path-and-attempt-accounting`](../adr/sink-send-path-and-attempt-accounting.md),
  decision 4). A failed connect or TLS handshake is an attempt and counts `clean`; an attempt the
  runtime cancels counts nothing. There's no HTTP status to classify.
- `logit.output.messages` (count): messages sent.
- `logit.output.events.skipped`: events with no `log` record, so nothing to render as a syslog
  message (ADR `multi-payload-events`).
- `logit.output.messages.truncated` and
  `logit.output.messages.dropped{reason="oversize_header"|"oversize_datagram"}`: per-message size
  handling (`docs/adr/syslog-output.md`'s "Sizing" section).
- `logit.output.structured_data.dropped{reason="invalid_sd_name"|"not_a_map"|"sd_id_collision"}`:
  an SD-ID or PARAM-NAME that isn't a valid RFC 5424 `SD-NAME` (an invalid SD-ID skips the
  element, an invalid PARAM-NAME that param); a `syslog.sd` element whose value isn't a map of
  PARAM-NAME to value; the opt-in element colliding with an SD-ID the event already carries.
- `logit.output.reconnects` (count, TCP only): every connect *after* the first. A climbing count in
  steady state means the peer or the network, not this sink, is unstable. Counted on plaintext and
  TLS (RFC 5425) connections alike, because both go through the pooled-stream driver
  `statsd_out` and `graphite_out` share (`crates/logit-outputs/src/stream.rs`). A failed connect
  doesn't count. UDP is connectionless and never reports it.

`Diagnostics` keys: `invalid_structured_data`, `message_truncated`, `oversize_datagram`, and
`oversize_header`, mirroring the counters above.

##### `statsd_out`

`crates/logit-outputs/src/statsd.rs`, `docs/adr/statsd-output.md`,
`docs/adr/statsd-out-multi-value-expansion.md`.

- `logit.output.batch.bytes`, `logit.output.request.duration`, and
  `logit.output.requests{class="ok"|"clean"|"ambiguous"|"rejected"|"refused"}`: `syslog_out`'s shape, on
  every transport.
- `logit.output.messages`: encoded messages, one per `MessageBuf` entry, on every transport,
  matching `syslog_out`'s. Usually one entry is one statsd line. A negative-absolute-gauge metric's
  two-line `0|g`/`-n|g` pair is one indivisible entry (`docs/adr/statsd-output.md`) and counts once
  on every transport, as does its `messages.dropped{reason="oversize_datagram"}` if a packed
  datagram carrying it is rejected. An expanded multi-value record counts one entry per component
  line.
- `logit.output.datagrams` (`udp` and `unix`): the packed datagrams a batch of lines was sent as.
  It's the number an operator tuning `max_packet_bytes` needs, because `statsd_out` (unlike
  `syslog_out`) packs several lines per datagram.
- `logit.output.messages.dropped{reason=...}`, with `reason` one of:
  `"unresolved_gauge_delta"|"unencodable_value"|"empty_name"|"oversize_line"|
  "oversize_datagram"|"dialect_field"|"dialect_event"|"invalid_service_check"|
  "invalid_event_field"`. The less obvious ones:
  - `dialect_field`: a `|c:`/`|T` field with nowhere to go under `format: statsd`.
  - `dialect_event`: a whole DogStatsD event or service check dropped under `format: statsd`, which
    has no `_e`/`_sc` wire form.
  - `invalid_service_check`: a service check whose first metric isn't a `Gauge` or has no status
    resolving into `0..=3`.
  - `invalid_event_field`: an event's `p:`/`t:` field alone omitted for an out-of-set value. It's
    its own counter, not `unencodable_value`, because the rest of the line still renders.
- `logit.output.metrics.degraded{metric_kind=...}` and `logit.output.metrics.skipped{metric_kind=...}`,
  with `metric_kind` one of `"distribution"|"set"|"histogram"|"exponential_histogram"|"summary"|
  "cumulative_sum"|"non_monotonic_delta_sum"`: a summarized kind written as per-component lines
  under `multi_value: expand` (the default), or dropped under `multi_value: skip`. Counted once per
  record, however many lines it writes. `skip` also raises the throttled
  `logit.component.diagnostics{key="unsupported_metric_kind"}`; `expand` raises no diagnostic.
- `logit.output.tags.dropped{reason="dialect"|"unrepresentable"}`: `format: statsd` dropping the
  whole tag segment, or an individual unrepresentable tag. Counted **per wire tag**, so a
  multi-valued attribute (an `Array`, from a repeated DogStatsD tag key, `docs/adr/statsd-output.md`'s
  amendment) that expands to several tags on the wire counts once per element, not once per
  attribute.
- `logit.output.messages.normalized{reason="dialect"|"member_sanitized"}`: a lossless-but-different
  rendering rather than a drop. A timer's `h`/`d` wire-type letter collapsing to `ms` under
  `format: statsd`, or a `SetMembers` member changing after lossy UTF-8 plus sanitization.
- `logit.output.reconnects` (count; `tcp`, `unix_stream`, and `unix`): every connect *after* the
  first, as for `syslog_out`. Counted on plaintext and TLS connections alike, because `tcp` and
  `unix_stream` both go through the shared pooled-stream driver.
  Under `unix` it counts each reconnect of the connected datagram socket after a timeout or a gone
  receiver ([ADR `datadog-agent-and-intake-relay`](../adr/datadog-agent-and-intake-relay.md),
  decision 12). UDP is connectionless and never reports it.

A `MetricKind::GaugeDelta` reaching this encoder with `relative_gauges: false` reports under
`logit.component.diagnostics{key="gauge_delta_unresolved"}`, the same key `influxdb_out` uses, so
one grep finds both sinks.

##### `collectd_out`

`crates/logit-outputs/src/collectd.rs`, `docs/adr/collectd-binary-relay.md`.

**The codec emits its own counters and diagnostics directly.** `logit_proto::collectd`'s module
doc has the full mapping-to-counter table: `logit.output.metrics.skipped{metric_kind|reason}`,
`logit.output.tags.dropped{reason}`, `logit.output.identity.sanitized{reason}`, and
`logit.output.messages.truncated` (an over-long notification message). Unlike `statsd_out`, whose
encoder returns an `EncodeStats` for the sink to turn into telemetry, `CollectdEncoder` holds the
same `Telemetry`/`Diagnostics` handles this sink's `with_telemetry`/`with_diagnostics` receive and
reports through them itself, so both halves of one `send` appear under one component id.

The sink adds only what a socket send can produce and the codec can't know:

- `logit.output.batch.bytes`, `logit.output.request.duration`, and
  `logit.output.requests{class="ok"|"clean"|"ambiguous"|"rejected"|"refused"}`: `syslog_out`'s shape.
- `logit.output.messages`: value lists actually sent (the per-datagram list count each
  `logit_proto::MessageBuf<usize>` entry's meta carries, summed).
- `logit.output.datagrams`: datagrams actually sent. Both this and `messages` are UDP concepts;
  collectd has no TCP mode.
- `logit.output.messages.dropped{reason="oversize_datagram"}` plus a throttled `oversize_datagram`
  diagnostic: `EMSGSIZE` on one already-packed datagram, as in `statsd_out`'s identical case. The
  datagram's own lists are dropped, not the whole batch, and sending continues with the next
  datagram.

A `log`-only event carrying a `collectd.severity` attribute is a notification, and the codec's
diagnostics mirror `collectd_in`'s: `notification_dropped` (severity absent despite being
attempted, or outside `{1, 2, 4}`), `empty_message`, `oversize_notification`, and
`message_truncated` (a message over 255 bytes).

##### `graphite_out`

`crates/logit-outputs/src/graphite.rs`, `docs/adr/graphite-carbon-relay.md`.

**The codec emits its own counters and diagnostics directly**, following `collectd_out`'s model
rather than `statsd_out`'s. `logit_proto::graphite`'s module doc has the full mapping-to-counter
table:

- `logit.output.metrics.skipped{reason|metric_kind}`
- `logit.output.metrics.degraded{metric_kind}` (a multi-value kind expanded, once per record)
- `logit.output.metrics.normalized{reason="path_sanitized"|"tag_sanitized"}`
- `logit.output.tags.dropped{reason="dialect"|"unrepresentable"|"empty"|"collision"}`
- `logit.output.tags.normalized{reason="multi_value"}` (lossy; see above)

The sink's `with_telemetry`/`with_diagnostics` feed the codec, so both halves of one `send` appear
under one component id, as for `collectd_out`. The sink adds only what a socket send can produce:

- `logit.output.batch.bytes`, `logit.output.request.duration`, and
  `logit.output.requests{class="ok"|"clean"|"ambiguous"|"rejected"|"refused"}`: `syslog_out`'s shape, on
  both transports.
- `logit.output.reconnects` (count, TCP only): every connect *after* the first, including the
  redial after the probe finds a pooled connection closed, as for `syslog_out`.
- `logit.output.messages`: entries actually sent (one plaintext line, or one
  already-length-prefixed pickle frame).
- `logit.output.datapoints`: Σ each sent entry's own datapoint count (`MessageBuf<usize>`'s `meta`).
  The two coincide for plaintext (every line's meta is `1`) and can differ for pickle, whose frames
  each carry several datapoints.
- `logit.output.datagrams` (UDP only): datagrams actually sent, `collectd_out`'s concept.
- `logit.output.messages.dropped{reason="oversize_datagram"}` plus a throttled `oversize_datagram`
  diagnostic: `EMSGSIZE` on one already-packed UDP datagram, as for `statsd_out`/`collectd_out`.
  That datagram's datapoints are dropped, not the whole batch, and sending continues.

##### `otlp_out`

`crates/logit-outputs/src/otlp.rs`, codec in `crates/logit-proto/src/otlp/`.

- `logit.output.requests{signal, class}` (count, one per request): `signal` is `logs`, `metrics`,
  or `traces`. Over OTLP/HTTP, `class` is the status class (`"1xx"|"2xx"|"3xx"|"4xx"|"5xx"|"other"`,
  `crates/logit-outputs/src/http.rs`'s `status_class`). Over OTLP/gRPC, it's the `grpc-status`
  name (`"ok"|"cancelled"|"unknown"|"invalid_argument"|"deadline_exceeded"|"permission_denied"|
  "resource_exhausted"|"aborted"|"out_of_range"|"unimplemented"|"internal"|"unavailable"|
  "data_loss"|"unauthenticated"|"other"`). On either
  transport, a transport error or a timeout is `network_error`.
- `logit.output.records.rejected{signal}` (count): records a collector rejected through a
  successful response's `partial_success`, with a throttled `otlp_partial_success` diagnostic
  carrying the collector's message.
- `logit.output.records.dropped{signal, reason="rejected"}` (count): the records of a signal's
  request the destination answered with a `Rejected` verdict (an HTTP 3xx, or a 4xx other than
  405, 407, and 429; or any gRPC status but the transient ones). An auth answer is counted here
  too, since a credential can be scoped per signal. The send goes on to the other signals, and
  returns `Ok` if any was accepted. A throttled `signal_rejected` diagnostic names the signal, the status, and the record
  count, with a hint to place `has_signal` or `keep_signals` ahead of the sink. Counts on the
  attempt that got the verdict, whether or not the send ends `Ok`. See [ADR `delivery-semantics`](../adr/delivery-semantics.md)'s
  "Amendment: per-request verdicts (2026-10-04)". A rejected signal is remembered for the batch
  and not resent on a retry, so its records count once per batch.
- `logit.output.records.dropped{signal, reason="ambiguous_at_most_once"}` (count): under
  `delivery: at_most_once`, the records of a signal answered with an `Ambiguous` verdict, and of
  every signal of the batch not yet sent, when that answer drops the batch. Signals accepted
  earlier in the batch aren't counted, though the runtime's `events.dropped` counts their events
  with the batch. See [ADR `sink-fault-classes`](../adr/sink-fault-classes.md)'s "Amendment:
  `otlp_out` retries per signal (2026-10-05)". Every per-signal counter has a record count to
  read, `SignalPayload::records`: log records, spans, or encoded metric points, not counting a
  metric the encoder skipped.
- From the encoder, for metric kinds OTLP can't carry exactly:
  `logit.output.metrics.degraded{metric_kind="samples"|"distribution"}` (sent as a `Summary`), and
  `logit.output.metrics.skipped{metric_kind="set_members"|"set"|"gauge_delta"}`, each with a
  throttled diagnostic: `otlp_set_members_metric_skipped`, `otlp_set_metric_skipped`, and the
  shared `gauge_delta_unresolved`.

There's no `logit.output.request.duration`; layer 2's `logit.component.send.duration` times each
attempt. The class table above says which of this sink's counters count once per batch and which
once per attempt.

##### `datadog_out`

`crates/logit-outputs/src/datadog.rs`, [ADR `datadog-agent-and-intake-relay`](../adr/datadog-agent-and-intake-relay.md).
One `send` is up to eight routes' requests, so every point carries `route`: `series`,
`distribution_points`, `sketches`, `check_run`, `events`, `logs`, `traces`, or `stats`.

| Name | Kind | Meaning |
|---|---|---|
| `logit.output.requests{route, class}` | count | one per request; `class` is the status class (`status_class`), or `network_error` for a transport error or timeout |
| `logit.output.request.duration{route}` | timing | one per request |
| `logit.output.request.bytes{route}` | count | the body as sent, after compression, for a request that got an answer or failed after it may have left (a timeout); a refused connection counts none |
| `logit.output.records{route}` | count | entries in a request Datadog accepted: series points, samples records, and sketches by record; logs, events, checks, spans, and stats groups by event. On the series route, less the series the `202` body names as dropped |
| `logit.output.records.rejected{route="series"}` | count | a series Datadog accepted the request for but dropped, one per `errors` entry in its `202` body (a point too far in the future, say), at most the request's entries; on the attempt that got that answer |
| `logit.output.records.dropped{route, reason="stale"}` | count | a record outside Datadog's window at the batch's send time, read once per batch: a metric more than 1h old or 10 min ahead, a log or event more than 18h old, a check more than 10 min old; once per batch |
| `logit.output.records.dropped{route, reason="too_many_tags"}` | count | a series, distribution point, or sketch record whose `tags` list holds more than 100 strings, over Datadog's limit; once per batch |
| `logit.output.records.dropped{route, reason="oversize"}` | count | an event whose body alone is over the route's byte limit, once per batch; or every entry of a request Datadog answered `413`, on the attempt that got that answer |
| `logit.output.records.dropped{route, reason="rejected"}` | count | every entry of a request Datadog answered with a 3xx or a `Rejected` 4xx (a `400`, say), on the attempt that got that answer. The send goes on to the next request. A `413` counts as `oversize` instead; a `401`, `403`, `404`, `405`, or `407` is `Refused` and a `408` or `429` `Ambiguous`, and none of them counts here |
| `logit.output.records.dropped{route="traces", reason="needs_agent_processing"\|"not_datadog_origin"}` | count | a span whose chunk's root has no `_top_level` mark: raw tracer output, or not a Datadog span at all; once per batch |

A dropped record is never sent, so a `buffer.disk:` replay after a long outage shows up here as
`stale`, not as a delivery. Every attempt at a batch measures the stale windows from the one send
time `observe_batch` read, so a point dropped as stale on one attempt is never sent on another.
The codec's own points (`logit.output.metrics.skipped`, including every kind no route carries;
`metrics.degraded`, `tags.dropped`, `spans.degraded`, `stats.*`) are the `datadog` codec's, under
[Codecs](#codecs), and this sink doesn't repeat them. They count once per batch, per route (the
class table above).

`Diagnostics` keys, each throttled: `api_key_rejected` (a `403`: Datadog refused the key; the key
itself is never logged), `request_refused` (a `401`, `404`, `405`, or `407`, which the sink holds
and retries), `request_rejected` (any other non-retryable `4xx` or `3xx`, quoting 256 bytes of the
body with the key redacted, and the entries dropped), `oversize` (an event dropped for its size),
`too_many_tags` (records over the tag limit, naming the route and the first record's metric), and
`series_rejected` (a series `202` naming dropped series, quoting the first entry with the key
redacted).

##### `datadog_trace_out`

`crates/logit-outputs/src/datadog_trace.rs`, [ADR `datadog-agent-and-intake-relay`](../adr/datadog-agent-and-intake-relay.md).
One `send` is up to two routes' requests, so every point carries `route`: `traces` or `stats`.

| Name | Kind | Meaning |
|---|---|---|
| `logit.output.requests{route, class}` | count | one per request; `class` is the status class (`status_class`), or `network_error` for a transport error or timeout, over TCP or the Unix socket alike |
| `logit.output.request.duration{route}` | timing | one per request |
| `logit.output.request.bytes{route}` | count | the body as sent, after compression, for a request that got an answer or failed after it may have left (a timeout); a refused connection or a missing socket file counts none |
| `logit.output.records{route}` | count | spans (`traces`) or stats groups (`stats`) in a request the Agent accepted |
| `logit.output.records.dropped{route, reason="oversize"}` | count | a trace's spans, or a stats group, too large for the Agent's 25 MiB request limit alone, once per batch; or every record of a request the Agent answered `413`, on the attempt that got that answer |
| `logit.output.records.dropped{route, reason="rejected"}` | count | every record of a request the Agent answered with a 3xx or a `Rejected` 4xx (a `400`, say), on the attempt that got that answer. The send goes on to the next route. A `413` counts as `oversize` instead; a `401`, `403`, `404`, `405`, `407`, or `415` is `Refused`, a `408` `Ambiguous`, and a `429` `Clean` or `Ambiguous`, and none of them counts here |

The codec's own points are the `datadog` codec's, under [Codecs](#codecs), and this sink doesn't
repeat them. They count once per batch, per route (the class table above). The one to watch here
is `logit.output.spans.degraded{reason="no_wire_form"}` under `version: v0.4`: the trace chunk and
tracer payload fields v0.4 can't carry. A tracer header's carrier doesn't count there, because the
request header carries it. A batch-resource carrier counts once per request body (the
paragraph on Datadog codec counters above).

`Diagnostics` keys, each throttled: `request_rejected` (a non-retryable `4xx`, `3xx`, or `1xx`,
quoting 256 bytes of the body), `request_refused` (a `401`, `403`, `404`, `405`, `407`, or `415`,
which the sink holds and retries), `oversize` (a trace or stats group dropped for its size), and
`bad_header` (a tracer header left out because its attribute isn't a legal header value, once per
batch).

##### `splunk_hec_out`

`crates/logit-outputs/src/splunk.rs`, codec in `crates/logit-proto/src/splunk/`,
[ADR `splunk-hec-relay`](../adr/splunk-hec-relay.md). Requests carry `route`: `event` for a
`/services/collector/event` body, `ack` for an acknowledgment poll.

| Name | Kind | Meaning |
|---|---|---|
| `logit.output.requests{route, class}` | count | one per request; `class` is the status class (`status_class`), or `network_error` for a transport error or timeout |
| `logit.output.request.duration{route}` | timing | one per request |
| `logit.output.request.bytes{route}` | count | the body as sent, after compression, for a request that got an answer or failed after it may have left (a timeout); a refused connection counts none |
| `logit.output.records` | count | records in a body Splunk accepted: one per log or span object, one per `metric_name:` field; also the records ahead of an object a `400` per-object code (6, 7, 12, 13, or 15) named, which are assumed indexed |
| `logit.output.records.dropped{reason="oversize"}` | count | an object larger than `max_body_bytes` alone, never sent, once per batch; or a lone object Splunk Cloud answered as over its cap (`400` code 6 at object 0 of a body over 5 MiB), on each attempt that gets that answer |
| `logit.output.records.dropped{reason="invalid_event"}` | count | the object a `400` per-object code (6, 7, 12, 13, or 15) named, dropped before the rest of its body is resent once |
| `logit.output.requests.rejected{code}` | count | one per `/event` request answered with a non-retryable status: `code` is the body's HEC code when Splunk documents it (`4` for an invalid token, `6` for invalid data, …), else `other` |
| `logit.output.acks{result}` | count | under `ack: true`, one per `/event` request: `acked`, `timeout` (still unacknowledged at `ack_timeout`, which fails the batch as ambiguous), or `unsupported` (a `200` with no `ackId`, or a poll answered `400` code 14: the token doesn't acknowledge, and the request counts as delivered) |

The codec's own counters (`logit.output.metrics.skipped` and `metrics.degraded` by `metric_kind`
under `multi_value`, `metrics.normalized{reason="name_sanitized"}`, `tags.dropped`,
`events.skipped`, `spans.degraded`) are in the tables of
`crates/logit-proto/src/splunk/mod.rs`'s module doc and its `logs`, `metrics`, and `spans`
submodules, under this component's id, and this sink doesn't repeat them.

`Diagnostics` keys, each throttled: `token_rejected` (a `401` or `403`, or a token code 21 or
22 on a `400`), `request_refused` (a channel code 10, 11, or 28, or code 16: every request would
get it), `request_rejected` (any other non-retryable `4xx` or `3xx`, quoting 256 bytes of the
body), `invalid_event` (an object dropped on a per-object code), `oversize` (an object dropped for its size, or a body split on Splunk
Cloud's oversize answer), `ack_unsupported`, and `ack_timeout`. The token never appears in any of
them. The class table above says which count once per batch: the codec's counters, and the
`max_body_bytes` drop with its `oversize` diagnostic; everything Splunk's answer decides counts per
attempt.

##### `logit_out`

`crates/logit-outputs/src/logit.rs`,
[ADR `native-transport-handshake-and-ack`](../adr/native-transport-handshake-and-ack.md),
[ADR `native-hop-identity-and-sequence`](../adr/native-hop-identity-and-sequence.md),
[ADR `native-hop-send-window`](../adr/native-hop-send-window.md),
[ADR `native-hop-named-acks`](../adr/native-hop-named-acks.md).

- `logit.proto.frames{direction="out",compression}` and `logit.proto.frame.bytes`: the
  send-side mirror of `logit_in`'s pair.
- `logit.output.ack.duration` (timer, one sample per read of the connection for an `Ack`; an
  `await_ack` answered by an `Ack` already read records none): the wait for one `Ack`,
  finer-grained than layer 2's `logit.component.send.duration`, which times a whole attempt or
  windowed round, connect, handshake, and writes included.
- `logit.output.in_flight` (gauge): frames written and not yet committed, set on every change. It
  reads 0 after every connection drop, a cancelled call's included. A value that sits at
  `logit.output.window` means the round trip, or the peer's forwarding, bounds this sink.
- `logit.output.window` (gauge): the window the live connection negotiated, the smaller of the
  configured `window` and the peer's answer, and at least 1. It reads 1 after every connection
  drop.
- `logit.output.reconnects` (count): every connect *after* the first whose `HelloAck` passed
  validation. A climbing count in steady state means the peer or the network, not this sink, is
  unstable.
- `logit.output.batches.resumed` (count): frames a reconnect committed from `HelloAck.marks`
  without resending them ([ADR `native-hop-named-acks`](../adr/native-hop-named-acks.md),
  decision 4).
- `logit.output.requests{class="ok"|"clean"|"ambiguous"|"rejected"|"refused"}` (count): the `Fault` taxonomy
  as request-outcome classes, one per `submit` that fails with a `Fault` and one per `await_ack`
  that returns. A `send` is a `submit` then an `await_ack`, so it counts once, and the total equals
  the number of `send` calls that returned. A connect or handshake failure and a batch too large to
  send count as failed submits. A submit that fails with frames already in flight (a stalled or
  failed write) carries no `Fault` and isn't counted: the `await_ack`s after it count the round's
  outcome, `ok` for each frame an `Ack` commits, an `await_ack` answered by an `Ack` already read
  included, and the class of the failure that ends it. A `Rejected` past the head
  is counted when it becomes the head. A cancelled call (the shutdown grace)
  returns nothing and isn't counted; `logit.component.errors` covers it. `ok` is an acknowledged
  frame; `clean` a failure before a frame was completely written and flushed with nothing in flight,
  or a `Reject{GOING_AWAY}` read in place of an `Ack`; `ambiguous` a lost `Ack` (a timeout, an EOF,
  a reset, another message) or an `Ack` naming no run of the frames in flight; `rejected` a
  size check or a frame-build error at the head, `Reject{FRAME_TOO_LARGE}`, or a rejected `Ack`
  naming the head (that head alone: the connection and the frames behind it stay,
  [ADR `native-hop-ack-status`](../adr/native-hop-ack-status.md)), and `refused` a `HelloAck` that doesn't answer the `Hello`
  (another version, an unoffered codec or compression, a mark for an identity `Hello.senders`
  didn't list, or two marks for one identity), or a reject that names a protocol mismatch ([ADR
  `sink-send-path-and-attempt-accounting`](../adr/sink-send-path-and-attempt-accounting.md),
  decision 6).

##### `prometheus_out`

`crates/logit-outputs/src/prometheus.rs`, codec in `crates/logit-proto/src/prometheus/`,
[ADR `prometheus-scrape-and-exposition`](../adr/prometheus-scrape-and-exposition.md) and
[ADR `prometheus-remote-write`](../adr/prometheus-remote-write.md).

**Two modes, with different metrics.**

**Registry mode (`bind:`)** is the one pull sink, so it has no
`requests`/`request.duration`/`batch.bytes`: nothing is pushed per batch. Instead:

- `logit.output.scrapes{class="ok"|"not_found"|"method"}` (count, one per inbound HTTP request).
  `ok` means *rendered*, not acknowledged, because a `Full<Bytes>` body has no completion hook.
- `logit.output.scrape.bytes`: post-gzip when negotiated, so it measures transfer cost rather than
  exposition size.
- Registry state, which no push sink holds: `logit.output.series` (gauge, series held after each
  `send`); `logit.output.series.evicted{reason="expired"|"cardinality"}` (the `expire_after` sweep,
  run on every `send` *and* every scrape, and the `max_series` LRU cap); and
  `logit.output.metrics.type_conflict` (a family re-typed across batches, evicting every series
  held under the old type).

**Sender mode (`endpoint:`)** is the ordinary push shape and uses `otlp_out`'s vocabulary
**exactly**, because it reuses that transport's own `Fault` table as code
(`crates/logit-outputs/src/http.rs`):

- `logit.output.requests{class="1xx"|"2xx"|"3xx"|"4xx"|"5xx"|"other"|"network_error"}` (count, one
  per request issued). There's no `429` class: a 429 is a `4xx`, and splitting it out would
  contradict `classify_status`, which reads the same status to pick the `Fault`. There's
  no `timeout` class either, because a timeout is a transport error and lands in `network_error`.
  `3xx` is a real class here, because this client doesn't follow redirects. There's no `signal`
  tag, unlike `otlp_out`: this sink carries exactly one signal, and a tag that never varies is
  noise.
- `logit.output.request.duration` (timing, one per request actually issued): the spelling
  `graphite_out`/`collectd_out`/`syslog_out`/`statsd_out`/`influxdb_out` use, which `otlp_out`
  doesn't have.
- `logit.output.samples` (count, on a successful request only): samples in the body as the codec
  counted them while encoding, not guessed from family counts, because one `Series` is one sample
  for a gauge and several for a histogram. It mirrors `prometheus_in`'s `logit.input.samples` in
  bind mode, so the two ends of a remote-write relay are comparable.

A batch that produces no series issues no request and reports none of the three.

**The codec's `PrometheusEncoder` counts in both modes.** In registry mode it's shared by `send`
and render, so both total under one component, and counts on every `send` and every render; in
sender mode it's a plain field with one direction, and counts once per batch however many attempts
it takes (the class table above).

- `logit.output.metrics.skipped{metric_kind="delta_sum"|"delta_histogram"|"gauge_delta"|
  "exponential_histogram"}` and `{reason="no_recorded_value"|"type_conflict"|"name_collision"}`.
  The latter two reasons are *within* one batch, distinct from the cross-batch `type_conflict`
  counter above.
- `logit.output.metrics.degraded{metric_kind="non_monotonic_sum"|"distribution"|"samples"|"set"|
  "set_members"}` and `{reason="exemplar_dropped"|"unit_not_suffix"}` (render-side).
- `logit.output.labels.dropped{reason="unrepresentable"|"reserved"|"collision"}`.
- `logit.output.labels.normalized{reason="multi_value"}`: a multi-valued attribute collapsed to its
  last representable element, because a Prometheus label set has no multi-value label. Lossy; see
  above.

Some reasons exist only on one path:

- `skipped{reason="stale"}` is the text writer stepping over a `Point::Stale`, which it sees only on
  a relay that fed it one.
- `skipped{reason="no_timestamp"}`, `skipped{reason="invalid_labels"}` (a family with an empty
  name; `__name__` may not be empty), `degraded{reason="sub_ms_collapsed"}` (two readings of one
  series landing on one millisecond, the later winning; `docs/known-gaps/mappings.md`), and
  `labels.dropped{reason="empty_value"}` are remote-write encode's, where the wire forbids what the
  exposition grammar merely renders differently.
- `skipped{reason="no_recorded_value"}` means something narrower in sender mode. The encoder runs
  with `with_stale_markers(true)` there, so a flagged `Gauge`/`Sum`/marker-untyped record is written
  as a stale marker rather than skipped. Only a flagged `Histogram`/`Summary`/sketch, kinds that
  expand to several derived series, still counts.

`Diagnostics` keys: `delta_temporality_unresolved` (both delta arms, naming the `aggregate` with
`temporality: cumulative` fix); the shared `gauge_delta_unresolved` key `influxdb_out`/`statsd_out`
use; `prometheus_exponential_histogram_skipped`; `prometheus_accept_failed` from the
registry-mode listener's accept loop; and `remote_write_rejected`, one per non-2xx answer in
sender mode (`crates/logit-outputs/src/prometheus.rs`'s module doc, "Faults, retries and duplicate
safety", has what it carries).

Retry is layer 2 in both modes: in registry mode `send` is an in-memory upsert with nothing to
retry, and in sender mode one `send` is one attempt by design, with `write_loop` owning the retry.

##### `null_out`

`crates/logit-outputs/src/null.rs`, `docs/plans/load-test-harness.md`.

**Layer 2 only.** `send` does no encoding and no I/O, so it has nothing of its own to report. The
generic write loop's `logit.component.batches.received`/`events.received`/`send.duration` already
cover a sink that never fails and never varies; a dedicated counter would duplicate
`events.received`.

#### Codecs

A codec shared by a listener and a sink reports through whichever component's handles it was
given, so these points appear under the component id of `datadog_in`, `datadog_trace_in`,
`datadog_out`, or `datadog_trace_out`.

##### `datadog`

`crates/logit-proto/src/datadog/`, [ADR `datadog-agent-and-intake-relay`](../adr/datadog-agent-and-intake-relay.md).
The module doc of `logit_proto::datadog` has the full mapping-to-counter tables.

| Name | Kind | Meaning |
|---|---|---|
| `logit.input.metrics.skipped{reason="bad_series"\|"bad_point"\|"null_value"\|"non_finite_value"\|"empty_distribution"}` | count | a series or point dropped while the rest of the request decodes: a malformed series, a malformed point, a v1 `null` value, a non-finite protobuf value, or a distribution point with no values |
| `logit.input.metrics.skipped{reason="bad_sketch"\|"empty_sketch"\|"legacy_distribution"}` | count | a malformed `Dogsketch`, an empty one (no bins, zero count), or a legacy `distributions` entry, which is ignored |
| `logit.input.metrics.degraded{reason="no_timestamp"}` | count | a point or sketch with no timestamp, stamped with `received_at` |
| `logit.output.metrics.skipped{metric_kind="cumulative_sum"\|"non_monotonic_delta_sum"\|"gauge_delta"\|"set_members"\|"histogram"\|"exponential_histogram"\|"summary"}` | count | a metric kind no Datadog route carries; counted by the series encoders only |
| `logit.output.metrics.skipped{reason="no_recorded_value"\|"non_finite_value"\|"empty_sketch"\|"oversized_sketch"}` | count | a flagged record, a non-finite value, a sketch with nothing in it, or a sketch whose counts split into more than `MAX_DOGSKETCH_ENTRIES` `k`/`n` entries |
| `logit.output.metrics.degraded{reason="set_estimate"\|"sample_rate_expanded"\|"rebinned"\|"fractional_count"}` | count | a `Set` sent as a gauge of its estimate, a sampled `Samples` expanded into repeated values, a non-Agent sketch re-binned into the Agent mapping, or a fractional bin count rounded |
| `logit.output.tags.dropped{reason="unrepresentable"\|"no_wire_form"}` | count | a tag value with no tag form (`Map`, `Bytes`, `Null`) or a carrier attribute of the wrong type; a `datadog.*` carrier the target route has no field for |
| `logit.input.logs.skipped{reason="not_an_object"\|"no_message"}` | count | a log array element that isn't an object, or a log with no `message` |
| `logit.input.events.skipped{reason="malformed"\|"no_title"}` | count | an events-envelope group that isn't an array or an item that isn't an object, or an event with neither a title nor a text |
| `logit.input.metrics.skipped{reason="malformed"\|"no_name"\|"invalid_status"}` | count | a service check that isn't an object, has no `check` name, or has a status outside 0 to 3 |
| `logit.output.metrics.skipped{reason="invalid_status"}` | count | a service-check event with no status in 0 to 3, on either its `statsd.service_check.status` or its gauge |
| `logit.output.tags.dropped{reason="reserved_key"}` | count | a log attribute named `message` or `timestamp`, which would collide with the log's own wire fields |
| `logit.input.stats.skipped{reason="malformed_payload"\|"malformed_bucket"\|"malformed_group"}` | count | an APM stats `ClientStatsPayload` (in an intake `StatsPayload`), bucket, or group that isn't a well-formed map, dropped while the rest decodes |
| `logit.input.stats.skipped{reason="empty_bucket"}` | count | an APM stats bucket with no groups, which decodes to no events |
| `logit.input.stats.skipped{reason="interpolation"\|"bad_sketch"}` | count | an `OkSummary`/`ErrorSummary` dropped from its group: an interpolated DDSketch mapping, or bytes that aren't a usable DDSketch |
| `logit.input.stats.degraded{reason="unknown_trilean"\|"bucket_start_overflow"\|"inexact_count"}` | count | an `IsTraceRoot` outside 0 to 2 (dropped), a bucket `Start` above `i64::MAX` (clamped), or a count above 2^53 that `f64` can't hold exactly |
| `logit.output.stats.skipped{reason="unrecognized_record"}` | count | a record on an APM stats event that is none of the six stats records, or one of their names with another kind |
| `logit.output.stats.degraded{reason="fractional_count"\|"bad_count"\|"count_overflow"\|"negative_timestamp"}` | count | a stats count rounded to an integer, a negative or non-finite count sent as 0, a count above 2^64 sent as `u64::MAX`, or a negative timestamp sent as bucket start 0 |
| `logit.output.stats.degraded{reason="agent_mapping"\|"bin_limit"\|"exact_summary"}` | count | a stats summary sent under the Agent mapping's logarithmic reading, with a bin limit other than 2048, or with an exact summary the DDSketch protobuf can't carry |
| `logit.input.spans.skipped{reason="malformed"\|"idx_payload"}` | count | a span, trace array, or chunk that doesn't parse (a v0.5 span of the wrong arity or with a dictionary index out of range included), dropped while the rest decodes; an `AgentPayload`'s v1.0 `idxTracerPayloads` entry, which isn't implemented |
| `logit.input.spans.degraded{reason="bad_tid"\|"negative_duration"\|"key_collision"\|"timestamp_range"\|"bad_attribute_type"\|"invalid_utf8"}` | count | an unparseable `_dd.p.tid` (kept as an attribute, high half zero), a negative duration clamped to 0, one key in two of `meta`/`metrics`/`meta_struct` (or a field spelled like a carrier) keeping one value, a span event time above `i64::MAX` clamped, a span event attribute of unknown type dropped, or a non-UTF-8 string read lossily |
| `logit.output.spans.degraded{reason="no_wire_form"}` | count | a span field the target form has no home for, one per item: `status: Ok`, span `flags`, a status message, `trace_state`, a dropped count; `datadog.chunk.*` in v0.4/v0.5; `datadog.tracer.*` in v0.4/v0.5 and `datadog.agent.*` below `AgentPayload` (once per request body); `meta_struct`, links, and events in v0.5 |
| `logit.output.spans.degraded{reason="int_as_f64"\|"json_text"\|"negative_duration"\|"timestamp_range"}` | count | an integer attribute sent as an inexact `metrics` double, an `Array`/`Map`/`Null` sent as JSON text, a span ending before it starts sent with duration 0, or a negative span event time sent as 0 |

`Diagnostics` keys: `bad_series` and `bad_sketch`; `malformed_log`, `bad_timestamp` (a log
timestamp that is neither a number nor RFC 3339, stamped with `received_at`), `malformed_event`,
`malformed_service_check`; `malformed_stats` (a dropped stats payload, bucket, or group) and
`bad_stats_sketch` (a dropped stats summary); `malformed_span` (a dropped span, trace array, or
chunk); `oversized_sketch` (an outgoing sketch dropped past the entry cap).

A counter for a batch-resource attribute or a stats bucket counts once per request body the
encoder writes, not once per record; the paragraph after the sink class table names them and says
what a body is on each Datadog sink.

## Metrics from Lua scripts

A script's `process()`/`flush()` can call `telemetry.count(name, n, tags?)` and
`telemetry.gauge(name, v, tags?)` (`crates/logit-script/src/telemetry.rs`, wired in by
`ScriptWorker::with_telemetry`, a builder rather than a constructor parameter, so
`ScriptWorker::new(script)` call sites are unchanged). A script's points go through the same
buffer, `internal` component, and downstream tools as everything else; there's no separate
script-telemetry pipeline to configure.

**Cardinality here is enforced by convention, not by the type system.** Every Rust `Telemetry`
call takes `&'static str` names and tags so that cardinality is bounded by code the compiler checks.
A Lua-provided string can't satisfy that at compile time, so it's round-tripped through the
process's own interner (`interner::resolve(interner::intern(s))`, which returns a genuine
`&'static str`). That reuses accepted infrastructure rather than adding a new leak mechanism, but
it makes the script author, not the compiler, responsible for never building a metric name or tag
value from per-event data. Full reasoning and the alternatives considered:
[ADR `lua-authored-telemetry-cardinality`](../adr/lua-authored-telemetry-cardinality.md). See
`docs/design/lua-api.md`'s "Emitting telemetry from a script" for the script-author-facing version
of this warning.

Scripts get no `timing()`: the sandboxed stdlib exposes no clock (`table`/`string`/`math` only), so
a script has no way to produce a duration.

**[`crates/logit-script/src/telemetry.rs`] holds two more boundaries**, because a script's input is
less constrained than a Rust call site's:

- **`Telemetry::is_enabled()` is checked first, on every call.** Reading a Lua argument, converting
  it, and interning it are all real work. A disabled handle (no `internal` component configured)
  skips all of it, not just the final `Telemetry::count` call; otherwise a pipeline with telemetry
  off would still permanently intern whatever a script passes. This carries the
  zero-cost-when-disabled guarantee all the way to the Lua boundary.
- **The `logit.` prefix is reserved.** A `(name, tags)` key in a component's buffer doesn't record
  which caller wrote it. A script calling
  `telemetry.count("logit.component.events.received", 1)` would coalesce into, and corrupt, the key
  the runtime itself writes, because `count` and `gauge` on the same key silently convert one into
  the other. The call fails with a clear Lua error naming the reserved namespace.

`component`, `kind`, and `role` are reserved for a point's own identity and can never become part
of a tag, which matters here because a Lua tag key is script-chosen. This holds at two levels:

- **Framework:** `PointKey::new` (`crates/logit-core/src/telemetry.rs`) filters a reserved key out
  *before* building a point's cardinality key, not just at drain time. Overwriting only the label
  would leave two differently-tagged calls (`{kind = "a"}` vs. `{kind = "b"}`) occupying two
  distinct, wasted key slots that drain to indistinguishable points instead of coalescing into
  one. This holds for any caller.
- **Lua binding:** it *rejects* a reserved tag key outright
  (`crates/logit-script/src/telemetry.rs`) rather than relying on that filter. A script that set
  one probably meant something by it, so a clear error beats a silent no-op.

## Adding a new internal metric

1. Decide which layer it belongs to. If it's uniform across every component of a kind, it probably
   belongs in `runtime.rs`/`fanout.rs`, not in one component. If it's specific to what one
   component knows internally, it belongs on that component, through its own `Telemetry` handle.
2. Pick a name following [Naming](#naming), with tags that are `&'static str` constants only.
3. Call `count`/`gauge`/`timing`/`timer` at the point that already knows the fact. No new type, no
   registration step, and no schema change.
4. Document it in this file's layer-2 tables or the component's layer-3 subsection.
5. If it's genuinely new ground (a new signal type, or a new source of process-level facts), read
   ADR `internal-telemetry-as-pipeline-events`'s "Alternatives considered" first: several shapes
   that look like natural extensions were deliberately not built, for stated reasons.

## What this is not

- **Not a time-series aggregation engine.** The buffer coalesces to bound volume between drains;
  any real windowed aggregation is `aggregate`, attached downstream like any other consumer.
- **Not a scrape endpoint.** There's no pull path; see ADR `internal-telemetry-as-pipeline-events`'s
  alternatives for why. The readiness and liveness endpoint (`docs/deploying.md`'s "Probes and exit
  codes", ADR `admin-readiness-endpoint`) isn't this either: it carries no metrics, and answers
  "can this process do its job right now", not "what are its numbers".
- **Not a replacement for `tracing`.** `Diagnostics` emits through `tracing` (ADR
  `tracing-for-self-logging`), and `TelemetryLayer` (this doc's "Logs" section) is a producer
  feeding this same buffer, alongside points and spans.
