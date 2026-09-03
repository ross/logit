# Pipeline graph

How config wires components together and how the runtime executes the result. Decision record:
[ADR `component-graph-configuration`](../adr/component-graph-configuration.md). This document is load-bearing per
`AGENTS.md` — read it before touching `logit-config`'s component types or the pipeline runtime.

## Config shape

One flat map. Every component has an id (the map key), a `type`, and a `sources` list naming the
other components it reads from:

```yaml
components:
  metrics_in:
    type: statsd_in
    bind: 0.0.0.0:8125

  windowed:
    type: aggregate
    sources: [metrics_in]
    interval: 10s

  enrich:
    type: lua
    sources: [windowed]
    script: |
      function process(event)
        event.attributes.env = event.attributes.env or "dev"
        return event
      end

  influx:
    type: influxdb_out
    sources: [enrich]
    url: http://influxdb:8086
    org: logit
    bucket: metrics
    token: !env INFLUXDB_TOKEN
```

This is the same statsd → aggregate → Lua → InfluxDB shape as today's
[examples/statsd-to-influxdb.yaml](../../examples/statsd-to-influxdb.yaml), reshaped: no `inputs`/
`outputs`/`pipelines` split, no separate `transforms:` chain — `sources` carries all the wiring, and
a "pipeline" is just whatever subgraph is reachable from a listener. There is no config-level notion
of a pipeline at all.

In Rust:

```rust
pub struct Config {
    #[schemars(schema_with = "non_empty_components_schema")]
    pub components: HashMap<String, Component>,
}

pub struct Component {
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(flatten)]
    pub kind: ComponentKind,
}

#[serde(tag = "type", rename_all = "snake_case")]
pub enum ComponentKind {
    StatsdIn { bind: String },
    SyslogIn { bind: String },
    OtlpIn { bind: String },
    FileTail { paths: Vec<String>, checkpoint_path: Option<String> },
    LogitIn { bind: String },

    Lua { script: String, interval: Option<Duration> },
    LuaFile { lua_file: String, interval: Option<Duration> },
    Aggregate { interval: Duration },
    Json { skip_to_brace: bool },
    // Turns attributes into metrics on the same event (docs/adr/kv-metrics-semantics.md).
    // Deliberately no `tags:` field -- tag selection is `Keep`'s job.
    KvMetrics { counters: Vec<MetricSpec>, gauges: Vec<MetricSpec>, distributions: Vec<MetricSpec> },
    // An allowlist: retains only the named attributes. Place before `aggregate` -- its
    // `SeriesKey` includes the whole attribute set, so pruning first bounds cardinality.
    Keep { fields: Vec<String> },
    // A denylist: drops the named attributes, keeping the rest.
    Remove { fields: Vec<String> },
    // Drops an event that doesn't carry a wanted signal -- never mutates a forwarded event
    // (docs/adr/signal-filtering-components.md).
    HasSignal { signals: Vec<Signal>, mode: MatchMode },
    // Retains only the listed signals' payloads, clearing the rest -- an allowlist, `has_signal`'s
    // mutating counterpart.
    KeepSignals { signals: Vec<Signal> },
    // A denylist: clears the listed signals' payloads, keeping the rest.
    DropSignals { signals: Vec<Signal> },
    // logfmt, kv, regex, csv, rename, filter, sample, throttle, dedup —
    // as each lands in logit-transforms, same shape: a `ComponentKind` variant, no `sources`
    // opinion of its own (that lives on `Component`, uniformly).

    InfluxDbOut { url: String, org: String, bucket: String, token: String },
    OtlpOut { endpoint: String },
    LogitOut { endpoint: String },
}
```

**Naming: every protocol kind is suffixed `_in`/`_out`.** Merging `InputConfig` and `OutputConfig`
into one tagged enum creates real collisions — `Otlp { bind }` (a listener) and `Otlp { endpoint }`
(a sink) can't both be `type: otlp` in an internally-tagged enum, and the same is true of the two
`Logit` variants. Suffixing *every* protocol kind uniformly, not just the two that collide today,
keeps the rule predictable as more protocols gain a second side — `syslog_out` (RFC 3164/5424 over
UDP or TCP, `docs/adr/syslog-output.md`) is exactly that case, landing well after `SyslogIn`.
Transform kinds — `lua`, `lua_file`, `aggregate`, `json`, `kv_metrics`, `keep`,
`remove`, `has_signal`, `keep_signals`, `drop_signals`, and any future native transform — take no
suffix; there's only ever one direction for a transform to be.

**`interval` stays a per-kind optional field, unchanged from today.** `lua`/`lua_file` already carry
an optional flush interval (`docs/adr/aggregation-window-semantics.md`); `aggregate` requires
one. That doesn't change here — only where the field lives changes (on the component's `type`-tagged
kind, same as today) — and the existing "a zero interval is rejected" rule (`require_nonzero_interval`,
`crates/logit-cli/src/pipeline.rs`) carries over as-is, generalized to any kind with an `interval`.

**Implementation risk to verify early:** `schemars` 0.8 (pinned in `Cargo.toml`) generating
`#[serde(flatten)]` over an internally-tagged enum produces an `allOf` composition in the emitted
JSON Schema. Confirm `schema/logit.schema.json` still validates real configs and that `serde_norway`
round-trips it before committing to this exact shape; if `flatten` misbehaves, the fallback is
repeating `sources: Vec<String>` on every `ComponentKind` variant instead of factoring it onto
`Component`.

## Environment substitution

`!env VAR_NAME` is a YAML tag, valid as the value of any field on any component, resolved against
the process environment when the config is loaded (`crates/logit-cli/src/config.rs`) --
[ADR `env-yaml-tag`](../adr/env-yaml-tag.md). It's what `influxdb_out`'s `token` above is for: rather
than a dedicated `token_env` field (an earlier, rejected design -- see the ADR), any field that's
secret or deployment-specific spells it the same way:

```yaml
url: !env INFLUXDB_URL
token: !env INFLUXDB_TOKEN
```

Resolution happens on the parsed YAML tree, before serde ever sees it, so `Config`'s types carry no
trace of it: a `!env`-tagged field looks, to serde, exactly like a field written with the
substituted value inline. The substituted value is re-parsed as a YAML scalar (`8125` becomes an
integer, `true` a bool; anything else, including a value that happens to look like a mapping or
sequence, stays a string) -- this is what lets `!env` work on a non-string field, at the cost of a
secret that happens to look like a number or bool needing to be quoted at the source.

Every `!env` reference must resolve, unconditionally, for all three commands -- `logit graph`
included, even though it never reads a component's field values (only `sources` and `type`). Any
tag other than `!env` is a hard error too -- a typo'd tag would otherwise silently deserialize as
the tag's literal argument string instead of failing.

## Roles come from kind, not topology

| Kind class | `sources` | May be another component's source |
|---|---|---|
| Listener (`statsd_in`, `syslog_in`, `otlp_in`, `file_tail`, `logit_in`) | must be empty | required (≥1 consumer) |
| Transform (`lua`, `lua_file`, `aggregate`, `json`, `kv_metrics`, `keep`, `remove`, `has_signal`, `keep_signals`, `drop_signals`) | ≥1 required | required (≥1 consumer) |
| Sink (`influxdb_out`, `stdio_out`, `otlp_out`, `logit_out`) | ≥1 required | must not be |

Deriving role from topology instead ("no sources → listener", "nothing reads it → sink") was
considered and rejected (ADR `component-graph-configuration`): a typo'd source reference would silently turn a real sink into
an orphaned transform, with no error, rather than a clear "did you mean" failure. The kind already
knows its own arity — config just states the edges.

## Validation

Replaces `validate_semantics` (`crates/logit-cli/src/pipeline.rs`). In order:

1. At least one component.
2. Every id appearing in any `sources` list resolves to a defined component.
3. No self-reference (a component listing itself as a source) — a special case of 5, but worth its
   own message since it's the most common typo shape.
4. No duplicate source within one component's `sources` list — a repeated id would otherwise push
   the same consumer onto that source's outbound edge list twice, giving its `Fanout` two live
   `Sender` clones into the same inbox and silently delivering every batch twice (doubling
   telemetry, and doubling every count through an `aggregate` component) rather than being
   rejected as the config typo it almost certainly is.
5. **No cycles.** DFS with a recursion-stack set, or Kahn's algorithm falling back to "nodes remain
   with no zero-indegree candidate." This is the one genuinely new must-have: a cycle plus bounded
   `mpsc` channels is a deadlock, not a slow pipeline, and today's linear-pipeline shape made cycles
   structurally impossible — the graph model makes them a real config mistake to guard against. The
   nodes still unresolved once Kahn's algorithm runs out of zero-indegree candidates are the cycle
   *plus* everything downstream of it, not the cycle alone — the error walks that set back down to
   one concrete cycle path before reporting it, so a downstream victim is never named as if it were
   part of the cycle.
6. Arity per kind, per the table above — a listener with `sources`, a sink with none, a sink named as
   another component's source.
7. Every non-sink component has ≥1 consumer — replaces today's "pipeline has no outputs" check and
   catches the same silent-black-hole failure (a transform whose only consumer was renamed or
   deleted, still accumulating state or running Lua for nothing).
8. Kind is actually implemented — the direct generalization of `require_implemented_input`/
   `require_implemented_output`/`require_implemented_transform` into one check over `ComponentKind`.
9. No zero-length `interval` on any kind that has one (`lua`, `lua_file`, `aggregate`) — unchanged
   from `require_nonzero_interval` today.
10. A `kv_metrics` with `counters`, `gauges`, and `distributions` all empty is rejected — it can
    only ever be a no-op, the same silent-black-hole failure rule 7 exists to catch.
11. A `kv_metrics` distribution entry with no `field` is rejected — a distribution of nothing is
    meaningless (`docs/adr/kv-metrics-semantics.md`).
12. A `kv_metrics` counter, gauge, or distribution entry with an empty `name` is rejected — the
    implemented `influxdb_out` sink can't encode a metric with no measurement name
    (`docs/adr/kv-metrics-semantics.md`).
13. At most one `internal` component — two would each drain (and so split) the same process-wide
    telemetry `Registry`, silently halving whichever one a downstream consumer happened not to be
    reading from rather than failing clearly.
14. A non-default `buffer:` block on a non-sink component is rejected — `buffer:`
    (`docs/adr/buffered-sink-delivery.md`) configures a sink's delivery queue, which only a
    sink has, so a listener or transform carrying one is almost certainly a misplaced block rather
    than a meaningful setting silently ignored.
15. A sink's `buffer.max_batches` or `buffer.max_bytes` of `0` is rejected — an impossible bound
    (no batch could ever be queued) rather than a small one.
16. `internal`'s `span_sample_rate` must be finite and within `[0, 1]` — a config error, not
    something to clamp silently.
17. A non-default `receive:` block is rejected on any kind that is not a **datagram listener**
    (`docs/adr/decoupled-listener-io.md`), today `statsd_in`/`syslog_in`. Deliberately not
    "any non-listener": `internal` is a listener by role but has no socket, no queue, and no
    decoder, so `receive:` on it would be exactly the silently-ignored-setting failure rule 14
    guards against on the sink side.
18. A datagram listener's `receive.max_datagrams`, `receive.max_bytes`, or `receive.batch_max_events`
    of `0` is rejected — the twin of rule 15. `receive.batch_flush_interval: 0s` is **not**
    rejected — it means "no flush timer," a meaningful setting, unlike the count bounds.
19. An empty `signals:` list on `has_signal`, `keep_signals`, or `drop_signals` is rejected.
    `keep_signals`/`drop_signals` additionally reject naming all three signals. Which of the two
    shapes is the silent black hole (rule 7's "no consumer" failure, recast here as "no event
    ever gets through") and which is the no-op (every event forwarded untouched) is *opposite*
    between the two kinds — an allowlist naming nothing keeps nothing (black hole), naming
    everything keeps everything (no-op); a denylist is the mirror. Both shapes are rejected
    either way, but the error message names the right one. `keep`'s empty `fields` list stays
    legal by contrast — "drop every attribute" is a real operation, "drop every event" is not.
    See `docs/adr/signal-filtering-components.md`.

**Sink reachability from a listener needs no separate rule.** It's implied by 2 + 5 + 7: every
acyclic chain of ≥1-source components terminates somewhere, and every non-terminal component in that
chain is required (by 7) to have a consumer, so the chain can only terminate at a sink.

**What disappears:** "input/output claimed by more than one pipeline is not yet supported" — today's
`validate_semantics` rejects this outright because the runtime had no way to express sharing. Under
the graph model it's simply a component with more than one entry in another component's `sources`;
no special-casing needed, and no restriction to state.

## Runtime model

- Each component is a node: one inbox (`mpsc::Receiver<EventBatch>`, capacity `CHANNEL_CAPACITY`,
  unchanged from today) and a `Fanout` — one `mpsc::Sender` per consumer, resolved from the inverted
  `sources` relation.
- **Fan-in is free**: N sources into one component is N cloned `Sender`s feeding the same inbox.
- **Fan-out costs a clone per extra consumer**: exactly the `output_txs.split_last()` pattern
  `send_batch` already uses (`crates/logit-cli/src/pipeline.rs`), generalized from "per output" to
  "per downstream consumer of any node."
- **Build in reverse topological order** — from sinks back toward listeners — so every node's
  outbound `Fanout` is fully wired (every consumer's inbox already exists) before that node can
  start producing. This generalizes what `run_config` already does today (build outputs, then the
  transform-worker thread, then inputs).
- Shutdown cascades by channel closure, propagating from listeners toward sinks in topological
  order — the same "closed inbox → drain and exit" shape `run_pipeline_worker` already implements
  for one chain, now per node.

### Thread model: only Lua needs its own OS thread

`mlua::Lua` is `!Send`/`!Sync` (`docs/design/lua-api.md`'s concurrency section; `AGENTS.md` lists
this as non-optional) — a Lua node cannot be *moved* into an async task at all, so it needs a
dedicated `std::thread`, same as today. What changes is the granularity: today one thread runs an
entire pipeline's transform chain serially, because chain adjacency was guaranteed by
`PipelineConfig.transforms`. In the graph model, adjacency isn't guaranteed — a Lua component's
sources and consumers can be arbitrary other components — so **each Lua component gets its own
thread**, communicating with its neighbors over the same `mpsc` channels every other node uses.

Everything else — listeners, sinks, and native `Send` transforms (`aggregate` today via
`logit-transforms::Aggregator`; `json`/`filter`/etc. as they land in the same crate) — runs as an
ordinary tokio task, no dedicated thread required. This is a strict generalization of today's split
(input/output tasks vs. one worker thread per pipeline), not a new idea — it just now applies per
node instead of per pipeline.

**Fusing a linear run of adjacent Lua nodes back onto one thread** (avoiding a thread and a channel
hop per hop) is a real, identifiable future optimization once thread count in practice warrants it —
explicitly not v1. Thread count is bounded by config size (one component, one thread at most), which
is enough to start from.

### Flush ticks become per-node, not per-pipeline

Today's `run_pipeline_worker` owns one `Vec<Option<Instant>>` deadline schedule across a whole
chain's stages (`next_flush`, `advance_flush_deadline`, `flush_due_stages` in
`crates/logit-cli/src/pipeline.rs`) because every stage in a pipeline shares one thread and one
receive loop. In the graph model each flush-bearing node (an `aggregate` component, or a `lua`/
`lua_file` component with `interval` set) owns its *own* single-entry version of that same
schedule — one deadline, advanced by the same constant-time `advance_flush_deadline` logic, raced
against its own inbox receive via the same `tokio::time::timeout`-around-`recv` pattern already
proven out today. No shared cross-node schedule is needed, and no change to the deadline-advancement
math itself — it was already correct per-stage, just iterated over a `Vec` that no longer needs to
exist.

A flushed event runs through that node's own `Fanout`, exactly like a normally-processed batch —
`flush_stage`'s "flushed output isn't exempt from downstream processing" property
(`docs/adr/aggregation-window-semantics.md`) holds automatically here, because downstream
processing is just "send to the node's consumers," the same path every event takes.

### Node kinds and the transform trait question

`Input`/`Output` (`logit-inputs`/`logit-outputs`) are already traits; native transforms
(`logit-transforms::Aggregator`) are not — today's `Stage` enum in `pipeline.rs` dispatches on a
closed, hand-written set because the whole chain lives on one thread with no `Send`/object-safety
pressure. The graph model's per-kind dispatch (arity, thread-vs-task, flush-or-not) is a strong
signal to give native transforms the same trait treatment `Input`/`Output` already have — a
`Transform` trait in `logit-pipeline` that `logit-transforms::Aggregator` and future native
transforms implement, letting the node runtime hold `Box<dyn Transform + Send>` next to
`Box<dyn Input + Send>`/`Box<dyn Output + Send>` rather than growing a parallel hand-written enum
per node kind. Lua nodes stay the one hand-special-cased kind, for the `!Send` reason above — a
trait object doesn't fix that, and shouldn't try to.

### Trace context propagation

Every `Delivered` (the channel payload one `Fanout` edge carries, `crates/logit-pipeline/src/fanout.rs`)
carries a `TraceContext { trace_id: [u8; 16], span_id: [u8; 8] }` — the substrate for internal
spans, decided and built per [ADR `trace-context-propagation-on-delivered`](../adr/trace-context-propagation-on-delivered.md) on
the measured evidence [ADR `minimize-allocations-over-event-size`](../adr/minimize-allocations-over-event-size.md) required.
[ADR `internal-span-emission-and-deterministic-sampling`](../adr/internal-span-emission-and-deterministic-sampling.md) is what actually turns
this plumbing into a `SpanRecord`-carrying `Event` — see `docs/design/internal-telemetry.md`'s
"Spans" section for the emit API, the sampler, and the bound.

**Which node kinds propagate a real parent, and which mint a fresh root, is not uniform — it's
exactly the 1-to-1-vs-*n*-to-1 distinction the rest of this doc already draws between a node's
per-batch processing and its flush:**

| Node kind | Context of what it emits | Span recorded |
|---|---|---|
| A listener's own batches | Always a fresh root — `Input::run` never receives a `Delivered` (arity rules out a `sources` entry pointing at one), so there is no parent to inherit. | `SpanKind::Producer`, in `Fanout::send`/`send_blocking` |
| `Transform::process`/`ScriptWorker::process` (the non-flush path) | A [`TraceContext::child`] of the one incoming batch that produced it — 1-to-1, unambiguous. | `SpanKind::Internal`, in `run_transform`/`run_lua` |
| `Transform::flush`/Lua's timer-driven `flush()` | A fresh root, deliberately — an *n*-to-1 relationship (however many batches were absorbed since the last tick), with no single correct parent to propagate. Tracked as an open gap, not silently approximated; see ADR `trace-context-propagation-on-delivered`'s "What this doesn't do." One root now covers *every* resource group a flush emits, not one root per group (ADR `internal-span-emission-and-deterministic-sampling`). | `SpanKind::Internal`, in `run_flush`/`run_lua`'s `flush_now` |
| `run_output` | Already borrows the incoming `Delivered` without unwrapping (`Output::send(&EventBatch)`, [ADR `arc-eventbatch-copy-on-write`](../adr/arc-eventbatch-copy-on-write.md)), so the context is there to read. Nothing further downstream to propagate *to* — the sink span mints `ctx.child()` as its own identity and then discards it (ADR `internal-span-emission-and-deterministic-sampling`). | `SpanKind::Client`, in `write_loop` |

Mechanically: `Fanout::send`/`send_blocking` mint a root, open the listener's own span, and
delegate to `Fanout::send_with_own_context` (new, ADR `internal-span-emission-and-deterministic-sampling`) — the *only* remaining caller of
`send`/`send_blocking`, now that a flush-driven emission (which used to call `send` directly) also
mints its own root and calls `send_with_own_context` instead, so it can record its own span around
the same context. `Fanout::send_with_context`/`send_blocking_with_context` (mint a child of a given
parent, no span of their own) are defined in terms of `send_with_own_context` too — additive
methods, no existing signature changed. `Delivered::context()` is a cheap `&self` accessor — read
it *before* `unwrap_batch` consumes the batch, since `unwrap_batch` itself still discards the
context (changing its return type to include one would force every existing caller, most of which
don't propagate anything, to thread a value through unused). `SinkQueue`'s entries carry the
context too now (`push`/`peek`, ADR `internal-span-emission-and-deterministic-sampling`) — the last place it was still being discarded, on the
one path (`drain_inbox` → `write_loop`) that needs it to parent the sink's own span.

A fan-out (one batch, several downstream branches) gives every branch the *identical* child
context — one emission forking into several consumers is still one hop, not several, and (per ADR
0022) records exactly one span for it, not one per branch.

## Backpressure: diamonds are the normal shape now

With filter components as the only branching mechanism (ADR `component-graph-configuration`), a config where one listener feeds
several filters that reconverge on shared sinks isn't a rare topology — it's the *expected* way to
express "route by condition." Two consequences worth stating rather than discovering in production:

- **Backpressure crosses branches.** A stalled sink backs up through every branch sharing an
  upstream with it, not just its own path — this is correct bounded-channel behavior, but it means
  one slow destination can head-of-line-block telemetry destined for an unrelated, healthy one.
- **Fan-out used to pay a flat clone cost; it now depends on shape.** Originally: every extra
  consumer of a node cloned the outgoing `EventBatch` — a deep `Vec<Event>` clone, incurred
  unconditionally wherever a filter fanned out. A routing primitive would have avoided this by
  construction; having ruled that out (ADR `component-graph-configuration`), the clone was load-bearing, not incidental — and
  it's also what makes branch isolation free: two branches of a fan-out never share the same
  `Event` value, so a mutation on one is structurally invisible to the other, with nothing extra to
  design or maintain for that guarantee (see [ADR `multi-payload-events`](../adr/multi-payload-events.md)'s
  branch-isolation note, proven by `crates/logit-pipeline/src/runtime.rs`'s
  `a_mutation_on_one_fan_out_branch_is_invisible_to_the_sibling_branch` — a test three rounds of
  the fix below never touched, only its doc comment).

  **`Arc<EventBatch>` copy-on-write landed** (`docs/adr/arc-eventbatch-copy-on-write.md`),
  after three rounds of measurement correcting an increasingly specific overclaim each time — worth
  reading end to end for that alone. The settled, shape-dependent result: a single-consumer edge
  (most edges in the shipped config) and an all-`Output` fan-out are both now unconditionally free
  or near-free. A fan-out mixing an `Output` branch with a mutating branch is genuinely racy — 1 or
  6 allocations, decided by real scheduling, never a fixed number. A fan-out with no `Output`
  branch at all still pays the full clone (6, one allocation worse than the pre-`Arc` code), with
  no path to improvement under the current design. "Load-bearing" was right, but there is no single
  number for "the fan-out cost" any more — see `docs/design/memory.md` §3 for the complete,
  shape-by-shape account.

Also worth carrying forward as an open question, not a decision: today's `send_batch` silently drops
a send on a closed downstream (`let _ = tx.blocking_send(...)`). Under a DAG that closure should
really propagate as a shutdown signal rather than vanish. A per-edge `on_full: block | drop` policy
is a plausible future answer; out of scope for the initial graph implementation.

**Sink-side buffering decouples a sink's own inbox from its delivery**
(`docs/adr/buffered-sink-delivery.md`). `run_output` used to await `Output::send` inline, so a
slow or backing-off sink stopped draining its own inbox for as long as delivery took — backpressure
from that sink reached its upstream almost immediately. It now splits into a drain half that moves
batches off the inbox into a `SinkQueue` and a writer half that delivers from that queue
independently (`crates/logit-pipeline/src/queue.rs`), so a slow sink no longer stalls its own
inbox just because delivery is slow. Backpressure doesn't disappear — a `SinkQueue` under `Block`
still applies it once the queue itself fills — it just surfaces later and deeper than the inbox's
`CHANNEL_CAPACITY=64`, and it's now visible ahead of time via
`logit.component.buffer.utilization` rather than only as a stalled inbox.

**Listener-side receive decoupling does the same thing one hop earlier**
(`docs/adr/decoupled-listener-io.md`). A UDP listener's `recv_from`, decode, and
`Fanout::send` used to share one loop, so downstream backpressure stopped the socket being read and
the kernel dropped datagrams silently and uncounted. `logit-inputs::udp::UdpListener` splits into a
read half that moves datagrams off the socket into a `ReceiveQueue` (`BoundedQueue<Datagram>`, the
same generalized type `SinkQueue` is an instance of, `crates/logit-pipeline/src/queue.rs`) and a
decode half that pops, decodes, accumulates, and sends independently. Unlike `SinkQueue`, the
receive queue defaults to `drop_oldest`, not `Block` — a UDP reader's producer is the kernel socket
buffer, which cannot be asked to wait, so blocking here would relocate loss into the kernel instead
of preventing it. See that ADR for the field research behind the default.

## `logit graph`: visualizing the resolved DAG

`logit graph <config>` prints the resolved component graph as graphviz DOT to stdout — the natural
answer to "what does this config actually do," which gets harder to eyeball by reading YAML once
config is a graph rather than a list of linear pipelines.

- Renders unconditionally, straight off the raw `Config` rather than a resolved `Graph` — it needs
  only that a `sources` id can be written as an edge target, which is true even when that id names
  no defined component: graphviz auto-creates a bare node for an edge whose target was never
  otherwise declared, so an unresolved source (rule 2) still renders as a visibly dangling edge
  rather than blocking output. (An earlier version of this section reasoned "an edge to an
  undefined component can't be drawn at all" and required rule 2 to pass first — that premise was
  simply wrong once actually tried; corrected here rather than left as a stated constraint the
  implementation quietly didn't follow.)
- Runs the full validation (all eighteen rules) after rendering and reports any failures to stderr with a
  non-zero exit — without suppressing the DOT output. This is deliberate: `graph` is most useful on
  exactly the configs that fail validation, since a cycle — or a typo'd source, now visibly
  dangling — is far easier to see rendered than to parse out of an error message naming two
  component ids.
- Styles nodes by role (listener / transform / sink) so the shape of the data flow — where it
  enters, where it forks, where it lands — reads at a glance without cross-referencing the arity
  table.
- Still needs every `!env` reference in the config to resolve, though ("Environment substitution"
  above) — a missing variable fails to load before `render` is ever called, same as `run`/
  `validate`, even for a field this command never reads.

Lives in `logit-cli` as a `Command::Graph` arm alongside `Schema`/`Validate`/`Run`
(`crates/logit-cli/src/main.rs`); stays synchronous like `Schema`/`Validate` — it only needs the
resolved graph structure, no I/O, no tokio runtime.

## Crate layout

The obvious arrangement is circular: a pipeline runtime needs to build inputs/outputs/transforms,
but `Input::run`/`Output::send`/a `Transform` implementation need the `Fanout` type the runtime
defines. Invert it — trait definitions and the runtime move into a new crate that the impl crates
don't depend on:

```
logit-core   logit-config   logit-script
        \         |         /
          logit-pipeline          — Input/Output/Transform traits, Fanout, graph, node runtime
           /            |            \
   logit-inputs   logit-transforms   logit-outputs   — impls only
           \            |            /
                    logit-cli                        — CLI + the kind → impl registry
```

`logit-pipeline`:
- Moves `Input` and `Output` out of `logit-inputs`/`logit-outputs` (which then hold only impls), and
  adds the `Transform` trait discussed above.
- Owns `Fanout` and the graph resolution/validation module (pure, no channels or threads — see
  below) and the node runtime.
- Depends on `logit-core` (for `EventBatch`), `logit-config` (for `ComponentKind`), and `logit-proto`
  (for `Buffer`/`InMemoryBuffer`, which `SinkQueue` wraps — `docs/adr/buffered-sink-delivery.md`),
  but *not* on `logit-inputs`/`logit-transforms`/`logit-outputs` — those depend on it instead, for
  the trait definitions. `logit-cli` is the one place that depends on everything and holds the
  kind-to-trait-object registry (today's `build_input`/`build_output`, generalized).
- Keeps the channel type out of `logit-core`, whose doc comment states "no I/O, no pipeline" —
  weakening that would blur a boundary the crate exists to hold.
- Owns `BoundedQueue<T: Queued>` and `BatchAccumulator` (`docs/adr/decoupled-listener-io.md`)
  alongside `Fanout` and the node runtime — both are transport-agnostic (nothing in either type
  mentions a socket or a decoder). The UDP socket bind, `SO_RCVBUF` setsockopt, and `recv_from` loop
  that *uses* them (`logit-inputs::udp::UdpListener`) stay in `logit-inputs`, following the same
  "traits and generic machinery here, concrete protocol impls there" split the crate already
  applies everywhere else.

`graph.rs` (resolution + the eighteen validation rules + topo-sort) is a **pure function over
`Config`** — no channels, no threads, no tokio — mirroring how `apply_transforms` in today's
`pipeline.rs` was deliberately kept pure specifically so it's unit-testable without spinning up
real I/O. `logit run`, `logit validate`, and `logit graph` are three different things layered on
top of the same pure resolution: run executes it, validate checks it and stops, graph renders it.

## `flush()` needs no new design

Restating for confirmation, not introducing anything: a stateful component (`aggregate`, or a Lua
component with `interval` set) is a node whose loop races its inbox receive against its own flush
deadline (previous section), emitting into its `Fanout` on that schedule independent of inbound
traffic. `docs/design/lua-api.md`'s `flush()` contract and
`docs/adr/aggregation-window-semantics.md`'s windowing semantics both apply unchanged — the
graph model changes *where* the flush timer lives (per-node instead of per-pipeline-chain), not what
it does.
