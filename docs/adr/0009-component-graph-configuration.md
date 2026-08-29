# 0009 — Configuration: a component graph, not inputs/outputs/pipelines

## Status
Accepted

## Context
Config today has three top-level maps — `inputs`, `outputs`, `pipelines` — where a `PipelineConfig`
(`crates/logit-config/src/lib.rs`) names an ordered `inputs: [..]` list, an ordered
`transforms: [..]` chain, and an ordered `outputs: [..]` list. `logit-cli::pipeline::validate_semantics`
has to reject an input or output named by more than one pipeline (`crates/logit-cli/src/pipeline.rs`),
because the runtime has no way to express sharing one: two pipelines both wanting `statsd_in` would
mean two UDP listeners racing for the same port, or two independent copies of one input's state,
neither an answer anyone wants. That restriction rules out a legitimate, common shape — one listener
feeding two independently-filtered downstream chains — not because it's unsafe, but because "pipeline"
is the only unit of composition config has.

The deeper issue is that an input, a transform, and an output are all the same kind of thing: something
with an id that reads events from somewhere and, except for a sink, produces events for something else
to read. Splitting them into three typed maps (`InputConfig`, `OutputConfig`, and an untagged
`TransformConfig`) forces that sameness to be re-expressed three times, and a pipeline is a straight
line through them — reusing one stage's output as input to two different downstream chains means
duplicating the stage.

## Decision
One flat `components:` map replaces `inputs`/`outputs`/`pipelines`. Every component has an id (the
map key), a `type:` naming its kind, and a `sources:` list of other components' ids it reads from.
There is no separate `pipelines` section — a "pipeline" is simply the subgraph reachable from a
listener, and nothing in config needs to name it as a unit.

A component's kind fixes its arity — how many sources it may have, and whether it may itself be a
source:

| Kind class | `sources` | May be another component's source |
|---|---|---|
| Listener (`statsd_in`, `syslog_in`, `otlp_in`, `file_tail`, `logit_in`) | must be empty | required (≥1 consumer) |
| Transform (`lua`, `lua_file`, `aggregate`, `json`, `filter`, …) | ≥1 required | required (≥1 consumer) |
| Sink (`influxdb_out`, `otlp_out`, `logit_out`) | ≥1 required | must not be |

Wiring resolves to a static DAG of bounded `tokio::mpsc` channels, built once at startup: invert
`sources` to get each component's outbound edges, reject cycles, and spawn in reverse topological
order so every node's downstream senders exist before it can produce. Full design, validation rules,
runtime model, and the `logit graph` subcommand: [docs/design/pipeline-graph.md](../design/pipeline-graph.md).

## Alternatives considered
- **An in-process pub-sub broker.** The original framing of this problem ("components subscribe to
  sources") suggested one. Rejected: the topology is fully known at config-load time — nothing
  subscribes or unsubscribes at runtime — so a broker would be solving a dynamic problem that isn't
  actually dynamic here, while adding a real one: dispatch indirection, and no natural backpressure
  story (see the next point).
- **`tokio::sync::broadcast` for fan-out.** The most literal pub-sub primitive available. Rejected:
  it drops messages for a receiver that falls behind rather than exerting backpressure — silent
  telemetry loss — and still clones per receiver, so it buys nothing over per-consumer `mpsc` while
  losing the guarantee that a slow output actually backs up its producers.
- **Deriving a component's role (listener/transform/sink) from topology** — "no sources = listener,
  nothing points at it = sink" — instead of from its kind. Rejected: it makes a typo silently
  reclassify a component (a misspelled downstream reference turns a real sink into an orphan
  transform with no error) rather than producing a clear message. The kind already knows its own
  arity; the config should say so.
- **Named outlets or edge predicates** for conditional routing ("send errors here, everything else
  there"). Considered and explicitly rejected, not deferred: that's what a chain of filter
  components already gives you — one component drops errors, a sibling drops everything else,
  downstream components choose which to read from by naming it as a source. Adding a second
  branching mechanism on top would be two ways to do the same thing.
- **Keep the three typed maps, only add `sources` to break the one-pipeline-per-input/output
  restriction.** Keeps `InputConfig`/`OutputConfig`/`TransformConfig` as-is, minimal schema churn.
  Rejected: it doesn't fix the deeper problem (three vocabularies for one idea, `TransformConfig`'s
  `#[serde(untagged)]` producing weak schema/error output), and "pipeline" would still be a unit of
  composition config has to reason about for no remaining reason — every actual constraint it
  enforced now lives in the graph's arity/cycle rules instead.

## Consequences
- The "input/output claimed by more than one pipeline" restriction disappears — sharing one listener
  across independently-filtered downstream chains, or feeding two sinks from one enrichment stage, is
  now just a config with more than one entry in some component's consumer set.
- Cycle detection becomes a mandatory validation rule, not a nice-to-have: a cycle plus bounded
  channels is a deadlock, not a slow pipeline.
- More OS threads in the general case: today one thread runs a whole pipeline's transform chain
  serially; the graph model gives each Lua component (`!Send`, per `docs/design/lua-api.md`'s
  concurrency section) its own thread, since chain adjacency is no longer guaranteed by config
  shape. Native `Send` transforms (`aggregate`, and future `logit-transforms` additions) run as
  ordinary tokio tasks and pay no such cost.
- Fan-out — one component feeding several consumers — costs a full `EventBatch` clone per extra
  consumer, same as today's per-output clone in `send_batch`. Filter-based branching (the only
  branching mechanism, per the alternatives above) makes this the normal case rather than an edge
  case; `docs/design/pipeline-graph.md` records it as a known cost with `Arc<EventBatch>` +
  copy-on-write as the identified future optimization, not a decision to make now.
- `TransformConfig`'s `#[serde(untagged)]` goes away — every component kind is tagged by `type`,
  which was already true for `InputConfig`/`OutputConfig` and is a strictly better schema/error
  experience for the one config type that lacked it.
- This is a breaking config-format change with no migration path — acceptable pre-release, per the
  project's current stage; there is exactly one shipped example config and no external users of the
  format yet.
