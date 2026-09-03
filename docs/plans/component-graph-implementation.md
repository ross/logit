---
created: 2026-08-29
updated: 2026-08-29
---

# Implementation plan: component graph pipeline

Staged execution plan for [ADR `component-graph-configuration`](../adr/component-graph-configuration.md) and
[docs/design/pipeline-graph.md](../design/pipeline-graph.md). Each stage is independently
reviewable (its own PR); later stages depend on earlier ones landing first. Read the design doc in
full before starting — this plan sequences the work, it doesn't re-derive the design.

## 1. `logit-config`: the component types

Replace `Config { inputs, outputs, pipelines }` with `Config { components }`, `Component`, and
`ComponentKind` per the design doc's grammar. Concretely:

- Delete `InputConfig`, `OutputConfig`, `PipelineConfig`; fold their variants into one
  `ComponentKind` with the `_in`/`_out` suffix renames (`Statsd` → `StatsdIn`, `InfluxDb` →
  `InfluxDbOut`, etc.).
- `TransformConfig`/`BuiltinTransformConfig` collapse into `ComponentKind` variants too — `Lua`,
  `LuaFile`, `Aggregate` first (the three real ones today), the rest of
  `BuiltinTransformConfig`'s dormant variants (`Json`, `Logfmt`, `Kv`, `Regex`, `Csv`, `Rename`,
  `Remove`, `Filter`, `Sample`, `Throttle`, `Dedup`) carried over unimplemented, same as today.
- `Component { sources: Vec<String>, #[serde(flatten)] kind: ComponentKind }`.
- **First thing to verify, before writing the rest**: generate the schema for a `#[serde(flatten)]`
  struct wrapping a `#[serde(tag = "type")]` enum with `schemars` 0.8 and confirm it's sane and that
  `serde_norway` round-trips a real config through it. If `flatten` produces something unusable,
  fall back to repeating `sources: Vec<String>` on every `ComponentKind` variant (design doc's
  documented fallback) before writing any more code against the flattened shape.
- `non_empty_pipelines_schema`'s `min_properties = 1` schema hook moves to `components`.
- Run `script/schema` and commit the regenerated `schema/logit.schema.json` (required by `AGENTS.md`
  after any `logit-config` change).
- Port `logit-config`'s existing tests (interval deserialization, min-properties) onto the new
  types; they should need no behavioral changes, just new type names.

## 2. `logit-pipeline`: new crate, traits and `Fanout`

Add to the workspace (`Cargo.toml` members and `[workspace.dependencies]`). Depends on
`logit-core`, `logit-config`; nothing else in the crate graph depends on it circularly (design
doc's "Crate layout" section).

- Move `Input` (from `logit-inputs`) and `Output` (from `logit-outputs`) here unchanged in
  signature, except `Input::run` takes `Fanout` instead of `mpsc::Sender<EventBatch>`.
- Add `Fanout`: a small wrapper around `Vec<mpsc::Sender<EventBatch>>` with a `send`/`send_owned`
  method implementing the clone-all-but-last pattern `send_batch` uses today
  (`crates/logit-cli/src/pipeline.rs`) — one copy, used everywhere fan-out happens instead of
  hand-rolled per call site.
- Add the `Transform` trait discussed in the design doc's "Node kinds" section, for native `Send`
  transforms. Don't implement it yet — that's stage 4, once `logit-transforms::Aggregator` is the
  first real implementer.

## 3. `graph.rs`: pure resolution and validation

Lives in `logit-pipeline`. No channels, no threads, no tokio — a pure function over `Config`,
mirroring how `apply_transforms` was kept pure today specifically for unit-testability.

- Build the inverted-edges map (`sources` → outbound consumer lists).
- Implement the nine validation rules from the design doc in order, each with its own clear error
  message naming the offending component id(s).
- Topological sort (reverse order for build sequencing).
- This module is what `logit run`, `logit validate`, and `logit graph` all sit on top of — write it
  and its tests before touching any of the three CLI commands.

**Test list** (port relevant cases from today's `validate_semantics` tests, add the new ones):
unknown source reference, self-reference, duplicate source within one component's `sources` list
(would otherwise double-deliver every batch from that source, not just a redundant edge), two-node
cycle, longer cycle, listener declaring sources, sink named as another component's source,
transform/listener with no consumers, sink shared by two branches (must now be *accepted* —
today's code rejects this; confirming it's accepted is the headline regression test for this whole
change), diamond fan-out/fan-in resolves correctly, unimplemented-kind rejection, zero-interval
rejection on each kind that has `interval`.

## 4. Node runtime

In `logit-pipeline`. Per-kind node loops (design doc's "Runtime model"):

- Listener nodes, sink nodes, and `Transform`-trait nodes run as ordinary tokio tasks.
- Lua nodes get a dedicated `std::thread` each, one `ScriptWorker`, its own single-entry flush
  schedule if `interval` is set — reuse `advance_flush_deadline`'s constant-time logic from today's
  `pipeline.rs` verbatim (it's already correct, just was iterated over a `Vec` that no longer needs
  to exist).
- `logit-transforms::Aggregator` becomes the first `Transform` trait implementer (stage 2's trait);
  runs as a tokio task with the same per-node flush schedule as a Lua node with `interval` set —
  same timer logic, different thread model.
- Build in reverse topological order (stage 3's sort); wire each node's `Fanout` from its
  consumers' already-created inboxes.

## 5. `logit-cli`: reduce `pipeline.rs` to the registry + command wiring

- `build_component(kind: &ComponentKind) -> Box<dyn Input/Transform/Output + Send>` (three small
  functions, one per role) replaces today's `build_input`/`build_output` and the `Stage`/
  `TransformSpec` machinery.
- `run_pipelines`/`run_config` become: parse config → `graph::resolve` (stage 3) → build every
  component via the registry → hand to the stage-4 runtime.
- `logit validate` calls `graph::resolve` and stops; no behavior change in spirit, new types
  underneath.
- Port the existing integration-style tests (`pipeline.rs`'s `#[cfg(test)] mod tests`) onto the new
  shape — most translate directly (one `Config` with `components` instead of three maps).

## 6. `logit graph` subcommand

- `Command::Graph { path }` in `crates/logit-cli/src/main.rs`, alongside `Schema`/`Validate`/`Run`;
  stays synchronous, no tokio runtime built.
- DOT emitter over the raw `Config`, not a resolved `Graph`: one node per component styled by role
  (listener/transform/sink), one edge per `sources` entry — graphviz auto-creates a bare node for
  an edge whose target isn't otherwise declared, so an unresolved source still renders as a
  visibly dangling edge instead of blocking output.
- Runs full validation (rules 2–9) after emitting DOT; reports failures to stderr, exits non-zero,
  **does not suppress the DOT output** — this is the point of the command on a broken config.
- Tests: DOT output for a diamond-shaped config (one listener, two filters, shared sink) is
  well-formed and includes every expected node/edge; a cyclic config still emits complete DOT while
  the process exits non-zero.
- Document the command in `README.md`'s command table alongside `schema`/`validate`/`run`.

## 7. Rewrite the example and docs that describe running code

- `examples/statsd-to-influxdb.yaml` → the component shape (statsd_in → aggregate → lua → 
  influxdb_out, matching today's actual pipeline). Verify `script/server` still runs it end to end
  against the compose stack — this is the only environment-dependent verification step in the whole
  plan.
- `AGENTS.md`: update the "Current state" paragraph (still describes real code, now the graph
  runtime) and the "Where things live" crate table (add `logit-pipeline`; note `logit-inputs`/
  `logit-transforms`/`logit-outputs` hold impls only). The "three design docs are load-bearing"
  sentence already grew to include `pipeline-graph.md` as part of the docs-only change that
  preceded this implementation — no further edit needed there.
- `README.md`: command table gets `graph`; any other `inputs:`/`outputs:`/`pipelines:` YAML
  fragments shown there move to the component shape.

## Verification

- `script/cibuild` clean at the end of every stage — don't let two stages' worth of breakage stack
  up before running it.
- Stage 3's `graph.rs` tests are the load-bearing regression suite for the whole rework; they should
  be written and passing before stage 4 exists to consume them.
- End-to-end: `script/server` against the rewritten example config (stage 7), confirming statsd →
  aggregate → Lua → InfluxDB still works, and `logit graph examples/statsd-to-influxdb.yaml` prints
  a sensible four-node DOT chain.
