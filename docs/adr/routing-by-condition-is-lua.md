---
created: 2026-09-07
updated: 2026-09-10
---

# Routing by condition, sampling, throttling, dedup, and renaming are `lua` components

## Status
Accepted

## Context

`crates/logit-config/src/lib.rs`'s `ComponentKind` carried nine unimplemented native-transform
variants, carried forward purely so a config referencing one got a clear "not implemented yet" at
`logit validate`/`logit run` rather than a deserialization error: `logfmt`, `kv`, `regex`, `csv`,
`rename`, `filter`, `sample`, `throttle`, `dedup`. All nine are also published in
`schema/logit.schema.json`, since the schema is generated directly from `ComponentKind`
(ADR `config-yaml-jsonschema`) with no way for an unimplemented variant to opt out — so a
schema-aware editor autocompletes `filter:`, and the binary then rejects the resulting config.
`docs/known-gaps.md` did not record this; nothing else in the repo did either.

The obvious next step was to implement them, starting with `filter`. `docs/design/pipeline-graph.md`
calls filter components "the only branching mechanism" and "the *expected* way to express 'route by
condition'" — the natural place to start.

Designing `filter`'s `where:` expression got far enough to cost three real options — a native
predicate grammar, a Lua expression, an off-the-shelf evaluator (CEL, `evalexpr`) — before the
framing itself was checked against what already runs. It didn't hold: `demo/logit.yaml`'s
`nginx_stdout` is a five-line `lua` component doing exactly a filter's job today —

```yaml
nginx_stdout:
  type: lua
  sources: [nginx_in]
  script: |
    function process(event)
      if event.attributes["log.iostream"] ~= "stdout" then
        return nil
      end
      return event
    end
```

— and it works, in production, in this repo's own demo stack. Routing by condition is not a
missing capability. A native `filter` component would buy an ergonomic (one line of config instead
of five), not new function. The same is true of `rename` (`event.attributes.new = event.attributes.old;
event.attributes.old = nil`), `sample` (`math.random() < rate`), `throttle` (a counter in a Lua
upvalue, reset in `flush()`), and `dedup` (a seen-set in a Lua upvalue) — each holds state across
events in ordinary Lua globals, and a `lua`/`lua_file` component already has a `flush()` hook
(`docs/design/lua-api.md`) for anything that needs to act on a timer rather than per event.

`logfmt`, `kv`, `csv`, and `regex` are a different kind of gap, and this ADR does not retire them:
LuaJIT ships Lua patterns, not a general parser or a real regex engine, so hand-writing a `k=v` or
CSV parser per event in a script is both slow and not something users should have to write. Those
four remain unimplemented `ComponentKind` variants, tracked as ordinary future work.

## Decision

**`logit` ships no native predicate language.** Routing by condition, per-event sampling,
throttling, deduplication, and attribute renaming are `lua`/`lua_file` components; there is no
`filter`, `rename`, `sample`, `throttle`, or `dedup` `ComponentKind`. The five variants are removed
from `crates/logit-config/src/lib.rs`, `crates/logit-pipeline/src/graph.rs`'s `role`/`kind_name`,
and the generated schema — not left as unimplemented placeholders, because leaving them would keep
advertising a component this decision says isn't worth building yet.

**The revisit trigger is explicit and measured, not a hunch: sustained central-collector
throughput pressure.** The cost of the Lua route, measured (`docs/design/memory.md`):

| | Lua node | Native `Transform` node |
|---|---|---|
| Allocations/event | **9** (`memory.md`'s `run_lua: set_resource + process + take_resource` row) | **1** (`memory.md`'s `process_batch` through `keep` row — `process_batch`'s own `Vec`) |
| Throughput | **1.07 µs/event** (`memory.md`'s `lua (proxy)` bench) | **360 ns/event** (`memory.md`'s `process_batch through keep` bench) |
| Concurrency | one dedicated OS thread **and** one LuaJIT VM *per node* — `crates/logit-pipeline/src/runtime.rs`'s `run_with_telemetry` spawns each Lua component via `std::thread::Builder::new().name(format!("logit-{id}")).spawn(move \|\| run_lua(...))` | an ordinary tokio task in the shared runtime (`crates/logit-pipeline/src/transform.rs`'s own module doc: a native transform "runs as an ordinary tokio task in the node runtime, unlike a Lua component, which needs its own OS thread") |

**The honest scale reading, so the trigger has teeth rather than being decorative:** at
sidecar/host-agent volume — thousands of events/sec, the deployment shape this project expects for
most users (`docs/OVERVIEW.md`) — the ~0.7 µs delta per event per filter is roughly 0.5% of one
core, and a handful of extra OS threads is noise. It becomes real specifically in the
central-aggregator role: at roughly 500k events/sec, a three-way routing diamond spends on the
order of a full core answering boolean questions that a native transform would answer for a third
of that. Per-node throughput is single-threaded either way in both routes — only the constant
differs, by roughly 3×. If and when that pressure is actually measured against a real config (not
assumed), this decision is the one to revisit, and the alternative below is where to resume.

## Alternatives considered

- **A native predicate grammar, in a new `logit_core::predicate` module.** Rejected as premature —
  not because it wouldn't work; the design got far enough to be worth preserving so a future
  attempt is a reading exercise, not a redesign:
  - An EBNF: `or`/`and`/`not` over comparisons and three functions (`exists`/`contains`/
    `starts_with`/`ends_with`), against explicitly namespaced paths (`attr.`, `resource.`, `log.`,
    `event.` — no bare identifiers, so there's no ambiguity about what's being addressed).
  - A dotted bare key addresses **one** attribute literally, never a traversal — `attr.log.iostream`
    is the attribute named `log.iostream`, matching ADR `kv-metrics-semantics`' "nested fields are
    not addressable" rule rather than inventing a second convention.
  - **Absent is `false` under every comparison, including `!=`** — every leaf is a positive
    assertion about data that's actually there, and `not(...)` is how the complement is spelled.
    This was going to be the single most surprising rule in the design and would have needed its
    own paragraph everywhere it's mentioned.
  - Numeric coercion so `attr.status >= 500` matches `Value::Str("500")`, matching ADR
    `kv-metrics-semantics`' identity commitment; `log.severity` compared by `Severity`'s `Ord`
    rather than string order, so `>= "warn"` means warn/error/fatal correctly.
  - The property that made it worth considering at all: **totality**. Define every comparison so
    it cannot fail at runtime — a type mismatch or missing key is `false`, not an error — and the
    fail-open/fail-closed question a filter otherwise raises dissolves rather than needing an
    answer. Any future native predicate work should keep this as a binding constraint on itself:
    an operation that can't be made total doesn't belong in `where:`.
  - **Forced placement, worth recording because it's non-obvious**: such a parser cannot live in
    `logit-transforms` (where the transform itself would live) or in `logit-config`.
    `crates/logit-cli/src/pipeline.rs`'s `validate_semantics` is literally `graph::resolve(config)?`
    — so anything not checked inside `graph::resolve` escapes `logit validate` and breaks
    `docs/deploying.md`'s preflight promise that a config validating cleanly won't fail at `run`.
    But `logit-transforms` depends on `logit-pipeline` depends on `logit-config`, so the only crate
    both `graph::resolve` (in `logit-pipeline`) and the transform (in `logit-transforms`) can share
    without a dependency cycle is `logit-core`. Note also that `logit-core`'s "shape only" module
    doc is already aspirational — it ships `time`'s RFC 3339 parser, `trace`'s traceparent parser,
    and the whole telemetry `Registry` — so a predicate module would be within its existing reality,
    not a new kind of thing for it to hold.
  - Rejected regardless of the above being sound, because the premise was wrong: routing already
    works.
- **A Lua expression `where:`, desugared to a generated `function process(event) if <expr> then
  return event end return nil end`.** Roughly 50 lines instead of 600, no new grammar, and
  identical runtime cost to hand-writing the equivalent Lua directly — because it *is* Lua. Rejected
  as a second way to spell the same thing config already expresses today, which ADR
  `component-graph-configuration` is on record disliking ("Adding a second branching mechanism on
  top would be two ways to do the same thing," said there about a routing primitive specifically,
  and equally true of sugar over an existing one). If routing already works via `lua`, sugar for it
  is not worth a component kind.
- **An off-the-shelf expression evaluator (CEL via `cel-interpreter`, or `evalexpr`).** Both are
  MIT/Apache-2.0 and pure Rust, so `deny.toml`'s license allowlist and ADR
  `containerized-development`'s no-C-toolchain constraint both hold — licensing was never the
  blocker. The blocker: both build an owned evaluation context per call (a `HashMap`-shaped bridge
  from `logit_core::Value` into the crate's own value model), which is a per-event allocation this
  project's own convention forbids (AGENTS.md; `docs/design/memory.md`'s allocation-count
  assertions). Both also reintroduce genuine runtime errors, undoing the one property (totality)
  that made a native grammar worth considering over Lua in the first place.
- **Keep the five variants as unimplemented placeholders, unchanged.** The status quo this ADR
  replaces. Rejected: it's the thing that created the problem in the first place — a schema that
  advertises a component the binary will never build unless a config author reads
  `docs/known-gaps.md` first.

## Consequences

- `docs/design/pipeline-graph.md`'s "filter components are the only branching mechanism" becomes,
  in practice, "a chain of `lua` components is the branching mechanism" — reworded in that doc to
  match. The generic use of "filter" as an architectural term in ADR `component-graph-configuration`
  itself (written before `ComponentKind::Filter` ever existed) is left as historical record,
  unchanged.
- A config using `type: filter` (or `rename`/`sample`/`throttle`/`dedup`) now fails at
  **deserialization** — `unknown variant 'filter', expected one of ...` — rather than at
  `graph::resolve`'s "kind not implemented yet." The message names valid alternatives, which is an
  improvement; the cost is that a deserialization error loses line/column once `!env` is in the
  config's picture (`docs/known-gaps.md`'s existing entry on that), a minor regression in error
  quality accepted in exchange for the schema no longer lying about what exists.
- `docs/known-gaps.md` gains two entries: the schema-advertised-more-than-the-binary-runs problem
  (narrowed, not closed — `logfmt`/`kv`/`csv`/`regex` and `logit_in`/`logit_out` remain
  unimplemented and still published), and the measured cost table above, so the revisit trigger has
  a fixed place to live rather than only this ADR.
- `demo/logit.yaml`'s `nginx_stdout` is unchanged and now cites this ADR directly, so the choice
  reads as decided rather than defaulted.
- `crates/logit-config/src/lib.rs`'s `Internal::span_sample_rate` doc comment, which justified its
  own name partly by pointing at `ComponentKind::Sample` ("there is already a
  `ComponentKind::Sample` transform"), needed rewording since that variant no longer exists.
- **The revisit trigger named above fired, for one specific case, on 2026-09-10.** Fan-out after
  `logit_in` (N branches merged into one `logit_out` connection, split back apart on the far side)
  is exactly the central-collector shape this ADR's cost table is about, multiplied by branch
  count. The response was not the retired predicate grammar -- `has_attributes`/`drop_attributes`
  (`docs/adr/attribute-filtering-components.md`) are a bounded key/value equality matcher, not a
  parser, and their config is deliberately no wider than `set`'s. This ADR's core holding is
  unchanged and this is not its supersession: `logit` still ships no native predicate language, no
  `filter`/`where`, no operators, no boolean algebra beyond a plain conjunction. Anything needing an
  actual operator (`>=`, `contains`, cross-attribute comparison) still means writing `lua`, and the
  preserved grammar sketch above is still where that design would resume.
- **The same trigger fired a second time, also on 2026-09-10, for batch provenance.**
  `has_provenance`/`drop_provenance` (`docs/adr/provenance-filtering-components.md`) filter on a
  batch's `origin`/`previous` -- component ids drawn from a graph's own `components:` map, a
  narrower, already-closed set than the attribute values `has_attributes` had to defend matching
  against. Same posture as above: no operators, no boolean algebra beyond the AND-across-fields/
  OR-within-a-field this pair adds (itself `has_attributes`' and `has_signal`'s shapes composed,
  not a new primitive), and this ADR's core holding is still unchanged.
