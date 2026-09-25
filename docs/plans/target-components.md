---
created: 2026-09-13
updated: 2026-09-13
---

# Enabling plan: `target` components and routers

## Context

[ADR `target-components`](../adr/target-components.md) decides the shape: a `target` is a no-field
component kind that is a named destination, fed by *direction* from a router and consumed through
ordinary `sources:`; two router kinds, a native equality-only `route` and `lua`/`lua_file` with a
`targets:` list and `event:to("id")`; unrouted events go to the router's own consumers or are
counted dropped; at runtime a target is a zero-cost alias -- one `Fanout` per target, owned by its
routers, stamped with the target's id and carrying the target's telemetry handle. This plan is the
build-out: what lands in which order, in which files, and how each piece is verified. Read the ADR
first -- this document doesn't repeat its reasoning, only its consequences.

The motivating topology is `fixtures/fan-out-central.yaml`: one `logit_in` splitting N tagged
streams back apart, today an N-way fan-out (a full `EventBatch` clone per extra branch,
`docs/design/memory.md` §3) plus N `has_attributes`/`drop_attributes` passes over every event.

## Workstreams

Stacked branches `feat/targets-w<N>`, one PR each, each PR based on its parent workstream's
branch, following the mechanics established for the `collectd`/`lossless-transit`/`load-test-harness`
efforts. **This stack is reviewed as a whole before any of it merges** -- it reopens a decision
`component-graph-configuration` made explicitly, so the full shape has to be on the table first.

| # | Scope | Files | Done when |
|---|---|---|---|
| W0 | **Docs.** The ADR and this plan; dated amendments to the two ADRs this reopens. | `docs/adr/target-components.md` (+ row atop `docs/adr/README.md`); `docs/plans/target-components.md` (+ row atop `docs/plans/README.md`); `docs/adr/component-graph-configuration.md` and `docs/adr/routing-by-condition-is-lua.md` (a dated bullet appended to Consequences, `updated:` bumped) | Both docs follow `TEMPLATE.md`'s headings, every relative link resolves, both README indexes gain a row. Docs only -- no code, no `script/cibuild`. |
| W1 | **Config types, roles, schema.** `ComponentKind::Target {}`, `ComponentKind::Route { by: RouteBy, routes: BTreeMap<String, String> }`, `RouteBy { Provenance(ProvenanceField), Attribute(String), Resource(String) }` (externally tagged, snake_case), `ProvenanceField { Origin, Previous }`, `Component.targets: Vec<String>`; `Role::Target` + `role()`/`kind_name()` arms (`Route` is a transform by arity); `is_implemented` **unchanged** so both kinds are rejected by rule 8 until W4; two temporary `bail!` arms in `build_spec`. | `crates/logit-config/src/lib.rs`; `crates/logit-pipeline/src/graph.rs`; `crates/logit-cli/src/pipeline.rs`; `schema/logit.schema.json` (regenerated) | `script/cibuild` green; `script/schema` produces no diff after commit; deserialization tests cover `target`, every `by:` form, and `targets`' default; a graph test proves both kinds fail rule 8 with the "not implemented" message. |
| W2 | **Graph: target edges, cycles, validation, DOT.** `graph::targets_of`/`target_edges` (pure, public); `ResolvedComponent.targets` in slot order; `topological_order` over `sources` *plus* router → target edges (indegree from an `incoming` map, the cycle-recovery walk over the same map); rules 47-51; rule 6's arity row for `Target`; rule 7 relaxed for routers; `dot.rs` target style and dashed, labelled router → target edges. | `crates/logit-pipeline/src/graph.rs`; `crates/logit-cli/src/dot.rs`; `docs/design/pipeline-graph.md` (roles table, rules 47-51, DOT bullet) | `script/cibuild` green; one `expect_err` test per rule clause; `router -> target -> transform -> router` is reported as one concrete cycle; a target sorts after its router; a many-to-one `routes:` collapses to one slot; two `dot.rs` tests (target style, labelled dashed edge). |
| W3 | **Runtime seam.** `logit_pipeline::router::{Router, Destination}`; `NodeSpec::{Router, Target}` and `targets` on `NodeSpec::Lua`; no channel for a target id; a pre-spawn pass building one `Fanout` per target (`with_component(target)`, `with_telemetry(target's handle)`, `NodeState::Alias`); the `Router` spawn arm cloning slot-ordered target `Fanout`s; `drop(target_fanouts)` beside `drop(senders)`; `run_router` (one child ctx, one span, one send per non-empty destination) and `pub fn route_batch` (route-borrow, count, `reserve_exact`, move, `mem::take`); `events.dropped{reason="unrouted"}` when there are no ordinary consumers. | `crates/logit-pipeline/src/router.rs` (new); `crates/logit-pipeline/src/runtime.rs`; `crates/logit-pipeline/src/readiness.rs`; `crates/logit-pipeline/src/lib.rs` (re-exports); `docs/deploying.md` (the `alias` node state) | `script/cibuild` green; runtime tests with an inline test `Router`: a two-target split lands the right events on the right consumers; unmatched events reach the ordinary consumers, or are counted `unrouted` when there are none; `previous` downstream of a target is the target's id and `origin` is untouched; one span per incoming batch regardless of destination count; a router exiting closes its targets' consumers' inboxes (the shutdown-cascade pin); two routers into one target both deliver. |
| W4 | **Native `route`; both kinds become implemented.** `logit_transforms::Route` on `provenance.rs`'s cached-`observe_provenance` pattern: `By::{Origin, Previous}(Vec<(Symbol, u16)>)` and `By::{Attribute, Resource}(Symbol, Vec<(SetValue, u16)>)`, names interned and slots resolved once in `Route::new`, attribute/resource equality through the same coercing comparator `has_attributes` uses; `build_spec` arms replacing W1's stubs; `is_implemented` flips `Target` and `Route` together. | `crates/logit-transforms/src/route.rs` (new) + `lib.rs`; `crates/logit-cli/src/pipeline.rs`; `crates/logit-pipeline/src/graph.rs` (`is_implemented`); `crates/logit-bench/tests/allocations.rs` (new `// Routing` section *after* `// Fan-out`, no existing constant moved); `docs/design/memory.md` §3 (same commit as the constants); `crates/logit-cli/tests/route_round_trip.rs` (new); `docs/design/internal-telemetry.md` (`unrouted`; a target's layer-2 set and absent receive side) | `script/cibuild` green; per-`by:` unit tests, absent key forwards, many-to-one, `status: 500` matching `"500"`; allocation tests pin `1 + used destinations` per batch and the today-equivalent fan-out + two `has_attributes` for comparison; the round-trip test drives a real `logit_out -> logit_in -> route{by: {provenance: origin}} -> two targets` and asserts placement and `previous` = target id. |
| W5 | **Lua routers.** `EventProxy` gains the mark (`Cell<Option<u16>>`) and a shared name → slot table; `event:to(id)`/`to(nil)`, unknown id → `RuntimeError` naming the id and the configured list, chaining return, `clone` copies the mark; `ScriptWorker::with_targets`; `ProcessOutcome::{Emit(Box<Event>, Option<u16>), EmitMany(Vec<(Event, Option<u16>)>), Drop}` and `flush() -> Vec<(Event, Option<u16>)>`; `run_lua` takes `targets`/target `Fanout`s, pushes straight into per-destination buffers, one send per non-empty destination for both the batch path and `flush_now`. | `crates/logit-script/src/proxy.rs`, `lib.rs`; `crates/logit-pipeline/src/runtime.rs`; `crates/logit-cli/src/pipeline.rs`; `docs/design/lua-api.md` ("Routing to a target" after "Script contract"; `targets:` in "Config shape") | `script/cibuild` green; script tests for mark/clear/unknown/clone/independent marks in a `{a, b}` return/a routing `flush()`; runtime tests for a Lua two-way split, an unmarked event reaching the ordinary consumers, and an unknown target counting a script error without killing the node. |
| W6 | **Examples and closing docs.** `fixtures/fan-out-central.yaml` onto `route` + targets (edge file untouched); `AGENTS.md`, `README.md`, `docs/known-gaps.md` (dated third narrowing of the predicate entry; the fan-out-clone entry amended), `docs/design/pipeline-graph.md` (runtime model, provenance, backpressure, the stale rule count). | as listed; `schema/logit.schema.json` if anything moved | `script/cibuild`, `script/validate` green; `logit graph fixtures/fan-out-central.yaml` renders dashed targets and labelled dashed router edges; the edge/central pair runs in the dev container with host metrics on `metrics_out`, app lines on `app_out`, an untagged event on `untagged_out`. |

The five validation rules this stack adds are numbered **47-51**, not the 43-47 this plan
originally wrote: the `syslog-tcp-ingress-and-tls` and `graphite-carbon-relay` rules landed on
`main` first and took 43-46, so the target rules shifted up by four when the stack merged `main`.

Landing order: **W0 → W1 → W2 → W3 → W4 → W5 → W6**, strictly linear -- each stage's types are
the next stage's inputs. Each PR targets its parent workstream's branch.

## Verification

- `script/check` after each stage; `script/cibuild` before each PR; `script/schema` after W1 and
  W5 (both touch `logit-config`), with `committed_schema_is_current` as the gate.
- `script/validate` after W6, and `every_shipped_config_loads_and_validates`
  (`crates/logit-cli/src/config.rs`) in ordinary tests.
- The real pair, in `script/console`: `logit run fixtures/fan-out-central.yaml &` then
  `logit run fixtures/fan-out-edge.yaml`, feeding statsd on 8125 and syslog on 5514 as the edge
  file's header documents.
- With an `internal` component added locally (`fixtures/internal-telemetry.yaml` as the template):
  `logit.component.events.sent` appears under the *target* ids; removing `untagged_out` produces
  `logit.component.events.dropped{reason="unrouted"}` under the router.
- Allocation counts: `script/test -p logit-bench`.
