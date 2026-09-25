---
created: 2026-09-13
updated: 2026-09-13
---

# `target` components: named destinations a router directs events into, beside `sources:`

## Status
Accepted

## Context

The component graph has exactly one way to express a branch: fan-out is unconditional (every
consumer of a component receives every batch it produces, `docs/design/pipeline-graph.md`'s
runtime model), so "send these events here and those there" is a filter component per branch,
each reading the whole flow and dropping what isn't its own. ADR
[`component-graph-configuration`](component-graph-configuration.md) chose that deliberately and
rejected "named outlets or edge predicates" as a second branching mechanism -- "two ways to do the
same thing." ADR [`routing-by-condition-is-lua`](routing-by-condition-is-lua.md) then declined a
native predicate language, and its measured revisit trigger has since fired twice for the
equality-only subcase, producing `has_attributes`/`drop_attributes`
([`attribute-filtering-components`](attribute-filtering-components.md)) and
`has_provenance`/`drop_provenance` ([`provenance-filtering-components`](provenance-filtering-components.md)).

Both of those fired on the same topology: one `logit_out` carrying N tagged streams over one
connection, and a central `logit_in` splitting them back apart
(`fixtures/fan-out-edge.yaml`/`fixtures/fan-out-central.yaml`). That central half today reads:

```
                          /--> host_stream (has_attributes) --> ...
  [the wire] --> central_in --> app_stream  (has_attributes) --> ...
                          \--> not_host (drop_attributes) --> untagged (drop_attributes) --> ...
```

Two costs are structural to this shape, not to the filters:

- **Every branch pays for every event.** N filters each run `process` over the whole flow to keep
  one N-th of it. The filter is cheap (1 allocation, ~525 ns per event, `docs/design/memory.md`),
  but it's paid N times per event, and the else-branch is a chain of complements whose length
  grows with N.
- **A fan-out with no `Output` branch clones the batch.** `Arc<EventBatch>` copy-on-write
  ([`arc-eventbatch-copy-on-write`](arc-eventbatch-copy-on-write.md)) made single-consumer edges
  and all-sink fan-outs free, but a fan-out whose branches all mutate -- which N filters after a
  listener always are -- still pays a full `EventBatch` clone per extra branch (6 allocations for
  the nginx shape, one *worse* than the pre-`Arc` code, `docs/design/memory.md` §3), "with no
  path to improvement under the current design."

The thing the filter chain cannot express is the one property that would remove both costs: that
each event has *one* destination, decided once. A batch's events could be partitioned -- moved,
not cloned -- into per-destination batches in one pass, if the graph had a place to send each
partition to. It doesn't: a component has one outbound edge set, and every consumer on it gets
everything.

The split-collection story in `docs/OVERVIEW.md` is what makes this worth a graph-level answer
rather than another filter kind. The central process's flows want to read *named streams* --
"the host metrics", "the app logs" -- and not care whether the thing producing them is a `route`
after `logit_in`, a Lua script, or two listeners on different ports. A stream needs a name in the
`components:` map that consumers can put in `sources:`.

## Decision

**A `target` is a component kind that is a named destination.** It has no fields and no
`sources:`. It is *fed by direction*: a router names it, per event, as where that event goes.
Downstream components read it exactly as they read anything else, by listing it in `sources:`.

```yaml
components:
  central_in:
    type: logit_in
    bind: 0.0.0.0:5150

  split:
    type: route
    sources: [central_in]
    by: {attribute: stream}
    routes:                  # value -> target id; these values are the graph's split -> target edges
      host: host_stream
      app: app_stream

  host_stream: {type: target}
  app_stream:  {type: target}

  windowed:
    type: aggregate
    sources: [host_stream]   # an ordinary source, nothing new to learn on this side
    interval: 60s

  untagged_out:
    type: stdio_out
    sources: [split]         # the router's own consumers: everything no route claimed
    target: stderr
```

**A router directs each event to at most one target.** Two router kinds:

- **`route`**, a native transform: equality only, one key read per event. `by:` is exactly one of
  `{provenance: origin}`, `{provenance: previous}`, `{attribute: <key>}`, `{resource: <key>}`;
  `routes:` maps a value to a target id. Several values may map to one target. An absent key, or
  a value no route names, is *unrouted*. This is `has_attributes`'/`has_provenance`'s bounded
  matcher pointed at a destination instead of a keep/drop verdict, and its config is deliberately
  no wider than theirs: no operators, no boolean algebra. `routing-by-condition-is-lua`'s core
  holding is unchanged -- anything needing an operator is still a `lua` component.
- **`lua`/`lua_file`**, with a `targets: [..]` list on the component and a new handle method,
  `event:to("target_id")`. The mark rides on the handle, so `return event` and `return {a, b}` are
  unchanged and each event carries its own destination; an unmarked event is unrouted;
  `event:to(nil)` clears; `event:clone()` copies the mark (a script fanning out a routed event
  gets two events headed the same way, and `b:to(nil)` is how they diverge); a `flush()`-built
  event honours its mark like any other. An id not in `targets:` is a script error, counted like
  every other script error, never a silent forward.

**Unrouted events go to the router's ordinary consumers** -- whoever lists the router in
`sources:` -- which is how the else-branch is spelled: not a chain of complements, but the
router's own edge. A router with targets and no ordinary consumers is a legal config (rule 50
below); its unrouted events are dropped and counted,
`logit.component.events.dropped{reason="unrouted"}`, never silently.

**`targets:` is a `Component` field, beside `sources:`/`buffer:`/`receive:`, not a field on the
Lua variants.** `ComponentKind` variants cannot `deny_unknown_fields` (a serde limitation with
`flatten`, recorded at `TailOptions`), so a `targets:` on the wrong kind would be silently
ignored -- exactly the failure rules 14/17/33 exist to catch for `buffer:`/`receive:`/
`compression:`. On `Component` it is rejectable by the same rule shape. `route` never sets it:
its edges derive from `routes:`' values, so there is nothing to repeat.

**Why this is not the "named outlets" `component-graph-configuration` rejected.** That rejection
was of a second way to spell an *edge*: an outlet name on the producer side, `sources:` on the
consumer side, two declarations for one thing. Here, direction is expressed in exactly one new
place -- router → target, declared on the router -- and every other edge in the graph, including
target → consumer, stays `sources:`. A router may not name an ordinary component (that would be
the inversion the rejection was guarding against); the *only* thing it can name is a `target`,
which has no `sources:` of its own to conflict with. And it is not a second branching mechanism
so much as the same one made cheap: filter chains still work, are still the answer for any
condition a router can't express, and nothing about them changes. What a target adds is the
runtime property they provably cannot have -- N-way fan-out plus N filter passes becomes one
partition pass, and N-1 deep clones become zero. The measured accounting is in Consequences.

**Runtime: a target is a zero-cost alias.** It has no task, no inbox, no channel. The node
runtime builds one `Fanout` per target, wired to that target's consumers' inboxes, and hands a
clone to every router that directs at it. A router therefore owns its ordinary `Fanout` plus a
slot-indexed `Vec<Fanout>`, one per target in `graph::targets_of` order. Per incoming batch it
routes every event (borrowing, never moving, so an ~800-byte `Event` is not copied through an
enum), counts per destination, reserves exactly, moves each event into its destination's
`Vec<Event>`, and sends every non-empty partition under **one** child `BatchContext` and **one**
span -- one incoming batch is one hop however many ways it forks, the same rule `Fanout` already
applies to an ordinary fan-out. Two routers directing at one target each hold a clone of the same
senders: fan-in at a target is free, exactly as `sources:` fan-in is.

Two operator-visible consequences fall out of the alias, and are features rather than accidents:

- **`previous` downstream of a target is the target's id**, never the router's; `origin` is
  untouched. Each target `Fanout` is built `with_component(<target id>)`, so
  [`batch-provenance-on-delivered`](batch-provenance-on-delivered.md)'s one stamping rule applies
  unchanged and a `has_provenance{previous: [host_stream]}` reads naturally. Which *router* fed
  a target is not recoverable from provenance; if that ever matters it is a router-side metric,
  not a provenance change.
- **A target has its own telemetry.** Its `Fanout` carries the target's `Telemetry` handle, so
  the uniform layer-2 producer set (`logit.component.batches.sent`/`events.sent`/
  `send.blocked.duration`/`events.dropped{reason="closed_consumer"}`) appears under the target's
  id -- per-stream volume becomes visible with no new metric. A target has no receive side at all.

**Graph.** A fourth role, `Target`, alongside listener/transform/sink. Router → target edges
come from a pure `graph::targets_of(&Component)` and participate in cycle detection: a target's
indegree counts its routers, so `router → target → … → router` is caught as the deadlock it
would be. `logit graph` renders targets as dashed boxes and router → target edges dashed,
labelled with the route key. Validation adds five rules (47-51, `docs/design/pipeline-graph.md`):
`targets:` only on `lua`/`lua_file`; every target reference resolves, names a `target`, isn't
the router itself, and isn't repeated; a `target` declares no `sources`, has at least one
consumer, and is directed to by at least one router (rule 7's mirror: a target nothing routes to
is the same black hole, seen from the other end); a router needs at least one consumer *or*
target; a `route` has a non-empty `routes:` with no empty key, value, or key name. Deliberately
not validated, per rule 37's reasoning: that a `{provenance: ..}` route key names a component in
*this* graph -- it is exactly as likely to name one relayed from another process.

**Naming.** `target` over `outlet`/`port` (those name a thing *on* a router; this is a thing in the
map), `stream` (already an attribute name in the examples), and `junction` (fan-in is a property,
not the purpose). `stdio_out`/`file_out` already have a `target:` *field* (`target: stdout`); a
`type: target` *kind* doesn't collide with it technically, and the two read differently enough in
context that renaming a shipped field wasn't worth it. `route` over `switch`/`split`: it names
what the component does to an event, the way `set`/`keep`/`scale` do. `event:to(..)` over
`event:route(..)`/`event:send(..)`: it marks, it doesn't emit, and `to` reads as a destination.

## Alternatives considered

- **Named outlets** (`split` exposes `host`/`app`; consumers write `sources: [split.host]`).
  The cleaner graph model: every edge stays consumer-declared, no fourth role, no producer-side
  edge set in cycle detection, roughly half the validation rules, no alias node state -- and the
  router trait, `route` transform, partition, and `event:to` would be identical. Rejected on
  what it can't do: an outlet is a port on a specific router, so consumers couple to the router's
  identity, and "two routers feed one stream" means every consumer listing both. Per-outlet
  telemetry would also need a registry entry per outlet name (tags are `'static`). The target's
  global name is precisely the abstraction the split-collection story wants -- consumers read a
  named stream and don't care what feeds it -- and everything below the naming layer is shared,
  so a pivot to outlets would be a graph-layer change only.
- **Edge predicates** (`sources: [{from: central_in, where: stream == host}]`). A predicate
  language on edges, ruled out by `routing-by-condition-is-lua`, and evaluated per consumer -- so
  still N passes over every event and no clone saved. Worse on every axis.
- **A `targets:` map on `logit_in`** (remote origin → local target). Fewer components in the
  central config, but per-input routing config that covers only the wire case; attribute and Lua
  routing would still need something else, and it goes against the project's settled preference
  for composable components over per-input fields (`set` over resource fields on listeners).
- **Routers naming ordinary components directly** (`routes: {host: windowed}`), no `target`
  kind. Rejected: that is a `sources:` entry written on the wrong side of the edge -- the
  inversion `component-graph-configuration`'s rejection was about -- and it makes a router the
  only kind whose consumers aren't visible in the consumers' own config.
- **A target with its own task and inbox** (a pass-through node). One extra channel hop and one
  extra task per target, for nothing the alias doesn't give -- provenance and telemetry both
  attach to a `Fanout`, not a task.
- **Keeping the filter chain and doing nothing.** Correct, and still the answer for any
  condition needing an operator. Rejected as the *only* answer by the two structural costs in
  Context, both of which grow with branch count on exactly the central-collector topology this
  project exists for.
- **Widening `Transform::process` to return a destination.** Rejected: every transform would
  carry a routing concern; a separate `Router` trait costs one small trait and one node kind.
- **A second Lua return value** (`return event, "host_stream"`). Minimal, but a `{a, b}` table
  could only go to one target, and it is a new return-contract shape; the handle mark composes
  with the existing contract instead.
- **Separate `route_provenance`/`route_attributes` kinds** on the `has_*` precedent. That
  precedent's reason (a mode *flag* making an error message depend on a field's value) doesn't
  apply to a closed, externally-tagged `by:` enum, and one kind keeps `routes:` in one place.

## Consequences

- **Config.** `ComponentKind::Target {}` and `ComponentKind::Route { by, routes }`; `RouteBy`/
  `ProvenanceField` enums; `Component.targets: Vec<String>`. Schema regenerated. A `Component`
  field shares one flattened key namespace with every `ComponentKind` variant's fields, so
  `targets` is now reserved the way `sources`/`buffer`/`receive` already are -- and
  `prometheus_in`'s scrape-URL list, which was named `targets`, is renamed `scrape_targets`
  (amendment in [`prometheus-scrape-and-exposition`](prometheus-scrape-and-exposition.md)). A
  breaking config change, accepted pre-release: the routing concept is the more general use of the
  word.
- **Graph.** `Role::Target`; `graph::targets_of`/`target_edges`; `ResolvedComponent.targets`
  (slot order); `topological_order` over `sources` plus target edges; rules 47-51; `dot.rs`
  styling. Rule 6's arity table gains a row; rule 7 is relaxed for routers only.
- **Runtime.** `logit_pipeline::Router` trait (`route(&mut self, &Arc<Resource>, &Event) ->
  Destination`, plus the `observe_*`/`map_resource` hooks `Transform` has; no flush -- no router
  flushes, and the trait says so rather than carrying an unused hook), `NodeSpec::{Router,
  Target}`, `run_router`, `route_batch`, a pre-spawn target-`Fanout` pass, `NodeState::Alias`.
  The target `Fanout` map must be dropped alongside the construction-only senders map or the
  shutdown cascade can never fire -- a hang, not a test failure, so it is pinned by a test.
- **Allocation accounting** (`crates/logit-bench/tests/allocations.rs`, exact-equality, with
  `docs/design/memory.md` §3 updated in the same commit): a router costs `1 + (destinations that
  received events)` allocations per batch and zero per event -- the count-then-`reserve_exact`
  partition exists specifically so that number is an integer and not a logarithm -- against one
  full clone per extra branch plus one `process` pass per filter for the same split today.
- **Lua.** `EventProxy` carries the mark (`Cell<Option<u16>>`) and a shared name → slot table
  built once per worker; `ProcessOutcome`/`flush()` return `(Event, Option<u16>)`. The mark
  stays off `Event` (`type_sizes.rs`) and off `Delivered`, for the same reason provenance does.
- **Telemetry.** `reason="unrouted"` joins `events.dropped`'s reasons; a target emits the layer-2
  producer set under its own id and nothing on the receive side.
- **Backpressure is unchanged.** A stalled consumer of one target still backs up through the
  router into every other target's flow -- routing decides *where* events go, not whether a slow
  branch can block a fast one. `docs/design/pipeline-graph.md`'s head-of-line warning still applies.
- **Docs.** `component-graph-configuration` and `routing-by-condition-is-lua` each gain a dated
  amendment, not a rewrite; `pipeline-graph.md`, `lua-api.md`, `internal-telemetry.md`,
  `memory.md`, `known-gaps.md`, `deploying.md` (the `alias` node state) updated;
  `fixtures/fan-out-central.yaml` rewritten onto `route` + targets with `fan-out-edge.yaml`
  untouched -- the same edge config, a central config that no longer clones.
- **Build-out:** [`docs/plans/target-components.md`](../plans/target-components.md).
