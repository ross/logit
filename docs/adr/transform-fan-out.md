---
created: 2026-10-16
updated: 2026-10-16
---

# Native transform fan-out: `Transform::process_into` emits extra events into the same batch

## Status
Accepted

## Context
A native transform can't turn one event into several. `Transform::process` borrows one event and
answers forward or discard ([ADR `in-place-transform-process`](in-place-transform-process.md)),
and `process_batch` (`crates/logit-pipeline/src/runtime.rs`) drives it with `Vec::retain_mut`
over the batch's own `events`, so an ordinary transform hop allocates nothing per batch.

Every other node kind that makes events can already make several from one input:

- A listener's `Decoder::decode_into(bytes, received_at, out: &mut Vec<Event>)` pushes any number
  of events per datagram or request.
- A Lua `process()` can `return {a, b}` (`ProcessOutcome::EmitMany`,
  [`lua-api.md`](../design/lua-api.md)'s "Script contract"). `run_lua` pushes each emitted event
  into its destination buffer in emission order, counts them as
  `logit.script.events.emitted{outcome="emit_many"}`, and sends them in the parent's batch. Each
  carries its own `event:to(..)` route mark.

The first native caller is `line_protocol`
([ADR `line-protocol-transform`](line-protocol-transform.md)), which turns one log event whose
body holds many InfluxDB line-protocol points into one event per point. Doing that in Lua costs a
VM thread, a reimplementation of line-protocol escaping and typing, and a second copy of
`influxdb_in`'s event shape.

## Decision
`Transform` gains one method with a default, so every existing implementor is unchanged:

```rust
fn process_into(
    &mut self,
    resource: &Arc<Resource>,
    event: &mut Event,
    emitted: &mut Vec<Event>,
) -> bool {
    let _ = emitted;
    self.process(resource, event)
}
```

The return value means what `process`'s does: `true` forwards `event` in place, `false` discards
it. Each event the transform pushes onto `emitted` is forwarded as well, whatever the bool says.
A transform that overrides `process_into` still implements `process`, and the runtime calls only
`process_into`.

**`process_batch` drives it.**

- The node loop owns one scratch `Vec<Event>`, kept across batches, and `process_batch` borrows it.
  `retain_mut` calls `process_into` with the scratch as `emitted`.
- After the loop, `process_batch` appends the scratch to the batch's `events` with `Vec::append`,
  which leaves the scratch empty at its capacity. Extras come after the surviving in-place events,
  in emission order across the whole batch.
- An empty batch is decided after the append, so a transform that discards every parent and
  emits children still sends a batch.
- A transform that never pushes costs one empty-`Vec` check per batch and allocates nothing. Every
  pin in `crates/logit-bench/tests/allocations.rs` stays as it is; a pin that moves is a
  regression, not a new constant.

**The children are part of the parent's batch.** They share its resource (after `map_resource`),
its scope, its `Delivered` trace context, and its provenance. There's no extra inbox send, no new
blocking point, and no change to the inbox bound, which counts batches.

**Telemetry stays as it is.**

- `events.received` counts the batch as it arrived.
- `events.discarded` counts the parents `process_into` returned `false` for.
- `events.sent` counts what left, children included, so it can exceed `events.received` on a
  fan-out node, as it already can on a `lua` node.
- The node's `"process"` span records the outgoing count.
- No `events.emitted` counter. The count is `events.sent − (events.received − events.discarded)`
  over the same interval. A transform that wants a count of its own emits one.

**No route marks.** A native transform has no `targets:`, so its children carry no mark and go to
the node's ordinary edge. Routers (`Router::route`) and sinks don't get an emit path.

**No cap on the events one parent emits.** A per-parent cap would drop data an operator asked for
to defend against a sender that isn't in the threat model
([ADR `deployment-threat-model`](deployment-threat-model.md)). The bound is the parent's own size:
a UDP datagram, a line under `max_line_bytes`, or a request under `max_request_bytes`. This is the
reasoning behind `flatten`'s uncapped expansion ([ADR `flatten-transform`](flatten-transform.md)).

**Flush is unchanged.** `Transform::flush` already returns any number of events.

## Alternatives considered
- **Change `process` itself to take `emitted`.** It touches every implementor, 35 today, to
  add an argument almost none of them use.
- **A batch-level trait method** that hands each transform the whole `Vec<Event>`.
  [ADR `in-place-transform-process`](in-place-transform-process.md) rejected it, because it moves
  batch telemetry out of the runtime and into every implementor. That reasoning still holds.
- **Interleave children at the parent's position, as `run_lua` does.** `run_lua` builds new
  destination buffers per batch anyway. `process_batch` keeps survivors in place so a forwarded
  event never moves, and interleaving would rebuild the batch on every fan-out, paying the per-batch
  `Vec` and the per-event memcpy that ADR removed. The relative order of a parent and its children
  carries no meaning for `line_protocol`, whose children carry their own timestamps.
- **A new node kind for expanders.** It needs its own runtime loop, graph rules, and registry
  plumbing for what one trait method with a default covers.
- **Leave fan-out to Lua.** A Lua node costs a dedicated OS thread, a VM, and a table conversion
  per constructed event, and every recipe re-implements its parser in Lua patterns.

## Consequences
- `process_batch`'s signature gains the scratch, so its callers in
  `crates/logit-bench/tests/allocations.rs` and the benches pass one.
- Event order within a batch changes for a fan-out transform: a forwarded parent arrives before
  every child, not beside its own. A downstream stage that keeps the last value in arrival order,
  such as `aggregate`'s gauge, sees a forwarded parent's value before its children's for the same
  series. Children from different parents keep their parents' order.
- Memory per inbox slot can grow by the fan-out factor, as it can behind a `lua` node today. The
  inbox bound counts batches, not events, so a node that emits 37 events per parent holds up to 37
  times the events per slot.
- `transform.rs`'s module doc ("`process` is per-event") and
  [`pipeline-graph.md`](../design/pipeline-graph.md)'s "Node kinds and the transform trait question"
  describe the emit path.
- The soak harness's `ledger.edge` (`tools/soak/soaklib/checks.py`) judges each transform in
  `sut_transforms` hop by hop from `events.received` and `events.sent`. A scenario that puts a
  fan-out transform there must account for its children first. No scenario does today.
