---
created: 2026-09-15
updated: 2026-09-15
---

# A Lua `flush()` runs in a root context, not the last batch's

## Status
Accepted

## Context

A `lua`/`lua_file` component with an `interval:` gets its `flush()` called on a timer (and once
more when its inbox closes), and whatever it returns is sent as one batch under a freshly minted
trace root with this component stamped as both `origin` and `previous`
([ADR `trace-context-propagation-on-delivered`](trace-context-propagation-on-delivered.md),
[ADR `batch-provenance-on-delivered`](batch-provenance-on-delivered.md)). That much was already
right. What the *script* saw inside `flush()` was not: the four batch-scoped globals
`docs/design/lua-api.md` gives a script -- `trace`, `provenance`, `resource`, `scope` -- were only
ever set before a `process()` batch, so a `flush()` read whatever the most recently processed
batch had left behind, and `run_lua` (`crates/logit-pipeline/src/runtime.rs`) stamped the flushed
batch with that same last-seen resource and scope (`last_resource`/`last_scope`). Three
`docs/known-gaps.md` entries carried this as "stale during `flush()`", with the script-side
`resource`/`scope` write inside `flush()`
([ADR `operator-declared-resource-attributes`](operator-declared-resource-attributes.md)) as the
workaround.

"Stale" was the wrong frame. The last batch is not an approximation of the right answer that
happens to be a little out of date; it is *unrelated* to a flush-driven emission. A flush is the
result of no one event and no one batch -- however many contributed, `logit` cannot know which --
so there is no incoming context it could be "fresh" with respect to.

## Decision

**A Lua `flush()` runs in a root context.** Immediately before every `flush()` call, `run_lua`
resets the script's batch-scoped globals to what a stand-alone emission genuinely is:

- `trace.trace_id`/`trace.span_id` -- the fresh root the emission is sent under (the same ids the
  node's own `flush` span records).
- `provenance.origin`/`provenance.previous` -- this component, both of them; i.e.
  `provenance.origin == provenance.previous == provenance.component`. This is what this node's
  own outbound edge stamps, pre-filled on the `BatchContext` so what the script reads and what
  the batch carries are one value, not two code paths agreeing. An event the script marked for a
  `target` takes one hop more, and that target's `Fanout` rewrites `previous` to the target's id
  exactly as it does on the `process()` path
  ([ADR `target-components`](target-components.md)); `origin` stays this node.
- `resource` -- empty. `scope` -- none (the all-clear defaults `scope` documents).

A script is free to change what it can already change: a write to `resource` or `scope` inside
`flush()` is committed onto the flushed batch exactly as before, and is now the *only* way a
flush-driven emission carries a resource or scope. Nothing from the most recently processed batch
carries over, by design. `trace` and `provenance` stay read-only in effect (`provenance` rejects
writes; a write to `trace` is clobbered on the next `process()`/`flush()`, as it always was).

The per-batch `process()` path is untouched.

## Alternatives considered

- **Keep the last-seen resource/scope as the default.** Rejected. It is right only when every
  upstream batch shares one resource and one scope, and silently wrong the moment a component has
  two -- the entire "real gap" the known-gaps entries admitted to. An empty resource is honest;
  a script that knows the answer writes it.
- **Track contributing contexts and link them, as `Transform::flush` does for `aggregate`.**
  Rejected for the reason [ADR `trace-context-propagation-on-delivered`](trace-context-propagation-on-delivered.md)
  already gives: a Lua component has no accumulator `logit` can inspect, so there is no state to
  record contributors *into*. A script that wants that can do its own bookkeeping in `process()`.
- **A per-component `resource:` config knob as the flush default.** Not needed:
  [ADR `operator-declared-resource-attributes`](operator-declared-resource-attributes.md)'s `set`
  component downstream of the Lua node, or a `resource[...]` write inside `flush()`, both already
  express it without a new field, and Ross's standing preference is composable components over
  per-input config.

## Consequences

- `run_lua`'s `last_resource`/`last_scope` are gone; `flush_now` calls the same four
  `ScriptWorker` setters the batch path does (`set_trace_context`, `set_provenance`,
  `set_resource`, `set_scope`), fed the root. No `ScriptWorker` API change.
- A stateful script that relied on the last batch's resource reaching its flushed events now
  emits under an empty resource unless it writes `resource` in `flush()`. Pre-release, no
  compatibility shim; `docs/design/lua-api.md` says so at each global.
- The three Lua-`flush()`-staleness entries in `docs/known-gaps.md` are closed by this ADR, and
  [ADR `aggregation-window-semantics`](aggregation-window-semantics.md)'s "stamped with whichever
  resource the worker most recently saw" sentence is superseded.
- A script reading `provenance.origin` inside `flush()` to decide what to do now sees its own id,
  which is what its emission goes out stamped with on this node's own edge -- the specific
  mismatch the old provenance entry called out.
- A flushed event marked for a `target` (`e:to("a")`) now reaches the target's consumers with
  `origin` naming the flushing Lua node; it used to be the *target's* id, since the flush context
  carried an empty `origin` for the target's `Fanout` to `get_or_insert` into. The new value is
  the one [ADR `batch-provenance-on-delivered`](batch-provenance-on-delivered.md) and
  [ADR `target-components`](target-components.md) already specify -- "`previous` downstream of a
  target is the target's id, `origin` is untouched" -- so this is the target rule finally
  applying to the flush path too, not a new rule.
