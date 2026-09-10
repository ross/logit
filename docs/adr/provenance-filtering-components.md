---
created: 2026-09-10
updated: 2026-09-10
---

# Provenance filtering is two transform components, combining has_attributes' and has_signal's shapes

## Status

Accepted

## Context

Batch provenance (`origin`, `previous` -- `crates/logit-core/src/provenance.rs`,
`docs/adr/batch-provenance-on-delivered.md`) landed with three ways to *read* it as a batch flows
through a graph: a native transform's `Transform::observe_provenance` hook, a sink's
`Output::observe_batch`, and a read-only Lua `provenance` global. None of those *filter* on it.
Routing by provenance today means a `lua` component per branch:

```yaml
from_edge_nginx:
  type: lua
  sources: [central_in]
  script: |
    function process(event)
      if provenance.origin ~= "edge_nginx_in" then return nil end
      return event
    end
```

`docs/adr/attribute-filtering-components.md` already answered this same shape of question for
attribute values, reopening `docs/adr/routing-by-condition-is-lua.md`'s "no native predicate
language" holding just far enough to add `has_attributes`/`drop_attributes` for a *closed,
bounded* match: key/value equality over `resource:`/`attributes:`, no operators, no boolean
algebra beyond conjunction. `has_signal`/`keep_signals`/`drop_signals`
(`docs/adr/signal-filtering-components.md`) is the older instance of the same reasoning, over a
closed three-element enum.

`origin`/`previous` are an *easier* case to defend than `has_attributes`' was: each names a
component id, drawn from a graph's own `components:` map -- closed and known ahead of time
(`ComponentKind::HasProvenance`'s config is `String`s, not arbitrary operator-supplied `Value`s),
not the "genuine widening of what a native component can express" `has_attributes` had to argue
for. Fan-out-after-`logit_in` is exactly the central-collector role
`attribute-filtering-components.md`'s cost table is about, and it applies here unchanged: a Lua
route costs 9 allocations/event and 1.07 µs/event plus a dedicated OS thread and LuaJIT VM per
node; a native `Transform` costs 1 allocation/event and 360 ns/event, an ordinary tokio task. This
is the same measured trigger firing a second time, not a new one.

## Decision

**Two new transforms, `has_provenance` and `drop_provenance`, in
`crates/logit-transforms/src/provenance.rs`, implementing `logit_pipeline::Transform` the same
way `has_attributes`/`drop_attributes` do -- no Lua VM, no `Diagnostics` (matching a fixed,
already-validated config can't fail).** Config is two fields, `origin: Vec<String>` and
`previous: Vec<String>`, mirroring `has_attributes`' `resource:`/`attributes:` split under one
kind rather than two kinds split per field (`has_origin`/`has_previous`) -- `provenance` is the
umbrella name, matching the `Provenance` type and the Lua global; `origin`/`previous` are its
config field names.

**Each field is a list of alternatives, OR'd within the field; the two fields AND together when
both are configured.** This is `has_signal`'s disjunction-within-one-list shape
(`signals:`), applied independently to each of two fields, combined with `has_attributes`'
conjunction-across-fields shape (`resource:`/`attributes:`) -- genuinely new among existing
components as a combination, but built entirely from the two shapes already established, not a
new matching primitive. `Matcher::matches` (`crates/logit-transforms/src/provenance.rs`) is the
whole rule:

```rust
fn matches(&self) -> bool {
    let origin_ok = self.origin.is_empty()
        || self.provenance.origin.is_some_and(|o| self.origin.contains(&o));
    let previous_ok = self.previous.is_empty()
        || self.provenance.previous.is_some_and(|p| self.previous.contains(&p));
    origin_ok && previous_ok
}
```

An empty list means "not checked" for that field, the same "absent contributes nothing"
convention `has_attributes`' empty maps use -- not `has_signal`'s "empty list satisfies nothing";
see Validation below for why the field-shape resemblance to `signals:` doesn't carry that
consequence along with it.

**The cached-provenance read, not a `process` parameter.** Both kinds implement the new
`Transform::observe_provenance(&mut self, provenance: Provenance)` hook
(`crates/logit-pipeline/src/transform.rs`) -- the first real implementers -- caching `provenance`
on `self.matcher.provenance` exactly the pattern `Aggregator::observe_batch_context` already
establishes for `TraceContext` (`crates/logit-transforms/src/aggregate.rs`). `process` then just
reads the cached value. Provenance is per-*batch*, not per-event, so widening the per-event hot
path to carry it would cost every other transform for the one family that needs it -- the same
reasoning `observe_batch_context`'s own doc comment gives.

**`drop_provenance` is the exact complement of `has_provenance` on the same config, taken at the
top level, not per field.** It drops a batch's events only when the *whole* configured match
succeeds; a batch matching only `origin:` but not `previous:` (when both are configured) is
forwarded. Structural, not a convention to remember: `DropProvenance::process` is
`HasProvenance::process` with a single `!` at the one call site, exactly
`has_attributes`'/`drop_attributes`' own relationship.

**A batch with no provenance at all (`Provenance::default()`) never matches a non-empty field** --
the same "absent is `false`" rule `has_attributes` has for a missing attribute. In practice this
only matters for a batch observed before any `Fanout` ever touched it (a bench or unit test
constructing a bare `Provenance` directly): `Fanout` stamps `origin`/`previous` on every real hop
(`docs/adr/batch-provenance-on-delivered.md`), so a batch flowing through an actual graph always
carries both by the time any transform sees it.

**Allocation-free on the hot path.** Both fields are `Copy` `Option<Symbol>`s cached from
`observe_provenance`; `Matcher::matches` does a `Vec::contains` linear scan over a handful of
`Symbol`s (plain integer compares), matching every existing filter component's allocation
profile.

### Validation

Rule 37 (`crates/logit-pipeline/src/graph.rs`) rejects: both `origin:` and `previous:` empty
(`has_provenance` -- "matches every event, a no-op"; `drop_provenance` -- "matches every event,
and so can only ever drop every one of them"); any empty-string entry within either list ("could
never name a real component id"); a duplicate entry within one list (the same "a repeated entry is
almost certainly a copy-paste typo" reasoning rule 4 already applies to a repeated `sources`
entry).

**The empty-config black-hole/no-op assignment lines up with rule 36's (`has_attributes`), not
rule 21's (`has_signal`'s family) -- despite each field's own contents being a list of
alternatives, the same shape `signals:` has.** The difference is what "empty" means at the
*field*, not the list. An empty `origin:`/`previous:` means "this field isn't part of the match"
(vacuously true, so it never narrows what matches) -- exactly like `has_attributes`' empty
`resource:`/`attributes:` map -- not "match against zero alternatives" (vacuously false), which is
what makes `has_signal`'s family the inverted case. Two independently-omittable AND'd fields, each
an OR internally, is `has_attributes`' top-level shape with `has_signal`'s per-field shape nested
inside it, and it is the *top* level that decides this assignment: both fields empty means
`has_provenance` matches every batch (a no-op) and `drop_provenance`, its exact complement,
therefore drops every one of them (a black hole).

This was caught during implementation, not designed correctly on the first pass: an earlier draft
of rule 37 copied rule 21's orientation by surface analogy to `signals:`, and only a concrete
trace through `Matcher::matches` with both fields empty (always returns `true`) caught the error
before it landed. Recorded here because the field-vs.-list distinction that resolves it is easy to
miss again.

**Deliberately not validated: that a configured id actually names a component present in *this*
graph.** `origin`/`previous` are exactly as likely to name a component in a *different* process's
graph, relayed unchanged across `logit_out`/`logit_in`
(`docs/adr/batch-provenance-on-delivered.md`) -- the central-collector fan-in case this feature
exists for. A referential check here would reject the feature's own primary use case.

## Alternatives considered

- **Split per field (`has_origin`/`has_previous`), instead of one pair with two fields.**
  Rejected: `provenance` is the umbrella name already established by the `Provenance` type and the
  Lua `provenance` global, and a single pair with two fields mirrors `has_attributes`' own
  `resource:`/`attributes:` split under one kind rather than two kinds. Confirmed with the
  requester before implementation.
- **One kind with `mode: has | drop`, instead of two.** Rejected on the same precedent
  `attribute-filtering-components.md` gives: "separate kinds sharing a module, never one kind with
  a mode field" -- and for the same specific reason, a mode flag would make rule 37's
  black-hole-vs-no-op error message conditional on a field's *value* rather than the component's
  *kind*.
- **Cross-field OR** (`origin: [a] OR previous: [b]`, rather than AND). Rejected per the
  composability principle `attribute-filtering-components.md` already established: N sibling
  single-condition components feeding one consumer already express "any of these," since fan-in is
  free, so a cross-field-OR reading would be a second way to spell something the graph already
  spells.
- **Validating that a configured id names a real component in the graph.** Rejected -- see
  Validation above: the feature's primary use case is a batch relayed from a different process's
  graph, which such a check would reject.

### Naming

`has_provenance`/`drop_provenance` belongs to the `has_` family established by
`has_signal`/`has_attributes`: whole-event, non-mutating predicate plus its destructive
complement, not `keep`/`remove`'s attribute-key trimming. `provenance` over `origin_previous` or
naming the two fields as separate kinds: it is the umbrella name the `Provenance` type and Lua
global already use, and `origin`/`previous` read naturally as *its* two fields, the same relation
`has_attributes`' `resource:`/`attributes:` have to that component's name.

## Consequences

- `crates/logit-pipeline/src/transform.rs`'s `observe_provenance` doc comment and
  `crates/logit-pipeline/src/runtime.rs`'s comment at the `observe_provenance` call site both
  previously said no transform had a use for provenance; both are updated in the same change that
  adds the first real implementers.
- `docs/design/internal-telemetry.md`'s `logit.transform.events.filtered` counter entry is shared,
  not duplicated -- `has_provenance`/`drop_provenance` reuse the same metric name
  `has_attributes`/`drop_attributes`/`has_signal` already register it under.
- If a future config needs an actual operator, or a condition over something other than
  `origin`/`previous`, that is new evidence for `routing-by-condition-is-lua`'s own revisit
  trigger, not a reason to grow this component past two closed fields of component ids.
