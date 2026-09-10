---
created: 2026-09-10
updated: 2026-09-10
---

# Attribute filtering is two transform components, and a bounded matcher is not a predicate language

## Status

Accepted

## Context

`Component.sources: Vec<String>` makes N→1 fan-in free -- a sink can already be fed from several
upstream branches at no extra graph cost (`docs/design/pipeline-graph.md`'s "Fan-in is free"). That
turns "one `logit_out` connection carrying N logical streams to a central node, instead of N
connections" into ordinary composition: tag each branch with `set` (`docs/adr/
operator-declared-resource-attributes.md`), then merge them into one `logit_out`. The native wire
format preserves both an event's own attributes and the batch's `Resource` end to end, one frame
per batch -- batches are never merged in flight, so each branch's tag survives the hop intact
(`docs/plans/native-transport.md`).

What has no spelling is the far side. Graph fan-out is *unconditional*: every consumer of a
component receives every batch it produces (`docs/design/pipeline-graph.md`'s runtime model), so
splitting one `logit_in` back into N branches needs a filter per branch, and there is no native
component that filters on an attribute's value. Today that filter is a five-line `lua` component,
once per branch:

```yaml
host_stream:
  type: lua
  sources: [central_in]
  script: |
    function process(event)
      if event.attributes["stream"] ~= "host" then return nil end
      return event
    end
```

**The revisit trigger this closes is a named one, not a hunch.** ADR `routing-by-condition-is-lua`
decided `logit` ships no native predicate language and retired `ComponentKind::Filter`, but it
named its own condition for reopening the question: *sustained central-collector throughput
pressure*, with a measured cost table -- a Lua route costs 9 allocations/event and 1.07 µs/event,
plus one dedicated OS thread **and** one LuaJIT VM per node; a native `Transform` costs 1
allocation/event and 360 ns/event, an ordinary tokio task in the shared runtime. Fan-out-after-
`logit_in` is precisely the central-collector role that table is about, and the cost multiplies by
branch count: every event reaching the listener is offered to all N branches, so an N-way split
pays N filter evaluations, N OS threads, and N LuaJIT VMs for a job that is, in every case here, one
attribute equality check. At the roughly 500k events/sec that ADR quotes for a central aggregator, a
four-way split spends on the order of four cores answering "does this event's `stream` tag say
`host`." That is the trigger firing, measured against a real shape this project's own architecture
produces, not an anticipation of one.

`has_signal`/`keep_signals`/`drop_signals` (`docs/adr/signal-filtering-components.md`) already
filter events natively, but they survived `routing-by-condition-is-lua`'s retirement of a general
`filter` component for a specific reason: they match against a *closed, three-element enum*
(`logs`/`metrics`/`traces`), not arbitrary operator-supplied data. `has_attributes`/
`drop_attributes` match arbitrary attribute values, so this is a genuine widening of what a native
component can express, and it needs its own defense rather than borrowing `has_signal`'s.

## Decision

**Two new transforms, `has_attributes` and `drop_attributes`, in `crates/logit-transforms/src/
attributes.rs`, implementing `logit_pipeline::Transform` the same way `has_signal`/`keep_signals`/
`drop_signals` do -- no Lua VM, no `Diagnostics` (nothing about matching a fixed set of configured
values can fail).** Config is `ComponentKind::Set`'s config, field for field: a `resource:` map and
an `attributes:` map, both `BTreeMap<String, SetValue>`. This matches on exactly what `set` can
stamp, and nothing more -- the two components are designed to be inverses of each other across that
boundary, not a general-purpose filter that happens to share a type.

**A bounded key/value equality matcher is not a predicate language.** No operators: `=` is never
spelled, because it is the only comparison the shape can mean. No boolean algebra: a map is a
conjunction, and there is no `or`, no `not`, no grouping. No expression grammar: nothing is parsed
-- the config is already a `BTreeMap<String, SetValue>` that `serde` deserializes directly, the same
literal type `set` uses. No addressing scheme: a key names one attribute literally, exactly ADR
`kv-metrics-semantics`' "nested fields are not addressable" rule. No runtime failure mode: total by
construction -- absent is `false`, a type mismatch is `false`, nothing here can error.

This is not the retired predicate grammar reduced in scope; it is a different kind of thing. That
grammar had to invent `attr.`/`resource.`/`log.`/`event.` namespacing, comparison operators, three
string functions (`exists`/`contains`/`starts_with`/`ends_with`), and `not(...)`. Here `resource:`/
`attributes:` *are* the namespacing -- two fields, closed -- and the only comparison is the one
`set` already implies by stamping a literal value.

**The bound is structural, not a documentation promise: `has_attributes`' config is `set`'s
config.** Whatever `set` can stamp is exactly what these two can match, field for field, literal
type for literal type. This surface cannot grow on its own -- it can only grow if `set`'s does,
which keeps the two review-linked rather than independently expandable.

**What stays Lua: everything with an actual operator in it** -- `>= 500`, `contains`,
`starts_with`, real regex, comparing two attributes to each other, `or`, `not` over more than one
pair. `routing-by-condition-is-lua`'s core holding -- that `logit` ships no native predicate
*language* -- is unchanged by this ADR; this is not an overrule of that decision, it is the
measured trigger it named, answered with the narrowest thing that answers it.

### Matching semantics

**A map is a conjunction -- every configured pair, within one map and across both `resource:` and
`attributes:` combined, must match.** This is what makes `has_attributes` and `set` inverses: a
`set` stage stamping `{stream: host, tier: gold}` and a `has_attributes` stage matching the
identical config forward exactly the events `set` tagged. Under an "any of these" reading, a
sibling branch sharing just one of the two pairs would leak through, breaking that inverse
property. **There is no `mode:` field, and the omission is a stated principle, not a placeholder for
later:** a mode flag is warranted only when the alternative reading cannot already be composed in
the graph. `has_signal`'s `mode: only` genuinely needs one -- "carries nothing outside this set" is
a negative assertion about payloads the config doesn't name, and there's no way to spell that by
adding more `has_signal` components. An "any of these pairs" reading needs no such flag: N sibling
`has_attributes` components, one pair each, all feeding one consumer, already say "any of these" in
the graph, because fan-in is free. Honestly stated: an event matching several of those siblings'
conditions is delivered to that consumer once per match, not deduplicated -- a real difference from
a single component with an internal `any_of`, and grounds to revisit if it turns out to matter in
practice, the same evidence-based posture `routing-by-condition-is-lua` itself asks for.

**`drop_attributes` is the exact complement of `has_attributes` on the identical config, taken at
the top level, not per pair.** It drops an event only when *every* configured pair matches; an
event matching some-but-not-all of several configured pairs is forwarded, not dropped. This is
structural, not a convention to remember correctly: `DropAttributes::process` is
`HasAttributes::process` with a single `!` at the one call site where the shared `Matcher`'s answer
is used.

The alternative reading -- "drop if *any* configured pair matches" -- is the complement of "forward
iff *none* match," which is a different filter, not this one's inverse. The top-level-complement
choice is forced by a second rule, not aesthetic:

**A configured key the event/resource doesn't carry never matches -- "absent is `false`."** Under a
*per-pair* negation instead of a top-level one, `drop_attributes {stream: host}` would drop every
event that doesn't carry a `stream` attribute at all -- a silent black hole for any untagged
traffic, and the exact opposite of what an else-branch in a fan-out needs. Under the top-level
complement this ADR chose, that same config forwards an untagged event instead, reading correctly
as "this event isn't one of the ones told to drop" -- precisely the else-branch role a
fan-out-after-`logit_in` topology needs downstream of the tagged branches. "Key present with any
value" is deliberately not expressible either way; `exists` was one of the three functions the
retired predicate grammar offered, and not shipping it is part of keeping this a bounded matcher
rather than a predicate language creeping back in one function at a time.

**Values coerce across numeric representations, modelled on `crate::numeric` and
`logit-script`'s `lua_value_matches`, deliberately not on `aggregate`'s `value_key_eq`.** Config
`status: 200` (a `SetValue::I64`) matches an event's `Value::I64(200)`, `Value::U64(200)`,
`Value::F64(200.0)`, and `Value::Str("200")` alike -- `json`, `logfmt`, and `scale` each produce a
different one of those variants for what an operator thinks of as the same value, and `aggregate`'s
`value_key_eq` is a *keying* equality (variant-exact, `f64` compared by bit pattern), correct for
hash-map identity and wrong here. `Value::Bytes` and `Value::Str` compare by byte equality so a
non-UTF8 attribute stays matchable. Two deliberate non-coercions, both load-bearing: two strings are
never compared numerically (`"01"` does not match `"1"` -- id-shaped tags are routinely
numeric-looking), and `Bool` never coerces to or from anything else, including a string (a
`logfmt`-sourced `sampled=true` is `Value::Str("true")`, so it must be matched as a string, not a
bool). A non-finite configured or actual value matches nothing, inherited from `numeric`'s
`is_finite` filter.

### Validation

At least one of `resource`/`attributes` must be non-empty; every key must be non-empty; every
numeric value must be finite. The black-hole/no-op assignment for the empty-config case is
*inverted* relative to `has_signal`'s family (rule 21): there, an allowlist (`keep_signals`) naming
nothing is the black hole, because `signals:` is a list of alternatives and an empty list satisfies
nothing. Here, `resource:`/`attributes:` is a map of *conjunctions*, and a conjunction over zero
pairs is vacuously true -- `has_attributes` with nothing configured therefore matches *every* event
(a no-op, forwarding everything untouched), and `drop_attributes` with nothing configured, being its
exact complement, also matches every event, which for a denylist means dropping every one of them (a
black hole). The same key appearing in both `resource:` and `attributes:` is deliberately *not*
rejected: the two maps address different objects (the batch vs. the event), so that configuration
is meaningful, not a mistake. `set`'s own empty-key gap (rule 12 previously checked only for "both
maps empty," not an individual empty key) is closed in the same change, so the "config is exactly
`set`'s config" claim above stays true of validation, not just of the type.

### Where the matcher lives

`routing-by-condition-is-lua` forces a retired predicate *parser* into `logit-core` specifically
because such a parser must be reachable from `graph::resolve` -- `logit-cli::pipeline::
validate_semantics` is literally `graph::resolve(config)?`, so anything not checked inside
`graph::resolve` escapes `logit validate` and breaks `docs/deploying.md`'s preflight promise, and
`logit-core` is the only crate both `logit-pipeline` (which owns `graph::resolve`) and
`logit-transforms` (which would own the transform) can share without a dependency cycle. That
argument does not apply here: rule 36 (`crates/logit-pipeline/src/graph.rs`) checks only
non-emptiness, empty keys, and finiteness -- all pure `logit-config` data, no `Value` comparison at
all. The value matcher (`value_matches`, `crates/logit-transforms/src/lib.rs`) is never called from
validation, so it stays `pub(crate)` in `logit-transforms`, beside `numeric`, which makes the same
placement argument for itself already.

## Alternatives considered

- **One kind with `mode: has | drop`, instead of two.** Rejected on the `keep_signals`/
  `drop_signals` and `logfmt`/`kv` precedent ("separate kinds sharing a module" over "one kind with
  a mode field") -- and for a reason specific to this pair: a mode flag would make rule 36's
  black-hole-vs-no-op error message conditional on a field's *value* rather than on the component's
  *kind*, which is exactly the confusion rule 21 already exists to avoid for `has_signal`'s family.
- **A third kind, or a `mode: any_of` field, for "any of these pairs matches."** Rejected per the
  composability principle above: N sibling single-pair `has_attributes` components already express
  it, since fan-in is free, and a mode flag would be a second way to spell something the graph
  already spells. The duplicate-delivery caveat (an event matching several siblings arrives once
  per match) is the honest cost of that choice, stated rather than hidden.
- **Reusing `aggregate::value_key_eq` for value comparison.** Rejected: it is a keying equality
  (variant-exact, bit-pattern `f64`), and using it here would mean `status: 200` silently never
  matches a `logfmt`- or `csv`-sourced `Value::Str("200")`, defeating the component for the two
  parser families whose output isn't JSON's native numeric syntax.
- **Putting the value matcher in `logit-core`**, mirroring the retired predicate parser's forced
  placement. Rejected: that placement was forced by a validation-reachability requirement this
  matcher doesn't have (see "Where the matcher lives" above) -- there's no cycle to avoid by moving
  it, so it stays where `numeric`, the function it's modelled on, already lives.
- **A `filter`/`where` component with a small expression language.** Still rejected, for the same
  reasons `routing-by-condition-is-lua` gave: it would be a second way to express what Lua already
  expresses, and the whole point of this ADR is to answer that decision's own named trigger with the
  narrowest possible native primitive, not to reopen the general question.

### Naming

`keep`/`remove` already own attribute-*key* trimming (drop/keep which *keys* survive on an event
that is forwarded either way); `has_attributes`/`drop_attributes` filter whole *events*, a different
operation entirely. Resolution: this pair belongs to the `has_signal` family, not the `keep`/
`remove` family -- the `has_` prefix is the tell, the same "whole-event, non-mutating" role
`has_signal` already established, and `drop_signals` already set the precedent that `drop_` names a
destructive whole-*something* operation (there, a payload slot; here, an event) rather than a
key-level trim, which stays `remove`'s word. Names considered and set aside: `lacks_attributes` (the
component doesn't express "lacks" -- it drops what *has* the configured attributes, i.e. the
complement of a positive assertion, not a negative one restated); `filter`/`where` (retired
terminology, see `routing-by-condition-is-lua`); `match_attributes`/`exclude_attributes` (an
asymmetric-sounding pair for what is a symmetric relationship, and `match` invites a regex reading
this component deliberately doesn't offer). One residual naming risk, stated rather than denied:
`drop_attributes` and `drop_signals` sit one word apart and both start a pipeline stanza with
`type: drop_`, so a config reader skimming quickly could momentarily conflate "drops payload slots"
with "drops whole events" -- their doc comments and this ADR are the disambiguation, not a renamed
component.

## Consequences

- `crates/logit-core/src/attrs.rs` gains `AttrMap::get_sym(Symbol) -> Option<&Value>`, a
  `binary_search_by_key` probe for a caller that already holds an interned key -- the read-side
  counterpart to `insert_sym`, needed because `has_attributes`/`drop_attributes` intern their
  configured keys once at construction and then probe those same `Symbol`s on every event, exactly
  `set`'s hot-path convention. `crates/logit-transforms/src/scale.rs` and `regex.rs` are migrated to
  it in the same change, replacing a `get(resolve(field))` round trip (a hash-table `resolve` to
  reconstruct a `&str`, immediately re-hashed by `get`) with a plain binary search.
- `crates/logit-pipeline/src/graph.rs`'s rule 12 (`set`) gains an empty-key check it didn't have
  before, so `set`'s own validation now rejects exactly what `has_attributes`/`drop_attributes`'s
  rule 36 rejects -- keeping "this component's config is `set`'s config" true of what's accepted,
  not just of the type declared.
- The resource-match cache (`crates/logit-transforms/src/attributes.rs`'s `Matcher`, `Set::
  map_resource`'s `Arc::ptr_eq` idiom applied to a read) always misses in the headline
  fan-out-after-`logit_in` topology, since `native::decode` mints a fresh `Arc<Resource>` per frame
  -- kept anyway, because unlike `set`'s own cache miss (which rebuilds an `AttrMap`/`Resource`/
  `Arc` and costs one allocation), a miss here costs nothing measurable: it only re-evaluates
  `get_sym` against the `Arc` already in hand. `docs/design/internal-telemetry.md` records no
  cache-miss counter for that reason -- one would advertise a cost that isn't actually there.
- `examples/fan-out-edge.yaml`/`examples/fan-out-central.yaml` are the worked example this ADR
  exists to make possible: N `set`-tagged branches into one `logit_out`, split back into N branches
  by `has_attributes` after `logit_in`, with the untagged else-branch expressed as a chained
  `drop_attributes`.
- If a future config needs an actual operator (`>=`, `contains`, cross-attribute comparison) rather
  than equality, that is new evidence for `routing-by-condition-is-lua`'s own revisit trigger, not a
  reason to grow this component past what `set` can stamp -- the retired predicate grammar sketch
  that ADR preserved is still where such a design would resume.
