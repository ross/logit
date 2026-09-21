---
created: 2026-09-21
updated: 2026-09-21
---

# `flatten`: dotted-key expansion as an opt-in, operator-placed transform

## Status
Accepted

## Context

`Value::Map(Box<AttrMap>)` and `Value::Array(Vec<Value>)` are first-class, and four things in this
codebase produce nesting today: the `json` transform (which merges only the *top* level, so one
level down stays a `Value::Map` — `crates/logit-transforms/src/json.rs`), OTLP's `KvlistValue`
decode (`crates/logit-proto/src/otlp/common.rs`), `syslog_in`'s deliberately two-level `syslog.sd`
map ([ADR `syslog-structured-data-convention`](syslog-structured-data-convention.md)), and Lua
(`crates/logit-script/src/value.rs`, which round-trips a `Value::Map`/`Array` to and from a real
table).

Five sinks cannot represent any of that, and drop it outright:

| Sink | What happens to a `Value::Map` attribute | Counter |
|---|---|---|
| `influxdb_out` | tag dropped **silently** | **none** |
| `statsd_out` | tag dropped | `logit.output.tags.dropped{reason="unrepresentable"}` |
| `prometheus_out` | label dropped | `logit.output.labels.dropped{reason="unrepresentable"}` |
| `graphite_out` | tag dropped | `logit.output.tags.dropped{reason="unrepresentable"}` |
| `collectd_out` | dropped (collectd has no tag concept at all) | `logit.output.tags.dropped{...}` |

So today there is no way at all to get a nested JSON or OTLP field into an InfluxDB tag, a
Prometheus label, a statsd tag, or a carbon tag. `otlp_out`/`logit_out` (native) carry nesting
losslessly; `stdio_out`/`file_out`/`syslog_out` stringify it via `stdio::render_value`. The gap is
exactly the tag-rendering sinks, and it is the sinks v0.1 shipped against
([`docs/OVERVIEW.md`](../OVERVIEW.md)).

This repo has rejected dotted-key flattening three times before, each time as *implicit* decoder
or matcher behavior:

- [ADR `json-parsing-into-attributes`](json-parsing-into-attributes.md) rejected "Dotted-key
  flattening (`http.status`, `tags.0`)" at parse time as lossy: a real attribute literally named
  `http.status` becomes indistinguishable from a flattened `http: {status: ...}`.
- [ADR `syslog-structured-data-convention`](syslog-structured-data-convention.md) rejected a
  flattened `syslog.sd.<id>.<param>` because SD-NAME (both SD-ID and PARAM-NAME) may itself contain
  `.`, which makes a flattened key ambiguous to reassemble.
- [ADR `kv-metrics-semantics`](kv-metrics-semantics.md) settled "nested fields are not
  addressable" — a `field:` names an attribute literally, never a path into a `Value::Map` — and
  explicitly deferred a dotted-path syntax "for now… revisit if a real nested-JSON source needs
  it."

Those decisions stand and this ADR does not reopen them: nothing about decoding or field-matching
changes. What has changed is that a fourth place asked the same question with a different answer
available: `docs/known-gaps.md` and `docs/design/data-shapes.md`'s survey both show real,
production-shaped sources (pino-http's request serializers, Kubernetes label/annotation maps,
CloudTrail's `userIdentity` chain) landing on `logit` as nested data with **no path at all** to an
InfluxDB tag or a Prometheus label, and `influxdb_out`'s silent drop means an operator currently has
no signal that this even happened. The precedent for resolving that gap without relitigating the
three rejections above is `graphite_out`'s `multi_value: expand`
(`docs/known-gaps.md`'s Graphite section): a naming convention nothing at the far end understands is
legitimate precisely when it is **opt-in, named, and chosen per pipeline leg** by the operator who
placed it — and illegitimate exactly when a decoder or matcher does it on everyone's behalf, which
is what all three prior ADRs actually rejected.

## Decision

**A new transform, `flatten`, not a decoder or sink option.** It sits in the graph like any other
transform, visible in `logit graph`, run only where an operator writes it into a pipeline. It
rewrites a nested attribute value into flat, dot-joined keys: `{"foo": {"key": "bar"}}` becomes
`foo.key = "bar"`; `{"tags": ["a", "b"]}` becomes `tags.0 = "a"`, `tags.1 = "b"`; the two compose
(`{"items": [{"name": "x"}]}` becomes `items.0.name = "x"`).

**Config:**

```yaml
  flat:
    type: flatten
    sources: [parsed]
    # every field below is optional, shown at its default
    attributes: all     # all | none | [http, k8s.labels]
    resource: none      # all | none | [k8s.labels]
    arrays: index        # index | skip
```

`attributes`/`resource` is an untagged three-shape field (`all`, `none`, or a list) — the same
shape `SetValue` already takes. A named entry is a **literal top-level attribute name, never a
path**, exactly as `keep`/`kv_metrics`/`scale` field names already are — `flatten` is what *makes*
a nested field addressable by those components afterward, not a new addressing syntax layered on
top of them. `attributes` defaults to `all`: the motivating shapes (a Kubernetes label map, an
OTLP `KvlistValue`) are ones the operator doesn't enumerate keys for. `resource` defaults to
`none`: a resource is a small, mostly operator-declared identity map, and paying a per-batch
`Resource` rebuild for a map that usually isn't nested is a cost with no case behind it.
`arrays: skip` treats an array as a leaf wherever it appears and leaves a top-level array attribute
untouched entirely — the escape hatch for a source whose arrays are wide data rather than
structure.

**No `separator:` field.** The dot is fixed. `prometheus_out` and `statsd_out` already sanitize
`.` out of names/tags on their own wire (`sanitize_label_name`, `statsd`'s own `sanitize_into`), so
a dotted key already lands downstream without `flatten` inventing a second knob for the same
problem those sinks already solve.

**A leaf is any non-container value, or an empty `Map`/`Array`. A source attribute is removed only
once its leaves have been written.** This single rule, not a set of special cases, covers every
edge:

- `{}`/`[]` as a top-level attribute is itself a leaf — not selected, left untouched. `flatten`
  never deletes an attribute.
- `{"a": {"b": {}, "c": 1}}` becomes `a.c = 1` **and** `a.b = {}` — the fact that key `b` existed
  survives.
- Lua's empty table (`Value::Map(AttrMap::new())`) needs no arm of its own.
- A value deeper than a fixed internal recursion bound is written back **whole, still nested**, at
  the path reached — never half-expanded — and counted
  `logit.transform.values.unflattened{reason="max_depth"}`. This bound is a stack-safety constant,
  not a config knob, mirroring `logit_proto::native::value::MAX_VALUE_DEPTH`'s decode-side cap; the
  deepest shape `docs/design/data-shapes.md`'s survey measured is 5, so nothing observed comes near
  it.

**Last write wins on collision, silently** — plain `AttrMap::insert_sym` semantics, the same
collision policy the `json` ADR already settled ("inventing a different collision policy here
would be a new, undocumented special case"). No new counter: see Consequences for why this is an
acceptable-not-ideal call, made explicitly rather than by default.

**No cap on the number of keys a value can expand into**, beyond the depth bound above. Every
distinct flattened path is interned for the life of the process (the interner never evicts); see
Consequences and the accompanying `docs/known-gaps.md` entry.

**Containers touched:** `Event.attributes` always; `Resource.attributes` behind `resource:`, via
`map_resource` and the same one-entry `Arc::ptr_eq` cache `keep_values`/`set` use, carrying
`dropped_attributes_count`/`schema_url` forward unchanged. `Scope.attributes` is not reachable —
`Transform` has no `map_scope` hook, only the read-only `observe_scope`. `SpanEvent`/`SpanLink`
attributes and `Exemplar.filtered_attributes` are deliberately **not** touched: nothing in this
codebase mutates them today, and they only ever reach the wire through `otlp_out`/`logit_out`,
both of which carry nesting losslessly — flattening them would be pure loss for no gain.

## Alternatives considered

- **A `flatten: true`/similar option on each of the five sinks.** Rejected: five sinks each
  growing the same knob and re-deriving the same walk, invisible in `logit graph`, and unable to
  compose with a `keep`/`keep_values` placed after it to bound the cardinality it creates.
- **Doing it in Lua.** Already expressible today with no new component. Rejected as the *only*
  path: the native-transform family exists precisely so a common shape doesn't pay a Lua VM per
  worker and a table conversion per event — the same reasoning that moved `demo/logit.yaml`'s
  postgres tier off a `lua` component onto `regex` ([ADR `regex-transform`](regex-transform.md)).
  A pipeline that still wants Lua's flexibility can use it; `flatten` is for the common case.
- **A configurable `separator:`.** Rejected for the reason above — the two sinks that need
  dot-free keys already sanitize on their own. Revisit only if a real source needs a separator
  other than `.` for its own reasons, not to route around a downstream sink's naming rules.
- **A cap on keys minted per source value (`max_keys:` or similar), to bound interner growth.**
  Considered and rejected for v1: it adds a config surface and an all-or-nothing-expansion
  semantic before there is a concrete case demanding it, and the mitigations that already exist
  (a narrowed `attributes:` list, `arrays: skip`) cover the shapes actually measured. Tracked as a
  documented, not guarded, known gap — revisit if a real pipeline hits it.
- **Deleting an empty `{}`/`[]` instead of writing it back as a leaf.** Rejected: it is the only
  path on which `flatten` would delete rather than rewrite an attribute, breaking the "flatten
  never drops an attribute" property everywhere else.
- **Tagging the two counters with the source attribute name**, matching `keep_values`' `field`
  tag. Rejected: under the default `attributes: all`, the source key is data the operator doesn't
  control, and tagging by it would mint an unbounded telemetry series — exactly the property
  `shape` ([ADR `shape-observer-component`](shape-observer-component.md)) is built to avoid.
  `keep_values` may tag `field` only because its fields are config-declared; `flatten`'s usually
  aren't.
- **Counting a collision** (`logit.transform.values.collided` or similar). Considered, because it
  would make the `json` ADR's "indistinguishable" objection visible rather than silent. Deferred:
  detecting it costs one extra lookup per leaf on the hot path for a signal with no concrete
  operator request behind it yet, and the case is symmetric with the interner-cap deferral above —
  both are additive follow-ons, not core to the shape.

## Consequences

- **`flatten` creates attribute width, and width is cardinality.** Place a `keep`/`keep_values`
  *after* it, never before — the same placement discipline `keep`'s own docs already recommend
  ahead of `aggregate`. This is the loudest operational consequence of the whole component: a
  blanket `flatten` on an unbounded Kubernetes annotation map turns one attribute into an unbounded
  one.
- **Every distinct flattened path is interned forever, with no cap beyond the fixed depth bound.**
  An array of N elements mints up to N new symbols on its first occurrence (`tags.0`…`tags.N-1`),
  and a map keyed by rotating identifiers (user IDs, request IDs) mints one new symbol per distinct
  key ever seen, for the life of the process. This is a known, deliberate gap — see
  `docs/known-gaps.md`'s interner-growth entry, extended to name `flatten`'s array-index axis as
  the one genuinely new exposure over what `json`/`syslog_in` already accept. `arrays: skip` and a
  narrowed `attributes:` list are the operator's levers; there is no automatic one.
- **A flattened key can silently collide with a real attribute of the same literal name**, exactly
  the ambiguity `json-parsing-into-attributes` named — accepted here, not solved, because the
  comparison at the point of use is not "nested vs. flat" but "flat vs. gone": every sink
  downstream of a `flatten` was already going to drop the nested value entirely. Last-write-wins is
  silent, matching `json`'s own documented collision behavior; an operator who needs to know a
  collision occurred does not have a counter for it today.
- **`otlp_in -> flatten -> otlp_out` is no longer a lossless relay.** That is the operator's choice
  of a lossy leg, exactly the status `multi_value: expand` already has for `graphite_out` — none of
  [ADR `lossless-transit`](lossless-transit.md)'s named pairs (`statsd_in`/`statsd_out`,
  `otlp_in`/`otlp_out`, `syslog_in`/`syslog_out`, `prometheus_in`/`prometheus_out`,
  `collectd_in`/`collectd_out`, `graphite_in`/`graphite_out`) contains a `flatten`, so that ADR's
  guarantee is unaffected by this one.
- **`influxdb_out`'s silent drop of a `Value::Map` stays silent for now.** `flatten` is the fix
  going forward, not a repair of the existing diagnosability gap — giving `influxdb_out` the
  `logit.output.tags.dropped{reason="unrepresentable"}` counter its four sibling sinks already have
  is tracked as separate follow-up work, not part of this transform.
