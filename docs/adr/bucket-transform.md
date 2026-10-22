---
created: 2026-10-11
updated: 2026-10-21
---

# `bucket`: a numeric-range classifier that writes a bounded attribute value

## Status
Accepted

## Context

The equality components gate on a fixed set of values: `has_attributes`/`drop_attributes`
([ADR `attribute-filtering-components`](attribute-filtering-components.md)) filter on it, and
`route` ([ADR `target-components`](target-components.md)) picks a destination by it. None of them
can say "a request slower than one second", because that's a comparison, and
[ADR `routing-by-condition-is-lua`](routing-by-condition-is-lua.md) holds that `logit` ships no
native predicate language. Today a numeric threshold costs a `lua` component: a dedicated OS
thread, a LuaJIT VM, and roughly 9 allocations per event, where a native transform costs one.

Numeric thresholds are common on the hot path that ADR's revisit trigger names: a
central collector splitting slow requests, large responses, or error-range status codes into their
own stream. In nearly every case the operator wants a small number of named classes (`fast`,
`ok`, `slow`), not an arbitrary comparison. A class is a bounded attribute value, and a bounded
attribute value is what the equality components already match.

[ADR `value-allowlist-cardinality-clamp`](value-allowlist-cardinality-clamp.md) set the
precedent: `keep_values` maps an unbounded string into a bounded set by config. `bucket` does the
same for a number, by range instead of by membership.

## Decision

**`bucket` is a new native transform that maps a numeric attribute into a named class, per
field.** It writes the class as a string attribute; it never filters, routes, or drops an event.
An operator composes it with an equality component to gate on a range:

```yaml
latency_class:
  type: bucket
  sources: [parsed]
  attributes:
    http.request.duration_s:
      into: http.request.duration_class
      ranges:
        - lt: 0.1
          value: fast
        - le: 1
          value: ok
        - value: slow          # catch-all; no bound; must be last
      other: unknown           # optional
      keep_source: true        # optional, default true
```

**Config.** `attributes` maps a source field to its spec. Each spec has:

- `into`: the attribute the class is written to. Naming the source field replaces the number with
  its class in place. It can't name another configured source field or another field's `into`:
  fields apply in key order over one attribute map, so either would make the result depend on
  how the keys sort.
- `ranges`: an ordered list, first match wins. A range has one upper bound, `lt` (value < bound)
  or `le` (value <= bound), and a class `value`. A range with no bound is the catch-all and may
  only be last. Bounds must be finite, and every range must be able to match: each bound is larger
  than the one before it, except that `le: X` may follow `lt: X`, which classes the single value
  `X` (`lt: 0`, `le: 0`, catch-all is negative, zero, positive).
- `other`: the class for a present value that isn't a finite number, or that no range matches.
  Absent means nothing is written.
- `keep_source`: default `true`. `false` removes the source after a write, unless `into` names the
  source.

There's no `resource:` scope.

**Only upper bounds, in two kinds.** Ranges are ordered and first-match, so each range's lower
bound is the previous range's upper bound, and a config never needs `gt` or `ge`. Both upper-bound
kinds exist because each half-open shape needs one of them: `lt: 1000` says "below 1000", and
`le: 0` followed by a catch-all is the only way to say "above 0". With `lt` alone, "above 0" would
need a bound slightly above zero; with `le` alone, "below 1000" would need one slightly below it.
Neither has an exact spelling for an arbitrary `f64`.

**Per event, per field:**

- Source absent: no-op. `bucket` never stamps a class on an event that didn't carry the number,
  matching `keep_values` and `scale`.
- Source a finite number: `I64`, `U64`, `F64`, or a `Str` that parses to a finite `f64`, through
  the same coercion `kv_metrics` uses ([ADR `kv-metrics-semantics`](kv-metrics-semantics.md)). The
  first range that contains it writes its class.
- Source present but non-numeric or non-finite, or a number no range matches: writes `other` if
  set, else nothing.

No diagnostics: a value that doesn't classify is steady-state messy data (an access log's `-` for
an unset field), as `kv_metrics` argues for its skips. Telemetry mirrors `keep_values`' per-field
pair: `logit.transform.values.bucketed` and `logit.transform.values.unmatched`, each tagged with the
config-declared `field`.

**Graph validation (rule 82)** rejects an empty field name, `into`, `ranges`, class `value`, or
`other`; an `into` that names another configured source field, or that two fields share; a range
with both `lt` and `le`; a boundless range that isn't last; a non-finite bound; and a range the
ranges before it already cover: a bound below the previous one, or equal to it unless `lt: X` is
followed by `le: X`. Each is a config that can never write a class, a range that can never match, or
a result that depends on key order. Two ranges may share a class. An empty `attributes` map is
legal: the component forwards every event unchanged.

## Alternatives considered

- **A comparison operator on `has_attributes` (`duration_s: {gt: 1}`).** Rejected. That's the first
  operator of a predicate language, the line `routing-by-condition-is-lua` draws. `bucket`'s output
  is a bounded value, so `has_attributes`, `drop_attributes`, and `route` stay equality-only and a
  class is visible on the event for every downstream consumer, sinks included.
- **A single bound kind.** Rejected. One of the two half-open shapes becomes inexpressible, as the
  Decision explains.
- **`below:` as the only bound name.** Rejected for the same reason: `below:` reads as `lt`, and
  "above 0" would still have no exact spelling.
- **A `resource:` scope**, mirroring `keep_values`. Deferred: no current config classifies a
  resource number, and adding the scope later doesn't change the attribute-scope config.

## Consequences

- **`routing-by-condition-is-lua`'s holding is unchanged.** There's still no native predicate
  language. A numeric range becomes a native gate in two steps (`bucket`, then an equality
  component), and anything needing cross-attribute comparison, `contains`, or a boolean
  combination still needs `lua`.
- **Steady-state cost is zero allocations per event.** Each class is a `Value` built once at
  construction, so a write is a reference-count increment (`docs/design/memory.md`).
- **Bounds are config, not data.** Changing a threshold is a config change and a restart, the same
  as any other component setting.
- **Ordering is the operator's responsibility.** `bucket` must sit before the equality component
  that reads its class; `logit validate` checks graph shape, not node order.
