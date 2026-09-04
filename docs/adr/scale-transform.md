---
created: 2026-09-03
updated: 2026-09-03
---

# `scale`: unit conversion by constant factor, and why it stays out of `kv_metrics`

## Status

Accepted

## Context

`demo/logit.yaml`'s HAProxy and nginx tiers both want to feed a shared `web.request_time`
`kv_metrics` distribution, tag-filtered per tier by `service.name` in InfluxDB. HAProxy's `%Tr`
reports response time in **milliseconds**; nginx's `$request_time` reports it in **seconds**. Two
sources reporting the same measurement name in different units would sum/quantile nonsense
together if a panel ever queried them unfiltered, and `influxdb_out` never writes a metric's
`unit:` at all (it encodes name, tags, and value only — `crates/logit-outputs/src/influxdb.rs`), so
nothing downstream of `kv_metrics` can catch or correct the mismatch after the fact.

`kv_metrics` itself can't do this: it only *reads* a field into a metric value
(`docs/adr/kv-metrics-semantics.md`), and giving it a per-field multiplier would blur its one job
(deriving metrics) with a second, unrelated one (rewriting attributes) that has nothing to do with
metric derivation — nginx's `request_time` attribute needs converting whether or not anything ever
turns it into a metric at all. A general native transform, run once ahead of `kv_metrics`, is the
smaller and more reusable primitive.

## Decision

**`scale` is a new `ComponentKind`/`Transform`: `fields: BTreeMap<String, f64>` mapping an
attribute name to a multiplication factor.** It reads each named attribute, multiplies it by its
factor, and writes the result back under the same name — no renaming, matching `set`'s
mutate-in-place idiom rather than `trace_context`'s lift-and-remove one.

**The result is always written as `Value::F64`, even when the product happens to be integral.** A
rule like "keep it `I64` when the multiplication stays exact" would make one attribute's type flap
between events depending on the input value, which is a real hazard for any OTLP/Loki consumer
downstream that infers a field's shape from one sample. A scaled value is a computed quantity;
`F64` is the honest, stable type for it.

**A missing, non-numeric, or non-finite-result field is a silent skip for that field, never a
dropped event** — exactly `kv_metrics`'s posture toward its own fields
(`docs/adr/kv-metrics-semantics.md`), for the same reason: a config-driven per-field operation
should not fail an entire event over one field it happens not to apply to. Numeric coercion is
identical to `kv_metrics`'s own rule — `I64`/`U64`/`F64` directly, a `Str` that parses cleanly to a
finite `f64`, everything else rejected — lifted into a shared `pub(crate) fn numeric` in
`crates/logit-transforms/src/lib.rs` rather than duplicated, since both transforms now need it.

**Graph validation rejects an empty `fields` map (a certain no-op, the same rule `kv_metrics` and
`set` already have), an empty field name (could never match a real attribute, the same rule
`trace_context` already has for `trace_id`), and a non-finite factor** (`NaN`/`inf` in config is
almost certainly a typo, and would only ever produce values `numeric` then silently rejects
downstream — catching it at `logit validate` time is strictly better than a config error nobody
notices).

**Stateless**, like `json`/`kv_metrics`: only `process` is overridden, `flush_interval`/`flush`
keep the `Transform` trait's defaults. No `Diagnostics` builder, matching `Keep`/`Remove`/`Set`: a
silent per-field skip is documented behavior, not a failure worth a throttled diagnostic.

## Alternatives considered

- **A `scale:` field directly on `kv_metrics`'s `MetricSpec`.** Rejected: it would only affect the
  derived *metric* value, leaving the source attribute itself unconverted — but `demo/logit.yaml`
  also needs the log-line attribute name (`request_time`) to keep meaning "seconds" outside the
  metrics branch, and a future consumer of the same attribute for something other than a metric
  (a `keep`-then-tag use, say) would silently see the wrong unit. Keeping the conversion as its own
  transform, upstream of `kv_metrics`, makes both consumers see the same corrected value.
- **A general `Value::as_f64` on `logit-core`, or a public `numeric` on `logit-core::Value`.**
  Rejected for the same reason `kv_metrics`'s own ADR rejected it: a method that silently parses
  strings out of any `Value` caller is a surprising, easy-to-misuse addition to a general-purpose
  type. `numeric` stays `pub(crate)` inside `logit-transforms`, shared by the two callers that
  actually need it.
- **Rounding or preserving integer types when the product is exact.** Rejected — see "The result is
  always `Value::F64`" above: type stability across events matters more than occasionally saving a
  few bytes on the wire.

## Consequences

- A pipeline that puts `scale` after the attribute it's converting has already reached a sink, or
  after `kv_metrics` has already derived a metric from the unconverted value, gets no error — same
  as `kv_metrics`'s own "no way to know a downstream `keep` is or isn't coming" consequence. Config
  ordering is the operator's responsibility; `logit validate` checks the graph's shape, not the
  semantic effect of node order.
- `event.attributes` can now be rewritten by three different transforms with three different
  postures: `set` (unconditional overwrite from constants), `trace_context` (lift-then-remove), and
  `scale` (read-modify-write in place, skip on failure). Each is documented in its own ADR; there is
  no unifying "attribute-mutation" trait, since each one's failure/skip semantics differ enough that
  a shared abstraction would need per-variant escape hatches anyway.
