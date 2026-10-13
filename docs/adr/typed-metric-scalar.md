---
created: 2026-10-13
updated: 2026-10-13
---

# Typed metric scalar: `MetricValue { F64, I64, U64, Bool }` replaces the `f64` in `Gauge`, `GaugeDelta`, and `Sum`

## Status
Accepted

## Context

Every scalar metric value in the model is a bare `f64`: `MetricKind::Gauge(f64)`,
`MetricKind::GaugeDelta(f64)`, and `Sum.value: f64` (`crates/logit-core/src/metric.rs`). Three
wire protocols `logit` relays carry typed scalars, and the `f64` collapses each of them:

- **OTLP.** A `NumberDataPoint` is `oneof { as_double, as_int }`. `otlp_in` reads `as_int` as
  `i as f64` and `otlp_out` always writes `as_double`, so an `otlp_in -> otlp_out` relay turns
  every integer point into a double, and an integer above 2^53 into a different number.
- **collectd.** `COUNTER` and `ABSOLUTE` are `u64`, `DERIVE` is `i64`. Above 2^53 the decoded
  `Sum.value` isn't the sender's number (`docs/known-gaps/mappings.md`'s `decode (collectd)`
  precision row, which already names "a typed integer metric value" as the core-model question).
- **InfluxDB line protocol.** A field is `1.0`, `1i`, `1u`, `"text"`, or `true`, and InfluxDB
  rejects a write whose field type conflicts with what the series already stores. A relay that
  turns `1i` into `1` changes the series' type at the destination.

[ADR `lossless-transit`](lossless-transit.md) treats a field a protocol carries and `Event` can't
represent as model debt, not an accepted codec limitation. The InfluxDB line-protocol pair
([ADR `influxdb-line-protocol-relay`](influxdb-line-protocol-relay.md)) can't be a fixed point
without typed scalars, and fixing the type for InfluxDB alone would leave the OTLP and collectd
collapse in place. The fix belongs in the model, once, for every protocol.

## Decision

**A new `MetricValue { F64(f64), I64(i64), U64(u64), Bool(bool) }`, `Copy`, replaces the `f64` in
`MetricKind::Gauge`, `MetricKind::GaugeDelta`, and `Sum.value`.** It's general: no variant exists
for one protocol, and every decoder whose wire distinguishes the types produces the matching
variant.

- **Equality is strict.** `PartialEq` compares the variant and the value: `F64(1.0) != I64(1)`.
  A fixed-point test that compares `EventBatch`es therefore catches a relay that changed a type.
- **Decoders.** `otlp_in` reads `as_int` as `I64` and `as_double` as `F64`. collectd reads
  `COUNTER` and `ABSOLUTE` as `U64`, `DERIVE` as `I64`, and `GAUGE` as `F64`. The InfluxDB codec
  reads `1.0` as `F64`, `1i` as `I64`, `1u` as `U64`, and `true`/`false` as `Bool`. Every other
  decoder has an untyped wire and produces `F64`.
- **Out of scope.** `Exemplar.value` stays `f64`, so an OTLP exemplar's `as_int` stays a
  `docs/known-gaps/mappings.md` row. The multi-number kinds (`Samples`, `Distribution`,
  `Histogram`, `Summary`, and the rest) keep their `f64` fields: no protocol `logit` relays types
  their values.

### Egress is always typed

`influxdb_out` writes `I64` as `Ni`, `U64` as `Nu`, and `Bool` as `true`/`false`, for every event,
not only for events relayed from `influxdb_in`, and whether or not the event carries the
line-protocol carrier. `logit` is pre-release and no stored data depends on today's all-float
output.

Every other metric sink has a numeric-only wire: `otlp_out`, `prometheus_out`, `graphite_out`,
`statsd_out`, `collectd_out`, `datadog_out`, `splunk_hec_out`, and `kafka_out` under
`format: otlp_proto | otlp_json`. Each writes an integer in its wire's integer form where the wire
has one (OTLP `as_int`, collectd `COUNTER`/`DERIVE`/`ABSOLUTE`, an integer in text), and `as_f64()`
where it doesn't. Each writes `Bool` as `1` or `0`, counted
`logit.output.metrics.normalized{reason="bool_value"}`. A `U64` above `i64::MAX` has no OTLP
integer form and leaves as `as_double`, counted under `logit.output.metrics.normalized`.

### Arithmetic widens, counted

`aggregate` is the only component that does arithmetic on a metric value. `scale` multiplies
attributes, and `kv_metrics` turns an attribute into a metric value with no arithmetic, keeping the
attribute's numeric type. `aggregate` widens as follows:

| Operands | Result |
|---|---|
| same type | that type, while the result fits |
| `I64` and `F64`, or `U64` and `F64` | `F64` |
| `I64` and `U64` | `I64` if the result fits, else `U64` if it fits, else `F64` |
| any integer overflow | `F64` |
| `Bool` | reads as `0` or `1` |

Every result whose type differs from an input's is counted
`logit.transform.metrics.normalized{reason="widened"}`.

### Lua sees a number

A metric value reads in Lua as a double, which is LuaJIT's only number type. `Bool` reads as `0` or
`1`. A read that can't be exact, an integer above 2^53, is counted
`logit.transform.metrics.normalized{reason="inexact_read"}`.

A write that equals the current value's `as_f64()` is a no-op and keeps the variant, the rule
[ADR `lua-value-identity-preservation`](lua-value-identity-preservation.md) set for attributes
(`lua_value_matches` in `crates/logit-script/src/value.rs`). Any other write stores `F64`.
`Event.new` takes an optional `type = "f64" | "i64" | "u64" | "bool"` beside a metric's value, and
`event:to_table()` emits `type` only for a non-`F64` value, so a float-only script sees no change.

### Native wire: a type byte before the eight value bytes

The native codec writes a `Gauge`, `GaugeDelta`, or `Sum` scalar as `[type: u8][8 bytes LE]`, in
place of today's bare 8-byte `f64`. The type byte is `0` for `F64`, `1` for `I64`, `2` for `U64`,
and `3` for `Bool`, whose eight bytes hold `0` or `1`. A decoder rejects any other type byte as
malformed.

There's no compatibility path for a frame written before this change, no length dispatch and no
version bump: `logit` is pre-release, and [ADR `native-hop-no-compatibility`](native-hop-no-compatibility.md)
already rules out old decode paths on the hop. A disk spool written by an older build doesn't
replay into a newer one. `docs/design/wire-protocol.md` and the bake-off's wire mirror
(`crates/logit-bench/src/bakeoff/wire_mirror.rs`) change with the codec.

## Alternatives considered

- **Keep `f64` and carry the original type as an attribute** (an `otel.value_type` or
  `influxdb.field_type` beside each record). Rejected: a type attribute doesn't restore the
  precision an `f64` already lost above 2^53, so it fixes InfluxDB's type conflict and leaves the
  OTLP and collectd value loss in place. It's also per protocol, which
  [ADR `lossless-transit`](lossless-transit.md)'s rule (a) rejects for a concept two protocols
  share, and it would leak into every other sink's tag set.
- **A `Str` variant, so a line-protocol string field becomes a metric.** Rejected: no
  metric-consuming sink has a wire form for a string value, and `aggregate` can't merge one, so
  every sink and transform would grow a skip arm for a variant only one protocol produces. A string
  field rides as an attribute instead
  ([ADR `influxdb-line-protocol-relay`](influxdb-line-protocol-relay.md)).
- **Expose an integer to Lua as LuaJIT's `int64_t` cdata** (`1LL`). Rejected: cdata integers
  don't mix with Lua numbers the way a script expects. `x + 1` on a cdata integer is a cdata
  integer, `x == 1` compares by identity unless both sides are cdata, and `tostring(x)` appends
  `LL`. Every existing script that compares or formats a metric value would change behavior. The
  no-op-write rule keeps the type through a script that doesn't touch the value, which is the case
  a relay needs, and the counted inexact read makes the remaining loss visible.

## Consequences

- **Closes two existing losses.** An OTLP `as_int` point relays as `as_int`, and a collectd
  `COUNTER` above 2^53 relays as the sender's number. `docs/known-gaps/mappings.md`'s
  `decode (collectd)` precision row closes, and new rows record `Bool` written as `1`/`0` at
  numeric-only sinks and a `U64` above `i64::MAX` leaving OTLP as a double.
- `MetricValue` is expected to be 16 bytes and `Sum` to grow to 24, with `MetricKind` still bound
  by `Samples`'s size.
  `crates/logit-core/tests/type_sizes.rs` gains exact pins for both, and `docs/design/memory.md`
  and `docs/design/data-model.md` change in the same commit. `Copy` means no allocation count
  should move; one that does gets a new constant and a `memory.md` row.
- Every match on `Gauge(`, `GaugeDelta(`, and `Sum {` across the workspace changes. Most sites
  wrap or unwrap `F64` mechanically. `MetricKind::gauge(f64)` beside `MetricKind::counter(f64)`
  keeps those sites short.
- `stdio_out`'s `format: json` prints `I64` and `U64` as JSON integers, amending
  [ADR `stream-json-format`](stream-json-format.md), and `tools/shape-survey/summarize.py`'s
  self-test pins it.
- Amendments follow in the workstream that changes each behavior:
  [ADR `collectd-binary-relay`](collectd-binary-relay.md),
  [ADR `metrics-model-v2`](metrics-model-v2.md),
  [ADR `aggregation-window-semantics`](aggregation-window-semantics.md),
  [ADR `relative-gauge-adjustments`](relative-gauge-adjustments.md),
  [ADR `lua-event-constructor`](lua-event-constructor.md), and
  [ADR `stream-json-format`](stream-json-format.md). `docs/design/internal-telemetry.md` gains the
  `widened`, `inexact_read`, and `bool_value` reasons, and `docs/design/lua-api.md` the `type` key.
- [`docs/plans/influxdb-relay.md`](../plans/influxdb-relay.md)'s `influx/w1` builds this.
