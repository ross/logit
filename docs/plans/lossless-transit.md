---
created: 2026-09-10
updated: 2026-09-11
---

# Closing plan: lossless like-protocol transit

## Context

[ADR `lossless-transit`](../adr/lossless-transit.md) states the goal: `statsd_in -> statsd_out`,
`otlp_in -> otlp_out`, and `syslog_in -> syslog_out` should each be a transparent relay, modulo a
named list of permitted normalizations, with any remaining loss tracked as debt rather than quietly
accepted. [`docs/design/telemetry-landscape.md`](../design/telemetry-landscape.md) surveys what
every protocol in scope can express. This plan assesses today's model and codecs against that
survey, proposes a target model, and orders the workstreams that close the gap.

## Decisions already settled

- Pre-release, no backward compatibility: `MetricKind`, the native wire format, and config are
  reshaped in place. No version negotiation, dual-read paths, or decode shims kept for compatibility's
  sake — the native codec's existing skip-unknown framing is retained because it's good hygiene, not
  because anything needs to read an old frame.
- statsd timers keep raw samples until an `aggregate` stage explicitly sketches them — a decoder
  never pre-summarizes.
- Counters: a single `Sum { value, temporality, monotonic }` representation, with a
  `MetricKind::counter(v)` constructor for the common delta-monotonic case. No separate `Counter`
  variant.
- DogStatsD events (`_e{}`) and service checks (`_sc`) are in scope, as a trailing workstream.
- `Event` growing from 800 to roughly 856 bytes to carry the new fields is accepted; the exact-size
  test and `docs/design/memory.md`'s table are updated in the same PR that changes the shape.

## Assessment: today's model and codecs against the survey

### The model today

`crates/logit-core/src/metric.rs`: `MetricRecord { name: Symbol, kind: MetricKind, unit:
Option<Symbol> }`; `MetricKind = Counter(f64) | Gauge(f64) | GaugeDelta(f64) | Set(HyperLogLog) |
Distribution(DdSketch) | Histogram { buckets: Vec<(f64, u64)> } | Summary { quantiles: Vec<(f64,
f64)> }`. No temporality, no monotonicity, no start time, no exemplars, no description — nothing
beyond what statsd and a merged sketch need. `crates/logit-core/src/lib.rs`'s `LogRecord` has no
`event_name` or `observed_timestamp`. `crates/logit-core/src/span.rs`'s `SpanRecord`/`SpanLink` have
no `flags` or `trace_state`; status message rides an `otel.status_message` attribute instead of a
field. `EventBatch` (`crates/logit-core/src/event.rs`) has a `resource: Arc<Resource>` but no scope.

### statsd_in -> statsd_out

Decode (`crates/logit-inputs/src/statsd.rs`, `build_event` ~L254-345): raw timer samples are
sketched into a `DdSketch` immediately (`add_weighted`, ~L301-327, weight clamped at
`MAX_SAMPLE_WEIGHT = 1000`) — a decoder pre-summarizing, which the ADR now forbids; `s` sets are a
hard decode error (~L329-334, `HyperLogLog` is still a stub per `docs/known-gaps.md`); `|c:`
container id and `|T` timestamp are silently accepted and ignored by the generic segment fallthrough
(~L229-231); DogStatsD events and service checks fail to parse as ordinary lines; `ms`/`h`/`d` all
collapse into one `Distribution` kind, losing which wire type produced it; `unit` is always `None`.

Encode (`crates/logit-outputs/src/statsd.rs`, `render_metric` ~L350): only `Counter` and
`Gauge`/`GaugeDelta` are emitted; `Distribution`/`Set`/`Histogram`/`Summary` are dropped whole and
counted (`unsupported_metric_kind`, ~L445-464) — this is the v1 deferral [ADR `statsd-output`](../adr/statsd-output.md)
named explicitly and left for "its own ADR once there's a concrete consumer." `GaugeDelta` is only
emitted under the opt-in `relative_gauges: true` — correct as-is, since the default guards against a
misconfigured pipeline missing `aggregate`, and the opt-in *is* the lossless path (kept unchanged by
this plan). No `@rate`, no `|T`, no unit, ever emitted.

Preserved end to end today: metric names, counters (sample-rate-extrapolated — a permitted
normalization), absolute gauges including negative values via the documented two-line `0|g`/`-n|g`
idiom, tags including bare valueless tags, and relative gauges when opted in. Seven real-decoder
relay tests already exist (`crates/logit-outputs/src/statsd.rs` ~L1400-1500); none exercise timers,
sets, `|T`, or `|c:`, because none of those currently survive to test.

### otlp_in -> otlp_out

Metrics (`crates/logit-proto/src/otlp/metrics.rs`): a cumulative `Sum` decodes to `Gauge` plus an
`otel.temporality = "cumulative"` attribute (~L437-441) rather than a real temporality field, and
its `is_monotonic` flag is lost entirely; `start_time_unix_nano` is never read on decode and encode
always stamps both start and point time with `Event::timestamp` (module doc, ~L8); `description`
and `metadata` are parsed off the wire but never read (`decode_metric` only consumes `name`/`unit`/
`data`, ~L411-414); `Histogram`'s `sum`/`min`/`max` are dropped both directions; `ExponentialHistogram`
decodes into explicit `Histogram{buckets}` with materialized bounds (~L488+, capped at
`MAX_DERIVED_BUCKETS = 512`) — exact for the ranges reported, but re-encoding produces a different
wire type, not the original one, so `otlp_in -> otlp_out` is not a fixed point for this metric type
today; `Summary`'s `count`/`sum` are dropped on both sides (~L178-194 encode, ~L519 decode);
exemplars are never read or written on any data point type; `NumberDataPoint`'s int/double
distinction collapses to `f64` (`number_value`); a `NO_RECORDED_VALUE`-flagged point is skipped
rather than round-tripped as a flagged point (permitted under the ADR's cross-protocol rule, but
worth closing since it's like-to-like here); scope name/version are decoded into `otel.scope.name`/
`otel.scope.version` **event attributes** (`crates/logit-proto/src/otlp/common.rs` ~L133) and then
re-encoded as ordinary data-point attributes under a hardcoded `{name: "logit", version:
CARGO_PKG_VERSION}` scope stamped on every encode (~L121) — a real identity loss, not a permitted
regroup; scope-level attributes, both levels' `dropped_attributes_count`, and `schema_url` are
dropped throughout. One `MetricRecord` becomes exactly one wire `Metric` with one data point
(module doc ~L11-14) — spec-legal and a permitted regroup, not a gap.

Logs (`crates/logit-proto/src/otlp/logs.rs`): severity encodes to only each band's base value and
decodes losing the original numeric distinction within a band (`INFO2` round-trips as plain `INFO`,
~L3-8, ~L56); `severity_text` is regenerated from the normalized variant's name rather than
preserved; `event_name` and `dropped_attributes_count` are parsed but dropped (~L47); `observed_time`
is unconditionally re-stamped with wall-clock now on encode (~L152) rather than preserved when the
decoded value was already non-zero, which is more aggressive than the ADR's "the relay is the
observer" permission requires; `body_format` correctly rides `logit.body_format` and should keep
doing so (a `logit`-only concept per the ADR's rule (c)); a log-and-span `Event` shatters into two
separate wire records and decodes back as two separate `Event`s
(`crates/logit-bench/tests/wire_format_bakeoff.rs:247`) — permitted regrouping, but the resulting
log gains a `trace: Some(..)` it didn't decode with, worth a one-line note in the codec's module doc
so it isn't mistaken for a bug later.

Traces (`crates/logit-proto/src/otlp/traces.rs`): `trace_state` and `flags` on both `Span` and
`Span.Link`, plus every `dropped_*_count`, are parsed and discarded (~L15-16); status message rides
`otel.status_message` (should become a field per the ADR's rule (a), since two record types — span
and, if it ever needs one, a future summary — would otherwise fight over the same reserved
attribute name).

Value fidelity (`crates/logit-proto/src/otlp/common.rs`): `Value::U64`/`Value::Timestamp` collapse
to `I64` on encode — irrelevant for a pure `otlp_in -> otlp_out` relay (OTLP itself has no source of
either), so this stays in the cross-protocol table, not the like-to-like assessment.

The integration test (`crates/logit-cli/tests/otlp_round_trip.rs`, `assert_round_tripped` ~L100)
only checks that a log, a metric, and a span each exist with roughly the right shape — no per-field
metric-kind assertion exists today, so none of the losses above are currently caught by CI.

**W4 outcome: every named loss above is closed, not just narrowed.**

Metrics: `start_time_unix_nano` ↔ `record.start_timestamp` both ways; `description` interns on
decode and resolves on encode; `Histogram.sum`/`min`/`max` and `Summary.count`/`sum` are real
fields both directions; `ExponentialHistogram` keeps its own variant and maps 1:1 (no more explicit-
bound materialization, no `MAX_DERIVED_BUCKETS` cap — the codec's own decode-side skip path is down
to one case, a `Metric` whose `data` oneof isn't set at all); exemplars decode/encode on every kind
that carries them on the wire (`Sum`/`Gauge`/`Histogram`/`ExponentialHistogram`) — `Summary`
genuinely has none on the wire at all, which stays a real gap, moved into
`docs/known-gaps.md`'s cross-protocol table as its own row rather than living here; a
`NumberDataPoint`'s int/double distinction still collapses to `f64` unchanged (a structural choice,
out of this workstream's scope, not a regression); a `NO_RECORDED_VALUE`-flagged point round-trips
flagged (`MetricRecord.flags`, `metrics-model-v2`'s W4 amendment) instead of being skipped. Scope
name/version/attributes, both levels' `dropped_attributes_count`, and `schema_url` are real fields
now (`EventBatch.scope`), grouped by `(Resource*, Scope*)` pair on decode instead of collapsing into
`otel.scope.*` event attributes — and `otlp_out`'s hardcoded `{name: "logit", version:
CARGO_PKG_VERSION}` fallback is gone: a batch with `scope: None` encodes an empty
`InstrumentationScope` instead. `internal` (`crates/logit-inputs/src/internal.rs`) is the one real
producer of the `"logit"`/version identity now, stamping a genuine `Scope` on every batch it sends
rather than the codec inventing one on `otlp_out`'s behalf (`docs/design/internal-telemetry.md`).
One `MetricRecord` per wire `Metric` with one data point remains a permitted regroup, unchanged.

Logs: `otel.severity_number`/`otel.severity_text` carry the raw wire value alongside the normalized
`Severity`, the same precedence rule `syslog.severity` already set — `INFO2` no longer collapses to
an indistinguishable `INFO` on a round trip. `event_name` and `dropped_attributes_count` are real
fields. `observed_time_unix_nano` is preserved when the decoded value was already non-zero and only
falls back to wall-clock `now` when it was unset — what makes `otlp_in -> otlp_out` a fixed point on
this field too. `body_format` stays on `logit.body_format`, unchanged, per the ADR's rule (c). The
log-and-span `Event` split/enrichment behavior is unchanged and documented in `otlp/logs.rs`'s own
module doc.

Traces: `trace_state`/`flags` and every `dropped_*_count` on `Span`/`Span.Link` are real fields now
(`SpanRecord.ext`/`SpanLink`), not parsed-and-discarded. Status message rides
`SpanRecord.ext.status_message`, a real field — `otel.status_message` is retired, so it no longer
risks being fought over by a second record type that might someday want the same reserved attribute
name, the concern the original assessment raised.

Value fidelity (`Value::U64`/`Value::Timestamp` collapsing to `I64`) is unchanged, as expected — it
was already filed in the cross-protocol table, not this like-to-like assessment, since OTLP itself
has no source of either.

The integration test is rewritten too: `otlp_round_trip.rs`'s `assert_round_tripped` is now per-
field `assert_eq!` (whole-`Event`/`EventBatch` equality via the `PartialEq` derives
`metrics-model-v2` added), covering `Some(scope)`, a populated `SpanExt`, exemplars, and a flagged
point. A new pure-codec `crates/logit-proto/tests/otlp_fixed_point.rs` checks
`decode_signal(encode_signals(b)) == vec![b]` and wire-level idempotence directly against
`OtlpEncoder`/`OtlpDecoder`, with no pipeline, transform, or transport in between, plus a
`proptest`-based `decode(encode(x)) == x` suite generating arbitrary `MetricRecord`s
(`otlp/metrics.rs`).

### syslog_in -> syslog_out

Decode (`crates/logit-inputs/src/syslog.rs`): RFC 5424 STRUCTURED-DATA is parsed only far enough to
be balanced and skipped (`skip_structured_data`, ~L529-565) — its contents never reach an attribute
at all. A non-numeric PROCID is dropped because `syslog.pid` is typed `Value::U64` (~L623-631). A
non-UTF-8 MSG is a rejected line rather than a `Value::Bytes` emission. Preserved: `syslog.facility`/
`.severity` at full 8-level fidelity (~L475-476/L589-590), hostname, tag/app-name, pid (when
numeric), and 5424's msgid.

Encode (`crates/logit-outputs/src/syslog.rs`): STRUCTURED-DATA is always emitted as the NILVALUE `-`
(~L365-368) regardless of what `syslog.sd` — which doesn't exist yet — or any other attribute might
carry, so a `syslog_in -> json -> syslog_out` relay is strictly less than byte-for-byte even before
considering `json`'s own additions; `syslog.timestamp` is parsed on decode but never consulted on
encode, which always stamps `event.timestamp` (receipt time) instead (~L20-30) — the single largest
named gap in `docs/known-gaps.md`'s syslog section. Output dialect (3164 vs. 5424) is a sink
configuration choice independent of the input's dialect — a permitted normalization, not a bug.
`syslog.severity` already outranks `log.severity` on encode (~L327-342), which is exactly the
pattern the ADR generalizes to OTLP severity.

Only one relay test exists (`crates/logit-outputs/src/syslog.rs:1161`, a single 3164-in/5424-out
hop asserting facility and hostname); no 5424-in/5424-out test, no msgid/procid/timestamp coverage,
and no integration-level test through real sockets.

### Native `logit_in -> logit_out`

Already the lossless path by design — every `MetricKind` variant round-trips including the
`DdSketch` via its `to_java_bytes`/`from_java_bytes` blob (`crates/logit-proto/src/native/record.rs`),
and it's the only pair with a real fidelity gate (`crates/logit-cli/tests/logit_round_trip.rs`,
`crates/logit-bench/tests/wire_format_bakeoff.rs:88-110`). Every model addition below has to land
here too, since `file_out format: native` and `buffer.disk:` both ride this same codec.

### Cross-protocol (best-effort, stays best-effort)

`docs/known-gaps.md`'s existing "Cross-protocol semantic gaps" table (`Distribution`→OTLP
`Summary`, `Set`→skip, `U64`/`Timestamp`→`I64`) is exactly what ADR `lossless-transit`'s "cross-
protocol egress stays best-effort" clause is for — it stays, relabeled as intentional degradation
rather than an open question, and gains one new row once `Samples` exists: a raw sample list has no
OTLP wire type either, so `otlp_out` sketches it first and counts the degradation the same way it
already does for a merged `Distribution`.

## Target model

```rust
// crates/logit-core/src/metric.rs
pub enum Temporality { Delta, Cumulative }

pub enum MetricKind {
    Sum(Sum),                            // replaces Counter; MetricKind::counter(v) = Sum{v, Delta, true}
    Gauge(f64),
    GaugeDelta(f64),                      // unchanged -- ADR relative-gauge-adjustments
    Samples(Samples),                     // raw observations, as statsd ms/h/d arrive
    Distribution(DdSketch),               // produced only by aggregate
    SetMembers(Vec<bytes::Bytes>),        // raw members, as statsd s arrives
    Set(HyperLogLog),                     // produced only by aggregate
    Histogram(Histogram),                 // explicit bounds
    ExponentialHistogram(ExpHistogram),   // OTLP/Prometheus-native shape -- kept distinct so
                                           // OTLP -> OTLP is a fixed point, not a lossy conversion
    Summary(Summary),
}

pub struct Sum { pub value: f64, pub temporality: Temporality, pub monotonic: bool }

/// Sized to match `DdSketch`'s footprint so `MetricKind` doesn't grow past 176 bytes --
/// confirm the exact inline capacity against `size_of::<DdSketch>()` when implementing.
pub struct Samples { pub values: SmallVec<[f64; N]>, pub sample_rate: f64 }

pub struct Histogram {
    pub buckets: Vec<(f64, u64)>, pub temporality: Temporality,
    pub sum: Option<f64>, pub min: Option<f64>, pub max: Option<f64>,
}
pub struct ExpHistogram {
    pub scale: i32, pub zero_count: u64, pub zero_threshold: f64,
    pub positive: (i32, Vec<u64>), pub negative: (i32, Vec<u64>),
    pub temporality: Temporality, pub count: u64,
    pub sum: Option<f64>, pub min: Option<f64>, pub max: Option<f64>,
}
pub struct Summary { pub quantiles: Vec<(f64, f64)>, pub count: u64, pub sum: f64 }

pub struct MetricRecord {
    pub name: Symbol, pub unit: Option<Symbol>, pub description: Option<Symbol>,
    pub start_timestamp: i64,      // 0 = unknown, OTLP's own convention -- avoids Option<i64>
    pub exemplars: Vec<Exemplar>,  // empty Vec allocates nothing on the common path
    pub kind: MetricKind,
}
pub struct Exemplar {
    pub timestamp: i64, pub value: f64, pub trace: Option<TraceRef>,
    pub filtered_attributes: AttrMap,
}

// crates/logit-core/src/lib.rs
pub struct LogRecord {
    /* existing fields */
    pub event_name: Option<Symbol>,
    pub observed_timestamp: i64,   // 0 = unset
    pub dropped_attributes_count: u32,
}

// crates/logit-core/src/span.rs
pub struct SpanRecord {
    /* existing fields */
    pub flags: u32,
    pub ext: Option<Box<SpanExt>>, // boxed: populated only on an error span or one with tracestate
}
pub struct SpanExt {
    pub status_message: Option<bytes::Bytes>, pub trace_state: Option<bytes::Bytes>,
    pub dropped_attributes_count: u32, pub dropped_events_count: u32, pub dropped_links_count: u32,
}
// SpanLink += flags: u32, trace_state: Option<Bytes>, dropped_attributes_count: u32
// SpanEvent += dropped_attributes_count: u32

// crates/logit-core/src/event.rs / resource.rs
pub struct EventBatch { pub resource: Arc<Resource>, pub scope: Option<Arc<Scope>>, pub events: Vec<Event> }
pub struct Scope {
    pub name: bytes::Bytes, pub version: bytes::Bytes, pub attributes: AttrMap,
    pub dropped_attributes_count: u32, pub schema_url: Option<bytes::Bytes>,
}
// Resource += dropped_attributes_count: u32, schema_url: Option<Bytes>
```

Expected sizes (confirm against `crates/logit-core/tests/type_sizes.rs` when implementing):
`MetricKind` stays 176 bytes (every new variant fits under `Distribution`'s existing footprint);
`MetricRecord` grows to roughly 224; `Event` to roughly 856. `ExponentialHistogram` is kept as its
own variant rather than always materializing explicit buckets on decode, specifically so
`otlp_in -> otlp_out` is a fixed point for this type — `aggregate` may still choose to convert one to
`Distribution` when summarizing.

**W1 outcome (measured, not estimated — [ADR `metrics-model-v2`](../adr/metrics-model-v2.md)):**
every shape above landed as designed, confirmed against `crates/logit-core/tests/type_sizes.rs`
rather than left at the estimates above. `MetricKind` held exactly at 176 bytes, but only once
`SAMPLES_INLINE` was picked correctly: `size_of::<DdSketch>()` measures 176 (confirming the
assumption this target model was written against), and `Samples` had to be sized to fit *under*
that footprint rather than at it — `SAMPLES_INLINE = 20` (a `Samples` at exactly 176 bytes too)
forces a real discriminant on top and pushes `MetricKind` to 184; `SAMPLES_INLINE = 19` leaves
`size_of::<Samples>() == 168`, just enough slack for the discriminant to land inside the existing
envelope. `MetricRecord` measured exactly 224 (`MetricList` 232); `LogRecord` grew 72 → 88;
`SpanRecord` grew 136 → 144. `Event` landed at 864 — 8 bytes over this plan's pre-implementation
"~856" estimate, and the actual sum of the measured per-field deltas (`+16` `LogRecord`, `+40`
`MetricList`, `+8` `SpanRecord`, over the 800-byte baseline) accounts for the full 64-byte growth
exactly; ~856 was simply an approximation made before any of the four record types had been
implemented and measured. See [`docs/design/memory.md`](../design/memory.md) §1 for the full,
current term-by-term breakdown.

### Attribute conventions (additions to `docs/design/data-model.md`'s well-known attribute table)

- `otel.severity_number` (`Value::I64`, 1-24), `otel.severity_text` (`Value::Str`): stamped by
  `otlp_in`, outrank the normalized `Severity` on `otlp_out` — the same rule `syslog.severity`
  already follows. **Landed in W4** (`docs/design/data-model.md`'s well-known attribute table).
- `otel.scope.*` attributes are **retired** — scope moves to `EventBatch::scope`. `otlp_in` produces
  one batch per distinct `(resource, scope)` pair instead of folding scope into event attributes;
  `otlp_out` groups outgoing events by the same key. `otel.temporality` and `otel.status_message`
  are retired once `Sum.temporality` and `SpanExt.status_message` exist as real fields. **Landed**
  — `otel.temporality` in W1, `otel.scope.*`/`otel.status_message` in W4.
- `statsd.container_id` (`Value::Str`) carries `|c:<id>` both ways. `|T<ts>` sets
  `Event::timestamp` directly and stamps a `statsd.timestamp: true` marker attribute so
  `statsd_out` knows to re-emit `|T` on that specific line (a per-line marker, not a sink-wide
  switch, since real DogStatsD traffic mixes timestamped and untimestamped points).
- DogStatsD events decode to a `LogRecord` (`message` = event text, severity derived from
  `alert_type`) plus `statsd.event.{title,priority,alert_type,aggregation_key,source_type,host}`
  attributes; service checks decode to `Gauge(status)` plus
  `statsd.service_check.{name,message,host}`. `statsd_out` re-encodes either shape when it finds
  the marker attributes present.
- `syslog.sd`: `Value::Map { "<SD-ID>" -> Value::Map { "<PARAM-NAME>" -> Value::Str |
  Value::Array<Value::Str> } }` — nested rather than flattened, because RFC 5424's `SD-NAME` grammar
  permits `.`, which would make a flattened `syslog.sd.<id>.<param>` key ambiguous to reassemble;
  nesting also interns one bounded key (`syslog.sd`) instead of interning attacker-chosen SD-ID/
  PARAM-NAME strings directly. A repeated PARAM-NAME becomes an `Array`. `syslog_out` re-emits every
  element with the same `"`/`\`/`]` escaping the RFC requires.
- A non-`syslog.*` attribute reaching `syslog_out` (the `syslog_in -> json -> syslog_out` gap) is
  carried only when the operator opts in via `structured_data: { sd_id: "<name>@<PEN>" }` — **no
  default private enterprise number is shipped**; `32473` in RFC 5424's own examples is
  documentation-only, and picking a real one (registering with IANA, or an operator supplying their
  own) is a decision for whoever turns this on, not something to default silently.
- `syslog.timestamp` precedence on `syslog_out`, per event: a `Value::Timestamp` (resolved 5424
  input) renders directly; a `Value::Str` (the raw, unresolvable 3164 token) renders verbatim when
  the *output* format is also 3164, and falls through to `event.timestamp` when the output is 5424
  (no year/timezone to construct an RFC 3339 stamp from); a `Value::Null` (a nil `-` TIMESTAMP,
  which `parse_5424` should now stamp explicitly rather than leaving the attribute simply absent)
  renders as `-`; an absent attribute falls through to `event.timestamp` exactly as today.
  `event.timestamp` itself stays receipt time — the opt-in `syslog_timestamp` transform already
  sketched in `docs/known-gaps.md` remains the correct place to resolve it deliberately.
- `syslog.pid` becomes `Value::Str` when PROCID doesn't parse as a number, rather than being
  dropped; stays `Value::U64` when it does.
- A non-UTF-8 syslog MSG decodes to a `Value::Bytes` message instead of rejecting the line; header
  fields are parsed off the raw bytes before UTF-8 validation is applied to the MSG slice alone.

### `aggregate`

`crates/logit-transforms/src/aggregate.rs` gains the sketching step statsd's decoder does today:
a `Samples` accumulator opens a `Distribution` and calls `add_weighted(v, (1.0 / rate).round().max(1.0).min(MAX_SAMPLE_WEIGHT))`
per value — the clamp and its diagnostic move here from `crates/logit-inputs/src/statsd.rs` verbatim.
A new `distributions: sketch | samples` config (default `sketch`) lets an operator keep raw samples
through the aggregation window too (concatenated, bounded by a `max_samples_per_series` cap;
overflow falls back to sketching and counts it) — this is what makes a
`statsd_in -> aggregate -> statsd_out` relay stay exact when the operator asks for it, rather than
only when `aggregate` is entirely absent. `SetMembers` merges as an exact bounded union (or into a
real `HyperLogLog`, wiring the still-stubbed crate the same PR needs anyway). Cumulative `Sum`,
`Histogram`, and `ExponentialHistogram` pass through unmerged, following the existing "no defined
merge rule → pass through" pattern for `Set`/`Histogram`/`Summary`.

### Sinks

`statsd_out` encodes `Samples` as `name:v1:v2|ms|@rate` (multi-value under `format: dogstatsd`, one
line per value under `format: statsd`, which has no multi-value grammar) and `SetMembers` as
`name:m|s`, one line per member. `Distribution`/`Set`/`Histogram`/`Summary`/`ExponentialHistogram`
reaching `statsd_out` remain the deferred cross-kind question ADR `statsd-output` already named —
now reachable only after an operator has explicitly asked `aggregate` to sketch, which is exactly
the ADR's "opt-in summarization" carve-out; `statsd-output` gets an amendment narrowing its v1
deferral to that case specifically. `otlp_out` encodes everything in the target model exactly
except `Samples`, which it sketches first and counts as a degradation (the one new cross-protocol
table row).

### Native codec

`crates/logit-proto/src/native/record.rs`: every model addition above becomes a new `tag + len +
payload` field or kind, following the skip-unknown TLV framing `Event`'s own fields already use.
Reshaped in place — no version negotiation, no dual decode path, per the "pre-release" decision
above. `crates/logit-bench/src/bakeoff/wire_mirror.rs`'s mirror type, `crates/logit-proto/tests/
robustness.rs`'s mutation suite, and `DiskQueue`'s spooled records all follow the same shape change.

### Tests

`crates/logit-cli/tests/{statsd,syslog}_round_trip.rs`, alongside the existing
`otlp_round_trip.rs` (the only crate depending on both `logit-inputs` and `logit-outputs`). Two
layers per pair: a pure-codec fixed-point check — `decode(encode(d)) == d` and
`encode(decode(encode(d))) == encode(d)`, which needs `PartialEq` on `Event` and the record types
(`DdSketch` compared via `to_java_bytes`) — generated by `proptest` (new dev-dependency; check
`deny.toml`) over each protocol's grammar; and a fixture corpus under `crates/logit-cli/tests/
fixtures/{statsd,syslog,otlp}/` drawn from the DogStatsD docs' own examples, RFC 5424 §6.5's
examples, and the nginx lines already in `crates/logit-bench/src/fixtures.rs`, with each fixture's
expected output written out explicitly (input modulo the ADR's permitted normalizations). Plus the
existing socket-level pattern extended to `statsd_in -> aggregate(samples) -> statsd_out` and
`syslog_in -> syslog_out`, and `otlp_round_trip.rs`'s `assert_round_tripped` rewritten to check
metric-kind fields, not just presence.

## Workstream ordering

| # | Work | Size | Depends on |
|---|---|---|---|
| W0 | This PR: ADR, survey, and this plan | S | — |
| W1 | **Landed (this PR).** Core model reshape (every type in "Target model" above), `PartialEq` derives, `type_sizes.rs` + `memory.md` §1, `estimated_heap_bytes`, and every exhaustive match site updated (`event.rs`, `outputs/{influxdb,stdio,statsd}.rs`, `proto/native/record.rs`, `proto/otlp/metrics.rs`, `transforms/aggregate.rs`, `bench/bakeoff/wire_mirror.rs`) — plus the native codec reshape in the same PR, since `record.rs` can't compile against the old model otherwise. New ADR `metrics-model-v2` (single `Sum`, raw-vs-sketch pairs for `Samples`/`SetMembers`, the `ExponentialHistogram` variant, boxed `SpanExt`, batch-level `Scope`); amends `relative-gauge-adjustments` (its recorded size-growth fallback is not triggered — `MetricKind` stays 176) | L | W0 |
| W2 | `aggregate`: `Samples` sketching moved out of decode, `distributions: sketch \| samples` config, `SetMembers` union plus a real `HyperLogLog`, cumulative-kind pass-through; amends `aggregation-window-semantics` | M | W1 |
| W3 | statsd pair: `Samples`/`SetMembers` in and out, `|c:`, `|T`, sample-rate retention on timers, `statsd_round_trip.rs`, updated `allocations.rs` cases; amends `statsd-output` (v1 deferral narrowed to post-sketch kinds; "no sample rate/timestamp" reversed) | M | W1, W2 |
| W4 | **Landed.** OTLP pair: start_time, description, exemplars, `NO_RECORDED_VALUE` round-tripped as a flagged point, batch-level scope grouping + `schema_url`, `event_name`, `observed_timestamp`, dropped-attribute counts, span fields, `otel.severity_*`; `otlp_round_trip.rs` rewritten to per-field assertions; new pure-codec `crates/logit-proto/tests/otlp_fixed_point.rs` plus a `proptest`-based `decode(encode(x)) == x` suite in `otlp/metrics.rs`; `internal` stamps a real `Scope` (`crates/logit-inputs/src/internal.rs`) now that `otlp_out` no longer invents one. New `MetricRecord.flags: u32`/`MR_FLAGS` native tag amends `metrics-model-v2`. (`Sum`/temporality/monotonic, `ExponentialHistogram`'s 1:1 mapping, and histogram sum/min/max + summary count/sum were pulled forward into W1 — see its "W1 outcome" note above.) | L | W1 |
| W5 | syslog pair: structured-data parse and emit, timestamp precedence and the nil case, `Value::Bytes` MSG, `Value::Str` PROCID, opt-in PEN-qualified structured-data element; `syslog_round_trip.rs`; new ADR `syslog-structured-data-convention`; amends `syslog-output` | M | W0 (parallel with W1) |
| W6 | DogStatsD events and service checks, in and out | S | W3 |
| W7 | Expose the new fields through the Lua proxy (`docs/design/lua-api.md`) — otherwise the model is lossless but the scripting surface can't see any of it | M | W1 |
| W8 | Closeout: rewrite or remove the `docs/known-gaps.md` entries each workstream closes, rewrite `docs/design/data-model.md`'s metric-kinds section for the new shapes, update `AGENTS.md`'s current-state paragraph | S | all |

Landing order: W0 → W1 → (W2, W4, W5 in parallel) → W3 → W6 → W7 → W8. Each workstream is its own
PR.

## Verification

This PR (W0) is documentation only:

- No code changes; existing CI is unaffected.
- Every relative link in the new and edited docs resolves to a real file.
- `docs/adr/README.md` and `docs/plans/README.md` carry the new rows, ordered by `created` as the
  existing convention requires.
- The new ADR's headings match `docs/adr/TEMPLATE.md` exactly.
- Every `file:line` citation in this plan's Assessment section was checked against the source in
  this session; every protocol claim in the survey was checked against the cited spec or reference
  implementation, or is marked as not independently verifiable where it wasn't.

Later workstreams (W1 onward) each verify against `cargo test`/`cargo clippy` per
[`AGENTS.md`](../../AGENTS.md)'s workflow, plus the exact-size and allocation-count assertions this
plan calls out, plus the new round-trip test suites this plan adds.
