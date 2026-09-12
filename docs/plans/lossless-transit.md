---
created: 2026-09-10
updated: 2026-09-12
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

### statsd_in -> statsd_out (W3, landed)

Decode (`crates/logit-inputs/src/statsd.rs`): `ms`/`h`/`d` lines decode straight to a raw
`MetricKind::Samples` — one `Event` per line, every colon-separated value in `Samples`'s own inline
`SmallVec`, `sample_rate` carried verbatim with no sketching or extrapolation at decode time at
all. The wire type letter survives as `statsd.type` (`ms`/`h`/`d`) since all three land on the same
`Samples` shape. `s` lines decode to `MetricKind::SetMembers`, one event per line, every member a
zero-copy `Bytes` slice of the datagram, mirroring `ms`/`h`/`d`. `statsd_in`'s own copy of the
`MAX_SAMPLE_WEIGHT`/`sample_rate_clamped` clamp (moved to `aggregate` by W2, kept here until this
workstream) is deleted — `aggregate` is now the only place that diagnostic fires. `|c:<id>` stamps
`statsd.container_id`; `|T<secs>` sets `Event::timestamp` and stamps the per-line
`statsd.timestamp: Value::U64(secs)` carrier -- the raw wire seconds themselves, not just a marker
bit, so a stage that rebuilds `Event::timestamp` after decode can't fabricate or collapse a `|T` on
the way back out; a malformed `|T` rejects only that line. DogStatsD events and service checks now
decode and re-encode losslessly too (W6, below). `unit` is still always `None`.

Encode (`crates/logit-outputs/src/statsd.rs`): `Samples` renders as one multi-value
`name:v1:v2|<type>|@rate` line under `format: dogstatsd` (`<type>` from `statsd.type`, defaulting
to `ms`; `@rate` omitted at `1.0`), or one `name:v|ms[|@rate]` line per value under `format: statsd`
(no multi-value grammar there; `h`/`d` normalize to `ms`, counted `type_normalized_dialect`).
`SetMembers` renders one `name:<member>|s` line per member, in both dialects (a member-specific
rule: `:`, `|`, and control bytes are substituted; everything else, including `@`, `#`, `,` and
spaces, is preserved -- counted `members_sanitized` when altered). `|c:`/`|T` round-trip under
`format: dogstatsd` only (`append_dialect_extras`), reading their carriers off `EncodeCtx` --
captured by `build_tag_suffix`'s merged resource⊕event walk, the same one that filters `statsd.*`
out of the generic tag segment, so a carrier set only on the resource is honored too; dropped and
counted (`dropped_dialect_fields`) under `format: statsd`, which has no equivalent segment.
`statsd.*` attributes are filtered out of the generic `|#k:v` tag segment (`build_tag_suffix`),
never re-emitted as tags. `Distribution`/`Set`/
`Histogram`/`ExponentialHistogram`/`Summary`/a cumulative or non-monotonic `Sum` remain dropped and
counted (`unsupported_metric_kind`) — reachable now only once `aggregate` has explicitly summarized
(its defaults, `distributions: sketch`/`sets: estimate`), exactly the "opt-in summarization"
carve-out the ADR names. `GaugeDelta` is still only emitted under the opt-in
`relative_gauges: true`, unchanged by this workstream.

Preserved end to end now: metric names, counters (sample-rate-extrapolated, unchanged), absolute
gauges (including negative values via the documented two-line `0|g`/`-n|g` idiom) and relative
gauges when opted in (both unchanged), tags including bare valueless tags, raw
timers/histograms/distributions (`Samples`, including sample rate), raw sets (`SetMembers`), and
`|c:`/`|T` under `format: dogstatsd`. `docs/adr/statsd-output.md` gained an amendment narrowing the
v1 metric-kind deferral to post-sketch kinds and reversing the "no sample rate, no timestamp"
decision for timers and `|T`-marked lines. `crates/logit-outputs/src/statsd.rs` and
`crates/logit-inputs/src/statsd.rs` both gained real-decoder relay tests for the new kinds and
segments, plus a `proptest` fixed point (`mod fixed_point::decode_encode_decode_is_a_fixed_point`);
`crates/logit-cli/tests/statsd_round_trip.rs` (mirroring `syslog_round_trip.rs`'s real-UDP-socket
harness) extends the same coverage end to end through real sockets.

### DogStatsD events and service checks (W6, landed)

Decode: `_e{tlen,xlen}:title|text|...` and `_sc|name|status|...` lines -- previously rejected
outright as malformed (any `_`-prefixed line fell into the generic "unknown metric type" error) --
now decode to one `Event::log` (an event: `message` is `TEXT` with its `\n` escape unescaped,
`severity` from `t:`, `body_format: Raw`, `event_name: None` on purpose, since a title is free
text an operator chose at send time, not a bounded vocabulary worth interning) or one
`Event::metric` (a service check: `MetricKind::Gauge(status as f64)` under the check's own
interned name), respectively. Every field either grammar carries lands as a `statsd.event.*`/
`statsd.service_check.*` attribute (rule (b), `docs/adr/lossless-transit.md`) rather than a model
field, alongside the same `statsd.timestamp`/`statsd.container_id`/`#tags` handling every metric
line already gets -- `d:<secs>` (not `|T<secs>`) plays the timestamp segment's role on these two
shapes. Encode: `statsd_out` re-emits both in one fixed canonical field order regardless of the
order they arrived in, `d:` from the `statsd.timestamp` carrier (never derived from
`Event::timestamp`), and drops the whole event under `format: statsd` (no `_e`/`_sc` wire form
there at all, counted `dropped_dialect_events`). A service check is read off the event's first
metric, which must be a `Gauge` or the whole event drops (`dropped_invalid_service_check`); an
out-of-set `p:`/`t:` value omits just that one field, counted separately
(`dropped_invalid_event_fields`) since the rest of the line still renders. See
`docs/adr/statsd-output.md`'s "DogStatsD events and service checks" amendment for the full field
order, carrier list, and sanitization rules, and its closing test enumeration for the coverage
landed on both the decode and encode sides plus `crates/logit-cli/tests/statsd_round_trip.rs`.

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

Decode (`crates/logit-inputs/src/syslog.rs`), as this assessment was originally written: RFC 5424
STRUCTURED-DATA was parsed only far enough to be balanced and skipped (`skip_structured_data`) — its
contents never reached an attribute at all. A non-numeric PROCID was dropped because `syslog.pid`
was typed `Value::U64` only. A non-UTF-8 MSG was a rejected line rather than a `Value::Bytes`
emission. Preserved even then: `syslog.facility`/`.severity` at full 8-level fidelity
(`parse_5424`/`parse_3164`), hostname, tag/app-name, pid (when numeric), and 5424's msgid.

Encode (`crates/logit-outputs/src/syslog.rs`), as it stood then: STRUCTURED-DATA was always emitted
as the NILVALUE `-` (`write_structured_data`'s predecessor) regardless of what `syslog.sd` — which
didn't exist yet — or any other attribute might carry, so a `syslog_in -> json -> syslog_out` relay
was strictly less than byte-for-byte even before considering `json`'s own additions;
`syslog.timestamp` was parsed on decode but never consulted on encode, which always stamped
`event.timestamp` (receipt time) instead — the single largest named gap in `docs/known-gaps.md`'s
syslog section at the time. Output dialect (3164 vs. 5424) is a sink configuration choice
independent of the input's dialect — a permitted normalization, not a bug. `syslog.severity`
already outranked `log.severity` on encode (`resolve_severity`), which is exactly the pattern the
ADR generalizes to OTLP severity.

At the time, only one relay test existed (a single 3164-in/5424-out hop asserting facility and
hostname); no 5424-in/5424-out test, no msgid/procid/timestamp coverage, and no integration-level
test through real sockets.

**W5 outcome ([ADR `syslog-structured-data-convention`](../adr/syslog-structured-data-convention.md)):**
landed as designed. `parse_structured_data`/`parse_sd_name`/`parse_param_value`
(`crates/logit-inputs/src/syslog.rs`) replace `skip_structured_data` with a real, quote-aware RFC
5424 §6.3 parser into `syslog.sd`; `write_structured_data`/`write_sd_element`/`write_sd_param`
(`crates/logit-outputs/src/syslog.rs`) are its exact encoder inverse, plus the opt-in
`structured_data: { sd_id }` element for non-`syslog.*` attributes. `syslog.pid` is `Value::Str`
when PROCID/the 3164 bracket isn't numeric; a nil 5424 TIMESTAMP stamps `syslog.timestamp` as
`Value::Null`; a non-UTF-8 MSG decodes to `Value::Bytes` instead of rejecting the line.
`syslog_out`'s TIMESTAMP now follows the precedence table in the new ADR instead of always being
`event.timestamp`. Unit coverage for the new parse/encode paths lands directly in
`crates/logit-inputs/src/syslog.rs`/`crates/logit-outputs/src/syslog.rs`'s own `#[cfg(test)]`
modules; the `crates/logit-cli/tests/syslog_round_trip.rs` integration coverage this assessment
named as missing (5424-in/5424-out and 3164-in/3164-out over real UDP sockets, plus a
`decode(encode(decode(x))) == decode(x)` proptest) is this workstream's Phase B, tracked separately
from this Phase C docs pass.

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

`MetricRecord.flags: u32` (OTLP `DataPointFlags`, bit 0 `FLAG_NO_RECORDED_VALUE`) was added on top
of this shape by W4, filling the 4 bytes of padding already following `name`/`unit`/`description`
so `MetricRecord` stays exactly 224 bytes — see [ADR `metrics-model-v2`](../adr/metrics-model-v2.md)'s
W4 amendment.

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
  permits `.`, which would make a flattened `syslog.sd.<id>.<param>` key ambiguous to reassemble.
  **Correction to an earlier draft of this rationale:** nesting does *not* avoid interning
  attacker-chosen SD-ID/PARAM-NAME strings — `AttrMap::insert` interns every key at every nesting
  depth, so the inner maps intern their own SD-ID/PARAM-NAME keys exactly as if they were top-level
  attribute names. The real reasons for the nested shape are disambiguation (above), a free round
  trip through the native codec (no separate encoding for a nested vs. flattened attribute), and
  Lua ergonomics (`event.attrs["syslog.sd"]["origin"]["ip"]` vs. parsing a dotted key back apart).
  Growth is bounded by RFC 5424's own grammar on parse (1 to 32 PRINTUSASCII bytes per SD-ID/
  PARAM-NAME), not by the nesting — the same interner exposure `json`
  ([ADR `json-parsing-into-attributes`](../adr/json-parsing-into-attributes.md)) already has for an
  arbitrary object's keys. See [ADR `syslog-structured-data-convention`](../adr/syslog-structured-data-convention.md).
  A repeated PARAM-NAME becomes an `Array`. `syslog_out` re-emits every element with the same
  `"`/`\`/`]` escaping the RFC requires.
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

### `aggregate` (W2, landed)

`crates/logit-transforms/src/aggregate.rs` gained the sketching step statsd's decoder used to do:
by default (`distributions: sketch`), an absorbed `Samples` record sketches every value directly
into the series' `DdSketch` via `logit_core::Samples::sketch`'s weighting rule
(`add_weighted(v, weight)`, `weight = round(1/sample_rate)` clamped to `[1, Samples::MAX_WEIGHT]`) —
the clamp and its diagnostic (`sample_rate_clamped`) moved here from
`crates/logit-inputs/src/statsd.rs`, which kept its own copy until W3 deleted it (below). A new
`distributions: sketch | samples` config (default `sketch`) lets an operator keep raw samples
through the aggregation window instead (concatenated, bounded by a `max_samples_per_series` cap;
overflow, or an incoming record's `sample_rate` disagreeing with the series' first one, falls back
to sketching and counts it via `logit.transform.samples.fallback{reason="cap"|"rate_mismatch"}`) —
this is what lets a `statsd_in -> aggregate -> statsd_out` relay stay exact now that W3 has added
the sink side, rather than only when `aggregate` is entirely absent. `SetMembers` merges as an exact,
capped, deduplicated union (`sets: members`, `max_set_members_per_series`, overflow falls back to a
`HyperLogLog` estimate and counts `logit.transform.set_members.fallback{reason="cap"}`) or, by
default (`sets: estimate`), into a real `HyperLogLog` — `crates/logit-core/src/metric.rs` now wraps
the `cardinality-estimator` crate (pinned at `1.0.3`) instead of being a method-less stub. Cumulative
`Sum`, `Histogram`, `ExponentialHistogram`, and `Summary` still pass through unmerged — `Set` no
longer belongs in that list now that `HyperLogLog` is real. `Transform::flush`'s `FlushOutput` also
grew a `scope` field (`Aggregator` now groups by `(resource, scope)` value, not resource alone),
closing the `otlp_in -> aggregate -> otlp_out` scope-loss gap this plan tracked. Full design in
[ADR `aggregation-window-semantics`](../adr/aggregation-window-semantics.md)'s amendment.

### Lua proxy (W7, landed)

`crates/logit-script/src/proxy.rs` gained two new sub-proxies and widened two existing globals so
every field the workstreams above added to the model is reachable from a script, not just present
on the wire ([`docs/design/lua-api.md`](../design/lua-api.md)'s "Reading and writing
`event.metrics`"/"Reading `event.span`"/"Reading and writing `scope`" sections have the full
field-by-field contract; this entry is the shorter "what changed and why" account).

`event.log` widens to `event_name`/`observed_timestamp` (both read/write — an interned string and
a nanos-string respectively, mirroring `event.timestamp`'s own string-not-number rule) and
`dropped_attributes_count` (read-only — a producer-supplied count a script can't meaningfully
change). `event.metrics` is new: an indexable, array-like proxy (`#event.metrics`, 1-based
`event.metrics[i]`) minting a small per-access `MetricProxy` rather than caching one, since a
metric list is typically short and usually read once. Every field on every kind is readable;
**only `sum`/`gauge`'s `value` and `sum`'s `temporality`/`monotonic` are writable** — every other
field, on every kind, stays read-only, including on `distribution`/`set`'s merged state and
`samples`/`set_members`'s raw collections — a script can adjust a counter or a gauge in place, or
rename/retag/re-time any metric (`name`/`unit`/`description`/`start_timestamp` are writable
regardless of kind), but can't mint a sketch or a cardinality estimate by hand, the same "metric
kinds must stay mergeable" rule `AGENTS.md` states for the Rust side of this model. `event.span` is
new too, and entirely read-only — there is still no script-visible way to construct or mutate a
span, only to read one `trace_context`'s `span:` block already minted or a wire codec already
decoded, including its `events`/`links` tables. `resource` and `scope` both gain `schema_url`
(read/write) and `dropped_attributes_count` (read-only); `scope` itself is new, mirroring
`resource`'s copy-on-write shape field for field, including the "a batch may carry none, so a
write starts from `Scope::default()`" case `resource` doesn't need (a batch's resource is never
absent). `crates/logit-pipeline/src/runtime.rs`'s `run_lua` re-stamps the outgoing batch from a
`scope` write the same way it already does for `resource`, including the same flush-time
staleness and script-side override (`docs/known-gaps.md`).

Every new field and error path is covered directly in `crates/logit-script/src/proxy.rs`'s and
`scope.rs`'s own `#[cfg(test)]` modules — no new integration-level suite, since nothing here
changes wire behavior, only what a script already running mid-pipeline can see. No ADR: this
extends the proxy design [`docs/design/lua-api.md`](../design/lua-api.md) already owns rather than
introducing a new one.

### Sinks

`statsd_out` encodes `Samples` as `name:v1:v2|ms|@rate` (multi-value under `format: dogstatsd`, one
line per value under `format: statsd`, which has no multi-value grammar) and `SetMembers` as
`name:m|s`, one line per member. `Distribution`/`Set`/`Histogram`/`Summary`/`ExponentialHistogram`
reaching `statsd_out` remain the deferred cross-kind question ADR `statsd-output` already named —
now reachable only after an operator has explicitly asked `aggregate` to sketch, which is exactly
the ADR's "opt-in summarization" carve-out; `statsd-output` got an amendment (W3) narrowing its v1
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
| W2 | **Landed.** `aggregate`: `Samples` sketching moved out of decode, `distributions: sketch \| samples`/`sets: estimate \| members` config with capped raw retention and a fallback-and-count rule, `SetMembers` union plus a real `HyperLogLog` (`cardinality-estimator`), `FlushOutput` scope, cumulative/`Histogram`/`ExponentialHistogram`/`Summary` pass-through unchanged; amends `aggregation-window-semantics` | M | W1 |
| W3 | **Landed.** statsd pair: `Samples`/`SetMembers` in and out (`statsd_in`'s own `MAX_SAMPLE_WEIGHT`/`sample_rate_clamped` copy, kept alive since W2, deleted), `|c:`/`|T` both ways (dogstatsd only on egress), sample-rate carried verbatim on timers with no decode-time extrapolation, `statsd_round_trip.rs` plus a `proptest` fixed point, updated `allocations.rs` cases; amends `statsd-output` (v1 deferral narrowed to post-sketch kinds; "no sample rate/timestamp" reversed for timers/`|T`) | M | W1, W2 |
| W4 | **Landed.** OTLP pair: start_time, description, exemplars, `NO_RECORDED_VALUE` round-tripped as a flagged point, batch-level scope grouping + `schema_url`, `event_name`, `observed_timestamp`, dropped-attribute counts, span fields, `otel.severity_*`; `otlp_round_trip.rs` rewritten to per-field assertions; new pure-codec `crates/logit-proto/tests/otlp_fixed_point.rs` plus a `proptest`-based `decode(encode(x)) == x` suite in `otlp/metrics.rs`; `internal` stamps a real `Scope` (`crates/logit-inputs/src/internal.rs`) now that `otlp_out` no longer invents one. New `MetricRecord.flags: u32`/`MR_FLAGS` native tag amends `metrics-model-v2`. (`Sum`/temporality/monotonic, `ExponentialHistogram`'s 1:1 mapping, and histogram sum/min/max + summary count/sum were pulled forward into W1 — see its "W1 outcome" note above.) | L | W1 |
| W5 | **Landed.** syslog pair: structured-data parse and emit, timestamp precedence and the nil case, `Value::Bytes` MSG, `Value::Str` PROCID, opt-in PEN-qualified structured-data element; `crates/logit-cli/tests/syslog_round_trip.rs` over real UDP sockets with a fixture corpus plus a `proptest` fixed point; new ADR `syslog-structured-data-convention`; amends `syslog-output` | M | W0 (parallel with W1) |
| W6 | **Landed.** DogStatsD events and service checks, in and out: `_e{...}`/`_sc\|...` decode to `Event::log`/`Event::metric` (`event_name: None` deliberately), `statsd.event.*`/`statsd.service_check.*` carriers, canonical field order and `d:` (not `\|T`) on egress, `format: statsd` whole-event drop, first-metric-is-the-check rule; amends `statsd-output` | S | W3 |
| W7 | **Landed.** Lua proxy: `event.log` gains `event_name`/`observed_timestamp` (read/write) and `dropped_attributes_count` (read-only); new `event.metrics` (array-like, every field readable, `value` writable on `sum`/`gauge`, `temporality`/`monotonic` writable on `sum`, everything else on every other kind read-only) and `event.span` (new, entirely read-only); `resource`/`scope` gain `schema_url` (read/write) and `dropped_attributes_count` (read-only), `scope` itself new, mirroring `resource`'s copy-on-write shape; extends `docs/design/lua-api.md`, no ADR (extension of the existing proxy design, not a new one) | M | W1 |
| W8 | **Landed.** Closeout: `AGENTS.md`'s `statsd_out` and current-state paragraphs rewritten for the landed model, the stale `HyperLogLog` doc comment (`crates/logit-core/src/metric.rs`) and internal-telemetry's raw-sample claims (`docs/design/internal-telemetry.md`, an amendment on ADR `internal-telemetry-as-pipeline-events`) corrected, this plan's closing assessment added, and ADR `lossless-transit`'s Status marked realized — `docs/known-gaps.md`, `docs/design/data-model.md`, and the other docs a prior scoping pass verified already in sync were left alone | S | all |
| W9 | **Landed.** A repeated DogStatsD tag key folds into a `Value::Array` at decode (`insert_tags`, mirroring `syslog_in`'s repeated-PARAM-NAME fold) instead of the last token silently winning; `statsd_out` expands an `Array`-valued attribute into one tag per element, with no dedupe on encode; `influxdb_out` and `prometheus_out` each render a multi-valued tag/label's last representable element, counted `*.{tags,labels}.normalized{reason="multi_value"}`; closes the "repeated DogStatsD tag key collapses to its last value" residual-debt item above; amends `statsd-output` and `lossless-transit` | M | W3, W6 |

Landing order: W0 → W1 → (W2, W4, W5 in parallel) → W3 → W6 → W7 → W8 → W9. Each workstream is its own
PR.

## Closing assessment

Every loss the "Assessment: today's model and codecs against the survey" section above named
against the three like-protocol pairs is closed:

- **statsd_in -> statsd_out**: raw timers/histograms/distributions (`Samples`) and raw sets
  (`SetMembers`) round-trip byte-for-byte under `format: dogstatsd` (under `format: statsd`,
  lossless modulo the ADR's permitted normalizations: multi-value lines split, `h`/`d` normalize to
  `ms`), `|c:`/`|T` survive under `format: dogstatsd`, and DogStatsD events/service checks decode
  and re-encode losslessly — see "statsd_in -> statsd_out
  (W3, landed)" and "DogStatsD events and service checks (W6, landed)" above. A repeated tag key
  round-trips too (W9, landed): `crates/logit-cli/tests/fixtures/statsd/` gained
  `repeated-tag-key-round-trips`, `repeated-tag-three-values`, `bare-and-valued-tag-mix`, and
  `repeated-tag-on-event-line` as byte-for-byte fixtures, plus `repeated-tag-exact-duplicate-deduped`
  and `bare-tag-exact-duplicate-deduped` pinning the agent's own exact-duplicate dedupe rule.
- **otlp_in -> otlp_out**: every metric field the original assessment named (start time,
  description, `Histogram`/`Summary` sum/min/max/count, `ExponentialHistogram` as its own 1:1
  variant instead of materialized buckets, exemplars, a `NO_RECORDED_VALUE` point round-tripped
  flagged, real batch-level scope grouping with `schema_url`/`dropped_attributes_count`), every log
  field (`event_name`, `observed_timestamp` preserved rather than re-stamped, `otel.severity_*`
  outranking the normalized `Severity`), and every span field (`trace_state`/`flags` on
  `Span`/`Span.Link`, every `dropped_*_count`, a real status-message field) are closed — see "W4
  outcome: every named loss above is closed, not just narrowed" above.
- **syslog_in -> syslog_out**: RFC 5424 STRUCTURED-DATA parses and re-emits through `syslog.sd`, a
  non-numeric PROCID survives as `Value::Str`, a non-UTF-8 MSG decodes to `Value::Bytes`, and
  `syslog_out`'s TIMESTAMP follows [ADR `syslog-structured-data-convention`](../adr/syslog-structured-data-convention.md)'s
  precedence table instead of always being receipt time — see "W5 outcome" above.

Each fixed point is proven by a real test suite, not just the workstream narrative above:
`crates/logit-cli/tests/statsd_round_trip.rs`, `crates/logit-cli/tests/syslog_round_trip.rs`, and
`crates/logit-cli/tests/otlp_round_trip.rs` each exercise their pair over real sockets/transports
with per-field equality assertions; `crates/logit-proto/tests/otlp_fixed_point.rs` checks
`decode_signal(encode_signals(b)) == vec![b]` at the pure-codec level with no pipeline, transform,
or transport in between; and the `proptest`-based fixed points in `crates/logit-outputs/src/statsd.rs`
(`mod fixed_point`), `crates/logit-outputs/src/syslog.rs` (`mod fixed_point`), and
`crates/logit-proto/src/otlp/metrics.rs` generate arbitrary records and assert
`decode(encode(x)) == x` holds beyond any hand-picked fixture.

What's left is what the ADR's "cross-protocol egress stays best-effort" clause accepts,
plus a short list of genuine model debt — both already tracked in `docs/known-gaps.md` rather than
newly discovered here:

- `statsd_out` still drops post-sketch metric kinds (`Distribution`/`Set`/`Histogram`/
  `ExponentialHistogram`/`Summary`/a cumulative or non-monotonic `Sum`) — reachable only once
  `aggregate` has explicitly summarized, which is the ADR's own opt-in-summarization carve-out, not
  a like-to-like loss (`docs/known-gaps.md`'s "`statsd_out` drops post-sketch metric kinds" entry).
- ~~`logit_proto::Encoder`'s one-`Bytes`-per-batch contract still doesn't fit `syslog_out`'s/
  `statsd_out`'s per-message framing, so both bypass the trait entirely — unchanged by this
  plan~~ — **closed as of 2026-09-12**: both now implement `logit_proto::FramedEncoder` over a
  shared `logit_proto::MessageBuf` ([ADR `framed-encoder`](../adr/framed-encoder.md)); the
  `docs/known-gaps.md` entry is closed, with collectd's adoption as the named follow-up.
- `statsd_out` still has no `unit` and no native metric rename/prefix, and only carries an egress
  timestamp on a `|T`-marked line — everything else is stamped with the receiver's own receipt time
  (`docs/known-gaps.md`'s "`statsd_out` has no `unit` and no metric renaming/prefixing..." entry).
- syslog's `event.timestamp` stays receipt time, not the sender's, even though `syslog_out`'s wire
  TIMESTAMP now follows [ADR `syslog-structured-data-convention`](../adr/syslog-structured-data-convention.md)'s
  precedence table (`docs/known-gaps.md`'s "`event.timestamp` is still receipt time..." entry).
- Cross-protocol egress (`P_in -> Q_out` for two different protocols) stays best-effort by design —
  a raw sample list has no OTLP wire type, a `DDSketch` has no statsd wire form — each such
  degradation is counted and documented per the ADR's own rule, in `docs/known-gaps.md`'s
  "Cross-protocol semantic gaps" table.

`docs/adr/lossless-transit.md`'s Status now records this closing assessment as the realization of
its Decision.

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
plan calls out, plus the new round-trip test suites this plan adds. Concretely, W1 through W7 each
ran a clean `script/cibuild` before landing, plus the round-trip/fixed-point suite its own
workstream entry above names — `type_sizes.rs`/`allocations.rs` for W1, `statsd_round_trip.rs` and
the `logit-outputs` `statsd` proptests for W3, `otlp_round_trip.rs` and `logit-proto`'s
`otlp_fixed_point.rs`/`proptest` suite for W4, `syslog_round_trip.rs` and the `logit-outputs`
`syslog` proptest for W5, and `logit-script`'s own `#[cfg(test)]` coverage for W7.
