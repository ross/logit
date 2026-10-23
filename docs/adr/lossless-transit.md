---
created: 2026-09-10
updated: 2026-10-23
---

# Lossless like-protocol transit: the internal model is a superset of every supported wire protocol

## Status
Accepted

Realized as of 2026-09-12: every named loss this ADR requires closed for like-to-like transit is
closed; see [Realization](#realization-2026-09-12).

## Context

[ADR `statsd-output`](statsd-output.md) shipped `statsd_out` with a v1 metric-kind deferral: a
`statsd_in -> aggregate -> statsd_out` relay drops every timer metric today, because the aggregator's
merged `DdSketch` no longer holds the samples it combined and "how does a merged sketch become
statsd lines" was left as an open design question. Working through that question surfaced a bigger
one `logit` has never answered directly: **should an operator be able to point `statsd_in` at
`statsd_out`, `otlp_in` at `otlp_out`, or `syslog_in` at `syslog_out`, and trust that nothing
between them was quietly dropped?** Today the honest answer is no for all three pairs, and each gap
was accepted independently, one sink at a time, with no shared principle saying whether that's fine
or debt.

The nearest existing statement of intent is [ADR `native-wire-format-with-otlp-bridge`](native-wire-format-with-otlp-bridge.md)'s
Consequences: "the internal event model must be a superset of what OTLP can express, or the OTLP
codec becomes lossy." That claim is scoped to OTLP alone, and [ADR `committed-pregenerated-otlp-protobuf`](committed-pregenerated-otlp-protobuf.md)
already had to qualify it once `MetricKind::Distribution` → OTLP `Summary` and `MetricKind::Set` →
skip showed the loss can run in the *other* direction too — `logit`'s own model, not OTLP's, can be
the narrower one.

`docs/known-gaps/mappings.md` separately catalogs a dozen losses that are each individually
"deliberate, already-identified... accepted": `statsd_out` drops
`Distribution`/`Set`/`Histogram`/`Summary` entirely; `syslog_out` emits no RFC 5424 STRUCTURED-DATA
and re-stamps the relayed timestamp; the "Cross-protocol semantic gaps" table lists
`Distribution`→`Summary` degradation, `Set` skip, `U64`/`Timestamp` collapse to `I64`, and more.
None of these is wrong on its own terms — every one is counted and documented, per this project's
own conventions — but nothing says which of them are acceptable forever versus which are a stated
goal not yet met. This ADR settles that.

## Decision

**Like-to-like transit is lossless.** For every protocol P with both a `P_in` and a `P_out`, a
pipeline `P_in -> P_out` — with no transforms, or only transforms that don't themselves summarize —
produces output that a consumer speaking P cannot distinguish from the original input, modulo the
permitted normalizations below. Concretely and immediately in scope: `statsd_in -> statsd_out`,
`otlp_in -> otlp_out`, `syslog_in -> syslog_out`.

**Permitted normalizations** — the things a relay may do and still count as lossless, so a
round-trip test can assert equality against a concrete expectation rather than "close enough":

- Batching/regrouping, and reordering within a batch.
- Splitting a multi-value statsd line (`a:1:2:3|c`) into several lines, or the reverse.
- Summing or merging increments/samples belonging to the same series (a counter's value, or two
  same-series metrics landing on one event).
- Attribute/tag reordering — `AttrMap` is already sorted by interned key, so this costs nothing to
  state.
- Number formatting and escaping that the protocol's own grammar declares equivalent.
- Sanitizer substitutions required for injection safety (an embedded newline in a syslog message,
  a `:`/`|`/`\n` in a statsd name or tag) — safety wins over byte-identity here, and this project
  already treats it that way.
- A sink-configured dialect change the operator explicitly asked for (RFC 3164 in, RFC 5424 out;
  plain statsd in, DogStatsD out) — the operator chose to normalize, so this isn't loss.
- Counter sample-rate folding: `hits:1|c|@0.1` arriving as `Counter(10)` and leaving as `hits:10|c`
  is permitted — a statsd counter has no wire concept of "this was extrapolated." **Timer/histogram
  sample rate is not folded away**: the raw value and its `@rate` both survive until something
  chooses to summarize (see below), because unlike a counter's value, a timer's individual samples
  are real values a lossless relay has to be able to hand back one-for-one.
- Exact-duplicate tag dedupe: a repeated DogStatsD tag key whose values are identical
  (`#team:a,team:a`) collapses to one occurrence (`#team:a`) at decode — the destination protocol's
  own stated behaviour (the Datadog agent dedupes only exact duplicates too), not a loss of a value
  the wire carried. A repeated key whose values *differ* (`#team:a,team:b`) is not a duplicate and
  is not folded away; it decodes to a `Value::Array` in wire order instead (`docs/adr/statsd-output.md`'s
  amendment).
- OTLP timestamp saturation: a wire timestamp past `i64::MAX` nanoseconds decodes as `i64::MAX`
  and relays as 2262-04-11T23:47:16.854775807Z, not the original value (the 2026-09-25
  amendment below).
- An OTLP histogram's overflow count: `metrics.proto` requires a `HistogramDataPoint`'s
  `bucket_counts` to hold one more entry than `explicit_bounds`, so `otlp_out` writes a `0`
  overflow count for a histogram whose last bound is finite, which reads back as an empty `+Inf`
  bucket. A conforming point relays unchanged; one sent with no overflow count gains it.
- An OTLP histogram point with only `count` and `sum`: `otlp_in` reads it as one `+Inf` bucket
  holding `count`, so `otlp_out` writes it back with one bucket count and no bounds (the
  2026-10-21 amendment below).
- A listener's sender attributes: under `peer:`, `proxy_protocol:`, or an HTTP listener's
  `forwarded:`, `network.peer.*` or `client.*` is added to every event, an opt-in addition the
  sender never sent ([ADR `listener-peer-address`](listener-peer-address.md), [ADR
  `forwarded-header-parsing`](forwarded-header-parsing.md)).

**The internal model is a superset of every supported protocol's data model, not only OTLP's.**
A field or semantic a protocol can carry that `Event`/`EventBatch` cannot represent at all is a
model gap, tracked as debt against this ADR (in `docs/known-gaps/`), not something a codec is free to accept quietly. This
generalizes and amends `native-wire-format-with-otlp-bridge`'s Consequences from "must be a superset
of OTLP" to "must be a superset of every protocol this project ships an `_in`/`_out` pair for."

**Summarization is opt-in and named.** Only a component whose stated purpose is to summarize —
`aggregate`, or a Lua script doing the equivalent — may discard information that a lossless relay
would otherwise have to preserve. A codec (a `Decoder`/`Encoder` implementation) never summarizes on
its own, and a decoder never pre-summarizes what a later, explicit stage should be the one to
decide about. Concretely: statsd `ms`/`h`/`d` values and `s` set members stay in the model as raw
observations after decode; only `aggregate` turns them into a sketch or a cardinality estimate, and
only when asked to.

**A concept the model can't represent at all becomes a typed field; a protocol's own raw encoding
of something the model already normalizes rides as a protocol-namespaced attribute that outranks
the normalized field on that protocol's own egress.** Two existing conventions, both already load-
bearing, generalized into one rule:

- (a) OTLP's aggregation temporality, monotonicity, exemplars, `event_name`, span `trace_state` and
  `flags`, and a status message have no field on today's `Event`/`MetricRecord`/`SpanRecord` at
  all — each is a genuine model gap, not something to smuggle onto an attribute where it would leak
  into every other sink's tag set the way `otel.status_message` does today.
- (b) A protocol's own raw encoding of something the model already normalizes rides as a
  protocol-namespaced attribute that outranks the normalized field on that protocol's own egress —
  already the rule `syslog_in`/`syslog_out` follow for severity: [ADR `syslog-output`](syslog-output.md)
  ("Header-field precedence") has `syslog.severity` deliberately outrank the normalized
  `log.severity` on `syslog_out`, precisely because `syslog_in`'s PRI-to-`Severity` mapping is lossy
  by construction. The same shape applies to OTLP severity (24 raw levels collapsing onto `logit`'s
  6): `otlp_in` stamps `otel.severity_number`/`otel.severity_text` attributes that `otlp_out`
  prefers over the normalized `Severity`, the same way `syslog.severity` already works.
- (c) `logit.*`-prefixed attributes stay reserved for a `logit`-specific concept crossing a foreign
  wire with nowhere else to go (`logit.body_format` is the standing example) — not for a concept two
  protocols both need, which belongs in (a) instead. `otel.temporality` is retired under this rule
  once temporality is a real field (see the companion plan); `otel.status_message` is retired once
  span status message is a real field.

**Cross-protocol egress stays best-effort, but every degradation is counted and documented.**
`P_in -> Q_out` for two different protocols may still degrade — a raw sample list has no OTLP
metric type, a DDSketch has no statsd wire form — and that is explicitly *not* what this ADR
requires to be lossless. What it does require: every such degradation increments a
`logit.output.metrics.{degraded,skipped}`-style counter (the existing convention), is named in the
receiving codec's own module doc, and appears in `docs/known-gaps/mappings.md`'s cross-protocol
table. Loss is acceptable exactly there, and nowhere else.

**Round-trip fixed point is the test that proves this, not a design review.** Each like pair gets a
`decode -> encode -> decode` equality test over a fixture corpus that exercises every wire feature
[`docs/design/telemetry-landscape.md`](../design/telemetry-landscape.md) lists for that protocol. A
wire feature with no fixture is a feature this ADR doesn't yet cover; a new one lands with its
fixture, not after.

## Alternatives considered

- **Leave every gap as an independently accepted `known-gaps/mappings.md` entry (status quo).**
  Rejected: each new sink re-litigates the same question from scratch (this ADR exists because
  `statsd_out` just did), and nothing stops the list from growing indefinitely in a direction no one
  chose on purpose. An operator relaying statsd through `logit` has a reasonable expectation of
  fidelity that the current state doesn't meet and doesn't say it's trying to meet.
- **Make OTLP the internal model.** Already rejected in `native-wire-format-with-otlp-bridge` for
  performance and log-model-fit reasons; also fails this goal directly, since OTLP itself cannot
  carry a mergeable sketch, statsd's relative-gauge delta, raw unaggregated samples, or syslog
  structured data without its own lossy workarounds.
- **Raw wire passthrough** (a `syslog.raw`/`statsd.raw` `Value::Bytes` holding the original
  datagram or line, alongside the normalized model). Byte-faithful by construction, but rejected
  even as a complement: it goes stale the instant any transform touches the event's normalized
  fields, doubles memory for every event that carries it, and answers a different question
  ("what bytes arrived") than the one this ADR is about ("does the *information* survive"). A
  transform-derived event has no raw bytes to begin with, so this couldn't be the general
  mechanism regardless.
- **Best-effort-with-counters everywhere, including like-to-like.** This is what exists today.
  Rejected as the permanent state for exactly the reason in Context: it's indistinguishable from
  not having tried, from the perspective of an operator who expected a relay to be transparent.
  Kept, deliberately, for the cross-protocol case, where an exact mapping frequently doesn't exist.

## Consequences

- `MetricKind`, `MetricRecord`, `LogRecord`, and `SpanRecord` are reshaped to close the gaps this
  ADR names as debt rather than accepted loss — [Realization](#realization-2026-09-12) records
  what landed, informed by the field-by-field survey in [`docs/design/telemetry-landscape.md`](../design/telemetry-landscape.md).
  `Event`'s exact-size test (`crates/logit-core/tests/type_sizes.rs`) and
  [`docs/design/memory.md`](../design/memory.md)'s table move as a result — expected and accepted,
  per [AGENTS.md](../../AGENTS.md)'s "when one fails, that's the test working."
- The native wire format (`crates/logit-proto/src/native/`) has to carry whatever the model gains;
  since `logit` is pre-release, this is a straight reshape of the record framing, not a version
  negotiation or a dual-read compatibility path.
- `docs/known-gaps/mappings.md`'s statsd/syslog/cross-protocol entries this ADR covers are
  reclassified from "accepted" to "tracked debt against this ADR," not removed — nothing is fixed by
  writing this record.
- `statsd-output`, `aggregation-window-semantics`, `syslog-output`, and `relative-gauge-adjustments`
  each get an amendment when their corresponding workstream lands, the same way
  `aggregation-window-semantics` already carries two amendments from later decisions.
- The OTLP integration test's current fidelity check
  (`crates/logit-cli/tests/otlp_round_trip.rs`'s `assert_round_tripped`) only checks that a log, a
  metric, and a span each exist with roughly the right shape — it becomes a per-field fixed-point
  assertion once the model changes it's meant to protect exist.

## Realization (2026-09-12)

The model and codec work this ADR called for closed every loss it named on the first three pairs.

### Pre-release, no backward compatibility

`logit` is pre-release, so `MetricKind`, the native wire format, and config are reshaped in place:
no version negotiation, dual-read path, or decode shim kept for an older build. The native codec
keeps its skip-unknown TLV framing as hygiene, not because anything reads an old frame.

### The model

[ADR `metrics-model-v2`](metrics-model-v2.md) has the shapes and
[`docs/design/memory.md`](../design/memory.md) §1 their measured sizes. In summary:

- `Sum { value, temporality, monotonic }` replaces `Counter`, with `MetricKind::counter(v)` for
  the delta-monotonic case.
- Raw and summarized pairs: `Samples` (statsd `ms`/`h`/`d`, values plus `sample_rate`) beside
  `Distribution`, and `SetMembers` beside `Set`. A decoder produces the raw kind; only `aggregate`
  produces the summarized one.
- `ExponentialHistogram` is its own variant, so `otlp_in -> otlp_out` is a fixed point for it;
  `Histogram` carries `sum`, `min`, and `max`, and `Summary` carries `count` and `sum`.
- `MetricRecord` gains `description`, `start_timestamp`, `exemplars`, and `flags` (OTLP's
  `NO_RECORDED_VALUE`).
- `LogRecord` gains `event_name`, `observed_timestamp`, and `dropped_attributes_count`.
- `SpanRecord` gains `flags` and a boxed `SpanExt` (`status_message`, `trace_state`, and the
  dropped counts); `SpanLink` gains `flags`, `trace_state`, and a dropped count.
- `EventBatch` gains a batch-level `Scope` (name, version, attributes, dropped count,
  `schema_url`), and `Resource` gains a dropped count and `schema_url`. `otlp_in` makes one batch
  per distinct resource and scope; `otel.scope.*`, `otel.temporality`, and `otel.status_message`
  attributes are retired.

### Per pair

- **`statsd_in -> statsd_out`.** Timers and sets decode to `Samples` and `SetMembers` with the
  sample rate carried verbatim, and round-trip byte for byte under `format: dogstatsd`; under
  `format: statsd` multi-value lines split and `h`/`d` normalize to `ms`. `|c:` rides a carrier
  and `|T` the neutral `_logit.metrics.closed: "timestamped"` flag, and both round-trip under
  `format: dogstatsd`. DogStatsD events and service checks relay
  ([ADR `statsd-output`](statsd-output.md)'s "DogStatsD events and service checks relay"), and a
  repeated tag key decodes to a `Value::Array` and re-expands.
- **`otlp_in -> otlp_out`.** Every metric, log, and span field the model gained above round-trips.
  `observed_timestamp` is kept when the wire set it and re-stamped only when unset, and
  `otel.severity_number`/`otel.severity_text` carriers outrank the normalized `Severity`. A batch
  with no scope encodes an empty `InstrumentationScope`; `internal` stamps its own `logit` scope.
  A `NumberDataPoint`'s int or double kind collapsed to `f64` until
  [ADR `typed-metric-scalar`](typed-metric-scalar.md), and a log-and-span `Event` still splits
  into two wire records, a permitted regroup (`otlp/logs.rs`'s module doc).
- **`syslog_in -> syslog_out`.** RFC 5424 STRUCTURED-DATA parses into `syslog.sd` and re-emits
  ([ADR `syslog-structured-data-convention`](syslog-structured-data-convention.md)), a non-numeric
  PROCID survives as `Value::Str`, a non-UTF-8 MSG decodes to `Value::Bytes`, and `syslog_out`'s
  TIMESTAMP follows that ADR's precedence table.
- **Native `logit_in -> logit_out`.** Lossless by design, and the codec `file_out format: native`
  and `buffer.disk:` share, so every model addition lands there too.

Each fixed point has a test suite: `crates/logit-cli/tests/{statsd,syslog,otlp}_round_trip.rs`
over real sockets with per-field equality, `crates/logit-proto/tests/otlp_fixed_point.rs` at the
pure-codec level, and `proptest` fixed points in `crates/logit-outputs/src/statsd.rs` and
`syslog.rs` (`mod fixed_point`) and `crates/logit-proto/src/otlp/metrics.rs`.

### Residual debt

Tracked in `docs/known-gaps/`, beside the cross-protocol table in
[`docs/known-gaps/mappings.md`](../known-gaps/mappings.md) that the best-effort clause accepts:

- [`docs/known-gaps/statsd.md`](../known-gaps/statsd.md): post-sketch kinds leave `statsd_out` as
  per-component lines whose quantiles and set estimates don't merge downstream, and `statsd_out`
  has no `unit`, no native rename, and an egress timestamp only on a `|T`-marked line.
- `Summary` has no exemplars on the OTLP wire (`docs/known-gaps/mappings.md`).

## Amendment: a fourth like pair (2026-09-11)

`prometheus_in -> prometheus_out` is a fourth like-protocol pair in scope under this ADR, alongside
`statsd`, `otlp`, and `syslog` — the first one designed against the model *after* it became the
superset this ADR requires, rather than retrofitted onto it. See
[ADR `prometheus-scrape-and-exposition`](prometheus-scrape-and-exposition.md) for the transports,
dialect negotiation, model mapping, and permitted normalizations.

## Amendment: a fifth like pair (2026-09-12)

`collectd_in -> collectd_out` is a fifth like-protocol pair in scope under this ADR, alongside
`statsd`, `otlp`, `syslog`, and `prometheus`. See
[ADR `collectd-binary-relay`](collectd-binary-relay.md) for the transport, the model mapping,
the `collectd.*` attribute convention, and the permitted normalizations.

## Amendment: a sixth like pair (2026-09-13)

`graphite_in -> graphite_out` is a sixth like-protocol pair in scope under this ADR, alongside
`statsd`, `otlp`, `syslog`, `prometheus`, and `collectd`. See
[ADR `graphite-carbon-relay`](graphite-carbon-relay.md) for the transports (plaintext and pickle,
over TCP or UDP), the model mapping, the restricted pickle codec, and the permitted normalizations.

## Amendment: a seventh and an eighth like pair (2026-09-23)

`datadog_in -> datadog_out` (Datadog's intake API) and `datadog_trace_in -> datadog_trace_out`
(the Datadog Agent's APM API) are the seventh and eighth like-protocol pairs in scope under this
ADR, alongside `statsd`, `otlp`, `syslog`, `prometheus`, `collectd`, and `graphite`. See
[ADR `datadog-agent-and-intake-relay`](datadog-agent-and-intake-relay.md) for the two protocols,
the `datadog.*` attribute convention, the Datadog-mapped `DdSketch` that makes sketch relay
bin-for-bin, and why traces relay over the Agent's protocol rather than OTLP. Their permitted
normalizations include a batch boundary per `TracerPayload`, `avg` recomputed from `sum`/`cnt`,
and Datadog's own name and tag sanitization.

Realized as of 2026-09-24: both pairs relay losslessly modulo the normalizations
`crates/logit-proto/src/datadog/mod.rs`'s module doc lists; what stays open is in
[`docs/known-gaps/datadog.md`](../known-gaps/datadog.md).

## Amendment: OTLP timestamp saturation is a permitted normalization (2026-09-25)

OTLP carries every timestamp as a `fixed64` of nanoseconds, and `Event` holds an `i64`. A wire
value past `i64::MAX` used to wrap negative on decode. [ADR
`untrusted-input-bounds`](untrusted-input-bounds.md) makes every OTLP decode site saturate it to
`i64::MAX` instead, and this amendment adds that to the permitted normalizations above: an
`otlp_in -> otlp_out` relay of such a timestamp emits 2262-04-11T23:47:16.854775807Z, not the
original value, and still counts as lossless. No real clock produces a timestamp past 2262, so
the normalization touches only malformed or hostile input.

## Amendment: a ninth like pair (2026-09-25)

`splunk_hec_in -> splunk_hec_out` (Splunk's HTTP Event Collector) is the ninth like-protocol pair
in scope under this ADR, alongside `statsd`, `otlp`, `syslog`, `prometheus`, `collectd`,
`graphite`, and the two Datadog pairs. See [ADR `splunk-hec-relay`](splunk-hec-relay.md) for the
protocol, the OpenTelemetry exporter's attribute vocabulary it adopts in place of a `splunk.*`
namespace, the metric and span mappings, and acknowledgment. Its permitted normalizations are listed in `crates/logit-proto/src/splunk/mod.rs`'s module doc (framing
and batching by resource; number spelling; the single-metric form leaving as multi-metric, with
`metric_type`; a nested `fields` value leaving flattened; hex ids leaving lowercase; `/raw` split
into one event per line).

Realized as of 2026-09-25: the pair relays losslessly modulo the normalizations its codec's
module doc lists; what stays open is in [`docs/known-gaps/splunk.md`](../known-gaps/splunk.md).

## Amendment: a tenth like pair (2026-10-13)

`influxdb_in -> influxdb_out` (InfluxDB 2.x line protocol over the v2 write API) is the tenth
like-protocol pair in scope under this ADR, alongside `statsd`, `otlp`, `syslog`, `prometheus`,
`collectd`, `graphite`, the two Datadog pairs, and `splunk_hec`. See
[ADR `influxdb-line-protocol-relay`](influxdb-line-protocol-relay.md) for the protocol, the
`influxdb.*` carrier (the measurement, string fields, and the request's org and bucket), fields as
typed `Gauge` records on one event, the all-or-nothing write, and the listener's responses. Its
permitted normalizations are numbered in that ADR, and
`crates/logit-proto/src/influxdb/mod.rs`'s module doc carries the same numbers.

The pair needs a typed metric scalar, which the model didn't have:
[ADR `typed-metric-scalar`](typed-metric-scalar.md) replaces the `f64` in `Gauge`, `GaugeDelta`,
and `Sum` with `MetricValue { F64, I64, U64, Bool }`. That change also closes two losses this ADR
already counted as debt: an OTLP `as_int` point leaving `otlp_out` as a double, and a collectd
`COUNTER`, `DERIVE`, or `ABSOLUTE` above 2^53 decoding to a different number.

## Amendment: rule (b)'s carriers are private (2026-10-13)

[ADR `private-attribute-namespace`](private-attribute-namespace.md) decides that rule (b)'s
protocol-namespaced carriers live under one private prefix, `_logit.`, with the protocol segment
kept: `syslog.severity` is spelled `_logit.syslog.severity`, and `otel.severity_number` is spelled
`_logit.otel.severity_number`. Under that decision, every protocol sink ignores `_logit.*` when it
renders attributes, except its own carriers, which it reads by exact name. See that ADR for which
keys move, the ignore rule, the inbound guard on metric-wire tags, and how `keep` and `flatten`
treat the namespace.

Rules (b) and (c) are opposites. A rule (c) key (`logit.body_format`) is public: it exists to cross
a foreign wire, where a sink writes it. A rule (b) carrier is private: it exists for its own
protocol's sink and never crosses a foreign wire as a tag, label, field, or attribute.

## Amendment: cross-protocol outcomes have a spec (2026-10-19)

[ADR `transit-spec`](transit-spec.md) gives the "counted and documented" clause a canonical copy:
`spec/transit/mapping.yaml` states the class of every cross-protocol transition, flows through the
real codecs check the code against it, and each codec's module-doc mapping table and
`docs/known-gaps/mappings.md`'s cross-protocol table are rendered from it. Like-pair fixed points
and the permitted-normalization lists stay as this ADR describes them.

## Amendment: a statsd set leaves one line per member (2026-10-19)

The "Per pair" entry for `statsd_in -> statsd_out` says timers and sets round-trip byte for byte
under `format: dogstatsd`. Sets don't: `statsd_out` writes a `SetMembers` record as one
`name:<member>|s` line per member, so `users:a:b|s` leaves as `users:a|s` and `users:b|s` and
reads back as two records. A statsd server counts the same distinct members either way, so this is
a permitted normalization, the per-member form of "multi-value lines split", and it applies under
both dialects. The transit spec's `set_members` row records it
([ADR `transit-spec`](transit-spec.md)).

## Amendment: a DogStatsD `host.name:` tag relays as the `host:` tag (2026-10-19)

Under `format: dogstatsd`, `statsd_out` writes a non-empty `host.name` as the `host:` tag on a line
with no `host:` tag and no non-empty `h:` field, because a DogStatsD Agent takes a line's host
from that tag. A `statsd_in -> statsd_out` relay therefore sends `#host.name:web-1` as
`#host:web-1`. That's a permitted normalization of the pair: the sender named a host, and the
relay sends it where the destination reads one. [ADR `statsd-output`](statsd-output.md)'s
amendment of the same date records the decision, and the transit spec's `string_value` row the
class.

## Amendment: a bucketless OTLP histogram keeps its count (2026-10-21)

`logit_core::Histogram` has no `count` field; its population is the sum of its buckets. An OTLP
`HistogramDataPoint` may carry only `count` and `sum` ("both 'explicit_bounds' and
'bucket_counts' must be omitted and only 'count' and 'sum' are known", `metrics.proto`), and
`otlp_in` used to read such a point as an empty histogram, so every sink saw a population of 0.

`otlp_in` now reads a bucketless point with a nonzero `count` as one `+Inf` bucket holding that
count. A single bucket covering every value describes the same population, so this is a
permitted normalization of `otlp_in -> otlp_out`, listed above: the point leaves as `count` n,
`bucket_counts` `[n]`, and no bounds. Every other sink sees the true population too, such as
`prometheus_out`'s `+Inf` bucket equal to `_count`. `datadog_out` sketches it as Datadog's
exporter sketches the bucketless point when it carries no `min` and `max`: one bin at the Agent's
key -1, about `-9.94e-10`, holding every observation, with the count and sum exact. Given a `min`
and a `max`, the exporter spreads the count between them, and under `histograms.mode: counters`
it writes no `.bucket` series, which `datadog_out` can't tell apart from a one-bucket point's
(`docs/known-gaps/datadog.md`; the `no-buckets-count*` cases of
`testdata/differential/datadog-sketch/`).

A point whose `count` isn't the sum of its `bucket_counts` breaks `metrics.proto`'s "The sum of
the bucket_counts must equal the value in the count field". `otlp_in` keeps the buckets, since
they're the distribution, and counts the point
`logit.input.metrics.degraded{reason="histogram_count_mismatch"}`, the reason `prometheus_in`
counts when `_count` disagrees with its `+Inf` bucket.

Adding a `count` field to `Histogram` would carry both cases unchanged, but it touches every
constructor, the native wire record, `event_json`, Lua's `Event.new` and `to_table`, and every
encoder, for a mismatch only a nonconforming producer sends. `docs/known-gaps/otlp.md` keeps the
mismatch as residual debt.
