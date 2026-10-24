---
created: 2026-10-22
updated: 2026-10-23
---

# Metric flags: producers set `_logit.metrics.*` flags, and `aggregate` reads flags instead of protocol carriers

## Status
Accepted

## Context

`aggregate` applies two kinds of special handling that depend on where an event came from. Today
it discovers both by probing for four protocol carriers on every event with metrics
(`Aggregator::process`, `crates/logit-transforms/src/aggregate.rs`):

| Probe | Handling it drives |
|---|---|
| `_logit.datadog.stats.name` on the event, else the batch resource | pass the event through whole, counted `passed_through{reason="bucketed"}` |
| `_logit.statsd.timestamp` (a `U64`) | pass the event through whole, counted `timestamped` |
| `_logit.prometheus.timestamp` (`Bool(true)`) | pass the event through whole, counted `timestamped` |
| any key under `_logit.influxdb.field.` | leave the key out of the series identity and out of the flushed series |

The first three answer one question, "did the source already close this event's time span?",
and have one outcome: the records are forwarded unmerged with their timestamp, because a window
would merge consecutive buckets of one series, stamp the flush time over the producer's, and,
under cumulative temporality, turn a closed bucket's counts into a running total
([ADR `aggregation-window-semantics`](aggregation-window-semantics.md), "Amendment: timestamped
records, APM stats, and non-finite gauges pass through (2026-10-19)"). The fourth answers a
different question, "is this attribute identity or data?", for a value InfluxDB calls a field
(the same ADR's "Amendment: InfluxDB string fields aren't series identity (2026-10-19)").

Each probe is a per-protocol fact, so `aggregate` imports `logit_proto::datadog`,
`logit_proto::influxdb`, `logit_proto::statsd`, and `logit_proto::prometheus` constants, and
every protocol whose wire carries such a fact adds a probe to a shared transform. Two of the four
are already flags in protocol clothing: `_logit.prometheus.timestamp` is a bare `Bool(true)`
whose only job is "don't window me", and `datadog_in`'s `_logit.datadog.units` batch mark is a
producer telling a consumer how to treat a batch. Nothing lets a Lua script or a `set` ask for
the same handling for an event it built, although both may write private keys
([ADR `private-attribute-namespace`](private-attribute-namespace.md), "Transforms").

The probes also cost: on the perf VM, `aggregate` rose from 0.334 to 0.353 CPU µs/event and
`aggregate-groups` from 0.289 to 0.311 between the 2026-10-15 baseline and `5e297cce`. The
2026-10-22 bisect put all of it in the string-field scan and the stats lookup; the 2026-10-23
measurement found most of it in how `1f30eb3c` built the series key, with the probes themselves
costing within the spread on `aggregate` and about 2% on `aggregate-groups`
([performance.md](../design/performance.md), "`aggregate`: the flag reads and the series-key copy
(2026-10-23)"). Speed is the secondary reason for this decision. The primary one is that a shared
transform shouldn't know protocols, and that the event's creator is the component that knows what
it made.

## Decision

`aggregate` reads flags, never protocol carriers. The handling it can apply is named, each kind
has a flag in the private namespace, and the component that creates an event sets the flag. The
protocol carriers stay where they are, because each protocol's own sink still reads them, except
where a carrier only repeated a flag: [ADR
`protocol-knowledge-stays-in-family`](protocol-knowledge-stays-in-family.md)'s "Amendment: move,
don't mirror (2026-10-23)" retired `_logit.statsd.timestamp` and `_logit.prometheus.timestamp`,
and `statsd_out` and `prometheus_out` read the flag instead. The same amendment's next
applications retired `_logit.datadog.interval` and the `rate` value of `_logit.datadog.type` for
`_logit.metrics.interval` and `_logit.metrics.type`, which `datadog_out` reads instead, and
moved `_logit.prometheus.type`'s `info`, `stateset`, and `gaugehistogram` values to
`_logit.metrics.type`, which `prometheus_out` and `otlp_out` read instead.

### The flags

| Key | Value | Meaning | Set by | Read by |
|---|---|---|---|---|
| `_logit.metrics.closed` | `Str`, one of `bucketed` or `timestamped` | Every metric record on this event summarizes a time span its source closed. Forward the event whole, unmerged, with its timestamp and attributes intact, and count each record `logit.transform.metrics.passed_through{reason=<value>}`. Any other `Str`, or a `Bool(true)`, means the same and counts `reason="flagged"`, so a script can't put free text into a telemetry tag. | `datadog_in` and `datadog_trace_in` on each APM stats group event (`bucketed`); `statsd_in` on a `\|T` line or an event's or service check's `d:` field, and `prometheus_in`'s scrape on an exposition sample with its own timestamp (`timestamped`); Lua or `set` | `aggregate`; `statsd_out` and `prometheus_out`, which write `Event::timestamp` as a `\|T`, a `d:`, or a sample timestamp for the value `timestamped` only |
| `_logit.metrics.fields` | `Bool(true)` | This event carries attributes under `_logit.field.`. The reader scans for them only when this flag is set, and `aggregate` leaves the flag itself out of the series key and the flushed series too, so readings of one series with and without a string field stay one series. | `influxdb_in` and `line_protocol` on an event with at least one string field; Lua or `set` when they add one | `aggregate` |
| `_logit.metrics.type` | `Str`, one of `rate`, `info`, `stateset`, or `gaugehistogram` | What kind of reading every metric record on this event is, where the model kind doesn't say. Each value names one kind, and on any other kind it's ignored. A reader ignores a value it has no rule for. See the four rows below. | see below | see below |
| `_logit.metrics.type: rate` | | Every `Gauge` record holds a per-second rate computed over `_logit.metrics.interval` seconds: an increment over that interval, not a state. A stage that collapses same-instant readings of one series sums two rates with equal intervals rather than keeping one. | `datadog_in` on a `rate` series point; Lua or `set` | `dedup_timestamps`, which sums rates of one interval; `prometheus_out`, which counts a same-millisecond rate collision `dropped` rather than collapsing it as an equal state; `datadog_out`, which writes a flagged `Gauge` as a `rate` series point |
| `_logit.metrics.type: info` | | Every `Gauge` record is an info metric: a `1` whose labels are the payload. | `prometheus_in` on an OpenMetrics `info` family; Lua or `set` | `prometheus_out`, which writes an `info` family; `otlp_out` and `kafka_out`'s OTLP formats, which write a cumulative non-monotonic `Sum` named `<name>_info` |
| `_logit.metrics.type: stateset` | | Every `Gauge` record is one state of a state set, `1` when the state holds and `0` when it doesn't. | `prometheus_in` on an OpenMetrics `stateset` family; Lua or `set` | `prometheus_out`, which writes a `stateset` family; `otlp_out` and `kafka_out`'s OTLP formats, which write a cumulative non-monotonic `Sum` |
| `_logit.metrics.type: gaugehistogram` | | Every `Histogram` record counts a quantity that can go down, so no bucket is a running count. | `prometheus_in` on an OpenMetrics `gaugehistogram` family; Lua or `set` | `prometheus_out`, which writes a `gaugehistogram` family; `otlp_out` and `kafka_out`'s OTLP formats, which skip the record, counted `logit.output.metrics.skipped{metric_kind="gauge_histogram"}` |
| `_logit.metrics.interval` | `I64`, whole seconds; written only when nonzero | The span the event's readings cover: a rate's divisor, a delta count's window. Seconds, Datadog's unit, because Datadog's series `interval` is the one wire that carries it. | `datadog_in` on a series point with a nonzero `interval`, of any type; Lua or `set`, such as a `set` after `aggregate` stamping its window | `dedup_timestamps`, which sums two rates only when their intervals are equal; `datadog_out`, which writes it as the series `interval` of any type |
| `_logit.field.<key>` | any `Value` | Data about the point, never series identity. `aggregate` leaves it out of the series key and out of the flushed series; a record that passes through keeps it. `influxdb_out` writes a `Str` under it as the string field `<key>`, and skips a non-`Str` value under it uncounted, since a non-`Str` isn't a string field. Replaces `_logit.influxdb.field.<key>`. | `influxdb_in` and `line_protocol` for each string field; Lua or `set` | `aggregate`, `influxdb_out` |

A flag is read as every private carrier is read: the event's value, else the batch resource's
(`logit_core::attrs::merged`). A producer or a `set resource:` can therefore stamp a whole batch
once. `aggregate` reads the resource half once per batch, in `Transform::begin_batch`, not once
per event.

A producer that writes `_logit.field.*` keys without `_logit.metrics.fields` gets those keys in
the series identity, and one that builds a closed bucket without `_logit.metrics.closed` gets it
windowed. That is the rule: the creator decides, and the consumer does what the flags say.
[ADR `protocol-knowledge-stays-in-family`](protocol-knowledge-stays-in-family.md) generalizes
the rule past `aggregate`: outside a lossless family, no component keys behavior on another
protocol's carrier.

### The segments

`_logit.metrics.` and `_logit.field.` name the signal a flag describes, not a protocol and not a
consumer. The private namespace's protocol segment (`_logit.<protocol>.`) gives each sink an
own-prefix to read; these two segments are what any producer writes for any reader. The names
carry no `aggregate` because a closed bucket is as wrong to collapse in `dedup_timestamps` or to
difference in `cumulative_to_delta` as it is to window, and those transforms may honor the same
flags later with no rename.

### What `aggregate` keeps

The record-level gates that are about the record's own value or kind stay: an OTLP
`NO_RECORDED_VALUE` point (`no_recorded_value`), a kind with no merge rule (`no_merge_rule`), and
a non-finite delta `Sum`, `Gauge`, or `GaugeDelta` (`non_finite`). After this change
`crates/logit-transforms/src/aggregate.rs` imports nothing from `logit_proto`.

## Alternatives considered

- **Patch the probes.** Look the resource up once per batch, keep a process-wide "a string-field
  key was interned" flag, or answer all four probes in one pass over the attributes. Each keeps
  `aggregate` reading four protocols' carriers and adds nothing for a fifth or for Lua. The
  per-batch resource lookup survives inside the flag read.
- **Batch-level carrier marks.** A bitset on `EventBatch` meaning "may carry protocol P's
  carriers", set by the decoders. It infers the handling from the presence of a carrier, so
  every component that can write a private key (`set`, Lua, `json`, `logfmt`, `kv`, `csv`,
  `regex`, `otlp_in`, `splunk_hec_in`) would have to set an "unknown" mark, which most real
  pipelines pass through. It also adds 8 bytes to `Delivered` (`EventBatch` and `BatchContext`
  have no padding; `delivered_is_72_bytes_no_wider_than_its_larger_variant`). A flag that is the
  contract, rather than a proxy for one, has no inference to get wrong.
- **A typed slot on `Event` or a `MetricRecord` flag bit.** Rejected by
  [ADR `private-attribute-namespace`](private-attribute-namespace.md)'s "Alternatives
  considered": an allocation or bytes on every event, and invisible to Lua, `route`, `set`, and
  `remove`. An attribute is visible to all of them, which is the point.
- **A flags byte on `Event`.** `Event` is 864 bytes with no padding
  (`crates/logit-core/tests/type_sizes.rs`), so a byte costs 8 per event and a measurement per
  signal class under [ADR `event-sizing-and-allocation-strategy`](event-sizing-and-allocation-strategy.md).
  It is the fallback if the perf VM shows the one attribute lookup per event is expensive, not
  the first form.

## Consequences

- **Two sources of truth only where a carrier says something the flag doesn't.** A flagged
  event's protocol carriers, such as `_logit.datadog.stats.name` beside `bucketed`, and the flag
  are written by the same decoder at the same point, and a `remove` of one and not the other
  gives a different result than before. Where a carrier only repeated the flag, the carrier is
  retired instead ([ADR `protocol-knowledge-stays-in-family`](protocol-knowledge-stays-in-family.md),
  "Amendment: move, don't mirror (2026-10-23)"). The transit spec's aggregate flows pin the
  decoders' side, and a regression test pins `aggregate`'s: an event carrying every protocol
  carrier and no flag is windowed.
- **One more attribute per flagged event.** Metric events run 0 to 6 attributes against an
  inline capacity of 8 ([data-shapes.md](../design/data-shapes.md)), so a flag rarely spills.
  `crates/logit-bench/tests/allocations.rs` says which fixtures move.
- **`keep` under its default `private: drop` strips the flags**, as it strips the carriers, so a
  `keep` ahead of `aggregate` windows a flagged event in both designs. No change.
- **`_logit.field.` is a wire-visible rename** for a reader of `format: json` output or a writer
  of `line_protocol`-bound JSON. `logit` is pre-release: no alias and no decode path for
  `_logit.influxdb.field.`.
- **Lua and `set` gain a capability.** A script can flag an event out of windowing, or add a
  string field to an InfluxDB point, with no new config field. The script owns the consequence.
- **Telemetry.** `passed_through{reason}` keeps `bucketed` and `timestamped` and gains `flagged`.
  The transit spec's `ledger/` files and `mapping.yaml` name these counters, so
  `script/transit gen` runs with the change.
- **Amendments.** [ADR `influxdb-line-protocol-relay`](influxdb-line-protocol-relay.md) and
  [ADR `private-attribute-namespace`](private-attribute-namespace.md) take the `_logit.field.`
  segment with the rename; [ADR `aggregation-window-semantics`](aggregation-window-semantics.md)
  and `private-attribute-namespace` take the `_logit.metrics.` segment with the flags, and the
  two 2026-10-19 amendments of `aggregation-window-semantics` become "the decoder sets the flag".
  [data-model.md](../design/data-model.md)'s "Well-known attribute names" gains a row per flag,
  and [lua-api.md](../design/lua-api.md) documents the capability.
- **Measurement.** The perf VM compares `72191ead` (before both probes), main, the flag build,
  and a floor build with no probe at all, on `aggregate`, `aggregate-groups`, `json-parse`, and
  `passthrough`. Measured 2026-10-23: once the series key is built in place, the flag build reads
  at `72191ead`'s level on all four, and the flag reads cost less than the spread against the
  floor, so the `Event` flags byte stays unbuilt ([performance.md](../design/performance.md),
  "`aggregate`: the flag reads and the series-key copy (2026-10-23)").
- [docs/plans/metric-flags.md](../plans/metric-flags.md) has the workstreams.
