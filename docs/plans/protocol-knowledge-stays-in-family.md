---
created: 2026-10-23
updated: 2026-10-23
---

# Enabling plan: protocol knowledge stays in family — the remaining cross-family reads

## Context

[ADR `protocol-knowledge-stays-in-family`](../adr/protocol-knowledge-stays-in-family.md) decides
that a component keys behavior on another component's internals only within a lossless family.
Outside one, it reads neutral meaning segments, public vocabulary, or its own config. Four
production reads break that rule today. This plan lists each one with its remedy, so a later
session can pick one up without redoing the survey.

None of this is scheduled. Each section is a future workstream with its own stream key, picked
when the work starts.

Goals:

- No shared transform, router, input, or foreign sink keys behavior on another family's carrier
  or wire-specific attribute name.
- Every special handling a cross-family read gives today stays available, through a neutral
  segment a Lua script or a `set` can also write.

Non-goals:

- Changing what a family's own members read. C4, C5, C7, and C8 below stay as they are.
- A native copy or rename component. C6's remedy may want one; it gets its own ADR if so.
- Honoring the new neutral flags anywhere they aren't needed to replace a cross-family read.

## The rule

Outside a lossless family (a like pair, the Datadog family, or the native hop's), a component
reads only neutral meaning segments (`_logit.metrics.`, `_logit.field.`), public vocabulary, and
what its own config names, a component id in `has_provenance`, `drop_provenance`, or a provenance
`route` included ([the ADR's Decision](../adr/protocol-knowledge-stays-in-family.md#decision)).

## C1 and C2: the Datadog rate carrier in `dedup_timestamps` and `prometheus_out`

**Future workstream, not scheduled.**

**What reads what.** `datadog_in` decodes a Datadog `rate` as a `Gauge` with
`_logit.datadog.type: rate` and its interval as `_logit.datadog.interval`
([ADR `datadog-agent-and-intake-relay`](../adr/datadog-agent-and-intake-relay.md)'s decision 8).
Two components outside the Datadog family read them through
`logit_proto::datadog::is_rate` and `logit_proto::datadog::interval`:

- `dedup_timestamps` (`DedupTimestamps::resolve` in
  `crates/logit-transforms/src/dedup_timestamps.rs`, which `begin_batch` calls per colliding set) sums a rate as an increment when its
  interval equals the survivor's, and passes it through otherwise
  ([ADR `dedup-timestamps-transform`](../adr/dedup-timestamps-transform.md)'s "Which reading
  survives").
- `prometheus_out` (the `Series` built in `crates/logit-proto/src/prometheus/mod.rs`, field
  `increment`) counts a same-millisecond rate collision as
  `logit.output.metrics.dropped{reason="duplicate_timestamp"}` rather than collapsing it as an
  equal state (the same ADR's "The sink-side counter").
  `logit_proto::replaced_by_a_later_reading`'s doc names the carrier too.

**What breaks if Lua or `set` writes the key.** A script whose gauge is an increment over an
interval has to spell a Datadog carrier to get the increment handling, and that carrier then
reaches `datadog_out`, which writes the point as a Datadog `rate`. A `set` of
`_logit.datadog.type: rate` on a non-Datadog batch does the same. The handling is a property of
the reading, not of Datadog, and nothing outside the Datadog family can ask for it by name.

**Remedy.** `datadog_in` sets a neutral flag beside the carrier: an increment flag and its
interval under `_logit.metrics.` (for example `_logit.metrics.increment` and
`_logit.metrics.interval`). `dedup_timestamps` and `prometheus_out` read the flag and import
nothing from `logit_proto::datadog`. `datadog_out` keeps reading `_logit.datadog.type` and
`_logit.datadog.interval`, its own family's carriers. Lua and `set` can write the flag.

Open design questions:

- The key names and values: a `Bool` increment flag, or a `Str` naming the gauge's kind
  (`rate`), and whether the interval is a separate key or part of the value.
- Whether the interval's value type and unit follow Datadog's (integer seconds) or a neutral
  duration.
- Whether a rate should instead become a typed model kind, which
  [ADR `datadog-agent-and-intake-relay`](../adr/datadog-agent-and-intake-relay.md)'s decision 8
  rejected as a delta `Sum` because folding a per-second value multiplies and rounds.
- The metric-flags consequence applies: the carrier and the flag become two sources of truth,
  and a `remove` of one changes the result.

**What else moves.** An amendment to
[ADR `dedup-timestamps-transform`](../adr/dedup-timestamps-transform.md) ("Which reading
survives", "The sink-side counter") and to [ADR `metric-flags`](../adr/metric-flags.md)'s flag
table; [data-model.md](../design/data-model.md)'s flag table; the transit spec's
`same_instant`, `same_instant_equal`, and `same_millisecond` rows in `spec/transit/mapping.yaml`
where their text names the carrier, and `script/transit gen`; the Lua capability in
[lua-api.md](../design/lua-api.md); the known-gaps row's C1 and C2.

**Estimate.** Two PRs: an ADR amendment and plan row (S), then the decoder, both readers, the
regression test (a rate carrier with no flag is a state), and the docs (M).

## C3: `_logit.prometheus.type` in `otlp_out` and `kafka_out`

**Future workstream, not scheduled.**

**What reads what.** `prometheus_in` stamps `_logit.prometheus.type` on a `gaugehistogram`,
`info`, `stateset`, `untyped`, or `unknown` family. `prometheus_out` reads it as its own carrier.
`otlp_out` reads it too, in `encode_metric` (`crates/logit-proto/src/otlp/metrics.rs`): a
`gaugehistogram` is skipped and counted `logit.output.metrics.skipped{metric_kind="gauge_histogram"}`,
and `info` and `stateset` leave as a cumulative non-monotonic `Sum`, `info` renamed `<name>_info`
([ADR `prometheus-scrape-and-exposition`](../adr/prometheus-scrape-and-exposition.md)'s
"Amendment (2026-10-21): `otlp_out` reads the family-type carrier"). `kafka_out`'s `otlp_proto`
and `otlp_json` formats share the encoder through `OtlpEncoder`
(`crates/logit-outputs/src/kafka/encode.rs`).

**What breaks if Lua or `set` writes the key.** A script that builds an info-style metric has to
spell a Prometheus carrier to get OTLP's non-monotonic `Sum`, and that carrier also changes
`prometheus_out`'s `# TYPE` line. Any other producer of a state-set or info metric (an OTLP
source, a `kv_metrics`) has no way to ask for the same mapping.

**Remedy.** Neutral kind metadata: either a `_logit.metrics.` key such as
`_logit.metrics.type: gaugehistogram | info | stateset`, set by `prometheus_in`, or a model field
on `MetricRecord`. `otlp_out` reads the neutral form. `prometheus_out` keeps its own carrier for
the two values only it needs (`untyped`, `unknown`).

Open design questions:

- Attribute or model field. A field costs bytes on every `MetricRecord` and a perf VM
  measurement under [ADR `event-sizing-and-allocation-strategy`](../adr/event-sizing-and-allocation-strategy.md);
  an attribute is visible to Lua and `set` at no size cost.
- Whether `prometheus_out` reads the neutral key as well, so a non-Prometheus producer gets
  OpenMetrics' `info` and `stateset` types there too.
- Whether other sinks with a matching concept (`datadog_out`, `influxdb_out`) should honor the
  key; out of scope unless one needs it.

**What else moves.** An amendment to
[ADR `prometheus-scrape-and-exposition`](../adr/prometheus-scrape-and-exposition.md); the transit
spec's `gauge_histogram`, `prometheus_info`, and `prometheus_stateset` rows in
`spec/transit/mapping.yaml` and `script/transit gen`; `otlp_out`'s module doc;
[data-model.md](../design/data-model.md); the known-gaps row's C3.

**Estimate.** Two PRs: the ADR amendment (S), then the decoder, the encoder, and the docs (M). A
model field instead of an attribute adds a VM measurement.

## C6: `collectd.host` in `datadog_out`

**Future workstream, not scheduled.**

**What reads what.** `collectd_in` stamps each value list's and notification's host as the public
`collectd.host`. `datadog_out` takes it as the host of a series, event, service check, or log,
below `host.name`, a DogStatsD `host:` tag, and the hostless marks: `collectd_host` in
`crates/logit-proto/src/datadog/mod.rs`, called from the series, events, and service-check
encoders, with the logs encoder (`datadog/logs.rs`) reading it after its own host attributes
([ADR `process-hostname`](../adr/process-hostname.md)'s "Amendment: `datadog_out` reads
`collectd.host` (2026-10-22)").

**What breaks if Lua or `set` writes the key.** Any event carrying a `collectd.host` attribute,
from a `json` log line or a script, becomes that host at `datadog_out`, and the Datadog codec
imports `logit_proto::collectd::ATTR_HOST`. The read is also asymmetric: no other foreign sink
reads `collectd.host`, so the same relay sends collectd's host to Datadog and not to Splunk.

**Remedy.** One of:

- Revert the amendment. The operator copies `collectd.host` to `host.name` ahead of
  `datadog_out` with a `lua` stage, the composable route; `examples/` or
  [docs/datadog.md](../datadog.md) carries the recipe.
- `collectd_in` stamps a neutral host key that every host-reading sink can read.

Open design questions:

- A neutral host key would be new vocabulary, and `host.name` itself isn't a candidate:
  [ADR `private-attribute-namespace`](../adr/private-attribute-namespace.md) keeps collectd's host
  off the resource so that it stays per-event identity, and a `host.name` attribute would render
  as a second label or tag at `prometheus_out`, `influxdb_out`, and `graphite_out`.
- Whether the recipe wants a native copy or rename component, which doesn't exist today, and
  would need its own ADR.

**What else moves.** A second amendment to [ADR `process-hostname`](../adr/process-hostname.md);
the Datadog codec's module doc and `datadog_out`'s host rows; the transit spec's `string_value`
and `no_host` cells for `datadog_out` in `spec/transit/mapping.yaml` and `script/transit gen`;
[docs/datadog.md](../datadog.md); the known-gaps row's C6.

**Estimate.** One PR for the revert and recipe (M), or two with a neutral key (an ADR, then the
code).

## Allowed, in family

These read another component's internals inside the Datadog family and stay as they are:

- **C4**: `statsd_out` writes an empty DogStatsD `host:` tag for `_logit.datadog.hostless`
  (`crates/logit-outputs/src/statsd.rs`, importing `logit_proto::datadog::ATTR_HOSTLESS`).
- **C5**: `datadog_out` routes an event carrying `_logit.statsd.event.*` to the events API
  (`is_datadog_event`) and one carrying `_logit.statsd.service_check.*` to `check_run`
  (`is_service_check`), both in `crates/logit-proto/src/datadog/mod.rs`; `statsd_out` writes the
  same carriers as `_e{}` and `_sc` lines, whether `statsd_in` or `datadog_in` wrote them.
- **C7**: `datadog_out` sends a trace chunk only when `trace_readiness`
  (`crates/logit-proto/src/datadog/mod.rs`) finds the Agent's marks, reading
  `datadog_trace_in`'s `_logit.datadog.tracer.*` and `_logit.datadog.agent.*` carriers.
- **C8**: `datadog_out` and `datadog_trace_out` route APM stats on `_logit.datadog.stats.name`
  (`is_datadog_stats`).

Follow-up, not a rule question: `is_datadog_stats` does an `interner::lookup` per call, per event,
in `datadog_out` and `datadog/series.rs`. `is_datadog_stats_sym` takes the symbol once and has no
caller outside `mod.rs` now that `aggregate` doesn't use it
([metric-flags plan](metric-flags.md)'s "Findings").
