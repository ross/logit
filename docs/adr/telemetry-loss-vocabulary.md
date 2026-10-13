---
created: 2026-10-13
updated: 2026-10-13
---

# Telemetry loss vocabulary: any nonzero `*.dropped` is loss, and intended outcomes use other words

## Status
Accepted

## Context
`logit`'s own telemetry ([ADR `internal-telemetry-as-pipeline-events`](internal-telemetry-as-pipeline-events.md),
[internal-telemetry.md](../design/internal-telemetry.md)) names loss inconsistently, in both
directions:

- **`*.dropped` counts outcomes the config asked for.** `logit.component.events.dropped` carries
  `absorbed` (a filter, `sample`, or `aggregate` removed the event), `script_drop` (a Lua script
  returned no event), and `unrouted` (a router had no ordinary consumer for an event no route
  claimed), beside real loss: `shutdown`, `rejected`, `overflow_oldest`, and the disk spool's
  faults. A sink's `tags.dropped` counts a field the wire format has no form for, on a record
  that's still delivered, and `input.batches.dropped{reason="busy"}` counts a `503` the client
  retries.
- **Real loss hides under other names.** A malformed point is
  `input.metrics.skipped{reason="bad_point"}`. A record a destination refused inside a `2xx` is
  `output.records.rejected`. The kernel's socket drops are `input.kernel.drops`. An event a Lua
  script raised an error on is lost and counted only as `component.errors{reason="process"}`.
  An unresolved relative gauge is `output.metrics.skipped{metric_kind="gauge_delta"}` in six
  sinks and `messages.dropped{reason="unresolved_gauge_delta"}` in `statsd_out`.

An operator can't alert on `*.dropped` without reading every reason, and can't find all loss by
looking at `*.dropped`. `crates/logit-cli/src/undrained.rs` already works around this: its
`INTENDED_REASONS` and `INTENDED_METRICS` exclusion lists decide which leftover `*.dropped`
points at exit are loss, and any reason added later warns until someone classifies it.

The soak harness ([ADR `soak-harness`](soak-harness.md)) reads the same counters as its loss
oracle and carries its own copy of the classification.

## Decision
**Any nonzero `*.dropped` is a problem to investigate. An intended outcome never uses the word.**
The name says whether a count is loss; no reader classifies reasons.

`logit` is pre-release, so every rename below is a plain rename: no aliases, no deprecated
second name, and no compatibility window.

### The vocabulary

The vocabulary is `*.dropped` for loss, `component.events.discarded` for an event removed by
design, `*.omitted` for a piece removed from a delivered record, `*.skipped`, `*.degraded`,
`*.truncated`, `*.normalized`, `*.evicted`, `*.untracked`, `input.batches.deferred`, and the
unchanged `input.connections.rejected` and `input.requests.rejected`. Each name's meaning and tags
are in one table, the operator's copy:
[internal-telemetry.md](../design/internal-telemetry.md#loss-vocabulary)'s "Loss vocabulary".
`*.dropped` always carries `reason` and never `metric_kind`, so a loss is always attributed to a
cause.

Three rules hold across the vocabulary:

- **A reason value never contains `dropped`.** The word appears only in a loss metric's name,
  so a search for it finds loss and nothing else. `exemplar_dropped` becomes `exemplar_omitted`,
  and `collectd_out`'s `notification_dropped` reason becomes `invalid_notification`. Other
  vocabulary words can name a cause (`rejected`, `truncated`, `exemplar_omitted`).
- **Diagnostic keys are a separate vocabulary and don't change.** A key such as
  `gauge_delta_unresolved` or `encrypted_packet_dropped` names a throttled self-log line, not a
  count.
- **`skipped` versus `dropped`.** Use `skipped` only when nothing was there to carry
  (`no_recorded_value`, `empty_sketch`), the operator chose it (`multi_value: skip`, an event
  under `statsd_out`'s `format: statsd`), or the destination format has no form for the kind
  (carbon has no set, collectd has no histogram). A gap in `logit`'s own implementation
  (`native_histogram`, `idx_payload`, `interpolation`) or a missing upstream stage (an unresolved
  relative gauge, delta temporality at a cumulative-only destination) is `dropped`, because a
  different `logit` or a different config would have delivered the data.

### Decisions the vocabulary forces

- **`component.events.discarded` is untagged and emitted only when nonzero.** It replaces
  `events.dropped{reason="absorbed"}` and `{reason="script_drop"}`. A reason would be constant
  per kind. For `aggregate`, a discarded event's metrics were folded into window state and
  reappear in a later flush; `logit.transform.metrics.absorbed` still counts the records it
  folded.
- **A component-specific counter exists only where it adds why or how.**
  `logit.transform.events.filtered` duplicates `events.discarded` on every filter and `sample`,
  so it's removed. `logit.transform.sample.decisions{outcome, by}` stays, because `by` says why,
  with the outcome `dropped` renamed `discarded`. `logit.transform.payloads.stripped{signal}`
  stays, because the event survives a strip.
- **An unrouted event is a config error, not a count.** Graph rule 7 (a non-sink component needs a
  consumer) loses rule 50's router exemption: a router (`route`, or `lua` with `targets:`) needs an
  ordinary consumer, which receives every event no target claims. An operator who wants those
  events gone names `null_out` as that consumer, so the choice is visible in the graph.
  `events.dropped{reason="unrouted"}` and the runtime branches that count it go away
  ([ADR `target-components`](target-components.md) is amended when this lands).
- **Hidden loss moves into `*.dropped`:**
  - A Lua `process` error counts `component.events.dropped{reason="script_error"}`, beside the
    `component.errors{reason="process"}` it already counts.
  - `output.records.rejected` (an OTLP `partial_success`, a Datadog series `202` naming entries)
    becomes `output.records.dropped{reason="rejected"}`, keeping its `signal` or `route` tag.
  - `input.kernel.drops` becomes `input.kernel.dropped{reason="socket"}`. It stays separate from
    `component.datagrams.dropped`, because the kernel discarded those datagrams before `logit`
    read them, and the listener identity (`input.datagrams` equals the `receive.latency` sample
    count plus every `datagrams.dropped` reason) counts only datagrams `logit` read.
  - A datagram the kernel truncated (`MSG_TRUNC`) is no longer decoded. The read side counts it
    `component.datagrams.dropped{reason="truncated"}` (and `bytes.dropped` with it) instead of
    queueing it, so it gets no `receive.latency` sample and the identity still holds.
    `input.datagrams.truncated` is removed.
  - An unresolved relative gauge is `output.metrics.dropped{reason="unresolved_gauge_delta"}` in
    every sink but `statsd_out`, which counts the same reason under `output.messages.dropped`,
    as it already did. A delta `Sum` or `Histogram` at
    `prometheus_out` is `output.metrics.dropped{reason="unresolved_delta"}`.
- **`logit.transform.metrics.absorbed` keeps its name.** It isn't loss, and the soak ledger's
  aggregate identity reads it.

## Alternatives considered
- **Keep one counter and classify reasons downstream.** This is what `undrained.rs`'s exclusion
  lists do today. Every consumer (the exit check, the soak ledger, a dashboard, an operator's alert)
  re-derives the classification, and a new reason defaults to the wrong class until each copy is
  updated.
- **Count unrouted events as an intended outcome** (`events.discarded`). A router with no consumer
  for unclaimed events is a config choice, and a config choice should be visible in the graph,
  not inferred from a counter.
- **Tag `events.discarded` with a reason.** The reason would be constant per component kind, so
  the tag adds a series and no information.

## Consequences
- Alerting reduces to one rule: alert on any nonzero `*.dropped`. `undrained.rs` loses both
  exclusion lists, and the soak ledger reads every `*.dropped` as loss.
- Every renamed counter moves its prose with it in the same change: the tables in
  [internal-telemetry.md](../design/internal-telemetry.md), each codec's module-doc mapping table,
  `logit-config` field docs and the generated schema, `docs/deploying.md`, `docs/known-gaps/`, and
  amendments to ADRs that name a counter
  ([`target-components`](target-components.md),
  [`shutdown-accounting-and-cancellation-safety`](shutdown-accounting-and-cancellation-safety.md),
  [`lossless-transit`](lossless-transit.md), [`sink-fault-classes`](sink-fault-classes.md),
  [`delivery-semantics`](delivery-semantics.md),
  [`signal-filtering-components`](signal-filtering-components.md),
  [`consistent-sampling-component`](consistent-sampling-component.md)). Closed plans under
  `docs/plans/` keep the old names as history.
- A renamed counter breaks a negative check silently: a test asserting the old name is zero, or a
  soak check reading a missing point as 0, still passes. Each change pairs every negative
  assertion with a positive one on the new name.
- Some loss is counted only by a diagnostic today and has no counter at all (see
  [Loss with no counter](#loss-with-no-counter)). The rule says it belongs under `*.dropped`;
  adding those counters is follow-up work this record tracks.

## Amendment (2026-10-13): counters for decode-side loss

The decode failures the "Loss with no counter" list named now count under `*.dropped`, each beside
the diagnostic it already raised, which keeps its key:

| Loss | Counter | Diagnostic |
|---|---|---|
| A datagram or framed message the decoder rejects whole, in the shared UDP, Unix, and TCP drivers (`statsd_in`'s invalid UTF-8, `collectd_in`'s first part malformed, `graphite_in`'s undecodable pickle frame) | `input.frames.dropped{reason="undecodable"}` | `bad_datagram`, `bad_frame` (and `bad_pickle`) |
| A line or message `statsd_in` or `syslog_in` rejects; the rest of its datagram or frame still decodes | `input.messages.dropped{reason="malformed"}` | `bad_line` |
| `collectd_in`: the rest of a datagram behind a malformed part, after decoded lists | `input.frames.dropped{reason="malformed_part"}` | `bad_part` |
| `collectd_in`: the rest of a datagram behind an Encryption part | `input.frames.dropped{reason="encrypted"}` | `encrypted_packet_dropped` |
| `collectd_in`: a value list with an empty host, plugin, or type, one per data source | `input.metrics.dropped{reason="incomplete_identity"}` | `incomplete_identity` |
| `collectd_in`: a notification that fails validation | `input.events.dropped{reason="invalid_notification"}` | `notification_dropped` |

`input.frames.dropped` already counted a frame the TCP framer refused (`oversize`, `drained`,
`malformed`, `truncated`), and `lines_in` counts an oversize line under it on every transport, so a
frame lost after framing extends the same counter rather than adding a name. On a datagram
transport the frame is the datagram. It isn't `component.datagrams.dropped`: a datagram the decoder
rejected was dequeued and has its `receive.latency` sample, so counting it there would break the
UDP listener identity. `input.messages.dropped` mirrors `output.messages.dropped`, which counts a
statsd line or a syslog message on the sink side. `bad_part` and `incomplete_identity` weren't on
the list but lose data the same way, so they get counters too.

## Amendment (2026-10-13): a counter for a disk spool's torn tail

The disk spool's torn tail, from the "Loss with no counter" list, now counts
`component.batches.dropped{reason="disk_torn"}`: one per torn tail `DiskQueue::open` truncates,
unless the persisted cursor shows the torn record was committed before the crash. The sink counted `batches.received` for that batch before its push, and no
process counted it delivered or dropped, so it's loss. A partial record holds no readable event
count, so no `events.dropped` is counted with it. `component.buffer.disk.truncated` stays the
count of repairs. [ADR `disk-backed-sink-buffer`](disk-backed-sink-buffer.md)'s amendment of the
same date has the bounds of the count and how it enters the sink identity.

## Amendment (2026-10-13): close-out

- **The vocabulary table lives in one place,**
  [internal-telemetry.md](../design/internal-telemetry.md#loss-vocabulary)'s "Loss vocabulary",
  and `docs/deploying.md`'s "Alerting on loss" gives the one alerting rule. This record keeps the
  decision, its rules, and the migration record.
- **Two classifications stay as the migration record has them**, by the maintainer's call:
  - `prometheus_out`'s `output.metrics.degraded{reason="sub_ms_collapsed"}` stays `degraded`.
    Of two readings of one series whose timestamps share a millisecond, the series is delivered
    with the later one, because the destination holds one sample per millisecond.
  - `datadog_out`'s cumulative and non-monotonic `Sum` stay
    `output.metrics.skipped{metric_kind}`, though "missing upstream stage" would make them
    `dropped`. No `logit` stage converts either to a delta monotonic `Sum` a Datadog `count` can
    carry, so an operator alerted on them has nothing to change until one exists.
- **`output.metrics.type_conflict` keeps its name** (a row in the Outputs table). It counts a
  family `prometheus_out`'s registry replaced because a later batch re-typed it: the new type
  wins and the old type's series are evicted, because exposition allows one type per family.
- **The soak harness reads every `*.dropped` as loss.** Its `drops` row surfaces every
  `logit.*.dropped` counter the system under test emits that no ledger row balances
  ([ADR `soak-harness`](soak-harness.md), `tools/soak/README.md`).
- **[Loss with no counter](#loss-with-no-counter) keeps three entries.** The `influx` stream,
  which rewrites `influxdb_out` and adds `influxdb_in`, adopts this vocabulary for both, so
  `influxdb_out`'s counters land there rather than here. `kafka_out`'s `format: text` and
  `docker_in`'s decoder are follow-ups.

## Amendment (2026-10-13): counters for `kafka_out`'s text format and `docker_in`'s decoder

Two entries leave [Loss with no counter](#loss-with-no-counter), which keeps only `influxdb_out`:

| Loss | Counter |
|---|---|
| `kafka_out` under `format: text`: the metric records and span of an event whose log body a record carries | `output.metrics.omitted{reason="format"}`, one per metric record, and `output.spans.omitted{reason="format"}`, one per span |
| `docker_in`'s decoder: a reassembled message over `max_line_bytes` (diagnostic `long_line`) | `input.frames.dropped{reason="oversize"}`, one per message, when its drop starts |
| `docker_in`'s decoder: a held reassembly discarded at an in-place truncation | `input.frames.dropped{reason="truncated"}`, one per stream that held one |

- **`kafka_out`'s piece is `omitted`, not `skipped`.** The list placed it under `*.skipped`, but
  the encoder writes a record with the log body, and a piece removed from a record that's
  otherwise sent is what `*.omitted` means. The `format` reason says the operator chose it. A metrics-only event
  still counts `output.events.skipped{reason="no_log"}` and nothing more, because no record is
  written for it. Both counters are encode-side, counted once per batch however many attempts
  the sink makes, so a record lost after encoding (under `record: event`, one the producer drops
  as oversize, or one the broker rejects or names) has still counted its omitted pieces, beside
  its `records.dropped`. An event whose own value is over the budget under `record: batch`
  counts `records.dropped{reason="oversize"}` instead.
- **`docker_in`'s counters reuse `tail_in`'s names.** The decoder counts a message, where the
  driver counts a line, and both are a frame the listener lost. A re-read after a restart
  re-drops an oversize message and counts it again, as `tail_in` counts a long line each time it
  reads it. A stream already dropping at a truncation counts nothing more: its message was
  counted when the drop started.
- **A truncation counts a held reassembly and the unterminated line behind it once.** The driver
  counts the line splitter's unterminated line only when the decoder holds no entry, because that
  line most often continues the held one. An unterminated line on the other stream, beside a
  held reassembly or a drop in progress, goes uncounted (`docs/known-gaps/tailing.md`).

## Migration record
The classification of every `*.dropped`, `*.skipped`, `*.degraded`, `*.truncated`, and loss-related
`*.rejected` name and reason that existed on 2026-10-13, verified against the emit sites in
`crates/` (test code excluded). Every name starts with `logit.`; the prefix is left out below. A
row reading "unchanged" keeps its name and every reason listed.

### Inputs

| Component | Old (metric, reason) | New (metric, reason) |
|---|---|---|
| UDP listeners | `component.datagrams.dropped`, `component.bytes.dropped` (`overflow_oldest`, `overflow_newest`, `shutdown`) | unchanged |
| UDP listeners | `input.datagrams.truncated` (untagged, prefix still decoded) | `component.datagrams.dropped`, `component.bytes.dropped` (`truncated`), not decoded |
| UDP listeners | `input.kernel.drops` (untagged) | `input.kernel.dropped` (`socket`) |
| TCP framing, `lines_in` | `input.frames.dropped` (`oversize`, `drained`, `malformed`, `truncated`) | unchanged |
| network listeners | `input.connections.rejected` (`limit`, `proxy_header`) | unchanged |
| HTTP listeners | `input.requests.rejected` (every reason) | unchanged |
| `datadog_in`, `datadog_trace_in`, `splunk_hec_in` | `input.batches.dropped` (`busy`) | `input.batches.deferred` (untagged) |
| HTTP listeners, `tail_in`, `docker_in`, `logit_in` | `input.batches.dropped` (`closed_consumer`, `rejected`) | unchanged |
| `splunk_hec_in` | `input.acks.dropped` (`expired`) | `input.acks.evicted` (`expired`) |
| `splunk_hec_in` | `input.acks.dropped` (`evicted`: ids evicted with their channel) | `input.acks.evicted` (`channel`) |
| `tail_in`, `docker_in` | `input.files.truncated` (a tailed file shrank in place, so it's reread from offset 0) | unchanged: a file event, not loss. The partial line it held from before the truncation is discarded, which now counts `input.frames.dropped` (`truncated`) |
| `tail_in`, `docker_in` | a line over `max_line_bytes`, dropped by the line splitter with diagnostic `long_line` only | `input.frames.dropped` (`oversize`), as `lines_in` counts one |
| `prometheus_in` | `input.metadata_cache.truncated` | unchanged |
| `graphite_in` | `input.metrics.skipped` (`bad_line`, `bad_tag`, `bad_timestamp`, `non_finite_value`, `bad_shape`) | `input.metrics.dropped`, same reasons |
| `graphite_in` | `input.tags.normalized` (`duplicate_key`) | unchanged |
| `otlp_in` | `input.metrics.skipped` (`metric_kind="unknown"`, `no_data`) | unchanged |
| `datadog_in` series, sketches | `input.metrics.skipped` (`bad_series`, `bad_point`, `non_finite_value`, `bad_sketch`, `legacy_distribution`) | `input.metrics.dropped`, same reasons |
| `datadog_in` series, sketches | `input.metrics.skipped` (`null_value`, `empty_distribution`, `empty_sketch`) | unchanged |
| `datadog_in` service checks | `input.metrics.skipped` (`malformed`, `no_name`, `invalid_status`) | `input.metrics.dropped`, same reasons |
| `datadog_in` | `input.metrics.degraded` (`no_timestamp`) | unchanged |
| `datadog_in` events | `input.events.skipped` (`malformed`, `no_title`) | `input.events.dropped`, same reasons |
| `datadog_in` logs | `input.logs.skipped` (`not_an_object`, `no_message`) | `input.logs.dropped`, same reasons |
| `datadog_in` traces | `input.spans.skipped` (`malformed`, `idx_payload`) | `input.spans.dropped`, same reasons |
| `datadog_in`, `datadog_trace_in` | `input.spans.degraded` (every reason) | unchanged |
| `datadog_in` APM stats | `input.stats.skipped` (`malformed_payload`, `malformed_bucket`, `malformed_group`, `interpolation`, `bad_sketch`) | `input.stats.dropped`, same reasons |
| `datadog_in` APM stats | `input.stats.skipped` (`empty_bucket`), `input.stats.degraded` (every reason) | unchanged |
| `prometheus_in` | `input.metrics.skipped` (`malformed_line`, `malformed_metadata`, `duplicate_type`, `duplicate_metadata`, `duplicate_series`, `duplicate_label`, `unknown_suffix`, `incomplete_series`, `empty_histogram`, `non_monotonic_buckets`, `invalid_labels`, `native_histogram`) | `input.metrics.dropped`, same reasons |
| `prometheus_in` | `input.metrics.degraded` (`exemplar_dropped`) | `input.metrics.degraded` (`exemplar_omitted`) |
| `prometheus_in` | `input.metrics.degraded` (`seed_mismatch`, `histogram_count_mismatch`) | unchanged |
| `splunk_hec_in` | `input.metrics.skipped` (`bad_name`, `bad_value`, `duplicate_name`, `incomplete_single_metric`) | `input.metrics.dropped`, same reasons |
| `splunk_hec_in` | `input.events.skipped` (`no_event`, `blank_event`) | `input.events.dropped`, same reasons |
| `splunk_hec_in` | `input.events.skipped` (`no_metric`), `input.events.degraded`, `input.spans.degraded` (every reason) | unchanged |

### Outputs

A kind a sink can't carry is classified by cause. Under an operator's `multi_value: skip`
(`graphite_out`, `splunk_hec_out`, and `statsd_out`), or where the wire format has no form for the
kind (`collectd_out`'s summarized kinds and its non-monotonic delta `Sum`
(`metric_kind="non_monotonic_delta_sum"`), since no collectd data-source type can go down, `otlp_out`'s `set` and `set_members`, `datadog_out`'s
series route, `prometheus_out`'s exposition dialects, and `splunk_hec_out`'s
`exponential_histogram`, as the OTel `splunk_hec` exporter does), it stays
`output.metrics.skipped{metric_kind}`. A row naming several sinks leaves off their `_out` suffix.

| Sink | Old (metric, reason) | New (metric, reason) |
|---|---|---|
| graphite, collectd, otlp (and `kafka_out`'s OTLP formats), datadog, prometheus, splunk | `output.metrics.skipped` (`metric_kind="gauge_delta"`) | `output.metrics.dropped` (`unresolved_gauge_delta`) |
| `prometheus_out` | `output.metrics.skipped` (`metric_kind="delta_sum"`, `"delta_histogram"`) | `output.metrics.dropped` (`unresolved_delta`) |
| `prometheus_out` remote-write | `output.metrics.skipped` (`metric_kind="exponential_histogram"`) | `output.metrics.dropped` (`native_histogram`) |
| `prometheus_out` exposition | `output.metrics.skipped` (`metric_kind="exponential_histogram"`) | unchanged |
| `splunk_hec_out` | `output.metrics.skipped` (`metric_kind="no_recorded_value"`) | `output.metrics.skipped` (`reason="no_recorded_value"`) |
| every sink that counts them | `output.metrics.skipped` (`no_recorded_value`, `stale`, `empty_sketch`) | unchanged |
| graphite, collectd, prometheus, datadog, splunk | `output.metrics.skipped` (`unencodable_value`, `unencodable_timestamp`, `empty_name`, `oversize_line`, `oversize_datapoint`, `oversize_value_list`, `oversize_notification`, `too_many_values`, `no_host`, `empty_message`, `invalid_status`, `non_finite_value`, `oversized_sketch`, `type_conflict`, `name_collision`, `invalid_labels`, `no_timestamp`, `duplicate_name`) | `output.metrics.dropped`, same reasons |
| `collectd_out` | `output.metrics.skipped` (`notification_dropped`) | `output.metrics.dropped` (`invalid_notification`) |
| every sink that counts them | `output.metrics.degraded` (every `metric_kind`; `set_estimate`, `rebinned`, `fractional_count`, `sample_rate_expanded`, `unit_not_suffix`, `sub_ms_collapsed`) | unchanged |
| `prometheus_out` | `output.metrics.degraded` (`exemplar_dropped`) | `output.metrics.degraded` (`exemplar_omitted`) |
| `prometheus_out` registry | `output.metrics.type_conflict` (untagged: a family re-typed across batches, its old series cleared) | unchanged |
| graphite, splunk | `output.metrics.normalized` (every reason) | unchanged |
| `statsd_out` | `output.messages.dropped` (`unresolved_gauge_delta`, `unencodable_value`, `empty_name`, `oversize_line`, `invalid_service_check`) | unchanged |
| `statsd_out` | `output.messages.dropped` (`unencodable_value` for an out-of-range `sample_rate`, where the line is still sent without `@rate`) | `output.fields.omitted` (`invalid_value`) |
| `statsd_out` | `output.messages.dropped` (`dialect_event`) | `output.messages.skipped` (`dialect`) |
| `statsd_out` | `output.messages.dropped` (`no_recorded_value`) | `output.messages.skipped` (`no_recorded_value`) |
| `statsd_out` | `output.messages.dropped` (`dialect_field`) | `output.fields.omitted` (`dialect`) |
| `statsd_out` | `output.messages.dropped` (`invalid_event_field`) | `output.fields.omitted` (`invalid_value`) |
| statsd, syslog, graphite, collectd | `output.messages.dropped` (`oversize_datagram`) | unchanged |
| `syslog_out` | `output.messages.dropped` (`oversize_header`) | unchanged |
| statsd, graphite, collectd, datadog, `datadog_trace_out`, splunk | `output.tags.dropped` (every reason: `dialect`, `unrepresentable`, `empty`, `collision`, `no_wire_form`, `reserved_key`) | `output.tags.omitted`, same reasons |
| `prometheus_out` | `output.labels.dropped` (`unrepresentable`, `reserved`, `collision`, `empty_value`) | `output.labels.omitted`, same reasons |
| `syslog_out` | `output.structured_data.dropped` (`invalid_sd_name`, `not_a_map`, `sd_id_collision`) | `output.structured_data.omitted`, same reasons |
| influxdb, graphite, prometheus | `output.tags.normalized`, `output.labels.normalized`, `output.messages.normalized` (every reason) | unchanged |
| syslog, collectd | `output.messages.truncated` | unchanged |
| statsd, syslog, splunk, kafka | `output.events.skipped` (untagged, `no_payload`, `no_log`) | unchanged |
| `splunk_hec_out` | `output.events.skipped` (`blank_event`) | `output.events.dropped` (`blank_event`) |
| datadog, `datadog_trace_out` | `output.stats.skipped` (`unrecognized_record`) | `output.stats.dropped` (`unrecognized_record`) |
| datadog, `datadog_trace_out`, splunk | `output.stats.degraded`, `output.spans.degraded` (every reason) | unchanged |
| otlp, datadog, `datadog_trace_out`, splunk, kafka | `output.records.dropped` (every reason) | unchanged |
| `otlp_out` | `output.records.rejected` (`signal`) | `output.records.dropped` (`signal`, `rejected`) |
| `datadog_out` | `output.records.rejected` (`route="series"`) | `output.records.dropped` (`route="series"`, `rejected`) |
| `splunk_hec_out` | `output.requests.rejected` (`code`) | unchanged; the records lost are counted under `records.dropped` or the runtime's `batches.dropped` |

### Runtime, transforms, and `internal`

| Area | Old (metric, reason) | New (metric, reason) |
|---|---|---|
| transforms (`process_batch`) | `component.events.dropped` (`absorbed`) | `component.events.discarded` (untagged) |
| `lua` | `component.events.dropped` (`script_drop`) | `component.events.discarded` (untagged) |
| `lua` | an event lost to a `process` error, counted only as `component.errors` (`process`) | `component.events.dropped` (`script_error`), and `component.errors` unchanged |
| routers | `component.events.dropped` (`unrouted`) | removed: a router with no ordinary consumer is a config error |
| fan-out, `lua` | `component.events.dropped` (`closed_consumer`) | unchanged |
| sinks, queues, disk spool | `component.events.dropped`, `component.batches.dropped` (`shutdown`, `rejected`, `ambiguous_at_most_once`, `overflow_oldest`, `overflow_newest`, `frame_too_large`, `disk_full`, `disk_io_error`, `disk_corrupt`) | unchanged |
| disk spool | `component.buffer.disk.truncated` (untagged: a torn tail cut at open) | unchanged |
| filters, `sample` | `transform.events.filtered` | removed; `component.events.discarded` counts the same events |
| `sample` | `transform.sample.decisions` (`outcome="dropped"`) | `transform.sample.decisions` (`outcome="discarded"`) |
| `keep_signals`, `drop_signals` | `transform.payloads.stripped` | unchanged |
| `keep`, `remove` | `transform.attributes.dropped` (untagged) | `transform.attributes.omitted` (untagged) |
| `aggregate` | `transform.links.dropped` (`contexts`) | `transform.links.omitted` (`contexts`) |
| `aggregate` | `transform.samples.non_finite_dropped` | `transform.samples.omitted` (`non_finite`) |
| `aggregate` | `transform.metrics.absorbed`, `transform.metrics.passed_through`, `transform.series.evicted` | unchanged |
| `shape` | `transform.batches.dropped` (untagged) | `transform.batches.untracked` (untagged) |
| transforms | `transform.{scaled,matched,derived,rows,timestamp,trace_context}.skipped` (every reason: the event is forwarded untouched) | unchanged |
| `logfmt`, `kv` | `transform.pairs.skipped` (untagged: an empty segment, an empty key, or a bareword under `bare_keys: false`) | unchanged: the token adds no attribute, but the message keeps it, and nothing was there to carry or the operator chose it |
| transforms | `transform.values.normalized`, `transform.http_access.{normalized,truncated}` | unchanged |
| `internal` | `internal.points.dropped` (`cardinality`), `internal.spans.dropped` (`buffer_full`), `internal.logs.dropped` (`buffer_full`) | unchanged |
| `internal` | `internal.span.links.dropped` (`cardinality`) | `internal.span.links.omitted` (`cardinality`) |

### Loss with no counter
These lose data and report it only through a diagnostic, or not at all. Under this rule each
belongs under `*.dropped`, or under `*.skipped` where the entry says so; none has a counter yet:

- `influxdb_out` reports what it doesn't write with diagnostics only. An unresolved relative
  gauge (`gauge_delta_unresolved`) and a metric that fails to encode (`encode_error`) belong
  under `*.dropped`. A `NO_RECORDED_VALUE` record (`no_recorded_value`: nothing to carry) and
  `SetMembers` and `ExponentialHistogram` (no line-protocol form, though they share the
  `encode_error` key) belong under `*.skipped`.
