# Transit spec

This directory states what each sink does with each metric kind it receives, and checks that
statement against the code. A **case** is a batch of events as a source's listener produces them.
A **flow** is a `logit` config that runs cases through stages into a sink. The **mapping table**,
[`mapping.yaml`](mapping.yaml), gives every kind and feature a class per sink. A test per flow runs
every case through the flow, reads the sink's bytes back through its pair's decoder, pins the result,
and fails when the result disagrees with the table.

The spec covers the codec layer: an encoder's bytes and the pair decoder's reading of them. It
doesn't cover sink behavior above the encoder (retries, request packing, stale-point drops) or what
a backend stores. [ADR `transit-spec`](../../docs/adr/transit-spec.md) records the decision and its
limits, and [the plan](../../docs/plans/transit-spec.md) the workstreams.

The sources are `statsd_in`'s metrics (`cases/statsd/`) and its DogStatsD events, with
hand-written spans and payload-less events beside them (`cases/statsd-logs/`), `influxdb_in`,
`prometheus_in`'s scrape (remote-write cases aren't written yet), `datadog_in`'s metrics routes and service checks (`cases/datadog/`) and its
logs and events (`cases/datadog-logs/`: the Agent's captures and hand-written `/api/v2/logs`
bodies) and its APM traces (`cases/datadog-traces/`: the Agent's capture and hand-written event
JSON lines), with no APM stats cases, `collectd_in`, `otlp_in`'s metrics
(`cases/otlp/`), logs (`cases/otlp-logs/`), and traces (`cases/otlp-traces/`), `graphite_in`,
plaintext and pickle, `syslog_in`, RFC 3164 and RFC 5424 over UDP and as a TCP stream, and
`splunk_hec_in`'s `/event` and `/raw` routes (`cases/splunk/`: the four recorded HEC clients'
captures and hand-written bodies), with no metric cases. A
source directory's name before its first `-` names its system, so
`datadog-logs` cases decode through `datadog_in`. The sink columns are `statsd_out`,
`prometheus_out` (exposition and remote-write), `influxdb_out`, `otlp_out`, `datadog_out`,
`datadog_trace_out` (`version: v0.4` and `v0.7`), `graphite_out`, `syslog_out`, `splunk_hec_out`,
and `kafka_out` (`format: json` and `text`); the last three have cells on the log, span, event,
and attribute rows and none on a metric kind's. `datadog_trace_out` sends spans alone, so beside
its span and attribute cells it has only the `skipped` cells of the log and gauge rows a span
reaches beside it, and the driver reads its requests back through `datadog_trace_in`'s own
decoding with the headers the sink sent. A
log record's rows have cells at `otlp_out`, `datadog_out`, `syslog_out`, `splunk_hec_out`,
`kafka_out`, and, for a DogStatsD event, `statsd_out`. `kafka_out` has no pair input, so the
driver reads its records back by convention: a `format: json` record through the event JSON
reader, and a `format: text` record as one log holding the record's value, at the record's
millisecond timestamp (`crates/logit-cli/src/transit/wire.rs`). Each flow's name says which
source meets which sink: `statsd-relay`, `statsd-logs-relay`, `influxdb-relay`, `datadog-relay`,
`datadog-logs-relay`, `datadog-traces-relay`, `otlp-relay`, `otlp-logs-relay`,
`otlp-traces-relay`, `graphite-relay`, `syslog-relay`,
`splunk-relay`, `prometheus-to-prometheus`, and `prometheus-to-prometheus-remote-write` are like
pairs, and `influxdb-to-prometheus` a cross-protocol one.

`datadog_out` makes two drops above its encoders that the driver leaves out: `stale` and
`too_many_tags`, which read the wall clock or apply an intake limit. It also cuts a route's events
into requests by size, dropping an event too large to send alone as `oversize`. None of these has
a row, and the driver encodes each route in one request without them. The driver does apply the
trace readiness gate, which judges a whole trace chunk: a span it holds back is counted as the
sink counts it and reads `dropped` on `span/not_datadog_origin` or `span/needs_agent_processing`.
The APM stats routes have no case. `crates/logit-outputs/src/datadog.rs`'s module doc has the
drops and the cuts. `datadog_trace_out` has no readiness gate, and the driver sends a batch's
spans as one trace request without the sink's cut by trace count and size, which drops a trace
too large to send alone as `oversize` (`crates/logit-outputs/src/datadog_trace.rs`'s "Size
limits").

## Layout

Expectations are one file per flow, not one per case: a file per case grows as flows times cases,
which reaches thousands once every source meets every sink column, while a file per flow grows
with the flows alone. Each case's document sits under its own key on its own lines, so a diff
still shows which case changed. A missing file fails naming the flow and its path; a case with no
key, a stale document, and a key no case has each fail naming the flow and the case.

| Path | What it holds |
|---|---|
| `mapping.yaml` | The table: the class vocabulary, the sinks, and one row per kind and feature |
| `features/<system>.yaml` | The wire features a source's cases can exercise, one `{name, text}` each |
| `cases/<system>/<case>.meta.yaml` | What the case exercises, and its wire file, a collectd case's `types_db:` file, a graphite case's `protocol:`, a syslog case's `framing:`, and tolerances if any |
| `cases/<system>/<case>.json` | The case's events, one `format: json` line each |
| `cases/<system>/<case>.<wire>` | The wire bytes a generated case decodes from, recorded, written as text, or hand-built ("Adding a case"): `.statsd` lines, InfluxDB line protocol in `.lp`, a Prometheus scrape body in `.prom`, a Datadog or OTLP/HTTP request body, compressed as sent, in `.bin`, one collectd datagram in `.raw`, carbon plaintext lines in `.graphite`, or one pickle payload with no length prefix in `.pkl` |
| `cases/<system>/<case>.headers` | The request beside an HTTP wire file, one `name: value` per line as `testdata/interop/` records it: `path:` with an InfluxDB write's `org`, `orgID`, `bucket`, and `precision`, a scrape's `content-type:` choosing its dialect, or a Datadog or OTLP/HTTP request's `path:`, `content-type:`, and `content-encoding:` |
| `flows/<flow>.yaml` | A runnable config: `replay_in -> [stages] -> <sink>` |
| `expected/<flow>.json` | Each case's run through the flow, keyed by case name: the events read back and the counters moved (generated) |
| `ledger/<flow>.json` | What the flow's runs fed the checks across flows: each cell reached and the classes observed there, and each row a sink has no cell in yet with the runs that reached it (generated) |
| `coverage.md` | Each system's features and the cases that exercise them, and each sink's cells no flow reaches (generated) |

## Running it

The spec's generator and the docs renderer are ordinary tests, so `script/test` and CI run them.
The generator is three kinds of test in `crates/logit-cli/src/transit/spec.rs`, so nextest runs
the flows in parallel and a failure names its flow:

- `transit::spec::cases` checks the case files, removes or reports a generated file no flow
  produces, and fails when the list of flow tests doesn't match `flows/*.yaml`.
- `transit::spec::flow::<flow>`, one per flow with `-` spelled `_`, runs the flow's cases, runs
  the class check, and compares `expected/<flow>.json` and `ledger/<flow>.json`.
- `transit::spec::ledger` merges every `ledger/<flow>.json` and runs the checks across flows
  ("The class check" below), then compares `coverage.md`. It runs no flow.

The checks across flows read the committed ledger files, not a live run. Each flow test fails
when its ledger file is stale, so when every test passes, the merged files hold what one run of
every flow would have seen.

Four wrappers drive them:

- `script/transit check` compares every generated file and docs region, and runs the class check.
- `script/transit gen` rewrites every generated file and docs region, then runs the class check.
  Review `git diff` and commit the result. Each kind of test reads what the one before it writes,
  so `gen` runs `cases`, then every `flow::`, then `ledger` and the docs test, as three nextest
  runs. A failing run doesn't stop the next, so one `gen` reports every failure and rewrites what
  it can, then exits with the first failure; a failing `flow::` leaves `ledger` reading that
  flow's committed file.
- `script/transit docs` rewrites only the docs regions ("Generated docs" below).
- `script/transit coverage` does what `gen` does, then prints `coverage.md`.

`gen` writes a flow's expectations and ledger file only after every case in the flow has run, and
only when none of them failed the class check; one class failure leaves both files untouched. A
failure means the code or the table is wrong, and a person decides which. A cell `coverage.md` lists as reached by no flow
is one nothing checks: add a case or a flow that reaches it.

## The classes

`mapping.yaml`'s `classes:` is the canonical vocabulary: `exact`, `normalized`, `degraded`, `lost`,
`skipped`, and `dropped`. They apply at two levels:

- **Record level**, for a metric kind, a log on an event with no metrics, a span on an event
  with no metrics or log, the span readiness rows `not_datadog_origin` and
  `needs_agent_processing`, an event with no metric, log, or span (`event/string_only` for an InfluxDB
  point whose fields are all strings, its measurement carrier intact, else
  `event/attributes_only`), and the `record` rows that decide a record's fate
  (`no_recorded_value`, `no_recorded_value_delta_sum`, `no_recorded_value_other_kind`,
  `service_check`, `name_unsafe`, `exemplar_key_unsafe`, `exemplar_key_unsafe_histogram`,
  `sample_rate`, `non_finite`, `superseded`, `same_instant`, `same_instant_equal`,
  `same_millisecond`, `same_millisecond_equal`), and the `log` rows for a message a log's
  record reaches beside its kind's row (`message_unsafe`, a message the sink rewrites, such as
  `datadog_out`'s JSON text of a structured one; `structured_message`; `bytes_message`;
  `blank_message`): `exact`, `normalized`, `degraded`, `skipped`, or `dropped`, best to
  worst.
- **Field level**, for the `record` rows `unit`, `description`, `start_timestamp`, `exemplars`,
  `exemplars_on_summary`, `unit_on_distribution`, `min_max`, and `sample_rate_unweighted`, every `attributes` row (`rate`,
  `rate_on_non_gauge`, `family_type`, `family_type_on_other_kind`, `interval`, `interval_on_distribution`, and `interval_on_histogram` among them), a log or span beside metrics
  (`beside_metrics`) or a span beside a log (`span/beside_log`), each a field of its event, every
  `span` row for a span's own fields (`name`, `trace_id`, `kind`, its status, its timestamps,
  `flags`, `trace_state`, its dropped counts, its events, and its links, with an `_unset` row for
  an absent parent or status, and a row for a span event's non-`Str` name and for each `Map` or
  `Array` event attribute and `Bytes`, `U64`, or `Timestamp` event or link attribute), and a
  field of any log, beside metrics or
  not (`severity`, `body_format`, `event_name`, `observed_timestamp`, `trace`, `trace_flags`,
  `dropped_attributes_count`, and `timestamp`, or `receipt_timestamp` for a log carrying
  `syslog.timestamp`; a trace's ids and flags reach `trace_beside_id_attribute` instead on a log
  that also carries a `trace_id` or `span_id` attribute): `exact`, `normalized`, or `lost`. A log beside metrics reaches those rows
  and its attribute rows as a log alone does, so `log/beside_metrics` records only whether the
  sink carried the log. A field the log doesn't set reaches its
  `_unset` row (`severity_unset`, `observed_timestamp_unset`, `trace_unset`), `normalized` where
  the sink fills it in. A log's record pairs on its message and attributes alone, so a log read
  back with another severity still pairs and the difference lands on `log/severity`. Under
  `syslog_out`'s `structured_data:` on RFC 5424, a log's attribute outside the header fields
  reads back nested, as the PARAM of its key in `syslog.sd`'s element under the configured
  SD-ID, holding its PARAM-VALUE text; its row reads that `normalized`. A span pairs on its
  trace id's low 64 bits and its span id alone, so a span read back with another status still
  pairs and the difference lands on its status row. A span's field rows have cells only at the span sinks
  (`otlp_out`, `datadog_out`, `datadog_trace_out`, `splunk_hec_out`, and `kafka_out`), since
  every other sink skips a
  span alone or leaves it off beside another signal.

Where a sink's outcome depends on the record's kind, the kind is in the feature's name, since a
`when:` key names sink config only: `no_recorded_value` is a flagged cumulative `sum` or
`gauge`, `no_recorded_value_delta_sum` a flagged delta `sum`, `exemplars_on_summary` a
summary's exemplars, `unit_on_distribution` a `samples` or `distribution` record's unit,
`rate_on_non_gauge` a `_logit.metrics.type: rate` flag on a record that isn't a `gauge`,
`family_type_on_other_kind` a `_logit.metrics.type` of `info` or `stateset` on a record that
isn't a `gauge`, or of `gaugehistogram` on one that isn't a `histogram`,
`interval_on_distribution` a `_logit.metrics.interval` flag on a `samples`, `distribution`, or
delta `exponential_histogram` record and `interval_on_histogram` one on a delta `histogram`, and
`exemplar_key_unsafe` and `exemplar_key_unsafe_histogram` a monotonic `sum` and a `histogram`
whose written exemplar has an attribute key `prometheus_out` sanitizes into a label name, the only sink that writes an exemplar's keys under a name rule. Some
rows name a shape one sink treats apart, and every sink that compares the record reaches them: `record/sample_rate` (a
`samples` record whose rate rounds to a weight above 1) and `record/sample_rate_unweighted` (a
rate below 1 that rounds to weight 1), `span/not_datadog_origin` and
`span/needs_agent_processing` (a span whose trace chunk `datadog_out`'s readiness gate holds
back), `log/trace_beside_id_attribute` (a log's trace beside a `trace_id` or `span_id`
attribute, which `datadog_out` and `splunk_hec_out` write in the trace's place), `attributes/datadog_carrier_log` (a `_logit.datadog.*` carrier on a log, a Datadog
event, or a service check), `attributes/host_beside_host_name` (a `host` attribute beside a
non-empty string `host.name` or the hostless mark, which `statsd_out` leaves off) and
`attributes/hostname` and `attributes/log_service` (a log's `hostname` attribute, or its
`service` beside `service.name`, which `datadog_out` leaves off), `attributes/log_service_field`
(a log's string `service` with no `service.name`, which `datadog_out` writes as the log's
`service`), `attributes/sd_id_collision`, `attributes/sd_name_unsafe`, and
`attributes/sd_empty_array` (a log's attribute `syslog_out` leaves out of its `structured_data:`
SD-ELEMENT, on a log whose `syslog.sd` already holds the configured SD-ID, under a key that isn't
an SD-NAME, or holding an empty array, reached only at that sink under the opt-in), and
the attributes `datadog_out` writes from a record's own fields,
`attributes/no_host`, `log_status`, `span_kind`, and `trace_id_high`, and `syslog_out`'s
`attributes/default_tag`. A flagged record's `no_recorded_value` row goes ahead of its kind's counted
loss where that loss's counter didn't move, since most sinks test the flag first
(`classes.rs`'s "Counters").

## The mapping table

Each row is a `kind` and, for a kind that has them, a `feature`. `cells` maps a sink to what it
does. A cell has a `class`, a `text` sentence for an operator, a `reason` (required on a
`normalized` cell), and the `counter` series it counts under. A `skipped`, `dropped`, or `lost`
cell names its counter, except a `lost` cell whose reason is `structural`: the destination's
format has no slot for the field, so every record loses it, the table is its record, and no
counter would add anything
([ADR `telemetry-loss-vocabulary`](../../docs/adr/telemetry-loss-vocabulary.md)'s amendment
"structural loss has no counter"). A loss that depends on the value, such as `false` or a
rewritten name, is counted. The table's schema accepts `reason: uncounted` only on the cells
`crates/logit-cli/src/transit/mapping.rs`'s `UNCOUNTED_ALLOWED` names, whose doc says which rule
each follows. It has eight entries. Five follow the same ADR's amendment "an ignored signal, and
a format the config selects": a sink ignores a signal it has no wire for, such as a log beside
metrics, uncounted, at any sink, and `datadog_trace_out`, whose one signal is spans, ignores every
log and gauge it reaches. Two are a sink finding: `otlp_out` writes a span name or a span
event name that isn't a `Str` as `""` (`span/name_structured`, `span/event_name_structured`).
One is a `_logit.datadog.*` carrier `datadog_out`'s log, event, and service-check routes have no
field for. Under that amendment too, `syslog_out`'s leave-off of every attribute
outside its header fields, under RFC 3164 and under RFC 5424 with no `structured_data:`, is
`structural`: the format the config selected has no slot for it.
When a sink's behavior depends on its config, the sink's entry is a list: each cell but the last
has a `when:` map of config fields, the first that matches the flow's sink config applies, and
the last applies otherwise:

```yaml
- kind: distribution
  cells:
    statsd_out:
      - when: {multi_value: skip}
        class: skipped
        counter: logit.output.metrics.skipped{metric_kind=distribution}
        text: 'Under `multi_value: skip`, no line is written.'
      - class: degraded
        counter: logit.output.metrics.degraded{metric_kind=distribution}
        text: Dotted count, sum, extreme, and quantile series.
```

A sink with no cell in a row is "not yet specified" there: the check prints the row and skips the
records that reach it, and doesn't fail. The table's kinds, features, and rules are in
`crates/logit-cli/src/transit/mapping.rs`'s module doc.

## The class check

For each flow and case, the check pairs every record that reached the sink with the records read
back, reading names, attribute keys, and values as that sink writes them (`statsd_out`'s `host`
attribute read back as `host.name`, `prometheus_out`'s
sanitized names and `_total`, `influxdb_out`'s `<name>.value` field, `otlp_out`'s integer
timestamps and an OpenMetrics `info` or `stateset` gauge read back as a non-monotonic sum, `datadog_out`'s hostless mark read back as a v2 `host` resource with no name, and the attributes it writes from a
record's own fields), then compares what it observed with the table. A record that pairs only by
name, in any metric kind (its own included) or as a dotted expansion, reads `degraded`. When both
sides are histograms, that pairing also needs the same total count, and the same sum where both
sides carry one: a sink may merge, drop, or fold bounds and still read `degraded`, but one that
loses or invents observations pairs nothing, and the check fails. A record's expected class is the worst of
the record-level cells it reaches, and the observed class must equal it. A `normalized` cell also
accepts `exact`, because a normalization touches only the shapes it names. A field may read back
better than its cell, never worse. Every sink counter the run moved must be named by a cell the
case reached, and a loss cell observed at its class must see its counter move. A record nothing
reads back is credited to the class of the counter that moved, or of a cell whose loss is
`uncounted`, with one exception: under `prometheus_out` exposition, a record whose series has a
later record read back was overwritten in the registry, so it reads at its expected class when
that's `normalized` or `degraded`. Across every
flow, every `normalized` or `lost` cell a record reached must be observed at that class at least
once; the `ledger` test checks this from the flows' ledger files.

`crates/logit-cli/src/transit/classes.rs`'s module doc has the rules in full: which rows a record
reaches, how records pair, and what each failure message names.
[`docs/known-gaps/mappings.md`](../../docs/known-gaps/mappings.md) lists where the check misreads
`graphite_out`, including why only a gauge-only flow runs `tags: drop`.

### Counters no row names yet

The check fails on a sink counter no reached cell names, so these `statsd_out` series have no row
because no case can reach them through the like pair: each needs input `statsd_in` never decodes.

| Series | Why no case reaches it |
|---|---|
| `messages.dropped{reason=empty_name}` | a name that sanitizes to nothing, which `statsd_in` refuses |
| `messages.dropped{reason=invalid_service_check}` | a service check carrier out of range, which `statsd_in` never stamps |
| `messages.dropped{reason=oversize_line}` | a line over `max_packet_bytes`, a transport bound the row model has no feature for; `statsd_in` reads lines up to its own datagram size |
| `fields.omitted{reason=invalid_value}` | a `_logit.statsd.*` carrier of the wrong type |

A cross-protocol source or a hand-written case can reach each; the row comes with that case. The
hand-written `cross-kinds` and `typed-attributes` cases reach `messages.dropped{reason=unencodable_value}`
(a non-finite gauge) and `tags.omitted{reason=unrepresentable}` (a map, bytes, or timestamp value).

These `graphite_out` series have no row, and no case reaches them yet:

| Series | What would reach it |
|---|---|
| `tags.omitted{reason=empty}` | a public attribute whose value is the empty string, which reads back lost against the `normalized` `string_value` cell |
| `tags.omitted{reason=collision}` | two attribute keys on one event that sanitize to one tag name, such as `a b` and `a_b` |
| `metrics.normalized{reason=tag_sanitized}`, from an array element alone | an array attribute whose last representable element holds whitespace, `;`, or a control character, on an event with no string, bool, number, or resource attribute: the `array_value` cell names `tags.normalized{reason=multi_value}` instead |
| `metrics.dropped{reason=empty_name}` | a name that sanitizes to nothing, which no decoder emits |
| `metrics.dropped{reason=unencodable_timestamp}` | an event at or before the Unix epoch's first second |
| `metrics.degraded{reason=non_finite_component}` | a record under `multi_value: expand` with a NaN or infinite `sum`, `min`, `max`, or quantile, which no case holds |
| `metrics.dropped{reason=oversize_line}`, `metrics.dropped{reason=oversize_datapoint}` | a line over `max_packet_bytes` under `transport: udp`, or a datapoint over `max_frame_bytes`, transport bounds the row model has no feature for |

## Determinism

A wire case decodes at a fixed receipt time, 2027-01-15T08:00:00Z. Each run's clock is the case's
latest event timestamp plus one second, so a stage flushes once and a statsd line without `|T`
reads back at that instant. The flow driver's module doc, `crates/logit-cli/src/transit/mod.rs`,
lists what a flow may hold and what it rejects: a fan-out or fan-in, `lua`, a router or target,
`has_stale` and `drop_stale`, and a sink it has no encoder arm for.

`otlp_out` stamps a log that has no observed time with the wall clock. The driver reads that stamp
back as the run's clock, so the expectation doesn't change between runs.

`format: json` writes an attribute map in the order the process interned its keys, so the same
event would write its attributes in another order once another source's cases had interned some
of those keys first. Every case `.json` the generator writes or compares, and every expectation
document, sorts each map under an `attributes` key by key, recursively (nested map values
included), and the `types` table by pointer. Every other object keeps the grammar's order, and no
value is re-spelled.

Where a value can move between runs for a reason the code can't remove, a case's meta declares a
tolerance: `tolerance: [{path: /metrics/0/estimate, abs: 1}]` lets the number at that RFC 6901
pointer, in each event of the document, differ from the committed one by up to `abs`.

## Generated docs

The mapping tables in the docs are rendered from `mapping.yaml`, so the table is the one copy.
Each sink column has two regions:

| Region | File | What it holds |
|---|---|---|
| `doc:statsd_out` | `crates/logit-outputs/src/statsd.rs` | Every cell of the column |
| `doc:prometheus_out` | `crates/logit-proto/src/prometheus/mod.rs` | Every cell of the column |
| `doc:influxdb_out` | `crates/logit-proto/src/influxdb/mod.rs` | Every cell of the column |
| `doc:otlp_out` | `crates/logit-proto/src/otlp/mod.rs` | Every cell of the column |
| `doc:datadog_out` | `crates/logit-proto/src/datadog/mod.rs` | Every cell of the column |
| `doc:datadog_trace_out` | `crates/logit-outputs/src/datadog_trace.rs` | Every cell of the column |
| `doc:graphite_out` | `crates/logit-proto/src/graphite/mod.rs` | Every cell of the column |
| `doc:syslog_out` | `crates/logit-outputs/src/syslog.rs` | Every cell of the column |
| `doc:splunk_hec_out` | `crates/logit-proto/src/splunk/mod.rs` | Every cell of the column |
| `doc:kafka_out` | `crates/logit-outputs/src/kafka/encode.rs` | Every cell of the column |
| `gaps:<sink>`, one per sink | `docs/known-gaps/mappings.md` | The column's `degraded`, `lost`, `skipped`, and `dropped` cells, in that file's row format |

A region opens on a line ending `<!-- transit:doc:<sink> -->` or `<!-- transit:gaps:<sink> -->`
and closes on the next line ending `<!-- transit:end -->`. Whatever precedes the opening marker
(`//! ` in a module doc, a list item's indentation in Markdown) starts every rendered line. Write
the two marker lines by hand and run `script/transit docs` to fill them. Everything between them
is generated, and `script/transit check` fails naming the file and the region when it differs from
the rendering, so a row changes in `mapping.yaml`, never in the region.

A `doc:` region holds two tables, the record-level rows and then the field-level ones. Each row
gives the cell's class, its reason and counter, and its `text` verbatim. Each cell of a `when:`
list is its own row, with its condition after the row's name: `under format: statsd`,
`with bind: unset` for a `null`, and `otherwise` for the last cell. A `gaps:` region leaves out
`exact` and `normalized` cells and labels a field-level row `field`. Where it leaves out an
earlier cell of a list, the last cell names the negation of the earlier conditions, such as
`with bind: set`, in place of `otherwise`.

Everything outside the regions stays hand-written: each codec's decode tables, its grammar, its
permitted-normalization list (per-codec policy, not per-cell outcomes), and the encode rules and
details the spec has no row for, such as `prometheus_out`'s name collisions or `influxdb_out`'s
`non_finite_component` count. Each module doc says which part is which. A new sink column needs both regions
and a protocol name in `docs.rs`'s `PROTOCOLS`, or the check fails.
`crates/logit-cli/src/transit/docs.rs`'s module doc has the rules.

## Adding a case

1. Pick the source system's directory under `cases/`.
2. For a case that starts on the wire, write the wire file (`<case>.statsd`, `<case>.lp`,
   `<case>.prom`, `<case>.bin`, `<case>.raw`, `<case>.graphite`, `<case>.pkl`, or `<case>.syslog`) and `<case>.meta.yaml` with `wire: <case>.statsd`.
   A Splunk case's `.headers` gives the request's `path:`, its query string included, which picks
   `/event` or `/raw` and, for `/raw`, the envelope, and its `content-encoding:`; the body decodes
   through `splunk_hec_in`'s own request decoding.
   An InfluxDB case needs a `<case>.headers` beside its wire file whose `path:` names a `bucket` and an `org` or `orgID`,
   as `influxdb_in` requires; a Prometheus case reads text 0.0.4 unless its `.headers` gives an
   OpenMetrics `content-type:`. A Datadog case's `.headers` gives the request's `path:`, which
   picks the route, and its `content-type:` and `content-encoding:`, and the body decodes through
   `datadog_in`'s own request decoding. An OTLP case's `.headers` does the same for `otlp_in`:
   `path:` picks the signal, `content-type:` protobuf or OTLP/JSON, and `content-encoding:`
   gzip; a capture with no `.headers`, as `testdata/interop/otlp/` records them, is read as the
   OTel Collector `file` exporter's lines, one OTLP/JSON metrics request each. A `wire:` path may point at a capture under `testdata/interop/`
   instead of copying it, and its `.headers` comes with it; an InfluxDB body sent with
   `content-encoding: gzip` is decompressed first, as `influxdb_in` does. A collectd case may name
   a `types.db` file in `types_db:`, relative to its directory, to name its data sources as
   `collectd_in`'s `types_db:` setting does. A graphite case reads plaintext unless its
   `protocol: pickle` says otherwise; a pickle wire file is a connection's length-prefixed frames,
   as `testdata/interop/graphite/` records them, or under `.pkl` one payload with no prefix, as
   `testdata/differential/graphite-pickle/` holds them. A syslog case is one UDP datagram, read
   one message per line as `syslog_in` reads UDP, unless `framing: stream` says it's a TCP
   connection's bytes, split by RFC 6587's framer as `testdata/interop/syslog/rsyslog-tcp-000.raw`
   records them. Run `script/transit gen` to write `<case>.json`.
3. For an edge case no producer sends, write `<case>.json` by hand and leave `wire:` out. The check
   requires it to read back to itself through `format: json`, and prints the form to use when it
   doesn't.

   The exception is a wire feature no capture has and a `.json` can't state, such as a collectd
   data-source type or a legacy time part: build the wire file by hand instead, because the case
   then runs through the real decoder. Describe the bytes part by part in the meta's comment, so
   a reviewer can check them without a hex dump. Only a wire with a fixed, hand-readable layout
   qualifies. A collectd datagram does, since it's a flat run of type-length-value parts; a
   remote-write protobuf body doesn't, so it waits for a recorded capture.
4. List the features it exercises in `exercises:`. Each must be in `features/<system>.yaml`.
5. Run `script/transit gen`, review the new expectations, and commit.

## Adding a flow

Write `flows/<flow>.yaml` with one `replay_in` whose `path:` names a case, such as
`../cases/statsd/mixed.json`. That case's directory is the flow's source, and every case in it runs
through the flow; naming a real case keeps the flow runnable by hand with `logit run`. Add the
flow to the `flow_tests!` list in `crates/logit-cli/src/transit/spec.rs`, since nothing runs a
flow the list leaves out and the `cases` test fails naming it. Then run `script/transit gen`,
which writes `expected/<flow>.json` and `ledger/<flow>.json`. `script/validate` and `every_shipped_config_loads_and_validates` load every
flow.

## Adding a sink column

Add the sink to `sinks:`, give it a cell in every row, and add a flow that ends in it. Add its
`doc:` and `gaps:` regions ("Generated docs") and run `script/transit docs`. The flow
driver must have an encoder arm for it (`crate::pipeline::sink_encoder`, and the pair decoder in
`crates/logit-cli/src/transit/wire.rs`). A row left without the sink's cell reads as not yet
specified. Leave a row without the sink's cell only when no record can reach it there, and say
why in a comment beside the row, as `name_unsafe` and `min_max` do.

## Adding a source

Add `features/<system>.yaml`, a `cases/<system>/` directory, and an arm for the system's decoder in
`decode_wire` (`crates/logit-cli/src/transit/spec.rs`) so wire cases can be generated. A second
directory for one system, such as `otlp-logs`, takes the system's name and a `-` suffix, and
reuses its arm. Flows that
read the new directory then pick up its cases.
