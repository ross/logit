---
created: 2026-10-19
updated: 2026-10-19
---

# Enabling plan: a transit spec — reference inputs, flows, and a mapping table that generates the docs

## Context

[ADR `lossless-transit`](../adr/lossless-transit.md) settles that like-protocol transit is
lossless and cross-protocol transit is best-effort, counted, and documented. What the project has
for checking that is spread across several places, each built for a different question:

- `testdata/interop/` holds real wire captures per protocol, and every codec has a
  `*_fixed_point.rs` test. Both check a decoder against what real producers send, and a like pair
  against itself. Neither says what a `prometheus_out` consumer sees when the source was
  `influxdb_in`.
- `testdata/differential/` pins a parser's reading against a reference implementation, with a
  declared verdict and a named divergence per case. It covers two parsers.
- Cross-protocol outcomes live in prose: each codec's module-doc mapping table, and
  `docs/known-gaps/mappings.md`'s cross-protocol table. Two tests
  (`crates/logit-proto/tests/prometheus_cross_route.rs`, `datadog_cross_route.rs`) are the only
  A-decode-into-B-encode precedents, and nothing checks the prose against the code.
- The interop harnesses (`script/victoria-interop`, `script/splunk-interop`) check that a series
  arrived with the expected name and labels. Only one leg compares values.

So the answer to "what does a metric's value become between `X_in` and `Y_out`" is a reading of
several documents and the code, and nothing fails when they disagree. This plan builds the
framework that makes the answer data: a reference corpus per source system, flows through real
components, and one mapping table that both the tests and the docs are generated from.

Goals:

- A reference set of inputs per source system, as that system's `_in` produces them, covering
  the ordinary and edge cases of every metric kind the system can carry.
- Flows `X_in -> [stages] -> Y_out` run through the real codecs and transforms, deterministic and
  service-free, each case's outcome pinned as data.
- One mapping table, kind × feature × sink, that states the class of every transition and is the
  canonical copy the module docs and `known-gaps/mappings.md` are generated from.
- A framework that logs and spans fit without redesign, filled in when a flow needs them.
- Anything built for this that an operator could use ships as a feature: a lossless JSON event
  form, and an input that replays it.

Non-goals:

- Lossless cross-protocol transit. A flow's expectation is the degraded form, and the table says
  how it degraded. Only like pairs are held to lossless, as the ADR already says.
- Backend specifics. A flow ends at a sink's encoder and is read back by its pair's decoder. What
  VictoriaMetrics does with a remote-write body is `prometheus_out`'s concern, checked by its own
  tests and the interop harnesses, not here.
- Sink behavior above the encoder: stale-point drops, tag-count limits, request packing, retries,
  bucket and org routing. Each `_out`'s unit tests own those.
- `aggregate` as a subject. A flow uses it to connect a raw kind to a sink that can't carry it;
  [ADR `aggregation-window-semantics`](../adr/aggregation-window-semantics.md) stays the authority
  on what it does per kind, and the table has no stage columns.
- Full coverage of every source × sink cell. The first PRs prove the framework on a few pairs;
  pairs are added one PR at a time as a question about one comes up.
- Replacing `testdata/interop/` or `testdata/differential/`. They guard the decoders and the
  like pairs, and the spec builds on them.

Stream key **`transit`**: branches `transit/w0` through `transit/w5`, a linear stack, then one
branch per pair added. PR stack only: nothing is merged by this workstream; Ross directs
merging.

Settled with Ross (2026-10-18 and 2026-10-19): the work runs at the codec layer, on `Encoder`
output read back through the pair's `Decoder`, because the question is whether a numeric value
keeps its observability value across a transition, not how a destination stores it. The
mapping table is the spec and the docs are generated from it, so there's one copy; ADR tables
stay as decision records with a note saying so. Flows are real `logit` configs. Cases are
authored as lossless event JSON, generated from wire bytes where a case starts on the wire and
written by hand otherwise; `generate_in` isn't the authoring tool. Raw encoded bytes are kept
only where a flow needs them. The shared-misunderstanding risk (a sink and its pair's decoder
agreeing on a wrong reading) is accepted: the interop captures and differential corpora guard it
to a reasonable degree, and a finding gets fixed when it surfaces.

## Design

Each item names the workstream that builds it.

### 1. Layout (W4)

```text
spec/transit/
  README.md                     what this is, how to add a case, a flow, or a pair
  mapping.yaml                  the table: kind × feature rows, one column per sink
  features/<system>.yaml        the wire features a source system's cases can exercise
  cases/<system>/<case>.meta.yaml  the features the case exercises, its wire file, its tolerances
  cases/<system>/<case>.json    one lossless event JSON file per case, as <system>_in produces it
  cases/<system>/<case>.<wire>  the wire bytes a generated case decodes from (optional)
  cases/<system>/<case>.headers the request headers beside an HTTP wire file (optional)
  flows/<flow>.yaml             a runnable logit config: replay_in -> [stages] -> <sink>
  expected/<flow>.json          each case's re-decoded output and counters, keyed by case, generated
  coverage.md                   each system's features and the cases exercising them, generated
```

A case's wire file is the authoring form where one exists: statsd lines, line protocol, exposition
text, carbon plaintext, and OTLP/JSON are readable text; a collectd datagram is `.raw`, and pickle,
remote-write, and Datadog protobuf are `.bin` with a `.headers` sidecar, as `testdata/interop/`
does. A recorded capture under `testdata/interop/` joins by reference, as the `wire:` path in the
case's `.meta.yaml`, not by copy. A hand-written edge case is the JSON alone, except where a wire
feature no capture has can't be stated in JSON (the `collectd_in` entry under "Findings"). Both are
"as `X_in` would produce it," so a flow reads one form.

Each case names the features it exercises from `features/<system>.yaml`, so coverage is a
listing, not a goal: `script/transit coverage` prints each system's features with the cases that
exercise them, and a feature with none is visible.

### 2. The lossless event JSON (W1)

`stdio_out`/`file_out` `format: json` ([ADR `stream-json-format`](../adr/stream-json-format.md),
`crates/logit-outputs/src/ndjson.rs`) is the form, extended additively so it round-trips every
`Event`. Today it can't: a `distribution` renders derived statistics without its sketch's bins, a
`set` renders an estimate without its registers, and a whole-number `F64` and an `I64` of the same
value write the same digits. The extension adds:

- a type tag beside every `MetricValue` and `Value` scalar where the JSON type doesn't already
  carry it (an `F64` that is whole, a `U64` above `i64::MAX`);
- a `distribution`'s mapping and bins, beside the statistics it renders today;
- a `set`'s registers, beside its estimate;
- anything else a round-trip test finds missing.

Nothing is removed or renamed, so the six Python readers the module doc lists keep reading, and
W1 checks each one against the new output. A Rust reader lands beside the writer in
`logit-outputs` (or `logit-proto`, where the native codec lives) and a round-trip test pins it:
every kind, every scalar type, flags, start time, exemplars, resource, scope, log, and span.

Shipped as an amendment to `stream-json-format`, since an archive written by `file_out` becomes
replayable by it.

### 3. `replay_in` (W2)

A new `ComponentKind`, `replay_in`, reads a file once and emits its contents as batches:
`format: json` reads `format: json` lines, `format: native` reads `CODEC_BATCH` frames. It ends
when the file does, as `generate_in` does under `count`, which ends the process. Its ADR records
the use cases: replaying an archive `file_out` wrote, injecting hand-written events, and the
spec's flows. Timestamps are the file's; a line or frame it can't read is skipped and counted
(`logit.input.frames.dropped{reason}`), and a torn final frame ends the file as `logit-perf`'s
`decode_dump` already tolerates.

Both bound a batch: consecutive lines with the same resource and scope form one, cut at
`batch:` events ([ADR `replay-input`](../adr/replay-input.md)).

### 4. The flow driver (W3)

A flow is a `logit` config under `spec/transit/flows/`, validated by `script/validate` and
`every_shipped_config_loads_and_validates` like every other shipped config, and runnable by hand
under `logit run` for a look at what a sink emits. The driver runs it in-process, in the
`#[cfg(test)]` module `crates/logit-cli/src/transit/`, because the kind registry
(`crates/logit-cli/src/pipeline.rs`) is what builds components from a config:

1. Build the flow's transforms from its config, and its sink's encoder, reading the case file in
   place of `replay_in`'s path.
2. Decode the case with a fixed `received_at`.
3. Run each stage with its real `Transform::process`, then `flush(now)` at a fixed `now`, so a
   window-driven stage emits once and deterministically.
4. Encode through the sink's real `Encoder` (or `FramedEncoder`, `SignalEncoder`), then decode the
   bytes through the pair's real `Decoder`.
5. Record the re-decoded batch as lossless event JSON, and the counters the sink and stages
   incremented, with reasons (`degraded`, `skipped`, `dropped`, `normalized`,
   `passed_through`, `discarded`, `omitted`, `evicted`).

Where output is nondeterministic for a reason the code can't remove (a `Set`'s estimate moves by
one with merge order past 128 members), the case declares a tolerance on the field, and the
comparison applies it.

The driver reaches each sink's encoder, not its `Output`: no sink has a transport seam, and
adding an in-memory capture to every `Output` would put test plumbing in each sink. The
registry's `sink_encoder` returns the encoder a sink kind writes with, configured from the same
config mapping `build_spec` uses (`statsd_encoder`, `to_remote_write_version`,
`to_remote_write_encoding`). The steps between an encoder and the wire are public functions on
the sink module: `logit_outputs::prometheus::encode_remote_write`, which the remote-write sender
calls; `render_batches`, which renders through the exposition sink's own registry and
`max_series:` cap; the statsd and InfluxDB sinks' stats-to-telemetry functions, which both sinks
call; and `logit_outputs::influxdb::destination`, a wrapper over the `resolve` the sink calls for
a batch's org and bucket, with `drop_no_bucket`, which the sink calls for a batch with none. So
the driver and the sink can't drift. The driver decodes an InfluxDB write under the resource
`influxdb_in` builds from that org and bucket (`logit_proto::influxdb::write_resource`), and
refuses one naming no org as `influxdb_in` does. Two things it doesn't reproduce: `has_stale` and
`drop_stale`, which read the wall clock per batch, so it rejects them; and `prometheus_out`'s
`expire_after:`, which reads a clock, so exposition reads back every series `max_series:` keeps.
One thing it rewrites: `otlp_out` stamps a log with no observed time with the wall clock, and the
driver reads that stamp back as the run's clock, so an expectation doesn't change between runs.
The driver is a `#[cfg(test)]` module of the
`logit` binary rather than a library target or an integration test, since only the binary's own
tests can reach `build_spec`. W4's generator follows the `script/differential --check` pattern
as a `#[test]` in the same module: with `TRANSIT_UPDATE=1` it rewrites the expectations, and
unset it compares, with `script/transit gen|check` wrapping the two.

### 5. The mapping table (W4)

`mapping.yaml` is the spec. A row is a model kind and the features that change its fate:

| Dimension | Values |
|---|---|
| kind | `sum`, `gauge`, `gauge_delta`, `samples`, `distribution`, `set_members`, `set`, `histogram`, `exponential_histogram`, `summary` |
| temporality | `delta`, `cumulative` (sum, histogram) |
| monotonic | `true`, `false` (sum) |
| scalar | `f64`, `i64`, `u64`, `bool` (sum, gauge, gauge_delta) |
| record fields | `unit`, `description`, `start_timestamp`, `exemplars`, a histogram's `min` and `max` (field level) |
| record features | `flags.no_recorded_value`, a service check's carriers, a name the sink must sanitize, a non-finite value, a later point of the series in the run, at the same instant, or in the same millisecond (record level) |
| attribute shapes | a string, boolean, number, `Array`, `Map`, bytes, or timestamp value, the sink's own `_logit.` carrier, another protocol's, a resource attribute, the batch's scope |
| logs | a DogStatsD event, any other log |

A column is a sink. A cell is a class, a reason, and the counter it counts under:

| Class | Level | Meaning |
|---|---|---|
| `exact` | both | the pair's decoder reads back the same kind and value |
| `normalized` | both | a permitted normalization applied (named) |
| `degraded` | record | the value survives in a lesser kind (a sketch as five quantiles) |
| `lost` | field | a field the record loses while the record survives, counted or named `uncounted` |
| `skipped` | record | the record leaves no wire form, counted `skipped{reason}` or named `uncounted` |
| `dropped` | record | the record is lost for a reason the config didn't ask for, counted `dropped{reason}` or named `uncounted` |

A sink whose outcome depends on its config (`statsd_out`'s `format:`, `multi_value:`,
`relative_gauges:`) gets a list of cells, each but the last with a `when:` map matched against the
flow's sink config. Each cell carries a sentence of operator-facing text: what a consumer of that
sink sees, and what they lose. That text is what the docs are generated from.

The generator checks every flow result against the table: it pairs each record that reached the
sink with what the pair's decoder read back, and the observed class must equal the worst
record-level cell the record reaches (a `normalized` cell also accepting `exact`), each field may
read back no worse than its cell, and every loss counter the sink moved must be named by a cell the
case reached. A disagreement fails the run with the case, the row, the cell, and the observed
outcome, and is a finding, never a regenerate: either the code is wrong, or the table is, and a
person decides which. `crates/logit-cli/src/transit/classes.rs`'s module doc has the rules.

Rows for log and span fields (severity, timestamp precedence, body format, structured data, trace
state) are added when a flow carries them. The table's schema doesn't change for them.

### 6. Generated docs (W5)

`script/transit docs` rewrites marked regions from `mapping.yaml`, and the test
`transit::docs::transit_docs`, part of `script/transit check` and so of `script/test`, fails
naming the file and the region when one is stale, the pattern `script/schema` and
`schema/logit.schema.json` already use. The renderer, `crates/logit-cli/src/transit/docs.rs`,
reads the table alone. Each sink column has two regions:

- `doc:<sink>`, the whole column, in the module doc that holds the sink's encode mapping:
  `crates/logit-outputs/src/statsd.rs`, `crates/logit-proto/src/prometheus/mod.rs`,
  `crates/logit-proto/src/influxdb/mod.rs`, and `crates/logit-proto/src/otlp/metrics.rs`. A
  table of the record-level rows, then one of the field-level rows, each a row label, the class,
  the reason and counter, and the cell's text; a `when:` cell is its own row.
- `gaps:<sink>`, in `docs/known-gaps/mappings.md`: every `degraded`, `lost`, `skipped`, and
  `dropped` cell in that file's row format, the field-level rows labeled `field`, in the
  table's row order, which groups them by kind.

A hand-written row the region states in full is gone; one that also states something the table
doesn't stays beside the region, and each module doc says which part is generated. The
permitted-normalization lists stay prose: they're per-codec policy, not per-cell outcomes. Each
relay ADR's mapping table stays as written, with a note that the table was the decision at the
time and `spec/transit/mapping.yaml` is current; `prometheus-remote-write` has no table, so its
note sits after its mapping sections.

### 7. The first pairs (W4)

Enough to prove the framework on every kind of transition, not to cover the matrix:

| Flow | What it proves |
|---|---|
| `statsd_in -> statsd_out` | a like pair reads as `exact` or a named normalization, under `format: dogstatsd` |
| `statsd_in -> aggregate -> statsd_out` | raw kinds through a stage, in each `distributions:` and `sets:` mode |
| `influxdb_in -> prometheus_out` | typed scalars, a Gauge per field, the measurement carrier, Ross's motivating example |
| `influxdb_in -> aggregate(cumulative) -> prometheus_out` | gauges through a cumulative window; the delta sum this row was meant to show is `statsd_in -> prometheus_out`'s |
| `prometheus_in -> influxdb_out` | histogram and summary into a wire with one number per field |
| `statsd_in -> otlp_out` | samples as a summary, set members skipped |

Sources: `statsd_in`, `influxdb_in`, `prometheus_in`. Sinks: `statsd_out`, `prometheus_out`,
`influxdb_out`, `otlp_out`. Later pairs follow `spec/transit/README.md`'s "Adding a sink
column" and "Adding a source" steps, one PR each.

W4b landed these with four more flows: `influxdb_in -> prometheus_out` as remote-write 2.0,
`statsd_in -> prometheus_out` both as exposition and as remote-write 1.0, and `statsd_in ->
influxdb_out`, so every kind the statsd cases hold reaches each new column in each mode.
`influxdb_in -> statsd_out` and `prometheus_in -> statsd_out` followed, so the `statsd_out`
column sees a cumulative counter, a histogram, a summary, a typed InfluxDB field, and a string
field.

`tcoll/w1b` added `collectd_in` as a source, with flows into each column:
`collectd-to-influxdb`, `collectd-to-prometheus`, `collectd-to-prometheus-remote-write`,
`collectd-to-statsd`, and `collectd-to-otlp`.

`totlp/w1` added `otlp_in`'s metrics as a source, with flows into each column: `otlp-relay`,
`otlp-to-statsd`, `otlp-to-prometheus`, `otlp-to-prometheus-remote-write`, `otlp-to-influxdb`,
and `otlp-to-datadog`. OTLP logs and traces wait for a later workstream.

`tgraph/w1` added `graphite_in` as a source, plaintext and pickle, with flows into each column:
`graphite-to-statsd`, `graphite-to-influxdb`, `graphite-to-prometheus`,
`graphite-to-prometheus-remote-write`, `graphite-to-otlp`, and `graphite-to-datadog`. The
`graphite_out` column and the like pair follow in `tgraph/w2`.

### 8. Not in this stack

- A backend layer: the same corpus through a real sink to a real backend, with the backend's
  reading recorded beside the expectation. It's the layer that would catch a shared
  misunderstanding, and it's deferred with that risk accepted.
- Sink-tier flows against an in-process capture server, for the sink behavior above the encoder.
- `kafka_out` and `splunk_hec_out` metric cells: their metric forms are JSON documents with their
  own readers, and neither has a `_in` pair that reads metrics back the same way (`splunk_hec_in`
  does; it joins when a flow asks). Both join as log columns in §9.
- Compressed JSON archives: gzip on `file_out`'s `format: json` rotation and `replay_in` reading a
  `.gz` file by its extension, added together in one follow-up (`docs/known-gaps/tailing.md`).

### 9. Logs

Stream key **`tlog`**. Log records are the next signal after metrics. Every metric source and
column is in the table, and what's left for metrics is unreached cells and the layers §8 defers.
Logs aren't classified yet, as `spec/transit/README.md`'s "The mapping table", `mapping.yaml`'s
header, and `UNCOUNTED_ALLOWED`'s doc each say:

- The class check compares a log as one body. Pass 2's `same_value` is message equality, so a
  log whose severity, trace, or observed time changes reads `normalized` with no row naming the
  change.
- Three of the six columns (`prometheus_out`, `influxdb_out`, `graphite_out`) have only
  `skipped` or `lost, uncounted` log cells, and `statsd_out` writes only DogStatsD events.
- `syslog` and `splunk_hec` lack a source and a column, OTLP logs lack a source, and Datadog logs
  have one case.

Spans follow as their own stream, `tspan`, once logs land.

#### Rows

A log's fields are rows under `kind: log` in the one table, with the cell schema, class check,
and renderer metrics use. `KINDS`' `log` entry grows from `dogstatsd_event`, `plain`, and
`beside_metrics` to the list below. Record-level rows take the five record classes. Field rows
take `exact`, `normalized`, or `lost`, as a metric's field rows do.

A row is chosen from the input alone, before pairing. So a field the input doesn't set has its
own `_unset` row: a sink that stamps a value where the input had none, such as `otlp_out`'s
observed time or `syslog_out`'s PRI severity 6, makes a different statement from one that
changes a value.

| Feature | Level | What reaches it |
|---|---|---|
| `plain` | record | A log on an event with no metrics. Every such log reaches it. |
| `dogstatsd_event` | record | A DogStatsD event on an event with no metrics. |
| `message_unsafe` | record | A message the sink's encoder rewrites, such as `syslog_out`'s C0 and DEL sanitizing. |
| `structured_message` | record | A `Map` or `Array` message: JSON text at `datadog_out` and `kafka_out`'s `format: text`, `{k=v}` text at `syslog_out`, and a JSON value at `splunk_hec_out`. |
| `bytes_message` | record | A `Bytes` message: lossy UTF-8 at `datadog_out` and `splunk_hec_out`, and raw bytes at `syslog_out` (C0 and DEL escaped) and `kafka_out`'s `format: text`. |
| `blank_message` | record | An empty message. |
| `severity` | field | The log's `Severity`. |
| `severity_unset` | field | A log with no severity, which a sink may stamp. |
| `body_format` | field | The log's body format. |
| `event_name` | field | OTLP's `event_name`. |
| `observed_timestamp` | field | A log's observed time, where it has one. |
| `observed_timestamp_unset` | field | A log with no observed time, which `otlp_out` stamps. |
| `trace` | field | The trace and span ids of `LogRecord::trace`. |
| `trace_unset` | field | A log with no `trace`, which `otlp_out` fills from the event's span. |
| `trace_flags` | field | The W3C trace flags of `LogRecord::trace`. |
| `dropped_attributes_count` | field | OTLP's dropped-attribute count. |
| `timestamp` | field | `event.timestamp`, at the precision the sink writes. |
| `receipt_timestamp` | field | The receipt-time `event.timestamp` of an event that carries `syslog.timestamp`. |
| `beside_metrics` | field | A log on an event that carries metrics. |

#### Identity and the respelled message

A log's identity is its message, respelled per sink. For a log, `Spelling::written` is the
message as the sink's encoder renders it and the pair's decoder reads it back: `syslog_out`'s
C0 and DEL sanitizing and `{k=v}` text of a `Map`, Datadog's JSON text of a `Map` and lossy
`Bytes`, Splunk's lossy `Bytes`, and the body `kafka_out`'s `format: text` writes. A rewritten
message pairs in the respelled pass and reads record-level `normalized` through
`log/message_unsafe` or `log/structured_message`, and a lossy `Bytes` through
`log/bytes_message`, as `record/name_unsafe` works for a metric. Once a log pairs, `same_value` for it is `true`, and the
field rows carry every other difference.

#### Timestamps

Log timestamps get rows. A metric compares no timestamp outside the `same_instant` rows, but a
log's time is a field its consumer reads:

- `log/timestamp`: Datadog and `kafka_out`'s `format: text` keep milliseconds, RFC 3164 keeps
  seconds and no year, and RFC 5424 keeps the encoder's own precision.
- `log/receipt_timestamp`: an event carrying `syslog.timestamp` has a receipt-time
  `event.timestamp`, which `syslog_out` has no slot for (`lost, structural`).

At `syslog_out` the check reads the written instant from the read-back `syslog.timestamp`, since
`syslog_in` stamps `event.timestamp` with the read-back's `received_at`, the driver's fixed
`Options::now` (the case's latest timestamp plus one second). No wall-clock clamp
is needed: `otlp_out`'s observed stamp is the one place the driver reads the clock (§4).

#### Severity carriers

The model's six-variant `Severity` reads back equal whether OTLP's severity 10 comes back as 10
or 9, so the loss is the carrier's. A new `attributes/severity_carrier` row takes
`_logit.otel.severity_*` and `_logit.syslog.severity` ahead of the private and foreign carrier
rows. It's `exact` at the family's own sink and `lost, structural` elsewhere, with text saying
the severity comes back at band resolution.

#### Per-source attribute rows

The log sinks treat these attributes differently by key, so each is a row keyed by attribute
name:

- `syslog_header`: `syslog.facility`, `syslog.tag`, `syslog.pid`, and `process.pid`.
- `syslog_msgid`: lost under RFC 3164.
- `syslog_sd`: lost under RFC 3164, and flattened at `splunk_hec_out`.
- `syslog_timestamp`: the `syslog.timestamp` attribute.
- `host_name`: HOSTNAME at `syslog_out`. At `splunk_hec_out` it moves to the envelope and reads
  back on the resource.
- `splunk_envelope`: the resource's `com.splunk.*` attributes.

Datadog's `status`, `ddsource`, and `ddtags`, and Splunk's public `otel.log.*`, reach the
value-type rows (`attributes/string_value` and the rest) and need no row of their own.

#### Derived attributes

`Spelling::derived` becomes per sink, and the check's field rows take the union of every sink's
entries:

- `syslog_out` derives `syslog_facility` from its config, `no_host`, and a `syslog.timestamp`
  used only for pairing.
- `splunk_hec_out` derives `no_host`, on the resource, and `log_severity_fields`, the
  `otel.log.severity.*` attributes it writes from `severity`.
- `datadog_out` derives `trace_id` and `span_id`, used only for pairing. `log/trace` observes
  them as `normalized`, because `datadog_in` reads the ids back as attributes rather than into
  `trace`.

`no_host` reaches every sink that fills a host, not only the sinks `datadog_out`'s predicate
covers.

#### Sink columns

`syslog_out`, `splunk_hec_out`, and `kafka_out` join `otlp_out` and `datadog_out` as log columns.
`kafka_out` covers `format: json`, lossless by construction, and `format: text` as `when:` cells.
Its OTLP and MessagePack formats are out of this stream. No `kafka_in` exists, so the driver
reads a `format: json` record back through `EventJsonDecoder` and a `format: text` record as one
`Raw` log per record at the record's timestamp. That reading is the driver's convention, not a
pair's decoder. The four metric sinks keep their log cells.

`syslog_out` writes no attribute outside its header and private carriers, uncounted, unless
`structured_data:` is set under RFC 5424. Under `when: {format: rfc3164}` the wire has no slot,
so those cells are `lost, structural`. Otherwise the slot exists and the config didn't opt in,
so the loss is real: the cells are `lost, uncounted`, with narrow `UNCOUNTED_ALLOWED` entries
per value-type row at `syslog_out`, and the missing counter is a sink finding. The
`structured_data:` opt-in is its own PR after W2, since attributes come back nested in
`syslog.sd` and need a nested-key spelling.

#### Uncounted losses

In W5, `UNCOUNTED_ALLOWED`'s `("log", None, None)` narrows to
`("log", Some("beside_metrics"), None)` plus the `syslog_out` entries above. Every other
uncounted loss of a log field is a format with no slot, and reads `lost, reason: structural`:

- Datadog: trace flags, event name, observed time, body format, and dropped-attribute count.
- syslog: trace, trace flags, event name, observed time, body format, and dropped-attribute
  count.
- Splunk: trace flags, observed time, body format, and dropped-attribute count.
- `kafka_out`'s `format: text` writes only the body.

The `span` entry stays for `tspan`.

`otlp_out`'s `log/plain`, `log/dogstatsd_event`, and `log/beside_metrics` cells move from
`normalized`, reason `observed_timestamp`, to `exact`, and the stamp lives on
`log/observed_timestamp_unset`. The ledger's cross-flow bound requires this: once the stamp has
its own row, no log reaches those cells at `normalized`.

The sort key doesn't change. `first_name` stays `""` for a log, and the canonical line already
orders logs that share a timestamp.

#### Case directories

Log cases live in log-only directories, so a log flow never routes a metric case into a sink with
no metric cells: `cases/otlp-logs/`, `cases/datadog-logs/` (where Datadog's `agent-logs` and
`agent-events` cases move), `cases/syslog/`, and `cases/splunk/`, each with its own
`features/<system>.yaml`. `decode_wire` keys on the directory name's prefix before `-`. Metric
flows don't change, and `cases/statsd/log-only` and its neighbors keep reaching the `log/*` cells
at the metric sinks.

#### The W1 framework

W1 changes `crates/logit-cli/src/transit/`'s `mapping.rs`, `classes.rs`, `spec.rs`, `wire.rs`,
`docs.rs`, and `mod.rs`, `crates/logit-cli/src/pipeline.rs`'s `SinkEncoder` and `sink_encoder`,
`spec/transit/mapping.yaml`, and `spec/transit/README.md`:

- **`mapping.rs`**: `KINDS`' `log` features as "Rows" lists them, a `LOG_FIELDS` const,
  `is_field_row` accepting a `log` row whose feature is in `LOG_FIELDS`, and the
  `UNCOUNTED_ALLOWED` doc and module-doc table to match.
- **`classes.rs`**:
  - A `LOG_FIELD_KEYS` list (`severity`, `format`, `event_name`, `observed_timestamp`,
    `trace_id`, `span_id`, `trace_flags`, `dropped_attributes_count`) that `Rec::core` and its
    type filter strip for a log, so a log's core is its message and the message's type tags.
  - `Field::LogBody`, `Field::Body`'s logic with a Datadog hook that reads `trace_id` and
    `span_id` found in attributes as `Normalized`; `Field::LogUnset`, `exact` when the read-back
    is also absent and `normalized` otherwise; and `Field::Timestamp { receipt }`, as
    "Timestamps" describes.
  - A `field_rows` branch for a log not beside metrics that pushes the field rows, and closures
    keyed by attribute name for `severity_carrier`, `syslog_header`, `syslog_msgid`, `syslog_sd`,
    `syslog_timestamp`, `host_name`, and `splunk_envelope`.
  - `log_features` beside `kind_features`, since `record_features` returns early for a record
    that isn't a metric: `message_unsafe` when `Spelling::log_message` differs from the input's
    message, `structured_message`, `bytes_message`, and `blank_message`.
  - `Spelling::log_message`, through each encoder's own renderer, made public for it: syslog's
    `render_message`, `sanitize_msg`, and `sanitize_msg_bytes` as one `wire_message`,
    `logit_proto::json::value_text` for Datadog, Splunk's message renderer, and `kafka_out`'s
    `push_body`. `Spelling::written` returns it for a log, and `same_value` for a log is `true`.
  - `Spelling::derived` per sink, as "Derived attributes" says, with `Field::Derived` observed
    per sink. The `score` subset rule is what keeps a log from pairing at `syslog_out`,
    `splunk_hec_out`, and `datadog_out` today, since each decoder adds public attributes
    (`syslog.facility`, `syslog.timestamp`, and `host.name`; `otel.log.severity.*`; `trace_id`
    and `span_id`). The derived entries make those pair. W1 lands the mechanism and
    `datadog_out`'s `trace_id` and `span_id` entries, and W2 and W4 the syslog and Splunk
    entries.
- **`spec.rs`**: `decode_wire` keys on the directory prefix, and an `otlp` wire file with no
  `.headers` picks its signal from the top-level key. W2 adds a `syslog` arm: `SyslogDecoder` at
  `RECEIVED_AT`, a datagram capture read as one datagram with line splitting, the listener's
  default, and a TCP stream capture through `Framer::new(FramingMode::Rfc6587Auto, …)` with
  line splitting off, as `SyslogInput::tcp` frames. W4 adds a `splunk` arm over `splunk_hec_in`'s new
  `decode_request`.
- **`pipeline.rs` and `wire.rs`**: three `SinkEncoder` variants. `Framed` stays typed to
  `StatsdEncoder`.
  - `Syslog { encoder, tcp }`, built by a `syslog_encoder` helper factored out of `build_spec`'s
    arm so the two can't drift, with `report_encode_stats` made public, and read back per
    `MessageBuf` entry by `SyslogDecoder::decode_into`.
  - `Splunk(SplunkEncoder)`, through `encode_objects`, its entries joined into one `/event` body
    and read back by `SplunkDecoder::decode_events`.
  - `Kafka { encoder, format }`, read back per entry by `EventJsonDecoder::decode_batches` under
    `format: json`, and as one `Raw` log per entry at `RecordMeta`'s timestamp under
    `format: text`.
- **`Spelling`**: `Syslog { rfc3164, facility, structured_data }`, `Splunk`, and `KafkaText`.
  `kafka_out`'s `format: json` is `Verbatim`.
- **`docs.rs`**: `PROTOCOLS` and `FILES` entries, and regions in
  `crates/logit-outputs/src/syslog.rs`, `crates/logit-proto/src/splunk/mod.rs`, and
  `crates/logit-outputs/src/kafka/encode.rs`. `otlp_out`'s `doc:` region moves from
  `crates/logit-proto/src/otlp/metrics.rs` to `crates/logit-proto/src/otlp/mod.rs`, and
  `docs/known-gaps/mappings.md` gains three `gaps:` regions.

W1's class-check changes get unit tests in `classes.rs`'s `mod tests`: a log whose severity,
trace, or observed time differs lands on its row, a rewritten message pairs respelled, and an
`_unset` row reads `exact` or `normalized`. Each is run once with its change reverted to show it
fails. Every PR in the stream commits the regenerated files and quotes `coverage.md`'s unreached
cells for its new column, down to the cells its `when:` guards can't reach. Every new
`normalized` or `lost` cell is observed at its class in some flow, or the PR names the case that
would reach it.

#### Not in this stream

- `lines_in` and parsed text logs, and `tail_in` and `docker_in`.
- `kafka_out`'s `otlp_proto`, `otlp_json`, and `msgpack` formats.
- Spans, which are `tspan`'s.
- Fixes for the sink findings. Each is recorded under "Findings" as the code behaves and, where
  an operator sees it, in `docs/known-gaps/`, and a later stream fixes them.

#### Workstreams

The stack is W1 ← W2 ← W2b ← W3 ← W4 ← W5: each PR targets its parent and retargets to `main`
when the parent merges.

| # | Branch | Scope | Size |
|---|---|---|---|
| W0 | `tlog/w0` | This section and [ADR `transit-spec`](../adr/transit-spec.md)'s log-records amendment | S |
| W1 | `tlog/w1` | The framework above, and the `otlp-logs` source: `/v1/logs` cases from `testdata/interop/otlp/logs.json`, whose signal comes from its top-level `resourceLogs` key, and hand-written ones covering every body kind, severity bands and carriers, `event_name`, a trace with flags, dropped counts, scope and `schema_url`, and a log beside metrics. Flows `otlp-logs-relay` and `otlp-logs-to-datadog`; the `datadog-logs` directory split, with `datadog-logs-relay` and `datadog-logs-to-otlp`; `otlp_out` and `datadog_out` cells on every new row; `datadog_out`'s derived `trace_id` and `span_id` entries; `otlp_out`'s region moved to `otlp/mod.rs` | L |
| W2 | `tlog/w2` | The `syslog` source and the `syslog_out` column: cases from `testdata/interop/syslog/*.raw` (logger's RFC 3164, RFC 5424, and Unicode lines, the Python handler, and rsyslog over UDP and as a TCP stream through the RFC 6587 `Framer`) and hand-written ones (structured data, nil fields, a `Bytes` MSG, PROCID forms, an out-of-range timestamp). Flows `syslog-relay` under `format: rfc3164` and `rfc5424`, `syslog-to-otlp`, `syslog-to-datadog`, `otlp-logs-to-syslog`, and `datadog-logs-to-syslog` | L |
| W2b | `tlog/w2b` | The `structured_data:` opt-in: a nested-key spelling for `syslog.sd`, and a `syslog-relay-sd` flow | S |
| W3 | `tlog/w3` | The `datadog-logs` cases: hand-written `/api/v2/logs` bodies covering status forms, `ddsource`, the `ddtags` `env:`, `version:`, and `service:` lift, `service` beside `service.name`, `hostname`, a nested and a non-string `message`, RFC 3339 and fractional-millisecond `timestamp`s, `trace_id` and `span_id` attributes, and 101 tags. `datadog-logs-to-syslog` grows to match | M |
| W4 | `tlog/w4` | The `splunk` source and the `splunk_hec_out` column: `pub fn decode_request` factored out of `handle` in `crates/logit-inputs/src/splunk.rs` (route, gzip, and the percent-decoded `/raw` envelope query); cases from the log `POST` captures under `testdata/interop/splunk/` (the OTel exporter's `otel-services-collector-001` and `-raw-000`, SC4S's, Docker's, and the Java appender's), not Docker's three `OPTIONS` probes or the `/health` GET, and hand-written ones (`otel.log.*` including `otel.log.name`, `fields` nesting, `time` forms, a non-string envelope, a blank event, an array body). Flows `splunk-relay`, `splunk-to-otlp`, `splunk-to-datadog`, `splunk-to-syslog`, `otlp-logs-to-splunk`, `datadog-logs-to-splunk`, and `syslog-to-splunk` | L |
| W5 | `tlog/w5` | The `kafka_out` column under `format: json` and `text`: the `otlp-logs`, `syslog`, `datadog-logs`, and `splunk` sources' `-to-kafka` and `-to-kafka-text` flows. `log/dogstatsd_event` through the new sinks; a coverage sweep to no unreached log cell; the `UNCOUNTED_ALLOWED` narrowing; `docs/known-gaps/mappings.md`'s hand-written Splunk and syslog log rows reconciled with the generated regions; `spec/transit/README.md`'s source and sink lists, `transit/mod.rs`'s account of what the driver rejects, and `AGENTS.md` | M |

## Workstreams

| # | Branch | PR title | Size | Depends on | Status |
|---|---|---|---|---|---|
| W0 | `transit/w0` | `transit/w0: ADR and plan for a transit spec` | S | — | done |
| W1 | `transit/w1` | `transit/w1: a lossless format: json, with a reader` | M | W0 | done |
| W2 | `transit/w2` | `transit/w2: replay_in, reading format: json lines and native frames` | M | W1 | done |
| W3 | `transit/w3` | `transit/w3: the in-process flow driver` | M | W2 | done |
| W4 | `transit/w4` | `transit/w4: spec/transit, the mapping table, and the first pairs` | L | W3 | done (W4a, W4b) |
| W5 | `transit/w5` | `transit/w5: module-doc and known-gaps tables generated from the spec` | M | W4 | done |

- **W0**: [ADR `transit-spec`](../adr/transit-spec.md) (the spec is the canonical copy of every
  transition's class; docs are generated regions; ADR tables are decision records), this plan,
  and a row in each README.
- **W1**: the additive extension to `ndjson.rs` (§2), its module-doc grammar, the Rust reader,
  the round-trip test over every kind and scalar type, each listed Python reader checked against
  the new output, and the `stream-json-format` amendment.
- **W2**: `replay_in` (§3): the `ComponentKind` variant and its config docs, `script/schema`,
  the input, graph rules for a finite input, its ADR, and a `fixtures/` config that replays a
  `file_out` archive.
- **W3**: the flow driver (§4) in `crates/logit-cli/src/transit/`, with the encoder-reach decision
  recorded, and a fixed-clock run of one flow as its own test.
- **W4**: `spec/transit/` (§1), `mapping.yaml` and its schema (§5), `script/transit
  gen|check|coverage`, the `--check` test, the features lists and cases for the three sources,
  the six flows (§7) and their expectations, and the AGENTS.md "Harnesses" entry and Environment
  table row. It lands as two PRs: W4a (`transit/w4a`) builds the framework on the statsd pair,
  with five `statsd_in -> [aggregate] -> statsd_out` flows and the `statsd_out` column; W4b
  (`transit/w4b`) adds the `influxdb_in` and `prometheus_in` sources, the other three sink
  columns, and the four cross flows.
- **W5**: `script/transit docs` and the drift test (§6), the marked regions in each codec's module
  doc and in `mappings.md`, and the note in each relay ADR.

## Findings

Recorded per workstream as each lands.

- **W1**: §2 named two gaps that weren't the whole list, and one that wasn't a gap. A `U64` above
  `i64::MAX` already read back unambiguously; `U64(5)` against `I64(5)` was the case to tag. The
  work also found `Bytes`, `Timestamp`, and non-finite `F64` values indistinguishable
  from strings in a value position, a backslash in `Bytes` that collided with the `\xHH` escape, a
  no-recorded-value metric that omitted its value, an exponential histogram side whose nonzero
  offset was dropped with its empty counts, and a sketch's fractional count rounded away. All are
  fixed additively; [ADR `stream-json-format`](../adr/stream-json-format.md)'s lossless
  amendment lists them.
- **W2**: the plan's `logit.input.lines.dropped` doesn't exist. Every line-framed listener counts a
  lost line under `logit.input.frames.dropped`, so a refused JSON line is
  `frames.dropped{reason="undecodable"}`, the stream driver's reason for a unit its decoder
  refused. No graph rule was needed for finiteness: the runtime already treats an input's `Ok`
  return as that node finishing, and only rule 93's field bounds are new.
- **W3**: §4's open question resolved as above, with no change to what `build_spec` builds. The
  driver runs every sink's encoder shape, both Prometheus modes and remote-write 1.0 and 2.0
  included, and each round trip already reads like a cell of §5's table: through `aggregate`, a
  timer reaches `statsd_out` as dotted lines counted `metrics.degraded`, `prometheus_out` drops a
  bare delta sum counted `metrics.dropped{reason="unresolved_delta"}`, and `influxdb_out` skips a
  set member. Exposition has no configured dialect, so the driver takes one as a parameter,
  OpenMetrics by default. `statsd_out` under `multi_value: skip` counts a skipped record's tags
  as `tags.omitted{reason="dialect"}` too, since it walks the tags before it skips; the counters
  section adds `*.omitted` to §4's list so W4's table sees that.
- **W4a**: the statsd column is filled from the module docs and `docs/known-gaps/statsd.md`, and
  the class check agreed with all of it except what follows, each now a cell of its own. Five
  outcomes neither doc stated:
  - `statsd_out` writes a multi-member `SetMembers` record as one `|s` line per member under
    either dialect, uncounted, so `users:alice:bob|s` reads back as two records. The module doc's
    "round-trips a timer or set line intact" holds for the value at a statsd server, not for the
    record (`set_members`: `normalized`, `member_per_line`). Only the encoder's function doc said
    so; the module doc now does too.
  - A negative absolute gauge, which `aggregate` produces from relative adjustments, leaves as the
    documented `0|g` then `-v|g` pair, and `statsd_in` reads that pair as a gauge of 0 and a
    `GaugeDelta`, uncounted (`gauge/negative`: `degraded`). A statsd server ends at the right
    value; a `logit` relay downstream receives two records.
  - Under `format: statsd`, splitting a multi-value timer line isn't counted;
    `messages.normalized{reason="dialect"}` counts only the `h`/`d`-to-`ms` rename.
  - An integer `sum` or `gauge` keeps every digit on the wire, but `statsd_in` reads every value
    as an `f64`, so one above 2^53 rounds on the way back (`i64`/`u64`: `normalized`,
    `f64_value`).
  - `statsd_out` counts `events.skipped` only for an event with no metrics. A log or a span on an
    event that also carries metrics is left off uncounted (`log/beside_metrics`,
    `span/beside_metrics`: `lost`, `uncounted`). A `*.skipped` or `*.omitted` counter for that loss
    is a candidate for a later PR.

  The review added two flows that set the options no other flow did, `statsd-relay-skip`
  (`multi_value: skip`, with `statsd-aggregate-skip` for the kinds only `aggregate` makes) and
  `statsd-relay-relative` (`relative_gauges: true`), and `coverage.md` now lists each cell no
  flow reaches; none is left. Every cell they reached agreed with the table.

  The schema grew where the cases needed it: `lost` as a field-level class beside the plan's five,
  `when:` lists for config-dependent cells, a `counter` field beside `reason`, and rows for the
  sink's own carrier against a foreign one, typed attribute values, a service check, a name the
  sink sanitizes, and logs. A flow's `replay_in` names one case file rather than a directory, so
  `logit run` can run it by hand; the file's directory is the flow's source. Coverage is a
  generated, committed `coverage.md`, checked like the expectations, rather than a printout.

  Expectations are one file per flow, `expected/<flow>.json`, each case's document under its
  name, rather than one file per flow and case. The per-case layout grows as flows times cases,
  224 files already, and reaches thousands once every source meets every sink column; one file
  per flow grows with the flows alone, and a diff inside it still shows which case changed.

  `format: json` writes attributes in the process interner's order, so adding cases for another
  source reordered attributes in the statsd cases and expectations and failed the hand-written
  ones. The spec sorts every map under an `attributes` key, and the `types` table, by key wherever
  it writes or compares JSON; the encoder, `AttrMap`, and the interner are unchanged. Pre-interning
  every key in the committed cases at the start of `generate` was rejected: it depends on global
  interner state and on scanning the whole case set.
- **W4b**: `influxdb_in` (16 cases, one a gzip Telegraf capture read in place) and
  `prometheus_in`'s scrape (12 cases) joined `statsd_in` (31 cases, three of them new), and
  `prometheus_out`, `influxdb_out`, and `otlp_out` joined `statsd_out` in all 54 rows: 16 flows,
  432 expectations, and every cell reached. Prometheus remote-write source cases aren't written: a
  protobuf body can't be hand-written, and no feature needs one until a native histogram can be
  decoded.

  The class check now reads each sink's spelling (`classes.rs`'s "Pairing"): `prometheus_out`'s
  sanitized names, `_total`, and label keys, `influxdb_out`'s `<name>.value`, the last element of
  an array for both, and `otlp_out`'s timestamp attribute as an integer. The schema gained
  `record/non_finite`, `record/superseded`, `record/same_instant`, `record/same_millisecond`,
  `record/min_max`, `attributes/map_value`, `bytes_value`, `timestamp_value`, and
  `attributes/scope`; a resource attribute under `_logit.` now reaches the carrier rows; and a
  `skipped` or `dropped` cell may say its loss is `uncounted` (the ADR's third amendment). The `otlp_out`
  driver arm reads the wall-clock observed time the encoder stamps on a log back as the run's
  clock. The attribute-order churn the new cases caused is W4a's fix above.

  Every cell agreed with its sink's module doc and `docs/known-gaps/mappings.md` except these,
  each recorded as the code behaves. None is fixed here:
  - `prometheus_out` and `influxdb_out` skip a log or a span on an event with no metrics without
    counting it (`log/*`, `span/alone`: `skipped`, `uncounted`), where `statsd_out` counts
    `events.skipped`. Neither the Prometheus encode table nor `mappings.md` names the loss;
    InfluxDB's table writes it with no counter.
  - `prometheus_out`'s exposition registry holds one sample per series, so an earlier point of a
    series in the same run is replaced, uncounted (`record/superseded`: `skipped`, `uncounted`).
    Neither the codec's normalization list nor `mappings.md` names it. Since `tfix/w3` the cell is
    `normalized`, reason `superseded`: the pull model's overwrite.
  - Remote-write sends each point of a series, but two at one instant reach `events_to_families`
    in one group, which keeps the later and drops the earlier, uncounted (`record/same_instant`:
    `dropped`, `uncounted`). Two at different nanoseconds of one millisecond are collapsed the
    same way and counted, under `metrics.degraded{reason="sub_ms_collapsed"}`, a `degraded`
    series for a reading that's gone (`record/same_millisecond`: `dropped`). Since `tfix/w3`
    both count `metrics.dropped{reason="duplicate_timestamp"}`, or
    `metrics.normalized{reason="duplicate_timestamp"}` for a state equal to the one sent
    (`record/same_instant_equal`, `record/same_millisecond_equal`)
    ([ADR `dedup-timestamps-transform`](../adr/dedup-timestamps-transform.md)).
  - `prometheus_out` writes no start time for a gauge: `_created` is a counter's, histogram's,
    or summary's, so a gauge's `start_timestamp` is lost uncounted. The encode table doesn't say.
  - Under OpenMetrics, `prometheus_out` writes a gauge whose `_logit.prometheus.type` is
    `untyped` as `unknown`, so the carrier reads back changed (`private_carrier`: `normalized`).
  - `influxdb_out` writes a `sum` as a `value` field that `influxdb_in` reads back as a gauge
    named `<name>.value`. The codec calls dropping the temporality normalization 10; since the
    kind changes, the table reads it `degraded`, and a gauge, renamed, `normalized`
    (`value_field`).
  - §7's `influxdb_in -> aggregate(cumulative) -> prometheus_out` can't show a delta sum:
    `influxdb_in` decodes every field to a gauge, so the window emits each series' latest value.
    `statsd_in -> prometheus_out` reaches the delta-sum rows instead.
  - `prometheus_out` writes no histogram `min` or `max`, uncounted (`record/min_max`: `lost`).
    The encode table and `mappings.md` both name it; the table records it as a field row of its
    own, since the rest of the histogram survives.

  None of the first five was in `docs/known-gaps/mappings.md`. W5's `gaps:` regions now state the
  first four there; the fifth is `normalized`, which those regions leave out, and is in
  `prometheus_out`'s module-doc region.
- **W5**: the four `doc:` regions and the four `gaps:` regions render every cell of the four
  columns, and the drift test fails on a hand edit inside one (checked by editing a rendered row
  in each form). Writing them settled what a region holds and how much prose it replaces
  ([ADR `transit-spec`](../adr/transit-spec.md)'s generated-regions amendment): a hand-written
  row goes only when the region states all of it, so a row that also names something the table
  doesn't stays beside the region.

  Removing hand rows found two places where the table said less than the prose, each fixed in
  `mapping.yaml`'s text, with no class changed:
  - The diagnostic keys: `delta_temporality_unresolved`, `gauge_delta_unresolved`,
    `encode_error`, `no_recorded_value`, `otlp_set_metric_skipped`, and
    `otlp_set_members_metric_skipped` were named only in the rows being replaced, so the cells
    that warn under them now say so.
  - `otlp_out`'s `attributes/number_value` said a `u64` reads back as an `i64`. One above
    `i64::MAX` leaves as a double (`crates/logit-proto/src/otlp/common.rs`), as `mappings.md`'s
    hand row says, and the cell now says so too.

  Review found two cells classed `exact` for an outcome the encoder doesn't produce, each the
  table's error, each fixed with a row and a hand-written `statsd` case that reaches it:
  - `record/exemplars` at `otlp_out` was `exact` with "except on a `Summary`, which has none",
    but a model `Summary` can carry exemplars and `SummaryDataPoint` has no field for them. The
    new field row `record/exemplars_on_summary` is `lost`, uncounted, at `otlp_out`, and `lost`,
    counted `exemplar_omitted`, at `prometheus_out` (case `summary-exemplars`).
  - `record/no_recorded_value` at `prometheus_out` under remote-write was `exact`, but only a
    flagged `Gauge` or `Sum` becomes a stale marker; any other flagged kind is skipped, counted,
    in both modes. The row now covers a flagged `sum` or `gauge`, and
    `record/no_recorded_value_other_kind` every other kind (case `flagged-histogram`).

  A `when:` key names sink config, so both rows put the kind in the feature's name. The flagged
  histogram also showed a sink counting a record once under the first reason it meets:
  `statsd_out` under `multi_value: skip` counts it `no_recorded_value`, not `histogram`, so of
  several reached cells at the record's worst class, the class check now lets the one whose
  counter moved decide (`classes.rs`'s "Counters").

  A second review round found the same order breaking the worst-class rule outright, since
  `statsd_out`, `influxdb_out`, and `prometheus_out` test the flag before the kind: a flagged
  `gauge_delta` is skipped there rather than dropped, a flagged exponential histogram under
  remote-write is skipped rather than dropped as `native_histogram`, and a flagged delta `sum`
  under remote-write is a stale marker rather than dropped as `unresolved_delta`. A flagged
  record's `no_recorded_value` row now goes ahead of a kind row's counted loss whose counter
  didn't move; `otlp_out`, which tests a `gauge_delta`'s kind first, moves that counter and the
  kind row decides as before. The flagged delta `sum` reads back as a flagged cumulative counter,
  since `stale_point` ignores temporality, so it has its own row,
  `record/no_recorded_value_delta_sum`, `degraded` under remote-write, uncounted, and the
  `prometheus_out` spelling gives a flagged monotonic `sum` its `_total` whatever its temporality
  (case `flagged-kinds`). A flagged non-monotonic delta `sum` under remote-write goes on a
  `gauge` counted `degraded{metric_kind=non_monotonic_sum}`, which the row's text states but no
  case reaches, so no cell names that counter for it yet.

  No hand-written row contradicted a cell's class except one already recorded: `influxdb_out`
  dropping a `Sum`'s temporality, which the codec's permitted-normalization list calls
  normalization 10 and the table reads `degraded`, since the kind changes. The list is prose and
  keeps it; the `gaps:` region states the `degraded` reading. ADR `prometheus-scrape-and-exposition`'s
  encode table says a delta `Sum` or `Histogram` is skipped under `skipped{metric_kind="delta_sum"}`;
  the code and the table drop it under `dropped{reason="unresolved_delta"}`. The ADR's table
  stays as written, under its new note.

  Hand-written rows kept because the table has no row or cell for what they state, each a
  candidate for a row with a case:
  - `influxdb_out`'s omission of a non-finite component of a `Distribution`, `Samples`,
    `Histogram`, or `Summary`, counted `metrics.degraded{reason="non_finite_component"}`; no case
    holds one. (Its `no_fields` drop is gone since ADR `influxdb-histogram-summary-shape`, which
    writes `count` on every such line.)
  - A `Null` attribute, omitted as unrepresentable at `prometheus_out` and `influxdb_out`; the
    table has no `null_value` row.
  - `prometheus_out`'s label and name collisions, its type conflicts, its `# UNIT` character
    rule, and `Resource::schema_url` and `dropped_attributes_count` at three sinks.

  Rows the table states that no hand-written doc did, now in the regions: the five W4b findings
  above; `statsd_out`'s negative-gauge pair read back as two records (`gauge/negative`), its
  uncounted loss of a log or span beside metrics, of the instrumentation scope, and of a foreign
  carrier; `otlp_out`'s uncounted loss of a `_logit.` carrier; and each sink's `when:` variants,
  such as `statsd_out`'s `format: statsd` tag and carrier omissions, side by side.
- **Split tests** (`transit/split-tests`, after W5): the generator ran every case through every
  flow in one `#[test]`, so nextest couldn't spread it across workers and its time grew with
  cases times flows. On CI it took 2.3 s at 8 flows (#769), 2.5 s at 16 (#770), and 5.0 s at 16 on
  a slow runner (#772), past nextest's 5 s `slow-timeout`; CI's runners run about 16 tests at once
  and vary up to about 9x in speed. It's now a test per flow beside a `cases` test and a `ledger`
  test, with each flow's observations committed as `ledger/<flow>.json` so the checks across
  flows need no flow run ([ADR `transit-spec`](../adr/transit-spec.md)'s per-flow amendment).
- **InfluxDB and Prometheus into `statsd_out`, and InfluxDB into its own pair and `otlp_out`**
  (`tcoll/w1a`): `influxdb-to-statsd` and `prometheus-to-statsd`, each `replay_in -> statsd_out`
  under DogStatsD, then `influxdb-relay` and `influxdb-to-otlp`. An `aggregate` variant would
  reach no cell these don't: the statsd variants exist to make distributions, set estimates, and
  running totals from raw statsd kinds, and these sources already carry running totals and
  gauges. Every cell they reached agreed with the table but two, each a gap in what the table
  could say rather than in the code:
  - `statsd_out` writes each reserved character in a tag key, and each but `:` in a tag value, as
    `_`, uncounted. `statsd_in` reads such a tag as written, since it splits a tag segment only
    on `,` and a tag on its first `:`, but no statsd case held one. The `tags-reserved` case does
    now, the check's `statsd_out` spelling rewrites keys and values as the encoder does, and
    `attributes/string_value` reads `normalized` (`tag_sanitized`) under DogStatsD.
  - An InfluxDB point whose fields are all strings decodes to an event with no metric, log, or
    span, and no row covered a record that isn't one. Two rows do now, split by the
    measurement carrier, since `influxdb_out` writes such a point's line only under it:
    `event/string_only`, the point with its carrier intact, and `event/attributes_only`, any
    other such event, which a stage that drops the carrier (`keep` under `private: drop`) or a
    Lua `Event.new` leaves, and the hand-written `no-carrier` case reaches. `statsd_out` skips
    either under `logit.output.events.skipped`, `prometheus_out` and `otlp_out` skip either
    uncounted, and `influxdb_out` writes the string-only point's line back and skips the other,
    uncounted.
- **`collectd_in` source** (`tcoll/w1b`): five cases. `value-lists` and `notification` read
  `testdata/interop/collectd/`'s captures in place, and `types-db` reads the value-list capture
  again with the stock `types.db` entries for its types, named in the meta's new `types_db:`
  field. Two hand-built datagrams cover what no capture has: `counter-absolute` (COUNTER, one of
  them above `i64::MAX`, ABSOLUTE, and a NaN GAUGE) and `legacy-time` (the second-resolution Time
  and Interval parts).

  Those two are wire files, not hand-written JSON. A data-source type or a legacy time part is a
  wire feature a `.json` can't state, and a hand-built datagram runs the real decoder. Each
  case's meta comment describes its bytes part by part. A collectd datagram qualifies because
  it's a flat run of type-length-value parts with a fixed, hand-readable layout; W4b's
  remote-write protobuf body doesn't, which is why those cases aren't written. The README's
  "Adding a case" states the rule.

  Every record reached an existing row at the class it states, so `collectd_in` added no row to
  the table; the `event/*` rows in the bullet above came with `tcoll/w1a`. What the flows show
  about collectd's data at the four sinks, each as the table already states it:
  - `influxdb_out` writes a COUNTER, DERIVE, or ABSOLUTE value as a plain `value` field, so its
    cumulative or delta reading is gone, uncounted (`sum/*`: `degraded`, no counter). Each data
    source of a multi-data-source list becomes its own measurement, `load.load.0` to
    `load.load.2`, rather than one measurement with a field per data source.
  - `prometheus_out` drops an ABSOLUTE value, counted, as for any bare delta sum, and writes a
    DERIVE value as a gauge, counted `degraded`. Its exposition registry keeps only the later of
    the two read cycles a datagram can carry, uncounted (`record/superseded`).
  - `statsd_out` writes a COUNTER or DERIVE running total as a `|g` gauge, counted `degraded`.
  - Every sink but `collectd_out` leaves off the `_logit.collectd.interval`, `plugin`, `type`, and
    `severity` carriers, uncounted (`attributes/foreign_carrier`). On a value list the plugin and
    type survive in the record name. A notification has no record name, so its plugin and type
    are gone at `otlp_out`, the one sink that writes a notification.
  - `influxdb_out` and `prometheus_out` skip a notification uncounted, and `statsd_out` counts it
    `events.skipped` (`log/plain`).
  - A `types_db:` changes only the record names (`load.load.0` against `load.load.shortterm`), so
    each sink writes the same values under other series names; no class differs.

  Two of these are gaps no cell states, and [the mappings known gaps](../known-gaps/mappings.md)
  records each: a list's data sources as separate measurements at `influxdb_out`, and a
  notification's plugin and type at `otlp_out`. It also records a wrapped COUNTER reading as a
  reset at `prometheus_out`, which no flow can show, since it's outside the codec layer.
- **`otlp_in` source, metrics** (`totlp/w1`): 14 cases over 56 features. Thirteen are hand-written
  OTLP/HTTP bodies in `.bin` beside a `.headers` naming `path:`, `content-type:`, and
  `content-encoding:`, decoded through `otlp_in`'s own `decode_request`. One of them is protobuf
  (`sums-protobuf`, the `MetricsData` the JSON decoder reads from `sums.bin`, so its `.json`
  equals `sums.json`), and one is gzip-compressed. The fourteenth, `telemetrygen`, reads
  `testdata/interop/otlp/metrics.json` in place: a capture with no `.headers` is read as the
  Collector `file` exporter's lines, one OTLP/JSON request each. OTLP/JSON is readable text, so a
  hand-written body runs the real decoder and needs no `.json` written by hand.

  `otlp-relay` is a fixed point for every case, schema URLs and scopes included. Every other
  record reached an existing row at the class it states but these, each recorded as the code
  behaves. None is fixed here:
  - `prometheus_out` writes an exemplar's attribute keys as label names unsanitized
    (`text::exemplar_labels`). Under OpenMetrics a key such as `customer.tier` makes the line
    invalid, and `prometheus_in` drops it, counted only at the decoder
    (`input.metrics.dropped{reason=malformed_line}`): a counter is lost
    (`record/exemplar_key_unsafe`: `dropped`, uncounted), and a histogram's count for that bucket
    reads back in the next one (`record/exemplar_key_unsafe_histogram`: `degraded`). Remote-write
    carries the key as written and `prometheus_in` reads it back. No row could say this, so the
    two rows and their reach rule in `classes.rs` are new; only `prometheus_out` writes
    exemplar keys under a name rule, so no other sink has a cell. The rule reads only the
    exemplars the writer puts on a line, through `text::exposed_exemplar_keys`, so a second
    exemplar the writer leaves off doesn't reach the rows (case `exemplars-unwritten`).
    `prometheus_out` now sanitizes those keys as it does a series label's, in both modes, so both
    rows read `normalized`.
  - `statsd_out` and `datadog_out` write each element of an array attribute as its own tag, so a
    number element reads back as a string (`attributes/array_value`: `normalized`,
    `stringified`). The statsd and Datadog cases held only string arrays, so the cells read
    `exact`.

  What the flows show about OTLP's metric model at each sink, each as the table already states
  it:
  - `datadog_out` writes only a delta monotonic sum and a gauge: a cumulative or non-monotonic
    sum, a histogram, an exponential histogram, and a summary are skipped, counted. Since
    [`docs/plans/ddotlp.md`](ddotlp.md)'s W1, it writes all but a cumulative monotonic sum,
    histogram, and exponential histogram, as Datadog's exporter does.
  - `influxdb_out` writes a histogram's finite buckets without its count, sum, min, max, or `+Inf`
    bucket, and a summary's quantiles without its count or sum, uncounted.
  - `statsd_out` writes an exponential histogram's count, sum, zero count, min, and max but no
    bucket, and a negative `.min` leaves as the `0|g` then `-v|g` pair.
  - `prometheus_out` writes no start time for a gauge, drops a delta sum or histogram, skips an
    exponential histogram, and leaves off the scope; exposition keeps one point per series, and
    remote-write collapses telemetrygen's two points in one millisecond.

  No feature row covers a resource's or scope's schema URL, which every sink but `otlp_out`
  leaves off uncounted; the hand-written rows beside the regions name it (W5's list above).
- **`graphite_in` source** (`tgraph/w1`): nine cases over 23 features.
  - Four read `testdata/interop/graphite/`'s captures in place: `write-graphite`, collectd's
    `write_graphite` plugin sending 100 CRLF plaintext lines over four read cycles, and
    `pickle-p2`, `pickle-p5`, and `pickle-py2`, CPython 3.12 at protocols 2 and 5 and Python 2.7's
    `cPickle`, each one length-prefixed frame on a connection.
  - Four read one payload each from `testdata/differential/graphite-pickle/` in place: a tagged
    path beside an untagged one recalled through the memo, the `-1` receipt-time timestamp, a
    Python 2 UTF-8 path, and the value 2^63.
  - `tagged` is hand-built plaintext, since no recorded sender writes carbon 1.1's tags: tags, a
    repeated key, a `_logit.` tag name, values holding `=` and `~`, exponent, signed, integer,
    and negative values, `-1` and fractional timestamps, runs of whitespace, and a non-ASCII
    path.

  A case's meta gains `protocol: plaintext | pickle`, read only for a graphite case, since a
  pickle capture and a plaintext one share the `.raw` extension. A pickle wire file is split into
  payloads by the length-prefix `Framer` `graphite_in` uses, or read as one payload under `.pkl`.
  The Dropwizard capture isn't read: its NaN gauge counts
  `input.metrics.dropped{reason=non_finite_value}`, and the generator refuses a wire file whose
  decode drops anything.

  `graphite_in` decodes every datapoint to an `f64` gauge with string tags, so the flows reach
  only `gauge/f64`, `gauge/negative`, `attributes/string_value`, `attributes/no_host`,
  `record/name_unsafe`, and `record/superseded`. Every record reached an existing row at the
  class it states, so the table gained no row. What the flows show at each sink, each as the
  table already states it:
  - `statsd_out` writes a negative gauge as the `0|g` then `-v|g` pair, which `statsd_in` reads
    back as a gauge of 0 and a relative adjustment (`gauge/negative`: `degraded`).
  - `prometheus_out` writes each `.`, `-`, and non-ASCII character of a path as `_`
    (`servers.web-1.cpu` as `servers_web_1_cpu`, `café.temperature` as `caf__temperature`),
    uncounted (`record/name_unsafe`), so two paths that differ only there share a series. Its
    exposition registry keeps only the last of `write-graphite`'s four read cycles, uncounted
    (`record/superseded`).
  - `influxdb_out` writes each path as a measurement with one `value` field, read back as a
    gauge named `<path>.value` (`gauge/*`: `normalized`, `value_field`).
  - `datadog_out` writes a `host` tag as the point's host and stamps the process hostname on a
    point with none (`attributes/no_host`: `normalized`), and writes a fractional timestamp in
    whole seconds.

  One disagreement in the class check, not fixed here: its `statsd_out` spelling (`classes.rs`'s
  `sanitized`) reads every character outside `[A-Za-z0-9._/-]` as `_`, while `statsd_out`
  rewrites only `:`, `|`, `@`, `#`, `,`, control, and whitespace characters. So `café.temperature`
  reaches `record/name_unsafe` at `statsd_out` and reads back `exact`, which the `normalized` cell
  accepts.
- **`graphite_out` column** (`tgraph/w2`): 63 of the table's 73 rows have a `graphite_out` cell,
  and 11 flows end in it: the like pair under plaintext, `protocol: pickle`, and `tags: drop`;
  one from each other source under the default `multi_value: skip`; and `statsd-to-graphite-expand`
  and `otlp-to-graphite-expand`. The driver encodes through `graphite_out`'s encoder with the
  sink's line cap (`logit_outputs::graphite::line_cap`) and reads each `MessageBuf` entry back
  through `GraphiteDecoder`, a pickle frame with its length prefix stripped as `graphite_in`'s
  framer strips it. The rows with no cell are the field rows of kinds `graphite_out` writes only
  in a lesser form, the two exemplar-key rows, `attributes/private_carrier` (`graphite_in`
  stamps no carrier), and the Datadog-derived log and span fields.

  The class check's name rule is now each sink's own: `Spelling::sanitize` reads a name through
  the encoder's own predicate, `logit_outputs::statsd::is_forbidden_in_name` for `statsd_out`
  and `logit_proto::graphite::encode`'s path, tag-name, and tag-value predicates and its
  tag-value renderer for `graphite_out`, made public for it, rather than a copy of either. That
  settles the disagreement `tgraph/w1` recorded: a non-ASCII or `=` character no longer reaches
  `record/name_unsafe` at `statsd_out`. Four records moved off the row, each read back `exact`
  before and after: `café.temperature`, `py2.café`, and `logit-fixture.diamond.café.count` in
  `graphite-to-statsd`, and `interop_escapes.f=e` in `influxdb-to-statsd`, where the case's
  other records still reach the row. The pass-3 dotted base uses the sink's spelling for
  `statsd_out` and `graphite_out` alike.

  The like pair reads `exact` everywhere under all three flows, `attributes/string_value`'s
  `tags: drop` cell aside, which reads `lost` as configured. No case holds a path or tag that
  `graphite_out` sanitizes (a `/`, a `\`, a tag name's `!` or `^`, a value's leading `~`), so
  normalization 10 isn't exercised by the pair. Settled in `tgraph/known-gaps`: a `tagged` line
  holds each of them, and its path reads back `normalized` under all three flows and its tags
  under the two that write tags, and a `pickle-p0` case reads the CPython protocol-0 capture.

  Each recorded as the code behaves, none fixed here:
  - Every `Sum` read `degraded` with no counter: the bare value reads back as a gauge, which the
    codec calls normalization 12, "not a skip". Settled in `tgraph/w3`: `graphite_out` counts
    `metrics.degraded{metric_kind=…}` once per record in `statsd_out`'s vocabulary,
    `cumulative_sum` and `non_monotonic_delta_sum`, plus `monotonic_delta_sum` for the monotonic
    delta sum `statsd_out` writes exact. Unlike `statsd_out`, it still writes every sum under
    `multi_value: skip`.
  - An event with no metrics (a log, a span, or attributes alone) was skipped with no counter or
    diagnostic, only `EncodeStats::skipped_no_metrics`. Settled in `tgraph/w4`: it counts
    `events.skipped{reason=no_metrics}` once per event, and the `log/dogstatsd_event`,
    `log/plain`, `span/alone`, `event/attributes_only`, and `event/string_only` cells name it. A
    log or span beside metrics is still lost uncounted (`log/beside_metrics`,
    `span/beside_metrics`).
  - Under `multi_value: expand`, a non-finite `.sum`, `.min`, `.max`, or quantile was left off
    uncounted, where `influxdb_out` counts `metrics.degraded{reason=non_finite_component}`.
    Settled in `tgraph/w4`: `logit_proto::multi_value::expand_dotted` returns how many components
    it skipped, and `graphite_out` counts that series once per record. `statsd_out` ignores the
    count, so it still leaves them off uncounted. No case reaches the series, so no cell names it
    as its counter; the `histogram/delta` cell's text mentions it.
  - `metrics.normalized{reason=tag_sanitized}` counts once per record, whichever attribute moved
    it, so the `string_value`, `bool_value`, `number_value`, and `resource_attribute` cells all
    name it. An array element alone can't move it without one of those beside it, which the
    README's "Counters no row names yet" lists with the column's other unreached series.
  - Under `tags: drop`, series that differ only in their tags write one path, which the
    `string_value` cell says. No other source's flow runs `tags: drop`: pass 3 pairs a sum by
    name, and under drop two sums of one name and different tags score the same, so the first
    claims both read-back gauges. `docs/known-gaps/mappings.md` records it, with two other
    places the check misreads `graphite_out`.
  - `record/superseded`, `same_instant`, and `same_millisecond` read `exact`, since the codec
    writes every point. A whisper store keeps one point per path and second, so two points of a
    series in one second keep only the later there; the cells say so, and no row counts it.
  - Carbon 1.1.10's `TaggedSeries.parse_carbon` sets `tags['name']` to the path after reading
    the tag segments, so an attribute named `name` is replaced at a tagged carbon, uncounted.
    `graphite_out` writes it as any other tag. No case carries one. Settled in `tgraph/w3`:
    `graphite_out` doesn't guard against it. The operator renames or removes the attribute at the
    source, or ahead of the sink with a `remove` or `lua` component.
- **Logs, from the survey** (`tlog/w0`): reading the log sinks for §9 found five behaviors, each
  recorded as the code behaves. None is fixed in this stream; W1 to W5 give each a cell, and an
  operator-visible one an entry in `docs/known-gaps/`.
  - `datadog_out` writes `trace_id` and `span_id` from `LogRecord::trace`
    (`crates/logit-proto/src/datadog/logs.rs`, under `WIRE_TRACE_ID` and `WIRE_SPAN_ID`), but
    `datadog_in`'s `decode_log_item` sets no `trace` and keeps them as attributes, so the pair
    isn't symmetric for trace context.
  - `DatadogEncoder::with_default_source` (`crates/logit-proto/src/datadog/mod.rs`) is called
    only from tests, so `datadog_out` never writes a default `ddsource`.
  - `splunk_hec_out` and `datadog_out` read neither `_logit.otel.severity_*` nor
    `_logit.syslog.severity` (`crates/logit-proto/src/splunk/logs.rs`,
    `crates/logit-proto/src/datadog/logs.rs`). Each writes a public severity attribute verbatim
    when the log carries one (`otel.log.severity.*`, Datadog's `status`), and otherwise the band
    `Severity`, so OTLP's `INFO2` (10) leaves Splunk as 9, `Info`.
  - `syslog_out` counts `logit.output.events.skipped` with no `reason` tag
    (`crates/logit-outputs/src/syslog.rs`, from `EncodeStats::skipped_no_log`), where every other
    sink that counts the series tags it. It drops MSGID under RFC 3164, since `write_rfc3164_header` takes no
    `msgid`, and every attribute outside its header and carriers, uncounted, unless
    `structured_data:` is set under RFC 5424.
  - `otlp_out`'s `encode_trace` (`crates/logit-proto/src/otlp/logs.rs`) writes the event's span
    ids for a log with no `trace`, so `otlp_in` reads back a `trace` the input didn't have: not a
    fixed point.
- **Logs through `otlp_out` and `datadog_out`** (`tlog/w1`): the `otlp-logs` and `datadog-logs`
  cases put every log field in front of both sinks. Each behavior below is a cell in
  `mapping.yaml`, observed at its class; none is fixed.
  - `datadog_out` writes a log's message through `logit_proto::json::value_text`
    (`crates/logit-proto/src/datadog/logs.rs`): a `Map`, `Array`, number, or bool message as its
    JSON text, a `Bytes` one as lossy UTF-8, and a log with no body as the message `"null"`.
    `datadog_in` reads each back as a string (`log/message_unsafe`, `structured_message`,
    `bytes_message`).
  - `datadog_out` writes a `Bytes` attribute on a log as base64 text, which `datadog_in` reads
    back as a string (`attributes/bytes_value`).
  - `datadog_out` writes a log's `event_name`, observed time, body format, trace flags, and
    dropped-attribute count nowhere, and `datadog_in` reads every log's format back as `raw`
    (`log/event_name`, `observed_timestamp`, `body_format`, `trace_flags`,
    `dropped_attributes_count`, each `lost`, structural). Its timestamp leaves in milliseconds.
  - A log's trace and span ids read back from `datadog_in` as the `trace_id` and `span_id`
    attributes, not as the log's `trace` (`log/trace`, `normalized`), as the `tlog/w0` finding
    above has it.
  - `otlp_out` writes `_logit.otel.severity_number` and `_logit.otel.severity_text` back as the
    record's own fields, and `datadog_out` leaves both off
    (`attributes/severity_carrier`); `otlp_out` stamps a log with no observed time
    (`log/observed_timestamp_unset`, `normalized`).
  - `otlp_in` reads a `u64` attribute above `i64::MAX`, which `otlp_out` writes as a
    `doubleValue`, back as an `F64`; the class check's OTLP spelling compares it as that double.
