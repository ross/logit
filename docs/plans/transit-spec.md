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

A case's wire file is the authoring form where one exists: statsd lines, line protocol,
exposition text, carbon plaintext, and OTLP/JSON are readable text; collectd, pickle, remote-write,
and Datadog protobuf are `.bin` with a `.headers` sidecar, as `testdata/interop/` does. A
recorded capture under `testdata/interop/` joins by reference, as the `wire:` path in the case's
`.meta.yaml`, not by copy. A hand-written edge case is the JSON alone. Both are "as `X_in` would produce it," so a flow
reads one form.

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
`expire_after:`, which reads a clock, so exposition reads back every series `max_series:` keeps. The driver is a `#[cfg(test)]` module of the
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
| record fields | `unit`, `description`, `start_timestamp`, `exemplars` (field level) |
| record features | `flags.no_recorded_value`, a service check's carriers, a name the sink must sanitize (record level) |
| attribute shapes | a string, boolean, number, or `Array` value, the sink's own `_logit.` carrier, another protocol's, a resource attribute |
| logs | a DogStatsD event, any other log |

A column is a sink. A cell is a class, a reason, and the counter it counts under:

| Class | Level | Meaning |
|---|---|---|
| `exact` | both | the pair's decoder reads back the same kind and value |
| `normalized` | both | a permitted normalization applied (named) |
| `degraded` | record | the value survives in a lesser kind (a sketch as five quantiles) |
| `lost` | field | a field the record loses while the record survives, counted or named `uncounted` |
| `skipped` | record | the record leaves no wire form, counted `skipped{reason}` |
| `dropped` | record | the record is lost for a reason the config didn't ask for, counted `dropped{reason}` |

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

`script/transit docs` rewrites marked regions from `mapping.yaml`, and a test fails when a region
is stale, the pattern `script/schema` and `schema/logit.schema.json` already use:

- each codec's module-doc mapping table (`crates/logit-proto/src/<codec>/mod.rs`), the column
  of the table for that sink, rendered as it reads today;
- `docs/known-gaps/mappings.md`'s cross-protocol table, every `degraded`, `lost`, `skipped`,
  and `dropped` cell, grouped by kind.

The permitted-normalization lists stay prose: they're per-codec policy, not per-cell outcomes.
Each relay ADR's mapping table stays as written, with a note that the table was the decision at
the time and `spec/transit/mapping.yaml` is current.

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

W4b landed these with three more flows: `influxdb_in -> prometheus_out` as remote-write 2.0, and
`statsd_in -> prometheus_out` and `statsd_in -> influxdb_out`, so every kind the statsd cases
hold reaches each new column.

### 8. Not in this stack

- A backend layer: the same corpus through a real sink to a real backend, with the backend's
  reading recorded beside the expectation. It's the layer that would catch a shared
  misunderstanding, and it's deferred with that risk accepted.
- Sink-tier flows against an in-process capture server, for the sink behavior above the encoder.
- `kafka_out` and `splunk_hec_out` columns: their metric forms are JSON documents with their own
  readers, and neither has a `_in` pair that reads metrics back the same way (`splunk_hec_in`
  does; it joins when a flow asks).
- Compressed JSON archives: gzip on `file_out`'s `format: json` rotation and `replay_in` reading a
  `.gz` file by its extension, added together in one follow-up (`docs/known-gaps/tailing.md`).

## Workstreams

| # | Branch | PR title | Size | Depends on |
|---|---|---|---|---|
| W0 | `transit/w0` | `transit/w0: ADR and plan for a transit spec` | S | — |
| W1 | `transit/w1` | `transit/w1: a lossless format: json, with a reader` | M | W0 |
| W2 | `transit/w2` | `transit/w2: replay_in, reading format: json lines and native frames` | M | W1 |
| W3 | `transit/w3` | `transit/w3: the in-process flow driver` | M | W2 |
| W4 | `transit/w4` | `transit/w4: spec/transit, the mapping table, and the first pairs` | L | W3 |
| W5 | `transit/w5` | `transit/w5: module-doc and known-gaps tables generated from the spec` | M | W4 |

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
- **W4b**: `influxdb_in` (15 cases) and `prometheus_in`'s scrape (12 cases) joined `statsd_in` (30
  cases, two of them new), and `prometheus_out`, `influxdb_out`, and `otlp_out` joined
  `statsd_out` in all 51 rows: 15 flows, 387 expectations. Every cell is reached but
  `prometheus_out`'s two remote-write cells for an exponential histogram, since no remote-write
  flow reads a source that makes one. Prometheus remote-write cases aren't written: a protobuf body
  can't be hand-written, and no feature needs one until a native histogram can be decoded.

  The class check now reads each sink's spelling (`classes.rs`'s "Pairing"): `prometheus_out`'s
  sanitized names, `_total`, and label keys, `influxdb_out`'s `<name>.value`, the last element of
  an array for both, and `otlp_out`'s timestamp attribute as an integer. The schema gained
  `record/non_finite`, `record/superseded`, `attributes/map_value`, `bytes_value`,
  `timestamp_value`, and `attributes/scope`; a resource attribute under `_logit.` now reaches the
  carrier rows; and a `skipped` or `dropped` cell may say its loss is `uncounted`. The `otlp_out`
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
    Remote-write sends each. Neither the codec's normalization list nor `mappings.md` names it.
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

  None of the first four is in `docs/known-gaps/mappings.md`, which W5 generates from the table.
