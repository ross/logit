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
  cases/<system>/<case>.json    one lossless event JSON file per case, as <system>_in produces it
  cases/<system>/<case>.<wire>  the wire bytes a generated case decodes from (optional)
  cases/<system>/<case>.headers the request headers beside an HTTP wire file (optional)
  flows/<flow>.yaml             a runnable logit config: replay_in -> [stages] -> <sink>
  expected/<flow>/<case>.json   the re-decoded output and the counters, generated
```

A case's wire file is the authoring form where one exists: statsd lines, line protocol,
exposition text, carbon plaintext, and OTLP/JSON are readable text; collectd, pickle, remote-write,
and Datadog protobuf are `.bin` with a `.headers` sidecar, as `testdata/interop/` does. A
recorded capture under `testdata/interop/` joins by reference from the case's JSON, not by copy.
A hand-written edge case is the JSON alone. Both are "as `X_in` would produce it," so a flow
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
under `logit run` for a look at what a sink emits. The driver runs it in-process, in
`crates/logit-cli/tests/`, because the kind registry (`logit_cli::pipeline`) is what builds
components from a config:

1. Build the flow's components from its config, substituting the case file for `replay_in`'s
   path and a capture for the sink's transport.
2. Decode the case with a fixed `received_at`.
3. Run each stage with its real `Transform::process`, then `flush(now)` at a fixed `now`, so a
   window-driven stage emits once and deterministically.
4. Encode through the sink's real `Encoder` (or `FramedEncoder`, `SignalEncoder`), then decode the
   bytes through the pair's real `Decoder`.
5. Record the re-decoded batch as lossless event JSON, and the counters the sink and stages
   incremented, with reasons (`degraded`, `skipped`, `dropped`, `normalized`,
   `passed_through`).

Where output is nondeterministic for a reason the code can't remove (a `Set`'s estimate moves by
one with merge order past 128 members), the case declares a tolerance on the field, and the
comparison applies it.

Open: how the driver reaches a sink's encoder through its `Output` without a socket. The in-process
round-trip tests bind a real listener; the driver should not. Either the registry exposes the
encoder a sink was built with, or the sink's transport takes an in-memory capture. W3 decides
and records it in the README.

### 5. The mapping table (W4)

`mapping.yaml` is the spec. A row is a model kind and the features that change its fate:

| Dimension | Values |
|---|---|
| kind | `sum`, `gauge`, `gauge_delta`, `samples`, `distribution`, `set_members`, `set`, `histogram`, `exponential_histogram`, `summary` |
| temporality | `delta`, `cumulative` (sum, histogram) |
| monotonic | `true`, `false` (sum) |
| scalar | `f64`, `i64`, `u64`, `bool` (sum, gauge, gauge_delta) |
| record fields | `unit`, `description`, `start_timestamp`, `exemplars`, `flags.no_recorded_value` |
| attribute shapes | an `Array` value, a `_logit.` carrier, a resource attribute |

A column is a sink. A cell is a class and a reason:

| Class | Meaning |
|---|---|
| `exact` | the pair's decoder reads back the same kind and value |
| `normalized` | a permitted normalization applied (named) |
| `degraded` | the value survives in a lesser kind (a sketch as five quantiles) |
| `skipped` | the record leaves no wire form, counted `skipped{reason}` |
| `dropped` | the record is lost for a reason the config didn't ask for, counted `dropped{reason}` |

Each cell carries a sentence of operator-facing text: what a consumer of that sink sees, and what
they lose. That text is what the docs are generated from.

The generator checks every flow result against the table: the re-decoded kind and the counters a
case produced must land in its cell's class, or the run fails with the case, the cell, and the
observed outcome. A disagreement is a finding, never a regenerate: either the code is wrong, or
the table is, and a person decides which.

Rows for log and span fields (severity, timestamp precedence, body format, structured data, trace
state) are added when a flow carries them. The table's schema doesn't change for them.

### 6. Generated docs (W5)

`script/transit docs` rewrites marked regions from `mapping.yaml`, and a test fails when a region
is stale, the pattern `script/schema` and `schema/logit.schema.json` already use:

- each codec's module-doc mapping table (`crates/logit-proto/src/<codec>/mod.rs`), the column
  of the table for that sink, rendered as it reads today;
- `docs/known-gaps/mappings.md`'s cross-protocol table, every `degraded`, `skipped`, and
  `dropped` cell, grouped by kind.

The permitted-normalization lists stay prose: they're per-codec policy, not per-cell outcomes.
Each relay ADR's mapping table stays as written, with a note that the table was the decision at
the time and `spec/transit/mapping.yaml` is current.

### 7. The first pairs (W4)

Enough to prove the framework on every kind of transition, not to cover the matrix:

| Flow | What it proves |
|---|---|
| `statsd_in -> statsd_out` | a like pair reads as `exact` throughout, under `format: dogstatsd` |
| `statsd_in -> aggregate -> statsd_out` | raw kinds through a stage, in each `distributions:` and `sets:` mode |
| `influxdb_in -> prometheus_out` | typed scalars, a Gauge per field, the measurement carrier, Ross's motivating example |
| `influxdb_in -> aggregate(cumulative) -> prometheus_out` | a delta sum that `prometheus_out` drops bare and carries cumulative |
| `prometheus_in -> influxdb_out` | histogram and summary into a wire with one number per field |
| `statsd_in -> otlp_out` | samples as a summary, set members skipped |

Sources: `statsd_in`, `influxdb_in`, `prometheus_in`. Sinks: `statsd_out`, `prometheus_out`,
`influxdb_out`, `otlp_out`. Later pairs follow the README's "Adding a pair" steps, one PR each.

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
- **W3**: the flow driver (§4) in `crates/logit-cli/tests/`, with the encoder-reach decision
  recorded, and a fixed-clock run of one flow as its own test.
- **W4**: `spec/transit/` (§1), `mapping.yaml` and its schema (§5), `script/transit
  gen|check|coverage`, the `--check` test, the features lists and cases for the three sources,
  the six flows (§7) and their expectations, and the AGENTS.md "Harnesses" entry and Environment
  table row.
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
