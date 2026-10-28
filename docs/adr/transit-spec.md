---
created: 2026-10-19
updated: 2026-10-28
---

# Transit spec: a mapping table under `spec/transit/` is the canonical statement of every cross-protocol transition, checked by flows and rendered into the docs

## Status
Accepted

## Context

[ADR `lossless-transit`](lossless-transit.md) decides two things about data crossing `logit`:
like-protocol transit is lossless, and cross-protocol transit is best-effort, with every
degradation counted and named in the receiving codec's module doc and in
`docs/known-gaps/mappings.md`. The first is checked by a fixed-point test per pair. The second is
checked by nothing: the module-doc tables and the known-gaps table are prose, written by hand per
codec, and no test fails when the code and either table disagree, or when the two tables
disagree with each other.

The question an operator asks is cross-protocol: "I send InfluxDB line protocol in and read
Prometheus exposition out; what does an integer field become, and what happens to a histogram?"
Answering it today means reading two codecs' module docs, the known-gaps table, and often the
code. Each of those is one copy of a list that the others restate, and the "one copy of a list"
rule in [AGENTS.md](../../AGENTS.md) can't be met while the canonical copy is prose in a module
doc that a second module doc has to summarize.

The test infrastructure that exists was built for other questions. `testdata/interop/` checks a
decoder against real producers. `testdata/differential/` checks a parser against a reference
implementation. The `*_fixed_point.rs` tests check a pair against itself. The interop harnesses
check that a series arrived at a backend by name. None of them runs `X_in`'s output through
`Y_out` and states what came out.

## Decision

**A mapping table is the spec.** `spec/transit/mapping.yaml` states, for every metric kind and
each feature that changes its fate (temporality, monotonicity, scalar type, record fields,
attribute shapes), the class of what each sink does with it: `exact`, `normalized`, `degraded`,
`skipped`, or `dropped`, with a reason and a sentence of operator-facing text. It is the
canonical copy of every transition's outcome. Rows for log and span fields join when a flow
carries them.

**The docs are generated from it.** Each codec's module-doc mapping table and
`docs/known-gaps/mappings.md`'s cross-protocol table are marked regions that `script/transit
docs` rewrites from the table, and a test fails when a region is stale, as `schema/logit.schema.json`
already works. A relay ADR's mapping table stays as written, with a note that it recorded the
decision at the time and the spec is current. Permitted-normalization lists stay prose in each
codec's module doc: they are per-codec policy, not per-cell outcomes.

**Flows check the table against the code.** A flow is a runnable `logit` config,
`replay_in -> [stages] -> Y_out`, run in-process by a test driver with a fixed clock: the case's
events through each stage's real `Transform`, through the sink's real encoder, and back through
the pair's real decoder. The re-decoded batch and the counters the run incremented are pinned as
generated, committed expectations, and the generator checks every case's observed class against
its cell. A disagreement fails the run as a finding; it is never regenerated over.

**Cases are the source system's output, authored as lossless event JSON.** A case under
`spec/transit/cases/<system>/` is one event batch as `<system>_in` produces it. Where a case
starts on the wire, the wire bytes sit beside it and the generator decodes them once; where it's
an edge case with no producer, the JSON is written by hand. Each case names the wire features it
exercises from a per-system list, so coverage is a listing rather than a target.

**The work stays at the codec layer.** A flow ends at an encoder's bytes and the pair decoder's
reading of them. Sink behavior above the encoder (stale-point drops, tag-count limits, request
packing, retries, routing by carried bucket or org) and a destination's own storage semantics
belong to each `_out`'s unit tests and the interop harnesses. `aggregate` is plumbing a flow uses
to connect a raw kind to a sink that can't carry it, with
[ADR `aggregation-window-semantics`](aggregation-window-semantics.md) the authority on what it
does; the table has no stage columns.

**What's built for this ships as features.** The lossless event form is an additive extension of
`stdio_out`/`file_out` `format: json` ([ADR `stream-json-format`](stream-json-format.md)), so an
archive `file_out` writes round-trips. `replay_in` reads that form, or a `format: native` file,
once and ends, and is the replay half of `file_out` as well as the spec's source.

## Alternatives considered

- **Author cases as native frames.** They're lossless and feed a pipeline directly, but a
  binary file can't be hand-written for an edge case, can't be read in a diff, and every model
  reshape invalidates it. The JSON is the authoring form; a native file is derived when a flow
  wants one.
- **Extend `generate_in` to every kind.** It exists for load, produces three kinds and `F64`
  only, and a template is a worse way to state an edge case than a literal event. Rejected in
  favor of a reader.
- **A second, test-only JSON form.** Two grammars to keep mirrored, and the six Python readers
  of the existing form would read neither. The existing form is extended additively instead.
- **Expectations as encoded bytes.** Deterministic and simple to diff, but they state spelling,
  not value, and a binary wire's bytes can't be read. The re-decoded model states "the value
  survives as a consumer sees it" in one form for every sink. Bytes are kept only where a flow
  asks.
- **Verify against real backends.** The only layer that catches a sink and its pair's decoder
  sharing a wrong reading, and it needs a running service per sink. Deferred with that risk
  accepted: the interop captures and the differential corpora guard the decoders, and a finding
  gets fixed when it surfaces. A backend layer can be added over the same corpus later.
- **Keep the module docs canonical and generate the spec from them.** A doc comment is not a
  machine-readable table, and a test that parses prose is brittle. The table is the data; the
  docs are a rendering.
- **Leave the mapping tables as hand-maintained prose.** The status quo, in which nothing fails
  when a codec changes and its table doesn't, and a cross-protocol outcome is derived by reading.
  Rejected as the thing this record exists to replace.

## Consequences

- `spec/transit/` is a new top-level directory, with a README, `mapping.yaml`, feature lists,
  cases, flows, and generated expectations. Flows join `script/validate` and
  `every_shipped_config_loads_and_validates` as shipped configs.
- `format: json` gains a type tag on scalars the JSON type doesn't carry, a distribution's bins,
  a set's registers, and a Rust reader; nothing is removed. The Python readers that mirror its
  grammar are checked against the new output.
- `replay_in` is a new `ComponentKind` with its own ADR, a finite input like `generate_in` under
  `count`.
- Each codec's module-doc mapping table and `docs/known-gaps/mappings.md`'s cross-protocol table
  become generated regions. Editing one by hand fails a test; the edit goes in `mapping.yaml`.
- Adding a sink column, or a pair, is one PR: the column's cells, a flow, and the generated
  expectations. Coverage grows one pair at a time, and the feature listing shows what no case
  exercises.
- The plan is [`docs/plans/transit-spec.md`](../plans/transit-spec.md).

## Amendment: the driver reaches the encoder, not the sink (2026-10-19)

The flow driver builds a flow's transforms through `build_spec`, but not its sink: it takes the
sink kind's encoder from the registry's `sink_encoder`, which configures it through the same
config mapping `build_spec` uses, and calls the public functions the sink itself calls for any
step between the encoder and the wire. Giving every `Output` an in-memory transport was the
alternative; it puts test plumbing into each sink for a layer the spec excludes (sink behavior
above the encoder). The driver lives in a `#[cfg(test)]` module of the `logit` binary,
`crates/logit-cli/src/transit/`, since only the binary's own tests reach the registry. W4's
generator will be a `#[test]` in the same module, rewriting the expectations under
`TRANSIT_UPDATE=1` and comparing them otherwise: a test reaches `build_spec` where a script or a
separate binary can't, and the `--check` mode is the same test with the variable unset.

## Amendment: the table's field level and the class check (2026-10-19)

Filling the first column, `statsd_out`, settled four things about the table the decision above
left open:

- **A sixth class, `lost`, at a second level.** A record's class (`exact`, `normalized`,
  `degraded`, `skipped`, `dropped`) can't say that a gauge reached the wire but its `unit` didn't.
  Rows for a record's own fields and for attribute shapes are field-level, and their class is
  `exact`, `normalized`, or `lost`, the last naming its counter or saying it's `uncounted` (now
  `structural`; see the 2026-10-21 note in "Amendment: uncounted losses and cells a sink can't
  reach").
- **A cell can depend on the sink's config.** `statsd_out`'s `format:`, `multi_value:`, and
  `relative_gauges:` each change a row's outcome, so a sink's entry in a row may be a list of
  cells, each but the last guarded by a `when:` map of config fields. A column per sink
  configuration was the alternative; it multiplies columns by every option a sink has.
- **A cell names its counter series**, not only a reason, and the check requires every loss
  counter a run moved to be named by a cell the case reached. A counter no cell names is a
  finding, as an unexplained record is.
- **A bound passes an improvement once, not forever.** A `normalized` cell also accepts a record
  that reads back `exact`, since a normalization rewrites only the shapes it names, and a field
  may read back better than its cell. Across every flow, each such cell a record reached must
  be observed at its own class at least once, so a fix that removes the normalization or the loss
  fails the check until the table follows.

`crates/logit-cli/src/transit/mapping.rs`'s and `classes.rs`'s module docs are the schema and the
check's rules.

## Amendment: uncounted losses and cells a sink can't reach (2026-10-19)

The cross-protocol columns settled two more things:

- **A `skipped` or `dropped` cell may say its loss is `uncounted`**, as a `lost` cell already
  could. `prometheus_out` and `influxdb_out` skip a log or span without counting it, and
  `prometheus_out` replaces an earlier point of a series without counting it. The table records a
  sink as it behaves, so an uncounted loss needs a cell that says so; refusing the cell would hide
  the loss instead. The check accepts such a cell's class for a record nothing reads back. Each
  is a finding for the sink, not an endorsement. [ADR
  `telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md)'s amendment "structural loss has
  no counter" (2026-10-21) narrows this: a `lost` or `degraded` cell names its counter or is
  `structural`, a `skipped` or `dropped` one names its counter, and `uncounted` stays only on the
  rows `crates/logit-cli/src/transit/mapping.rs`'s `UNCOUNTED_ALLOWED` names.
- **A row may omit a sink's cell when no record is compared against it there**, and
  `mapping.yaml` says why beside the row: `name_unsafe` at a sink that rewrites no name, or a
  field row at a sink that writes the kind only in a lesser form, so the field is never compared.
  A field row is reached only where its record is compared. A cell nothing is compared against
  would sit in `coverage.md`'s unreached list forever.

## Amendment: the generated regions (2026-10-19)

Rendering the table into the docs settled what a region is and how much of a doc it replaces:

- **Two forms per sink.** A `doc:<sink>` region in the sink's codec or sink module doc holds the
  whole column, `exact` cells included, since a reader there wants the whole mapping. A
  `gaps:<sink>` region in `docs/known-gaps/mappings.md` holds only the `degraded`, `lost`,
  `skipped`, and `dropped` cells, in that file's row format. A region is an HTML comment pair,
  `<!-- transit:doc:<sink> -->` to `<!-- transit:end -->`, so one marker works in a `//!` module
  doc and in Markdown, and renders as nothing in either.
- **The renderer reads the table only.** It doesn't look at the code; the class check ties the
  table to the code. A cell's `text` is rendered verbatim, so a text that reads wrong in a doc is
  fixed in `mapping.yaml`. A `when:` cell is its own row, its condition rendered from the `when:`
  map.
- **A hand-written row goes when the region states its mapping and its counter.** A row of a
  codec's encode table, or of `mappings.md`, that also states an outcome the table doesn't (a
  drop with no row of its own, a decode-side fact) stays, verbatim, beside the region; a
  diagnostic key a removed row named moves into the cell's text. The reasoning behind a mapping,
  why a wire has no form for something or why a destination refuses it, isn't an outcome: it
  stays in the sink's or codec's module doc prose, and a region doesn't repeat it. Moving the
  remaining outcomes into the table is a row or a text at a time, not a rewrite of the prose.
- **Every sink column has both regions.** The test fails for a sink in `sinks:` with no region
  of either form, so adding a column adds its docs in the same PR.

`crates/logit-cli/src/transit/docs.rs`'s module doc has the rules, and
[`spec/transit/README.md`](../../spec/transit/README.md)'s "Generated docs" the reader's account.

## Amendment: a test per flow, and committed ledger files (2026-10-19)

The generator was one `#[test]` running every case through every flow, so nextest couldn't spread
it across workers, and its time grew with cases times flows: 2.3 s at 8 flows and 5.0 s at 16 on
a slow CI runner, past nextest's `slow-timeout`. It's now a test per flow, which spreads the work
and names the failing flow, with two more tests beside them:

- **`transit::spec::flow::<flow>`** runs one flow's cases, the class check, and the comparison of
  `expected/<flow>.json`. A `macro_rules!` list names the flows; `transit::spec::cases` fails when
  the list and `flows/*.yaml` differ, so a new flow can't go unrun. A build script that generated
  the list was the alternative; it adds a build step for a list that changes once per flow.
- **The checks across flows read committed files.** "Each `normalized` or `lost` cell observed at
  its own class at least once" and `coverage.md`'s unreached cells need every flow's
  observations, and nextest runs each test in its own process in no order. Each flow test
  compares what its runs fed the check with `ledger/<flow>.json`, generated and committed beside
  its expectations, and `transit::spec::ledger` merges those files and runs the checks with no
  flow run. A file holds the observations, not the cells' classes, which `ledger` takes from the
  current `mapping.yaml`. Since a flow test fails when its file is stale, every test passing means
  the merged files equal what one run of every flow would have built. Recomputing every flow
  inside `ledger` was the alternative; it would restore the single slow test.
- **`gen` runs in three phases.** Under `TRANSIT_UPDATE=1` a flow reads the case files `cases`
  rewrites, and `ledger` reads the files the flows rewrite, so `script/transit gen` runs `cases`,
  then every flow, then `ledger` and the docs test, each as its own nextest run, running every
  phase and every test even when one fails so one run reports every failure. `check` compares
  only, so it runs everything at once.

## Amendment: log records (2026-10-28)

The table classifies metric records only. The class check compares a log as one body, its
`same_value` message equality, so any other change to a log reads `normalized` with no row
naming it, and four of the six columns have only `skipped` or `lost, uncounted` log cells. The
four pairs that carry logs, `syslog`, OTLP logs, Datadog logs, and `splunk_hec`, each lack a
column, a source, or both. The `tlog` stream fills that in, and settles these:

1. **Log fields are rows under `kind: log`** in the one table, with the cell schema, class check,
   and renderer metrics use. Record-level rows (`plain`, `dogstatsd_event`, `message_unsafe`,
   `structured_message`, `bytes_message`, `blank_message`) take the five record classes, and
   field rows take `exact`, `normalized`, or `lost`. A field the input doesn't set has its own
   `_unset` row, since a row is chosen from the input before pairing, and a sink that stamps a
   value where the input had none makes a different statement from one that changes a value.
2. **A log's identity is its message, respelled per sink.** `Spelling::written` for a log is the
   message as the sink's encoder renders it and the pair's decoder reads it back, so a rewritten
   message pairs and reads `normalized` through `log/message_unsafe` or
   `log/structured_message`, as `record/name_unsafe` works for a metric. Once a log pairs,
   `same_value` is `true` for it, and the field rows carry the rest.
3. **Log timestamps get rows**, unlike a metric's: `log/timestamp` for the precision a sink
   writes, and `log/receipt_timestamp` for the receipt time of an event that carries
   `syslog.timestamp`, which `syslog_out` has no slot for.
4. **Severity resolution loss belongs to the carrier.** The six-variant `Severity` reads back
   equal at band resolution, so `attributes/severity_carrier` takes `_logit.otel.severity_*` and
   `_logit.syslog.severity` ahead of the private and foreign carrier rows: `exact` at the
   family's own sink, `lost, structural` elsewhere.
5. **Attribute rows keyed by name** where the log sinks treat keys differently: `syslog_header`,
   `syslog_msgid`, `syslog_sd`, `syslog_timestamp`, `host_name`, and `splunk_envelope`. Other
   log attributes reach the value-type rows.
6. **Derived attributes are per sink.** `Spelling::derived` lists what each sink writes from a
   record's own fields, and the field rows take the union: `syslog_out`'s, `splunk_hec_out`'s,
   and `datadog_out`'s. `no_host` reaches every sink that fills a host.
7. **Three log columns join `otlp_out` and `datadog_out`**: `syslog_out`, `splunk_hec_out`, and
   `kafka_out` under `format: json` and `format: text`. With no `kafka_in`, the driver reads
   `kafka_out` back through `EventJsonDecoder`, or as one `Raw` log per record under
   `format: text`.
8. **`syslog_out`'s drop of unopted attributes** is `lost, structural` under RFC 3164, where the
   wire has no slot, and `lost, uncounted` under RFC 5424 without `structured_data:`, where the
   slot exists and the loss is real. The missing counter is a sink finding.
9. **`UNCOUNTED_ALLOWED` narrows** for logs to `log/beside_metrics` and `syslog_out`'s
   value-type rows. Every other uncounted loss of a log field is a format with no slot and reads
   `lost, structural`. The `span` entry stays until spans are classified.
10. **Log cases live in log-only directories** (`cases/otlp-logs/`, `cases/datadog-logs/`,
    `cases/syslog/`, `cases/splunk/`), so a log flow never routes a metric case into a sink with
    no metric cells. `decode_wire` picks a decoder by the directory name's prefix before `-`.
11. **`otlp_out`'s observed-time stamp moves to its own row.** Its `log/plain`,
    `log/dogstatsd_event`, and `log/beside_metrics` cells become `exact`, and
    `log/observed_timestamp_unset` carries the stamp, since the cross-flow bound would otherwise
    fail those cells for never reading `normalized`.
12. **The sort key doesn't change.** `first_name` stays `""` for a log, and the canonical line
    already orders logs that share a timestamp.

Consequences: `KINDS`' `log` entry and the class check grow a log path, and the driver gains
`syslog_out`, `splunk_hec_out`, and `kafka_out` encoder arms and `syslog` and `splunk` source
arms. Sink behavior the new cells record, an uncounted loss included, is recorded as the code
behaves and fixed in a later stream. Lines parsed from text (`lines_in`, `tail_in`, `docker_in`),
`kafka_out`'s OTLP and MessagePack formats, and spans stay out.
[`docs/plans/transit-spec.md`](../plans/transit-spec.md)'s "9. Logs" has the feature table, the
framework's changes, and the workstreams, and its "Findings" the sink behaviors already known.
