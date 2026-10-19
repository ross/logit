---
created: 2026-10-19
updated: 2026-10-19
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
  `exact`, `normalized`, or `lost`, the last naming its counter or saying it's `uncounted`.
- **A cell can depend on the sink's config.** `statsd_out`'s `format:`, `multi_value:`, and
  `relative_gauges:` each change a row's outcome, so a sink's entry in a row may be a list of
  cells, each but the last guarded by a `when:` map of config fields. A column per sink
  configuration was the alternative; it multiplies columns by every option a sink has.
- **A cell names its counter series**, not only a reason, and the check requires every loss
  counter a run moved to be named by a cell the case reached. A counter no cell names is a
  finding, as an unexplained record is.
- **A bound passes an improvement once, not forever.** A `normalized` cell also accepts a record
  that reads back `exact`, since a normalization rewrites only the shapes it names, and a field
  may read back better than its cell. Across the whole run, each such cell a record reached must
  be observed at its own class at least once, so a fix that removes the normalization or the loss
  fails the check until the table follows.

`crates/logit-cli/src/transit/mapping.rs`'s and `classes.rs`'s module docs are the schema and the
check's rules.
