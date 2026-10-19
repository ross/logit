# Transit spec

This directory states what each sink does with each metric kind it receives, and checks that
statement against the code. A **case** is a batch of events as a source's listener produces them.
A **flow** is a `logit` config that runs cases through stages into a sink. The **mapping table**,
[`mapping.yaml`](mapping.yaml), gives every kind and feature a class per sink. A test runs every
case through every flow, reads the sink's bytes back through its pair's decoder, pins the result,
and fails when the result disagrees with the table.

The spec covers the codec layer: an encoder's bytes and the pair decoder's reading of them. It
doesn't cover sink behavior above the encoder (retries, request packing, stale-point drops) or what
a backend stores. [ADR `transit-spec`](../../docs/adr/transit-spec.md) records the decision and its
limits, and [the plan](../../docs/plans/transit-spec.md) the workstreams.

## Layout

| Path | What it holds |
|---|---|
| `mapping.yaml` | The table: the class vocabulary, the sinks, and one row per kind and feature |
| `features/<system>.yaml` | The wire features a source's cases can exercise, one `{name, text}` each |
| `cases/<system>/<case>.meta.yaml` | What the case exercises, and its wire file and tolerances if any |
| `cases/<system>/<case>.json` | The case's events, one `format: json` line each |
| `cases/<system>/<case>.<wire>` | The wire bytes a generated case decodes from, such as `.statsd` |
| `flows/<flow>.yaml` | A runnable config: `replay_in -> [stages] -> <sink>` |
| `expected/<flow>/<case>.json` | The run's document: the events read back and the counters moved (generated) |
| `coverage.md` | Each system's features and the cases that exercise them (generated) |

## Running it

The spec's generator is an ordinary test, so `script/test` and CI run it. Three wrappers drive it:

- `script/transit check` compares every generated file and runs the class check.
- `script/transit gen` rewrites every generated file, then runs the class check. Review
  `git diff spec/transit/` and commit the result.
- `script/transit coverage` rewrites `coverage.md` and prints it.

`gen` never rewrites over a class check failure. A failure means the code or the table is wrong,
and a person decides which.

## The classes

`mapping.yaml`'s `classes:` is the canonical vocabulary: `exact`, `normalized`, `degraded`, `lost`,
`skipped`, and `dropped`. They apply at two levels:

- **Record level**, for a metric kind, a log, and the `record` rows that decide a record's fate
  (`no_recorded_value`, `service_check`, `name_unsafe`): `exact`, `normalized`, `degraded`,
  `skipped`, or `dropped`, best to worst.
- **Field level**, for the `record` rows `unit`, `description`, `start_timestamp`, and
  `exemplars`, and every `attributes` row: `exact`, `normalized`, or `lost`.

## The mapping table

Each row is a `kind` and, for a kind that has them, a `feature`. `cells` maps a sink to what it
does. A cell has a `class`, a `text` sentence for an operator, a `reason` (required on a
`normalized` cell), and the `counter` series it counts under (required on a `skipped` or `dropped`
cell, and on a `lost` cell unless its reason is `uncounted`). When a sink's behavior depends on its
config, the sink's entry is a list: each cell but the last has a `when:` map of config fields, the
first that matches the flow's sink config applies, and the last applies otherwise:

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
back, then compares what it observed with the table. A record's expected class is the worst of
the record-level cells it reaches, and the observed class must equal it. A `normalized` cell also
accepts `exact`, because a normalization touches only the shapes it names. A field may read back
better than its cell, never worse. Every sink counter the run moved must be named by a cell the
case reached, and a loss cell observed at its class must see its counter move. Across the whole
run, every `normalized` or `lost` cell a record reached must be observed at that class at least
once.

`crates/logit-cli/src/transit/classes.rs`'s module doc has the rules in full: which rows a record
reaches, how records pair, and what each failure message names.

## Determinism

A wire case decodes at a fixed receipt time, 2027-01-15T08:00:00Z. Each run's clock is the case's
latest event timestamp plus one second, so a stage flushes once and a statsd line without `|T`
reads back at that instant. The flow driver's module doc, `crates/logit-cli/src/transit/mod.rs`,
lists what a flow may hold and what it rejects: a fan-out or fan-in, `lua`, a router or target,
`has_stale` and `drop_stale`, and a sink it has no encoder arm for.

Where a value can move between runs for a reason the code can't remove, a case's meta declares a
tolerance: `tolerance: [{path: /metrics/0/estimate, abs: 1}]` lets the number at that RFC 6901
pointer, in each event of the document, differ from the committed one by up to `abs`.

## Adding a case

1. Pick the source system's directory under `cases/`.
2. For a case that starts on the wire, write the wire file (`<case>.statsd`) and
   `<case>.meta.yaml` with `wire: <case>.statsd`. A `wire:` path may point at a capture under
   `testdata/interop/` instead of copying it. Run `script/transit gen` to write `<case>.json`.
3. For an edge case no producer sends, write `<case>.json` by hand and leave `wire:` out. The check
   requires it to read back to itself through `format: json`, and prints the form to use when it
   doesn't.
4. List the features it exercises in `exercises:`. Each must be in `features/<system>.yaml`.
5. Run `script/transit gen`, review the new expectations, and commit.

## Adding a flow

Write `flows/<flow>.yaml` with one `replay_in` whose `path:` names a case, such as
`../cases/statsd/mixed.json`. That case's directory is the flow's source, and every case in it runs
through the flow; naming a real case keeps the flow runnable by hand with `logit run`. Then run
`script/transit gen`. `script/validate` and `every_shipped_config_loads_and_validates` load every
flow.

## Adding a sink column

Add the sink to `sinks:`, give it a cell in every row, and add a flow that ends in it. The flow
driver must have an encoder arm for it (`crate::pipeline::sink_encoder`, and the pair decoder in
`crates/logit-cli/src/transit/wire.rs`). A row left without the sink's cell reads as not yet
specified.

## Adding a source

Add `features/<system>.yaml`, a `cases/<system>/` directory, and an arm for the system's decoder in
`decode_wire` (`crates/logit-cli/src/transit/spec.rs`) so wire cases can be generated. Flows that
read the new directory then pick up its cases.
