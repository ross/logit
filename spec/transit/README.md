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

The sources are `statsd_in`, `influxdb_in`, and `prometheus_in`'s scrape (remote-write cases
aren't written yet). The sink columns are `statsd_out`, `prometheus_out` (exposition and
remote-write), `influxdb_out`, and `otlp_out`. Each flow's name says which source meets which
sink: `statsd-relay` is the like pair, and `influxdb-to-prometheus` a cross-protocol one.

## Layout

Expectations are one file per flow, not one per case: a file per case grows as flows times cases,
which reaches thousands once every source meets every sink column, while a file per flow grows
with the flows alone. Each case's document sits under its own key on its own lines, so a diff
still shows which case changed. A missing file, a case with no key, a stale document, and a key
no case has each fail, naming the flow and the case.

| Path | What it holds |
|---|---|
| `mapping.yaml` | The table: the class vocabulary, the sinks, and one row per kind and feature |
| `features/<system>.yaml` | The wire features a source's cases can exercise, one `{name, text}` each |
| `cases/<system>/<case>.meta.yaml` | What the case exercises, and its wire file and tolerances if any |
| `cases/<system>/<case>.json` | The case's events, one `format: json` line each |
| `cases/<system>/<case>.<wire>` | The wire bytes a generated case decodes from: `.statsd` lines, InfluxDB line protocol in `.lp`, or a Prometheus scrape body in `.prom` |
| `cases/<system>/<case>.headers` | The request beside an HTTP wire file, one `name: value` per line as `testdata/interop/` records it: `path:` with an InfluxDB write's `org`, `orgID`, `bucket`, and `precision`, or a scrape's `content-type:` choosing its dialect |
| `flows/<flow>.yaml` | A runnable config: `replay_in -> [stages] -> <sink>` |
| `expected/<flow>.json` | Each case's run through the flow, keyed by case name: the events read back and the counters moved (generated) |
| `coverage.md` | Each system's features and the cases that exercise them, and each sink's cells no flow reaches (generated) |

## Running it

The spec's generator is an ordinary test, so `script/test` and CI run it. Three wrappers drive it:

- `script/transit check` compares every generated file and runs the class check.
- `script/transit gen` rewrites every generated file, then runs the class check. Review
  `git diff spec/transit/` and commit the result.
- `script/transit coverage` does what `gen` does, then prints `coverage.md`.

`gen` writes a flow's expectations only after every case in the flow has run, and only when none
of them failed the class check; one class failure leaves the flow's whole file untouched. A
failure means the code or the table is wrong, and a person decides which. A cell `coverage.md` lists as reached by no flow
is one nothing checks: add a case or a flow that reaches it.

## The classes

`mapping.yaml`'s `classes:` is the canonical vocabulary: `exact`, `normalized`, `degraded`, `lost`,
`skipped`, and `dropped`. They apply at two levels:

- **Record level**, for a metric kind, a log or span on an event with no metrics, and the
  `record` rows that decide a record's fate (`no_recorded_value`, `service_check`,
  `name_unsafe`, `non_finite`, `superseded`): `exact`, `normalized`, `degraded`, `skipped`, or
  `dropped`, best to worst.
- **Field level**, for the `record` rows `unit`, `description`, `start_timestamp`, and
  `exemplars`, every `attributes` row, and a log or span beside metrics (`beside_metrics`), which
  is a field of its event: `exact`, `normalized`, or `lost`.

## The mapping table

Each row is a `kind` and, for a kind that has them, a `feature`. `cells` maps a sink to what it
does. A cell has a `class`, a `text` sentence for an operator, a `reason` (required on a
`normalized` cell), and the `counter` series it counts under (required on a `skipped`, `dropped`,
or `lost` cell unless its reason is `uncounted`). An `uncounted` loss is the sink's behavior as it
stands, recorded so the table says so, not an endorsement. When a sink's behavior depends on its
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
back, reading names, attribute keys, and values as that sink writes them (`prometheus_out`'s
sanitized names and `_total`, `influxdb_out`'s `<name>.value` field, `otlp_out`'s integer
timestamps), then compares what it observed with the table. A record's expected class is the worst of
the record-level cells it reaches, and the observed class must equal it. A `normalized` cell also
accepts `exact`, because a normalization touches only the shapes it names. A field may read back
better than its cell, never worse. Every sink counter the run moved must be named by a cell the
case reached, and a loss cell observed at its class must see its counter move. Across the whole
run, every `normalized` or `lost` cell a record reached must be observed at that class at least
once.

`crates/logit-cli/src/transit/classes.rs`'s module doc has the rules in full: which rows a record
reaches, how records pair, and what each failure message names.

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

## Adding a case

1. Pick the source system's directory under `cases/`.
2. For a case that starts on the wire, write the wire file (`<case>.statsd`, `<case>.lp`, or
   `<case>.prom`) and `<case>.meta.yaml` with `wire: <case>.statsd`. An InfluxDB case needs a
   `<case>.headers` beside its wire file whose `path:` names a `bucket` and an `org` or `orgID`,
   as `influxdb_in` requires; a Prometheus case reads text 0.0.4 unless its `.headers` gives an
   OpenMetrics `content-type:`. A `wire:` path may point at a capture under `testdata/interop/`
   instead of copying it, and its `.headers` comes with it. Run `script/transit gen` to write
   `<case>.json`.
3. For an edge case no producer sends, write `<case>.json` by hand and leave `wire:` out. The check
   requires it to read back to itself through `format: json`, and prints the form to use when it
   doesn't.
4. List the features it exercises in `exercises:`. Each must be in `features/<system>.yaml`.
5. Run `script/transit gen`, review the new expectations, and commit.

## Adding a flow

Write `flows/<flow>.yaml` with one `replay_in` whose `path:` names a case, such as
`../cases/statsd/mixed.json`. That case's directory is the flow's source, and every case in it runs
through the flow; naming a real case keeps the flow runnable by hand with `logit run`. Then run
`script/transit gen`, which writes `expected/<flow>.json`. `script/validate` and `every_shipped_config_loads_and_validates` load every
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
