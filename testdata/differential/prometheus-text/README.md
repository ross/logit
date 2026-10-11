# Prometheus text differential corpus

Exposition bodies, each beside the reading Prometheus's own parser gave it, for the text 0.0.4 and
OpenMetrics 1.0 decoder in `crates/logit-proto/src/prometheus/text.rs` and its assembler.
`crates/logit-proto/tests/prometheus_text_differential.rs` runs the decoder over every body and
checks it against the reading, with no Go installed; its module doc lists the checks and every
normalization the comparison applies.

To regenerate the readings, run `script/differential prom-text`; to check the committed ones are
current, run `script/differential prom-text --check`. See [`../README.md`](../README.md).

## Two sources

- **`cases/`**: hand-built bodies, each a shape the formats or Prometheus's parser treat in a
  particular way. They're inputs, committed by hand; the generator reads them and doesn't write
  them.
- **`../../interop/prometheus-scrape/`**: scrape bodies recorded from real exporters by
  `script/record-fixtures prometheus-scrape`, which stay where they are. Its
  [README](../../interop/prometheus-scrape/README.md) has their provenance.

`reference/cases/<stem>.json` and `reference/prometheus-scrape/<stem>.json` hold the readings.

## Provenance

| Input | Pinned at |
|---|---|
| Prometheus's parser | the Go module `github.com/prometheus/prometheus v0.314.0` (Prometheus 3.14.0, the release whose `prompb` is vendored under `crates/logit-proto/proto/prometheus/` and that recorded `../../interop/prometheus/`), commit `d7598b7141418fa35be2b5ec5d0fefb634199610`, through `tools/differential/prom-text/go.mod` and `go.sum` |
| the Go toolchain | `golang:1.27.2-bookworm@sha256:5cf287a799e6b94384bad13d16b14904c531f51ba65792237e122ce42b392f61` (Go 1.27.2), run with `GOTOOLCHAIN=local` and `GOFLAGS=-mod=readonly` |

`tools/differential/prom-text/main.go` calls `textparse.New(body, contentType,
labels.NewSymbolTable(), textparse.ParserOptions{})`, the scrape loop's own call with every option
at the scrape loop's default: no fallback protocol, `_created` series kept as series, no
type-and-unit labels, and no conversion of classic histograms. `StartTimestamp` isn't called, since it reads ahead and changes
whether later `_created` lines are skipped. Where `New` returns no parser, which fails a scrape,
the reading records the error and goes on with the parser
`fallback_scrape_protocol: PrometheusText0.0.4` selects.

`script/differential prom-text` prints `go version` and the module version on every run. Its first
container downloads the modules `go.sum` names into the docker volume `logit-differential-go`, the
only step with a network; the second builds and runs the program with no network and the module
proxy off. Downloaded modules never land under the repository. To start from an empty cache, run
`sudo docker volume rm logit-differential-go`.

## Files

A case is three files:

- `<stem>.txt` (a text 0.0.4 body) or `<stem>.om` (an OpenMetrics one), byte for byte;
- `<stem>.headers`, the response's `content-type: <value>` line, or empty for a missing header;
- `<stem>.expect.json`, what `logit` must make of it: `result` (`ok` or `malformed`), `skips` and
  `degraded` (each `logit.input.metrics.skipped`/`degraded` reason and its count, from the parse
  and the model mapping), and, where `logit` reads a series Prometheus read differently, `divergent`
  (each such series, `[name, [[label, value], ...]]`, in Prometheus's order) and `divergence` (why,
  ending with the `docs/known-gaps/mappings.md` row it names).

A name ends `-text` or `-om` when one shape is in both dialects. `cli-*` cases are copies of
`crates/logit-cli/tests/fixtures/prometheus/*.in`, and `metadata-target` is a copy of
`tools/record-fixtures/prometheus-metadata-target.prom`. `doc-exposition-formats` is the example
in Prometheus's exposition format documentation (its `-no-weird` twin drops the one line Prometheus
rejects), and `doc-openmetrics` the OpenMetrics specification's. The rest cover:

- histograms: a missing `+Inf` bucket, alone and beside a `_count` above the highest bucket,
  buckets out of order, repeated, decreasing, spelled `1`/`1.0`/`1e1`, a `_count` that disagrees,
  counts that are `NaN`, negative, or fractional;
- summaries: quantiles outside `[0, 1]`, one that isn't a number;
- values: `NaN`, the infinities, `-0`, the largest and the smallest subnormal `f64`, one past the
  range, hex floats, `_`, signed `NaN`, negative counters;
- timestamps: negative, decimal, exponent forms, past 2262, `NaN`, past `i64`;
- `# EOF`: missing, followed by content, twice, without a newline, followed by a blank line, and
  in text 0.0.4;
- label values: every escape at a value's end, a lone trailing `\`, an undefined escape, a raw
  newline, no comma between pairs, a trailing comma, a repeated name, an empty value, a NUL, bytes
  that aren't UTF-8;
- Prometheus 3's quoted UTF-8 metric, label, and `# TYPE` names;
- metadata: conflicting and repeated `# TYPE`/`# HELP`, `# HELP` escapes, `# TYPE` after samples,
  non-contiguous families, an unknown type keyword, an empty `# HELP`, `untyped`, tabs, `# UNIT` in
  text 0.0.4 and one that isn't the name's suffix, OpenMetrics-only types in text 0.0.4;
- OpenMetrics shapes: `info`, `stateset`, `gaugehistogram`, exemplars on every kind of line,
  `_created`, a counter with no `_total`, and `_created` in text 0.0.4;
- framing: CRLF, no trailing newline, blank lines, leading spaces, comments;
- `Content-Type`: missing, garbage, another media type, `version=0.0.1`, bare media types, an
  `escaping` parameter, an uppercase media type, a trailing `;`, a longer media type with the
  OpenMetrics one as its prefix, and a malformed parameter.

Each reading is `json.Encoder` output with sorted keys, one space of indent, no HTML escaping, and
a trailing newline:

- `input`: `path` (from `testdata/`), `len`, and `crc32c` (`0x` and 8 hex digits, Castagnoli) of
  the body read, which the test recomputes;
- `content_type`: the sidecar's value, or `null`;
- `parser`: `prom` or `openmetrics`, or `{"error": ..., "fallback": ...}` where `New` returned no
  parser;
- `entries`: every entry `Next` returned, in order: `{kind: "type"|"help"|"unit", name, value}`, or
  `{kind: "series", name, labels, value, ts_ms?, exemplar?}`, where `labels` is every label but
  `__name__`, sorted, `value` is `0x` and the 16 hex digits of the `float64`'s bits, and `exemplar`
  is `{labels, value, ts_ms?}`;
- `error`: `null`, or `{after_entry, message}`, the first error `Next` returned and how many
  entries came before it. Prometheus stops there and fails the scrape.

## What the generator checks

Generation fails, writing nothing, when a case has no `.headers` or `.expect.json`, a file in
either directory belongs to no body, a sample carries a second exemplar the reading has no field
for, or a recorded body doesn't parse to its end.

## Size

About 220 KB: 40 KB of cases and 180 KB of readings, 49 KB of them the node_exporter body's.
