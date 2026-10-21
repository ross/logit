# Datadog sketch differential corpus

Delta OTLP histograms, each beside the Agent sketch Datadog's OpenTelemetry translator builds from
it, and histograms and summaries beside the series it writes for them, for
`crates/logit-core/src/sketch/from_otlp.rs` and `datadog_out`'s encoder. Three tests read it, with
no Go installed:

- `sketch::from_otlp::tests` in `logit-core` converts every case and checks the result bin for
  bin: each key's count, and the summary's `cnt`, `sum`, `min`, `max`, and `avg`, bit for bit.
- `crates/logit-proto/tests/datadog_otlp_sketch_differential.rs` sends every case through the
  sketches route and checks the `Dogsketch`, its `k`/`n` order included, or the counter a case
  with no sketch moves.
- `crates/logit-proto/tests/datadog_otlp_series_differential.rs` sends every `series/` case
  through the v2 series route and checks each series' name, tags, type, and value, and whether it
  has a unit.

To regenerate the corpus, run `script/differential datadog-sketch`; to check that the committed one
is current, run `script/differential datadog-sketch --check`. See [`../README.md`](../README.md).

## Provenance

| Input | Pinned at |
|---|---|
| the translator | `github.com/DataDog/datadog-agent/pkg/opentelemetry-mapping-go/otlp/metrics`, with `pkg/util/quantile` and every other datadog-agent module, at commit `45132452b8555c0b0df112e3bd48e39284873e03` (pseudo-version `v0.84.0-devel.0.20261020233841-45132452b855`), through `replace` directives in `tools/differential/datadog-sketch/go.mod` |
| `sketches-go` | `github.com/DataDog/sketches-go v1.4.8`, the version that commit requires |
| the Go toolchain | `golang:1.27.2-bookworm@sha256:5cf287a799e6b94384bad13d16b14904c531f51ba65792237e122ce42b392f61` (Go 1.27.2), run with `GOTOOLCHAIN=local`, `GOFLAGS=-mod=readonly`, and `GOAMD64=v1` |
| the CPU | linux/amd64 with AVX and FMA; the generator refuses any other |

The commit is newer than the Collector's `datadogexporter` pin (`a8c2777f7050`, 2026-09-30, with
`pkg/util/quantile v0.83.0`), which predates the observation cap, the `(0, B]` point mass, and the
exponential histogram's `min`/`max` clamps that `from_otlp` reproduces. Without the `replace`
directives, version selection picks the `v0.84.4` tag (commit `d8e8868f`), which also lacks them.
`sum.golang.org` had no entry for the pseudo-version when it was pinned, so the download step skips
the checksum database for datadog-agent modules; `go.sum` holds their hashes and the build step,
with no network, verifies against it.

The CPU matters because Go's `math.Exp` is assembly on amd64 with a fused multiply-add path taken
only when the CPU has AVX and FMA, and `math.Log` is assembly there too. A Collector on arm64 runs
other code and can disagree with this corpus in the last place, which moves counts.
`crates/logit-core/src/sketch/gomath.rs` ports the amd64 functions.

`tools/differential/datadog-sketch/main.go` puts each point through `metrics.NewDefaultTranslator`
with no options, which is `histograms.mode: distributions`, and reads the sketch back from its
`ConsumeSketch` call, so the cases run the code the exporter runs rather than a copy of it. Each
point runs through a fresh translator. The generator fails, writing nothing, on a CPU other than
amd64 with AVX and FMA, on a duplicate case name, on more than one sketch from one point, on an
error `MapMetrics` returns that it can't classify, and when the build info lacks a pinned module.

## Files

- **`cases/<kind>-<name>.json`**: one case. `kind` is `explicit` (a `HistogramDataPoint`) or
  `exponential` (an `ExponentialHistogramDataPoint`); `input` is the point, every float a string
  in Go's shortest round-tripping form (`+Inf`, `-Inf`, and `NaN` included). `outcome` is `sketch`,
  with `sketch` holding the translator's `Cols()` (`k` and `n`, a key repeated where a count passes
  65535) and its summary, or the reason there's none: `empty`, `too_many_observations`,
  `non_monotonic_bounds`, or `unconvertible`, with the error or log line in `detail`. An explicit
  point's `count` is the sum of its `counts`, since `logit`'s `Histogram` carries no count. A
  `null` sum is given to the translator as `NaN`: `logit` reads an absent sum as the translator
  reads a non-finite one, where the translator itself would read protobuf's default `0`
  ([ADR `datadog-otlp-metric-mapping`](../../../docs/adr/datadog-otlp-metric-mapping.md)'s
  "Histograms").
- **`series/<kind>-<name>.json`**: one case put through a translator with
  `histograms.mode: counters`, the aggregation metrics, quantiles, and units on: the explicit and
  hand-built exponential cases, and summaries (`kind: summary`, OTLP's cumulative kind, with no
  start time, so the translator writes only their `.quantile` gauges). `series` lists every series
  it wrote, in order, with its unit as the translator maps it (`ms` as `millisecond`).
- **`primitives.json`**: Go's `Log`, `Exp`, and `Pow` on the inputs the conversion uses them on,
  and `Log1p(2/128)`, the Agent's `gamma.ln`. The test checks `gomath.rs` against each, and
  `Mapping::agent`'s own `gamma_ln` against the last.
- **`manifest.json`**: the Go version, the CPU, and each module's version, from the build info.

Cases named `seeded-*` come from a fixed-seed generator in `main.go`; the rest are hand-built for
one shape each: no buckets, a single bucket, open-ended first and last buckets, `(0, B]` and
`(A, 0]` buckets, negative bounds, counts above 65535, the observation cap and one past it, a bucket
range wide enough that `trimLeft` folds bins, `NaN` and infinite explicit bounds, non-finite sums, `min` and `max` set and unset, and,
for exponential histograms, every scale from -10 to 20, negative buckets, a zero bucket, and
values past the Agent's highest key.

## Behavior the corpus records

- A point with one bucket and no bounds, `(-Inf, +Inf]`, keys every observation at `-1`, and its
  `min` and `max` are about `-1e-9`. Both bounds become `-Inf`, and Go's `int(+Inf)` is
  `math.MinInt64` on amd64, which keys clamp to 1.
- At scales -10 to -8, every case is `unconvertible`, because the base or a bucket boundary
  overflows a float64. At scale -7, a case is `unconvertible` once its buckets reach 2^1024.
