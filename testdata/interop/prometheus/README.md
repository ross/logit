# Prometheus remote-write interop fixtures

Real HTTP request bodies that a real Prometheus and a real vmagent `POST`ed, captured verbatim by
`tools/record-fixtures/raw_capture.py --proto http`, with **no parsing, no decompression, and no
re-encoding.** Each `.bin` is exactly the compressed protobuf the sender put on the wire: Snappy,
or zstd for vmagent's default wire.
`logit`'s own encoder never touches these, which is the point:
`crates/logit-proto/src/prometheus/remote_write.rs` was written from the two remote-write specs and
the vendored `prompb`, and these fixtures check that reading against the sender every deployment
runs.

To regenerate, run `script/record-fixtures prometheus vmagent`. See `../README.md` and the header
comment in `script/record-fixtures`.

## Fixtures

**Each request has two files.** The `.bin` is the body. The `.headers` sidecar beside it holds the
request's method, its path, and every request header with the name lowercased, one per line. The
sidecar is part of the fixture, not a convenience: remote-write's framing lives in its headers, so
the wire version (`Content-Type`, `X-Prometheus-Remote-Write-Version`) and the compression
(`Content-Encoding: snappy`) are only recoverable from it. For the same reason, this corpus is
captured through an HTTP sink that **answers** `204`, not through the read-only `--proto tcp` mode
the `*.raw` corpora use: a real client won't send a second request to a listener that never replied
to the first.

The seven `prometheus-*` requests came from one `script/record-fixtures prometheus` run, in three
captures.
Each capture renders `tools/record-fixtures/prometheus.yml` differently. The renderings differ only
in:

- `protobuf_message`
- `metadata_config.send_interval`
- The scrape target
- Whether `write_relabel_configs` keeps the four families or drops everything

| File | Producer | Invocation | Captured | Construct exercised |
|---|---|---|---|---|
| `prometheus-v1-000.bin` (860 bytes) | Prometheus 3.14.0 (`prom/prometheus:v3.14.0`, the upstream revision this repo's `prompb` is vendored from -- `crates/logit-proto/proto/README.md`) | Stock image command line, `tools/record-fixtures/prometheus.yml` with `protobuf_message: prometheus.WriteRequest`, `scrape_interval: 5s`, `metadata_config.send_interval: 1m`; Prometheus scraping its own `/metrics`, `remote_write[].url: http://capture:9091/api/v1/write` | 2026-09-18 | One scrape's samples as remote-write **1.0**. `WriteRequest.metadata[]` is **empty**, because Prometheus's 1.0 sender ships metadata in separate requests; that's the shape that motivates the metadata cache. 26 series over the four families `write_relabel_configs` keeps: a gauge (`prometheus_build_info`, ten labels), a counter (`prometheus_tsdb_wal_page_flushes_total`), a histogram (`prometheus_tsdb_compaction_duration_seconds`, 15 `le` buckets plus `_sum`/`_count`), and a summary (`go_gc_duration_seconds`, five `quantile` lines plus `_sum`/`_count`) |
| `prometheus-v1-001.bin` (861 bytes) | Same | Same | 2026-09-18 | The next scrape of the same series: same shape, later timestamps, so a consuming test can see that one request isn't a one-off |
| `prometheus-v1-metadata-000.bin` (399 bytes) | Same | Same config rendered with `metadata_config.send_interval: 1s`, `write_relabel_configs` as `action: drop` over `.*`, and the scrape target pointed at the static `tools/record-fixtures/prometheus-metadata-target.prom` served by `python3 -m http.server` | 2026-09-18 | A **metadata-only 1.0 request**: `MetricMetadata` entries and no `timeseries` at all. This is the message `prometheus_in`'s `metadata_cache:` exists to remember. Both properties are deterministic. Dropping every series means this run *can't* put a sample on the wire. The small static target means there are **exactly four** entries, one per family the sample captures carry, each with its help text: `go_gc_duration_seconds` (`Summary`), `prometheus_build_info` (`Gauge`), `prometheus_tsdb_compaction_duration_seconds` (`Histogram`), and `prometheus_tsdb_wal_page_flushes_total` (`Counter`). The synthetic `up`/`scrape_*` series a scrape also appends carry no metadata: Prometheus generates them rather than parsing them from an exposition, so the scrape cache never saw a `# TYPE` line for them |
| `prometheus-v1-metadata-001.bin` (379 bytes) | Same | Same | 2026-09-18 | The same declarations one ticker interval later. Prometheus re-sends a family's metadata every `send_interval`, and `MetadataCache::learn` must recognize that repetition as a no-op |
| `prometheus-v1-metadata-002.bin` (379 bytes) | Same | Same | 2026-09-18 | The same again, so a consuming test can seed from all three and get the same table it would get from one |
| `prometheus-v2-000.bin` (771 bytes) | Same | Same config rendered with `protobuf_message: io.prometheus.write.v2.Request` | 2026-09-18 | The same 26 series as remote-write **2.0**: an interned `symbols` table with every label name and value referenced by index, one `Metadata` per series, and `Sample.start_timestamp` present on the wire. For what 3.14.0 actually puts in that `Metadata`, see "What isn't covered here (yet)" |
| `prometheus-v2-001.bin` (786 bytes) | Same | Same | 2026-09-18 | The next scrape, as above |

The four `vmagent-*` requests came from one `script/record-fixtures vmagent` run, in two captures
of two requests each. vmagent scrapes `tools/record-fixtures/prometheus-metadata-target.prom`
(the metadata target above, so the same four families) through `tools/record-fixtures/vmagent.yml`.
The two captures differ only in `-remoteWrite.forcePromProto`. vmagent sends a scrape's samples and
its `MetricMetadata` as **separate requests in no fixed order**, so each capture holds one of each,
and a consuming test tells them apart by content, never by file name.

| File | Producer | Invocation | Captured | Construct exercised |
|---|---|---|---|---|
| `vmagent-zstd-000.bin` (663 bytes) | vmagent v1.152.0 (`victoriametrics/vmagent:v1.152.0`, `vmagent-20260911-130401-tags-v1.152.0-0-g540b91da03`) | `-promscrape.config=vmagent.yml -remoteWrite.url=http://capture:9091/api/v1/write`, nothing else: vmagent's default wire | 2026-09-24 | The **VictoriaMetrics remote write protocol**: a 1.0 `WriteRequest` with `Content-Encoding: zstd`, `X-VictoriaMetrics-Remote-Write-Version: 1`, and no `X-Prometheus-Remote-Write-Version`. `raw_capture.py` answers `204`, so vmagent never sees the `415` or `400` that would downgrade it to Snappy. One scrape's 17 series: the target's four families plus vmagent's own `up` and six `scrape_*` series. The zstd frame header sets `Single_Segment_Flag` and declares its content size (2489 bytes). Labels are **not sorted**: vmagent appends `instance`, `job`, and `monitor` after the exposition's own labels |
| `vmagent-zstd-001.bin` (312 bytes) | Same | Same | 2026-09-24 | A metadata-only 1.0 request on the zstd wire: four `MetricMetadata` entries, one per target family, and no series |
| `vmagent-snappy-000.bin` (379 bytes) | Same | The same, plus `-remoteWrite.forcePromProto` | 2026-09-24 | The metadata-only request on plain remote-write 1.0: `Content-Encoding: snappy`, `X-Prometheus-Remote-Write-Version: 0.1.0`. The same four entries as `vmagent-zstd-001` |
| `vmagent-snappy-001.bin` (843 bytes) | Same | Same | 2026-09-24 | The same 17-series scrape as `vmagent-zstd-000`, Snappy-compressed. Decompressed, the two sample bodies are the same length |

The Prometheus version is what `prom/prometheus:v3.14.0 --version` reported inside the recording
container, and the vmagent version what `victoriametrics/vmagent:v1.152.0 --version` reported. `record_prometheus` prints it on every run, so a re-record's drift from this table shows
in the script's own output.

The seven Prometheus bodies total ~4.4 KB (the sidecars add ~1.5 KB), and the four vmagent bodies
~2.2 KB (the sidecars ~0.9 KB), inside `../README.md`'s size discipline: low single-digit KB per fixture and well under 100 KB for the whole directory. Two
choices keep it there:

- **The four families `write_relabel_configs` keeps on the sample side.** Between them they cover
  all four classic metric types in 26 series. That's one `max_samples_per_send` request's worth, so
  a capture is one whole scrape rather than a fragment.
- **The small static target the metadata side scrapes.** It keeps a *complete* metadata request
  under 400 bytes, where a self-scrape's would be ~16 KB. vmagent scrapes the same target, so its
  sample request is one scrape of four families.

## Tests that consume these fixtures

`crates/logit-proto/tests/prometheus_remote_write_interop.rs` consumes these fixtures. It asserts
that:

- Every body decodes with nothing `Malformed` and **nothing skipped or degraded**.
- The sidecar's `Content-Type` alone selects the wire version.
- `instance`, `job`, and Prometheus's own `external_labels` arrive as ordinary labels and stay
  that way.
- A sample request with no metadata behind it decodes to eight flat `unknown` families: every
  suffixed name is its own family, and `quantile` and `le` are ordinary labels.
- A metadata-only request decodes to declarations and no groups at all.
- Seeding a sample request with the declarations the *metadata* captures reported folds those
  eight families back into the four Prometheus meant, each with its own type and with
  `quantile`/`le` moved into the point. This assertion puts both halves together.
- vmagent's Snappy sample request decodes every one of its 17 series, with its unsorted labels
  sorted, and its own metadata request types the target's four families while `up` and the
  `scrape_*` series stay untyped.
- The zstd captures carry `content-encoding: zstd`, vmagent's version header and no Prometheus
  one, and a zstd frame. The test has no zstd decoder, so it stops there;
  `docs/plans/victoriametrics-interop.md`'s W2 adds one and makes both decode.

**This corpus found a codec bug, which is what it was for.** Recorded against the assembler as it
stood, the first of those assertions read `["unknown_suffix"]` for every sample capture.

A summary is the one classic kind whose bare name is a sample. Remote-write sorts a request's
series by name, so the bare `go_gc_duration_seconds{quantile=…}` always arrives first and opened
the implicit family `go_gc_duration_seconds`. The suffix scan then matched `…_sum` and `…_count`
against that implicit base and discarded them, since no suffix has a role under an untyped family.
That lost two real samples per scrape on every metadata-less request: 1.0 before the first metadata
write, after a TTL lapse, and permanently under `max_families: 0`. Given the finding below, that
includes a pure-2.0 fleet. A histogram never triggered it, having no bare-named sample, which is
why the round-trip and fixed-point suites looked sound.

The fix is in the assembler ("only a declared base claims a suffix"). What the cache buys is now
what it always should have been: **typing, never samples**. A metadata-less request is flatter than
the producer's shape, not lossier.

**The vmagent corpus found a second one.** vmagent doesn't sort a series' labels, which both
remote-write specs require of a sender, and the decoder skipped any series whose set wasn't
strictly ascending as `invalid_labels`. Against vmagent that is every series with a label of its
own: `script/victoria-interop`'s vmagent leg received vmagent's `up` and `scrape_*` series and
none of the target's. Prometheus's and VictoriaMetrics's receivers both sort on arrival, and the
decoder now does too, rejecting only a repeated name.

## What isn't covered here (yet)

- **Inline metadata on remote-write 2.0.** In this topology, Prometheus 3.14.0 sends every 2.0
  series with an *empty* `Metadata`: `type: UNSPECIFIED`, with no help or unit reference. That was
  checked with and without `--enable-feature=metadata-wal-records`, on the first request of a
  process's life and on its tenth, with and without `write_relabel_configs`, and with a scrape
  interval long enough that the remote-write WAL watcher finished replaying before the first
  scrape. So these two captures exercise 2.0's symbol table, its per-sample layout, and its
  `Content-Type` negotiation, but **not** the inline-metadata path the spec describes.
  `crates/logit-proto/tests/prometheus_remote_write_fixed_point.rs` and
  `crates/logit-cli/tests/prometheus_remote_write_round_trip.rs` cover that path against `logit`'s
  own 2.0 sender, which does populate it. Re-check this when a later Prometheus is vendored.
- **A populated `Sample.start_timestamp`.** The field is on the wire in the 2.0 captures but is
  `0` (unset) on every sample. Prometheus only emits a created timestamp with
  `--enable-feature=created-timestamp-zero-ingestion` against a target that exposes one, and its
  own `/metrics` doesn't.
- **Native histograms.** `TimeSeries.histograms` is empty in every capture. `logit` skips and
  counts them either way today (`docs/known-gaps.md`), so a fixture carrying one would exercise the
  skip and nothing else. It belongs with the native-histogram follow-up, not here.
- **Exemplars.** Prometheus only remote-writes exemplars with `--enable-feature=exemplar-storage`
  and a target exposing them in OpenMetrics, and its own `/metrics` doesn't. The codec's own
  fixed-point suite and the round-trip test's OpenMetrics corpus cover the exemplar mapping.
- **A 4xx or a partial write.** `raw_capture.py` answers `204` unconditionally and sets none of
  2.0's `X-Prometheus-Remote-Write-*-Written` headers, so Prometheus (correctly) logs a
  non-recoverable error after each 2.0 request. That's a property of the capture sink, not of
  these bodies. `crates/logit-inputs/src/prometheus.rs` tests the receiver's own response table.
