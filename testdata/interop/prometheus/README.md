# Prometheus remote-write interop fixtures

Real HTTP request bodies a real Prometheus `POST`ed, captured verbatim by
`tools/record-fixtures/raw_capture.py --proto http` -- **no parsing, no decompression, no
re-encoding.** Each `.bin` is exactly the Snappy-compressed protobuf Prometheus put on the wire;
`logit`'s own encoder never touches these, which is the whole point:
`crates/logit-proto/src/prometheus/remote_write.rs` was written from the two remote-write specs and
the vendored `prompb`, and these fixtures are what checks that reading against the sender every
deployment actually runs. Regenerate with `script/record-fixtures prometheus` (see `../README.md`
and `script/record-fixtures`'s own header comment).

**Two files per request.** The `.bin` is the body; the `.headers` sidecar beside it holds the
request's method, its path, and every request header with the name lowercased, one per line. That
sidecar is part of the fixture rather than a convenience: remote-write's framing lives in its
headers, so the wire version (`Content-Type`, `X-Prometheus-Remote-Write-Version`) and the
compression (`Content-Encoding: snappy`) are only recoverable from it. This is also why this corpus
is captured through an HTTP sink that **answers** `204` rather than through the read-only `--proto
tcp` mode the `*.raw` corpora use: a real client will not send a second request to a listener that
never replied to the first.

All seven came out of one `script/record-fixtures prometheus` run, in three captures against
`tools/record-fixtures/prometheus.yml` rendered three ways -- the only differences between the
renderings are `protobuf_message`, `scrape_interval` and `metadata_config.send_interval`.

| File | Producer | Invocation | Captured | Construct exercised |
|---|---|---|---|---|
| `prometheus-v1-000.bin` (860 bytes) | Prometheus 3.14.0 (`prom/prometheus:v3.14.0`, the upstream revision this repo's `prompb` is vendored from -- `crates/logit-proto/proto/README.md`) | stock image command line, `tools/record-fixtures/prometheus.yml` with `protobuf_message: prometheus.WriteRequest`, `scrape_interval: 5s`, `metadata_config.send_interval: 1m`; Prometheus scraping its own `/metrics`, `remote_write[].url: http://capture:9091/api/v1/write` | 2026-09-18 | One scrape's samples as remote-write **1.0**, and the shape that motivates the metadata cache: `WriteRequest.metadata[]` is **empty**, because Prometheus's 1.0 sender ships metadata in requests of its own. 26 series over the four families `write_relabel_configs` keeps -- a gauge (`prometheus_build_info`, ten labels), a counter (`prometheus_tsdb_wal_page_flushes_total`), a histogram (`prometheus_tsdb_compaction_duration_seconds`, 15 `le` buckets plus `_sum`/`_count`) and a summary (`go_gc_duration_seconds`, five `quantile` lines plus `_sum`/`_count`) |
| `prometheus-v1-001.bin` (861 bytes) | Same | Same | 2026-09-18 | The next scrape of the same series -- same shape, later timestamps, so a consuming test can see one request is not a one-off |
| `prometheus-v1-metadata-000.bin` (909 bytes) | Same | Same config rendered with `scrape_interval: 30s` and `metadata_config.send_interval: 1s`, so the metadata ticker fires between scrapes rather than alongside them | 2026-09-18 | A **metadata-only 1.0 request**: ten `MetricMetadata` entries, no `timeseries` at all. This is the message `prometheus_in`'s `metadata_cache:` exists to remember. Carries a real spread of declared types (`Summary`, `Gauge`, `Counter`) with their help text, and -- the reason this file is the one a consuming test seeds from -- `go_gc_duration_seconds`, which is also one of the four families the sample captures carry |
| `prometheus-v1-metadata-001.bin` (1088 bytes) | Same | Same | 2026-09-18 | The next ten families on the same ticker, including a `Histogram` declaration |
| `prometheus-v1-metadata-002.bin` (795 bytes) | Same | Same | 2026-09-18 | Ten more again -- between the three, 30 of Prometheus's own several hundred families, which is all `metadata_config.max_samples_per_send: 10` lets into one request |
| `prometheus-v2-000.bin` (771 bytes) | Same | Same config rendered with `protobuf_message: io.prometheus.write.v2.Request` | 2026-09-18 | The same 26 series as remote-write **2.0**: an interned `symbols` table with every label name and value referenced by index, one `Metadata` per series, `Sample.start_timestamp` present on the wire. See "What isn't covered here (yet)" for what 3.14.0 actually puts in that `Metadata` |
| `prometheus-v2-001.bin` (786 bytes) | Same | Same | 2026-09-18 | The next scrape, as above |

The Prometheus version is what `prom/prometheus:v3.14.0 --version` reported inside the recording
container; `record_prometheus` prints it on every run, so a re-record's drift from this table is
visible in the script's own output. Total fixture bytes here are ~6 KB across the seven bodies,
inside `../README.md`'s "low single-digit KB per fixture, whole directory well under 100 KB"
budget; the four families `write_relabel_configs` keeps are what holds it there, and they were
chosen because between them they cover all four classic metric types in 26 series -- one
`max_samples_per_send` request's worth, so a capture is one whole scrape rather than a fragment.

`crates/logit-proto/tests/prometheus_remote_write_interop.rs` consumes these: that every body
decodes with nothing `Malformed` and **nothing skipped or degraded**; that the sidecar's
`Content-Type` alone selects the wire version; that `instance`, `job` and Prometheus's own
`external_labels` arrive as ordinary labels and stay that way; that a sample request with no
metadata behind it decodes to eight flat `unknown` families (every suffixed name its own family,
`quantile` and `le` ordinary labels); that a metadata-only request decodes to declarations and no
groups at all; and -- the one that puts both halves together -- that seeding a sample request with
the declarations the *metadata* captures reported folds three of those eight back into the one
`Summary` Prometheus meant.

**This corpus found a codec bug, which is what it was for.** Recorded against the assembler as it
stood, the first of those assertions read `["unknown_suffix"]` for every sample capture. A summary
is the one classic kind whose bare name is a sample, so `go_gc_duration_seconds{quantile=…}` opened
the implicit family `go_gc_duration_seconds` -- remote-write sorts a request's series by name, so
the bare one always arrives first -- and `…_sum` and `…_count` were then matched against that
implicit base by the suffix scan and thrown away, since no suffix has a role under an untyped
family. Two real samples per scrape, on every metadata-less request: 1.0 before the first metadata
write, after a TTL lapse, and permanently under `max_families: 0` — including a pure-2.0 fleet,
given the finding below. A histogram never armed it, having no bare-named sample, which is why the
round trip and the fixed-point suites looked sound. The fix is in the assembler ("only a declared
base claims a suffix"), and what the cache buys is now exactly what it always should have been:
**typing, never samples**. A metadata-less request is flatter than the producer's shape, not
lossier.

## What isn't covered here (yet)

- **Inline metadata on remote-write 2.0.** Prometheus 3.14.0 sends every 2.0 series with an
  *empty* `Metadata` -- `type: UNSPECIFIED`, no help or unit reference -- in this topology. That was
  checked with and without `--enable-feature=metadata-wal-records`, on the first request of a
  process's life and on its tenth, with `write_relabel_configs` and without, and with a scrape
  interval long enough that the remote-write WAL watcher had finished replaying before the first
  scrape. So these two captures exercise 2.0's symbol table, its per-sample layout and its
  `Content-Type` negotiation, but **not** the inline-metadata path the spec describes.
  `crates/logit-proto/tests/prometheus_remote_write_fixed_point.rs` and
  `crates/logit-cli/tests/prometheus_remote_write_round_trip.rs` cover that against `logit`'s own
  2.0 sender, which does populate it. Re-check this when a later Prometheus is vendored.
- **A populated `Sample.start_timestamp`.** The field is on the wire in the 2.0 captures but is
  `0` (unset) on every sample: Prometheus only emits a created timestamp with
  `--enable-feature=created-timestamp-zero-ingestion` against a target that exposes one, which
  scraping its own `/metrics` does not.
- **Native histograms.** `TimeSeries.histograms` is empty in every capture. `logit` skips and
  counts them either way today (`docs/known-gaps.md`), so a fixture carrying one would exercise the
  skip and nothing else; it belongs with the native-histogram follow-up, not here.
- **Exemplars.** Prometheus only remote-writes exemplars with `--enable-feature=exemplar-storage`
  and a target exposing them in OpenMetrics; its own `/metrics` does not. The exemplar mapping is
  covered by the codec's own fixed-point suite and by the round-trip test's OpenMetrics corpus.
- **A 4xx or a partial write.** `raw_capture.py` answers `204` unconditionally and sets none of
  2.0's `X-Prometheus-Remote-Write-*-Written` headers, which Prometheus (correctly) logs as a
  non-recoverable error after each 2.0 request. That is a property of the capture sink, not of
  these bodies; the receiver's own response table is tested in `crates/logit-inputs/src/prometheus.rs`.
