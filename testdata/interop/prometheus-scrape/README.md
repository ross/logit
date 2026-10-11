# Prometheus scrape interop fixtures

Scrape response bodies two real exporters served, captured verbatim by curl with **no parsing and
no trimming**. Each `.body` is the bytes the exporter wrote, and its `.headers` sidecar
holds the response's `content-type: <value>` line, which is what picks the dialect, the same rule
as [`../prometheus/`](../prometheus/README.md)'s request sidecars.

`logit`'s own encoder never touches these. They're the recorded half of the Prometheus text
differential corpus ([`../../differential/prometheus-text/`](../../differential/prometheus-text/README.md)),
where Prometheus's own parser reads each one and
`crates/logit-proto/tests/prometheus_text_differential.rs` checks that `logit` reads it the same
way with nothing skipped or degraded.

To regenerate, run `script/record-fixtures prometheus-scrape`, then `script/differential prom-text`
for the readings. See `../README.md` and the header comment above `record_prometheus_scrape` in
`script/record-fixtures`.

## Fixtures

curl fetched each target twice: once with `prometheus_in`'s own `Accept` header
(`crates/logit-inputs/src/prometheus.rs`'s `ACCEPT_HEADER_VALUE`, read at record time), then with a
forced `text/plain;version=0.0.4`. The second body is kept only when its `Content-Type` differs.

| File | Producer | Invocation | Captured | Construct exercised |
|---|---|---|---|---|
| `node-exporter-0.body` (22,647 bytes) | node_exporter 1.12.1 (`prom/node-exporter:v1.12.1@sha256:1b4e4438faca4dd7e001dd445d161a4a2091b0fededa84093b3a8dfeae1f1be0`, revision `6044da783597cc3b57aef7580ddcdcff58a4ee99`) | `--collector.disable-defaults --collector.loadavg --collector.meminfo --collector.netdev --collector.uname`, scraped with `prometheus_in`'s `Accept` | 2026-10-09 | Text 0.0.4 (`text/plain; version=0.0.4; charset=utf-8; escaping=underscores`): node_exporter answers both `Accept` values in text 0.0.4, so it records one body. 380 lines: 25 counters, 90 gauges, and one summary (`go_gc_duration_seconds`) across the four collectors and the exporter's own `go_*`, `process_*`, and `promhttp_*` families |
| `python-client-0.body` (4,305 bytes) | prometheus_client 0.26.0 on CPython 3.12.15 (`python:3.12.15-slim@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1`, `pip install prometheus_client==0.26.0`) | `tools/record-fixtures/python_prometheus_client_app.py` through the library's `start_http_server`, scraped with `prometheus_in`'s `Accept` | 2026-10-09 | OpenMetrics 1.0 (`application/openmetrics-text; version=1.0.0; charset=utf-8; escaping=underscores`): counters with `_created` and an exemplar, a gauge with `# UNIT` holding `NaN`, both infinities, `-0.0`, the largest `f64`, and the smallest subnormal, a histogram with a bucket exemplar, a summary, `info`, `stateset`, `gaugehistogram`, `unknown`, label values with `\"`, `\\`, `\n`, and non-ASCII text, `_created` in exponent form, exemplar timestamps with seven fractional digits, and the library's process and GC collectors |
| `python-client-1.body` (4,618 bytes) | Same | The same app, scraped with `Accept: text/plain;version=0.0.4` | 2026-10-09 | The same metrics as text 0.0.4: `_created` as separate gauge families, `info` as an `_info` gauge, `stateset` as a gauge, `gaugehistogram` as a `histogram` beside separate `_gcount` and `_gsum` gauge families, `unknown` as `untyped`, and no exemplars |

A re-record changes the measured values (node_exporter's readings, the process and GC collectors'),
the `_created` instants, and the exemplar timestamps; the app's own values are fixed.

The three bodies total about 31 KB, inside `../README.md`'s size discipline. node_exporter runs
four collectors chosen for small output; a body is never trimmed, since a trimmed body isn't real
bytes, so the way to shrink one is to drop a collector.
