# victoria-interop

`script/victoria-interop` checks `logit` against real VictoriaMetrics, VictoriaLogs,
VictoriaTraces, and vmagent. It runs the legs in [The legs](#the-legs), queries each backend for
what arrived, and prints one row per leg. [Recorded run](#recorded-run) records what a run
showed. What the backends were found to accept is in
[ADR `victoriametrics-interop`](../../docs/adr/victoriametrics-interop.md)'s "Verified behavior".

The script runs on the host and drives docker (`$DOCKER`, `sudo docker` by default), like
`script/shape-survey` and `script/record-fixtures`. It isn't part of `script/cibuild`, and no test
depends on it running.

## What it runs

`compose.yaml` starts one stack under the compose project `victoria-interop`, on the network
`victoria-interop-net`, with no host ports:

| Service | Image | Role |
|---|---|---|
| `victoria-metrics` | `victoriametrics/victoria-metrics:v1.152.0` | `-graphiteListenAddr=:2003`; OTLP over HTTP is on by default |
| `victoria-logs` | `victoriametrics/victoria-logs:v1.52.0` | `-syslog.listenAddr.tcp=:514` |
| `victoria-traces` | `victoriametrics/victoria-traces:v0.11.1` | `-otlpGRPCListenAddr=:4317`, `-otlpGRPC.tls=false` |
| `vmagent` | `victoriametrics/vmagent:v1.152.0` | scrapes `logit-expose` (`vmagent-scrape.yaml`), remote-writes to VictoriaMetrics and to `logit-vmagent-in` |
| `logit-<leg>` | `logit:victoria-interop`, built from the current tree | one per `logit-<leg>.yaml` |
| `logit-influx-relay` | same image | leg 11: `influxdb_in` on `:8087` relayed by `influxdb_out` to VictoriaMetrics; `check.py` posts one line-protocol body to it and the same body directly to VictoriaMetrics, and compares the two stored series sets |

Each `logit-<leg>.yaml` says at its top which leg it is. The legs that need synthetic traffic use
`generate_in`, and the OTLP legs build their metrics, logs, and spans with a `lua` stage's
`Event.new`. Every config passes `logit validate`: `script/validate` and the
`every_shipped_config_loads_and_validates` test both cover `logit-*.yaml` here.

## The legs

1. `prometheus_out` `version: 1` → VictoriaMetrics `/api/v1/write`. Leg 1z is the same with
   `compression: zstd`.
2. `prometheus_out` `version: 2` → VictoriaMetrics, recording the status and body it answers.
3. `prometheus_out` `bind:` scraped by vmagent.
4. `influxdb_out` → VictoriaMetrics `/api/v2/write`, recording which of `org`, `bucket`, and `db`
   become labels.
5. `graphite_out` plaintext, `tags: carbon` → VictoriaMetrics `:2003`.
6. `otlp_out` over HTTP, one sink per product behind `keep_signals`: metrics → VictoriaMetrics
   `/opentelemetry` (a delta `Sum` beside a cumulative one, and an `ExponentialHistogram`); logs →
   VictoriaLogs `/insert/opentelemetry` with `VL-Stream-Fields` and `VL-Msg-Field` in `headers:`;
   traces → VictoriaTraces `/insert/opentelemetry`.
7. `otlp_out` over gRPC → VictoriaTraces.
8. `syslog_out` over TCP → VictoriaLogs syslog, confirming octet counting is detected.
9. `prometheus_in` scraping VictoriaMetrics `/federate?match[]=...`; the series come back
   untyped.
10. vmagent → `prometheus_in` `bind:`: vmagent stays on zstd, counted
    `logit.input.writes{class="ok",encoding="zstd"}`, and never logs its downgrade to Snappy.
11. `influxdb_in` → `influxdb_out` → VictoriaMetrics, compared with the same body posted straight
    to VictoriaMetrics.

The probes replay committed captures from `testdata/interop/prometheus/`: `prometheus-v2-000` for
a remote-write 2.0 request, and `vmagent-zstd-000` under each version header and
`Content-Encoding` variant.

## A run

1. Builds `logit:victoria-interop` from `Dockerfile` (set `VICTORIA_INTEROP_SKIP_IMAGE=1` to reuse
   it) and validates every leg config with it.
2. Brings the stack up with `docker compose up --wait`, which waits on each `logit` service's
   `logit ready` health check.
3. Lets traffic flow for `VICTORIA_INTEROP_WINDOW` seconds (default 45).
4. Copies every service's log into the run directory and runs `check.py` in a
   `python:3.12-slim` container on the stack's network. `check.py` queries VictoriaMetrics
   (`/api/v1/series`, `/api/v1/export`), VictoriaLogs (`/select/logsql/query`), and
   VictoriaTraces (`/select/jaeger/api/traces`), and reads the files the `federate` and
   `vmagent-in` legs write. It also replays committed captures from
   `testdata/interop/prometheus/` at VictoriaMetrics to record how it answers a remote-write 2.0
   request and each zstd header variant.
5. Tears the project down (`down -v --remove-orphans`) on every exit.

Each leg's row is `PASS` (the leg did what [The legs](#the-legs) says), `GAP` (the backend or
`logit` behaves in a way [Recorded run](#recorded-run) and `docs/known-gaps/` record as a gap), or
`FAIL`. The script exits 1 on any `FAIL`.

A run writes `perf/results/victoria-interop/<timestamp>/` (gitignored, or under
`VICTORIA_INTEROP_OUT`): `results.md` and `results.json`, `provenance.txt` with the image tags,
`logs/<service>.log`, and the `logit` file outputs (`stdio_out` with `format: json`, one object per line,
which `check.py` reads with `json.loads`).

## Recorded run

Run 2026-09-24 at `73ca4931` against `victoriametrics/victoria-metrics:v1.152.0`,
`victoriametrics/vmagent:v1.152.0`, `victoriametrics/victoria-logs:v1.52.0`, and
`victoriametrics/victoria-traces:v0.11.1`. Legs 1z and 10 are from a rerun at `cacf17bf`, after
zstd landed on remote-write; leg 11 is from 2026-10-13.

| Leg | Result | Detail |
|---|---|---|
| 1 | Worked | `vi_rw1_gauge` and `vi_rw1_requests_total` (a `Sum` through `aggregate` `temporality: cumulative`) stored with their `leg` label and nothing added |
| 1z | Worked | `vi_rw1z_gauge` and `vi_rw1z_requests_total` stored from a zstd body, otherwise as leg 1 |
| 2 | Gap | VictoriaMetrics answers a remote-write 2.0 request `204` with an empty body and stores nothing, with nothing logged, so `prometheus_out` `version: 2` reports every batch delivered while all of it is lost. Replaying `prometheus-v2-000` gives the same `204` ([`docs/known-gaps/prometheus.md`](../../docs/known-gaps/prometheus.md)) |
| 3 | Worked | vmagent scrapes `prometheus_out`'s `bind:` and remote-writes `vi_expose_gauge` and `vi_expose_requests_total` to VictoriaMetrics with `job="logit-expose"` and `instance` added |
| 4 | Worked | `vi_influx_gauge` arrives as `vi_influx_gauge_value{leg="influx"}` (`<measurement>_<field>`); none of `org`, `bucket`, and `db` becomes a label |
| 5 | Worked | `vi_graphite_gauge{leg="graphite"}`: carbon's `;leg=graphite` segment is a label |
| 6 | Worked | Each signal reaches its product. VictoriaMetrics stores the delta `Sum` as raw per-interval points and the `ExponentialHistogram` as `vmrange` buckets plus `_count` and `_sum`; VictoriaLogs stores the OTLP body as `_msg` with or without `VL-Msg-Field`, and `service.name` as the stream; VictoriaTraces serves the spans on its Jaeger API. VictoriaMetrics and VictoriaLogs both add `scope.name="unknown"` and `scope.version="unknown"` to a record with an empty OTLP scope |
| 7 | Gap, plus a fix | Spans arrive, but VictoriaTraces's gRPC listener closes every connection about 5 s after it opens, with a TCP FIN and no HTTP/2 `GOAWAY` (a packet capture puts the FIN 5.0 s after connect, whatever is in flight). A request in flight then fails `Ambiguous` ([`docs/known-gaps/otlp.md`](../../docs/known-gaps/otlp.md)). An isolated 20 s rerun at 1 batch/s saw 3 closes and 2 dropped batches; the recorded run passed because no request raced a close. The same capture showed each export stalled 40 ms between its HEADERS and DATA frames, Nagle against delayed ACK, fixed in `73ca4931` by setting `TCP_NODELAY` on `otlp_out`'s gRPC connections |
| 8 | Worked | 53 records under `app_name:vi-syslog`, `_msg` the message with no length prefix, so VictoriaLogs detects `syslog_out`'s octet counting; `format=rfc5424`, stream `{app_name, hostname, proc_id}` |
| 9 | Worked | `vi_rw1_gauge` comes back from `/federate` every 5 s a `Gauge` with `prometheus.type="untyped"` and its wire timestamp kept |
| 10 | Worked | vmagent's first request already succeeds: `class="ok",encoding="zstd"` for every write (22 in the window), `class="unsupported"` 0, no downgrade in vmagent's log, and both `vi_expose_*` series arrive with nothing skipped. Against a receiver without zstd, vmagent's first request gets `415`, it logs "Downgrading protocol from VictoriaMetrics to Prometheus remote write for all future requests", and stays on Snappy |
| 11 | Worked | The relayed and the direct path store the same 12 series, labels, timestamps, and distinct values. VictoriaMetrics stores a bool as 1/0 and a string field as `0` on both paths. It keeps an exact duplicate point as a second sample at the same timestamp; `influxdb_in` merges it first, so the relayed series has one |

The harness's own rows from that run:

```
| 1 | prometheus_out version: 1 -> VictoriaMetrics /api/v1/write | PASS | vi_rw1_gauge, vi_rw1_requests_total stored (62 points), labels ['leg'] |
| 1z | prometheus_out version: 1, compression: zstd -> VictoriaMetrics /api/v1/write | PASS | vi_rw1z_gauge, vi_rw1z_requests_total stored (63 points) via compression: zstd, labels ['leg'] |
| 2 | prometheus_out version: 2 -> VictoriaMetrics | GAP | VictoriaMetrics answered every 2.0 request 2xx and stored nothing; prometheus_out logged no rejection (see the probe row for the status) |
| 3 | prometheus_out bind: <- vmagent scrape -> VictoriaMetrics | PASS | vi_expose_gauge, vi_expose_requests_total scraped by vmagent, labels ['instance', 'job', 'leg'] |
| 4 | influxdb_out -> VictoriaMetrics /api/v2/write | PASS | vi_influx_gauge_value, labels ['leg']; of org/bucket/db, none became labels |
| 5 | graphite_out plaintext, tags: carbon -> VictoriaMetrics :2003 | PASS | vi_graphite_gauge, labels ['leg'] (the ;leg= tag is a label) |
| 6 | otlp_out HTTP -> VictoriaMetrics, VictoriaLogs, VictoriaTraces | PASS | metrics ['vi_otlp_cumulative_sum', 'vi_otlp_delta_sum', 'vi_otlp_exphist_bucket', 'vi_otlp_exphist_count', 'vi_otlp_exphist_sum']; delta sum stored as values [1]; exphist as 3 vmrange buckets; logs[default] _msg='victoria-interop otlp-http log 54' _stream={service.name="victoria-interop-otlp-http"}; logs[msg-field] _msg='victoria-interop otlp-http log 54' _stream={service.name="victoria-interop-otlp-http"}; traces: 20 with vi-otlp-http-span |
| 7 | otlp_out gRPC -> VictoriaTraces | PASS | 20 traces with vi-otlp-grpc-span in VictoriaTraces |
| 8 | syslog_out TCP -> VictoriaLogs syslog | PASS | 53 records, _msg='victoria-interop syslog line 52' (no length prefix: octet counting detected), format=rfc5424, _stream={app_name="vi-syslog",hostname="vi-syslog-host",proc_id="-"} |
| 9 | prometheus_in scrape <- VictoriaMetrics /federate | PASS | 10 scrapes of vi_rw1_gauge, rendered `vi_rw1_gauge gauge=42`, prometheus.type=untyped, wire timestamp kept |
| 10 | vmagent remote-write -> prometheus_in bind: | PASS | writes class=ok,encoding=zstd 22 (class=ok total 22), class=unsupported 0; vmagent log does not show the downgrade; received ['vi_expose_gauge', 'vi_expose_requests_total'] vi_expose_* series; 0 series skipped |
| 11 | influxdb_in -> influxdb_out -> VictoriaMetrics, against a direct write | PASS | 12 series over fields ['idle', 'note', 'usage_system', 'usage_user'] and 20 points on the direct path, identical by series, labels, timestamps, and distinct values on the relayed path; a bool is stored as 1/0 and the string field (note) as 0 on both paths, so the comparison expects the same loss on both; VictoriaMetrics keeps the duplicate point as 4 extra sample(s) at one timestamp, which influxdb_in merges before it relays |
```

## Cleanup and a shared daemon

Everything the script creates belongs to the `victoria-interop` project, so cleanup can't touch
another session's containers. The project name is fixed, so one run at a time per daemon: the
script refuses to start while the project has containers, and prints the `down` command for a
stack a crashed run left behind.
