# `logit` demo stack

A self-contained stack for trying `logit`: no Rust toolchain, no `script/*`, no knowledge of the
rest of this repo. See [docs/plans/demo-stack.md](../docs/plans/demo-stack.md) for why it
exists and what's next.

## Quick start

```sh
cd demo
docker compose up --build
```

The first run builds `logit`'s production image from source (vendored LuaJIT, no dependency-layer
caching by design — see `../Dockerfile`'s comment) — expect several minutes before anything
appears. Every run after that reuses the built image.

Once it's up, **start at http://localhost:8080** — a small page that links to Grafana with
get-started instructions and shows this stack's own pipeline, rendered live.

| Service | URL | What it's for |
|---|---|---|
| Hello-world app | http://localhost:8080 | Start here. Landing page, Grafana link, and the demo's traffic source. |
| Grafana | http://localhost:3000 | Anonymous admin access. Open the "logit" folder for the pre-built dashboard. |
| InfluxDB | http://localhost:8086 | `logit`/`logit-demo-password`. Bucket `metrics`, org `logit`. |
| Loki | http://localhost:3100 | Provisioned as a Grafana datasource; receives `logit`'s own logs via `syslog_out` → `alloy`. |
| Tempo | http://localhost:3200 (query), :4317/:4318 (OTLP) | Provisioned as a Grafana datasource; receives `logit`'s own internal spans over OTLP/gRPC. |

`docker compose logs -f logit` shows every decoded event as a `stdio_out` block — the fastest way
to see the pipeline doing something. You may occasionally see a `component 'trace_out': batch
dropped after a permanent send failure` warning (at most once a minute) — real and harmless, not a
sign anything is broken: `trace_windowed` (`logit.yaml`) periodically flushes a metrics-only batch
that Tempo (a traces-only backend) rejects, the same way it would reject any OTLP metrics request;
the far more frequent traces-only batches in between all succeed. See
[docs/known-gaps.md](../docs/known-gaps.md)'s "`otlp_out` aborts an entire batch's `send`..." entry
for the full account, including why `trace_windowed` exists at all — without it, this interaction
doesn't just log a warning, it stops `logit` a minute after startup.

## What's actually flowing

The hello-world app at `:8080` ([`hello/app.py`](hello/app.py), stdlib Python, no dependencies) is
both the front door and the traffic source: it logs every real visit, plus a background stream of
synthetic requests every half-second so the dashboard has something to show immediately — the same
RFC 3164 + JSON-body shape `../crates/logit-bench/src/fixtures.rs` measures — to `logit`'s
`syslog_in` listener. [`logit.yaml`](logit.yaml) runs it through `json`, then fans out three ways:
to `stdio_out` (the `docker compose logs` tap), to a logs leg (`log_out`, `syslog_out` over UDP RFC
5424 to `alloy:5141` → Loki), and to a metrics leg (`kv_metrics` → `keep` → `aggregate` →
`influxdb_out`) — plus `logit` observing its own pipeline via `internal`
(`../docs/design/internal-telemetry.md`) into that same InfluxDB bucket *and*, as real spans, over
OTLP/gRPC into Tempo, one span per node-visit at `span_sample_rate: 1.0` so nothing is thinned out
(`../docs/adr/internal-span-emission-and-deterministic-sampling.md`,
`../docs/adr/hand-rolled-grpc-over-hyper.md`). **All three signals work end to end today** —
that's what the shipped Grafana dashboard shows, side by side: the `logit.*` InfluxDB panels, a Loki
logs panel, and a Tempo traces panel, all over the same pipeline. `log_out` round-trips `access_in`'s
own `syslog.hostname`/`syslog.tag` attributes onto every relayed message
(`../docs/adr/syslog-output.md`), so Loki gets real `host`/`app` stream labels with no extra
config.

The pipeline diagram on the landing page (and at `:8080/graph.svg` directly) is rendered at
startup, not hand-drawn: `graph-dot` runs `logit graph logit.yaml` against the actual config this
stack is running, `graph-svg` pipes that DOT through real Graphviz
([`graph-renderer/Dockerfile`](graph-renderer/Dockerfile)), and `hello` serves the result. Both are
one-shot containers gated with `depends_on: condition: service_completed_successfully` — expect to
see them as `Exited (0)` in `docker compose ps`, that's them having finished, not crashed. (On
`podman-compose` specifically, that condition is reportedly unimplemented and may be ignored; if
so, the page just shows a "not rendered yet" placeholder until a refresh after `graph-svg` catches
up — the SVG is read fresh on every request, nothing is cached.)

## What isn't exercised yet

All three signals reach a real backend now — nothing in this demo's own pipeline is left pending.
One thing genuinely isn't exercised, though: **`otlp_in`** (`crates/logit-inputs/src/otlp.rs`) ships
implemented and tested, but nothing in this stack sends *to* it — `demo/logit.yaml` only ever uses
`otlp_out`, as a client. The natural next step is reworking [`hello/app.py`](hello/app.py) to use a
real Python OTLP SDK, which would exercise `otlp_in` with genuine third-party traffic and put
application spans in Tempo alongside `logit`'s own internal ones
(`../docs/plans/otlp-end-to-end.md`'s "Follow-ups deliberately left out"). Grafana's Loki
datasource also carries a `derivedFields` link to Tempo (`tracesToLogsV2` on the Tempo datasource,
`grafana/provisioning/datasources/datasources.yaml`), so a `trace_id=<hex>` in a log line clicks
straight through to the matching Tempo trace — `logit`'s own emitted logs don't currently carry one
(`syslog_out` round-trips `syslog.*` attributes, not trace context), so this link is proven wiring
waiting on a future log line that names a trace, not a gap in what's shipped.

## Stopping

```sh
docker compose down        # stop, keep data
docker compose down -v     # stop, wipe all volumes (InfluxDB/Grafana/Loki/Tempo/Alloy/graph state)
```
