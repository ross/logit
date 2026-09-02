# `logit` demo stack

A self-contained stack for trying `logit`: no Rust toolchain, no `script/*`, no knowledge of the
rest of this repo. See [docs/plans/0003-demo-stack.md](../docs/plans/0003-demo-stack.md) for why it
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
| Loki | http://localhost:3100 | Provisioned as a Grafana datasource; see below for what's actually in it. |
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
`syslog_in` listener. [`logit.yaml`](logit.yaml) runs it through `json` → `kv_metrics` → `keep` →
`aggregate` → `influxdb_out`, plus `logit` observing its own pipeline via `internal`
(`../docs/design/internal-telemetry.md`) into the same InfluxDB bucket *and* over OTLP/gRPC into
Tempo, as real spans — one per node-visit, at `span_sample_rate: 1.0` so nothing is thinned out
(`../docs/adr/0022-internal-span-emission-and-deterministic-sampling.md`,
`../docs/adr/0024-hand-rolled-grpc-over-hyper.md`). **Metrics and traces both work end to end
today** — that's what the shipped Grafana dashboard shows, side by side: the `logit.*` InfluxDB
panels and a Tempo traces panel over the same pipeline.

The pipeline diagram on the landing page (and at `:8080/graph.svg` directly) is rendered at
startup, not hand-drawn: `graph-dot` runs `logit graph logit.yaml` against the actual config this
stack is running, `graph-svg` pipes that DOT through real Graphviz
([`graph-renderer/Dockerfile`](graph-renderer/Dockerfile)), and `hello` serves the result. Both are
one-shot containers gated with `depends_on: condition: service_completed_successfully` — expect to
see them as `Exited (0)` in `docker compose ps`, that's them having finished, not crashed. (On
`podman-compose` specifically, that condition is reportedly unimplemented and may be ignored; if
so, the page just shows a "not rendered yet" placeholder until a refresh after `graph-svg` catches
up — the SVG is read fresh on every request, nothing is cached.)

## What isn't wired yet

Traces now work end to end (`logit` emits real spans and exports them to Tempo over OTLP/gRPC —
see the previous section). One leg is still open:

- **Logs → Loki** need a `syslog_out` output, which doesn't exist (not implemented, not even a
  declared config kind). Loki is genuinely empty — no scaffolding double-write, an honest
  "provisioned, nothing lands here yet" story. `alloy` (the syslog → Loki shim) is still up,
  unfed, ready for `logit` to point at it once `syslog_out` lands — see `logit.yaml`'s
  commented-out `log_out` component. This was dropped deliberately from the session that landed
  OTLP (`../docs/plans/0005-otlp-end-to-end.md`'s "Out of scope"), not an oversight — OTLP alone
  was large enough to warrant the whole session. Grafana's Loki datasource is already wired for
  the day it lands: `tracesToLogsV2` on the Tempo datasource and a `derivedFields` link on Loki
  (`grafana/provisioning/datasources/datasources.yaml`) mean a `trace_id=<hex>` in a log line will
  click straight through to the matching Tempo trace with no further provisioning work.

## Stopping

```sh
docker compose down        # stop, keep data
docker compose down -v     # stop, wipe all volumes (InfluxDB/Grafana/Loki/Tempo/Alloy/graph state)
```
