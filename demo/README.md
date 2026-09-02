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
| Loki | http://localhost:3100 | Provisioned as a Grafana datasource; receiving logs via `alloy`. |
| Tempo | http://localhost:3200 (query), :4317/:4318 (OTLP) | Provisioned as a Grafana datasource; empty. |

`docker compose logs -f logit` shows every decoded event as a `stdio_out` block — the fastest way
to see the pipeline doing something.

## What's actually flowing

The hello-world app at `:8080` ([`hello/app.py`](hello/app.py), stdlib Python, no dependencies) is
both the front door and the traffic source: it logs every real visit, plus a background stream of
synthetic requests every half-second so the dashboard has something to show immediately — the same
RFC 3164 + JSON-body shape `../crates/logit-bench/src/fixtures.rs` measures — to `logit`'s
`syslog_in` listener. [`logit.yaml`](logit.yaml) runs it through `json`, then fans out to
`stdio_out` (the `docker compose logs` tap) and to a metrics leg (`kv_metrics` → `keep` →
`aggregate` → `influxdb_out`) and a logs leg (`log_out`, `syslog_out` over UDP RFC 5424 to
`alloy:5141`), plus `logit` observing its own pipeline via `internal`
(`../docs/design/internal-telemetry.md`) into the same InfluxDB bucket. **Metrics and logs both
work end to end today** — the shipped Grafana dashboard shows both, and `log_out` round-trips
`access_in`'s own `syslog.hostname`/`syslog.tag` attributes onto every relayed message
(`../docs/adr/0022-syslog-output.md`), so Loki gets real `host`/`app` stream labels with no extra
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

## What isn't wired yet

Tempo is up, healthy, and provisioned as a Grafana datasource — but `logit` can't write to it yet:

- **Traces → Tempo** need both an `otlp_out` output (declared in config, rejected at validation
  today — no OTLP code exists) and something that actually produces spans (`bench/internal-spans-
  costing`, PR #39, measured the cost of carrying trace context through the pipeline and reverted
  the prototype — nothing emits a span anywhere yet). Tempo's OTLP receiver is up and will accept
  data the moment both exist; `logit.yaml`'s commented-out `trace_out` is the shape it'll take.

## Stopping

```sh
docker compose down        # stop, keep data
docker compose down -v     # stop, wipe all volumes (InfluxDB/Grafana/Loki/Tempo/Alloy/graph state)
```
