# `logit` demo stack

A self-contained stack for trying `logit`: no Rust toolchain, no `script/*`, no knowledge of the
rest of this repo. See [docs/plans/demo-stack.md](../docs/plans/demo-stack.md) for why it
exists, and [docs/plans/demo-tracing-stack.md](../docs/plans/demo-tracing-stack.md) for its
tracing rework: a real three-hop request path (haproxy → nginx → app) tied together by a W3C
trace context, plus what's still to come.

## Quick start

```sh
cd demo
docker compose up --build
```

The first run builds `logit`'s production image from source (vendored LuaJIT, no dependency-layer
caching by design — see `../Dockerfile`'s comment) — expect several minutes before anything
appears. Every run after that reuses the built image.

Once it's up, **start at http://localhost:8080** — a small page that links to Grafana with
get-started instructions and shows this stack's own pipeline, rendered live. That's `haproxy`,
the demo's front door now; requests flow `haproxy` → `nginx` → `hello`.

| Service | URL | What it's for |
|---|---|---|
| Front door (haproxy) | http://localhost:8080 | Start here. Mints the request's W3C `traceparent`, then proxies to `nginx`. |
| Grafana | http://localhost:3000 | Anonymous admin access. Open the "logit" folder for the pre-built dashboard. |
| InfluxDB | http://localhost:8086 | `logit`/`logit-demo-password`. Bucket `metrics`, org `logit`. |
| Loki | http://localhost:3100 | Provisioned as a Grafana datasource; receives `logit`'s own logs directly via `otlp_out`. |
| Tempo | http://localhost:3200 (query), :4317/:4318 (OTLP) | Provisioned as a Grafana datasource; receives `logit`'s own internal spans over OTLP/gRPC. |

`nginx` and `hello` (the landing-page app) are internal-only now — reached through `haproxy`,
not published on the host.

`docker compose logs -f logit` shows every decoded event as a `stdio_out` block — the fastest way
to see the pipeline doing something. You may occasionally see a `component 'tempo_out': batch
dropped after a permanent send failure` warning (at most once a minute) — real and harmless, not a
sign anything is broken: `trace_windowed` (`logit.yaml`) periodically flushes a metrics-only batch
that Tempo (a traces-only backend) rejects, the same way it would reject any OTLP metrics request;
the far more frequent traces-only batches in between all succeed. See
[docs/known-gaps.md](../docs/known-gaps.md)'s "`otlp_out` aborts an entire batch's `send`..." entry
for the full account, including why `trace_windowed` exists at all — without it, this interaction
doesn't just log a warning, it stops `logit` a minute after startup.

## What's actually flowing

A request now crosses three real tiers: **`haproxy` → `nginx` → `hello`**
([`haproxy/haproxy.cfg`](haproxy/haproxy.cfg), [`nginx/nginx.conf`](nginx/nginx.conf),
[`hello/app.py`](hello/app.py)). `haproxy` mints a W3C `traceparent`
(https://www.w3.org/TR/trace-context/) if the request doesn't already carry one, or reuses an
inbound one; `nginx` and `hello` relay it and split it into plain `trace_id`/`span_id`/
`trace_flags` fields — `logit` has no `traceparent` parser by design
(`crates/logit-transforms/src/trace_context.rs`'s doc comment explains why decimal-only flags
matter), so each tier does the split itself before logging. [`traffic`](compose.yaml) is now the
demo's traffic source, driving a low-volume request loop through the whole chain — `hello`'s old
in-process synthetic loop is gone, since it fabricated syslog lines directly and never crossed
`haproxy`/`nginx`, carrying no trace context.

Each tier logs its own RFC 3164 + JSON-body access line to its own `syslog_in` listener in
[`logit.yaml`](logit.yaml) — one listener per tier (`haproxy_in`/`nginx_in`/`app_in` on
5140/5141/5142), because `set`'s `resource:` block stamps identity onto a whole *batch*, and three
tiers sharing one listener would interleave into one batch with one wrong `service.name`. Each
tier's chain is `set` (`../docs/adr/operator-declared-resource-attributes.md`) → `json` →
`trace_context` (`../docs/adr/log-record-trace-context.md`), which lifts the split trace fields
onto `LogRecord.trace` — then all three fan into a shared `stdout` (`stdio_out`) and a shared
`loki_out` (`otlp_out` over HTTP straight to Loki — no relay service in between,
`../docs/plans/otlp-logs-and-resource-identity.md`). The nginx tier alone continues on to the
metrics leg (`kv_metrics` → `keep` → `aggregate` → `influxdb_out`) — plus `logit` observing its own
pipeline via `internal` (`../docs/design/internal-telemetry.md`) into that same InfluxDB bucket
*and*, as real spans, over OTLP/gRPC into Tempo, one span per node-visit at `span_sample_rate: 1.0`
so nothing is thinned out (`../docs/adr/internal-span-emission-and-deterministic-sampling.md`,
`../docs/adr/hand-rolled-grpc-over-hyper.md`).

**All three signals work end to end, and now carry a correlated trace id across all three tiers**
— that's what the shipped Grafana dashboard shows, side by side: the `logit.*` InfluxDB panels, a
Loki logs panel, and a Tempo traces panel, all over the same pipeline. Each tier's `set` stamps a
real `service.name`/`service.namespace` (`haproxy`/`nginx`/`demo-hello`, all under `demo`) so Loki
gets real stream labels with no extra `loki.yaml` config, and Loki's `derivedFields` (both in
`grafana/provisioning/datasources/datasources.yaml`) click straight through to the matching Tempo
trace — see "What isn't exercised yet" below for what that trace contains today.

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

**`otlp_in`** (`crates/logit-inputs/src/otlp.rs`) ships implemented and tested, but nothing in this
stack sends *to* it yet — `demo/logit.yaml` only ever uses `otlp_out`, as a client. The Tempo trace
a log line's "View trace" link resolves to today therefore only ever contains `logit`'s own
internal spans, not a real application span. Closing that — replacing `hello` with a real
framework (Django) that exports genuine OTel spans into `otlp_in` — is
[docs/plans/demo-tracing-stack.md](../docs/plans/demo-tracing-stack.md)'s workstream B. Browser-side
tracing is sketched, not built, in that plan's workstream C
([docs/plans/browser-tracing.md](../docs/plans/browser-tracing.md)).

## Stopping

```sh
docker compose down        # stop, keep data
docker compose down -v     # stop, wipe all volumes (InfluxDB/Grafana/Loki/Tempo/graph state)
```
