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
the demo's front door now; requests flow `haproxy` → `nginx` → `app` (a real Django app).

| Service | URL | What it's for |
|---|---|---|
| Front door (haproxy) | http://localhost:8080 | Start here. Mints the request's W3C `traceparent`, then proxies to `nginx`. |
| Grafana | http://localhost:3000 | Anonymous admin access. Open the "logit" folder for the pre-built dashboard. |
| InfluxDB | http://localhost:8086 | `logit`/`logit-demo-password`. Bucket `metrics`, org `logit`. |
| Loki | http://localhost:3100 | Provisioned as a Grafana datasource; receives `logit`'s own logs directly via `otlp_out`. |
| Tempo | http://localhost:3200 (query), :4317/:4318 (OTLP) | Provisioned as a Grafana datasource; receives `logit`'s own internal spans over OTLP/gRPC. |

`nginx` and `app` (the landing-page app) are internal-only now — reached through `haproxy`,
not published on the host.

`docker compose logs -f logit` shows every decoded event as a `stdio_out` block — the fastest way
to see the pipeline doing something. `self` (`internal`, observing `logit`'s own telemetry) mixes
metric-only and span-only events in one stream, but `trace_only` (`type: has_signal`) drops the
metric-only ones before they ever reach `tempo_out` — Tempo is a traces-only backend and would
reject them. Unlike an earlier `aggregate`-based version of this filter, `has_signal` never
forwards a metrics-only batch at all, so you shouldn't see any `component 'tempo_out'` send
failures in steady state. See
[docs/known-gaps.md](../docs/known-gaps.md)'s "`otlp_out` aborts an entire batch's `send`..." entry
for the full account, including why `trace_only` exists at all — without it, this interaction
doesn't just log a warning, it stops `logit` a minute after startup.

## What's actually flowing

A request crosses three real tiers: **`haproxy` → `nginx` → `app`**
([`haproxy/haproxy.cfg`](haproxy/haproxy.cfg), [`nginx/nginx.conf`](nginx/nginx.conf),
[`app/`](app/) — a real Django project, `gunicorn`-served). `haproxy` mints a W3C `traceparent`
(https://www.w3.org/TR/trace-context/) if the request doesn't already carry one, or reuses an
inbound one; `nginx` relays the trace but mints its *own* span id and forwards a new `traceparent`
carrying it, so `app`'s span parents to nginx's, not straight to haproxy's; `app` reads its own
already-split `otelTraceID`/`otelSpanID` straight off the request span
`opentelemetry-instrumentation-django` creates from the same header.
[`traffic`](compose.yaml) is the demo's traffic source, driving a low-volume request loop through
the whole chain.

Each tier logs its own RFC 3164 + JSON-body access line to its own `syslog_in` listener in
[`logit.yaml`](logit.yaml) — one listener per tier (`haproxy_in`/`nginx_in`/`app_in` on
5140/5141/5142), because `set`'s `resource:` block stamps identity onto a whole *batch*, and three
tiers sharing one listener would interleave into one batch with one wrong `service.name`. Each
tier's chain is `set` (`../docs/adr/operator-declared-resource-attributes.md`) → `json` →
`trace_context` (`../docs/adr/log-record-trace-context.md`), which lifts the trace context onto
`LogRecord.trace` — parsing the `traceparent` header natively, and the well-known `trace.id`/
`span.id`/`trace.flags` attributes each access line also carries
(`../docs/design/data-model.md`'s "Well-known attribute names") — then all three fan into a shared
`stdout` (`stdio_out`) and, filtered down to logs only, a shared `loki_out` (`otlp_out` over HTTP
straight to Loki — no relay service in between,
`../docs/plans/otlp-logs-and-resource-identity.md`). **The haproxy and nginx tiers' `trace_context`
stages also mint a real `SpanRecord` on the same event** (`span:`,
`../docs/adr/trace-context-span-lifting.md`) — the access line's own start/end/duration become a
server span, so those two tiers get a real trace span from their existing log line, no new
telemetry SDK required. Those spans are filtered out and sent to Tempo (see below); the log side of
the same event continues on to the metrics leg — nginx's own chain adds a `scale` step first,
converting `request_time` from seconds to milliseconds so it shares a measurement and a unit with
haproxy's already-millisecond `%Tr` (`../docs/adr/scale-transform.md`) — before each tier's own
`kv_metrics` (`nginx_metrics`/`haproxy_metrics`) fans into a shared `keep` → `aggregate` →
`influxdb_out` tail, told apart in InfluxDB by the `service.name` tag each tier's `set` already
stamped. `logit` also observes its own pipeline via `internal`
(`../docs/design/internal-telemetry.md`) into that same InfluxDB bucket *and*, as real spans, over
OTLP/gRPC into Tempo, one span per node-visit at `span_sample_rate: 1.0` so nothing is thinned out
(`../docs/adr/internal-span-emission-and-deterministic-sampling.md`,
`../docs/adr/hand-rolled-grpc-over-hyper.md`) — alongside haproxy's and nginx's own access-line
spans, sharing that same `tempo_out`.

`app` also has its own real OpenTelemetry request span — but, deliberately, it never goes through
`logit` at all: it's exported over OTLP/HTTP protobuf straight to Tempo's own OTLP receiver
(`demo/tempo/tempo.yaml`'s `otlp.protocols.http`, :4318), wired up in
[`app/gunicorn.conf.py`](app/gunicorn.conf.py)'s `post_fork` hook
(`opentelemetry-instrumentation-django`, `opentelemetry-instrumentation-logging`,
`opentelemetry-exporter-otlp-proto-http`). Not every telemetry leg needs `logit` in front of it,
and this demo shows that honestly rather than routing everything through `logit` just to prove it
can — see `demo/logit.yaml`'s header comment. Because the default W3C propagator extracts
whichever `traceparent` it's handed automatically regardless of where the span is headed, it's
still a genuine *child* of nginx's span, with no code in `app` deciding so.

**All four tiers produce a real trace span and agree on one trace, even though only two of them
(haproxy, nginx) derive theirs from a plain access log via `trace_context`, and only three ever
touch `logit` at all** — that's what the shipped Grafana dashboard shows, side by side: the
`logit.*` InfluxDB panels, a Loki logs panel, and two Tempo traces panels (one scoped to `logit`'s
own internal spans, one to this demo's own request traces), all over the same pipeline. Each
tier's `set` (or, for `app`'s spans, its own OTel resource) stamps a real
`service.name`/`service.namespace` (`haproxy`/`nginx`/`demo-app`, all under `demo`) so Loki gets
real stream labels with no extra `loki.yaml` config, and Loki's `derivedFields` (both in
`grafana/provisioning/datasources/datasources.yaml`) click straight through to the matching Tempo
trace — which contains four real spans (haproxy, nginx, and app's, plus `logit`'s own internal
ones for that request if sampled), arrived at via two different Tempo receivers, not `logit`'s
internal spans alone.

The landing page shows two diagrams. The pipeline one (also at `:8080/graph.svg` directly) is
rendered at startup, not hand-drawn: `graph-dot` runs `logit graph logit.yaml` against the actual
config this stack is running, `graph-svg` pipes that DOT through real Graphviz
([`graph-renderer/Dockerfile`](graph-renderer/Dockerfile)), and `app` serves the result. The other,
[`architecture.dot`](architecture.dot) (also at `:8080/architecture.svg`), is the service topology
one level up — who talks to whom, and what *kind* of traffic (web / logging / tracing / metrics /
query), not which wire protocol — rendered the same way via `arch-svg`, but from a hand-authored
`.dot` file rather than a generated one: there's no single machine-readable source "traffic type"
could be derived from the way the pipeline is derived from `logit.yaml`, so this one can drift
from reality if the topology changes and the file isn't updated alongside it — see its own header
comment. Both are one-shot containers gated with `depends_on: condition:
service_completed_successfully` — expect to see them as `Exited (0)` in `docker compose ps`,
that's them having finished, not crashed. (On `podman-compose` specifically, that condition is
reportedly unimplemented and may be ignored; if so, the page just shows a "not rendered yet"
placeholder until a refresh after the renderer catches up — both SVGs are read fresh on every
request, nothing is cached.)

## What isn't exercised yet

**`otlp_in`** (`crates/logit-inputs/src/otlp.rs`) still ships implemented and tested with nothing
in this stack sending *to* it — `app`'s spans go straight to Tempo instead, by design (see
"What's actually flowing" above), so this isn't an oversight to close so much as a deliberate
choice about where `logit` belongs in the pipeline. If you want to see `otlp_in` exercised with
real traffic, point `app`'s `OTEL_EXPORTER_OTLP_ENDPOINT` (`demo/compose.yaml`) at `http://logit:4318`
instead of `http://tempo:4318`, and re-add a `tempo_out` source for it in `demo/logit.yaml` — that
was this demo's shape until this rework; it's a small, well-understood change to reverse.

What's left client-side: browser-side tracing is sketched, not built, in
[docs/plans/demo-tracing-stack.md](../docs/plans/demo-tracing-stack.md)'s workstream C
([docs/plans/browser-tracing.md](../docs/plans/browser-tracing.md)) — same-origin OTLP export
through `haproxy` needs no `logit` change, but a real OTel browser SDK needs `otlp_in` to accept
OTLP/JSON, which it doesn't today ([docs/known-gaps.md](../docs/known-gaps.md)).

## Stopping

```sh
docker compose down        # stop, keep data
docker compose down -v     # stop, wipe all volumes (InfluxDB/Grafana/Loki/Tempo/graph state)
```
