# `logit` demo stack

A self-contained stack for trying `logit`: no Rust toolchain, no `script/*`, and no knowledge of the
rest of this repo. Real traffic crosses a three-hop request path (haproxy → nginx → app) tied
together by a W3C trace context, and `logit` carries the resulting logs, metrics, and traces into
Loki, VictoriaMetrics, and Tempo, with a pre-built Grafana dashboard over all three.
[docs/plans/demo-stack.md](../docs/plans/demo-stack.md) explains why the demo exists, and
[docs/plans/demo-tracing-stack.md](../docs/plans/demo-tracing-stack.md) covers its tracing design
and what's still to come.

**This is a demo, not a production example.** It shows a large portion of `logit`'s functionality
in a way that's quick to spin up, and takes several shortcuts that don't belong in a real
deployment: anonymous Grafana admin, hardcoded secrets, root privileges for one tier, and more.
The landing page's "This demo is not a production example" section lists exactly which ones and
why.

## Quick start

**Prerequisite: native Linux Docker Engine.** The `logit` service reads `nginx`'s and `redis`'s
logs straight off the host, which only works there. See
[Why `logit` runs as root](#why-logit-runs-as-root).

```sh
cd demo
docker compose up --build
```

From the repo root, `script/demo` runs the same `up --build` against `demo/compose.yaml`; pass
other compose arguments to it, such as `script/demo logs -f logit` or `script/demo down -v`.

The first run builds `logit`'s production image from source (vendored LuaJIT, no dependency-layer
caching by design — see `../Dockerfile`'s comment), so expect several minutes before anything
appears. Later runs reuse the built image. `app`'s image has a client-side build step
(`app/Dockerfile`'s `bundle` stage, `docs/adr/browser-tracing-sdk.md`), so the first build needs
npm registry access alongside PyPI, crates.io, and apt, unlike every other image in this stack.

## Check that it's working

1. **Open http://localhost:8080.** This is `haproxy`, the demo's front door. It serves a small page
   that links to Grafana with get-started instructions and shows this stack's own pipeline,
   rendered live.
2. **Watch `logit` decode events.** `docker compose logs -f logit` shows every decoded event as a
   `stdio_out` block — the fastest way to see the pipeline doing something.
3. **Open Grafana at http://localhost:3000** and open the "logit" folder for the pre-built
   dashboard.

In `docker compose ps`, expect `graph-dot`, `graph-svg`, and `arch-svg` to show `Exited (0)`.
They're one-shot renderers that have finished, not crashed (see
[Pipeline and architecture diagrams](#pipeline-and-architecture-diagrams)).

## Services

| Service | URL | What it's for |
|---|---|---|
| Front door (haproxy) | http://localhost:8080 | Start here. Mints the request's W3C `traceparent`, then proxies to `nginx`. |
| Grafana | http://localhost:3000 | Anonymous admin access. Open the "logit" folder for the pre-built dashboard. |
| VictoriaMetrics | http://localhost:8428/vmui | No login. Receives `logit`'s metrics over Prometheus remote-write; `vmui` runs PromQL/MetricsQL queries directly. |
| Loki | internal only, query it through Grafana | Provisioned as a Grafana datasource; receives `logit`'s own logs directly via `otlp_out`. |
| Tempo | :4317/:4318 (OTLP ingest only), query it through Grafana | Provisioned as a Grafana datasource; receives `logit`'s own internal spans over OTLP/gRPC. |

These services are internal-only, reached through other services rather than published on the
host:

- `nginx` and `app` (the landing-page app), reached through `haproxy`.
- `postgres`, `app`'s own database (`docs/plans/demo-richer-traces.md`), reached only from
  `app`/`worker` and from `logit`'s own `postgres_in`, which tails its jsonlog.
- `redis` and `worker`: Celery's broker and the background worker process that consumes from it
  (same doc, workstream D).

## What's actually flowing

The shipped Grafana dashboard shows all of the following side by side over the same pipeline: the
`web.*` and `logit.*` VictoriaMetrics panels, a Loki logs panel, and two Tempo traces panels (one
scoped to `logit`'s own internal spans, one to this demo's own request traces).

### Request path

A request crosses three real tiers: **`haproxy` → `nginx` → `app`**
([`haproxy/haproxy.cfg`](haproxy/haproxy.cfg), [`nginx/nginx.conf`](nginx/nginx.conf),
[`app/`](app/) — a real Django project, `gunicorn`-served).

- `haproxy` mints a W3C `traceparent` (https://www.w3.org/TR/trace-context/) if the request
  doesn't already carry one, or reuses an inbound one.
- `nginx` relays the trace but mints its *own* span id and forwards a new `traceparent` carrying
  it, so `app`'s span parents to nginx's, not straight to haproxy's.
- `app` reads its own already-split `otelTraceID`/`otelSpanID` straight off the request span that
  `opentelemetry-instrumentation-django` creates from the same header.

[`traffic`](compose.yaml) is the demo's traffic source, driving a low-volume request loop through
the whole chain.

### Logs from each tier

Each tier logs its own JSON-body access line to its own listener in [`logit.yaml`](logit.yaml).
There's one listener per tier because `set`'s `resource:` block stamps identity onto a whole
*batch*; three tiers sharing one listener would interleave into one batch with one wrong
`service.name`.

- `haproxy` and `app` ship theirs as RFC 3164 to a `syslog_in` (`haproxy_in`/`app_in`,
  5140/5142).
- `nginx` logs to stdout only. `nginx_in` is a `docker_in` that tails its container's Docker
  json-file log directly off the host (`../docs/adr/file-tailing-and-docker-json-logs.md` — this
  demo is that ADR's end-to-end proof). The container's `error_log` shares the same json-file log,
  told apart only by `log.iostream`, so an inline `lua` stage, `nginx_stdout`, drops anything that
  isn't `stdout` before it reaches `nginx_identity`. That ADR's "Alternatives considered" covers
  why this is a Lua stage rather than a `docker_in` config field.

Each tier's chain is then `set` (`../docs/adr/operator-declared-resource-attributes.md`) → `json`
→ `trace_context` (`../docs/adr/log-record-trace-context.md`). `trace_context` lifts the trace
context onto `LogRecord.trace`, parsing the `traceparent` header natively as well as the
well-known `trace.id`/`span.id`/`trace.flags` attributes each access line also carries
(`../docs/design/data-model.md`'s "Well-known attribute names"). All three tiers then fan into a
shared `stdout` (`stdio_out`) and, filtered down to logs only, a shared `loki_out` (`otlp_out`
over HTTP straight to Loki, with no relay service in between,
`../docs/plans/otlp-logs-and-resource-identity.md`).

Each tier's `set` (or, for `app`'s spans, its own OTel resource) stamps a real
`service.name`/`service.namespace` (`haproxy`/`nginx`/`demo-app`, all under `demo`), so Loki gets
real stream labels with no extra `loki.yaml` config. `nginx`'s resource also carries
`container.id`/`container.name`/`container.image.name`/`container.image.tag` from `docker_in`
itself. They survive `nginx_identity`'s `set` untouched, because `set`'s `map_resource` overlays
onto the resource it's handed rather than replacing it, and show up as Loki structured metadata on
that tier's log lines. Loki's `derivedFields` (in
`grafana/provisioning/datasources/datasources.yaml`) click straight through to the matching Tempo
trace.

### Spans from access logs

**The haproxy and nginx tiers' `trace_context` stages also mint a real `SpanRecord` on the same
event** (`span:`, `../docs/adr/trace-context-span-lifting.md`). The access line's own
start/end/duration become a server span, so those two tiers get a real trace span from their
existing log line, with no new telemetry SDK.

Both tiers log raw OTel semconv attribute names (haproxy's dashed, because `%{+json}o` can't write
a `.`). An `http_access` stage ahead of each `trace_context` (`haproxy_http`/`nginx_http`,
`../docs/adr/http-access-normalization.md`) normalizes them and derives a bounded `http.route`
from a shared `routes:` block. So on both the `haproxy` and `nginx` services, **the server spans
are named `GET /work`, `GET /{probe}` (`/health`), `GET /{asset}` (`/graph.svg`), `GET /`,
`GET /boom`, and `GET /{other}` (`/missing`)**, never the raw path.

Their `span.status` follows semconv: **error** on a 5xx (`/boom`, `/work`'s occasional `503`), and
**unset — not `ok`** — on anything else, a `404` included. A TraceQL `{status=ok}` query finds none
of them.

These spans are filtered out and sent to Tempo through `tempo_out`. The log side of the same event
continues on to the metrics leg.

### Metrics

`http_access` has already put both tiers' durations in seconds (`http.request.duration_s`), so
neither chain has a `scale` step. Each tier's own `kv_metrics` (`nginx_metrics`/`haproxy_metrics`)
fans into a shared `keep` → `aggregate` → `prometheus_out` tail. `victoria_out` sends Prometheus
remote-write 1.0, zstd-compressed, to VictoriaMetrics. VictoriaMetrics tells the tiers apart by the
`service_name` label, which is the `service.name` each tier's `set` stamped.

Two things differ from the metric names in `logit.yaml`, and the dashboard's PromQL uses the
Prometheus spelling:

- Dots become underscores, and a counter gains `_total`: `web.requests` is `web_requests_total`.
- The `aggregate` runs with `temporality: cumulative`, because `prometheus_out` skips a delta
  counter. A distribution such as `web.request_time` becomes a summary:
  `web_request_time{quantile="0.5"}` through `{quantile="0.99"}`, one per 10-second window.

### `logit`'s own telemetry

`logit` also observes its own pipeline via `internal` (`../docs/design/internal-telemetry.md`).
It writes into the same VictoriaMetrics *and*, as real spans, over OTLP/gRPC into Tempo: one span
per node-visit at `span_sample_rate: 1.0`, so nothing is thinned out
(`../docs/adr/internal-span-emission-and-deterministic-sampling.md`,
`../docs/adr/hand-rolled-grpc-over-hyper.md`). These share the same `tempo_out` as haproxy's and
nginx's access-line spans.

`self` (`internal`) mixes metric-only and span-only events in one stream. `trace_only`
(`type: has_signal`) drops the metric-only ones before they reach `tempo_out`, because Tempo is a
traces-only backend and would reject them. `has_signal` never forwards a metrics-only batch at
all, so you shouldn't see any `component 'tempo_out'` send failures in steady state. Without
`trace_only`, this interaction doesn't just log a warning: it stops `logit` a minute after
startup. See [docs/known-gaps.md](../docs/known-gaps.md)'s "`otlp_out` aborts an entire batch's
`send`..." entry for the full account.

### The app's own traces

`app` also has its own real OpenTelemetry request span, but it deliberately never goes through
`logit`. It's exported over OTLP/HTTP protobuf straight to Tempo's own OTLP receiver
(`demo/tempo/tempo.yaml`'s `otlp.protocols.http`, :4318), wired up in
[`app/gunicorn.conf.py`](app/gunicorn.conf.py)'s `post_fork` hook
(`opentelemetry-instrumentation-django`, `opentelemetry-instrumentation-logging`,
`opentelemetry-exporter-otlp-proto-http`). Not every telemetry leg needs `logit` in front of it,
and this demo shows that honestly rather than routing everything through `logit` just to prove it
can — see `demo/logit.yaml`'s header comment. The default W3C propagator extracts whichever
`traceparent` it's handed, regardless of where the span is headed, so the span is still a genuine
*child* of nginx's span, with no code in `app` deciding so.

**All four tiers produce a real trace span and agree on one trace, even though only two of them
(haproxy, nginx) derive theirs from a plain access log via `trace_context`, and only three ever
touch `logit` at all.** The Tempo trace a Loki line links to contains four real spans (haproxy,
nginx, and app's, plus `logit`'s own internal ones for that request if sampled), arriving through
two different Tempo receivers, not `logit`'s internal spans alone.

### Richer traces: `/work` and `/boom`

Without them, every request would take the same chain with the same latency and the same `200`.
Two routes on `app` exist purely to give the traces real shape
(`docs/plans/demo-richer-traces.md`):

- `/work` sleeps a jittered amount and occasionally answers a real `503`.
- `/boom` always fails with an uncaught exception, so its OTel span carries a real `exception`
  event. `trace_context` never mints span *events* on the spans it lifts from a plain access log
  line, so this is the only path to one anywhere in this demo.

`/work` also calls back into **`nginx`, not `haproxy`** (`pages/views.py`'s `INNER_URL`). The
request deliberately re-enters the chain partway rather than from the front door, so the resulting
`nginx` server span (still `logit`-minted, from the same Docker json-file log) becomes a genuine
subtree under `app`'s own `requests` CLIENT span rather than a second top-level branch. This has
two side effects:

- `nginx`'s `web.requests` counter runs roughly double `haproxy`'s, because it serves this inner
  hop too.
- Its `server.address` tag gains a second value, `nginx` itself, from `proxy_set_header Host
  $host` on a request whose `Host` genuinely is `nginx`.

`traffic`'s loop (`compose.yaml`) drives all of this, weighted toward `/work`, because a
guaranteed `500` on every cycle would swamp the dashboard's error panel.

### Postgres

`/work` also writes to, and counts rows in, a real Postgres (`docs/plans/demo-richer-traces.md`'s
workstream C) through `app`'s own `psycopg` driver. It's instrumented the same way `requests` is,
so every statement gets a real CLIENT span sent straight to Tempo, the app's usual path.

Postgres's own log line for that statement still reaches Loki carrying the *same* trace id, with
no SDK on Postgres's side:

1. `opentelemetry-instrumentation-psycopg`'s sqlcommenter (`enable_commenter=True`,
   `app/demoproj/telemetry.py`) appends a trailing SQL comment carrying `traceparent='...'` to the
   statement text.
2. Postgres logs the whole statement verbatim (`log_min_duration_statement=0`).
3. `postgres_trace_lift`, a `regex` stage in `demo/logit.yaml` (docs/adr/regex-transform.md),
   extracts that substring and hands it to `trace_context` exactly as it would a real HTTP header.

`postgres_in` is a `tail_in` over a plain file, not a `docker_in` container log. Postgres's jsonlog
rotates into a fresh `postgresql-<timestamp>.json` file periodically, so `postgres_in`'s `paths:`
glob does real, live discovery work rather than tailing one static file for the life of the stack.
It's checkpointed on the same `logit_state` volume `nginx_in` uses.

### Celery worker and Redis

`/work`'s last step (`docs/plans/demo-richer-traces.md`'s workstream D) hands off to a real
background worker over Redis rather than doing everything inline: `.delay()` enqueues a task and
returns immediately, well before that task runs. `opentelemetry-instrumentation-celery` turns that
into a real Celery PRODUCER span in `app` (parented to the request that called `.delay()`, like
`requests`'s CLIENT span) and a real CONSUMER span in the separate `worker` process that picks the
task up. `opentelemetry-instrumentation-redis` covers the broker traffic in between.

Both spans, and the task's own `psycopg` write, go straight to Tempo like every other app-tier
span. `worker`'s one log line per task reaches Loki through `logit`'s `worker_in`, the fourth
per-tier `syslog_in`. It's a sibling of `app_in`, not a shared listener, for the same
resource-identity reason every tier has its own.

The result is a trace shape nothing else in this demo produces: spans that keep arriving in Tempo
*after* the HTTP response that started them has reached the client. `app`'s gunicorn workers and
the `worker` service run the identical `TracerProvider`/instrumentor setup
(`app/demoproj/telemetry.py`), each with its own `service.name` from `OTEL_SERVICE_NAME`
(`demo-app`/`demo-worker`, `compose.yaml`).

Redis's own server log reaches Loki too, as `service.name: redis`. `redis_in` is a second
`docker_in`, tailing the `logit-demo-redis` container's json-file log the same way `nginx_in`
tails nginx's, and `redis_parse` (a `regex` stage) splits `process.pid`, the one-letter
`redis.role`, and the one-character `redis.level` off Redis's plain-text line format. Redis has no
per-command log, so these are startup, background-save, and warning lines only, with no trace id.
`demo-app`'s and `demo-worker`'s `opentelemetry-instrumentation-redis` CLIENT spans in Tempo
already cover the broker's query traffic (`LPUSH`, `LLEN`, ...).

### `otlp_in` from the browser

`browser_in` (`demo/logit.yaml`) is a real `otlp_in` listener
(`crates/logit-inputs/src/otlp.rs`), reachable through `haproxy`'s own `/v1/*` route
(`demo/haproxy/haproxy.cfg`) rather than published directly, because a browser (or `curl`)
reaching it same-origin is the whole point. Unlike every syslog, docker, and tail input in this
stack, no `set` stage stamps a resource in front of it, because a real OTel SDK sets its own
`Resource`. `app`'s own spans still go straight to Tempo rather than through `browser_in`, by
choice. Every landing-page load sends `browser_in` a real `documentLoad` batch (see
[Browser-side tracing](#browser-side-tracing)); it was first verified with a manual OTLP/JSON
POST.

### Pipeline and architecture diagrams

The landing page shows two diagrams:

- **The pipeline**, also at `:8080/graph.svg`, is rendered at startup, not hand-drawn:
  `graph-dot` runs `logit graph logit.yaml` against the config this stack is running, `graph-svg`
  pipes that DOT through real Graphviz ([`graph-renderer/Dockerfile`](graph-renderer/Dockerfile)),
  and `app` serves the result.
- **The service topology**, [`architecture.dot`](architecture.dot), also at
  `:8080/architecture.svg`, shows who talks to whom and what *kind* of traffic (web / logging /
  tracing / metrics / query), not which wire protocol. `arch-svg` renders it the same way, but from
  a hand-authored `.dot` file: there's no single machine-readable source "traffic type" could be
  derived from, the way the pipeline is derived from `logit.yaml`. It can drift from reality if the
  topology changes and the file isn't updated alongside it — see its own header comment.

The renderers are one-shot containers sequenced with `depends_on: condition:
service_completed_successfully`, which is why `docker compose ps` shows them as `Exited (0)`. On
`podman-compose`, that condition is reportedly unimplemented and may be ignored. If so, the page
shows a "not rendered yet" placeholder until you refresh after the renderer catches up; both SVGs
are read fresh on every request, and nothing is cached.

### Why `logit` runs as root

**The `logit` service runs as root, and reading `nginx`'s and `redis`'s logs this way only works on
native Linux Docker Engine.** On a stock install, Docker's per-container state directories are
`root:root 0710` and the log files `root:root 0640`, so `docker_in` needs both root and a
read-only bind mount of `/var/lib/docker/containers` (`demo/compose.yaml`'s `logit` service). This
cost is specific to reading the json-file driver directly rather than through the docker
socket/API (see the ADR's "Root privileges" section), and only this one service pays it.

Other runtimes don't match the default `root:` this demo leaves unset: rootless Docker uses
`~/.local/share/docker/containers`, Docker Desktop's paths live inside its VM, and Podman uses a
different log format entirely.

## Browser-side tracing

The real `@opentelemetry/sdk-trace-web` SDK runs on the landing page itself
(`app/browser/telemetry.js`, bundled by an `esbuild` stage in `app/Dockerfile`, this demo's only
client-side build step; see the "Quick start" note above). This closes out
[docs/plans/browser-tracing.md](../docs/plans/browser-tracing.md)'s workstream C.
[ADR `browser-tracing-sdk`](../docs/adr/browser-tracing-sdk.md) has the implementation decisions,
including two places the real SDK's behavior didn't match that plan's original, spec-derived
expectations.

- A page load produces a `documentLoad` span, parented (confirmed empirically — not linked, see
  the ADR) to the request's own server span via a `<meta name="traceparent">` tag
  (`pages/context_processors.py`).
- Each sub-resource gets a `resourceFetch` span, *linked* to the real
  `haproxy_trace`/`nginx_trace`-minted span that served it via HAProxy's `Server-Timing` header.
- The **Call /work via fetch()** button on the page exercises `instrumentation-fetch`'s context
  propagation the same way: that fetch span becomes a real parent of `app`'s own Django request
  span for the call it triggers.

## Stopping

```sh
docker compose down        # stop, keep data
docker compose down -v     # stop, wipe all volumes (VictoriaMetrics/Grafana/Loki/Tempo/graph/logit state)
```

`docker compose down -v` also wipes `nginx_in`'s checkpoint in the `logit_state` volume, along with
everything else this stack persists.
