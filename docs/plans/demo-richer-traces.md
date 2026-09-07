---
created: 2026-09-07
updated: 2026-09-07
---

# Enabling plan: richer traces in the demo stack

## Context

`demo/` produces exactly one trace shape, over and over: a four-span straight line (`haproxy` →
`nginx` → `demo-app`, plus `logit`'s own internal node-visit spans), every request a 200, every
request roughly the same latency. The span model `logit` implements
(`crates/logit-core/src/span.rs`) is far richer than what the demo exercises — `SpanKind` is never
anything but `Server`, `SpanStatus` never anything but `Unset`, `events`/`links` always empty. Two
shipped Grafana panels ("Loki: 5xx lines/sec", the `web.request_time` p50/p99 pair) plot nothing or
a flat line because `demo/compose.yaml`'s `traffic` loop never produces a 5xx and never varies
latency.

The goal: a demo whose Tempo waterfall is worth opening — a real tree rather than a chain, with
error status, latency spread, a database hop, an internal HTTP hop that re-enters the stack, and
asynchronous work that outlives the response.

Two decisions taken up front:

- **New spans come from the Python SDK and go straight to Tempo's OTLP/HTTP receiver**, exactly as
  `demo-app`'s request span already does. `demo/logit.yaml`'s header comment's "not every
  telemetry leg needs `logit` in front of it" stance is preserved; `otlp_in` stays unexercised.
- **The app's outbound HTTP call targets `nginx`, not `haproxy`**, so the inner hop takes a
  *different* path than the outer one — and, usefully, that inner hop's span is still one `logit`
  mints itself from nginx's Docker json-file log. `logit` stays in the middle of the new subtree
  without any span being re-routed through it.

**No `logit` code changes anywhere in this plan** — every capability it uses already ships.

## Workstream dependency graph

`A → B → C → D`. Each is one PR, each independently valuable and shippable on its own.

---

## A. Latency and error variety

**Status: landed.**

The cheapest change and the biggest visual win: nothing new to run, and it lights up two dashboard
panels and `SpanStatus::Error` on spans `logit` mints itself.

**Files:**

- `demo/app/pages/views.py` — two new views. `work` (the composite route B/C/D keep extending) does
  a jittered `time.sleep` and returns 503 on a small fraction of requests. `boom` raises an
  uncaught exception, so Django returns 500 *and* the SDK span carries `status=ERROR` with a real
  exception span event — the only path to `SpanRecord.events` in this demo, since `trace_context`
  always mints `events: Vec::new()`.
- `demo/app/demoproj/urls.py` — registers `/work`, `/boom`.
- `demo/app/pages/templates/pages/index.html` — links both.
- `demo/compose.yaml` — extends `traffic`'s path list, weighting `/work` so a 500 doesn't dominate.
- `demo/grafana/dashboards/logit-internal.json` — one new Tempo panel filtered to error traces.

**Done when:** `web.request_time`'s p50/p99 separate, "Loki: 5xx lines/sec" is non-empty, and a
Tempo trace for a `/boom` request shows `span.status=error` on the haproxy/nginx spans.

**Verified live** (2026-09-07), `script/demo up --build` through `down -v`: haproxy's/nginx's
`stdio_out` blocks show `kind=server status=error` on `/boom`, sharing one `trace_id` with a
real `demo-app` span whose OTel `events` carry a genuine `exception` event
(`exception.type=RuntimeError`, full traceback) — pulled straight from Tempo's own `/api/traces`
endpoint. The new dashboard panel's TraceQL query (`{status=error}`) returns those traces via
Grafana's own datasource proxy. InfluxDB's `web.request_time` now spans ~3ms to ~210ms across
windows (previously flat) with `status` tags across `200`/`404`/`500`/`503`, confirming both
previously-empty panels now have real data.

---

## B. Outbound HTTP, back in through nginx

**Status: landed.**

- `demo/app/requirements.txt` — `requests`, `opentelemetry-instrumentation-requests`.
- `demo/app/gunicorn.conf.py` — `RequestsInstrumentor().instrument()`.
- `demo/app/pages/views.py` — `work` calls `INNER_URL`; a new `inner` view.
- `demo/compose.yaml` — `INNER_URL: http://nginx/inner`, `WEB_CONCURRENCY` raised to 4 (two
  concurrent in-flight `/work` requests each hold a worker while blocking on `/inner`, which needs
  a worker of its own — a self-deadlock at 2).

**Done when:** one `/work` trace shows `demo-app` → `requests` CLIENT → a second, logit-minted
`nginx` server span → `demo-app`'s `/inner` span.

**Verified live** (2026-09-07), `script/demo up --build` through `down -v`: pulled a `/work` trace
straight from Tempo's `/api/traces` and confirmed the full six-span chain by parent id — haproxy →
nginx → `demo-app "GET work"` (server) → `demo-app "GET"` (CLIENT, `requests`) → nginx (server, a
*second* nginx span, logit-minted from the same Docker json-file log) → `demo-app "GET inner"`
(server). `WEB_CONCURRENCY: 4` confirmed via `gunicorn`'s own startup log (4 workers booted); no
deadlock across ten back-to-back `/work` requests. `web.requests` in InfluxDB: nginx's total (56)
now exceeds haproxy's (39) over the same window, and the `host` tag carries both `haproxy:8080`
and a genuine `nginx` value, exactly as `demo/README.md` now documents.

---

## C. Postgres, and the demo's first `tail_in`

**Status: landed.**

- `demo/compose.yaml` — `postgres:17-alpine`, `jsonlog` destination, `log_min_duration_statement:
  0`.
- `demo/logit.yaml` — `postgres_in` (`tail_in`, glob over the jsonlog directory, checkpointed) →
  `postgres_identity` (`set` — mandatory, `tail_in` stamps no resource of its own) →
  `postgres_json` → inline `lua` lifting the sqlcommenter `traceparent` → `trace_context` → the
  shared `stdout`/`loki_out` tail.
- `demo/app/requirements.txt` — `psycopg[binary]`, `opentelemetry-instrumentation-psycopg`
  (`enable_commenter=True`).
- `demo/app/demoproj/settings.py` — a real `DATABASES` block; `pages/models.py` + migrations.
- `demo/compose.yaml` — one-shot `app-migrate`, same pattern as `graph-dot`/`graph-svg`.

**Done when:** a postgres log line in Loki carries the real `trace_id` of the request whose
psycopg CLIENT span issued it, and `logit.input.files.open` is nonzero for this tier.

**Verified live** (2026-09-07), `script/demo up --build` through `down -v`, including the exact
mechanics this workstream was uncertain about (verified against a real, throwaway
`psycopg`+`opentelemetry-instrumentation-psycopg` + Postgres 17 probe before touching
`demo/logit.yaml`): `enable_commenter=True` does append `traceparent='<header>'` to every
statement (alongside four other, ignored keys, sorted so `traceparent` isn't reliably last —
`postgres_trace_lift`'s regex doesn't assume position), and `log_min_duration_statement=0` logs it
verbatim. Confirmed against the real stack: a `/work` request's `trace_id` appears identically on
haproxy's, both nginx hops', `demo-app`'s, *and* postgres's own `INSERT`/`SELECT` log lines
(cross-checked by grepping `logit`'s `stdio_out` for one trace id across all five). Loki's own
`query_range` API shows `trace_id` as a genuine stream label (not just a body match) on the
postgres stream, confirming the derived-field click-through works unmodified. `app-migrate`
applied its migration cleanly (after one real fix below); `logit.input.files.open{component:
postgres_in}` read `3`, and Postgres's own `log_rotation_age: 5min` genuinely rotated to a new
`postgresql-<timestamp>.json` file mid-session, which `postgres_in`'s glob picked up with no
restart -- real live proof this is directory discovery, not a single static file. `arch-svg`
exited 0 against the edited `architecture.dot` (new `postgres` node/edges), and both `graph.svg`
and `architecture.svg` served 200 through the landing page.

**One real bug, found only by running the stack:** `app-migrate` crashed with
`ValueError: Unable to configure handler 'access_syslog'` -- `manage.py`'s `django.setup()` loads
`LOGGING` (and so resolves the syslog handler's `logit` hostname) regardless of which management
command runs, and `app-migrate` had no dependency on `logit` at all, so its DNS alias sometimes
didn't exist yet. Fixed by adding `depends_on: logit: condition: service_started` to
`app-migrate`, the identical dependency (and identical reasoning) `app` itself already has.

---

## D. Async work: Redis + a Celery worker

**Status: landed.**

- `demo/compose.yaml` — `redis:7-alpine`; a `worker` service reusing `logit-demo-app:latest` (no
  second `build:`), running `celery worker`.
- `demo/app/requirements.txt` — `celery`, `redis`, `opentelemetry-instrumentation-celery`,
  `opentelemetry-instrumentation-redis`.
- `demo/app/demoproj/telemetry.py` (new) — the provider/instrumentor setup factored out of
  `gunicorn.conf.py`'s `post_fork`, called from both it and Celery's `worker_process_init`
  (prefork; the export thread does not survive `fork()`, same reasoning `gunicorn.conf.py`
  documents).
- `demo/app/pages/tasks.py` (new); `demo/app/pages/views.py`'s `work` enqueues it.
- `demo/logit.yaml` — `worker_in` (`syslog_in`, `:5143`), identical shape to `app_in`.

**Done when:** `/work`'s trace gains Redis CLIENT + Celery PRODUCER spans, and the worker's
CONSUMER span and its own DB span arrive under the same trace after the response has already
returned.

**Verified live** (2026-09-07), `script/demo up --build` through `down -v`: pulled a `/work` trace
straight from Tempo and confirmed all 12 spans by parent id — the richest trace anywhere in this
demo. Notably, `opentelemetry-instrumentation-celery` produces a genuine cross-process
**parent-child** relationship, not a `SpanLink` as this plan originally assumed: `demo-worker`'s
CONSUMER span (`run/pages.background_work`) parents directly to `demo-app`'s PRODUCER span
(`apply_async/pages.background_work`), which itself parents to the `/work` request's own server
span, with a Redis `LPUSH` CLIENT span (the broker publish) as the PRODUCER span's other child and
the worker's own `psycopg` INSERT nested under its CONSUMER span. Simpler than a link, and no less
correct. Real temporal proof the trace outlives its response: cross-checking one trace id across
`logit`'s `stdio_out` showed the worker's own `INSERT` landing ~0.65s *after* `/work`'s haproxy
access line had already been logged — the response had already reached the client by then. The
worker's log line reaches Loki with `trace_id` as a genuine stream label, same as every other
tier. Ten back-to-back `/work` requests and six `/boom` requests produced zero panics/tracebacks
across `logit`, `app`, and `worker`, and `docker compose ps` showed all twelve containers stable
(no restart loops) throughout.

---

## Cross-cutting, at the end of each workstream

- `demo/README.md` — service table, "What's actually flowing", drops `tail_in` from "What isn't
  exercised yet" once C lands (`otlp_in` stays).
- `demo/architecture.dot` — new nodes/edges; its own header comment already accepts drift as a
  known trade-off.
- `AGENTS.md`, `docs/known-gaps.md` — `tail_in`'s "unexercised by the demo" note.
- `demo/grafana/dashboards/logit-internal.json` — panels for the new tiers.

No ADR: nothing here is a new decision about `logit` itself, only about what the demo exercises.

## Verification, across the whole plan

1. `script/validate` after every workstream; `examples/` untouched throughout.
2. `script/demo up --build`, `script/demo logs -f logit` — one `stdio_out` block per tier per
   request.
3. Pull a `/work` trace from Tempo's own API and confirm the expected span tree per workstream.
4. Grafana dashboard and Loki/Tempo derived-field checks per workstream (see each section above).
5. `script/demo down -v`, cold `up --build` — no ordering surprises.
6. `script/cibuild` before opening each PR.
