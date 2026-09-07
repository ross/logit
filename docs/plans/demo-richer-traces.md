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

**Status: not started.**

- `demo/app/requirements.txt` — `requests`, `opentelemetry-instrumentation-requests`.
- `demo/app/gunicorn.conf.py` — `RequestsInstrumentor().instrument()`.
- `demo/app/pages/views.py` — `work` calls `INNER_URL`; a new `inner` view.
- `demo/compose.yaml` — `INNER_URL: http://nginx/inner`, `WEB_CONCURRENCY` raised to 4 (two
  concurrent in-flight `/work` requests each hold a worker while blocking on `/inner`, which needs
  a worker of its own — a self-deadlock at 2).

**Done when:** one `/work` trace shows `demo-app` → `requests` CLIENT → a second, logit-minted
`nginx` server span → `demo-app`'s `/inner` span.

---

## C. Postgres, and the demo's first `tail_in`

**Status: not started.**

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

---

## D. Async work: Redis + a Celery worker

**Status: not started.**

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
