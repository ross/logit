---
created: 2026-09-03
updated: 2026-09-03
---

# Enabling plan: a traced demo stack — HAProxy → nginx → app

## The target, generically

The demo stack (`demo/`) was one hop deep: `demo/hello/app.py`, a stdlib-Python `http.server`,
fabricated RFC 3164 syslog datagrams with a JSON body and fired them at `logit`'s `syslog_in`. It
was both the front door and its own traffic source. Nothing in it carried a trace context.

Meanwhile `logit` grew the whole application-trace stack (PR #68, workstream D of
[otlp-logs-and-resource-identity.md](otlp-logs-and-resource-identity.md)):
`LogRecord.trace` (`crates/logit-core/src/trace.rs`), the `trace_context` transform
(`crates/logit-transforms/src/trace_context.rs`), Lua `event.log.trace_id`, OTLP carriage, and
`stdio_out` rendering. None of it was exercised by anything shipped. Grafana's Loki datasource
already carried a `derivedFields` click-through to Tempo
(`demo/grafana/provisioning/datasources/datasources.yaml`) with no log line to match. `otlp_in`
(`crates/logit-inputs/src/otlp.rs`) was implemented and tested and received no traffic at all.

The target: a real multi-tier request path — **HAProxy → nginx → an app** — where HAProxy mints
a W3C `traceparent`, every tier logs the same trace id, and the app emits real spans into
`otlp_in`. One request then produces a correlated HAProxy + nginx + app log triple in Loki *and*
a real application trace in Tempo, clickable from either side.

Three PRs. **No `logit` code changes anywhere in this plan.** Browser-side tracing is
deliberately not implemented — workstream C is a documentation deliverable
([browser-tracing.md](browser-tracing.md)) recording exactly what it would take, including the
one `logit` gap that blocks the clean version.

## Decisions already settled

| Decision | Why |
|---|---|
| W3C Trace Context (`traceparent`) on the wire | `00-<32 hex trace>-<16 hex span>-<2 hex flags>` (https://www.w3.org/TR/trace-context/). Every tier and SDK speaks it natively. **Verified: HAProxy 3.0 has no native traceparent support** — grepping the 3.0 configuration manual for `traceparent`/`otel` returns nothing but the out-of-scope `filter opentracing`. It has to be minted by hand. |
| HAProxy mints it, reusing an inbound one when present | It's the edge. Reuse keeps the demo honest about being one hop in a larger system, and makes the behaviour testable with a supplied header. |
| Each tier logs `trace_id`/`span_id`/`trace_flags` as **separate JSON fields**, flags **decimal** | `trace_context` takes pre-split hex and decimal-only flags (`crates/logit-transforms/src/trace_context.rs::numeric_flags`) — its own doc comment explains why: a traceparent's 2-hex-digit flags octet would silently parse as the wrong decimal value if accepted directly. `logit` has no `traceparent` parser and this plan doesn't add one — the split happens in HAProxy vars, an nginx `map`, and Python. No `lua` component anywhere. |
| Stock nginx, not `nginx:*-otel` | nginx only relays and logs the header; a `map` with positional regex captures does the split. Zero extra modules, zero exporter config. |
| One `syslog_in` listener **per tier** | `set` writes the batch's *resource*, not per-event attributes. Tiers sharing one listener would interleave into one batch and get one wrong `service.name`. |
| The demo keeps its own copies of every config | ADR `demo-stack-separate-from-dev-stack`. `demo/nginx/nginx.conf` is a new file adapted from `examples/nginx/nginx.conf`, not an edit of it — that file is a three-vhost njs proof artifact for [nginx-integration.md](nginx-integration.md) and stays as it is. |
| Host port 8080 stays the front door | It's what `demo/README.md` and the landing page's links promise. HAProxy takes it; nginx and the app become internal-only. |
| The app tier keeps `service.name: demo-hello` through workstream A | Four panels in `demo/grafana/dashboards/logit-internal.json` hardcode `{service_name="demo-hello"}` (including a panel title). Renaming in A would blank them for no gain; the rename belongs in B, alongside the dashboard edit and the app rewrite. |

## Gaps this plan exists to schedule

| Gap | Closed by |
|---|---|
| Nothing shipped emits an application trace context, so `trace_context`, `LogRecord.trace`, and `stdio_out`'s `trace_id=` rendering are untested in anger | A |
| Grafana's Loki→Tempo `derivedFields` link has never had a matching log line, and is a body regex rather than Loki's native `trace_id` structured metadata | A |
| The demo is one hop deep — no upstream, no proxy chain, nothing a trace id could usefully *tie together* | A |
| `otlp_in` receives no traffic from anything, ever | B |
| The demo app is a stdlib stub, so "real framework integration points" (syslog, drop-in tracing, metrics) are unproven | B |
| Browser telemetry has no path into `logit`, and nothing records why or what it would take | C ([browser-tracing.md](browser-tracing.md), documented, not built) |

## Reference topology

```
                          traffic (curl loop, ~0.5 rps)
                                     |
                                     v
  browser ------------------> haproxy :8080  ── mints traceparent; Server-Timing on the way back
                                     |  traceparent: 00-<trace>-<span>-01
                                     v
                                  nginx :80   ── map splits $http_traceparent, relays it upstream
                                     |
                                     v
                                 hello :8080

  syslog/UDP, one listener per tier:
     haproxy --> :5140  haproxy_in -> haproxy_identity -> haproxy_json -> haproxy_trace --+
     nginx   --> :5141  nginx_in   -> nginx_identity   -> nginx_json   -> nginx_trace  --+|
     hello   --> :5142  app_in     -> app_identity     -> app_json     -> app_trace ----+||
                                                                                       vvv
                                            tap (stdio_out) <---------------------------+++
                                            log_out (otlp_out/HTTP) --> Loki

     nginx_trace -> access_metrics -> trimmed (keep) -> windowed -> influx_out
                                                                        ^
     self (internal) -> self_windowed -------------------------------- -+
     self -> trace_windowed ------------------------------> trace_out --> Tempo
                                                                 ^
     app --OTLP/HTTP protobuf--> :4318 app_otlp_in -> app_spans -+  (workstream B)
```

## Workstream dependency graph

`A → B → C`. Each is one PR. B depends on A's three-listener config and traceparent chain; C is
docs-only and depends on B only for the file it describes changing.

---

## A. HAProxy front door, W3C trace context, three-tier ingestion

**Status: landed.**

A real three-hop request path where HAProxy mints a `traceparent`, nginx and the app propagate
it, all three tiers log it as split hex fields, and `trace_context` lifts it onto
`LogRecord.trace` so Loki gets a native `trace_id` and `stdio_out` renders one.

**Files:**

- `demo/haproxy/haproxy.cfg` (new) — `haproxy:3.0-alpine`. Reuses an inbound `traceparent`
  (`bytes(3,32)` off the header) or mints one (`uuid(),regsub('-','','g')`); always mints a fresh
  span id for its own hop. Logs a JSON access line over syslog/UDP to `logit:5140` using
  `%{+json}o` (new in 3.0) — deliberately not `%{+Q,+E}`, whose escaping targets RFC 5424
  structured data and would emit an invalid JSON escape (`\]`). Sets
  `Server-Timing: traceparent;desc="..."` and `Timing-Allow-Origin: *` on every response —
  unused until workstream C, but free to add now.
- `demo/nginx/nginx.conf` (new) — `nginx:1.27-alpine`. Three `map` blocks split
  `$http_traceparent` into `$trace_id`/`$span_id`/`$trace_flags` (the last as decimal 0/1, never
  the raw hex octet); relays upstream via `proxy_pass`. Log field names for the first five fields
  are unchanged from before this plan, so the existing metrics chain and Grafana's InfluxDB
  panels need no edits.
- `demo/hello/app.py` (edit, stays stdlib) — reads the inbound `traceparent`, splits it the same
  way, adds `trace_id`/`span_id`/`trace_flags` to its JSON body. `_synthetic_loop` and its thread
  are gone — `demo/compose.yaml`'s new `traffic` service replaces it, driving requests through the
  full chain so synthetic traffic also carries a trace context.
- `demo/logit.yaml` (edit) — three listeners (`haproxy_in`/`nginx_in`/`app_in` on
  5140/5141/5142), three `set` identities, three `json`, three `trace_context` (one per tier),
  fanning into a shared `tap`/`log_out`. The nginx tier alone continues to the existing metrics
  leg. `trimmed` (`keep`) now also strips `trace_id`/`span_id` before `aggregate`, same
  per-request-cardinality reasoning it already applies to `request_id`.
- `demo/compose.yaml` (edit) — added `haproxy` (publishes `8080:8080`), `nginx`, and `traffic`;
  `hello` no longer publishes a port.
- `demo/grafana/provisioning/datasources/datasources.yaml` (edit) — added a `matcherType: label`
  derived field keyed on Loki's native `trace_id` structured metadata (the one that fires now
  that `trace_context` runs with `keep_source: false`); kept the original body-regex field as a
  fallback.
- `demo/README.md` (edit) — new service table, the three-hop story.

**Notes from getting this working:**

- Multiple `sources:` on one sink was already supported and already in use (`influx_out` takes
  `[windowed, self_windowed]`); a single shared `tap`/`log_out` across all three tiers needed no
  new capability.
- The riskiest HAProxy line turned out to be `bytes(<offset>,<length>)` on a `str`-typed sample
  (`req.hdr(traceparent)`), and it works exactly as the manual's own example implies — confirmed
  with `haproxy -c` and a live request end to end.
- Two real bugs surfaced only by actually running the stack, not by config review:
  - **HAProxy's `\`-newline continuation splits a quoted value's quotes across lines instead of
    joining them.** `haproxy -c` rejected the first draft with `unmatched quote` / `unknown
    keyword` on the continuation lines. Every multi-field HAProxy directive here is one physical
    line as a result — no wrapping for readability.
  - **`req.hdr(...)` can't be read directly inside `log-format`** — `haproxy -c` error: "may not
    be reliably used here because it needs 'HTTP request headers' which is not available here."
    Fixed by capturing it into a `txn` var (`http-request set-var(txn.host) req.hdr(host)`) before
    the log line is built, the same pattern already used for `trace_id`/`span_id`.
  - **HAProxy's `%B` is the *total* response size, headers included** — a 2-byte test body logged
    as 117. Named `bytes_sent`, not `body_bytes_sent`, so it doesn't imply an equivalence with
    nginx's true body-only `$body_bytes_sent` that isn't there.
  - **Compose word-splits a bare-scalar `command:` itself**, stopping at the first `;` — the
    `traffic` service's shell loop silently collapsed to `["while", "true"]` and crash-looped
    (`unexpected end of file (expecting "do")`) until wrapped as a one-element YAML *list*
    holding the whole script (`command:` / `  - |` / `    while true; do ...`), the same pattern
    `graph-dot`/`graph-svg` already used for their own one-liners.
  - **Compose interpolates `$VAR` in its own YAML before the shell ever sees it** — the traffic
    loop's `for p in ...; do curl .../"$p"` needed `$$p`, or compose silently substituted an empty
    string for an "unset variable `p`".

**Done when:** `script/demo up --build` comes up; `script/demo logs -f logit` shows three
`stdio_out` blocks per request — haproxy, nginx, app — each rendering the same `trace_id=<hex>`
with different `span_id=`; Grafana's Loki panels show all three with a working "View trace" link;
the InfluxDB panels are visibly unchanged; `script/validate` passes. **Verified live** (2026-09-03):
all of the above confirmed end to end against a real `script/demo up --build` — three-tier
`stdio_out` triples sharing one `trace_id`, Loki's `service_name` label carrying `demo-hello`/
`nginx`/`haproxy`, both Grafana derived fields provisioned, the InfluxDB `web.requests` leg
unaffected, and the landing page/`graph.svg` rendering through the full chain.

---

## B. App replaces the stub, and real spans reach `otlp_in`

**Status: not started.**

A real framework at the bottom of the chain (Django, chosen for drop-in OTel
instrumentation/logging/metrics — `opentelemetry-instrumentation-django`,
`opentelemetry-instrumentation-logging`, syslog via `logging.handlers.SysLogHandler`), emitting
structured logs to syslog *and* real OTel spans over OTLP/HTTP protobuf into `logit`'s own
`otlp_in`, forwarded to Tempo alongside `logit`'s internal spans and parented to HAProxy's trace.

**Files:**

- `demo/app/` (new) — Django project, `Dockerfile`, `requirements.txt`, `gunicorn.conf.py`.
  Instrumentation initialized in gunicorn's `post_fork` hook, not at import — the batch span
  processor's exporter thread does not survive `fork`. `OTEL_EXPORTER_OTLP_ENDPOINT=http://logit:4318`,
  `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf` (`otlp_in` rejects `application/json` with a 415 —
  `crates/logit-inputs/src/otlp.rs:194-205` — so this must be the protobuf exporter package, not
  the default). Default W3C propagator makes Django's server span a child of HAProxy's span with
  no code. `LoggingInstrumentor` puts `otelTraceID`/`otelSpanID` on every log record; confirm
  against `crates/logit-inputs/src/syslog.rs` whether `SysLogHandler`'s headerless `<PRI>message`
  parses, or have the formatter emit an RFC 3164 header itself.
- `demo/logit.yaml` (edit) — `app_otlp_in` (`otlp_in`, `bind: 0.0.0.0:4318`) → `app_spans`
  (`aggregate`, 60s, stripping non-span signals before Tempo the same way `trace_windowed`
  already does for `self`) → added onto `trace_out`; rename the app tier's `service.name` to
  `demo-app` and update the four dashboard panels that hardcode `demo-hello`.
- `demo/compose.yaml` (edit) — replace `hello` with `app`, built from `demo/app/`.
- `demo/hello/` (delete). `demo/README.md`, `docs/known-gaps.md` (edit) — `otlp_in` is exercised
  now.

**Done when:** one request through HAProxy produces, in Tempo, a trace containing the Django
server span, reachable by clicking "View trace" from any of the three Loki lines for that
request; `script/validate` passes.

---

## C. Write down what browser tracing would take

**Status: landed as documentation** — see [browser-tracing.md](browser-tracing.md). No `demo/`
code changes; the summary is that same-origin OTLP export through HAProxy works with no `logit`
change, but a real OTel *browser SDK* needs `otlp_in` to accept OTLP/JSON, which it doesn't
today. Recorded in [known-gaps.md](../known-gaps.md).

## Verification, across the whole plan

1. `script/validate` — `logit validate` over `demo/logit.yaml` and every `examples/*.yaml`. Must
   pass after every workstream; `examples/` must be untouched by all three.
2. `haproxy -c -f demo/haproxy/haproxy.cfg` before the first `up` — this is where the `bytes()`
   and `regsub()` quoting either works or doesn't.
3. `script/demo up --build`, then `script/demo logs -f logit`: one `stdio_out` block per tier per
   request, all sharing `trace_id=` and differing in `span_id=`.
4. `curl -i http://localhost:8080/` — confirm the `Server-Timing` response header. Then confirm
   an inbound header is *reused*, not replaced:
   `curl -H 'traceparent: 00-<known 32 hex>-<known 16 hex>-01' http://localhost:8080/` must
   produce that exact trace id in all three log lines.
5. Grafana → Explore → Loki: `{service_namespace="demo"}` shows all three `service_name`s, and
   "View trace" resolves in Tempo.
6. Grafana → the shipped `logit` dashboard: the InfluxDB `web.*` panels look exactly as they did
   before A, proving the metrics leg survived the re-point to the nginx tier.
7. The landing page at `:8080` still renders the pipeline SVG, now through HAProxy → nginx → app.
8. `script/demo down -v` and a cold `up --build` — no ordering surprises from the new services.
