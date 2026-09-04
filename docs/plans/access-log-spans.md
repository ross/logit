---
created: 2026-09-04
updated: 2026-09-04
---

# Enabling plan: access log lines become trace spans

## Context

[ADR `trace-context-span-lifting`](../adr/trace-context-span-lifting.md) is the design decision;
this is the workstream plan that lands it and carries it through to `examples/` and `demo/`. See
that ADR for the full reasoning — this file tracks what's built and what's left, PR by PR.

Goal, restated from `docs/OVERVIEW.md`'s own framing: see one request pass through the stack,
server to server to app, as a single trace. `demo/`'s HAProxy → nginx → app chain
([demo-tracing-stack.md](demo-tracing-stack.md)) already mints and propagates a W3C `traceparent`
end to end and already gets a real span for the app tier (a genuine OTel SDK). haproxy and nginx,
which do the same job of receiving and forwarding a request, produced only logs. `trace_context`'s
new `span:` block closes that gap without a new component or a second config concept.

## Workstreams

### A. Core helpers, the transform, config, schema, tests — **landed**

- `logit_core::trace`: `parse_traceparent`, `random_id_bytes` (moved from
  `logit_pipeline::fanout`).
- `logit_core::time`: `parse_rfc3339_to_nanos` (hoisted from `syslog_in`), `parse_decimal_nanos`.
- `logit_config::ComponentKind::TraceContext`: `trace_id`/`span_id`/`flags` now default to the
  convention (`trace.id`/`span.id`/`trace.flags`); new opt-in `span: SpanLiftConfig` block
  (`mint_id`, `name`, `kind`, `max_skew`).
- `logit_pipeline::graph`: rule 19 extended (empty `span_id`/`flags` field names rejected); new
  rule 25 (`span.name`/`span.max_skew`).
- `logit_transforms::TraceContext::with_span` and the full lift algorithm — see the ADR's
  "Decision" and "Producer timing model" sections for the exact rules.
- `logit_cli::pipeline`: wires the config block through; `to_span_lift` maps `SpanKindConfig` to
  `logit_core::SpanKind`.
- Tests: unit tests in `trace_context.rs` covering every precedence rule, every timing form,
  overflow, skew, and the all-or-nothing failure contract; a config test per new field; graph
  validation tests for rules 19/25; a `logit-cli::pipeline` build test exercising the span block
  end to end; a bench fixture (`fixtures::trace_context_with_span`/`nginx_traced_event`) and
  allocation test (`trace_context_mints_a_span_from_the_convention`, pinned at **1**).
- Docs: `docs/design/data-model.md`'s "Well-known attribute names" table,
  `docs/design/internal-telemetry.md`'s catalog entry, `docs/design/lua-api.md`,
  `docs/design/memory.md`'s allocation table, `docs/known-gaps.md` (Lua's missing span API; the
  not-yet-built upstream CLIENT-span derivation; the service-graph panel item updated), `AGENTS.md`
  and `README.md`'s transform blurbs, `docs/design/pipeline-graph.md`'s rule list.

### B. `examples/nginx` emits the convention — next

`examples/nginx/nginx.conf` gains stock `map`s splitting `$http_traceparent` (no njs needed for
this part — the existing `delay.js` stays as is) into `trace_id`/`span_id`, forwards a
`traceparent` header on the proxied vhost, and its `access_json_syslog` format gains
`traceparent`/`trace.id`/`span.id`/`span.end_s`/`span.duration_s`/`span.status` fields per the
ADR's nginx pairing (`$msec` as end, `$request_time` as duration — never `$msec` as a start).
`examples/nginx-to-influxdb.yaml` gains an `nginx_trace` (`trace_context`, `span: {kind: server,
name: http.request}`) stage between `nginx_json` and `nginx_metrics`, plus a `keep_signals:
[traces]` branch to a `stdio_out` tap so the resulting span is visible without standing up Tempo.
A new bench-adjacent fixture line documents the shape. Verify with `nginx -t` in the pinned image
and `script/validate`.

### C. `demo/` migration — after B

Deltas to the already-landed haproxy/nginx/app chain, not a rebuild of it:

- `demo/haproxy/haproxy.cfg`: replace the underscore `trace_id`/`span_id`/`trace_flags` log-format
  items with the convention's dotted names, add `traceparent` (captured into a `txn` var, the same
  pattern the file already uses for `host`) and the timing pair `span.start_us =
  request_date(us)` / `span.duration_ms = %Ta` (see the ADR's "Producer timing model"), plus the
  finer-grained `haproxy.timer.*` attributes from `%TR`/`%Tw`/`%Tc`/`%Tr`/`%Td` (not turned into a
  second span yet — `docs/known-gaps.md`'s new entry has the derivation for when that lands).
  `option logasap` must stay unset.
- `demo/nginx/nginx.conf`: the same `map`s as workstream B; nginx now mints and forwards its own
  span id (it currently only relays haproxy's), so the app's real OTel span parents to nginx
  instead of directly to haproxy. Adds `$upstream_connect_time`/`$upstream_header_time` alongside
  the existing `$upstream_response_time`.
- `demo/app/pages/logging_formatter.py`: rename its emitted keys to `trace.id`/`span.id`/
  `trace.flags`; no `span:` block for the app tier — its span is already real.
- `demo/logit.yaml`: drop the three tiers' now-unnecessary field-name overrides;
  `haproxy_trace`/`nginx_trace` gain `span: {kind: server, name: http.request}`; a new
  `keep_signals: [logs]` stage in front of `loki_out` (Loki's OTLP endpoint is logs-only, and
  `otlp_out` aborts the whole `send` on any one failing signal — see the existing `trace_only`
  comment in that file for the precedent); a new `keep_signals: [traces]` stage from
  `haproxy_trace`/`nginx_trace` feeding `tempo_out` alongside the existing `trace_only` (`internal`)
  source. The metrics leg (`nginx_scale`/`*_metrics`/`trimmed`/`windowed`) is untouched.
- `demo/grafana/`: the Loki datasource's body-regex derived-field fallback updated for the dotted
  key; the Tempo panel scoped to `{resource.service.name="logit"}` so it isn't also catching the
  new access spans, plus a new panel over `haproxy`'s spans.
- `demo/README.md`, `demo/architecture.dot`, `demo/tempo/tempo.yaml`'s header comment (currently
  says nothing writes to Tempo but `logit`'s own internal spans — no longer true).

## Verification

`script/cibuild` after A and B. Before any `demo/` `up`: `haproxy -c` against the edited config
(this is where a dotted `%(name)` item either works or the log-format needs to fall back to
per-field flags rather than `%{+json}o`) and `nginx -t`. Then `script/demo up --build`: per
request, `logit`'s `stdio_out` shows a haproxy span and an nginx span sharing one trace id with
nginx's `parent_span_id` equal to haproxy's `span_id`, and Tempo's `/api/traces/<id>` shows
haproxy (root) → nginx → demo-app with start times in the expected order given each tier's actual
clock resolution (haproxy µs-ish, nginx ms, app ns).
