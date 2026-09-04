---
created: 2026-09-03
updated: 2026-09-03
---

# Enabling plan: browser tracing for the demo

> **Documentation only.** This is workstream C of
> [demo-tracing-stack.md](demo-tracing-stack.md), deliberately shipped as findings rather than
> code: everything below was checked against the current codebase and current upstream
> OpenTelemetry-JS state as of the date above, so a follow-on session can pick this up cold
> without re-deriving it. No `demo/` files change as part of landing this document.

## The target, generically

`demo-tracing-stack.md`'s workstreams A and B give the demo a correlated server-side trace:
HAProxy mints a W3C `traceparent`, and it flows through nginx and the app into Loki (as
correlated log lines) and Tempo (as real spans, from workstream B). Browser tracing would extend
that trace into the page itself — page-load timing and, ideally, real client-side spans, both
tied to the same trace id the server side already has.

## What already works, with no new code

- **The initial HTML request can never carry a `traceparent`** — no script runs before the
  browser asks for the document. The standard answer is for the *server* to hand its context
  back, two ways, both usable together:
  - HAProxy's `Server-Timing: traceparent;desc="00-<trace>-<span>-01"` response header
    (`demo/haproxy/haproxy.cfg`, added in workstream A specifically as this plan's foundation).
  - A `<meta name="traceparent" content="...">` tag rendered by the app from its active span (a
    ~5-line Django context processor once workstream B lands).

  Both are what `@opentelemetry/instrumentation-document-load` reads to associate the
  document-load span with the server trace — as a link, not a parent: the spec is explicit that
  the causal relationship runs the other way for the very first request (the server's span
  already finished by the time the browser could report anything).

- **Same-origin export sidesteps CORS entirely.** A browser exporter posting to `/v1/traces` on
  HAProxy's own origin, with HAProxy routing that path to `logit:4318`, never triggers a
  preflight. This matters concretely: `otlp_in` is POST-only with no `OPTIONS` handling
  (`crates/logit-inputs/src/otlp.rs:188`), so a cross-origin exporter would fail before sending
  anything at all.

## The blocker

OpenTelemetry's browser exporters emit **OTLP/JSON**, not protobuf.
`@opentelemetry/exporter-trace-otlp-proto` is Node-only — protobuf-in-the-browser has been an
open upstream request since 2022 (`open-telemetry/opentelemetry-js#3118`, still open as of this
writing). `logit`'s `otlp_in` rejects `Content-Type: application/json` outright, with a 415 and
an explicit `"OTLP/JSON is not supported; send protobuf as application/x-protobuf"` message
(`crates/logit-inputs/src/otlp.rs:194-205`).

So **real browser spans into `logit` need OTLP/JSON decoding in `otlp_in`/`logit-proto`** — the
one `logit` change every other workstream in `demo-tracing-stack.md` avoided. This is the actual
decision point for whoever picks this up: build the small `logit` feature, or ship the
no-`logit`-change alternative below.

## Two shapes, with their real costs

### Beacon to the app — no `logit` change

A small hand-rolled script reads `PerformanceNavigationTiming`/`PerformanceResourceTiming` plus
the `<meta>` traceparent, and `POST`s a compact JSON beacon to an app endpoint. The app logs each
browser event as one syslog JSON line carrying the page's `trace_id`/`span_id`, flowing through
workstream B's existing `app_trace` chain into Loki.

- Cost: one small endpoint, one small script, no bundler, no npm dependency, no `logit` change.
- What it actually delivers: browser telemetry as *correlated log events on the same trace*, not
  client-side spans. The Loki→Tempo "View trace" link still works from a browser-originated log
  line — it just isn't a leaf in the trace itself.
- Recommended if the goal is "browser activity visibly tied to the trace" rather than "a
  technically complete OTel browser SDK integration."

### The real OTel browser SDK — needs the `logit` change

`@opentelemetry/sdk-trace-web` + `instrumentation-document-load` +
`instrumentation-fetch`/`instrumentation-xml-http-request` + `context-zone`, bundled with an
`esbuild` (or similar) stage in the app's `Dockerfile` and served as a static file — there is no
official zero-build browser bundle for these packages. `propagateTraceHeaderCorsUrls` set for
same-origin fetch/XHR so client-issued requests continue the trace into the app. Exports
OTLP/JSON to same-origin `/v1/traces`, proxied by HAProxy to `logit:4318`.

- Cost: a real client-side build step (new for this demo — everything else in it is either
  stdlib or a stock image), plus the `otlp_in` OTLP/JSON prerequisite below.
- What it delivers: genuine client-side spans — page load, resource timing, fetch/XHR — as real
  children/links in the same trace, visible in Tempo exactly like the server spans.
- Recommended if the goal is a complete, idiomatic demonstration of end-to-end OTel tracing.

## The `logit` prerequisite, if the SDK route is chosen

`otlp_in` accepting `application/json` in addition to `application/x-protobuf` is a bounded,
well-specified feature — OTLP/JSON is a documented 1:1 mapping of the same protobuf messages onto
JSON, not a new wire format to design. It's plausibly worth having independent of this demo:
OTLP/JSON is what browsers and a fair number of polyglot SDKs send by default. Recorded as its
own entry in [known-gaps.md](../known-gaps.md) rather than folded into this plan, since it's a
`logit` feature PR, not a demo PR — pick it up there when someone wants to build it.

## Recommendation

Ship the beacon approach if/when this plan is picked up for real: it's genuinely useful, costs
nothing in `logit`, and is honest about what it is. Revisit the SDK route once (or if) the
`otlp_in` OTLP/JSON gap gets closed as its own piece of work — at that point the SDK integration
described above should need no further design, just implementation.
