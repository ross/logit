---
created: 2026-09-03
updated: 2026-09-10
---

# Enabling plan: browser tracing for the demo

> **Documentation only, and now unblocked.** This was workstream C of
> [demo-tracing-stack.md](demo-tracing-stack.md), originally shipped as findings rather than code
> because it hinged on a `logit` feature ("The blocker" below) that didn't exist yet. That feature
> has since landed ([ADR `otlp-json-decoding`](../adr/otlp-json-decoding.md)), which flips this
> plan's own recommendation -- see "Recommendation, updated" at the bottom. No `demo/` files have
> changed as part of landing *this* document; wiring the SDK into the demo is tracked as its own
> follow-on plan.

## The target, generically

`demo-tracing-stack.md`'s workstreams A and B give the demo a correlated server-side trace:
HAProxy mints a W3C `traceparent`, and it flows through nginx and the app into Loki (as
correlated log lines) and Tempo (as real spans, from workstream B). Browser tracing would extend
that trace into the page itself — page-load timing and, ideally, real client-side spans, both
tied to the same trace id the server side already has.

## What already works, with no new code

- **The initial HTML request can never carry a `traceparent`** — no script runs before the
  browser asks for the document. The standard answer is for the *server* to hand its context
  back:
  - A `<meta name="traceparent" content="...">` tag rendered by the app from its active span (a
    ~5-line Django context processor once workstream B lands). This is what
    `@opentelemetry/instrumentation-document-load` actually reads to associate the document-load
    span with the server trace — as a link, not a parent: the spec is explicit that the causal
    relationship runs the other way for the very first request (the server's span already
    finished by the time the browser could report anything). **Correction (2026-09-10):** an
    earlier version of this section also claimed the instrumentation reads HAProxy's
    `Server-Timing` header for this same purpose. Checked against the instrumentation's own
    source and README: it doesn't. `Server-Timing` is real and useful, but for a different job —
    see the next point.
  - HAProxy's `Server-Timing: traceparent;desc="00-<trace>-<span>-01"` response header
    (`demo/haproxy/haproxy.cfg`, added in workstream A specifically as this plan's foundation) is
    for **sub-resources**, not the document. A `<script src>`/`<img>` request can't carry an
    outbound `traceparent` either, so HAProxy mints each one a *fresh* trace id — that
    sub-resource's browser-side `resourceFetch` span and its logit-minted `haproxy_trace`/
    `nginx_trace` spans land in different traces unless something stitches them back together.
    `Server-Timing` is that stitch: it's exposed to JS as
    `PerformanceResourceTiming.serverTiming` (populated for resource entries, not just
    navigation, Baseline since March 2023; same-origin needs no extra header, which is what this
    demo already is), and `instrumentation-document-load`'s `applyCustomAttributesOnSpan.resourceFetch`
    hook receives both the span and its timing entry, so reading `serverTiming` there and adding a
    link (or, if the SDK's `Span.addLink` isn't available in the pinned version, plain
    `server.trace_id`/`server.span_id` attributes) turns a browser resource-load span into a real,
    followable link to the exact logit-minted span that served it. `Timing-Allow-Origin: *`
    (also already set) is what makes the timing detail on that entry readable at all, same-origin
    or not. Standardization in progress upstream:
    [opentelemetry-specification#3811](https://github.com/open-telemetry/opentelemetry-specification/issues/3811),
    "Standardize `Server-Timing: traceparent` propagator across vendors" (accepted,
    Spec-In-Progress; already shipped by Grafana, Splunk, and Microsoft).

- **Same-origin export sidesteps CORS entirely.** A browser exporter posting to `/v1/traces` on
  HAProxy's own origin, with HAProxy routing that path to `logit:4318`, never triggers a
  preflight. This still matters even with the blocker below closed: `otlp_in` remains POST-only
  with no `OPTIONS` handling (`docs/known-gaps.md`), so a *cross-origin* exporter still fails
  before sending anything at all — same-origin, via a proxy, stays the only supported shape.

## The blocker — closed

~~OpenTelemetry's browser exporters emit **OTLP/JSON**, not protobuf.
`@opentelemetry/exporter-trace-otlp-proto` is Node-only — protobuf-in-the-browser has been an
open upstream request since 2022 (`open-telemetry/opentelemetry-js#3118`). `logit`'s `otlp_in`
rejected `Content-Type: application/json` outright.~~

**Closed 2026-09-10.** `otlp_in`'s HTTP transport now accepts `application/json` alongside
protobuf, decoded through a hand-written dialect layer onto the same event model the protobuf path
already produces — see [ADR `otlp-json-decoding`](../adr/otlp-json-decoding.md) for the design,
and `docs/known-gaps.md` for what's still open around it (CORS, the error-body content type, and
the JSON path's relative memory cost). Both shapes below are now buildable with no further `logit`
change; see "Recommendation, updated" at the bottom for which to build.

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

## Recommendation, updated

The original recommendation ("ship the beacon; it costs nothing in `logit`") was written
specifically *because* the SDK route was blocked. With the blocker closed, that's no longer the
deciding factor — see the follow-on plan doc (this document's Workstream C in the browser-tracing
implementation plan) for the actual decision and its reasoning: the demo app is expected to grow
client-side complexity over time, and the SDK's `resourceFetch` span for its own ~180 KB bundle
load is treated as a feature (real DNS/connect/TTFB/download timing) rather than a cost. That plan
also folds in the `Server-Timing`-based sub-resource linking above, which this document didn't
originally propose.

The beacon approach above is left in place as the cheaper alternative if a future reader wants
"browser activity visibly tied to the trace" without a build step, rather than a complete SDK
integration — it remains genuinely useful and still needs no `logit` change.
