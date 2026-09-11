---
created: 2026-09-10
updated: 2026-09-10
---

# Browser tracing: the real OTel-JS SDK, `addLink` for sub-resources, and living with document-load's parent (not link) behaviour

## Status
Accepted

## Context

[browser-tracing.md](../plans/browser-tracing.md) laid out two shapes for extending the demo's
already-correlated server-side trace into the browser -- a hand-rolled beacon script, or the real
`@opentelemetry/sdk-trace-web` SDK -- and left the SDK route blocked on `otlp_in` accepting
OTLP/JSON. [ADR `otlp-json-decoding`](otlp-json-decoding.md) closed that blocker. This ADR is the
follow-on: implementing the SDK route (`demo/app/browser/telemetry.js`, bundled by a new `esbuild`
stage in `demo/app/Dockerfile`), and settling three things `browser-tracing.md` explicitly left
open pending empirical verification against the pinned package versions.

## Decision

**Ship the real SDK**, not the beacon: `@opentelemetry/sdk-trace-web` +
`instrumentation-document-load` + `instrumentation-fetch` + `context-zone` +
`exporter-trace-otlp-http` (the JSON/HTTP exporter -- `exporter-trace-otlp-proto` is protobuf and
Node-only), all pinned exactly in `demo/app/browser/package.json`. `esbuild` bundles
`browser/telemetry.js` into one minified file in a new `node:22-alpine` Dockerfile stage -- this
demo's first client-side build step, so `docker compose up --build` now needs npm registry access
too, not just PyPI/crates.io/apt.

The three open questions, checked directly against each package's own published source rather than
assumed:

1. **`applyCustomAttributesOnSpan.resourceFetch` exists and receives `(span: Span, resource:
   PerformanceResourceTiming)`** -- confirmed against
   `instrumentation-document-load@0.67.0`'s `_addCustomAttributesOnResourceSpan`. Used to read
   `resource.serverTiming` for the traceparent HAProxy stamped on that sub-resource's response
   (`demo/haproxy/haproxy.cfg`'s `Server-Timing` header, "Foundation for browser-tracing.md") and
   turn it into a span link to the exact `haproxy_trace`/`nginx_trace`-minted span that served it.

2. **`Span.addLink()` is real and used, not the attribute fallback.** Confirmed present on
   `@opentelemetry/api@1.9.1`'s `Span` interface (added in 1.9.0, and every pinned package here
   peers on `^1.3.0`/`>=1.0.0 <1.10.0`, so 1.9.1 satisfies all of them). `telemetry.js` builds a
   `SpanContext` by hand from the regex-parsed `Server-Timing` value and calls
   `span.addLink({ context })` inside the `resourceFetch` hook above -- confirmed end-to-end
   against a live trace: a `resourceFetch` span for `/telemetry.js`'s own load carried a link to
   `haproxy`'s real access-line span, in a different trace, and that target span was independently
   confirmed to exist by querying Tempo directly for it.

3. **`documentLoad` is a PARENT of the server span, not a link -- contradicting this repo's own
   prior (spec-correct) expectation.** `browser-tracing.md` argued the causal edge should be a
   link, since the server's own span has already ended by the time the browser can report
   anything. Checked directly against `instrumentation-document-load@0.67.0`'s
   `_collectPerformance`: it does
   `context.with(propagation.extract(ROOT_CONTEXT, { traceparent }), () => { const rootSpan =
   this._startSpan(...) })`, with no `links:` anywhere in `_startSpan`. `propagation.extract`
   followed by `context.with` makes the extracted context *active*, and `tracer.startSpan` with no
   explicit parent parents to whatever's active -- so `documentLoad`, as this package actually
   ships it, is a real parent-child edge. There is no config option to change this. Confirmed
   against a live trace in Tempo: the `documentLoad` span's `parentSpanId` decoded to exactly the
   `demo-app` request span id the `<meta name="traceparent">` tag (rendered by
   `pages/context_processors.py`'s new `traceparent` processor) carried, with an empty `links`
   array. **Decision: accept upstream's actual behaviour rather than fight it** -- there's no
   supported way to force a link here short of disabling the instrumentation's own traceparent
   reading and re-implementing `_collectPerformance`'s span creation by hand, which would mean
   maintaining a fork of real logic for a demo. The parent edge is still a real, correct-enough
   causal relationship for a demo whose point is showing a trace connect across tiers, just not
   the spec-ideal shape.

`propagateTraceHeaderCorsUrls` (`FetchInstrumentation`) is left unset: confirmed against
`sdk-trace-web`'s own `shouldPropagateTraceHeaders` that same-origin requests always get the
`traceparent` header regardless of this option -- it only gates cross-origin URLs, and this demo's
one `fetch()` call (`/work`, same-origin through HAProxy) doesn't need it.

## Alternatives considered

- **The beacon script** (`browser-tracing.md`'s cheaper alternative). Rejected per that document's
  own updated recommendation: the blocker that originally motivated it is closed, and a demo whose
  purpose is showing OTel end-to-end should show the real SDK, not a hand-rolled substitute.
- **Forcing `documentLoad` into a link** by bypassing `instrumentation-document-load`'s own
  traceparent handling (e.g. disabling it and manually creating the root span with `links:` at
  start time). Rejected: this would mean re-implementing a meaningful slice of the
  instrumentation's own logic (navigation-entry timing, resource-span nesting, network events) just
  to change one edge's semantics, for a demo. Documenting the real, current behaviour honestly (in
  code comments, this ADR, and the PR body) was judged more valuable than a custom span-creation
  path that drifts further from what a real integration looks like.
- **webpack/rollup/vite** instead of `esbuild` for the bundler. Rejected: `esbuild` is a single
  static Go binary with no further transitive npm tooling, matching this demo's general bias
  toward the smallest thing that actually does the job (`browser-tracing.md` assumed it too).

## Consequences

- `docker compose up --build` needs npm registry access now, alongside PyPI/crates.io/apt --
  flagged in `demo/app/Dockerfile`'s header comment and `demo/README.md`.
- The `documentLoad` -> server-span edge in Tempo is a parent, not a link, for as long as
  `instrumentation-document-load` ships it that way -- a future upstream change (or a documented
  workaround) could flip this; nothing in this repo depends on it being a link today.
- `resourceFetch` spans for anything logit-fronted (the bundle itself, the two SVGs) carry a real,
  followable link into a different trace -- the mechanism `browser-tracing.md` called this
  workstream's most interesting trick, now real and verified against a running Tempo rather than
  only designed.
