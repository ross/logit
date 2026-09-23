// Real browser-side OpenTelemetry tracing for the demo's landing page
// (docs/plans/browser-tracing.md; ADR `browser-tracing-sdk`). Bundled by esbuild
// (../Dockerfile's `bundle` stage) into one minified file, served by demo/app/pages/views.py's
// `browser_telemetry_js` view, and loaded from pages/templates/pages/index.html with
// `<script type="module" defer>`. Spans go through HAProxy's `/v1/*` route to `logit`'s
// `browser_in`, then on to Tempo.
//
// Every API shape below was checked against the pinned versions in package.json (npm registry
// metadata plus each package's GitHub source), not assumed from documentation that may have
// drifted. The comments mark each place this differs from what docs/plans/browser-tracing.md
// originally sketched.

import { TraceFlags } from "@opentelemetry/api";
import { resourceFromAttributes } from "@opentelemetry/resources";
import {
  BatchSpanProcessor,
  WebTracerProvider,
} from "@opentelemetry/sdk-trace-web";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { DocumentLoadInstrumentation } from "@opentelemetry/instrumentation-document-load";
import { FetchInstrumentation } from "@opentelemetry/instrumentation-fetch";
import { ZoneContextManager } from "@opentelemetry/context-zone";
import { registerInstrumentations } from "@opentelemetry/instrumentation";

// `resourceFromAttributes`, not `new Resource(...)`: @opentelemetry/resources 2.x (checked against
// 2.11.0's source) replaced the constructible `Resource` class with a plain interface plus factory
// functions (`resourceFromAttributes`/`emptyResource`/`defaultResource`/`detectResources`).
// `service.name`/`service.namespace` follow the same scheme as every other tier's `set` component
// (demo/logit.yaml's `*_identity` stages). This is the one `logit`-bound tier whose resource the
// OTel SDK supplies itself, with no `set` (demo/logit.yaml's `browser_in` comment).
const resource = resourceFromAttributes({
  "service.name": "demo-browser",
  "service.namespace": "demo",
});

// Same-origin and relative, NOT an absolute cross-origin URL. `otlp_in` has no CORS/`OPTIONS`
// support (docs/known-gaps.md, crates/logit-inputs/src/otlp.rs), so this must resolve against the
// page's own origin and go through HAProxy's `/v1/*` route (demo/haproxy/haproxy.cfg's `is_otlp`
// ACL) to reach `browser_in` (demo/logit.yaml). A cross-origin exporter would fail at preflight,
// before `otlp_in` ever saw the real POST.
const exporter = new OTLPTraceExporter({ url: "/v1/traces" });

const provider = new WebTracerProvider({
  resource,
  spanProcessors: [new BatchSpanProcessor(exporter)],
});

// `ZoneContextManager`, not the SDK's default `StackContextManager`: context has to survive
// async boundaries (promise chains, `fetch()`'s callbacks, `setTimeout` inside
// instrumentation-document-load's `_onDocumentLoaded`) for the fetch span below to become a real
// parent of whatever it kicks off, and for the resourceFetch spans to nest correctly under
// `documentLoad`. `provider.register()` with no explicit `propagator:` also installs the default
// W3C tracecontext + baggage propagator globally. It's the same one every server tier here uses,
// and it's what makes instrumentation-fetch's outbound `traceparent` header and
// instrumentation-document-load's `propagation.extract` (below) work with no extra wiring.
provider.register({
  contextManager: new ZoneContextManager(),
});

// --- Sub-resource correlation ------------------------------------------------------------------
//
// HAProxy stamps every response, this bundle's own load included, with
// `Server-Timing: traceparent;desc="00-<trace>-<span>-01"` (demo/haproxy/haproxy.cfg's
// `Server-Timing` comment). That header is for SUB-RESOURCES, not the document itself
// (docs/plans/browser-tracing.md's "Correction 5"): the document's trace context comes from the
// <meta name="traceparent"> tag instead, because Server-Timing on the *navigation* entry is set
// too late for anything to read before the browser starts painting. It's exposed to JS as
// `PerformanceResourceTiming.serverTiming` (`Timing-Allow-Origin: *`, also set by HAProxy, is
// what makes the timing detail on that entry readable at all). This code reads it and turns it
// into a span LINK to the exact logit-minted span (haproxy_trace/nginx_trace, demo/logit.yaml)
// that served this resource, in a DIFFERENT trace than the one this resourceFetch span lives in.
//
// `Span.addLink()` is present on `@opentelemetry/api@1.9.1`'s `Span` interface (added in 1.9.0),
// so this uses a real link, not the attribute fallback docs/plans/browser-tracing.md left open as
// a contingency. A `SpanContext` is built by hand from the parsed ids rather than through
// `propagation.extract`, because that API extracts into a `Context` (for making a span a CHILD
// of), and there's no public "parse a traceparent into a bare SpanContext" helper. The regex below
// is the same one other tiers in this stack hand-parse a traceparent with
// (demo/nginx/nginx.conf's `map` blocks, demo/haproxy/haproxy.cfg's `bytes()` slicing).
const TRACEPARENT_RE = /^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/;

function linkServerSpanFromServerTiming(span, resource) {
  const entry = resource.serverTiming?.find((t) => t.name === "traceparent");
  if (!entry) return;
  const match = TRACEPARENT_RE.exec(entry.description);
  if (!match) return;
  const [, traceId, spanId] = match;
  span.addLink({
    context: {
      traceId,
      spanId,
      // HAProxy always mints `-01` (demo/haproxy/haproxy.cfg's comment on `trace-flags`). This
      // is the linked span's own sampled bit, unrelated to whether this browser trace is sampled
      // (the WebTracerProvider above has no sampler configured, so it's ALWAYS_ON and every
      // browser span sends).
      traceFlags: TraceFlags.SAMPLED,
      isRemote: true,
    },
  });
}

registerInstrumentations({
  instrumentations: [
    new DocumentLoadInstrumentation({
      applyCustomAttributesOnSpan: {
        resourceFetch: linkServerSpanFromServerTiming,
      },
    }),
    new FetchInstrumentation({
      // `propagateTraceHeaderCorsUrls` is NOT set. Per @opentelemetry/sdk-trace-web@2.11.0's
      // `shouldPropagateTraceHeaders` (packages/opentelemetry-sdk-trace-web/src/utils.ts),
      // same-origin requests ALWAYS get the traceparent header regardless of this option; it only
      // gates CROSS-origin URLs, and this demo's one fetch() call (the button below, `/work`) is
      // same-origin through HAProxy. Setting it would do nothing.
    }),
  ],
});

// --- documentLoad is parented to the server span, not linked --------------------------------
//
// docs/plans/browser-tracing.md says (correctly, per the OTel spec) that the `documentLoad` span
// *should* link to the server-side span named by <meta name="traceparent">: the server's span has
// already ended by the time the browser can report anything, so a parent edge is semantically
// backwards. But @opentelemetry/instrumentation-document-load@0.67.0's source
// (packages/instrumentation-document-load/src/instrumentation.ts's `_collectPerformance`) does
// `context.with(propagation.extract(ROOT_CONTEXT, { traceparent }), () => { const rootSpan =
// this._startSpan(...) ... })`, with no `links:` anywhere in `_startSpan`. `propagation.extract`
// followed by `context.with` makes the extracted context ACTIVE, and `tracer.startSpan` with no
// explicit parent parents to the active context. So, as shipped, `documentLoad` is a real
// parent-child edge from the server span, not a link, and no config option changes that. This file
// leaves the library's behavior as is rather than working around it; ADR `browser-tracing-sdk`
// has the full account.

const workButton = document.getElementById("work-btn");
if (workButton) {
  // A real fetch() call, not a plain `<a href="/work">`: instrumentation-fetch exists for context
  // propagation across an async boundary, which a plain navigation doesn't exercise. The resulting
  // span becomes a genuine parent of the app's Django request span for this call
  // (ZoneContextManager above keeps the click handler's context alive into fetch()'s promise
  // chain).
  workButton.addEventListener("click", () => {
    fetch("/work").catch(() => {
      // Swallowed deliberately: pages/views.py's `work` view has its own real error paths (an 8%
      // chance of a 503, a possible 502 from its inner call), and this button exists to trigger
      // them, not to surface them a second time in the browser.
    });
  });
}
