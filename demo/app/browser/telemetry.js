// Real browser-side OpenTelemetry tracing for the demo (docs/plans/browser-tracing.md's
// Workstream C). Bundled by esbuild (../Dockerfile's `bundle` stage) into one minified file,
// served by demo/app/pages/views.py's `browser_telemetry_js` view and loaded from
// pages/templates/pages/index.html with `<script type="module" defer>`.
//
// Every API shape below was checked directly against the pinned versions in package.json (npm
// registry metadata + this package's own GitHub source), not assumed from documentation that may
// have drifted -- see the comments at each divergence from what docs/plans/browser-tracing.md
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

// `resourceFromAttributes`, not `new Resource(...)` -- confirmed against
// @opentelemetry/resources@2.11.0's own source: the 2.x major replaced the constructible
// `Resource` class with a plain interface plus factory functions
// (`resourceFromAttributes`/`emptyResource`/`defaultResource`/`detectResources`). `service.name`/
// `service.namespace` match every other tier's `set` component (demo/logit.yaml's `*_identity`
// stages) -- this is the one tier whose resource a real OTel SDK supplies itself, not `set`
// (demo/logit.yaml's `browser_in` comment).
const resource = resourceFromAttributes({
  "service.name": "demo-browser",
  "service.namespace": "demo",
});

// Same-origin, relative -- NOT an absolute cross-origin URL. `otlp_in` has no CORS/`OPTIONS`
// support (docs/known-gaps.md, crates/logit-inputs/src/otlp.rs), so this must resolve against
// this page's own origin and go through HAProxy's `/v1/*` route (demo/haproxy/haproxy.cfg's
// `is_otlp` ACL) to reach `browser_in` (demo/logit.yaml) -- a cross-origin exporter would fail
// outright at preflight, before `otlp_in` ever saw the real POST.
const exporter = new OTLPTraceExporter({ url: "/v1/traces" });

const provider = new WebTracerProvider({
  resource,
  spanProcessors: [new BatchSpanProcessor(exporter)],
});

// `ZoneContextManager`, not the SDK's default `StackContextManager` -- context has to survive
// async boundaries (promise chains, `fetch()`'s own callbacks, `setTimeout` inside
// instrumentation-document-load's own `_onDocumentLoaded`) for the fetch span below to become a
// real parent of whatever it kicks off, and for the resourceFetch spans below to nest correctly
// under `documentLoad`. `provider.register()` with no explicit `propagator:` also installs the
// default W3C tracecontext + baggage propagator globally -- the same one every server tier here
// already uses, and what makes instrumentation-fetch's outbound `traceparent` header and
// instrumentation-document-load's `propagation.extract` (below) both work with no extra wiring.
provider.register({
  contextManager: new ZoneContextManager(),
});

// --- Sub-resource correlation: the point of this whole workstream -----------------------------
//
// HAProxy stamps every response -- this bundle's own load included -- with
// `Server-Timing: traceparent;desc="00-<trace>-<span>-01"` (demo/haproxy/haproxy.cfg, "Foundation
// for docs/plans/browser-tracing.md"). That header is for SUB-RESOURCES, not the document itself
// (docs/plans/browser-tracing.md's "Correction 5" -- the document's own trace context comes from
// the <meta name="traceparent"> tag below instead, since Server-Timing on the *navigation* entry
// is set too late for anything to read before the browser starts painting). It's exposed to JS as
// `PerformanceResourceTiming.serverTiming` (`Timing-Allow-Origin: *`, also already set, is what
// makes the timing detail on that entry readable at all) -- read here and turned into a span
// LINK to the exact logit-minted span (haproxy_trace/nginx_trace, demo/logit.yaml) that served
// this resource, in a DIFFERENT trace than the one this resourceFetch span itself lives in.
//
// `Span.addLink()` -- confirmed present on `@opentelemetry/api@1.9.1`'s `Span` interface (added in
// 1.9.0) -- so this uses a real link, not the attribute fallback docs/plans/browser-tracing.md
// left open as a contingency. A `SpanContext` is built by hand from the parsed ids rather than
// going through `propagation.extract`: that API extracts into a `Context` (for making a span a
// CHILD of), there's no public "parse a traceparent into a bare SpanContext" helper, and the
// regex below is the same one every other tier in this stack already hand-parses a traceparent
// with (demo/nginx/nginx.conf's `map` blocks, demo/haproxy/haproxy.cfg's `bytes()` slicing).
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
      // HAProxy always mints `-01` (demo/haproxy/haproxy.cfg's own comment on `trace.flags`) --
      // this is the linked span's own sampled bit, unrelated to whether this browser trace is
      // itself sampled (the WebTracerProvider above has no sampler configured, so it's ALWAYS_ON
      // -- every browser span sends).
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
      // NOT set: confirmed against @opentelemetry/sdk-trace-web@2.11.0's own
      // `shouldPropagateTraceHeaders` (packages/opentelemetry-sdk-trace-web/src/utils.ts) --
      // same-origin requests ALWAYS get the traceparent header regardless of this option; it only
      // gates CROSS-origin urls, and this demo's own fetch() call (the button below, `/work`) is
      // same-origin through HAProxy either way. Left unset rather than set to a value that would
      // do nothing.
    }),
  ],
});

// --- Confirmed empirically: documentLoad is a PARENT of the server span, not a LINK ------------
//
// docs/plans/browser-tracing.md asserts (correctly, per the OTel spec) that the `documentLoad`
// span *should* be a link to the server-side span named by <meta name="traceparent"> -- the
// server's own span has already ended by the time the browser can report anything, so a parent
// edge is semantically backwards. Checked directly against
// @opentelemetry/instrumentation-document-load@0.67.0's own source
// (packages/instrumentation-document-load/src/instrumentation.ts's `_collectPerformance`): it
// does `context.with(propagation.extract(ROOT_CONTEXT, { traceparent }), () => { const rootSpan =
// this._startSpan(...) ... })` with no `links:` anywhere in `_startSpan`. `propagation.extract`
// followed by `context.with` makes the extracted context ACTIVE, and `tracer.startSpan` with no
// explicit parent argument parents to whatever context is active -- so this instrumentation, as
// actually shipped, makes `documentLoad` a real PARENT-child edge to the server span, not a link.
// There is no config option to change this behaviour. Left as the library's real, current
// behaviour rather than fought -- see this PR's description for the full account; nothing in this
// file works around it.

const workButton = document.getElementById("work-btn");
if (workButton) {
  // A real fetch() call, not a plain `<a href="/work">` -- instrumentation-fetch's whole point is
  // context propagation across an async boundary, which a plain navigation doesn't exercise at
  // all. The resulting span becomes a genuine parent of the app's own Django request span for
  // this one call (ZoneContextManager above is what keeps the click handler's context alive into
  // fetch()'s own promise chain).
  workButton.addEventListener("click", () => {
    fetch("/work").catch(() => {
      // Swallowed deliberately -- pages/views.py's `work` view has its own real error paths (an
      // 8% chance 503, a possible 502 from its own inner call); this button exists to trigger
      // them, not to surface them a second time in the browser.
    });
  });
}
