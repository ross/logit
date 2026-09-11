"""docs/plans/browser-tracing.md's Workstream C: hands this request's own server span context back
to the template as a W3C `traceparent` (https://www.w3.org/TR/trace-context/), rendered into
<meta name="traceparent"> (pages/templates/pages/index.html) -- what
`@opentelemetry/instrumentation-document-load` reads to associate its `documentLoad` span with
this request's own trace. `DjangoInstrumentor().instrument()` (demo/app/gunicorn.conf.py) makes
this request's span the active one for the whole request/response cycle, template rendering
included, so `trace.get_current_span()` here is that same span -- no extra wiring needed.
"""

from opentelemetry import trace


def traceparent(request):
    span_context = trace.get_current_span().get_span_context()
    if not span_context.is_valid:
        # No active span -- e.g. `DjangoInstrumentor` not yet instrumented (shouldn't happen once
        # gunicorn's `post_fork` has run, but this context processor runs for every template
        # render in this project, not just index.html, so it stays defensive). An empty attribute
        # is what instrumentation-document-load treats as "no traceparent" -- same as not
        # rendering the tag at all.
        return {"traceparent": ""}
    return {
        "traceparent": (
            f"00-{span_context.trace_id:032x}-{span_context.span_id:016x}-"
            f"{span_context.trace_flags:02x}"
        )
    }
