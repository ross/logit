"""Wires OpenTelemetry tracing into each gunicorn worker (docs/plans/demo-tracing-stack.md's
workstream B).

Done in `post_fork`, not at module import time, and not via the `opentelemetry-instrument` CLI
wrapper: the `BatchSpanProcessor` this sets up owns a background export thread, and a thread does
not survive `fork()` -- only the forking thread's state does, so a `TracerProvider` built before
gunicorn's default (non-`--preload`) sync worker model forks would leave every worker holding a
processor whose export thread only ever existed in the master, silently exporting nothing.
`post_fork` runs inside each freshly forked worker, before that worker imports `demoproj.wsgi`
(`bind`/`workers` below; no `preload_app`), so `DjangoInstrumentor().instrument()` is in place
before Django's own instrumentation-relevant machinery (URL resolution, middleware) loads.
"""

import os

# Set here, not left to demoproj/wsgi.py's `setdefault` -- confirmed live, not just by inference:
# `DjangoInstrumentor().instrument()` below reads Django settings itself (to find `MIDDLEWARE`
# etc.), and if `DJANGO_SETTINGS_MODULE` isn't set yet when it does, Django's lazy `settings`
# object falls back to an empty `settings.configure()`-style holder over `global_settings` -- and
# once that's happened, `demoproj.wsgi`'s later `get_wsgi_application()` no longer loads
# `demoproj.settings` at all (`LazySettings` only consults `DJANGO_SETTINGS_MODULE` the *first*
# time it's forced to configure itself). Every request then 500s with `AttributeError: module
# 'django.conf.global_settings' has no attribute 'ROOT_URLCONF'` -- `demoproj/settings.py` was
# simply never loaded. Setting the env var here, before `post_fork`, closes that ordering gap.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "demoproj.settings")

bind = "0.0.0.0:8080"
workers = int(os.environ.get("WEB_CONCURRENCY", "2"))
# Sync workers, not `gthread`/`gevent` -- fewer moving parts to explain alongside the tracing story
# above; each request runs to completion on its own worker process either way.


def post_fork(server, worker):
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.django import DjangoInstrumentor
    from opentelemetry.instrumentation.logging import LoggingInstrumentor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.trace.sampling import ALWAYS_ON

    # `service.name` is what lets Tempo (and, upstream of it, `logit`'s `otlp_in` -> `trace_out`)
    # resolve a root service for these spans, the same way demo/logit.yaml's `internal` component
    # stamps `service.name: logit` on its own. `service.namespace` matches every other tier's `set`
    # component (demo/logit.yaml) so this app's spans and its syslog-carried logs agree on identity.
    resource = Resource.create(
        {
            "service.name": os.environ.get("OTEL_SERVICE_NAME", "demo-app"),
            "service.namespace": "demo",
        }
    )
    provider = TracerProvider(resource=resource, sampler=ALWAYS_ON)
    # Reads OTEL_EXPORTER_OTLP_ENDPOINT from the environment (demo/compose.yaml) and appends
    # `/v1/traces` itself, per the OTLP exporter spec -- exactly the path `otlp_in` routes
    # (`crates/logit-inputs/src/otlp.rs`'s `route_path`). Protobuf, not OTLP/JSON: `otlp_in`
    # rejects `application/json` outright (same file, ~line 194), and this exporter package only
    # ever speaks protobuf.
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)

    # Stamps otelTraceID/otelSpanID onto every LogRecord created while a span is active --
    # demo/app/pages/logging_formatter.py reads those to build the access log's trace_id/span_id
    # fields. `inject_trace_context=True` is NOT the default -- confirmed by reading this
    # package's own `_instrument` (both `set_logging_format` and `inject_trace_context` default to
    # `False`); calling `instrument()` bare leaves every LogRecord exactly as `old_factory` built
    # it, no `otelTraceID` attribute at all, so demo/app/pages/logging_formatter.py's
    # `getattr(record, "otelTraceID", None)` silently saw `None` and every access log line shipped
    # with no trace fields whatsoever. `set_logging_format=False` (still the default): this app
    # supplies its own formatter rather than asking LoggingInstrumentor to rewrite the root
    # logger's default format string. `enable_log_auto_instrumentation=False`: this app doesn't
    # want a second OTel *logs* pipeline (a `LoggerProvider`/log exporter) it never configured --
    # only the trace-context injection above.
    LoggingInstrumentor().instrument(
        inject_trace_context=True, enable_log_auto_instrumentation=False
    )

    # The default W3C `tracecontext` propagator extracts haproxy's `traceparent` automatically, so
    # this request's server span is a genuine child of haproxy's span with no code here at all.
    DjangoInstrumentor().instrument()
