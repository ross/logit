"""OpenTelemetry setup shared by every process this app forks workers in --
`demo/app/gunicorn.conf.py`'s web workers and `demo/app/demoproj/celery.py`'s task workers alike
(`docs/plans/demo-richer-traces.md` workstream D). Factored out here rather than duplicated:
both entry points need the identical `TracerProvider`/exporter/instrumentor setup, just a
different `service.name` default and, gunicorn's alone, Django's own request-span instrumentation
layered on top afterward.

Must run after fork in both callers, never at import time -- `gunicorn.conf.py`'s own header
comment has the long version of why (the `BatchSpanProcessor`'s export thread does not survive
`fork()`); Celery's default worker pool forks too (`worker_process_init`, `celery.py`), so the
same constraint applies there unchanged.
"""

import os


def setup(default_service_name):
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.celery import CeleryInstrumentor
    from opentelemetry.instrumentation.logging import LoggingInstrumentor
    from opentelemetry.instrumentation.psycopg import PsycopgInstrumentor
    from opentelemetry.instrumentation.redis import RedisInstrumentor
    from opentelemetry.instrumentation.requests import RequestsInstrumentor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.trace.sampling import ALWAYS_ON

    # `service.name` is what lets Tempo resolve a root service for these spans, the same way
    # demo/logit.yaml's `internal` component stamps `service.name: logit` on its own.
    # `service.namespace` matches every other tier's `set` component (demo/logit.yaml) so this
    # process's spans and its syslog-carried logs agree on identity. `default_service_name` is the
    # caller's own fallback (`demo-app`/`demo-worker`) -- `OTEL_SERVICE_NAME` (demo/compose.yaml)
    # still wins when set, same precedence every other tier's env var follows.
    resource = Resource.create(
        {
            "service.name": os.environ.get("OTEL_SERVICE_NAME", default_service_name),
            "service.namespace": "demo",
        }
    )
    provider = TracerProvider(resource=resource, sampler=ALWAYS_ON)
    # Reads OTEL_EXPORTER_OTLP_ENDPOINT from the environment (demo/compose.yaml: `http://tempo:4318`)
    # and appends `/v1/traces` itself, per the OTLP exporter spec -- Tempo's own OTLP/HTTP receiver
    # (demo/tempo/tempo.yaml's `otlp.protocols.http`), not `logit`'s `otlp_in`. Deliberate --
    # `logit` isn't in this path at all (demo/logit.yaml's header comment explains why); `otlp_in`
    # stays unexercised by this demo. Protobuf, not OTLP/JSON: this exporter package only ever
    # speaks protobuf, which Tempo's receiver accepts natively either way.
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)

    # Stamps otelTraceID/otelSpanID onto every LogRecord created while a span is active --
    # demo/app/pages/logging_formatter.py reads those to build each log line's trace_id/span_id
    # fields. `inject_trace_context=True` is NOT the default -- confirmed by reading this
    # package's own `_instrument` (both `set_logging_format` and `inject_trace_context` default to
    # `False`); calling `instrument()` bare leaves every LogRecord exactly as `old_factory` built
    # it, no `otelTraceID` attribute at all, so `logging_formatter.py`'s
    # `getattr(record, "otelTraceID", None)` silently saw `None` and every log line shipped with no
    # trace fields whatsoever. `set_logging_format=False` (still the default): this app supplies
    # its own formatter rather than asking LoggingInstrumentor to rewrite the root logger's
    # default format string. `enable_log_auto_instrumentation=False`: this app doesn't want a
    # second OTel *logs* pipeline (a `LoggerProvider`/log exporter) it never configured -- only the
    # trace-context injection above.
    LoggingInstrumentor().instrument(
        inject_trace_context=True, enable_log_auto_instrumentation=False
    )

    # `pages/views.py`'s `work` calls back into `nginx` (`docs/plans/demo-richer-traces.md`
    # workstream B) via the stdlib `requests` library -- this instruments every such call into a
    # real CLIENT span, and injects a fresh `traceparent` onto the outbound request via the
    # default W3C propagator, with no code at the call site itself. Harmless, if unused, in the
    # worker: nothing in `pages/tasks.py` makes an outbound HTTP call today.
    RequestsInstrumentor().instrument()

    # `docs/plans/demo-richer-traces.md` workstream C: a real driver-level CLIENT span per SQL
    # statement, and -- via `enable_commenter` -- a `traceparent` appended to the SQL text itself
    # (sqlcommenter, https://google.github.io/sqlcommenter/), the same default W3C propagator
    # injecting it as everywhere else in this module. Postgres logs the whole statement verbatim
    # (`log_min_duration_statement=0`, demo/compose.yaml), so that `traceparent` lands in
    # Postgres's own jsonlog -- what `demo/logit.yaml`'s `postgres_trace` stage lifts back out via
    # a small inline `lua` regex, no SDK on Postgres's side at all. Both `work` (gunicorn) and
    # `pages/tasks.py`'s task (the worker) write to Postgres, so both processes need this.
    PsycopgInstrumentor().instrument(enable_commenter=True)

    # `docs/plans/demo-richer-traces.md` workstream D: a CLIENT span around every broker
    # interaction Celery itself makes over Redis -- publishing a task (gunicorn's own workers,
    # `work`'s `.delay()`) and consuming one (the worker). Instruments the `redis` package
    # directly, so any *other* direct Redis use in either process would get spans too, though
    # nothing here makes one.
    RedisInstrumentor().instrument()

    # The other half of workstream D: a real PRODUCER span around `.delay()` in whichever process
    # calls it (gunicorn's workers), and a real CONSUMER span around task execution in whichever
    # process runs it (the `worker` service) -- the same instrumentor call covers both roles,
    # Celery's own signals decide which fires. The default W3C propagator carries the calling
    # request's `traceparent` through Celery's own message headers with no code in
    # `pages/tasks.py`, so the consumer span is a genuine child of the request that enqueued it,
    # even though by the time it runs the response has already been returned.
    CeleryInstrumentor().instrument()
