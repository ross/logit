"""`logging.Formatter`s that render one event as flat JSON plus a split W3C trace context, over
syslog to `logit` -- the same shape demo/haproxy/haproxy.cfg and demo/nginx/nginx.conf already
emit for their own access lines. `TraceContextJSONFormatter` is the shared base: every subclass
just names which record attributes become which JSON keys (`FIELDS`); the trace-field logic is
identical everywhere, so it lives here once rather than once per formatter
(`docs/plans/demo-richer-traces.md` workstream D generalized this from a single access-log-only
formatter).

Trace fields come from `LoggingInstrumentor` (opentelemetry-instrumentation-logging, wired in
`demoproj/telemetry.py`, shared by every process this app forks workers in), which stamps every
`LogRecord` created while a span is active with `otelTraceID`/`otelSpanID` -- 32/16-char lowercase
hex, or the literal string `"0"` when there's no active span (record created outside a request or
task, or the span's context is invalid). This tier emits no `span:` block of its own
(demo/logit.yaml) -- every span it produces is already real
(opentelemetry-instrumentation-django/-celery/-psycopg), not one `trace_context` would mint from a
log line -- so only the log-correlation trio matters here: `trace.id`/`span.id`/`trace.flags`, the
dotted names docs/design/data-model.md's "Well-known attribute names" gives them
(docs/adr/trace-context-span-lifting.md; the flags field stays decimal-only regardless of the
dotted rename -- `crates/logit-transforms/src/trace_context.rs`). Omitted entirely rather than
sent as "0" when there is no real trace (demo/logit.yaml's `trace_context` then reports
`skipped{reason="missing"}`, not the noisier `invalid`).
"""

import json
import logging


def _is_valid_hex_id(value, length):
    return (
        isinstance(value, str)
        and len(value) == length
        and value != "0" * length
        and all(c in "0123456789abcdef" for c in value)
    )


class TraceContextJSONFormatter(logging.Formatter):
    """Subclasses set `FIELDS`: a `{json_key: record_attribute}` mapping naming this formatter's
    own non-trace fields. `None` values are dropped, matching the convention every other tier's
    JSON access line already follows -- an absent field, not a null one.
    """

    FIELDS = {}

    def format(self, record):
        body = {key: getattr(record, attr, None) for key, attr in self.FIELDS.items()}

        trace_id = getattr(record, "otelTraceID", None)
        if _is_valid_hex_id(trace_id, 32):
            body["trace.id"] = trace_id
            span_id = getattr(record, "otelSpanID", None)
            if _is_valid_hex_id(span_id, 16):
                body["span.id"] = span_id
            # This demo's `TracerProvider` runs `ALWAYS_ON` (demoproj/telemetry.py) -- every span
            # it creates is sampled, so `1` is always correct here, not a guess.
            body["trace.flags"] = 1

        body = {k: v for k, v in body.items() if v is not None}
        return json.dumps(body, separators=(",", ":"))


class AccessLogJSONFormatter(TraceContextJSONFormatter):
    FIELDS = {
        "request_method": "request_method",
        "path": "request_path",
        "status": "status_code",
        "body_bytes_sent": "bytes_sent",
        "request_time": "request_time",
        "host": "request_host",
    }


class WorkerLogJSONFormatter(TraceContextJSONFormatter):
    # `pages/tasks.py`'s own event, not an access line -- no `host`/`status`, since a background
    # task answers no request of its own.
    FIELDS = {
        "task": "task_name",
        "duration_ms": "duration_ms",
        "record_id": "record_id",
    }
