"""A `logging.Formatter` that renders one access-log event as the same flat JSON shape
demo/haproxy/haproxy.cfg and demo/nginx/nginx.conf already emit -- `host`/`request_method`/
`status`/`body_bytes_sent`/`request_time` plus a split W3C trace context, over syslog to `logit`.

Trace fields come from `LoggingInstrumentor` (opentelemetry-instrumentation-logging, wired in
demo/app/gunicorn.conf.py's `post_fork`), which stamps every `LogRecord` created while a span is
active with `otelTraceID`/`otelSpanID` -- 32/16-char lowercase hex, or the literal string `"0"`
when there's no active span (record created outside a request, or the span's context is invalid).
This tier emits no `span:` block of its own (demo/logit.yaml) -- `app`'s span is already a real
OTel span, not one `trace_context` would mint from these log fields -- so only the log-correlation
trio matters here: `trace.id`/`span.id`/`trace.flags`, the dotted names
docs/design/data-model.md's "Well-known attribute names" gives them
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


class AccessLogJSONFormatter(logging.Formatter):
    def format(self, record):
        body = {
            "request_method": getattr(record, "request_method", None),
            "path": getattr(record, "request_path", None),
            "status": getattr(record, "status_code", None),
            "body_bytes_sent": getattr(record, "bytes_sent", None),
            "request_time": getattr(record, "request_time", None),
            "host": getattr(record, "request_host", None),
        }

        trace_id = getattr(record, "otelTraceID", None)
        if _is_valid_hex_id(trace_id, 32):
            body["trace.id"] = trace_id
            span_id = getattr(record, "otelSpanID", None)
            if _is_valid_hex_id(span_id, 16):
                body["span.id"] = span_id
            # This demo's `TracerProvider` runs `ALWAYS_ON` (demo/app/gunicorn.conf.py) -- every
            # span it creates is sampled, so `1` is always correct here, not a guess.
            body["trace.flags"] = 1

        body = {k: v for k, v in body.items() if v is not None}
        return json.dumps(body, separators=(",", ":"))
