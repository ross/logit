"""One access-log line per request, logged from inside the request.

gunicorn's own `gunicorn.access` logger runs in gunicorn's arbiter/worker code, outside Django's
request handling, so it never sees a `LogRecord` with a real trace context
(demo/app/pages/logging_formatter.py). `opentelemetry-instrumentation-django` doesn't add itself
to `MIDDLEWARE` (demo/app/demoproj/settings.py); it wraps the WSGI handler directly
(`DjangoInstrumentor().instrument()`, demo/app/gunicorn.conf.py). So its request span is active
for Django's *entire* request/response cycle, including this middleware wherever it sits in
`MIDDLEWARE`. It's placed last (innermost) for a different reason: to see the real status
code/body length the view produced, not an earlier middleware's guess.
"""

import logging
import time

access_logger = logging.getLogger("demoapp.access")


class AccessLogMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        started = time.monotonic()
        response = self.get_response(request)
        elapsed = time.monotonic() - started

        bytes_sent = response.get("Content-Length")
        bytes_sent = int(bytes_sent) if bytes_sent is not None else len(response.content)

        access_logger.info(
            "access",
            extra={
                "request_method": request.method,
                "request_path": request.path,
                "status_code": response.status_code,
                "bytes_sent": bytes_sent,
                "request_time": round(elapsed, 3),
                "request_host": request.get_host(),
            },
        )
        return response
