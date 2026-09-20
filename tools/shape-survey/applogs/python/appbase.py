"""The HTTP skeleton every `applogs` Python app shares -- routes, timing, and the fixed
access-log field set, with logging itself left entirely to the app that imports it.

Deliberately stdlib-only (`http.server.ThreadingHTTPServer`): the survey is measuring what a
*logging library* emits, so a web framework in the picture would only add its own fields and its
own install cost. The routes and the field names here are identical across the Python, Node, Go
and Ruby apps so that a width difference between two sources is the library's envelope and not a
different app.

THE FIELD SET IS FIXED AND SHARED ON PURPOSE. structlog, python-json-logger, log/slog and zap
have no request serializer of their own -- an application hands them fields. These eight are the
canonical access-log fields (nginx's combined format, essentially), so a row's width reads as
"library envelope + 8", and the library envelopes stay comparable. pino-http is the one app here
that supplies its *own* serializers, and its row is labelled as such.
"""

import http.server
import random
import time
import traceback
import urllib.parse
import uuid

FIELDS = (
    "method",
    "path",
    "status",
    "duration_ms",
    "bytes",
    "remote_addr",
    "user_agent",
    "request_id",
)


def route(path):
    """The shared route table: (status, body, raise?) for a request path.

    `/boom` raises, so every app logs a real exception with a real traceback through its
    library's own exception path -- the widest record any of them produces.
    """
    parsed = urllib.parse.urlparse(path)
    segments = [s for s in parsed.path.split("/") if s]
    if not segments:
        return 200, b"ok\n", False
    if segments[0] == "boom":
        return 500, b"internal server error\n", True
    if segments[0] == "items":
        if len(segments) > 1 and segments[1] == "0":
            return 404, b"not found\n", False
        return 200, b'{"items":[]}\n', False
    if segments[0] == "search":
        return 200, b'{"results":[]}\n', False
    if segments[0] == "healthz":
        return 200, b"ok\n", False
    return 404, b"not found\n", False


class Handler(http.server.BaseHTTPRequestHandler):
    """Routes, times and answers; `on_request` (set by the app) does all the logging."""

    protocol_version = "HTTP/1.1"
    on_request = None
    seq = 0

    def do_GET(self):  # noqa: N802 -- BaseHTTPRequestHandler's own naming
        started = time.perf_counter()
        status, body, boom = route(self.path)
        exc = None
        if boom:
            try:
                raise ValueError(f"synthetic failure serving {self.path}")
            except ValueError as e:  # the app logs it; the request still gets a 500
                exc = e
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

        type(self).seq += 1
        fields = {
            "method": self.command,
            "path": self.path,
            "status": status,
            "duration_ms": round((time.perf_counter() - started) * 1000, 3),
            "bytes": len(body),
            "remote_addr": self.client_address[0],
            "user_agent": self.headers.get("User-Agent", "-"),
            "request_id": uuid.uuid4().hex,
        }
        type(self).on_request(fields, exc, type(self).seq)

    def log_message(self, *args):
        """Silence BaseHTTPRequestHandler's own stderr access line -- it is not a library under
        measurement, and it would otherwise land in the same stream."""


def serve(port, on_request):
    Handler.on_request = staticmethod(on_request)
    random.seed(0)
    http.server.ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
