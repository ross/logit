#!/usr/bin/env python3
"""The demo's innermost tier: a one-page app that (a) tells a first-time visitor how to get into
Grafana and (b) shows the running pipeline as an SVG rendered live from `logit graph`.

Stdlib only, on purpose -- demo/compose.yaml bind-mounts this single file into a stock
python:*-slim image and runs it. No Dockerfile, no requirements.txt, nothing to build. (This
tier is replaced by a real Django app in docs/plans/demo-tracing-stack.md's workstream B --
until then, it stays the stdlib stand-in.)

Sits behind haproxy -> nginx (docs/plans/demo-tracing-stack.md's workstream A). Every request it
serves logs one line as RFC 3164 syslog over UDP to logit's `app_in` listener, with a JSON body --
the same shape crates/logit-bench/src/fixtures.rs measures (NGINX_SYSLOG_LINE) plus the split W3C
trace context, which is what demo/logit.yaml's json -> trace_context chain expects. The app is no
longer its own traffic source: demo/compose.yaml's `traffic` service drives requests through the
whole haproxy -> nginx -> here chain instead, so every log line -- real or synthetic -- carries a
trace context tying all three tiers' log lines (and, from workstream B on, a Tempo span) together.
"""

import http.server
import json
import os
import re
import socket
import time

# `logit` is a network alias on the `demo` network (demo/compose.yaml). UDP is fire-and-forget:
# if the listener isn't bound yet, or ever, these sends are simply lost, which is why nothing
# here blocks on or retries them.
LOGIT_HOST = os.environ.get("LOGIT_HOST", "logit")
LOGIT_PORT = int(os.environ.get("LOGIT_PORT", "5142"))  # app_in, not haproxy_in/nginx_in

LISTEN_PORT = int(os.environ.get("PORT", "8080"))
GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://localhost:3000")

# Written by the `graph-svg` one-shot service into the shared `graph_data` volume, mounted here
# read-only. May not exist yet on the very first request -- see _load_graph_svg below.
SVG_PATH = os.environ.get("GRAPH_SVG", "/graph/logit.svg")

# PRI 134 = facility 16 (local0), severity 6 (info) -- matches demo/haproxy/haproxy.cfg's and
# demo/nginx/nginx.conf's own access-log priority, for a consistent look across all three tiers.
PRI = 134
SYSLOG_HOST = "demo-hello"     # RFC 3164 HOSTNAME token -- must not end in ':', or syslog_in
                                # reads it as the TAG instead (crates/logit-inputs/src/syslog.rs).
SYSLOG_TAG = "demoapp"
LOG_HOST_FIELD = "demo.local"  # the JSON body's `host` field -- one of the three tags that
                                # survive `keep` in demo/logit.yaml.

# W3C Trace Context (https://www.w3.org/TR/trace-context/): "00-<32 hex trace id>-<16 hex span
# id>-<2 hex flags>". `logit` has no traceparent parser (demo/haproxy/haproxy.cfg's header
# comment explains why) -- so, like the other two tiers, this splits it by hand into separate
# fields for demo/logit.yaml's `trace_context` transform. `trace_flags` is sent as a DECIMAL int
# (never the hex octet) -- trace_context's `flags` field rejects hex by design
# (crates/logit-transforms/src/trace_context.rs::numeric_flags).
_TRACEPARENT_RE = re.compile(
    r"^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$"
)

_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


def _syslog_timestamp():
    # RFC 3164's "%b %e %H:%M:%S": a space-padded day-of-month, so the 1st is "Sep  1" (two
    # spaces). Built by hand rather than strftime("%e"), which is a glibc extension not every
    # libc guarantees.
    now = time.localtime()
    return "%s %2d %02d:%02d:%02d" % (
        time.strftime("%b", now), now.tm_mday, now.tm_hour, now.tm_min, now.tm_sec
    )


def _trace_fields(traceparent):
    """Split an inbound `traceparent` header into the JSON body's trace fields.

    Returns {} when the header is absent or malformed -- trace_context then reports
    skipped{reason="missing"}, not the noisier "invalid", the same choice demo/nginx/nginx.conf's
    `map` blocks make (no `default` => empty string => Missing, not Invalid).
    """
    if not traceparent:
        return {}
    m = _TRACEPARENT_RE.match(traceparent)
    if not m:
        return {}
    trace_id, span_id, flags_hex = m.groups()
    return {
        "trace_id": trace_id,
        "span_id": span_id,
        "trace_flags": int(flags_hex, 16) & 1,  # decimal, sampled-bit only
    }


def emit_log_line(method, path, status, bytes_sent, request_time, traceparent,
                   host=LOG_HOST_FIELD):
    """Send one access-log event to `logit`.

    `request_method`, `status`, `body_bytes_sent`, `request_time`, and `host` are the fields
    demo/logit.yaml's `kv_metrics` and `keep` stages name (mirrored here for a consistent shape
    across tiers, even though the metrics leg sources the nginx tier only); `path` rides along
    and is dropped by `keep`, same as `trace_id`/`span_id` (per-request cardinality in
    `aggregate`'s SeriesKey).
    """
    body = {
        "request_method": method,
        "path": path,
        "status": int(status),
        "body_bytes_sent": int(bytes_sent),
        "request_time": round(float(request_time), 3),
        "host": host,
    }
    body.update(_trace_fields(traceparent))
    line = "<%d>%s %s %s: %s" % (
        PRI, _syslog_timestamp(), SYSLOG_HOST, SYSLOG_TAG,
        json.dumps(body, separators=(",", ":")),
    )
    try:
        _sock.sendto(line.encode("utf-8"), (LOGIT_HOST, LOGIT_PORT))
    except OSError:
        # Name resolution or the socket itself failing isn't worth taking the web server down
        # for -- the demo is still useful with the metrics half dark.
        pass


# -- The one page ---------------------------------------------------------------------------

PAGE_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>logit demo</title>
<style>
  body { font-family: system-ui, sans-serif; max-width: 46rem; margin: 3rem auto;
         padding: 0 1rem; line-height: 1.5; }
  code { background: #f0f0f0; padding: 0.1em 0.3em; }
  img { max-width: 100%; border: 1px solid #ddd; }
  .caption { color: #666; font-size: 0.85rem; }
</style>
</head>
<body>
<h1>Hello from the <code>logit</code> demo</h1>

<p>This page sits behind haproxy and nginx -- every request through that chain gets a W3C
<code>traceparent</code> minted at the edge, and all three tiers log the same trace id as RFC 3164
syslog over UDP to <code>logit</code>, which parses, counts, aggregates, and writes the result to
Loki, InfluxDB, and Tempo.</p>

<h2>See it in Grafana</h2>
<ol>
  <li>Open <a href="__GRAFANA_URL__">__GRAFANA_URL__</a> -- anonymous access is on, no login.</li>
  <li>Dashboards &rarr; the <strong>logit</strong> folder &rarr; <strong>logit internals</strong>.</li>
  <li>Give it ten seconds: <code>aggregate</code> flushes on a 10s window.</li>
</ol>

<h2>The pipeline</h2>
<img src="/graph.svg" alt="logit pipeline graph">
<p class="caption">Rendered at startup from this stack's own config:
<code>logit graph demo/logit.yaml | dot -Tsvg</code>.</p>
</body>
</html>
"""

PLACEHOLDER_SVG = (
    b'<svg xmlns="http://www.w3.org/2000/svg" width="460" height="60">'
    b'<text x="10" y="35" font-family="monospace" font-size="14">'
    b'pipeline graph not rendered yet -- refresh in a moment</text></svg>'
)


def _render_page():
    return PAGE_TEMPLATE.replace("__GRAFANA_URL__", GRAFANA_URL).encode("utf-8")


def _load_graph_svg():
    # Opened fresh per request, not cached: graph-svg is a one-shot init container that may not
    # have finished yet (or, under a compose implementation that doesn't honor
    # service_completed_successfully, may not have even started) when `hello` first starts
    # serving -- so a page load a few seconds later just works, no restart needed.
    try:
        with open(SVG_PATH, "rb") as fh:
            return fh.read()
    except OSError:
        return PLACEHOLDER_SVG


class Handler(http.server.BaseHTTPRequestHandler):
    server_version = "logit-demo/1.0"

    def log_message(self, fmt, *args):
        pass  # the real access log is the syslog line emit_log_line sends, not stderr

    def _respond(self, status, content_type, payload):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)
        return len(payload)

    def do_GET(self):
        started = time.monotonic()
        path = self.path.split("?", 1)[0]
        traceparent = self.headers.get("traceparent", "")

        if path == "/":
            sent = self._respond(200, "text/html; charset=utf-8", _render_page())
            status = 200
        elif path == "/graph.svg":
            sent = self._respond(200, "image/svg+xml", _load_graph_svg())
            status = 200
        elif path == "/health":
            sent = self._respond(200, "text/plain; charset=utf-8", b"ok\n")
            status = 200
        else:
            sent = self._respond(404, "text/plain; charset=utf-8", b"not found\n")
            status = 404

        emit_log_line(
            method="GET", path=path, status=status, bytes_sent=sent,
            request_time=time.monotonic() - started, traceparent=traceparent,
        )


def main():
    server = http.server.ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), Handler)
    print("listening on 0.0.0.0:%d, logging to %s:%d" % (LISTEN_PORT, LOGIT_HOST, LOGIT_PORT),
          flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
