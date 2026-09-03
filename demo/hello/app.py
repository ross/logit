#!/usr/bin/env python3
"""The demo's front door: a one-page app that (a) tells a first-time visitor how to get into
Grafana, (b) shows the running pipeline as an SVG rendered live from `logit graph`, and (c) is
itself the traffic source demo/logit.yaml consumes.

Stdlib only, on purpose -- demo/compose.yaml bind-mounts this single file into a stock
python:*-slim image and runs it. No Dockerfile, no requirements.txt, nothing to build.

Every log line goes out as RFC 3164 syslog over UDP to logit's syslog_in listener, with a JSON
body -- the same shape crates/logit-bench/src/fixtures.rs measures (NGINX_SYSLOG_LINE), which is
what makes demo/logit.yaml's json -> kv_metrics -> keep -> aggregate chain meaningful. Two
producers share that one emitter:

  - a background thread of synthetic traffic, so the Grafana dashboard has something to draw
    within seconds of `docker compose up` and keeps moving whether or not anyone visits;
  - the real HTTP handler, which logs each actual request it served.

The synthetic loop fabricates varied methods/paths/statuses *inside the JSON body only*. Those
are log-body values, not routes -- this server really does serve exactly two paths.
"""

import http.server
import json
import os
import random
import socket
import threading
import time

# `logit` is a network alias on the `demo` network (demo/compose.yaml). UDP is fire-and-forget:
# if the listener isn't bound yet, or ever, these sends are simply lost, which is why nothing
# here blocks on or retries them.
LOGIT_HOST = os.environ.get("LOGIT_HOST", "logit")
LOGIT_PORT = int(os.environ.get("LOGIT_PORT", "5140"))

LISTEN_PORT = int(os.environ.get("PORT", "8080"))
GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://localhost:3000")

# Written by the `graph-svg` one-shot service into the shared `graph_data` volume, mounted here
# read-only. May not exist yet on the very first request -- see _load_graph_svg below.
SVG_PATH = os.environ.get("GRAPH_SVG", "/graph/logit.svg")

# PRI 134 = facility 16 (local0), severity 6 (info) -- the same priority nginx's `access_log
# syslog:` directive used in the pre-reset demo.
PRI = 134
SYSLOG_HOST = "demo-hello"     # RFC 3164 HOSTNAME token -- must not end in ':', or syslog_in
                                # reads it as the TAG instead (crates/logit-inputs/src/syslog.rs).
SYSLOG_TAG = "demoapp"
LOG_HOST_FIELD = "demo.local"  # the JSON body's `host` field -- one of the three tags that
                                # survive `keep` in demo/logit.yaml.

_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
_request_id = 0
_request_id_lock = threading.Lock()


def _next_request_id():
    global _request_id
    with _request_id_lock:
        _request_id += 1
        return _request_id


def _syslog_timestamp():
    # RFC 3164's "%b %e %H:%M:%S": a space-padded day-of-month, so the 1st is "Sep  1" (two
    # spaces). Built by hand rather than strftime("%e"), which is a glibc extension not every
    # libc guarantees.
    now = time.localtime()
    return "%s %2d %02d:%02d:%02d" % (
        time.strftime("%b", now), now.tm_mday, now.tm_hour, now.tm_min, now.tm_sec
    )


def emit_log_line(method, path, status, bytes_sent, request_time, host=LOG_HOST_FIELD,
                   request_id=None):
    """Send one access-log event to `logit`.

    `request_method`, `status`, `body_bytes_sent`, `request_time`, and `host` are the fields
    demo/logit.yaml's `kv_metrics` and `keep` stages name; `path` and `request_id` ride along and
    are dropped by `keep` (they'd otherwise be per-request cardinality in `aggregate`'s
    SeriesKey).
    """
    body = {
        "request_method": method,
        "path": path,
        "status": int(status),
        "body_bytes_sent": int(bytes_sent),
        "request_time": round(float(request_time), 3),
        "host": host,
        "request_id": str(request_id if request_id is not None else _next_request_id()),
    }
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


# -- Synthetic background traffic --------------------------------------------------------------
#
# Deliberately small and easy to extend -- add to these lists, or replace _synthetic_once with
# something with more structure. These are invented log content, not real routes; see the module
# docstring.
SYNTH_METHODS = ["GET", "GET", "GET", "GET", "POST", "PUT", "DELETE"]
SYNTH_PATHS = ["/", "/login", "/api/items", "/api/items/42", "/static/app.js", "/health"]
SYNTH_STATUSES = [200, 200, 200, 200, 201, 204, 301, 404, 500]
SYNTH_INTERVAL_SECONDS = float(os.environ.get("SYNTH_INTERVAL", "0.5"))


def _synthetic_once():
    emit_log_line(
        method=random.choice(SYNTH_METHODS),
        path=random.choice(SYNTH_PATHS),
        status=random.choice(SYNTH_STATUSES),
        bytes_sent=random.randint(200, 20200),
        request_time=random.randint(0, 999) / 1000.0,
    )


def _synthetic_loop():
    while True:
        _synthetic_once()
        time.sleep(SYNTH_INTERVAL_SECONDS)


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

<p>This page is the demo's traffic source. It logs every visit -- and a few synthetic requests a
second in the background -- as RFC 3164 syslog over UDP to <code>logit</code>, which parses,
counts, aggregates, and writes the result to InfluxDB.</p>

<h2>See it in Grafana</h2>
<ol>
  <li>Open <a href="__GRAFANA_URL__">__GRAFANA_URL__</a> -- anonymous access is on, no login.</li>
  <li>Dashboards &rarr; the <strong>logit</strong> folder &rarr; <strong>logit internals</strong>.</li>
  <li>Give it ten seconds: <code>aggregate</code> flushes on a 10s window.</li>
</ol>
<p>Loki and Tempo are up and provisioned too -- Loki is receiving this app's own logs via
<code>syslog_out</code> and Grafana Alloy, but Tempo stays empty by design: <code>logit</code> has
no <code>otlp_out</code> yet. See <code>demo/README.md</code>.</p>

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

        if path == "/":
            sent = self._respond(200, "text/html; charset=utf-8", _render_page())
        elif path == "/graph.svg":
            sent = self._respond(200, "image/svg+xml", _load_graph_svg())
        else:
            self._respond(404, "text/plain; charset=utf-8", b"not found\n")
            return  # not part of the demo's surface, not worth a metric

        emit_log_line(
            method="GET", path=path, status=200, bytes_sent=sent,
            request_time=time.monotonic() - started,
        )


def main():
    threading.Thread(target=_synthetic_loop, daemon=True).start()
    server = http.server.ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), Handler)
    print("listening on 0.0.0.0:%d, logging to %s:%d" % (LISTEN_PORT, LOGIT_HOST, LOGIT_PORT),
          flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
