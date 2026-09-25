#!/usr/bin/env python3
"""A tiny Flask service run under `ddtrace-run`, for `script/record-fixtures datadog-*` to capture
what a real dd-trace tracer sends: through a Datadog Agent (`datadog-agent`), or straight to the
capture sink standing in for one (`datadog-tracer`).

It serves three requests to itself (a plain route, a route with a child span and a tag, and a route
answering 500, so a trace carries an error), then waits for the tracer's periodic flushes before
exiting. `RECORD_PATHS` (comma-separated) serves a subset instead: a Flask request is about ten
spans, so a capture that needs only one trace asks for one. Where the traces and stats go is the
tracer's own configuration: `DD_TRACE_AGENT_URL`, `DD_TRACE_API_VERSION`,
`DD_TRACE_STATS_COMPUTATION_ENABLED`, all set by `script/record-fixtures`.

Needs `flask` and `ddtrace`, which `script/record-fixtures` `pip install`s at record time. The
resolved versions print first, for `testdata/interop/datadog/README.md`'s provenance table.
"""

import os
import sys
import threading
import time
import urllib.error
import urllib.request

from importlib.metadata import version

from flask import Flask

PORT = 5050
#: Longer than the tracer's trace flush (1 s) and two of its 10 s client-stats buckets: a bucket
#: is flushed only once the next one has closed.
FLUSH_WAIT_SECONDS = 25

app = Flask("record-fixtures")


@app.route("/checkout")
def checkout():
    return "ok\n"


@app.route("/orders/<int:order_id>")
def order(order_id):
    from ddtrace.trace import tracer

    with tracer.trace("orders.lookup", resource="SELECT order", span_type="sql") as span:
        span.set_tag("order.id", str(order_id))
        time.sleep(0.005)
    return "order {}\n".format(order_id)


@app.route("/fail")
def fail():
    raise RuntimeError("record-fixtures: an intentional error")


def main():
    print("ddtrace=={} flask=={}".format(version("ddtrace"), version("flask")))
    server = threading.Thread(
        target=lambda: app.run(host="127.0.0.1", port=PORT, use_reloader=False), daemon=True
    )
    server.start()
    time.sleep(1.5)
    for path in os.environ.get("RECORD_PATHS", "/checkout,/orders/42,/fail").split(","):
        try:
            with urllib.request.urlopen("http://127.0.0.1:{}{}".format(PORT, path)) as response:
                print("GET {} -> {}".format(path, response.status))
        except urllib.error.HTTPError as err:
            print("GET {} -> {}".format(path, err.code))
    sys.stdout.flush()
    time.sleep(FLUSH_WAIT_SECONDS)
    print("done")


if __name__ == "__main__":
    main()
