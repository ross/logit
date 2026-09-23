"""The views this tier serves (docs/plans/demo-tracing-stack.md): the landing page, its two
live-rendered diagrams, the browser telemetry bundle, a health check for demo/compose.yaml's
`traffic` generator, and the `/work`, `/inner`, and `/boom` routes that give the demo's traces
their shape. Every request gets `opentelemetry-instrumentation-django`'s automatic request span
(demo/app/gunicorn.conf.py).
"""

import os
import random
import time

import requests
from django.http import Http404, HttpResponse, JsonResponse
from django.shortcuts import render

from pages.models import WorkRecord
from pages.tasks import background_work

GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://localhost:3000")

# nginx, not haproxy (`docs/plans/demo-richer-traces.md`), so this hop takes a different path
# than the request that triggered it, while still landing on a tier whose access log `logit` turns
# into a real span (`nginx_trace`'s `span:` block, demo/logit.yaml) rather than routing straight
# back to app. `RequestsInstrumentor` (demo/app/demoproj/telemetry.py) injects a fresh
# `traceparent` on this call with no code here, and nginx's `map` blocks (demo/nginx/nginx.conf)
# on the receiving end make the resulting nginx span a genuine child of this request's span.
INNER_URL = os.environ.get("INNER_URL", "http://nginx/inner")

# Both written by one-shot services into the shared `graph_data` volume, mounted here read-only
# (demo/compose.yaml): `logit.svg` by graph-dot -> graph-svg (`logit graph`, from the running
# config), and `architecture.svg` by arch-svg (demo/architecture.dot, hand-authored; its header
# comment says why there's no generation step). Either may not exist yet on the first request.
GRAPH_SVG_PATH = os.environ.get("GRAPH_SVG", "/graph/logit.svg")
ARCH_SVG_PATH = os.environ.get("ARCH_SVG", "/graph/architecture.svg")

# ../Dockerfile's `bundle` stage's only output: the real @opentelemetry/sdk-trace-web SDK,
# esbuild-bundled from browser/telemetry.js (docs/plans/browser-tracing.md). The image's
# `WORKDIR` is `/app`, so this is relative to this tier's own root, unlike GRAPH_SVG_PATH/
# ARCH_SVG_PATH above (a shared volume another service writes into).
BROWSER_BUNDLE_PATH = os.environ.get("BROWSER_BUNDLE_PATH", "browser/telemetry.bundle.js")


def _svg_placeholder(label):
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="460" height="60">'
        f'<text x="10" y="35" font-family="monospace" font-size="14">'
        f"{label} not rendered yet -- refresh in a moment</text></svg>"
    ).encode("utf-8")


def _serve_svg(path, placeholder_label):
    # Opened fresh per request, not cached: the one-shot renderer that writes this file may not
    # have finished (or, under a compose implementation that doesn't honor
    # service_completed_successfully, even started) when this tier starts serving. A page load a
    # few seconds later then works, with no restart.
    try:
        with open(path, "rb") as fh:
            payload = fh.read()
    except OSError:
        payload = _svg_placeholder(placeholder_label)
    return HttpResponse(payload, content_type="image/svg+xml")


def browser_telemetry_js(request):
    # Deliberately NOT `_serve_svg`'s placeholder-on-`OSError` pattern, despite reading a file the
    # same "open fresh per request" way. `graph.svg`/`architecture.svg` are written by one-shot
    # renderer containers that race with this tier's startup, so a missing file there is a
    # normal, transient "hasn't run yet". This bundle has no such race: ../Dockerfile's `bundle`
    # stage produces it at image-build time, `COPY --from=bundle` puts it in the image, and this
    # tier can't start serving without that COPY having succeeded. A missing file here means a
    # broken image, not a timing window. A real 404 says so; a placeholder script would load fine
    # and silently trace nothing, the worst failure mode for a demo whose point is showing tracing
    # working.
    try:
        with open(BROWSER_BUNDLE_PATH, "rb") as fh:
            payload = fh.read()
    except OSError as exc:
        raise Http404(f"browser telemetry bundle not found: {exc}") from exc
    # Not `application/javascript`: RFC 9239 obsoletes it in favor of `text/javascript`.
    # index.html's `<script type="module">` tag accepts either, but the modern MIME type costs
    # nothing.
    return HttpResponse(payload, content_type="text/javascript; charset=utf-8")


def index(request):
    return render(request, "pages/index.html", {"grafana_url": GRAFANA_URL})


def graph_svg(request):
    return _serve_svg(GRAPH_SVG_PATH, "pipeline graph")


def architecture_svg(request):
    return _serve_svg(ARCH_SVG_PATH, "architecture diagram")


def health(request):
    return HttpResponse(b"ok\n", content_type="text/plain; charset=utf-8")


# `work` and `boom` below exist to give the demo's traces a real *shape*: some latency spread and
# some real errors (`docs/plans/demo-richer-traces.md`). Without them, `traffic`
# (demo/compose.yaml) only produces flat 200s, and two of the shipped dashboard panels
# ("Loki: 5xx lines/sec", the `web.request_time` p50/p99 pair) plot nothing or a flat line.


def work(request):
    # Jittered, not fixed: a flat sleep would leave `web.request_time`'s p50/p99 collapsed onto
    # one value. Kept short (well under a second) because `demo/app/gunicorn.conf.py`'s sync
    # workers are a shared, finite pool, and this view also issues a blocking inbound request
    # below that needs a *different* worker to answer it. Long sleeps here shrink that headroom
    # for no benefit. `WEB_CONCURRENCY` is 4 for the same reason (demo/compose.yaml): at 2, two
    # concurrent `/work` requests each hold a worker while blocking on `/inner`, which needs a
    # worker of its own to answer, and they self-deadlock.
    time.sleep(random.uniform(0.02, 0.25))
    # A real error path, not a hand-set status code: `SpanStatus::Error` on haproxy's and nginx's
    # logit-minted spans is derived (demo/logit.yaml's `haproxy_http`/`nginx_http`) from the
    # status actually observed on the wire, so this has to be a genuine 5xx response, not a 200
    # that claims one in its body. Checked before the outbound call below, not after: this models
    # the app declining the work itself, rather than a downstream failure.
    if random.random() < 0.08:
        return HttpResponse(b"temporarily overloaded\n", status=503)

    # A real write and a real read against Postgres (`docs/plans/demo-richer-traces.md`), each its
    # own driver-level CLIENT span (opentelemetry-instrumentation-psycopg,
    # demo/app/demoproj/telemetry.py). The same instrumentation's sqlcommenter puts a
    # `traceparent` in the SQL text itself, which `demo/logit.yaml`'s `postgres_trace` stage lifts
    # back out of Postgres's jsonlog.
    WorkRecord.objects.create()
    count = WorkRecord.objects.count()

    # Hands the rest of the "work" to a real background worker over Redis
    # (`docs/plans/demo-richer-traces.md`). `.delay()` is fire-and-forget (this view never waits on
    # the result), but it's still a real Celery PRODUCER span (opentelemetry-instrumentation-celery,
    # demoproj/telemetry.py) parented to this request's span, with a real Redis CLIENT span
    # (opentelemetry-instrumentation-redis) underneath it for the publish. The `worker` service
    # picks it up seconds later, so its spans arrive in Tempo after this request's response has
    # gone back to the client: one trace whose spans don't all finish before the HTTP response
    # does.
    background_work.delay()

    # The re-entrant hop (`docs/plans/demo-richer-traces.md`): back through nginx (not haproxy;
    # see `INNER_URL`'s comment above), giving one trace a real subtree instead of a single chain.
    # A failure here (nginx or `/inner` down, or the timeout below) is reported, not swallowed: a
    # 502 is the honest status for "this tier's own upstream call failed."
    try:
        response = requests.get(INNER_URL, timeout=2)
        response.raise_for_status()
    except requests.RequestException as exc:
        return HttpResponse(f"inner call failed: {exc}\n".encode(), status=502)

    return HttpResponse(
        f"work done ({count} records)\n".encode(), content_type="text/plain; charset=utf-8"
    )


def inner(request):
    # The far end of `work`'s outbound call above, reached through nginx: a real second hop with
    # its own logit-minted server span (demo/logit.yaml's `nginx_trace`), not a direct call back
    # into this process.
    time.sleep(random.uniform(0.01, 0.15))
    return JsonResponse({"status": "ok"})


def boom(request):
    # Deliberately uncaught: Django turns this into a 500, so the OTel SDK's request span
    # (opentelemetry-instrumentation-django, demo/app/gunicorn.conf.py) records it as a real
    # exception span *event*. `trace_context` (crates/logit-transforms/src/trace_context.rs) never
    # mints span events on the spans it lifts from a plain access log line, so this is the only
    # path to a span event anywhere in this demo.
    raise RuntimeError("boom: this route always fails, on purpose")
