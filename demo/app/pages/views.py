"""The three views this tier serves: the landing page, the live-rendered pipeline diagram, and a
health check for demo/compose.yaml's `traffic` generator. Replaces demo/hello/app.py's stdlib
`http.server` handler (docs/plans/demo-tracing-stack.md's workstream B) -- same two content routes,
now behind Django's URL dispatcher, template engine, and (demo/app/gunicorn.conf.py)
`opentelemetry-instrumentation-django`'s automatic request span.
"""

import os

from django.http import HttpResponse
from django.shortcuts import render

GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://localhost:3000")

# Written by the `graph-svg` one-shot service into the shared `graph_data` volume, mounted here
# read-only (demo/compose.yaml). May not exist yet on the very first request.
SVG_PATH = os.environ.get("GRAPH_SVG", "/graph/logit.svg")

PLACEHOLDER_SVG = (
    b'<svg xmlns="http://www.w3.org/2000/svg" width="460" height="60">'
    b'<text x="10" y="35" font-family="monospace" font-size="14">'
    b'pipeline graph not rendered yet -- refresh in a moment</text></svg>'
)


def index(request):
    return render(request, "pages/index.html", {"grafana_url": GRAFANA_URL})


def graph_svg(request):
    # Opened fresh per request, not cached: graph-svg is a one-shot init container that may not
    # have finished yet (or, under a compose implementation that doesn't honor
    # service_completed_successfully, may not have even started) when this tier first starts
    # serving -- so a page load a few seconds later just works, no restart needed. Same contract
    # demo/hello/app.py's _load_graph_svg had.
    try:
        with open(SVG_PATH, "rb") as fh:
            payload = fh.read()
    except OSError:
        payload = PLACEHOLDER_SVG
    return HttpResponse(payload, content_type="image/svg+xml")


def health(request):
    return HttpResponse(b"ok\n", content_type="text/plain; charset=utf-8")
