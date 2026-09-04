"""The views this tier serves: the landing page, its two live-rendered diagrams, and a health
check for demo/compose.yaml's `traffic` generator. Replaces demo/hello/app.py's stdlib
`http.server` handler (docs/plans/demo-tracing-stack.md's workstream B) -- same two content
routes plus one more, now behind Django's URL dispatcher, template engine, and
(demo/app/gunicorn.conf.py) `opentelemetry-instrumentation-django`'s automatic request span.
"""

import os

from django.http import HttpResponse
from django.shortcuts import render

GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://localhost:3000")

# Both written by one-shot services into the shared `graph_data` volume, mounted here read-only
# (demo/compose.yaml) -- `logit.svg` by graph-dot -> graph-svg (`logit graph`, live from the
# actual running config); `architecture.svg` by arch-svg (demo/architecture.dot, hand-authored --
# see its own header comment for why there's no equivalent generation step). Either may not exist
# yet on the very first request.
GRAPH_SVG_PATH = os.environ.get("GRAPH_SVG", "/graph/logit.svg")
ARCH_SVG_PATH = os.environ.get("ARCH_SVG", "/graph/architecture.svg")


def _svg_placeholder(label):
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="460" height="60">'
        f'<text x="10" y="35" font-family="monospace" font-size="14">'
        f"{label} not rendered yet -- refresh in a moment</text></svg>"
    ).encode("utf-8")


def _serve_svg(path, placeholder_label):
    # Opened fresh per request, not cached: the one-shot renderer that writes this file may not
    # have finished yet (or, under a compose implementation that doesn't honor
    # service_completed_successfully, may not have even started) when this tier first starts
    # serving -- so a page load a few seconds later just works, no restart needed. Same contract
    # demo/hello/app.py's old _load_graph_svg had.
    try:
        with open(path, "rb") as fh:
            payload = fh.read()
    except OSError:
        payload = _svg_placeholder(placeholder_label)
    return HttpResponse(payload, content_type="image/svg+xml")


def index(request):
    return render(request, "pages/index.html", {"grafana_url": GRAFANA_URL})


def graph_svg(request):
    return _serve_svg(GRAPH_SVG_PATH, "pipeline graph")


def architecture_svg(request):
    return _serve_svg(ARCH_SVG_PATH, "architecture diagram")


def health(request):
    return HttpResponse(b"ok\n", content_type="text/plain; charset=utf-8")
