"""Four views: one flat, one that queries the database, one that calls another view over HTTP
with `requests`, and one that raises.

Nothing here touches OpenTelemetry. Every span, metric and log record this leg produces comes from
`opentelemetry-instrument` wrapping the process from outside -- which is the point: the survey is
measuring the *auto-instrumentation's* output at SDK defaults, not a hand-instrumented app's.
"""

import logging
import os

import requests
from django.http import HttpResponse, JsonResponse

from pages.models import Item

logger = logging.getLogger(__name__)

SELF = os.environ.get("SELF_BASE_URL", "http://django:8000")


def index(request):
    logger.info("index")
    return HttpResponse("ok\n")


def items(request):
    """A real DB query -- one dbapi CLIENT span per statement."""
    rows = list(Item.objects.filter(price__gte=0).order_by("id")[:10])
    logger.info("listed %d items", len(rows))
    return JsonResponse({"items": [{"id": r.id, "name": r.name} for r in rows]})


def item(request, item_id):
    row = Item.objects.filter(id=item_id).first()
    if row is None:
        logger.warning("item %s not found", item_id)
        return JsonResponse({"error": "not found"}, status=404)
    return JsonResponse({"id": row.id, "name": row.name, "price": row.price})


def fanout(request):
    """Calls another view over HTTP -- one `requests` CLIENT span plus a second, nested Django
    SERVER span, so the trace has real depth rather than one span per trace."""
    response = requests.get(f"{SELF}/items/", timeout=5)
    logger.info("fanout got %s", response.status_code)
    return JsonResponse({"upstream": response.status_code})


def boom(request):
    logger.error("about to fail", exc_info=ValueError("synthetic failure"))
    raise ValueError("synthetic failure serving /boom/")
