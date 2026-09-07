"""The one background task this demo has -- enqueued by `pages/views.py`'s `work`, run by the
`worker` service, not any gunicorn worker (`docs/plans/demo-richer-traces.md` workstream D).
Deliberately ordinary: a brief sleep, one DB write, one log line -- the point isn't the work
itself, it's that this task's real Celery CONSUMER span (opentelemetry-instrumentation-celery,
`demoproj/telemetry.py`) lands under the same trace as the request that enqueued it, arriving in
Tempo seconds after that request's own response has already gone back to the client.
"""

import logging
import random
import time

from demoproj.celery import app
from pages.models import WorkRecord

# A plain, explicitly-configured logger (`demoproj/settings.py`'s `LOGGING`) -- not Celery's own
# `get_task_logger`, whose records feed Celery's own root-logger setup by default (a different,
# harder-to-predict path than every other tier's logger in this demo takes). `propagate: False`
# there keeps this on the same footing as `pages/middleware.py`'s `demoapp.access`: one handler,
# one destination, no Celery-internal console output mixed in.
worker_logger = logging.getLogger("demoapp.worker")


@app.task(name="pages.background_work")
def background_work():
    started = time.monotonic()
    # Jittered, the same reasoning `pages/views.py`'s `work` already gives its own sleep -- real
    # latency spread, not a fixed number, on the span this produces.
    time.sleep(random.uniform(0.1, 0.6))

    record = WorkRecord.objects.create()

    elapsed_ms = round((time.monotonic() - started) * 1000, 1)
    worker_logger.info(
        "background_work",
        extra={
            "task_name": "pages.background_work",
            "duration_ms": elapsed_ms,
            "record_id": record.id,
        },
    )
