"""The demo's one background task, enqueued by `pages/views.py`'s `work` and run by the
`worker` service, not a gunicorn worker (`docs/plans/demo-richer-traces.md`).

Deliberately ordinary: a brief sleep, one DB write, one log line. The point is that this task's
real Celery CONSUMER span (opentelemetry-instrumentation-celery, `demoproj/telemetry.py`) lands
under the same trace as the request that enqueued it, arriving in Tempo seconds after that
request's response has gone back to the client.
"""

import logging
import random
import time

from demoproj.celery import app
from pages.models import WorkRecord

# A plain, explicitly configured logger (`demoproj/settings.py`'s `LOGGING`), not Celery's
# `get_task_logger`, whose records feed Celery's root-logger setup by default (a different,
# harder-to-predict path than every other logger in this demo takes). `propagate: False` there
# keeps this on the same footing as `pages/middleware.py`'s `demoapp.access`: one handler, one
# destination, no Celery-internal console output mixed in.
worker_logger = logging.getLogger("demoapp.worker")


@app.task(name="pages.background_work")
def background_work():
    started = time.monotonic()
    # Jittered, like `pages/views.py`'s `work` sleep, so the span this produces shows real
    # latency spread rather than a fixed number.
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
