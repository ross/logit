"""The Celery app. `pages/tasks.py`'s task runs here, inside the `worker` service
(`demo/compose.yaml`), a separate process from every gunicorn worker
(`docs/plans/demo-richer-traces.md`). Started as `celery -A demoproj.celery worker`.

`django.setup()` runs at import time, in the master process before Celery forks its worker pool.
Unlike telemetry setup below, that's safe: it only populates Django's app registry
(`pages.tasks`'s `from pages.models import WorkRecord` needs that to resolve) and opens no socket
or thread of its own. Nothing here touches a real connection until a task runs.
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "demoproj.settings")
django.setup()

from celery import Celery  # noqa: E402 -- after django.setup(), deliberately
from celery.signals import worker_process_init  # noqa: E402

app = Celery("demoproj")
app.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://redis:6379/0")
# No result backend: `pages/views.py`'s `work` fires the task and moves on, and nothing ever reads
# a task's return value.
app.conf.result_backend = None
app.autodiscover_tasks(["pages"])


@worker_process_init.connect
def _setup_worker_telemetry(**kwargs):
    # The mirror of `gunicorn.conf.py`'s `post_fork` hook, for the same reason: this worker's
    # default pool (`--pool=prefork`) forks too, and a `BatchSpanProcessor` built before that fork
    # would leave every child holding an export thread that only ever existed in the parent
    # (`gunicorn.conf.py`'s module docstring has the long version). `worker_process_init` fires
    # once per forked child, after the fork: Celery's `post_fork` equivalent.
    from demoproj.telemetry import setup

    setup(default_service_name="demo-worker")
