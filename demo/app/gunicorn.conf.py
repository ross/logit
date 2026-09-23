"""Wires OpenTelemetry tracing into each gunicorn worker (docs/plans/demo-tracing-stack.md).

This runs in `post_fork`, not at module import time, and not via the `opentelemetry-instrument` CLI
wrapper. The `BatchSpanProcessor` this sets up owns a background export thread, and a thread does
not survive `fork()`: only the forking thread's state does. A `TracerProvider` built before
gunicorn's default (non-`--preload`) sync worker model forks would leave every worker holding a
processor whose export thread only ever existed in the master, silently exporting nothing.
`post_fork` runs inside each freshly forked worker, before that worker imports `demoproj.wsgi`
(`bind`/`workers` below; no `preload_app`), so `DjangoInstrumentor().instrument()` is in place
before Django's instrumentation-relevant machinery (URL resolution, middleware) loads.
"""

import os

# Set here, not left to demoproj/wsgi.py's `setdefault`. `DjangoInstrumentor().instrument()` below
# reads Django settings itself (to find `MIDDLEWARE` etc.). If `DJANGO_SETTINGS_MODULE` isn't set
# yet when it does, Django's lazy `settings` object falls back to an empty
# `settings.configure()`-style holder over `global_settings`. After that, `demoproj.wsgi`'s later
# `get_wsgi_application()` never loads `demoproj.settings` (`LazySettings` only consults
# `DJANGO_SETTINGS_MODULE` the *first* time it configures itself), and every request 500s with
# `AttributeError: module 'django.conf.global_settings' has no attribute 'ROOT_URLCONF'`. Setting
# the env var here, before `post_fork`, closes that ordering gap.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "demoproj.settings")

bind = "0.0.0.0:8080"
workers = int(os.environ.get("WEB_CONCURRENCY", "2"))
# Sync workers, not `gthread`/`gevent`: fewer moving parts to explain alongside the tracing story
# above, and each request runs to completion on its own worker process either way.


def post_fork(server, worker):
    from opentelemetry.instrumentation.django import DjangoInstrumentor

    from demoproj.telemetry import setup

    # Everything but Django's request-span instrumentation is shared with the Celery worker's
    # equivalent hook (`demoproj/celery.py`'s `worker_process_init`,
    # `docs/plans/demo-richer-traces.md`) through `demoproj/telemetry.py`. See that module for
    # what each instrumentor call is for.
    setup(default_service_name="demo-app")

    # The default W3C `tracecontext` propagator extracts the `traceparent` nginx forwards
    # (demo/nginx/nginx.conf), so this request's server span is a genuine child of nginx's span
    # with no code here.
    # Django-specific, so it stays here rather than in the shared `telemetry.setup`: the worker
    # never serves HTTP.
    DjangoInstrumentor().instrument()
