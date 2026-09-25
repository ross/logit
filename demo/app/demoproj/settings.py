"""Settings for the demo's app tier (docs/plans/demo-tracing-stack.md), shared by the gunicorn
web workers and the Celery `worker` service.

Deliberately close to `django-admin startproject`'s defaults: the point of this tier is to show a
real framework's drop-in integration points (tracing via `opentelemetry-instrumentation-django`,
logging via stdlib `logging.handlers.SysLogHandler`), not a from-scratch minimal app. Tracing is
wired in demo/app/gunicorn.conf.py's `post_fork` (and demoproj/celery.py's `worker_process_init`),
not here, because it must run once per forked worker, before Django's instrumentation-relevant
imports happen in that worker.
"""

import os
import socket
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# A fixed demo value, like demo/compose.yaml's POSTGRES_PASSWORD: this stack has no real users or
# sessions to protect. Never do this outside a throwaway demo.
SECRET_KEY = "demo-only-not-a-real-secret"

DEBUG = False
# No real hostname to pin here: nginx forwards whatever Host a client sent (haproxy's hostname,
# `localhost:8080`, ...). Fine for a demo; a real deployment would list real hostnames.
ALLOWED_HOSTS = ["*"]

# No `django.contrib.staticfiles`: this tier's one template is inline-styled and serves no static
# assets of its own. `graph.svg`/`architecture.svg` are dynamic views reading a shared volume, and
# `telemetry.js` (docs/plans/browser-tracing.md) is a dynamic view reading the bundle esbuild
# produced at image-build time. A whole app plus `STATIC_URL`/`STATIC_ROOT`/`collectstatic` for
# one bundled JS file would be more machinery than the thing it serves.
INSTALLED_APPS = [
    "pages",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.middleware.common.CommonMiddleware",
    # Last (innermost), so it runs closest to the view and logs the real status code/body length
    # the view produced. It's inside the OpenTelemetry request span either way, because that span
    # wraps the WSGI handler, not `MIDDLEWARE`; see demo/app/pages/middleware.py's module
    # docstring.
    "pages.middleware.AccessLogMiddleware",
]

ROOT_URLCONF = "demoproj.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                # Renders the page's <meta name="traceparent"> (docs/plans/browser-tracing.md);
                # see pages/context_processors.py's module docstring.
                "pages.context_processors.traceparent",
            ],
        },
    },
]

WSGI_APPLICATION = "demoproj.wsgi.application"

# Real state (`docs/plans/demo-richer-traces.md`), with a real driver-level CLIENT span per
# statement (opentelemetry-instrumentation-psycopg, demo/app/demoproj/telemetry.py). Django
# auto-selects the psycopg 3 backend when `psycopg` (not `psycopg2`) is the importable driver
# (Django 4.2+); it's still spelled `django.db.backends.postgresql` either way.
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.environ.get("POSTGRES_DB", "demo"),
        "USER": os.environ.get("POSTGRES_USER", "demo"),
        "PASSWORD": os.environ.get("POSTGRES_PASSWORD", ""),
        "HOST": os.environ.get("POSTGRES_HOST", "postgres"),
        "PORT": os.environ.get("POSTGRES_PORT", "5432"),
    }
}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

USE_TZ = True

# `logit`'s syslog listeners for this app, one per tier (demo/logit.yaml's `app_in`, :5142, and
# `worker_in`, :5143; docs/plans/demo-richer-traces.md), distinct from haproxy's :5140.
# `set`'s `resource:` block stamps a whole *batch*, so each tier needs its own listener, or they'd
# interleave into one batch with one wrong `service.name`. This settings module is shared by both
# processes (gunicorn's workers and the `worker` service, demo/compose.yaml). Each only logs
# through its own logger below, so both handlers existing in both processes is harmless.
LOGIT_HOST = os.environ.get("LOGIT_HOST", "logit")
LOGIT_PORT = int(os.environ.get("LOGIT_PORT", "5142"))
WORKER_LOGIT_PORT = int(os.environ.get("WORKER_LOGIT_PORT", "5143"))

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "access_json": {"()": "pages.logging_formatter.AccessLogJSONFormatter"},
        "worker_json": {"()": "pages.logging_formatter.WorkerLogJSONFormatter"},
    },
    "handlers": {
        "access_syslog": {
            # Not the base `logging.handlers.SysLogHandler`; see pages/syslog_handler.py's
            # module docstring for why.
            "class": "pages.syslog_handler.NoNulSysLogHandler",
            "address": (LOGIT_HOST, LOGIT_PORT),
            "socktype": socket.SOCK_DGRAM,
            # Matches demo/haproxy/haproxy.cfg's facility (PRI 134 = facility 16/local0), for
            # consistency across tiers, though `logit` doesn't key on it.
            "facility": "local0",
            "formatter": "access_json",
        },
        "worker_syslog": {
            "class": "pages.syslog_handler.NoNulSysLogHandler",
            "address": (LOGIT_HOST, WORKER_LOGIT_PORT),
            "socktype": socket.SOCK_DGRAM,
            "facility": "local0",
            "formatter": "worker_json",
        },
    },
    "loggers": {
        # `propagate: False`: this logger's only purpose is the one `access_syslog` line per
        # request (demo/app/pages/middleware.py), and it shouldn't also hit Django's root logger
        # and print to stderr.
        "demoapp.access": {
            "handlers": ["access_syslog"],
            "level": "INFO",
            "propagate": False,
        },
        # Same reasoning as `demoapp.access`: `pages/tasks.py`'s one log line per task, nothing
        # else.
        "demoapp.worker": {
            "handlers": ["worker_syslog"],
            "level": "INFO",
            "propagate": False,
        },
    },
}
