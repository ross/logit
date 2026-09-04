"""Settings for the demo's app tier (docs/plans/demo-tracing-stack.md's workstream B). Deliberately
close to `django-admin startproject`'s own defaults -- the point of this tier is to show a real
framework's drop-in integration points (tracing via `opentelemetry-instrumentation-django`, logging
via stdlib `logging.handlers.SysLogHandler`), not a from-scratch minimal app. Tracing itself is
wired in demo/app/gunicorn.conf.py's `post_fork`, not here -- it must run once per forked worker,
before Django's own instrumentation-relevant imports happen in that worker.
"""

import os
import socket
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# A fixed demo value, like demo/compose.yaml's INFLUXDB_TOKEN -- this stack has no real users or
# sessions to protect. Never do this outside a throwaway demo.
SECRET_KEY = "demo-only-not-a-real-secret"

DEBUG = False
# No real hostname to pin here -- nginx forwards whatever Host a client sent (haproxy's own
# hostname, `localhost:8080`, ...). Fine for a demo; a real deployment would list real hostnames.
ALLOWED_HOSTS = ["*"]

# No `django.contrib.staticfiles` -- this tier's one template is inline-styled and serves no
# static assets of its own (`graph.svg` is a dynamic view reading a shared volume, not a static
# file), so there's nothing for it to collect or serve.
INSTALLED_APPS = [
    "pages",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.middleware.common.CommonMiddleware",
    # Last (innermost) -- runs closest to the view, so it logs the real status code/body length
    # the view produced. It's inside the OpenTelemetry request span either way (that span wraps
    # the WSGI handler, not `MIDDLEWARE`, so position here doesn't affect that) -- see
    # demo/app/pages/middleware.py's own header comment.
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
            ],
        },
    },
]

WSGI_APPLICATION = "demoproj.wsgi.application"

# No database -- this tier holds no state of its own (docs/plans/demo-tracing-stack.md's
# workstream B), and nothing installed above needs one.
DATABASES = {}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

USE_TZ = True

# `logit`'s own syslog listener for this tier (demo/logit.yaml's `app_in`, :5142) -- distinct from
# haproxy's :5140 and nginx's :5141, same reasoning as both: `set`'s `resource:` block stamps a
# whole *batch*, so each tier needs its own listener or they'd interleave into one batch with one
# wrong `service.name`.
LOGIT_HOST = os.environ.get("LOGIT_HOST", "logit")
LOGIT_PORT = int(os.environ.get("LOGIT_PORT", "5142"))

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "access_json": {"()": "pages.logging_formatter.AccessLogJSONFormatter"},
    },
    "handlers": {
        "access_syslog": {
            # Not the base `logging.handlers.SysLogHandler` -- see
            # pages/syslog_handler.py's header comment for why.
            "class": "pages.syslog_handler.NoNulSysLogHandler",
            "address": (LOGIT_HOST, LOGIT_PORT),
            "socktype": socket.SOCK_DGRAM,
            # Matches demo/haproxy/haproxy.cfg's own facility (PRI 134 = facility 16/local0) --
            # consistent across tiers, though `logit` doesn't key on it.
            "facility": "local0",
            "formatter": "access_json",
        },
    },
    "loggers": {
        # `propagate: False` -- this logger's only purpose is the one `access_syslog` line per
        # request (demo/app/pages/middleware.py); it shouldn't also hit Django's root logger and
        # print to stderr.
        "demoapp.access": {
            "handlers": ["access_syslog"],
            "level": "INFO",
            "propagate": False,
        },
    },
}
