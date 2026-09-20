"""The smallest Django settings module that is still a real Django app.

Deliberately unremarkable: this leg of the survey measures what **OpenTelemetry's Python
auto-instrumentation** produces at SDK defaults, so anything configured here that the
instrumentation can see would contaminate the measurement. No logging config (the OTel logging
instrumentation attaches its own handler to the root logger), no middleware beyond Django's own
default list, no custom context processors.

SQLite, not Postgres -- said out loud in the producer's provenance. The span the survey is
counting attributes on is `psycopg`/`sqlite3`'s **dbapi** span, and both go through
`opentelemetry-instrumentation-dbapi`'s same attribute set (`db.system`, `db.name`,
`db.statement`, `db.user`, `net.peer.name`, `net.peer.port`); sqlite's has no network peer, so its
row is at the low end of the desk count's 3 - 6 - 7 rather than the high end. Postgres would have
cost an image, a wait and a psycopg build for one or two more attributes per span.
"""

import pathlib

BASE_DIR = pathlib.Path(__file__).resolve().parent.parent

SECRET_KEY = "shape-survey-not-a-secret"
DEBUG = False
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "pages",
]

MIDDLEWARE = [
    "django.middleware.common.CommonMiddleware",
]

ROOT_URLCONF = "project.urls"
TEMPLATES = []
WSGI_APPLICATION = "project.wsgi.application"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": "/tmp/shape-survey.sqlite3",
    }
}

USE_TZ = True
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
