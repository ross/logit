"""`python-json-logger` on stdlib `logging`, in two configurations at once.

Config source, copied rather than invented:
  https://nhairs.github.io/python-json-logger/latest/quickstart/

  * DEFAULT stream: `JsonFormatter()` with no format string -- the quickstart's own first
    example. The library's bare default *is* JSON, so unlike structlog there is a real default
    to measure: the record carries `message`, whatever `extra=` supplied, and nothing else.
  * PRODUCTION stream: the quickstart's format-string form, which is how the library documents
    selecting which stdlib `LogRecord` attributes to include. The field list here is the
    documented example's (`asctime levelname name message`) rather than one chosen here -- an
    operator adding `%(module)s`/`%(process)d`/`%(threadName)s` makes the record one key wider
    per attribute, linearly, so the measured width is a floor in exactly that sense.

Both streams log the same request through the same eight `extra=` fields (appbase.FIELDS), to
separate files and through separate logger names, so the summary can put the two side by side.
"""

import logging
import sys

from pythonjsonlogger.json import JsonFormatter

import appbase

PROD_OUT, DEFAULT_OUT, PORT = sys.argv[1], sys.argv[2], int(sys.argv[3])

#: Every Nth request is logged a second time through the default-configuration logger. The
#: production stream gets every request; this one gets a sample, which is plenty for a
#: distribution over a five-minute capture and keeps the two streams from doubling the app's work.
DEFAULT_EVERY = 4


def logger(name, path, formatter):
    handler = logging.FileHandler(path)
    handler.setFormatter(formatter)
    log = logging.getLogger(name)
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    log.propagate = False
    return log


prod = logger(
    "app.prod",
    PROD_OUT,
    JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s"),
)
default = logger("app.default", DEFAULT_OUT, JsonFormatter())


def on_request(fields, exc, seq):
    if exc is not None:
        prod.error("request failed", exc_info=exc, extra=fields)
    else:
        prod.info("request", extra=fields)
    if seq % DEFAULT_EVERY == 0:
        if exc is not None:
            default.error("request failed", exc_info=exc, extra=fields)
        else:
            default.info("request", extra=fields)


prod.info("starting", extra={"port": PORT, "library": "python-json-logger"})
default.info("starting", extra={"port": PORT, "library": "python-json-logger"})
appbase.serve(PORT, on_request)
