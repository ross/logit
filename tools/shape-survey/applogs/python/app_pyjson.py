"""`python-json-logger` on stdlib `logging`, in two configurations at once.

Config source, copied rather than invented:
  https://nhairs.github.io/python-json-logger/latest/quickstart/

  * DEFAULT stream: `JsonFormatter()` with no format string, the quickstart's first example.
    The library's bare default is JSON, so unlike structlog there is a real default to measure:
    `message`, whatever `extra=` supplied, and nothing else.
  * PRODUCTION stream: the quickstart's format-string form, with the documented example's field
    list (`asctime levelname name message`). Each `LogRecord` attribute an operator adds makes the
    record one key wider, so the measured width is a floor.

Both streams log the same eight `extra=` fields (appbase.FIELDS) to separate files and logger
names, so the summary can put them side by side.
"""

import logging
import sys

from pythonjsonlogger.json import JsonFormatter

import appbase

PROD_OUT, DEFAULT_OUT, PORT = sys.argv[1], sys.argv[2], int(sys.argv[3])

#: Every Nth request is logged again through the default-configuration logger: enough for a
#: distribution without doubling the app's work.
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
