"""`structlog` in its documented production/JSON configuration.

Config source, copied rather than invented:
  https://www.structlog.org/en/stable/standard-library.html#suggested-configurations
  https://www.structlog.org/en/stable/performance.html   (WriteLoggerFactory, cache_logger_on_first_use)
  https://www.structlog.org/en/stable/exceptions.html    (dict_tracebacks is the JSON answer to
                                                          format_exc_info)

NO DEFAULT-CONFIGURATION STREAM FROM THIS APP, and that is a finding rather than an omission:
structlog's out-of-the-box default (`structlog.get_logger()` with no `configure()`) renders with
`ConsoleRenderer` -- colorized key=value text, not JSON. There is no "bare default JSON" of
structlog's to measure, so this app emits the production stream only and the summary says so.
"""

import logging
import pathlib
import sys

import structlog

import appbase

OUT = pathlib.Path(sys.argv[1])
PORT = int(sys.argv[2])

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.dict_tracebacks,
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    logger_factory=structlog.WriteLoggerFactory(file=OUT.open("a", buffering=1)),
    cache_logger_on_first_use=True,
)

log = structlog.get_logger("app")


def on_request(fields, exc, seq):
    if exc is not None:
        log.error("request failed", exc_info=exc, **fields)
    else:
        log.info("request", **fields)


log.info("starting", port=PORT, library="structlog", version=structlog.__version__)
appbase.serve(PORT, on_request)
