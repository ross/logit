#!/usr/bin/env python3
"""Drives `demo/app/pages/syslog_handler.py`'s `NoNulSysLogHandler` to produce real fixture
datagrams for `script/record-fixtures`'s `python-syslog-handler` producer.

This is the actual handler the demo app configures in production (`demo/logit.yaml`'s app tier),
not a hand-rolled stand-in for it -- `script/record-fixtures` bind-mounts
`demo/app/pages/syslog_handler.py` alongside this file so the import below is the real module, not
a copy. Two messages are sent, both through the same handler/formatter path a real Django
`logging` call would use:

1. A plain text message, to exercise the ordinary case.
2. A JSON body, the shape `demo/logit.yaml`'s app tier actually emits -- and the exact case
   `NoNulSysLogHandler` exists for: the base `SysLogHandler` appends a trailing NUL byte that used
   to land right after this message's closing `}` and break `logit`'s `json` transform. Recording
   it as a fixture pins that behavior (or catches a regression in Python's stdlib) going forward.

Usage: python3 python_syslog_producer.py --host <capture-container-name> --port 5514
"""

import argparse
import json
import logging
import logging.handlers
import time

from syslog_handler import NoNulSysLogHandler


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True, help="Hostname/container name of the raw_capture.py listener")
    ap.add_argument("--port", type=int, required=True)
    args = ap.parse_args()

    logger = logging.getLogger("logit-fixture-producer")
    logger.setLevel(logging.INFO)
    handler = NoNulSysLogHandler(
        address=(args.host, args.port),
        facility=logging.handlers.SysLogHandler.LOG_USER,
    )
    logger.addHandler(handler)

    logger.info("hello from python logging.handlers.SysLogHandler, captured for logit interop fixtures")
    time.sleep(0.2)  # Keep datagrams as visibly separate captures, not merged by the listener.

    logger.info(json.dumps({"level": "info", "msg": "request handled", "path": "/", "status": 200}))
    time.sleep(0.2)


if __name__ == "__main__":
    main()
