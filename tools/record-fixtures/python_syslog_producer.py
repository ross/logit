#!/usr/bin/env python3
"""Drives the demo app's `NoNulSysLogHandler` (`demo/app/pages/syslog_handler.py`) to produce
fixture datagrams for `script/record-fixtures`'s `python-syslog-handler` producer.

`script/record-fixtures` bind-mounts the real module beside this file, so the import is the
handler the demo app configures, not a copy. Two messages go through the handler/formatter path a
Django `logging` call uses:

1. A plain text message.
2. A JSON body, the shape the demo's app tier emits and the case `NoNulSysLogHandler` exists for:
   the base `SysLogHandler` appends a trailing NUL after the closing `}`, which breaks `logit`'s
   `json` transform. The fixture pins the fix and catches a stdlib regression.

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
