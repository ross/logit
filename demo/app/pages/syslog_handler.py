"""`logging.handlers.SysLogHandler` with `append_nul` off.

The base handler appends a trailing NUL byte to every UDP datagram by default (`append_nul = True`
is a class attribute, not a constructor argument -- there's no way to disable it from
`LOGGING`/`dictConfig` without subclassing). `crates/logit-inputs/src/syslog.rs` decodes each
datagram as UTF-8 and hands the tail of the line to the `json` transform as-is; a trailing NUL
survives both steps as a literal character after the JSON object's closing `}`, which
`serde_json`'s parser rejects as trailing garbage. Confirmed against the parser's own source, not
assumed -- see demo/logit.yaml's `app_json` comment.
"""

import logging.handlers


class NoNulSysLogHandler(logging.handlers.SysLogHandler):
    append_nul = False
