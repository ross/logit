# Syslog interop fixtures

Raw UDP datagrams, captured verbatim by `tools/record-fixtures/raw_capture.py` -- **no parsing, no
re-encoding.** Each file is exactly the bytes a real sender put on the wire; `logit`'s own encoder
never touches these. Regenerate with `script/record-fixtures logger python-syslog-handler rsyslog rsyslog-tcp`
(see `../README.md` and `script/record-fixtures`'s own header comment).

| File | Producer | Invocation | Captured | Construct exercised |
|---|---|---|---|---|
| `logger-rfc3164-basic-000.raw` | util-linux `logger(1)` 2.38.1-5+deb12u3 (Debian 12 "bookworm", `debian:bookworm-slim`) | `logger -n capture -P 5514 -d --rfc3164 -t logit-fixture -p user.notice "hello from logger(1)..."` | 2026-09-10 | RFC 3164: `<PRI>TIMESTAMP HOSTNAME TAG: MSG`, hostname present, `user.notice` (facility 1, severity 5) |
| `logger-rfc3164-unicode-000.raw` | Same as above | Same, message body `"héllo wörld — ünïcödé ✓ (UTF-8 multibyte smoke test)"` | 2026-09-10 | RFC 3164 with a UTF-8 multibyte MSG body -- a real sender, not just a hand-typed test literal, emitting non-ASCII |
| `logger-rfc5424-basic-000.raw` | Same as above | `logger -n capture -P 5514 -d --rfc5424 -t logit-fixture -p user.notice "hello from logger(1) in RFC 5424 mode..."` | 2026-09-10 | RFC 5424: full `<PRI>1 TIMESTAMP HOSTNAME APP-NAME PROCID MSGID STRUCTURED-DATA MSG`, with a real `[timeQuality tzKnown="1" isSynced="1" syncAccuracy="..."]` STRUCTURED-DATA element (`logit` only balances/skips this, per `crates/logit-inputs/src/syslog.rs`'s module doc -- this fixture is exactly the case that exercises the skip, not a hand-typed one) and nil PROCID/MSGID |
| `python-syslog-handler-000.raw` | Python 3.12.14 stdlib `logging.handlers.SysLogHandler`, via `demo/app/pages/syslog_handler.py`'s `NoNulSysLogHandler` (`python:3.12-slim`) | `tools/record-fixtures/python_syslog_producer.py`, plain `logger.info(...)` call | 2026-09-10 | Minimal RFC 3164-ish framing Python's handler actually emits: `<PRI>TAG: MSG`, no TIMESTAMP/HOSTNAME fields at all -- and **no trailing NUL byte** (verified byte-for-byte; the base `SysLogHandler.append_nul` default is what `NoNulSysLogHandler` exists to disable, see that file's docstring) |
| `python-syslog-handler-001.raw` | Same as above | Same, `logger.info(json.dumps({...}))` -- the same shape `demo/logit.yaml`'s app tier logs in production | 2026-09-10 | Same framing, MSG is a JSON object -- the exact case `NoNulSysLogHandler` exists for: a trailing NUL landing right after the closing `}` used to break `logit`'s `json` transform (`demo/app/pages/syslog_handler.py`'s docstring, `docs/known-gaps.md`) |
| `rsyslog-000.raw` | rsyslog 8.2302.0-1+deb12u1 (Debian 12 package, installed fresh into `debian:bookworm-slim` at record time -- no official `rsyslog/*` Docker Hub image exists, verified) | `tools/record-fixtures/rsyslog.conf` (`imuxsock` input, `omfwd`/UDP output) + local `logger -t logit-fixture "hello from rsyslog..."` | 2026-09-10 | RFC 3164 framing from a real second-hop *forwarder*, not a direct sender -- rsyslog's own `omfwd` formatting, which a hand-typed test literal can't stand in for |
| `rsyslog-tcp-000.raw` | rsyslog 8.2302.0-1+deb12u1 (same Debian 12 package as above, installed fresh into `debian:bookworm-slim` at record time) | `tools/record-fixtures/rsyslog-tcp.conf` (`imuxsock` input, `omfwd`/TCP output, **no** `TCP_Framing` parameter) + local `logger -t logit-fixture "hello from rsyslog..."` | 2026-09-13 | RFC 6587 §3.4.2 non-transparent (LF-terminated) TCP framing -- rsyslog's own `omfwd` default when `TCP_Framing` is left unset, which is what a stock rsyslog forwarder actually sends; one TCP connection, captured whole by `raw_capture.py --proto tcp`, holding exactly one LF-terminated RFC 3164 message |

## What isn't covered here (yet)

- **syslog-ng** -- not attempted; no fixture, no harness support yet. See
  `docs/plans/recorded-interop-fixtures.md`'s follow-on list.
- **Octet-counted TCP framing** (RFC 6587 §3.4.1, rsyslog's `TCP_Framing="octet-counted"`) -- only
  rsyslog's *default* non-transparent TCP framing is captured so far (`rsyslog-tcp-000.raw` above).
  A `record_rsyslog_tcp_octet_counted`-shaped producer (`rsyslog-tcp.conf` plus
  `TCP_Framing="octet-counted"`) is the natural next fixture.
- **syslog-ng over TCP** -- not attempted, same as the UDP case above.
- **HAProxy/nginx access-log syslog output** -- deliberately out of scope for this corpus; already
  covered by the existing `demo/`/`examples/nginx/` integration, not a fixture-corpus concern (see
  the plan doc's scope section).
