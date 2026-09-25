# Syslog interop fixtures

Raw wire bytes, one UDP datagram or one TCP connection's byte stream per file, captured verbatim by
`tools/record-fixtures/raw_capture.py`, with **no parsing and no re-encoding.** Each file is exactly
the bytes a real sender put on the wire; `logit`'s own encoder never touches these.

To regenerate, run `script/record-fixtures logger python-syslog-handler rsyslog rsyslog-tcp`. See
`../README.md` and the header comment in `script/record-fixtures`.

## Fixtures

| File | Producer | Invocation | Captured | Construct exercised |
|---|---|---|---|---|
| `logger-rfc3164-basic-000.raw` | util-linux `logger(1)` 2.38.1-5+deb12u3 (Debian 12 "bookworm", `debian:bookworm-slim`) | `logger -n capture -P 5514 -d --rfc3164 -t logit-fixture -p user.notice "hello from logger(1)..."` | 2026-09-10 | RFC 3164: `<PRI>TIMESTAMP HOSTNAME TAG: MSG`, hostname present, `user.notice` (facility 1, severity 5) |
| `logger-rfc3164-unicode-000.raw` | Same as above | Same, with the message body `"héllo wörld — ünïcödé ✓ (UTF-8 multibyte smoke test)"` | 2026-09-10 | RFC 3164 with a UTF-8 multibyte MSG body: a real sender emitting non-ASCII, not a hand-typed test literal |
| `logger-rfc5424-basic-000.raw` | Same as above | `logger -n capture -P 5514 -d --rfc5424 -t logit-fixture -p user.notice "hello from logger(1) in RFC 5424 mode..."` | 2026-09-10 | RFC 5424: full `<PRI>1 TIMESTAMP HOSTNAME APP-NAME PROCID MSGID STRUCTURED-DATA MSG`, with nil PROCID/MSGID and a real `[timeQuality tzKnown="1" isSynced="1" syncAccuracy="..."]` STRUCTURED-DATA element. `logit` parses this element into `syslog.sd` rather than merely balancing and skipping it, per `crates/logit-inputs/src/syslog.rs`'s module doc, and this fixture is exactly the case that exercises that parse with real rather than hand-typed input |
| `python-syslog-handler-000.raw` | Python 3.12.14 stdlib `logging.handlers.SysLogHandler`, through `demo/app/pages/syslog_handler.py`'s `NoNulSysLogHandler` (`python:3.12-slim`) | `tools/record-fixtures/python_syslog_producer.py`, plain `logger.info(...)` call | 2026-09-10 | The minimal RFC 3164-ish framing that Python's handler actually emits: `<PRI>MSG`, with no TIMESTAMP, HOSTNAME, or TAG field at all, and **no trailing NUL byte** (verified byte for byte). The base handler's `SysLogHandler.append_nul` default is what `NoNulSysLogHandler` exists to disable; see that file's docstring |
| `python-syslog-handler-001.raw` | Same as above | Same, `logger.info(json.dumps({...}))`, the same shape `demo/logit.yaml`'s app tier logs in production | 2026-09-10 | Same framing, with a JSON object as the MSG. This is the exact case `NoNulSysLogHandler` exists for: a trailing NUL landing right after the closing `}` used to break `logit`'s `json` transform (`demo/app/pages/syslog_handler.py`'s docstring, `docs/known-gaps.md`) |
| `rsyslog-000.raw` | rsyslog 8.2302.0-1+deb12u1 (Debian 12 package, installed fresh into `debian:bookworm-slim` at record time; no official `rsyslog/*` Docker Hub image exists, verified) | `tools/record-fixtures/rsyslog.conf` (`imuxsock` input, `omfwd`/UDP output) + local `logger -t logit-fixture "hello from rsyslog..."` | 2026-09-10 | RFC 3164 framing from a real second-hop *forwarder*, not a direct sender, at PRI 13 (`user.notice`, `logger`'s default with no `-p`). rsyslog's own `omfwd` formatting is something a hand-typed test literal can't stand in for |
| `rsyslog-tcp-000.raw` | rsyslog 8.2302.0-1+deb12u1 (same Debian 12 package as above, installed fresh into `debian:bookworm-slim` at record time) | `tools/record-fixtures/rsyslog-tcp.conf` (`imuxsock` input, `omfwd`/TCP output, **no** `TCP_Framing` parameter) + local `logger -t logit-fixture "hello from rsyslog..."` | 2026-09-13 | RFC 6587 §3.4.2 non-transparent (LF-terminated) TCP framing. This is rsyslog's own `omfwd` default when `TCP_Framing` is unset, and so what a stock rsyslog forwarder actually sends. One TCP connection, captured whole by `raw_capture.py --proto tcp`, holding exactly one LF-terminated RFC 3164 message at the same PRI 13 as `rsyslog-000.raw` |

## Tests that consume these fixtures

- `crates/logit-inputs/src/syslog.rs`'s `interop_fixture_*` tests decode each UDP fixture and
  assert on its message text and, depending on the fixture, its `syslog.tag`, its severity, or, for
  `logger-rfc5424-basic-000.raw`, the decoded `syslog.sd` structured data and the absent
  PROCID/MSGID.
- `crates/logit-inputs/src/tcp.rs`'s `interop_fixture_rsyslog_tcp_non_transparent_frame` runs
  `rsyslog-tcp-000.raw` through the TCP framer, then decodes the one frame and asserts on its tag,
  severity, and message.
- `crates/logit-cli/tests/syslog_round_trip.rs` reads the six UDP captures (every one but
  `rsyslog-tcp-000.raw`) in place, without copying them
  into its own corpus, and relays them through `syslog_out` over UDP and TCP. The captures that
  carry their own timestamp (`logger-rfc3164-basic-000.raw`, `logger-rfc3164-unicode-000.raw`,
  `rsyslog-000.raw`, and `logger-rfc5424-basic-000.raw`) must come back byte for byte.

## What isn't covered here (yet)

- **syslog-ng.** Not attempted: there's no fixture and no harness support yet. See
  `docs/plans/recorded-interop-fixtures.md`'s follow-on list.
- **Octet-counted TCP framing** (RFC 6587 §3.4.1, rsyslog's `TCP_Framing="octet-counted"`). Only
  rsyslog's *default* non-transparent TCP framing is captured so far (`rsyslog-tcp-000.raw` above).
  A `record_rsyslog_tcp_octet_counted`-shaped producer (`rsyslog-tcp.conf` plus
  `TCP_Framing="octet-counted"`) is the natural next fixture.
- **syslog-ng over TCP.** Not attempted, same as the UDP case above.
- **HAProxy/nginx access-log syslog output.** Deliberately out of scope for this corpus. The
  existing `demo/`/`fixtures/nginx/` integration already covers it, so it isn't a fixture-corpus
  concern (see the plan doc's scope section).
