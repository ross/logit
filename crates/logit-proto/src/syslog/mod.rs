//! The syslog decoder: [`SyslogDecoder`] turns RFC 3164 and RFC 5424 messages into log events.
//! `syslog_in` (`crates/logit-inputs/src/syslog.rs`) wraps it in a UDP or TCP listener, with
//! optional TLS; `syslog_out`'s encoder (`crates/logit-outputs/src/syslog.rs`) is its mirror.
//!
//! **This module doc is the decoder's canonical grammar and mapping table**, which docs, tests, and
//! examples point at. The decoder is the input half of the `syslog_in -> syslog_out` lossless-relay
//! pair ([ADR `lossless-transit`](../../../../docs/adr/lossless-transit.md); the mirror is
//! [ADR `syslog-output`](../../../../docs/adr/syslog-output.md)). It never sees a peer: the
//! sender attributes `syslog_in` can add are stamped after decode. It reads one message per `\n`
//! by default; [`SyslogDecoder::with_line_splitting`] turns that off for a transport whose framer
//! already delimits messages, since an octet-counted MSG may contain `\n`.
//!
//! ## Dialect disambiguation
//!
//! Per message, right after `<PRI>`: a digit then a space (`1 `) means RFC 5424, anything else
//! RFC 3164. This sniff is a guess, since RFC 5424's VERSION allows `1`-`999` and a tag-less RFC
//! 3164 line whose MSG starts `4 requests failed` matches it too. A failed RFC 5424 parse with
//! version `1` (the only version real senders emit) is rejected as malformed RFC 5424. A failed
//! parse with any other digit is taken for a false-positive sniff: the line is reparsed as RFC
//! 3164 (whose parse never fails) with a throttled `sniff_fallback` diagnostic, which would
//! surface a future RFC 5424 version. So each RFC 5424 field rejection below rejects a version-`1`
//! line and sends any other version to the RFC 3164 fallback.
//!
//! ## Mapping
//!
//! `Value`, `Severity`, and `TimestampError` below are [`logit_core::Value`],
//! [`logit_core::Severity`], and [`logit_core::time::TimestampError`].
//!
//! PRI must be 1-3 digits with no leading zero (except `<0>`) and at most 191, or the line is
//! malformed. It yields `syslog.facility` and `syslog.severity` (`Value::U64`) and a `Severity`
//! (see `map_severity`). The other attributes:
//!
//! - `syslog.timestamp`: the sender's TIMESTAMP (below).
//! - `syslog.hostname`: HOSTNAME.
//! - `syslog.tag`: RFC 3164's TAG name or RFC 5424's APP-NAME.
//! - `syslog.pid`: RFC 3164's `tag[pid]` bracket or RFC 5424's PROCID (below).
//! - `syslog.msgid`: RFC 5424's MSGID.
//! - `syslog.sd`: RFC 5424's STRUCTURED-DATA (below).
//!
//! An RFC 5424 nil (`-`) or empty HOSTNAME/APP-NAME/PROCID/MSGID stamps nothing.
//!
//! **Timestamp semantics.** Every event's `timestamp` is *receipt* time: the `received_at` passed
//! to `decode_into`, captured when the datagram came off the socket
//! (`docs/adr/decoupled-listener-io.md`), not when decode runs and never the sender's own. RFC
//! 3164's timestamp has no year and no timezone, so resolving it means guessing both, and resolving
//! only RFC 5424's would give two senders on one listener different semantics. The sender's
//! timestamp lands in `syslog.timestamp` instead:
//!
//! - RFC 5424's RFC 3339 form is a `Value::Timestamp`.
//! - RFC 3164's is the raw `Value::Str`, since resolving it needs a guess.
//! - A nil RFC 5424 TIMESTAMP (`-`) is an explicit `Value::Null`, so `syslog_out` or a Lua script
//!   can tell "the sender said no timestamp" from RFC 3164's absent timestamp, which omits the
//!   attribute.
//! - A well-formed RFC 5424 TIMESTAMP outside the `i64`-nanosecond range
//!   (`TimestampError::OutOfRange`) keeps the event, omits `syslog.timestamp`, and reports a
//!   throttled `timestamp_out_of_range`. One that doesn't parse (`Malformed`) rejects the line.
//!
//! To carry the sender's time instead, place a `timestamp` transform (`format: rfc3164`, `from:
//! syslog.timestamp`) after `syslog_in`: it resolves `event.timestamp` from the attribute, and
//! uses an RFC 5424 `Value::Timestamp` as-is (`docs/adr/timestamp-transform.md`).
//!
//! **RFC 5424 STRUCTURED-DATA becomes `syslog.sd`.** `parse_structured_data` is a quote-aware
//! RFC 5424 §6.3 parser (`docs/adr/syslog-structured-data-convention.md` has the rationale):
//!
//! - The nil marker `-` produces no attribute.
//! - One or more `[SD-ID SP PARAM-NAME="PARAM-VALUE" ...]` SD-ELEMENTs produce `syslog.sd` = a
//!   `Value::Map` of `"<SD-ID>"` to a nested `Value::Map` of `"<PARAM-NAME>"` to
//!   `Value::Str`. It nests rather than flattening because `SD-NAME` may contain `.`, which would
//!   make a `syslog.sd.<id>.<param>` key ambiguous.
//! - `SD-NAME` (`SD-ID` and `PARAM-NAME`) is 1..=32 bytes of PRINTUSASCII (`%d33-126`) excluding
//!   `=`, SP, `]`, and `"`.
//! - A repeated `SD-ID` in one message is a grammar violation, since two elements sharing an id
//!   have no defined merge. A `PARAM-NAME` repeated within one element is legal and becomes a
//!   `Value::Array` of `Value::Str` in wire order.
//! - `PARAM-VALUE` is a quoted UTF-8 string in which only `\"`, `\\`, and `\]` are escapes,
//!   unescaped on decode; a backslash before any other byte is kept, with that byte.
//! - Any violation rejects the line, with a `bad_line` naming the rule and its byte offset.
//!
//! **A leading RFC 5424 §6.4 UTF-8 BOM (`EF BB BF`) on MSG is stripped**, so it doesn't leak into
//! `log.message` as U+FEFF: it is a `MSG-UTF8` signal, not payload. It is stripped only when the
//! whole MSG is valid UTF-8; a non-UTF-8 MSG has no `Value::Str` to strip it from.
//!
//! **Header fields are parsed off raw bytes and validated one by one; only MSG may hold non-UTF-8
//! bytes.** `decode_into` splits on the `\n` byte with no whole-line UTF-8 check.
//! PRI, the RFC 3164 timestamp, HOSTNAME, TAG/APP-NAME, PROCID, MSGID, and STRUCTURED-DATA are
//! PRINTUSASCII by grammar and validated where extracted. A violation in an RFC 5424 field rejects
//! the line. RFC 3164's header parse never fails, because the sniff fallback depends on that: a
//! non-UTF-8 RFC 3164 HOSTNAME candidate is left unstamped with a throttled `hostname_not_utf8`.
//! MSG is validated on its own: valid UTF-8 becomes `Value::Str`, and invalid UTF-8 becomes
//! `Value::Bytes` rather than rejecting the line, since RFC 5424 allows arbitrary binary MSG-ANY.
//!
//! **`syslog.pid`** is `Value::U64` when PROCID or a `tag[pid]` bracket parses as one, and
//! `Value::Str` of the raw token otherwise: RFC 5424's PROCID is free-form PRINTUSASCII. For RFC
//! 3164, `is_tag_shaped` accepts non-numeric bracket content that is PRINTUSASCII without `]`
//! (so the bracket still balances), so the `[...]` isn't absorbed into the message body.
//!
//! ## The RFC 3164 header
//!
//! nginx's `nohostname` option omits a field RFC 3164 calls mandatory, and nginx's MSG is JSON full
//! of `": "`, so the header can't be parsed by scanning for the first `: ` or assuming HOSTNAME is
//! present. `parse_3164`'s rule:
//!
//! 1. `<PRI>`: `<`, 1-3 digits, `>`. A missing or non-numeric PRI is a malformed line.
//! 2. The `Mmm dd hh:mm:ss` timestamp (15 bytes), if present; absent is tolerated.
//! 3. **At most the next two whitespace-delimited tokens** are HOSTNAME/TAG candidates. If the
//!    first is TAG-shaped, there is no hostname. Otherwise, if the second is TAG-shaped, the first
//!    is the hostname. Everything after the TAG token (minus one leading space) is MSG.
//! 4. If neither is TAG-shaped, there is no tag: the whole remainder is MSG, with no `syslog.tag`.
//!    **The two-token bound is what makes this safe**: an unbounded "find the first `: `" scan
//!    would find one inside a JSON body on a tag-less message and truncate the log line.
//! 5. `tag[pid]:` splits into `syslog.tag` + `syslog.pid`.
//!
//! `is_tag_shaped` is stricter than "ends in `:` or `]:`": the token's body must also look like a
//! process name (letters, digits, `_`, `-`, `.`, `/`, optionally a bracketed PID). Otherwise a
//! tag-less message starting `{"status": 200, ...}` would have `{"status":` taken for a tag and
//! part of its body eaten; no real tag contains `{` or `"`.

pub mod decode;

pub use decode::SyslogDecoder;
