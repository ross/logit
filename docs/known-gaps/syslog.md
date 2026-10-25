# Known gaps: syslog

Entry format and the other areas: [the known-gaps index](README.md).

- **`syslog_out` re-stamps a relayed timestamp with receipt time when the origin's can't be
  rendered on the output format.** Under the precedence rule in
  [ADR `syslog-structured-data-convention`](../adr/syslog-structured-data-convention.md)
  (`write_5424_timestamp`/`write_3164_timestamp`, `crates/logit-outputs/src/syslog.rs`), a
  `Value::Timestamp` `syslog.timestamp` renders on either format. A `Value::Str` renders verbatim
  only on a 3164 output, and only in the 15-byte `Mmm dd hh:mm:ss` shape
  (`is_rfc3164_timestamp_shape`). These cases fall through to `event.timestamp` (receipt time):
  - a nil `Value::Null` relayed onto a 3164 output (3164 has no NILVALUE);
  - an absent attribute.

  A `timestamp` component with `from: syslog.timestamp` and `format: rfc3164` ahead of
  `syslog_out` makes the fall-through render the sender's instant
  ([ADR `timestamp-transform`](../adr/timestamp-transform.md)).
- **`syslog_out`'s opt-in `structured_data:` element can't carry a log's trace context.** The
  element (`structured_data: { sd_id: "<name>@<PEN>" }`, `write_structured_data`) carries every
  attribute except `syslog.*`, `host.name`, and `process.pid`, but a log's native trace context (`log.trace`,
  [ADR `log-record-trace-context`](../adr/log-record-trace-context.md)) isn't an `event.attribute`.
  - **Consequence:** a `trace_context`-enriched log relayed through `syslog_out` loses its trace
    and span ids.
- **A binary syslog MSG with a `0x0A` byte is split on UDP.** A non-UTF-8 MSG decodes to a
  `Value::Bytes` message, but on `syslog_in`'s UDP transport `SyslogDecoder::decode_into`
  (`crates/logit-proto/src/syslog/decode.rs`) splits on `\n` before parsing, which cuts a binary
  payload at any `0x0A` byte (see the HAProxy "CBOR" entry under
  [HTTP access logs](transforms.md#http-access-logs-nginx-haproxy-and-http_access)).
  - **Workaround:** `framing: datagram` on `transport: udp`, for a sender that sends one message
    per datagram. `SyslogInput::udp_datagram` turns line splitting off, so a `0x0A` inside the MSG
    survives. The default, `framing: lines`, still splits
    ([ADR `syslog-udp-datagram-framing`](../adr/syslog-udp-datagram-framing.md)).
  - **Workaround:** `transport: tcp`. `SyslogInput::tcp` turns line splitting off
    (`SyslogDecoder::with_line_splitting(false)`), and the octet-counting `Framer`
    (`logit_proto::framing`) that `logit-inputs::tcp::TcpListener` drives delimits by declared
    length
    ([ADR `syslog-tcp-ingress-and-tls`](../adr/syslog-tcp-ingress-and-tls.md)), so a `0x0A` inside
    an octet-counted MSG survives end to end. The HAProxy CBOR entry records why the decoder that
    would consume such a payload was measured and not built.
