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
- **An RFC 3164 `syslog_out` message with no TAG has its HOSTNAME read as part of MSG.** A log
  with no `syslog.tag` and no `app_name:` leaves `syslog_out` under `format: rfc3164` as
  `<PRI>TIMESTAMP HOSTNAME MSG`. RFC 3164 makes no field certain, so `syslog_in`'s header rule
  (`crates/logit-proto/src/syslog/mod.rs`'s "The RFC 3164 header") reads a HOSTNAME only before a
  TAG-shaped token, and here there's none.
  - **Consequence:** the receiver reads back no `host.name` and a message that starts with the
    hostname and a space. A message whose first word is TAG-shaped, such as `error: disk full`,
    reads back with that word as `syslog.tag`. The PID needs a TAG too, so `process.pid` or
    `syslog.pid` is left off, uncounted.
  - **Workaround:** set `app_name:` on `syslog_out`, or `syslog.tag` upstream with a `set`
    component, so every message has a TAG; or use the default `format: rfc5424`, whose header
    fields are positional.
- **`syslog_out` leaves off every attribute outside its header fields, uncounted.** Only
  `syslog.*`, `host.name`, `process.pid`, and the `_logit.syslog.severity` carrier reach the wire.
  Under RFC 3164 there's no field for anything else, MSGID and STRUCTURED-DATA included. Under RFC
  5424 the opt-in `structured_data:` element carries the rest, as the entry above says; without
  it, no counter moves. A log's metrics, and a span beside a log, are left off the same way.
  - **Workaround:** under RFC 5424, set `structured_data:`, which `syslog_in` reads back inside
    `syslog.sd`.
- **`syslog_out`'s `structured_data:` writes a `Bytes` attribute as its length, and an empty
  array as nothing, uncounted.** A PARAM-VALUE is text, so the encoder renders a `Bytes` value as
  `<N bytes>`, and an `Array` as one PARAM per element, which writes no PARAM for an empty one.
  Neither moves a counter, so an opted-in empty array leaves no PARAM and nothing counts it. A
  null attribute, which `datadog_in` decodes from a JSON `null` and `otlp_in` from an `AnyValue`
  with no value set, is an empty PARAM-VALUE, read back as the empty string. A key that isn't an
  RFC 5424 SD-NAME, and every attribute of a log whose own `syslog.sd` already holds the
  configured SD-ID, are left off counted, under
  `logit.output.structured_data.omitted{reason=invalid_sd_name}` and `{reason=sd_id_collision}`.
  - **Consequence:** the receiver reads back `<N bytes>` in place of the bytes, no PARAM for an
    empty array, and `""` for a null.
  - **Workaround:** rewrite a `Bytes` attribute as text in a `lua` stage ahead of `syslog_out`.
- **`syslog_out` counts a skipped event with no `reason` tag.** An event with no log, a span or
  metrics alone, counts `logit.output.events.skipped` with no tag, where every other sink that
  counts the series tags it (`EncodeStats::skipped_no_log` in
  `crates/logit-outputs/src/syslog.rs`). A query or alert that filters the series on `reason`
  misses these.
