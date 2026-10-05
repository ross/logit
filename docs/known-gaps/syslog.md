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
- **`syslog_out`'s control-character escaping is ambiguous with a message that already contained
  the escape sequence literally.** The encoder (`sanitize_msg`) escapes an embedded newline as the
  two characters `\`/`n` (likewise `\r`, NUL, and every other C0 control and DEL) so it can't
  forge a second syslog message downstream. It leaves a literal backslash untouched, because
  escaping it would double every backslash in a JSON message body and break a `| json` LogQL
  filter on every line.
  - **Consequence:** a message that contained the literal two characters `\`/`n` is
    indistinguishable on the wire from one with a real newline. Accepted in
    [ADR `syslog-output`](../adr/syslog-output.md).
- **SD-ELEMENT/SD-PARAM order is canonicalized by name, not by wire position.**
  `write_structured_data`/`write_sd_element` (`crates/logit-outputs/src/syslog.rs`) sort SD-IDs and
  PARAM-NAMEs by name bytes rather than reproducing `AttrMap`/attribute iteration order, which is
  process-global intern order, not wire order. A repeated PARAM-NAME's occurrences are emitted
  grouped, so a wire `a b a` interleaving comes out as `a a b`. Permitted under
  [ADR `lossless-transit`](../adr/lossless-transit.md)'s attribute-reordering normalization, and
  recorded in
  [ADR `syslog-structured-data-convention`](../adr/syslog-structured-data-convention.md)'s
  Consequences and `crates/logit-cli/tests/syslog_round_trip.rs`'s normalization list.
- **`syslog_out`'s opt-in `structured_data:` element can't carry a log's trace context.** The
  element (`structured_data: { sd_id: "<name>@<PEN>" }`, `write_structured_data`) carries every
  non-`syslog.*` attribute, but a log's native trace context (`log.trace`,
  [ADR `log-record-trace-context`](../adr/log-record-trace-context.md)) isn't an `event.attribute`.
  - **Consequence:** a `trace_context`-enriched log relayed through `syslog_out` loses its trace
    and span ids.
- **No RFC 6012 (DTLS, syslog over TLS over UDP).** `syslog_in` and `syslog_out` support only RFC
  5425 (TLS over TCP); DTLS is out of scope for
  [ADR `syslog-tcp-ingress-and-tls`](../adr/syslog-tcp-ingress-and-tls.md) (see its Alternatives).
  A `tls:` block under `transport: udp` is a config error on both (graph rules 43 and 44), not
  ignored.
  - **Workaround:** `transport: tcp` with `tls:`.
- **A binary syslog MSG with a `0x0A` byte is split on UDP.** A non-UTF-8 MSG decodes to a
  `Value::Bytes` message, but on `syslog_in`'s UDP transport `SyslogDecoder::decode_into`
  (`crates/logit-inputs/src/syslog.rs`) splits on `\n` before parsing, which cuts a binary payload
  at any `0x0A` byte (see the HAProxy "CBOR" entry under
  [HTTP access logs](transforms.md#http-access-logs-nginx-haproxy-and-http_access)).
  - **Workaround:** `transport: tcp`. `SyslogInput::tcp` turns line splitting off
    (`SyslogDecoder::with_line_splitting(false)`), and `logit-inputs::tcp::TcpListener`'s
    octet-counting `Framer` delimits by declared length
    ([ADR `syslog-tcp-ingress-and-tls`](../adr/syslog-tcp-ingress-and-tls.md)), so a `0x0A` inside
    an octet-counted MSG survives end to end. The HAProxy CBOR entry records why the decoder that
    would consume such a payload was measured and not built.
