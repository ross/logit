# Known gaps: syslog

Entry format and the other areas: [the known-gaps index](README.md).

- **`event.timestamp` is still receipt time, not the sender's.** `syslog_in` stamps every event
  with the instant its datagram came off the socket (`received_at`, captured by the read half and
  passed to `Decoder::decode_into`; [ADR `decoupled-listener-io`](../adr/decoupled-listener-io.md)).
  It keeps the sender's own timestamp separately as the `syslog.timestamp` attribute: a
  `Value::Timestamp` for RFC 5424's RFC 3339 form, a raw `Value::Str` for RFC 3164's, or
  `Value::Null` for a nil 5424 TIMESTAMP. The two always diverge by network and queueing delay, and
  diverge arbitrarily when the sender's clock is skewed or messages are replayed or relayed.
  `syslog_out`'s emitted TIMESTAMP follows the precedence rule in
  [ADR `syslog-structured-data-convention`](../adr/syslog-structured-data-convention.md), so a
  `syslog_in -> syslog_out` relay's wire timestamp can reflect the origin even though
  `event.timestamp` doesn't. Tracked as debt against
  [ADR `lossless-transit`](../adr/lossless-transit.md); the residual-debt list is in
  [`docs/plans/lossless-transit.md`](../plans/lossless-transit.md).
  - **Consequence:** everything keyed on time (`aggregate`'s tumbling window, the point timestamp
    `influxdb_out` writes) uses `event.timestamp`, so a delayed or replayed message lands in the
    window it arrived in, not the one it happened in.
  - **Why not derive it from the sender:** RFC 3164's timestamp has no year and no timezone, so
    resolving it to an instant means guessing both. Resolving only RFC 5424 would give two senders
    on one listener different timestamp semantics with nothing in the config saying so.
  - **Worth exploring: an optional `syslog_timestamp` transform**, added to a flow explicitly,
    that replaces `event.timestamp` with a resolved `syslog.timestamp` and makes the guesswork
    configurable. A separate, opt-in component rather than a `syslog_in` flag keeps the listener's
    contract simple and makes "we trust our senders' clocks" a visible line in the config graph.
    It would need:
    - RFC 5424: parse the RFC 3339 timestamp directly, with no inference.
    - RFC 3164: fill in year and timezone. The default year is the one that puts the message
      closest to receipt time (handling a New Year's Eve rollover both ways). An explicit
      `timezone:` field defaults to UTC, never the host's local zone, which would make behavior
      depend on an environment variable.
    - A bounded sanity window (`max_skew:`, say): a resolved timestamp further from receipt time
      than the window is rejected, keeping receipt time, with a throttled diagnostic. Without it,
      one sender with a badly wrong clock can write points years away and poison a dashboard.
    - The skip rule every other transform follows: an event with no `syslog.timestamp`, or one
      that doesn't resolve, passes through with `event.timestamp` untouched, never dropped.
- **`syslog_out` re-stamps a relayed timestamp with receipt time when the origin's can't be
  rendered on the output format.** Under the precedence rule in
  [ADR `syslog-structured-data-convention`](../adr/syslog-structured-data-convention.md)
  (`write_5424_timestamp`/`write_3164_timestamp`, `crates/logit-outputs/src/syslog.rs`), a
  `Value::Timestamp` `syslog.timestamp` renders on either format. A `Value::Str` renders verbatim
  only on a 3164 output, and only in the 15-byte `Mmm dd hh:mm:ss` shape
  (`is_rfc3164_timestamp_shape`). These cases fall through to `event.timestamp` (receipt time):
  - a 3164-origin `Value::Str` relayed onto a 5424 output (no year or timezone to build an RFC 3339
    stamp from);
  - a nil `Value::Null` relayed onto a 3164 output (3164 has no NILVALUE);
  - an absent attribute.

  The `syslog_timestamp` transform sketched in "`event.timestamp` is still receipt time" (this
  file) would resolve `event.timestamp` itself, in either direction.
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
