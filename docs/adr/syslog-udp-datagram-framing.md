---
created: 2026-10-16
updated: 2026-10-16
---

# `syslog_in` UDP framing: `framing: lines | datagram`, with today's per-line split as the default

## Status
Accepted

## Context
`syslog_in` over UDP splits every datagram on `\n` and parses each piece as its own message
(`SyslogDecoder::decode_into`, `crates/logit-proto/src/syslog/decode.rs`). The split exists for
senders that pack several messages into one datagram: nginx's `syslog:` writer is the reason, and
its `escape=json` guarantees no raw newline inside one message.

A sender whose single message has a multi-line MSG loses every line after the first. Prusa's
Buddy firmware sends one RFC 5424 message per datagram whose MSG is a header and about 37 InfluxDB
line-protocol points, one per line ([`prusa-metrics`](../plans/prusa-metrics.md) has the capture).
After the split, only the first piece starts with `<PRI>`, so the rest are counted
`logit.input.messages.dropped{reason="malformed"}`. The same split already cuts a binary MSG at any
`0x0A` byte ([syslog known gaps](../known-gaps/syslog.md)), whose only workaround is
`transport: tcp`. The firmware sends UDP only.

RFC 5426 §3.1 says each syslog UDP datagram carries one message. The per-line split is a `logit`
leniency for real senders that break that rule, and it can't serve both kinds of sender at once.

The TCP arm already decodes one message per frame: `SyslogInput::tcp` builds its decoder with
`SyslogDecoder::with_line_splitting(false)`, and RFC 6587 framing is detected per connection
([ADR `syslog-tcp-ingress-and-tls`](syslog-tcp-ingress-and-tls.md)). The config doc says
`syslog_in` has no `framing:` field because TCP needs none.

## Decision
`syslog_in` gains `framing: lines | datagram`, which applies to `transport: udp` only.

- **`lines`, the default,** keeps today's behavior: a datagram splits on `\n`, with one trailing
  `\r` stripped from each piece, and each piece is a message.
- **`datagram`** makes one datagram one message. The decoder is the TCP arm's,
  `SyslogDecoder::with_line_splitting(false)`: one trailing `\n` is stripped, then one `\r` behind
  it, and every other byte, `\n` included, stays in the MSG. A multi-line MSG and a binary MSG
  both survive.
- **`framing:` set to any value under `transport: tcp` is a config error**, from a new graph rule,
  rather than ignored. TCP framing stays detected per connection. The field is an `Option` in
  `logit_config` so that an explicit `framing: lines` on TCP is rejected too.
- **Field name.** `framing` is free: no `syslog_in` field uses it, and the only collision is the
  config doc's sentence "There is no `framing:` field", which changes to say the field is UDP-only. The
  operator's question is how a datagram maps to messages, and RFC 5426 and RFC 6587 call that
  framing. Its values name the unit of a message, `lines` or the whole `datagram`.

Nothing else changes: the receive queue, `receive:`, `peer:`, `reuse_port`, and the per-message
parse are the same under either value. A datagram is bounded by the UDP receive path, as it is
today.

## Alternatives considered
- **Make `datagram` the default, following RFC 5426.** It would silently break nginx and every
  other sender that packs several messages per datagram, which is what the listener was built for.
- **Detect the framing per datagram,** for example splitting only when a later line starts with
  `<PRI>`. A MSG line can start with `<` and digits, so the guess is wrong for some input, and an
  operator can't tell from the config which rule applied.
- **`split_lines: bool`.** Equivalent today, but a bool can't take a third value, such as a
  length-prefixed datagram, without a second field.
- **A name scoped to UDP, such as `datagram: lines | message`.** It avoids reusing a word TCP's docs
  use, but it reads as a property of the datagram rather than of the message boundary, and the
  graph rule already makes the TCP case an error with a message that names the field.
- **`lines_in` with `transport: udp` and a stateful `lua` stage.** It works only if a datagram's
  lines reach the script in order with no other datagram's lines between them, which the
  `lines_in` ADR doesn't promise, and it rebuilds the syslog header parse in Lua.

## Consequences
- The binary-MSG known gap gains a second workaround, `framing: datagram`, for a sender that sends
  one message per datagram.
- A `framing: datagram` listener fed by a packing sender such as nginx decodes each datagram as
  one message whose MSG holds the rest of the datagram, newlines included. Nothing counts it,
  because nothing failed to parse. The config doc says which senders need `lines`.
- `lines_in`'s UDP transport keeps its per-line split. A plain-text sender with multi-line
  datagrams is a separate case, not covered here.
