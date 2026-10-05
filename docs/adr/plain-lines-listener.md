---
created: 2026-10-05
updated: 2026-10-05
---

# `lines_in`: a plain-lines listener that emits one raw log event per line and parses nothing

## Status
Accepted

## Context
Two documented gaps had the same cause: `logit` had no listener that takes a newline-delimited line
as plain text.

- An application that writes JSON lines to a Datadog Agent's TCP `logs` port can't point at `logit`
  instead, because `syslog_in` parses each line as a syslog message
  ([Datadog known gaps](../known-gaps/datadog.md)).
- A Splunk heavy forwarder's `[tcpout]` with `sendCookedData = false` writes each event's raw text
  followed by one LF and nothing else. The interop run observed that framing
  (`tools/splunk-interop/README.md`, "What the run showed"), and no listener could read it
  ([Splunk known gaps](../known-gaps/splunk.md)).

The shared stream and datagram drivers already provide what such a listener needs: LF framing
(`statsd_in`, carbon plaintext), the connection cap, TLS, and timeouts
([ADR `decoupled-listener-io`](decoupled-listener-io.md),
[ADR `syslog-tcp-ingress-and-tls`](syslog-tcp-ingress-and-tls.md)).

## Decision
Add the kind `lines_in`. One line is one raw log event, and the listener parses nothing.

- **Transports.** `transport:` is `tcp` (the default, with an optional `tls:` block), `udp`, `unix`,
  or `unix_stream`, each on the shared driver `statsd_in` uses for it. A Unix socket file is made
  mode `socket_mode:`, `0722` by default, as `statsd_in`'s is
  ([ADR `datadog-agent-and-intake-relay`](datadog-agent-and-intake-relay.md)'s socket-mode
  amendment).
- **No parsing.** The event is a log with `BodyFormat::Raw`, no attributes, and no severity. An
  operator composes `json`, `logfmt`, `kv`, or `regex` after it, as the pipeline model already
  expects.
- **Body type.** The message is `Value::Str` when the line is valid UTF-8 and `Value::Bytes`
  otherwise, as `syslog_in` keeps a MSG. No byte is rewritten.
- **Identity.** The resource is empty and nothing about the peer, such as its address, is
  attached. A `set` stage per listener stamps what the operator knows.
- **Line bound.** `max_line_bytes` defaults to 64 KiB. A longer line is dropped and counted as
  `logit.input.frames.dropped{reason="oversize"}` on every transport, and the lines around it
  still decode.
- **Stream framing.** LF-terminated, with one trailing CR stripped, and an empty line skipped. An
  unterminated tail when a connection ends is dropped and counted `reason="truncated"`, the stream
  driver's rule for every line-framed listener. `unix_stream` frames the same way, not with
  `statsd_in`'s length prefix.
- **Datagram framing.** A datagram splits on LF, and its end also ends its last line, so an
  unterminated tail is emitted.

[`crates/logit-inputs/src/lines.rs`](../../crates/logit-inputs/src/lines.rs)'s module doc is the
canonical copy of the framing and event rules.

## Alternatives considered
- **Lossy UTF-8, as `tail_in` does.** Rewriting invalid bytes loses data a relay should carry
  ([ADR `lossless-transit`](lossless-transit.md)). The `Str`-or-`Bytes` split costs a downstream
  stage one type check.
- **A peer-address attribute.** It needs a hook in the shared driver and in `Decoder` that every
  TCP input would then carry. The need is tracked as a known gap
  ([runtime gaps](../known-gaps/runtime.md)), and a `set` stage covers it for a listener with one
  known sender.
- **TCP only.** The Datadog Agent's `logs` listener takes UDP too, and a local Unix socket is the
  usual path for a same-host application. The shared drivers provide the other transports at no
  extra cost.
- **A parsing mode inside the listener** (`format: json`). It duplicates `json`, `logfmt`, `kv`, and
  `regex`, and every parsing option would have to be added to this one component as well.

## Consequences
- A message with an embedded newline arrives as several events. The wire can't carry the newline,
  so a `regex` or `lua` stage downstream has to merge them.
- A stream sender gets no acknowledgment beyond TCP flow control, and a partial last line at a
  close is lost, counted `truncated`.
- A Splunk forwarder's `[tcpout]` output reaches `logit` without `host`, `source`, `sourcetype`, or
  `index`, because the cooked-off wire carries none. The operator stamps them with `set`.
- A change to the shared drivers' line framing changes this listener too.
