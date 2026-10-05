---
created: 2026-10-05
updated: 2026-10-05
---

# Listener peer address: opt-in `network.peer.*` on the shared drivers, and PROXY protocol on TCP

## Status
Accepted

## Context
No listener records who sent an event, though every connection and datagram knows. The shared
drivers drop the peer before a decoder sees the bytes:

- `crates/logit-inputs/src/tcp.rs`'s accept loop discards `accept()`'s address
  (`accepted.map(|(s, _)| ...)`).
- `crates/logit-inputs/src/udp.rs`'s `recvmmsg` headers pass a NULL `msg_name`, so the kernel
  never reports a source address.
- `logit_proto::Decoder::decode_into` has no peer parameter.

A pipeline can't tell which of several senders on one listener wrote an event, or route on it. The
workaround is one listener per sender, each stamped by a `set` stage, which doesn't scale past a
handful of senders and doesn't work for a sender behind a load balancer. The gap is recorded in
[runtime gaps](../known-gaps/runtime.md).

[ADR `plain-lines-listener`](plain-lines-listener.md) rejected a peer-address attribute for
`lines_in` on the grounds that the hook belongs in the shared drivers and every TCP input would
carry it. This ADR reverses that rejection by building the hook there, for every listener at once.

## Decision
Every listener on the shared TCP, UDP, and Unix-socket drivers gains an opt-in `peer: bool`,
default `false`. When it's on, the driver stamps each event with the immediate socket peer as
`network.peer.address` and `network.peer.port`. TCP transports also gain an opt-in
`proxy_protocol: bool` that reads a PROXY protocol header and stamps the origin it names as
`client.address` and `client.port`.

The listeners in scope are those on the shared drivers: `statsd_in`, `syslog_in`, `graphite_in`,
and `lines_in` on every transport they offer, and `collectd_in` on UDP.

### Peer attributes
- **Names.** OpenTelemetry semantic conventions: `network.peer.address` is the immediate socket
  peer, `network.peer.port` its port. The address is a `Value::Str` in the standard text form,
  with an IPv4-mapped IPv6 address written as IPv4, so one sender reads the same on a dual-stack
  socket and a v4 one. The port is a `Value::Int`.
- **Event attributes, never the resource.** A UDP batch mixes senders, and `BatchAccumulator`
  compares resources with `Arc::ptr_eq`, so a per-sender resource would flush a batch at every
  sender change. One location on every transport keeps a downstream config the same whichever
  transport feeds it.
- **Unix sockets.** A bound peer's path is the address, with no port. An unbound peer, the usual
  case for a client socket, gets neither attribute.
- **No reverse DNS.** A lookup blocks, its answer is whatever the peer's resolver says, and a
  resolver stall would become an intake stall. An operator who wants a name maps addresses
  downstream.
- **Collisions.** The driver writes after the decoder, so its value replaces a same-named
  attribute a decoder produced. The operator asked for the observed peer.

### Hook and cost
The driver stamps the events that `decode_into` appended to its output for that connection or
datagram. The `Decoder` trait doesn't change, and no decoder learns about peers.

The values are `Value::Str(Bytes)`, so stamping an event is a reference-count increment. The TCP
driver builds the values once per connection. The UDP driver formats once per datagram and reuses
the previous datagram's values when the sender repeats. With `peer: false` the code path is the
one that exists today, so the existing allocation pins don't move; the `peer: true` path gets pins
of its own ([`docs/design/memory.md`](../design/memory.md)).

### PROXY protocol
HAProxy's [PROXY protocol](https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt) lets a
load balancer pass the original client's address ahead of the stream.

- **TCP transports only.** A graph rule rejects `proxy_protocol: true` on UDP and Unix-socket
  transports.
- **Required when on.** Every connection must open with a header. It's never auto-detected: the
  spec's security note says a receiver must not guess, because a client that can reach the
  listener directly could then forge its own origin.
- **Both versions.** v1 (text) and v2 (binary) are told apart by their signatures.
- **Before TLS.** The header is read off the raw stream before the TLS handshake, as a proxy sends
  it, under the listener's `handshake_timeout`.
- **Failure closes the connection.** A missing, malformed, or slow header closes it with a
  throttled diagnostic, counted as `logit.input.connections.rejected{reason="proxy_header"}`.
- **`LOCAL` keeps the socket peer.** A v2 `LOCAL` command (a proxy's own health check) and a v1
  `UNKNOWN` carry no origin, so no `client.*` attribute is stamped.
- **TLVs are ignored.** v2's type-length-value extensions are skipped by their length.
- **Attributes.** The header's source address and port become `client.address` and `client.port`
  whether or not `peer:` is on. `network.peer.*`, when on, stays the proxy, which is what semconv
  means by it.

The parser is hand-rolled, not the `ppp` crate (Apache-2.0, version 2.3.0 at writing). The format
is small: a v1 header is one CRLF-terminated text line of at most 107 bytes, and a v2 header is a
16-byte fixed part followed by a length-prefixed address block of 12, 36, or 216 bytes. `logit`
needs only the command, the family, and the source address and port. The work a crate wouldn't
save is the stream side: how many bytes to read before parsing, the 107-byte cap, and the timeout.
A dependency would add a single-maintainer crate to `deny.toml`'s and `script/audit`'s scope for
what the pickle reader, the gRPC framing, and the native codec show this codebase writes itself.
The parser lives in `logit-proto`, beside the other codecs that read peer bytes, so it joins the
fuzz targets ([ADR `out-of-ci-fuzzing`](out-of-ci-fuzzing.md)), whose workspace depends on
`logit-core` and `logit-proto` only.

## Alternatives considered
- **On by default.** Two of the lossless pairs this would touch, `statsd_in -> statsd_out` and
  `syslog_in -> syslog_out`, would gain attributes the sender never sent
  ([ADR `lossless-transit`](lossless-transit.md)). Every series keyed by attributes would split by
  sender, and every event would carry an IP address, which is personal data in some
  jurisdictions. Each of those is an operator's call.
- **Resource attributes.** The batch-splitting cost above, on every UDP listener with more than one
  sender.
- **A peer parameter on `Decoder::decode_into`.** It changes every decoder for a value none of them
  reads. Stamping after decode needs no decoder change.
- **A hostname from reverse DNS.** Rejected under "No reverse DNS" above.
- **Auto-detecting the PROXY header.** The spec forbids it for the forgery reason above, and a v1
  header is plain text that a `lines_in` or carbon plaintext sender could send by accident.

## Consequences
- An operator turns on `peer:` per listener. With it on, a sender's address reaches every sink
  that writes attributes, so `keep` or `remove` ahead of a sink is how to keep it out of one.
- A listener behind a load balancer reports the load balancer as `network.peer.address`. The
  origin needs `proxy_protocol: true` and a proxy configured to send the header.
- With `proxy_protocol: true`, a client that connects without the header is refused. A health
  check has to go through the proxy or use v2 `LOCAL`.
- Deferred, each its own follow-up:
  - The HTTP listeners (`otlp_in`, `datadog_in`, `datadog_trace_in`, `splunk_hec_in`) and
    `logit_in`, which don't use the shared drivers.
  - A mutual-TLS client's identity (`tls.client.subject`) and the server name it asked for.
  - `SO_PEERCRED` on `unix_stream`, which would name a local sender's process and user.
  - `network.connection.id`, to tell two connections from one address apart.
