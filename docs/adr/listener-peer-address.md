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
and `lines_in` on every transport they offer, and `collectd_in` on UDP. The HTTP listeners take the
same two options under the amendment below.

### Peer attributes
- **Names.** OpenTelemetry semantic conventions: `network.peer.address` is the immediate socket
  peer, `network.peer.port` its port. The address is a `Value::Str` in the standard text form,
  with an IPv4-mapped IPv6 address written as IPv4, so one sender reads the same on a dual-stack
  socket and a v4 one. The port is a `Value::I64`.
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
driver builds the values once per connection. The UDP driver formats them only when a datagram's
sender differs from the previous datagram's. With `peer: false` the code path is the
one that exists today, so the existing allocation pins don't move; the `peer: true` path gets pins
of its own ([`docs/design/memory.md`](../design/memory.md)).

### PROXY protocol
HAProxy's [PROXY protocol](https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt) lets a
load balancer pass the original client's address ahead of the stream.

- **TCP transports only.** A graph rule rejects `proxy_protocol: true` on UDP and Unix-socket
  transports.
- **Required when on.** Every connection must open with a header, and it's never auto-detected,
  as the spec requires. Requiring it only rejects a client that connects without one by mistake.
  It doesn't stop a client that reaches the listener directly from sending a header of its own
  and naming any origin. `client.*` is as trustworthy as the network path to the listener, so the
  operator makes the port reachable only through the proxy, the same operator boundary
  [ADR `deployment-threat-model`](deployment-threat-model.md) draws for every listener.
- **Both versions.** v1 (text) and v2 (binary) are told apart by their signatures.
- **Before TLS.** The header is read off the raw stream before the TLS handshake, as a proxy sends
  it, under the listener's `handshake_timeout`.
- **Failure closes the connection.** A missing, malformed, or slow header closes it with a
  throttled diagnostic, counted as `logit.input.connections.rejected{reason="proxy_header"}`.
- **No origin keeps the socket peer.** A v2 `LOCAL` command (a proxy's own health check), a v2
  `PROXY` with family `AF_UNSPEC` or transport `UNSPEC`, a v2 `AF_UNIX` source with an empty path
  (an unnamed or abstract socket), and a v1 `UNKNOWN` carry no usable origin, so the connection
  is accepted and no `client.*` attribute is stamped. A `DGRAM` transport's source is an origin
  like a `STREAM` one's.
- **TLVs are ignored.** v2's type-length-value extensions follow the address block inside the
  header's length and are skipped unread, so a `PP2_TYPE_CRC32C` checksum isn't verified, which
  the spec permits a receiver that doesn't implement it.
- **Attributes.** The header's source address and port become `client.address` and `client.port`
  whether or not `peer:` is on. A v2 `PROXY` with family `AF_UNIX` stamps the source path as
  `client.address` and no `client.port`. `network.peer.*`, when on, stays the proxy, which is what
  semconv means by it. `client.*` follows the same collision rule as `network.peer.*`: the
  driver's value replaces a same-named attribute a decoder produced.

The parser is hand-rolled, not the `ppp` crate (Apache-2.0, version 2.3.0 at writing). The format
is small. A v1 header is one CRLF-terminated text line of at most 107 bytes. A v2 header is a
16-byte fixed part whose last 2 bytes give the length of what follows, 0 to 65,535 bytes: the
address block (0 bytes for `AF_UNSPEC`, 12 for IPv4, 36 for IPv6, 216 for `AF_UNIX`) and then any
TLVs. The parser reads the whole length, takes the address block, and skips the rest. `logit`
needs only the command, the family, and the source address and port. The work a crate wouldn't
save is the stream side: how many bytes to read before parsing, the 107-byte v1 cap and the
16-plus-length v2 bound, and the timeout.
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
- **Auto-detecting the PROXY header.** The spec forbids it: a listener that guesses accepts a
  header from every client, including the direct ones it also serves, so any of them can name its
  own origin. A v1 header is also plain text that a `lines_in` or carbon plaintext sender could
  send by accident.

## Consequences
- An operator turns on `peer:` per listener. With it on, a sender's address reaches every sink
  that writes attributes, so `keep` or `remove` ahead of a sink is how to keep it out of one.
- A listener behind a load balancer reports the load balancer as `network.peer.address`. The
  origin needs `proxy_protocol: true` and a proxy configured to send the header.
- With `proxy_protocol: true`, a client that connects without the header is refused. A health
  check has to go through the proxy or use v2 `LOCAL`.
- `client.*` is only as trustworthy as the network path to the listener. A client that can reach
  the port directly can send its own header and name any origin, so a `proxy_protocol: true` port
  must be reachable only through the proxy.
- Deferred, each its own follow-up:
  - `logit_in`, which doesn't use the shared drivers. The HTTP listeners are in scope under the
    amendment below.
  - A mutual-TLS client's identity (`tls.client.subject`) and the server name it asked for.
  - `SO_PEERCRED` on `unix_stream`, which would name a local sender's process and user.
  - `network.connection.id`, to tell two connections from one address apart.
  - An allowlist of trusted proxy source addresses for `proxy_protocol:`, so a header from any
    other peer is refused.

## Amendment (2026-10-05): the HTTP listeners

`peer:` and `proxy_protocol:` extend to the listeners that accept their own connections:
`otlp_in` (HTTP and gRPC), `datadog_in`, `datadog_trace_in` (TCP and its Unix socket),
`splunk_hec_in`, and `prometheus_in`'s remote-write receiver. The attribute names, their text form,
the cost, the collision rule, and the PROXY rules are the ones in the Decision above. Only where
the stamp happens differs:

- **Once per request.** Each listener stamps after it decodes a request and before it delivers
  anything from it, on every batch the request produced. A request that decodes into several
  batches (one per OTLP resource, one per HEC envelope) carries the same values on each.
- **Built once per connection.** The values come from the socket peer and the PROXY header,
  both fixed for the connection's life.
- **The PROXY header comes first.** On a TCP listener under `proxy_protocol: true`, the header is
  read after the connection permit and before the TLS accept or the plaintext first-byte peek,
  under the listener's `handshake_timeout`.
- **`datadog_trace_in`'s Unix socket** follows the `unix_stream` rule under "Peer attributes": a
  bound client's path is the address, with no port, and an unbound client gets neither attribute.
  `proxy_protocol:` applies only to its TCP `bind:` listener.

An L7 proxy puts the client in a forwarding header rather than a PROXY header. [ADR
`forwarded-header-parsing`](forwarded-header-parsing.md) decides how these listeners read one, and
how its `client.*` relates to a PROXY origin. [The `hpeer` plan](../plans/http-listener-peer-address.md)
has the per-listener stamp points.

## Verification of the datagram read
`script/unsafe-check`'s `udp-peer-eintr-retry` scenario, run on 2026-10-05 against
`peer_stamps_each_udp_senders_own_address_and_port`, passes. The trace shows the injected `EINTR`
on the first `recvmmsg`, then a retry returning all eight datagrams, each header decoded by
`strace` as `msg_name={sa_family=AF_INET, sin_port=..., sin_addr=inet_addr("127.0.0.1")}` with
`msg_namelen=128 => 16`. So the retry rebuilds every header with the full `sockaddr_storage` size,
and the kernel shrinks it to the address it wrote. `script/unsafe-check miri` runs the `msg_name`
construction, the `msg_namelen` harvest, and the address parser in `batch_reader_helpers`.
