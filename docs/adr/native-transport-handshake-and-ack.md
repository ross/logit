---
created: 2026-09-09
updated: 2026-09-09
---

# Native transport: handshake, implicit sequencing, and per-batch acknowledgement

## Status
Accepted

## Context

[ADR `native-wire-format-encoding`](native-wire-format-encoding.md) settled the payload format;
[`docs/design/wire-protocol.md`](../design/wire-protocol.md) sketched the connection protocol's
shape (TCP, TLS via `rustls`, a version/codec/compression handshake, credit-based flow control) but
left it as future work. `docs/plans/native-transport.md` is the enabling plan that turned that
sketch into `logit_in`/`logit_out`, real, running `ComponentKind`s. This ADR records the specific
choices that plan made and this repo now runs, per `AGENTS.md`'s "don't pick a design in passing
while implementing something else, record the outcome as an ADR" rule — those choices are narrow
enough (how one connection's frames are numbered and acknowledged) not to need their own design
doc, but consequential enough to want a yes/no record of what was decided.

## Decision

**Framing.** The existing 24-byte `frame.rs` header, unchanged. `flags` bit 0
([`FLAG_CONTROL`](../../crates/logit-proto/src/frame.rs)) marks a control frame (handshake message
or ack) rather than a native-v1 data frame; every other bit stays reserved. This is exactly the use
`HEADER_LEN`'s own doc comment already earmarked those bits for.

**Control payload.** Hand-rolled TLV over `native::varint` (`crates/logit-proto/src/native/
control.rs`), the same `tag(u8) + len(uvarint) + payload` shape and skip-unknown forward
compatibility as `native::record`: `Hello { version, codecs, compressions, max_frame_bytes, window
}`, `HelloAck { version, codec, compression, max_frame_bytes, window }`, `Ack { seq }`, `Reject {
code, message }`.

**Sequence numbers are implicit.** TCP is ordered, so the Nth data frame on a connection is always
seq N; `Ack.seq` is the cumulative count of data frames the receiver has forwarded. No seq field on
the data frame itself, so the native-v1 payload is untouched, and a future credit window > 1 can
use cumulative acks unchanged.

**Ack point: after `Fanout::send` returns**, not after the frame is merely decoded. A stalled
downstream delays the ack, which stalls the sender's own delivery attempt — that *is* this
protocol's backpressure. `logit_in` needs no receive-side queue on top of this; the ack itself is
the queue depth of one.

**Handshake.** The connecting side (`logit_out`) sends `Hello` first; the listener replies
`HelloAck` (codec/compression = the intersection, its own `max_frame_bytes`, its own `window`) or
`Reject` and closes. A version mismatch or no shared codec is `Reject`, not a silently-corrupted
stream.

**In-flight: one frame per connection, in this plan.** `window` is negotiated and recorded in both
directions, but the sender only ever has one frame outstanding — `LogitOutput`'s own `SinkQueue`
`peek`/`commit` is the retransmit state, and the ack point above is what makes even that one frame
safe. Credit-based flow control (several frames outstanding, cumulative acks against them) is real,
designed-for future work — the field exists in the handshake specifically so it can be added
without a wire-format version bump — not built here.

**Connection limit: reject outright, not queue.** `logit_in` uses a non-blocking
`try_acquire_owned` on its connection-count semaphore: a connecting client past the cap gets an
immediate `Reject` and the connection closes, rather than `otlp_in`'s shape (a blocking
`acquire_owned`, where the connection is accepted at the kernel level and then left with a
handshake that silently never starts). `logit`-to-`logit` peers are expected to retry on their own,
the same assumption the ack-driven backpressure above already leans on — an explicit "try later"
is more useful to that peer than a hung connection. That `Reject` is written after the TLS wrap
when TLS is configured, not onto the raw `TcpStream` — a TLS-configured `logit_out` past the cap is
waiting for a ServerHello, not framed bytes, so writing the reject in the clear would look like a
protocol violation rather than a clean, decodable refusal. The cost is one TLS handshake per
rejected connection instead of one write, bounded per-connection by the same handshake timeout the
`Hello` read itself uses but not bounded in count (there is no permit to hold while it happens).

**`Reject.code` classification: only three codes are permanent.** `REJECT_VERSION_MISMATCH`,
`REJECT_NO_COMMON_CODEC`, and `REJECT_FRAME_TOO_LARGE` name a condition that retrying the identical
`Hello`/frame would hit identically, so `logit_out` classifies those `Fault::Permanent`. Every
other code — `REJECT_INTERNAL` (the peer is at its connection cap) and `REJECT_GOING_AWAY` (the
peer is shutting down), plus any code a future peer adds — is transient: `logit_out` classifies
those `Fault::Clean` at the handshake (nothing written yet) and `Fault::Ambiguous` once a data
frame has already left (the batch may or may not have landed). The `Ambiguous` case is genuinely
reachable, not just theoretical: `serve_connection`'s per-frame `select!` only races shutdown
against the frame *header* read, so it can take the shutdown arm with a header already readable —
`GOING_AWAY` arrives in place of the `Ack` for a batch that may or may not have been forwarded.

**Shutdown: `logit_in` overrides `Input::run_until_shutdown`.** Every spawned connection task holds
its own `Fanout` clone (the cancel-by-drop shutdown mechanism [ADR
`service-lifecycle-and-output-retry`](service-lifecycle-and-output-retry.md) depends on requires
nothing to outlive the listener's own future), so each connection races its next-frame read against
a cloned shutdown signal and, once idle at a frame boundary, sends `Reject{GOING_AWAY}` and closes.
An already-in-flight frame is allowed to finish. `otlp_in` has no such override and can leave an
idle keep-alive connection's `Fanout` clone open past shutdown — a real gap, tracked in
`docs/known-gaps.md`, that this design deliberately doesn't repeat.

## Alternatives considered

- **Reuse gRPC-over-hyper** (`otlp_out`'s hand-rolled transport, [ADR
  `hand-rolled-grpc-over-hyper`](hand-rolled-grpc-over-hyper.md)) for `logit`-to-`logit` traffic
  too, rather than a bespoke framing. Rejected: the entire point of the native path is no
  per-request HTTP/gRPC framing overhead — the bake-off in [ADR
  `native-wire-format-encoding`](native-wire-format-encoding.md) already measured OTLP/protobuf
  losing decisively on every axis against the native format for exactly this kind of hop.
- **An explicit `seq` field on every data frame.** Rejected: TCP's own ordering already makes it
  redundant, and leaving it off keeps the native-v1 payload itself unmodified by the transport
  layer — the same frame bytes work identically written to a file (a durable buffer) or a socket.
- **Building credit-based flow control (window > 1) now**, per `wire-protocol.md`'s original
  sketch. Deferred: it needs `logit-pipeline`'s `SinkQueue` to track several outstanding,
  unacknowledged batches instead of one, a real queue-shape change out of this plan's scope. The
  handshake already negotiates and records `window` so this is additive later, not a wire-format
  break.

## Consequences

- `crates/logit-proto/src/native/control.rs` (new): `Hello`/`HelloAck`/`Ack`/`Reject`, plus the
  `REJECT_*` reason codes. `FrameHeader::read` and `frame::MAX_SANE_UNCOMPRESSED_LEN` are now
  `pub`; `write_frame_with_flags`/`read_frame_with_header` are new entry points alongside the
  existing `write_frame`/`read_frame`. `CodecError::Truncated` distinguishes "need more bytes" from
  "corrupt" for a streaming reader.
- `crates/logit-inputs/src/logit.rs` / `crates/logit-outputs/src/logit.rs` (new): `LogitInput`/
  `LogitOutput`, both real, tested `ComponentKind` implementations now.
- `crates/logit-inputs/src/tls.rs` / `crates/logit-outputs/src/tls.rs` (new): the TLS builders
  `otlp_in`/`otlp_out` already had, extracted so `logit_in`/`logit_out` share them rather than
  duplicating `rustls` construction a third time.
- `docs/known-gaps.md`'s schema-drift entry ("the published schema advertises kinds the binary
  can't run") closes outright — `logit_in`/`logit_out` were the last two declared-and-unimplemented
  kinds. The credit-window, QUIC, and OTLP-passthrough-codec follow-ups this plan explicitly
  deferred are recorded there instead, alongside `otlp_in`'s shutdown gap this ADR names above and
  `logit_in`'s currently-fixed (not operator-tunable) 5s shutdown grace.
