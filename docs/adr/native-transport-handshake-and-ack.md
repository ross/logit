---
created: 2026-09-09
updated: 2026-10-05
---

# Native transport: handshake, implicit sequencing, and per-batch acknowledgement

## Status
Accepted. Superseded in part on 2026-10-01 by
[ADR `native-hop-identity-and-sequence`](native-hop-identity-and-sequence.md): "Sequence numbers
are implicit", the `Ack` entry of "Control payload", the rejected alternative "An explicit
`seq` field on every data frame", and "Ack point", which gains a second case. Superseded in
part on 2026-10-01 by [ADR `native-hop-send-window`](native-hop-send-window.md): "In-flight: one frame per
connection, in this plan", the deferred alternative "Building credit-based flow control
(window > 1) now", and the sentence of "Sequence numbers are implicit" that expects a future
window to use cumulative acks (the acks stay empty and in frame order). Superseded in part on
2026-10-01 by [ADR `native-hop-no-compatibility`](native-hop-no-compatibility.md): "Control
payload"'s skip-unknown forward compatibility. A control message's fields are each required, and
an unknown tag is malformed. Superseded in part on 2026-10-02 by [ADR
`native-hop-named-acks`](native-hop-named-acks.md): the `Ack` entry of "Control payload" and the
`Hello`/`HelloAck` field lists. `Ack` is `{ id, seq }`, `Hello` gains `senders`, and `HelloAck`
gains `marks`. Superseded in part on 2026-10-04 by [ADR `native-hop-ack-status`](native-hop-ack-status.md): the `Ack` entry of
"Control payload" (`Ack` gains a required status) and the 2026-09-25 amendment's answer to a batch
past its decode budget, which is now a rejected `Ack` naming the frame, with the connection kept.

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
compatibility as `native::record` [superseded on 2026-10-01 by [ADR
`native-hop-no-compatibility`](native-hop-no-compatibility.md): every field required, unknown
tags malformed]: `Hello { version, codecs, compressions, max_frame_bytes, window
}`, `HelloAck { version, codec, compression, max_frame_bytes, window }`, `Ack { seq }`, `Reject {
code, message }`.
[Superseded in part on 2026-10-01 by [ADR
`native-hop-identity-and-sequence`](native-hop-identity-and-sequence.md): `Ack` carries no
fields.]

**Sequence numbers are implicit.** [Superseded in part on 2026-10-01 by [ADR
`native-hop-identity-and-sequence`](native-hop-identity-and-sequence.md): a sender identity and a
sequence ride in every data frame's v2 trailer, assigned by the sink's store, and `Ack` carries no
fields. Since 2026-10-05 ([ADR `native-hop-ack-status`](native-hop-ack-status.md)) the pair leads the hop payload instead.] TCP is ordered, so the Nth data frame on a connection is always
seq N; `Ack.seq` is the cumulative count of data frames the receiver has forwarded. No seq field on
the data frame itself, so the native-v1 payload is untouched, and a future credit window > 1 can
use cumulative acks unchanged. [Superseded in part on 2026-10-01 by [ADR
`native-hop-send-window`](native-hop-send-window.md): a window above 1 keeps `Ack` empty, with
acks arriving in frame order, not cumulative.]

**Ack point: after `Fanout::send` returns**, not after the frame is merely decoded. [Superseded
in part on 2026-10-01 by [ADR
`native-hop-identity-and-sequence`](native-hop-identity-and-sequence.md): a frame at or below
its sender's high-water mark is acknowledged on the mark alone, with no `Fanout::send`. A frame
above it is acknowledged as written here.] A stalled downstream delays the ack, which stalls the
sender's own delivery attempt — that *is* this protocol's backpressure. `logit_in` needs no
receive-side queue on top of this; the ack itself is the queue depth of one.

**Handshake.** The connecting side (`logit_out`) sends `Hello` first; the listener replies
`HelloAck` (codec/compression = the intersection, its own `max_frame_bytes`, its own `window`) or
`Reject` and closes. A version mismatch or no shared codec is `Reject`, not a silently-corrupted
stream.

**In-flight: one frame per connection, in this plan.** [Superseded in part on 2026-10-01 by [ADR
`native-hop-send-window`](native-hop-send-window.md): `logit_out` keeps up to the negotiated
`window` of frames in flight, the sink's store reserves a prefix of items instead of the head,
and acks answer frames in frame order, with no credit messages.] `window` is negotiated and recorded in both
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
frame has already left, except `REJECT_GOING_AWAY`, which stays `Fault::Clean` there too.
`serve_connection`'s per-frame `select!` races shutdown against the frame *header* read, so it can
take the shutdown arm with a whole frame already readable, and `GOING_AWAY` then arrives in place
of that frame's `Ack`. That frame was never forwarded: see the 2026-09-25 amendment "`GOING_AWAY`
is never written for a forwarded frame".

**Shutdown: `logit_in` overrides `Input::run_until_shutdown`.** Every spawned connection task holds
its own `Fanout` clone (the cancel-by-drop shutdown mechanism [ADR
`service-lifecycle-and-output-retry`](service-lifecycle-and-output-retry.md) depends on requires
nothing to outlive the listener's own future), so each connection races its next-frame read against
a cloned shutdown signal and, once idle at a frame boundary, sends `Reject{GOING_AWAY}` and closes.
An already-in-flight frame is allowed to finish. `otlp_in` and the other HTTP listeners have no
such override yet (`docs/known-gaps/native-hop.md`); [ADR
`idle-connection-timeout`](idle-connection-timeout.md)'s "Amendment: shutdown is the second
trigger of the close sequence" decides theirs.

## Alternatives considered

- **Reuse gRPC-over-hyper** (`otlp_out`'s hand-rolled transport, [ADR
  `hand-rolled-grpc-over-hyper`](hand-rolled-grpc-over-hyper.md)) for `logit`-to-`logit` traffic
  too, rather than a bespoke framing. Rejected: the entire point of the native path is no
  per-request HTTP/gRPC framing overhead — the bake-off in [ADR
  `native-wire-format-encoding`](native-wire-format-encoding.md) already measured OTLP/protobuf
  losing decisively on every axis against the native format for exactly this kind of hop.
- **An explicit `seq` field on every data frame.** [Superseded in part on 2026-10-01 by [ADR
  `native-hop-identity-and-sequence`](native-hop-identity-and-sequence.md): the sequence now
  rides in the v2 trailer (ahead of the batch since 2026-10-05, [ADR `native-hop-ack-status`](native-hop-ack-status.md)), and `logit_out`
  re-encodes the payload on every attempt, so the socket-and-file argument no longer binds.] Rejected: TCP's own ordering already makes it
  redundant, and leaving it off keeps the native-v1 payload itself unmodified by the transport
  layer — the same frame bytes work identically written to a file (a durable buffer) or a socket.
- **Building credit-based flow control (window > 1) now**, per `wire-protocol.md`'s original
  sketch. [Superseded in part on 2026-10-01 by [ADR
  `native-hop-send-window`](native-hop-send-window.md): a fixed window negotiated at the
  handshake is built, with no credit messages, over a store that reserves a prefix of items.]
  Deferred: it needs `logit-pipeline`'s `SinkQueue` to track several outstanding,
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
- `crates/logit-inputs/src/tls.rs` / `crates/logit-outputs/src/tls.rs` (new; both builders now live in
  `logit_pipeline::tls`, per [ADR `tls-certificate-reload`](tls-certificate-reload.md)): the TLS builders
  `otlp_in`/`otlp_out` already had, extracted so `logit_in`/`logit_out` share them rather than
  duplicating `rustls` construction a third time.
- `docs/known-gaps/runtime.md`'s schema-drift entry ("the published schema advertises kinds the
  binary can't run") closes outright — `logit_in`/`logit_out` were the last two
  declared-and-unimplemented kinds. The credit-window, QUIC, and OTLP-passthrough-codec follow-ups
  this plan explicitly deferred are recorded there instead, alongside `otlp_in`'s shutdown gap this
  ADR names above and `logit_in`'s currently-fixed (not operator-tunable) 5s shutdown grace.
- **Amendment (2026-09-13):** the pre-`Hello` timeout is operator-tunable now. `LogitIn` carries a
  `handshake_timeout: Duration` field (default 5s, still applied *per* pre-`Hello` phase -- the TLS
  accept, then the `Hello` read -- rather than as one shared deadline), set through
  `LogitInput::with_handshake_timeout`, which is `pub` rather than `#[cfg(test)]`. `syslog_in` and
  `otlp_in` gained the identically-named field at the same time, so one number and one field name
  now cover every ingress listener kind (`syslog_in`, `logit_in`, `otlp_in`) -- not every TCP
  listener in the tree: `prometheus_out`'s exposition server and the `admin:` endpoint have their
  own, separately-decided budgets; `otlp_in`'s accept loop also picked up the
  `tokio::time::timeout` around its TLS accept that this ADR's own "Connection limit" section
  describes here, closing `docs/known-gaps/intake.md`'s "`otlp_in`'s TLS accept has no timeout" row.
  Graph rule 45 keeps the value non-zero. The *shutdown grace* named just above is a different
  knob and stays fixed at 5s -- still open, still tracked in `docs/known-gaps/native-hop.md`.

## Amendment: `GOING_AWAY` is now also the idle-close signal, and `logit_out` probes for it (2026-09-14)

`LogitIn` gained an opt-in `idle_timeout:` field ([ADR `idle-connection-timeout`](idle-connection-timeout.md)):
a handshaken connection that stays quiet longer than the configured value is closed the same way
this ADR's own shutdown path already closes one -- `Reject{GOING_AWAY, "idle for <dur>"}` written
first, then the connection drops -- so a `logit_out` peer needs no new case to tell an idle close
from an ordinary shutdown; both arrive as the identical control message. The clock is measured from
the last `Ack` written (or the handshake, on a connection that has sent nothing yet), never from
the last frame read [superseded on 2026-10-02 by [ADR
`native-hop-named-acks`](native-hop-named-acks.md), decision 2: the clock runs from the last
frame handled, since a coalesced ack can trail its frame]: a peer waiting on an `Ack` this listener is deliberately delaying for a slow
downstream is, by this ADR's own ack-as-backpressure design, not idle, so time blocked in
`Fanout::send` never counts against it. A frame body gets the same bound per `read` rather than in
total.

This closes a real loss window on the client side. `logit_out` pools one connection per remote and
reuses it across batches; before this amendment, a peer that idle-timed-out and closed some time
ago left a stale pooled connection that the next `send` would write into, landing exactly the
`Fault::Ambiguous` case this ADR's own "reachable, not just theoretical" paragraph above already
names for the shutdown race. `logit_out` (and every other pooled TCP sink -- `syslog_out`,
`statsd_out`, `graphite_out`) now probes a *reused* pooled connection once before its first write
of a send attempt: one non-cancellable `poll_read`, never a `tokio::time::timeout(read)` that could
cancel mid-TLS-record and discard bytes already received. An immediate EOF or unsolicited bytes --
on this protocol, that is exactly `Reject{GOING_AWAY}` arriving unprompted -- drops the pooled
connection and dials a fresh one before anything is written, the ordinary `Clean`/reconnect path
rather than a lost or ambiguous batch. The residual case is unchanged: a FIN racing the probe
itself, the peer closing *while* this sink is writing, is still today's `Fault::Ambiguous`.

## Amendment: a single-copy body read and bounded control writes (2026-09-25)

[ADR `untrusted-input-bounds`](untrusted-input-bounds.md) changes how `logit_in` reads and writes,
without changing a byte on the wire:

- **Single-copy body read.** `read_frame_body` still sizes its buffer from the header's declared
  `compressed_len`, but it reads the body into the frame's final buffer directly instead of into a
  separate `Vec` that is then copied next to the header. The body is copied once, not twice. The
  per-read stall bound and every `FrameReadError` are unchanged.
- **Bounded control writes.** `HelloAck`, the per-frame `Ack`, the handshake's two `Reject`s, the
  past-the-cap `Reject`, and `GOING_AWAY` are each written under `handshake_timeout`. A wedged
  peer, such as a stopped process or one whose receive buffer is full, can no longer hold a
  connection, and its permit, in a blocked write. A timed-out past-the-cap `Reject` is an error;
  the others end the connection and are counted.

## Amendment: `GOING_AWAY` is never written for a forwarded frame, and `compressed_len` has its own bound (2026-09-25)

**The invariant.** `logit_in` writes every `Reject`, `GOING_AWAY` included, before the frame it
answers reaches `Fanout::send_relayed`: the past-the-cap `Reject`, the handshake's two, the
loop-top and `select!` shutdown arms, an idle close, and `FRAME_TOO_LARGE`. After `send_relayed`,
the only write is that frame's `Ack`. So `GOING_AWAY` in place of an `Ack` means the batch never
landed. The Decision section above said such a batch "may or may not have been forwarded"; that was
wrong. `logit_out` now classifies `REJECT_GOING_AWAY` after a data frame as `Fault::Clean`, which
[ADR `service-lifecycle-and-output-retry`](service-lifecycle-and-output-retry.md)'s `is_retryable`
retries at every delivery posture. Under the old `Fault::Ambiguous`, the default at-most-once
posture dropped the batch: a test that raced a shutdown against each of 300 sends lost one
batch this way. `a_frame_answered_with_going_away_is_never_forwarded` in
`crates/logit-inputs/src/logit.rs` pins the invariant; a change that writes `GOING_AWAY` after
`send_relayed` must move `logit_out` back to `Fault::Ambiguous`.

`Fault::Ambiguous` stays for an EOF, a reset, or an ack timeout after the frame left. A full
downstream inbox can park `send_relayed` past the sender's ack timeout, and the batch is then
forwarded without an `Ack`. At most once delivers it once and at least once duplicates it, which
is the posture contract.

**A compressed bound.** `logit_in` used to bound `compressed_len` by `max_frame_bytes`, the same
number as `uncompressed_len`, while `logit_out` bounded only the uncompressed payload. An
incompressible payload within about 0.4% of the cap grows past it under lz4, so `logit_in`
refused a frame `logit_out` considered in bounds. It also closed without a `Reject`, so the sender
saw an EOF, classified it `Fault::Ambiguous`, and dropped the batch at the default posture. Now:

- `frame::compressed_bound(n)` is lz4's worst case, `n + n / 255 + 16`, the formula
  `MAX_SANE_COMPRESSED_LEN` already used. `logit_in` bounds `compressed_len` by
  `compressed_bound(max_frame_bytes)`.
- `logit_in` answers an over-bound header with `Reject{FRAME_TOO_LARGE}` before closing, so the
  sender sees a permanent refusal instead of an EOF. A batch that decodes past its per-frame decode
  budget ([ADR `untrusted-input-bounds`](untrusted-input-bounds.md)) gets the same answer: it
  would fail the same way on every resend, and at-least-once would otherwise retry it forever. It
  is counted once, as `logit.proto.errors{reason="decode_budget"}`. [Superseded in part on
  2026-10-04 by [ADR `native-hop-ack-status`](native-hop-ack-status.md): a batch past its decode budget, and a header past
  `max_frame_bytes` within the compressed bound, are answered by a rejected `Ack` naming the frame,
  and the connection goes on. A header past the compressed bound still gets `FRAME_TOO_LARGE`.]
- `logit_out` checks its compressed frame against the same bound before sending, and drops a batch
  over it as `Fault::Permanent` with nothing written.

**Write-stall accounting.** The bounded writes of the amendment above are counted under
`logit.proto.errors{reason}`: `ack_write_stalled` for an `Ack` (the connection ends as an error),
and `reject_write_stalled` for any `Reject` (the write is abandoned and the connection closes).

## Amendment: both sides flush, a write fault is `Clean`, and a close is clean under TLS (2026-09-29)

[ADR `sink-send-path-and-attempt-accounting`](sink-send-path-and-attempt-accounting.md), decisions
4, 6, 7, 11, and 12, records these changes. None changes a byte on the wire.

- **Flushes.** `logit_out` flushes after the `Hello` write and after the frame write, before it
  waits for a reply, and `logit_in` flushes every control write inside its `handshake_timeout`
  bound. Under TLS a write can return with ciphertext still queued, and a reply read that
  processes its records cleanly doesn't send it, so an unflushed frame ended in an `Ambiguous` ack
  timeout, and under at-most-once a dropped batch the peer never received.
- **Write faults.** Every `logit_out` failure before the frame is completely written and flushed
  is `Fault::Clean`, with the `io::Error` kept: `logit_in` holds a batch only once it has the
  whole frame and its CRC checks, so bytes of the frame reaching the wire don't mean the peer
  holds the batch. The ack wait is the one `Ambiguous` window. The 2026-09-14 amendment's
  residual, a peer closing while this sink writes, is now `Clean` when the write or flush fails,
  and still `Ambiguous` when the write completes and the ack read then meets the close.
- **`HelloAck` validation.** A `HelloAck` with another `version`, or a `codec` or `compression`
  the `Hello` didn't offer, fails the handshake `Fault::Permanent`, as `REJECT_VERSION_MISMATCH`
  and `REJECT_NO_COMMON_CODEC` do. An unknown compression byte used to fall back to none.
- **Close.** `logit_out`'s `Output::flush` shuts its pooled connection down, which sends
  `close_notify` under TLS. `logit_in` reads `UnexpectedEof` at a frame boundary, a TLS peer gone
  without `close_notify`, as a close, not an error; it logged every TLS disconnect as
  `connection_error` before. A close part-way through a header counts
  `logit.proto.errors{reason="truncated_header"}`.
- **Control frames.** `logit_out` bounds a control frame at `control::MAX_CONTROL_MESSAGE_BYTES`
  (4096 bytes), not the 64 MiB data-frame cap, and `logit_in` bounds a `Hello` the same way.
- **`requests`.** `logit.output.requests` counts every returned attempt, including connect,
  handshake, and too-large returns, tagged `class=ok|clean|ambiguous|permanent`.

## Amendment: the native hop's target is effectively-once (2026-09-29)

[ADR `delivery-semantics`](delivery-semantics.md), item 7, sets requirements this record's wire
doesn't meet: a sender identity that outlives a connection, a sequence assigned when a batch
enters the sink's store and persisted by a disk spool, and a bounded window in which `logit_in`
recognizes a resend and doesn't forward it.

"Sequence numbers are implicit" and the rejected alternative "An explicit `seq` field on every
data frame" describe the wire as built. A follow-up record decides the new layout and supersedes
both: [ADR `native-hop-identity-and-sequence`](native-hop-identity-and-sequence.md). Until its
implementation lands (`docs/plans/delivery-semantics.md`, W5), `Ack.seq` counts frames on one
connection and restarts on a reconnect, and `logit_in` forwards a resend.

"Ack point" keeps its rule for a frame above its sender's mark, and that record's item 3 adds
that `logit_in` doesn't acknowledge a batch no consumer took. The follow-up record adds the
case of a frame at or below the mark, acknowledged with no forward.

## Amendment: `GOING_AWAY` also answers a frame no consumer took (2026-09-30)

"Ack point" holds that a frame is acknowledged once the batch is in every open downstream inbox.
`Fanout::send_relayed` now reports whether any consumer took the batch, and a batch none took is
not acknowledged, except a frame at or below its sender's mark, which is acknowledged with no
forward ([ADR `native-hop-identity-and-sequence`](native-hop-identity-and-sequence.md)):
`logit_in` writes `Reject{GOING_AWAY, "no consumer took the batch"}` and closes the connection,
before the sequence advances. `GOING_AWAY` has three causes: shutdown, an idle
close, and no consumer taking the frame. The frame wasn't forwarded, so `logit_out` classifies it
`Clean`, redials, and resends, and the sender and receiver sequences stay aligned. The invariant
in the 2026-09-25 amendment, that `GOING_AWAY` is written only for a frame that wasn't forwarded,
holds. See [`delivery-semantics.md`](delivery-semantics.md), item 3, and its W3 amendment.

## Amendment: the transport is a seam, and the gRPC rejection rests on a different reason (2026-10-05)

The rejected alternative "Reuse gRPC-over-hyper" gives the wrong reason. It cites the bake-off's
framing overhead, but [ADR `native-wire-format-encoding`](native-wire-format-encoding.md) measured
protobuf as the *payload encoding*, not HTTP/2 as the *transport*. A native hop frame carried as
one message on a long-lived HTTP/2 stream costs a 9-byte `DATA` header per 16 KiB (HTTP/2's
default frame size) and a 5-byte gRPC message prefix per batch, with HPACK headers once per
stream. Against a multi-kilobyte batch behind a 24-byte native header, that cost is noise. The
rejection stands, on these grounds instead:

- **The hard parts are the hop's own semantics, and HTTP/2 removes none of them.** Sender
  identity and sequence, the named cumulative `Ack`, the receiver's high-water marks and resend
  dedup, the reconnect resume from `HelloAck.marks`, and a spool replay are application state
  either way. HTTP/2's flow control is per-stream bytes, not application frames awaiting an
  acknowledgment, so the send window stays too. What a gRPC stream would replace is the framing,
  the version negotiation (ALPN and headers), keepalive, graceful close (`GOAWAY` for
  `Reject{GOING_AWAY}`), the frame-size settings, and the TLS plumbing: a minority of this ADR's
  surface, though several of its amendments live there.
- **A long-lived stream gets little from L7 infrastructure.** A stream pins to one backend, so a
  load balancer can't balance it, and a second `logit_in` behind one holds no mark for the sender
  either way (`docs/known-gaps/native-hop.md`). Cloud load balancers close an idle stream on their
  own timer, often 60 s, which this ADR's idle-close design would have to track.
- **The gRPC in tree is unary only** ([ADR `hand-rolled-grpc-over-hyper`](hand-rolled-grpc-over-hyper.md)).
  A bidirectional stream is new transport code, not a reuse.

What the decision costs, stated so it isn't rediscovered: the hop crosses L4 infrastructure
(TCP proxies, network load balancers, and stateful firewalls; `logit_in` takes no
`proxy_protocol:`, so a TCP proxy's origin is lost) and not L7 (an HTTP ingress, a service mesh
in HTTP mode, an application load balancer, an HTTP `CONNECT` egress proxy, or TLS termination
that routes on HTTP). A deployment that can only open an HTTP path between two `logit` processes
has no native hop today; `otlp_out` to `otlp_in` is the fallback, at OTLP's fidelity and encode
cost. The industry splits on this: Vector's native hop is gRPC; Fluentd's forward protocol and
Kafka's are bespoke framing over TCP.

**The transport is a seam, not the decision.** The frame format is transport-agnostic (the same
`CODEC_HOP_BATCH` frame goes to a socket and inside a `buffer.disk:` spool record, each encoded
under its own compression), and the session protocol above is defined over an ordered, reliable
byte stream, so it carries unchanged over any transport that provides one. The remedy for the L7
limitation is a second transport for the same frames and the same control messages, selected per
component, not a second protocol. Two candidates, neither designed here:

- **gRPC**, for L7 infrastructure: the hop frames as messages on one bidirectional stream, or one
  `POST` per frame with the `Ack` in the response. The second shape gives a proxy per-request
  visibility but reorders a window of concurrent requests, which the cumulative `Ack` assumes it
  won't see. Choosing between them is the first question of that work.
- **QUIC**, for a lossy or migrating WAN hop: the frames on one `quinn` stream, no HTTP/3 needed.
  The hop is one ordered sequence with cumulative acks, so QUIC's independent streams buy
  nothing here. Its wins are 0-RTT reconnect, connection migration, and loss recovery. A raw QUIC
  stream is as opaque to L7 infrastructure as TCP is.

`docs/known-gaps/native-hop.md`'s transport entry tracks both. Neither is built speculatively;
the trigger is a deployment that needs one.
