---
created: 2026-10-01
updated: 2026-10-02
---

# Native hop identity and sequence: a per-store sender identity and sequence in the batch trailer, an `Ack` with no fields, and a high-water mark at `logit_in`

## Status
Accepted. Supersedes, in part, [ADR
`native-transport-handshake-and-ack`](native-transport-handshake-and-ack.md): "Sequence numbers
are implicit", the `Ack` entry of its control payload, its rejected alternative "An explicit
`seq` field on every data frame", and "Ack point", which now has a second case. It narrows [ADR
`delivery-semantics`](delivery-semantics.md) for the native hop: item 3's acknowledgment, item
7's "Outside the window, forward", and item 8's replayed set. Superseded in part on 2026-10-01 by
[ADR `native-hop-send-window`](native-hop-send-window.md): decision 4's one-frame-in-flight reasoning and its
"`window` stays 1" sentence, and the rejected alternative "`Ack` echoing the sequence".
Superseded in part on 2026-10-01 by [ADR
`native-hop-no-compatibility`](native-hop-no-compatibility.md): decision 1's "The unsequenced
rule", decision 2's "A v1 connection sends unsequenced frames", decision 3's "An old spool's
records replay unsequenced", step 1 of decision 5's algorithm, and the "The breaking change"
consequence. A frame or record without a complete pair is malformed, not unsequenced.
Superseded in part on 2026-10-02 by [ADR `native-hop-named-acks`](native-hop-named-acks.md):
decision 4, "`Ack` carries no fields" and the "nothing acknowledges a sequence" clause of "The
sequence is a deduplication identity, never a credit", the Context's "nothing acknowledges a
sequence", and the rejected alternative "`Ack` echoing the sequence". `Ack` names an identity and
a sequence and covers every frame of that identity at or below it, and grants no credit.

## Context

[ADR `delivery-semantics`](delivery-semantics.md), item 7, makes the `logit_out` to `logit_in` hop
effectively-once. It requires a sender identity, a per-batch sequence assigned when the batch
enters the sink's store and persisted by a disk spool, and a bounded window in which `logit_in`
recognizes a resend, acknowledges it, and doesn't forward it. It leaves the wire layout, the
window, and the spool record to this record.

As built, the wire numbers frames implicitly per connection: `logit_in` acknowledges the Nth data
frame as `Ack { seq: N }`, the count restarts on a reconnect, and `logit_in` forwards every frame
(`docs/known-gaps.md`, "The native hop has no sender identity and no deduplication").

One constraint shapes every choice below: delivery verification must not become flow control. The
sink sends a sequence, the receiver uses it only to recognize a resend, and nothing acknowledges a
sequence. Credit-based flow control stays separate work. [Superseded in part on 2026-10-02 by
[ADR `native-hop-named-acks`](native-hop-named-acks.md): `Ack` names the identity and sequence of
the frames handled, and still grants no credit.]

These facts about the code fix the design:

- **The frame header has no room.** The 24-byte header (`frame::HEADER_LEN`) has 2 reserved
  bytes, and its CRC-32C covers only the payload, so a field there is too narrow for a sequence
  and unprotected.
- **The v2 trailer is the batch-level extension point.** A `CODEC_NATIVE_V2` payload ends in a
  length-prefixed trailer of `tag(u8) + len(uvarint) + bytes` entries: tag 1 is `origin` and tag
  2 is `previous` ([ADR `batch-provenance-on-delivered`](batch-provenance-on-delivered.md)).
  `native::decode_batch_v2` skips an unknown tag and caps each field at
  `MAX_SANE_TRAILER_FIELD_BYTES` (4096). A new tag needs no codec byte.
- **The spool record can't widen, and doesn't need to.** A spool record is
  `[trace_id:16][span_id:8][frame]`, and `CONTEXT_LEN`'s doc forbids widening the prefix, so
  anything new rides inside the frame. `parse_record` decodes every record, and `logit_out`
  re-encodes the payload on every attempt (`LogitOutput::send`), so the concern behind the old
  rejected alternative, that one frame's bytes serve both a socket and a file, no longer binds.
- **The store's pushes are sequential, and it has one per-batch hook.** `SinkStore::push` takes
  `(Arc<EventBatch>, BatchContext)` from `drain_inbox` and, for a disk store after `drain_inbox`
  stops, from `run_output`'s shutdown sweep, which pushes the batch in hand and then the inbox's
  remainder.
  `Output::observe_batch` runs once per batch, never between retries. `BatchContext` is pinned at
  32 bytes and `Delivered` at 72 by `fanout.rs`'s size tests, and the delivery record's item 4
  keeps identity off in-process edges.
- **Randomness is per process.** `logit_core::random_id_bytes::<N>()` is a per-thread SplitMix64
  seeded from `RandomState`: 64 bits of entropy, independent per process.
- **A spool can lose acknowledged bytes in a power loss.** Segments are `fdatasync`ed only on
  rotation and at shutdown ([ADR `disk-backed-sink-buffer`](disk-backed-sink-buffer.md),
  "Durability"). A "next sequence" recovered from the records that survive could reuse a number
  the receiver already saw, and the receiver would then drop a new batch as a resend: a loss. The
  decision below never recovers a number.

## Decision

The sender identity and the sequence travel in the v2 batch trailer of every data frame, assigned
by the sink's store; `Ack` carries no fields; and each `logit_in` component keeps one high-water
mark per sender identity, in a table bounded by its connection cap.

### 1. Identity and sequence ride in the v2 batch trailer

- **Two new trailer tags.** Tag 3 is the sender identity, 16 bytes. Tag 4 is the sequence, a
  uvarint whose first value is 1. Both ride in every data frame's trailer, per frame.
- **The handshake carries no identity.** `Hello` and `HelloAck` are unchanged.
- **The unsequenced rule.** A frame or spool record without a complete, well-formed pair is
  unsequenced, and `logit_in` forwards it. That covers a v1 codec frame, a v2 frame written
  before this record's implementation, a frame with one of the two tags missing, an identity
  that isn't 16 bytes, and a sequence of 0 or with bytes left over after its uvarint.
  [Superseded on 2026-10-01 by [ADR
  `native-hop-no-compatibility`](native-hop-no-compatibility.md): every one of those is a
  malformed frame or a corrupt record. Nothing is unsequenced.]

### 2. A sink's store numbers what it holds

- **Numbers are assigned in push order.** Pushes are sequential, `drain_inbox`'s and then, for
  a disk store, the shutdown sweep's, and the store, not the pusher, assigns the next number
  when it encodes the batch (disk) or admits it (memory), advancing its counter before the
  push's first `.await`.
- **A gap is legal.** A number consumed by a push that fails or is cancelled (`frame_too_large`,
  `drop_newest`, `disk_full`, a cancelled push) is never sent, and the receiver ignores the gap.
- **The pair travels beside `BatchContext`, never on it.** The store item becomes
  `(Arc<EventBatch>, BatchContext, Option<SeqId>)`, with `SeqId { id: [u8; 16], seq: u64 }`
  `Copy`. `peek` returns it, and `Output::observe_batch` receives it with the context.
  `BatchContext` stays 32 bytes and `Delivered` 72.
- **Only `logit_out` encodes it.** On a connection that negotiated `CODEC_NATIVE_V2`, `logit_out`
  writes the pair into the trailer. A v1 connection sends unsequenced frames. Every other sink
  ignores the pair. [Superseded in part on 2026-10-01 by [ADR
  `native-hop-no-compatibility`](native-hop-no-compatibility.md): there is no v1 connection.]
- **A resend reuses the pair.** A resend inside one `deliver_with_retry` and a resend on a new
  connection carry the identity and number the batch was first given.
- **A relay numbers its own batches.** `logit_in` never puts a decoded pair on `send_relayed`.
  The pair names one hop, and a relay's own store assigns the next hop's.

### 3. A store takes a fresh identity every time it opens

- **Every open mints one.** A memory store and a disk store alike take
  `random_id_bytes::<16>()` when they open, and their sequence starts at 1.
- **A spool record keeps the pair it was written with.** The spool writes the pair in the
  record's v2 trailer, and `parse_record` returns it. A record read back after a crash goes out
  under its old identity and number, so a `logit_in` that saw it recognizes the replay. New
  batches start at 1 under the new identity. Replayed records precede new ones in the file, and
  each identity's numbers increase, so order within an identity holds.
- **An old spool's records replay unsequenced.** A record written before this record's
  implementation has no pair, and `logit_in` forwards it. [Superseded on 2026-10-01 by [ADR
  `native-hop-no-compatibility`](native-hop-no-compatibility.md): a record without a pair is
  corrupt and skipped.]
- **Restart without a spool.** The memory store opens with a new identity, and `logit_in` reads
  its first frame as a new sender, not a resend.
- **Restart with a spool.** Replayed records carry their recorded identities and numbers, and new
  batches carry the new identity from 1.
- **A spool whose records were all unlinked.** It takes a new identity like any other open.
  Nothing recovers a next sequence number: there's no identity file and no next-sequence file.
- **Entropy and cloning.** The identity is 64 bits of randomness in a 16-byte field. A running
  process that's cloned, such as by a VM snapshot or CRIU, shares its identity and sequence with
  its clone, and that's unsupported: `logit_in` reads the second copy's numbers as resends.

### 4. `Ack` carries no fields

- **`Ack` means the frame is handled.** With one frame in flight per connection the reply is
  unambiguous, so `Ack` names nothing. [Superseded in part on 2026-10-01 by [ADR
  `native-hop-send-window`](native-hop-send-window.md): several frames can be in flight, and
  `Ack` still names nothing, because `logit_in` answers one connection's frames in order, so the
  k-th `Ack` answers the k-th unanswered frame.] It means the frame was forwarded, or recognized as a
  resend and not forwarded. `logit_out` drops its `Ack.seq == conn.seq` check. [Superseded on
  2026-10-02 by [ADR `native-hop-named-acks`](native-hop-named-acks.md): `Ack { id, seq }` is
  cumulative per identity, and `logit_out` commits the frames it names.]
- **The acknowledgment point gains a second case.** The native-transport record's "Ack point"
  acknowledges a frame after `Fanout::send` returns, and the delivery record's item 3 says an
  acknowledgment means accepted into the pipeline. Both still hold for a frame above its mark.
  A frame at or below it is acknowledged on the mark alone, with no `send_relayed`: for a
  resend that's the earlier forward's acknowledgment repeated, and for a batch the sender
  dropped and a spool replayed (decision 5) it's an acknowledgment of a batch no consumer took,
  which the sender then commits. The sender gave that batch up before the replay, so nothing
  is lost that wasn't already counted.
- **The sequence is a deduplication identity, never a credit.** [Superseded in part on
  2026-10-01 by [ADR `native-hop-send-window`](native-hop-send-window.md): `window` is
  negotiated up to 1024, with no credit messages, and acks answer frames in frame order.] Nothing
  acknowledges a sequence, `window` stays 1, and a future credit-based flow-control record
  decides its own acknowledgment form. [Superseded in part on 2026-10-02 by [ADR
  `native-hop-named-acks`](native-hop-named-acks.md): `Ack { id, seq }` acknowledges the frames
  it names; the "never a credit" half stands.]
- **No version changes.** `PROTOCOL_VERSION` stays 1, the frame `VERSION` stays 1, and the codec
  byte stays `CODEC_NATIVE_V2`.
- **Breaking change.** `logit` is pre-release, so this is a breaking change to the native wire
  with no compatibility path.

### 5. `logit_in` keeps a high-water mark per sender identity

- **One table per `logit_in` component, not per process.** Two listeners can feed different
  graphs, so a frame one forwarded says nothing about the other.
- **The per-frame algorithm.** For each decoded data frame:
  1. If it's unsequenced, forward it as today. [Superseded on 2026-10-01 by [ADR
     `native-hop-no-compatibility`](native-hop-no-compatibility.md): a frame without a complete
     pair fails to decode and ends the connection as a protocol error; the algorithm is steps 2
     and 3.]
  2. If its sequence is at or below its identity's mark, count it, write `Ack`, and don't
     forward it.
  3. Otherwise, call `send_relayed`. If a consumer took the batch, raise the mark to the larger
     of itself and the frame's sequence, and write `Ack`. If none took it, write
     `Reject{GOING_AWAY}` and leave the mark unchanged ([ADR
     `delivery-semantics`](delivery-semantics.md), "Amendment: W3 decisions").

  An identity the table doesn't hold has a mark of 0. Gaps above the mark are ignored.
- **The window is every number at or below the mark.** Per sender, `logit_in` holds one 64-bit
  mark and no list of numbers seen.
- **A dropped batch that a spool replays stays dropped.** A batch the sender dropped (an
  exhausted `buffer.retry_budget`, `at_most_once`, or `drop_oldest` with a cursor that wasn't
  persisted) and a spool later replays is at or below the mark once a later batch of its
  identity was taken, and `logit_in` doesn't forward it.
  This narrows the delivery record's item 7, "a frame `logit_in` can't place is forwarded", and
  its item 8, "the set includes a batch it dropped", for the native hop.
- **Forwarded, duplicate accepted.** A receiver restart, an evicted sender, and a second
  `logit_in` behind a load balancer each forward the resend, per item 7.

### 6. The table is bounded from the connection cap

- **Capacity.** The table holds `max_connections + max_connections / 4` identities: 25% headroom
  over the connection cap for reconnects and restarts, 1280 at the default cap of 1024
  (`MAX_CONCURRENT_CONNECTIONS`).
- **Eviction.** When the table is full, the least recently seen identity is evicted.
- **No config field.** The bound follows the connection cap.

**Amendment (2026-10-01): the cap is configurable, the table still has no field.** The connection
cap is now each `logit_in`'s `max_connections:` field (default 1024). The table's bound still
follows it: `max_connections + max_connections / 4` identities, 1.25 times the configured cap, with
no floor or ceiling. A cap of N costs 1.25N entries of tens of bytes each.

### 7. No per-sender lock, and the parked-forward race is a residual

`logit_in` takes no lock per identity across a forward. The race that leaves open: a forward on
the old connection parks on a full inbox past the sender's ack timeout, the sender redials and
resends the same number, and the new connection reads a mark not yet advanced and forwards a
second copy. The target tolerates that duplicate (item 7 prefers the duplicate to a loss), and
the implementation adds a `docs/known-gaps.md` entry for it.

### 8. Counters

Names follow [`docs/design/internal-telemetry.md`](../design/internal-telemetry.md), "Naming":

| Name | Type | Counts |
|---|---|---|
| `logit.input.batches.resends` | count | frames at or below their identity's mark: acknowledged, not forwarded. These are the first replays `logit` can count ([ADR `delivery-semantics`](delivery-semantics.md), item 11). |
| `logit.input.senders` | gauge | identities in the component's table |
| `logit.input.senders.evicted` | count | identities evicted from a full table |

### Known limit: a phantom spool record can raise a mark

`frame::resync` can surface a phantom record embedded in a payload ([ADR
`disk-backed-sink-buffer`](disk-backed-sink-buffer.md), "Amendment: an in-cap corrupt length is
corruption, not a torn tail"). A phantom that carries the current identity and a high number
would raise the mark above the real records that follow, and `logit_in` would then drop them as
resends. It needs the spool's own bytes logged back through the same sink, and this record
accepts it.

## Alternatives considered

- **The identity in `Hello`, with the next sequence recovered from the spool.** The handshake
  would carry one identity per sender, and a spool would persist it and recover its next number
  from the records that survive. After a power loss that can reuse a number the receiver already
  saw, and the receiver drops the new batch as a resend: a silent loss. A fresh identity per
  store open, with each record carrying its own, never reuses a number.
- **The sequence in the frame header's reserved bytes.** Two bytes are too narrow for a sequence,
  and the header's CRC doesn't cover them, so a flipped bit would misname a batch unseen.
- **A control frame ahead of each data frame.** It doubles the writes per batch, and a spool
  record would need a separate carrier for the pair, since the control frame isn't part of the
  record.
- **A new codec byte (`CODEC_NATIVE_V3`).** Unnecessary: the v2 trailer skips unknown tags, so a
  new tag extends it without a codec, a negotiation change, or a dispatch arm in `parse_record`.
- **`Ack` echoing the sequence.** [Superseded in part on 2026-10-01 by [ADR
  `native-hop-send-window`](native-hop-send-window.md): several frames can be in flight, and the
  echo stays rejected because a serial receiver acknowledges in frame order.] That's sequence acknowledgment in all but name, the first step
  toward the flow control this record keeps out. With one frame in flight the echo adds nothing.
- **A config `max_senders`.** A knob with no driver: the connection cap already bounds how many
  senders can be live, and the headroom covers reconnects. [Amendment (2026-10-01): the cap is
  now the `max_connections:` field, and the table follows it, so a separate `max_senders` stays
  rejected.]
- **A per-sender lock held across the forward.** It closes the parked-forward race at the cost
  of a lock per frame and a wait during the race, to prevent a duplicate the target tolerates.
- **A receiver table persisted across a `logit_in` restart.** It adds a file, its `fsync`, and its
  corruption handling to `logit_in` to save a duplicate after a restart, which item 7 accepts.
- **A sequence window instead of a mark**, a bitmap of recently seen numbers per sender. It would
  tell a resend from a number below the mark that was never seen, but on this hop that number is
  a batch the sender already gave up on, and the mark treating it as handled is the rule in
  decision 5. A mark is one number per sender, with no window size to choose.

## Consequences

- **Code.** The trailer tags, `SeqId` through the store and `Output::observe_batch`, store
  numbering, the emptied `Ack`, the per-component table, the counters, the stale text to fix,
  and the allocation and size pins each change trips are listed once, in
  [`docs/plans/delivery-semantics.md`](../plans/delivery-semantics.md), W5, with that
  workstream's tests. A tripped pin is updated in the same commit as `docs/design/memory.md`.
- **The breaking change.** An older `logit_out` against a newer `logit_in` doesn't work, and no
  version bump marks it: it decodes the empty `Ack` as `seq` 0, which fails its equality check,
  so it reads every attempt as `Ambiguous`. The other direction works without deduplication: an
  older `logit_in` skips the two tags and writes `Ack { seq }`, which a newer `logit_out`
  ignores. [Superseded on 2026-10-01 by [ADR
  `native-hop-no-compatibility`](native-hop-no-compatibility.md): an `Ack` with a body is
  malformed, and neither direction across that boundary is a supported deployment.]
- **The residual race** stays documented in `docs/known-gaps.md` (decision 7).
- **Operator docs (the plan's W6).** `docs/design/wire-protocol.md` and `docs/deploying.md`
  describe the native hop once the implementation lands.
