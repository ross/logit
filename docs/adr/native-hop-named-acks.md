---
created: 2026-10-02
updated: 2026-10-02
---

# Native hop acks: a named cumulative `Ack`, coalesced at `logit_in`, and a resume mark in `HelloAck`

## Status
Accepted. Supersedes in part:

- [ADR `native-hop-identity-and-sequence`](native-hop-identity-and-sequence.md): decision 4,
  "`Ack` carries no fields" and the "nothing acknowledges a sequence" clause of "The sequence is
  a deduplication identity, never a credit", the Context's "nothing acknowledges a sequence", and
  the rejected alternative "`Ack` echoing the sequence".
- [ADR `native-hop-send-window`](native-hop-send-window.md): the Decision's "no sequence in
  `Ack`", decision 1's "Acks arrive in frame order" (the k-th `Ack` answers the k-th unanswered
  frame) and the "about 47 KB under TLS" arithmetic behind `RECEIVER_MAX_WINDOW`, decision 4's
  "`in_flight` is the loop's count" drift check, decision 5's `await_ack` ("`Ack` decrements
  `in_flight`"), and the rejected alternative "`Ack` carrying the sequence".
- [ADR `native-hop-no-compatibility`](native-hop-no-compatibility.md): decision 4's "`Ack` is the
  message byte alone".
- [ADR `native-transport-handshake-and-ack`](native-transport-handshake-and-ack.md): the `Ack`
  entry of "Control payload", `Hello`/`HelloAck`'s field lists, and the idle clock's "measured
  from the last `Ack` written".
- [ADR `idle-connection-timeout`](idle-connection-timeout.md): "`logit_in`: idle measured from
  the last `Ack` written". The clock runs from the last frame handled.

## Context

Every data frame on the native hop carries its sender identity and sequence, and `logit_in`
acknowledges a frame at or below its sender's high-water mark without forwarding it ([ADR
`native-hop-identity-and-sequence`](native-hop-identity-and-sequence.md)). A frame without that
pair is malformed ([ADR `native-hop-no-compatibility`](native-hop-no-compatibility.md)). The
acknowledgment, though, still names nothing: `Ack` is one byte, written and flushed once per
frame, and the sender matches acks to frames by count. Three costs follow:

- **One write and one flush per frame at `logit_in`.** `serve_frames` writes `Ack` after every
  frame and flushes it. On loopback the perf harness shows `native-relay` ack-bound
  (`docs/design/performance.md`, "Peak RSS: what is live data and what is jemalloc retention",
  the `logit_out` sink-queue-full row). The per-frame flush is the suspected cost; W4 of the plan
  measures it. Under TLS each flush is a record of its own.
- **A positional contract the sender has to defend.** The k-th `Ack` answers the k-th
  unacknowledged frame, so `logit_out` keeps `Conn.in_flight` as a count, and `submit` fails
  `Ambiguous` and drops the connection when the write loop's count disagrees with it ([ADR
  `native-hop-send-window`](native-hop-send-window.md), decision 4). A miscount anywhere would
  commit the wrong batch, and the only defense is the drift check.
- **A reconnect resends what the receiver already has.** After a fault mid-window, the write
  loop resubmits every outstanding batch from the head. `logit_in` recognizes each as a resend
  and acks it without forwarding, but the sender has paid the encode and the bytes for every one.
  On a WAN after a fault, that is a window of frames re-sent for nothing.

These facts about the code fix the design:

- **Frames of one identity arrive in increasing sequence.** A store numbers in push order and
  the write loop submits in store order, so on one connection a run of frames from one identity
  has increasing `seq`. Identities don't interleave arbitrarily: a spool replays its records in
  file order, where each earlier store open left one contiguous run, and new batches follow
  under the fresh identity. A resend after a fault repeats a prefix of the same order.
- **`logit_in` is serial per connection** and reads the next frame only after it has handled the
  last. It knows at every point which frames it has handled and not yet acknowledged.
- **The receiver's marks are the resume information.** `SenderTable` holds one 64-bit
  mark per identity, bounded from the connection cap. A frame at or below the mark is already
  handled, whatever connection it arrives on.
- **The sender knows which frames it will resend.** When a connection drops and the round is
  retried, `Conn.in_flight` names every frame the next connection's fill will resubmit, and the
  write loop resubmits them from the head in the same order. A round that isn't retried (an
  `Ambiguous` fault under `at_most_once`, or an exhausted budget) drops them instead, and a
  stale identity in the next `Hello` costs one lookup.
- **Control messages are strict** (`native-hop-no-compatibility`, decision 4): every field
  required once, an unknown tag malformed. A new field on `Hello`/`HelloAck` is a required field
  on both ends, and this is a pre-release breaking change like the two before it.

## Decision

`Ack` names an identity and a sequence and means every frame of that identity at or below the
sequence is handled; `logit_in` writes one `Ack` per run of handled frames rather than one per
frame; and the handshake carries the sender's identities out and the receiver's marks back, so a
reconnect commits what the receiver already holds without resending it.

### 1. `Ack { id, seq }` is cumulative per identity

- **Wire.** `Ack` has two required fields: tag 1 `id`, the 16-byte sender identity, and tag 2
  `seq`, a uvarint; a `seq` of 0 is `Malformed`, as it is in a frame's trailer. Message byte
  `MSG_ACK` is unchanged. Both fields are required once, and an unknown tag is `Malformed`, as
  for every control message.
- **Not a credit.** The ack names frames the receiver has handled. It grants no permission to
  send: the window is still fixed at the handshake, and nothing in an ack changes how many frames
  the sender may have in flight. The earlier records' "nothing acknowledges a sequence" was a
  guard against flow control arriving through the acknowledgment, and this record keeps that
  guard while naming the frame.
- **Meaning.** "Every data frame of identity `id` with a sequence at or below `seq` that this
  connection carried is handled: forwarded, or recognized as a resend and not forwarded." It
  says nothing about any other identity.
- **Identity order.** `logit_in` never lets an `Ack` for one identity cover a frame of another:
  before it handles a frame whose identity differs from the pending ack's, it writes the pending
  ack (decision 2). So on the wire, acks for one identity's run arrive before any frame of the
  next identity is acknowledged, and the sender can require that the front of its in-flight list
  carries the acked identity.
- **`seq` is a frame the connection carried.** `logit_in` sets `seq` to the sequence of the last
  frame it handled in the run, never the mark, so a resend run below the mark is acknowledged by
  its own numbers and the sender can match it by number.

### 2. `logit_in` coalesces acks

- **Pending ack.** After handling a frame (forwarded, or a resend not forwarded), `serve_frames`
  records `pending = (id, seq)` of that frame and a count of frames it covers, instead of writing.
- **Flush points.** It writes `Ack{pending}` and flushes, then clears `pending`, at the first of:
  1. the next frame's identity differs from `pending.id`, before that frame is handled;
  2. the next header read would block: `serve_frames` polls the stream once for header bytes
     into a fresh buffer and, if that poll is pending, flushes before waiting. A pending
     `poll_read` consumes nothing, plain or TLS, and a partial fill is carried into the header
     read that follows, so no byte is lost. A sender with nothing more to send gets its ack at
     once; a sender streaming faster than the receiver forwards gets one ack per burst;
  3. `pending` covers `ACK_COALESCE_MAX` frames, 32, so a sender with a large window commits
     and frees store space before its window drains;
  4. before any `Reject` (`GOING_AWAY` on shutdown, idle, or no consumer; `FRAME_TOO_LARGE`)
     and before the lingering close on every other exit from `serve_frames`, best effort.
- **The `GOING_AWAY` invariant holds.** With the pending ack flushed first, every frame still
  unanswered on a connection that read `Reject{GOING_AWAY}` was unforwarded, as [ADR
  `native-hop-send-window`](native-hop-send-window.md) decision 5 relies on.
- **A stalled flush changes no exit.** The flush before a `Reject` is attempted first; a write
  that stalls past `handshake_timeout` is counted `ack_write_stalled`, the `Reject` is skipped
  because the peer has stopped reading, and the connection ends as it would have (a shutdown or
  idle close is still a clean close, never an error).
- **The idle clock runs from the last frame handled.** An ack can now trail its frame, so
  `logit_in` measures idleness from the last frame it forwarded or recognized as a resend, not
  from the last ack it wrote. [ADR `idle-connection-timeout`](idle-connection-timeout.md) and
  [ADR `native-transport-handshake-and-ack`](native-transport-handshake-and-ack.md) said "the
  last `Ack` written"; for a connection acked per frame the two clocks were the same.
- **No timer.** Rule 2 gives a quiet sender its ack immediately and a busy one an ack per burst,
  and rule 3 bounds a burst, so no time-based flush is needed.
- **Counters.** `logit.input.acks` counts acks written; against `logit.proto.frames{direction="in"}`
  it shows the coalescing ratio.

### 3. `logit_out` commits by name

- **In-flight is a list, not a count.** `Conn.in_flight: VecDeque<InFlight { seq: SeqId, acked:
  bool }>`, pushed by `submit` in the order frames were written.
- **An `Ack` marks a prefix.** On `Ack { id, seq }`, `read_ack` walks from the front while the
  entry's identity is `id` and its sequence at or below `seq`, marking each `acked`. It requires
  that the front entry's identity is `id` and that an entry with sequence `seq` was marked; any
  other shape (an unknown identity at the front, a sequence not in flight) is a protocol error,
  `Ambiguous`, and drops the connection.
- **`await_ack` answers one head per call.** The `Output` trait keeps its shape: `await_ack`
  returns `Ok` once per acknowledged frame, in order. If the front entry is `acked`, it pops it
  and returns without touching the wire; otherwise it reads one control message and applies it,
  then pops the front if that marked it. One wire `Ack` thus satisfies several `await_ack`s.
- **The drift check goes.** `submit`'s `in_flight` argument and `LogitOutput.drifted` are
  removed: acks name frames, so a miscounted window can't commit the wrong batch. `Output::submit`
  loses the parameter.
- **`broken` is unchanged.** A connection that stopped taking frames is kept until its in-flight
  list is empty, so the acks it still owes are read.

### 4. The handshake carries identities out and marks back

- **`Hello.senders`**, tag 6, required: the distinct identities of the frames this connection
  will resubmit, up to `MAX_HELLO_SENDERS` (16), 16 bytes each, in in-flight order; empty when
  nothing is being resent. `logit_out` takes them from the in-flight list of the connection it
  last dropped, which a retried round resubmits from the head. More than 16 distinct identities
  in one window lists the first 16; the rest are resent and deduplicated as today.
- **`HelloAck.marks`**, tag 6, required: one entry per `senders` identity the receiver's table
  holds, 24 bytes each (the identity, then the mark as a big-endian u64), in the order asked;
  an identity the table doesn't hold is omitted and reads as mark 0. The lookup neither inserts
  nor evicts.
- **Both lists decode strictly.** A `senders` or `marks` field whose length isn't a whole number
  of entries, or that holds more than `MAX_HELLO_SENDERS` entries, is `Malformed`, under the same
  rule every control-message field follows.
- **Marks must answer the `Hello`.** A `HelloAck` whose marks name an identity the `Hello` didn't
  list, or name one identity twice, doesn't answer the `Hello` and fails the attempt `Permanent`,
  as a `HelloAck` naming an unoffered codec does. A conforming `logit_in` never sends one; the
  class says the peer isn't a `logit_in` this sink can talk to, which a retry won't change.
- **A frame at or below its mark is committed without a send.** The marks exist only once the
  new connection's handshake has run, and `submit` today encodes and size-gates a batch before
  it connects, so an oversized batch never connects. With identities to resend and no
  connection, `submit` connects and handshakes first, then checks the head against the marks,
  then encodes; a covered frame is pushed to the in-flight list as `acked` and `submit` returns
  `Ok` with nothing encoded or written, so `await_ack` commits it in order. With nothing to
  resend the order stays encode, size gate, connect. The marks are read at the handshake and a
  mark never decreases, so a covered frame is handled whatever happens afterwards.
- **Counter.** `logit.output.batches.resumed` counts frames committed from a mark.

### 5. No version bump, no compatibility

`PROTOCOL_VERSION`, the frame `VERSION`, and the codec bytes don't change. A peer from before
this record fails the handshake: its `Hello` lacks tag 6 and is `Malformed`, as decision 4 of
[ADR `native-hop-no-compatibility`](native-hop-no-compatibility.md) requires. Nothing in the
code accounts for it.

### What stays

- **Forwarding and the mark are unchanged.** A frame at or below its mark is still acknowledged
  without forwarding; a frame above it is still forwarded before the mark is raised; a frame no
  consumer took still gets `Reject{GOING_AWAY}` and leaves the mark alone.
- **The window is still fixed at the handshake.** No credits, no window change on the wire.
- **`logit_in` stays serial per connection.** Coalescing is about when the ack is written, not
  about handling frames concurrently.
- **The store's `peek_at`/`commit` contract.** Acknowledged frames are a prefix of the store, so
  `await_ack` returning `Ok` per head and the loop committing per head is unchanged.

## Alternatives considered

- **A positional cumulative ack (`Ack { count }`).** Fewer bytes, but it keeps the count
  contract and the drift check, and it can't express a resume mark. Naming the frame costs 17
  more bytes per ack and removes a class of bug.
- **One ack per identity per window, with no count cap.** Liveness holds without the cap (a
  sender blocked on a full window stops sending, the read blocks, the ack flushes), but a sender
  with a 1024-frame window and a memory store would hold 1024 reserved batches before its first
  commit. The cap bounds the sender's wait in frames, not time.
- **A time-based flush.** A timer adds a wakeup per connection and a tuning knob, for a case the
  read-would-block rule already covers: a sender that stops sending gets its ack at once.
- **A separate `Resume` message after the handshake.** It adds a message type and a round trip
  for information both sides have at `Hello`/`HelloAck`. Two fields on the existing messages
  cost nothing extra.
- **The identity in `Hello` alone, with marks resolved per frame.** The receiver already
  resolves per frame: that is the dedup rule. What the sender needs is the mark before it encodes
  and sends, which only the handshake can give it.
- **Out-of-order acknowledgment.** Still rejected ([ADR `native-hop-send-window`](native-hop-send-window.md)):
  `logit_in` forwards in order, so nothing is acknowledged out of order to gain from, and the
  spool would need per-record ack state.
- **Keeping the drift check beside named acks.** It would defend a contract that no longer
  exists.

## Consequences

- **Code.** `control.rs` gains `Ack`'s two fields, `Hello.senders`, and `HelloAck.marks`;
  `serve_frames` gains the pending ack and its flush points; `logit_out` gains the in-flight
  list, prefix marking, and the mark check in `submit`; `Output::submit` loses `in_flight`;
  `SenderTable` gains a read-only mark lookup. The workstreams, files, tests, and pins are listed
  once, in [`docs/plans/native-hop-named-acks.md`](../plans/native-hop-named-acks.md).
- **Telemetry.** `logit.input.acks` and `logit.output.batches.resumed`, recorded in
  `docs/design/internal-telemetry.md`. `logit.output.ack.duration` records one sample per wire
  read, not per `await_ack` that returned from the list.
- **Operator docs.** `docs/design/wire-protocol.md`'s connection protocol and
  `docs/deploying.md`'s forwarding section describe the named ack, coalescing, and the resume.
- **The receiver's unread-ack bound grows.** A named ack is 46 to 55 bytes on the wire (a 24-byte
  header, the message byte, the 18-byte identity field, and 3 to 12 bytes of sequence), about 70
  to 80 bytes under TLS, against about 46 for the empty ack. With one ack per frame, the worst
  case the send-window record sized `RECEIVER_MAX_WINDOW` by (1024 unread acks a peer leaves in
  the listener's send buffer) grows from about 47 KB to about 80 KB, still under the default
  `tcp_rmem`. Coalescing lowers the common case well below either. The constant stays 1024; its
  doc and the `ack_write_stalled` reasoning in `docs/known-gaps.md` carry the new arithmetic.
- **Known gaps.** A new residual: `Hello.senders` is capped at 16 identities, so a window
  spanning more than 16 store opens resends the rest.
- **Measurement.** The perf VM confirmed the per-frame flush was the ack-bound cost: at
  `window: 32`, `native-relay` costs 0.858 µs/event after against 0.958 before, and at `window: 1`
  it reads +2.3% (1.150 → 1.177), under the 5% gate. Under a 10 ms round trip, coalescing costs
  about 2 points of the ceiling (97.2% against 99.5%), and `ACK_COALESCE_MAX` isn't the cause
  (`docs/design/performance.md` §1; the plan's "Findings" section).
