---
created: 2026-10-04
updated: 2026-10-05
---

# Native hop ack status: the sender pair leads the hop payload, and a rejected `Ack` settles one frame by name

## Status
Accepted. Supersedes in part:

- [ADR `native-hop-named-acks`](native-hop-named-acks.md): decision 1's `Ack` wire (two fields)
  and "Meaning" (every `Ack` covers a run), decision 2's flush point 4 (`FRAME_TOO_LARGE` for a
  batch past its decode budget), decision 3's "any other shape is a protocol error", and "What
  stays"'s "a frame above its mark is forwarded before the mark is raised" (a refused frame
  raises it unforwarded).
- [ADR `native-hop-identity-and-sequence`](native-hop-identity-and-sequence.md): decision 1's
  "ride in the v2 batch trailer" (they lead the payload), decision 3's "in the record's v2
  trailer", and decision 5's per-frame algorithm, which gains a refused case.
- [ADR `native-hop-no-compatibility`](native-hop-no-compatibility.md): decision 1's
  `CODEC_HOP_BATCH` layout, and decision 2's trailer tags 3 and 4 and "a frame whose decode fails
  is a protocol error".
- [ADR `native-hop-send-window`](native-hop-send-window.md): decision 4's "every `Err` from
  `await_ack` means the sink dropped its connection" and decision 5's "Every `Err` drops the
  connection": a rejected `Ack` keeps it.
- [ADR `delivery-semantics`](delivery-semantics.md): item 3's exception for the native hop. A
  rejected `Ack` is a second case where `logit_in` answers a frame no consumer took and raises
  its mark with no forward; it reports a drop, not an acknowledgment of delivery.
- [ADR `native-transport-handshake-and-ack`](native-transport-handshake-and-ack.md): the
  2026-09-25 amendment's "A batch that decodes past its per-frame decode budget gets the same
  answer", `Reject{FRAME_TOO_LARGE}`, and the `Ack` entry of "Control payload".

## Context

[ADR `sink-fault-classes`](sink-fault-classes.md) decided that `logit_in` answers a frame it can't
take with a status on the acknowledgment, so `logit_out` drops that batch as `Rejected` and keeps
going. Today the hop can't say that:

- **A refusal ends the connection.** A batch that decodes past `logit_in`'s per-frame decode
  budget is answered `Reject{FRAME_TOO_LARGE}` and the connection closes. A body that doesn't
  decode closes it with no answer. Every frame behind the refused one in the sender's window is
  lost with the connection: `logit_out` reads an `Ambiguous` close for them, redials, and resends
  them all.
- **A refusal names nothing.** `Reject` carries a code and a message, no sequence. With a window
  of frames in flight, the sender attributes it to the head by position alone.
- **The receiver can't name a frame whose body it can't decode.** The sender pair rides in the
  hop trailer, after the batch. `decode_hop_batch` decodes the batch first, so a body past its
  budget fails before the pair is read.
- **The decode budget is the case that happens.** `HelloAck` tells a sender `max_frame_bytes`,
  never the budget, so a stock `logit_out` batch between roughly 10% and 100% of the cap can
  decode past it (`docs/known-gaps/native-hop.md`, "The native decode budget bounds only what
  arrives over `logit_in`"). `logit_out` checks the cap itself and never sends a frame past it, so
  an oversize header comes from a sender that isn't a stock `logit_out`.

Two facts constrain the fix. A frame's CRC covers its compressed bytes, and an lz4 payload can't
be read without decompressing it, so naming a compressed frame means decoding part of it.
`logit_in` reads frames serially, and a frame whose body it doesn't read leaves it no way to find
the next frame's header.

## Decision

The sender identity and sequence move from the hop trailer to a prefix ahead of the batch;
`Ack` gains a required status, `accepted` or `rejected(reason)`; and a rejected `Ack` settles the
one frame it names, with the connection and the frames behind it untouched.

### 1. The hop payload leads with its sender pair

```
hop_payload   := id[16] | uvarint(seq) | batch | uvarint(trailer_len) | trailer_bytes[trailer_len]
trailer_bytes := (tag: u8, len: uvarint, value: [u8; len])*     -- tag 1 origin, tag 2 previous
```

- **`id` is 16 bytes and `seq` a uvarint of at least 1**, as before. A payload shorter than the
  pair, or a `seq` of 0, is `Malformed`.
- **The trailer keeps provenance only.** Tags 3 and 4 are no longer written. An unknown tag is
  still skipped, so a stray 3 or 4 is ignored, as any other unknown tag is.
- **The trailer length stays mandatory**, so no proper prefix of a hop payload decodes.
- **`read_hop_prefix` reads the pair alone** and `decode_hop_body` reads the rest;
  `decode_hop_batch` is the two together. `logit_in` calls them separately.
- **The longest prefix is 26 bytes** (`HOP_PREFIX_MAX_LEN`): 16, plus a 10-byte uvarint.

### 2. `Ack` carries a status

`Ack`'s fields, each `tag(u8) + len(uvarint) + value`:

| Tag | Field | Value | Rule |
|---|---|---|---|
| 1 | `id` | 16 bytes | required |
| 2 | `seq` | uvarint, at least 1 | required |
| 3 | `status` | one byte: `0` accepted, `1` rejected | required; any other value or length is `Malformed` |
| 4 | `reason` | uvarint, a `u16` | required with `rejected`; `Malformed` with `accepted` |
| 5 | `message` | at most 1024 bytes of UTF-8, lossily decoded and cut to the cap | optional with `rejected`; `Malformed` with `accepted` |

- **Strict, as every control message is.** Each field at most once, an unknown tag `Malformed`
  ([ADR `native-hop-no-compatibility`](native-hop-no-compatibility.md), decision 4).
- **Reason codes, not an enum**, like `Reject.code`: `ACK_REJECTED_TOO_LARGE` (1),
  `ACK_REJECTED_DECODE_BUDGET` (2), `ACK_REJECTED_MALFORMED` (3). A reader that doesn't know a
  code still reads the frame as rejected.
- **The message is bounded like `Reject.message`.** The longest control message is now a
  rejected `Ack` at the message cap, 1066 bytes, under `MAX_CONTROL_MESSAGE_BYTES` (4096), which
  doesn't change. An accepted `Ack` is 3 bytes longer than before: 49 to 58 bytes on the wire.
- **Meaning.** An accepted `Ack` keeps [ADR `native-hop-named-acks`](native-hop-named-acks.md)'s
  meaning: every frame of `id` at or below `seq` that the connection carried is handled. A
  rejected `Ack` covers the one frame `seq` names: `logit_in` read its pair and dropped it, as it
  would on every resend.

### 3. `logit_in` refuses a frame by name and reads on

| The frame | Answer | Connection |
|---|---|---|
| its batch decodes past the per-frame decode budget | `Ack{rejected(decode_budget)}` | kept |
| its body after a valid prefix doesn't decode (batch or trailer) | `Ack{rejected(malformed)}` | kept |
| `uncompressed_len` past `max_frame_bytes`, `compressed_len` within `compressed_bound(max_frame_bytes)` | the compressed body read and CRC-checked, only the first 26 payload bytes decoded, `Ack{rejected(too_large)}` | kept |
| `compressed_len` past `compressed_bound(max_frame_bytes)` | `Reject{FRAME_TOO_LARGE}`, nothing of the body read | closed |
| a prefix that doesn't parse, a CRC mismatch, an lz4 body that doesn't decompress, another codec byte | no answer, counted in `logit.proto.errors` | closed |
| a batch no consumer took | `Reject{GOING_AWAY}` | closed |
| a `Hello` it refuses (version, codec) | `Reject{VERSION_MISMATCH}` or `Reject{NO_COMMON_CODEC}` | closed |

- **The run goes first.** An accepted `Ack` covers every earlier frame of its identity, so
  before a rejected `Ack`, `logit_in` writes the pending accepted run, of any identity, then the
  rejected `Ack` on its own. "An `Ack` never covers another identity" holds.
- **The mark rises.** A refused frame raises its sender's mark, as a forward does, so a resend of
  that sequence on a later connection, or a resume through `HelloAck.marks`, isn't forwarded.
- **A rejected `Ack` reports a drop, not a delivery.** [ADR `delivery-semantics`](delivery-semantics.md)'s
  item 3 has `logit_in` acknowledge only a batch a consumer took, or one at or below its mark. A
  rejected `Ack` is neither: it tells the sender the batch was dropped. The raised mark is what
  keeps a resend from being forwarded, not a claim that anything landed.
- **The decode precedes the resend check**, as it did. A resend whose body still fails is refused
  by name again rather than acknowledged on the mark, so the sender learns the frame was
  dropped.
- **An oversize frame is read at the cost of a frame at the cap.** `logit_in` reads its body only
  when `compressed_len` is within the bound it reads every frame under, and never allocates from
  `uncompressed_len`: `frame::read_frame_prefix` checks the CRC and decodes the lz4 block only as
  far as the prefix. A frame past the compressed bound can't be read without reading past the
  bound, so its pair and where the next frame starts are unknown, and the connection closes.
- **`GOING_AWAY` still answers a frame no consumer took.** A consumer may take it on a resend, so
  it isn't a rejection, and `GOING_AWAY` and forwarding still exclude each other: a rejected
  `Ack`, like every `Reject`, goes out before its frame could reach `send_relayed`.
- **Telemetry.** A refused frame counts `logit.proto.errors` under its reason (`decode_budget`,
  `malformed`, or `too_large`) and `logit.input.batches.dropped{reason="rejected"}`, and
  `logit.input.acks` counts the rejected `Ack`. The decode budget is diagnosed under its own
  `decode_budget` key, the other two under `frame_rejected`.

### 4. `logit_out` settles the head and keeps the window

- **A rejected `Ack` must name the front entry**, which no earlier `Ack` marked. `await_ack`
  pops it and returns `Fault::Rejected` with `logit_in`'s reason and message, keeping the
  connection and every frame behind the head. A rejected `Ack` naming any other frame is a
  protocol error, `Ambiguous`, and drops the connection.
- **The runtime tells "this head failed" from "the connection failed"** through a marker,
  `logit_pipeline::HeadOnly`, attached as `anyhow` context beside the `Fault` the way `Fault`
  itself travels. On an `await_ack` error carrying it, `window_round` counts the head alone at
  fault and leaves `outstanding` counting the frames still in flight; `write_loop` commits the
  head as `batches.dropped{reason="rejected"}`, prints the destination's text in the
  `send_failed` diagnostic, and goes on to the next head with nothing resubmitted. A disk spool
  commits the head, so it never replays. The marker is legal only beside `Fault::Rejected`: a
  retried head would have to be resubmitted behind frames already in flight.
- **Counted** as `logit.output.requests{class="rejected"}`, once.
- **The sender learns of a rejection only from the rejected `Ack` it reads.** A mark carries no
  status. If the connection fails after `logit_in` raised the mark and before `logit_out` read the
  rejected `Ack` (a write failure, a stall, the sender's ack timeout), `logit_out` reads the loss
  as `Ambiguous`, and the next handshake's `HelloAck.marks` covers that sequence. The sink then
  commits the frame as resumed and delivered (`logit.output.batches.resumed`,
  `logit.component.batches.delivered`), while `logit_in` counted it
  `logit.input.batches.dropped{reason="rejected"}`. Raising the mark later wouldn't close this: a
  later forwarded frame raises it past the refused one anyway, and a mark resume never resends the
  bytes that would be refused again. `docs/known-gaps/native-hop.md` tracks it.

### 5. A `Hello` refusal is `Refused`

`logit_out` reads `Reject{VERSION_MISMATCH}` and `Reject{NO_COMMON_CODEC}`, and a `HelloAck` that
doesn't answer its `Hello`, as `Fault::Refused`: the peer answers every batch the same way, so the
head holds and retries until the peer or the config changes. `Reject{INTERNAL}` at the connection
cap stays `Clean`: it's transient, and no frame left. Both were already true in code; this record
states them.

### 6. No compatibility, and the spool

`PROTOCOL_VERSION`, the frame `VERSION`, and the codec bytes don't change ([ADR
`native-hop-no-compatibility`](native-hop-no-compatibility.md)). A peer from before this record
fails: it reads this record's `Ack` as `Malformed` (an unknown tag 3), and its hop payload, batch
first, doesn't decode here. The `buffer.disk:` spool records hop frames, so a record written before
this change doesn't parse: `parse_record` counts it
`logit.component.batches.dropped{reason="disk_corrupt"}` and skips it, as any corrupt record.
Drain a spool before upgrading a `logit_out` that has one.

## Alternatives considered

- **Keep the pair in the trailer and send a rejection that names nothing.** `logit_out` would
  attribute it to the head by position, the contract [ADR
  `native-hop-named-acks`](native-hop-named-acks.md) removed for accepted acks, and a frame
  refused while another identity's run is pending would be ambiguous. Moving 17 to 26 bytes to the
  front costs nothing on the wire and names every refusal.
- **Answer `Ack{rejected}` only after a full decode.** A body past its budget never finishes
  decoding, and that's the refusal that happens. Decoding the trailer first would need the batch's
  length up front, which the bare batch payload doesn't carry.
- **Treat a rejected `Ack` as a connection fault.** It's what `Reject{FRAME_TOO_LARGE}` does now:
  every frame behind the head is resent after a reconnect, and under `at_most_once` an
  `Ambiguous` reading of them loses them. The receiver has handled each of those frames or will,
  so nothing about the connection is in doubt.
- **A new `Reject` code with a sequence field.** `Reject` closes the connection by definition, and
  a refusal of one frame shouldn't.
- **A separate `Nack` message type.** The status is a property of the acknowledgment, the run
  rule already keeps a rejected sequence out of an accepted run, and one message type keeps one
  ordering rule.

## Consequences

- **Code.** `logit_proto::native`'s hop layout, `read_hop_prefix`, and `decode_hop_body`;
  `control::Ack`'s status and `AckStatus`; `frame::read_frame_prefix`; `logit_in`'s refused path
  and `ack_rejected`; `logit_out`'s `settle_rejected`; `Output::await_ack`'s `HeadOnly` marker and
  `window_round`'s use of it. The fuzz seeds are regenerated for the new layout, and the
  `native_frame` target reads each position's prefix as `logit_in` does.
- **A refusal no longer costs the window.** The frames behind a refused head are acknowledged and
  delivered on the same connection, with no reconnect and no resend.
- **An uncompressed oversize frame still closes the connection.** For a frame without
  compression, `compressed_len` equals `uncompressed_len`, so only a frame within
  `max_frame_bytes / 255 + 16` bytes of the cap is read and refused by name. An lz4 frame that
  compresses under the bound is refused by name however large its payload declares itself. A
  stock `logit_out` checks the cap and never sends either.
- **The decode budget gap narrows, it doesn't close.** A sender still learns the cap, not the
  budget, so a stock batch can still be refused; it's now dropped alone, by name.
- **The spool doesn't survive the upgrade.** Records written before this change are skipped as
  corrupt (decision 6).
- **Follow-up, not built.** A streaming drain, reading a frame through a fixed scratch buffer with
  a running CRC up to a drain cap, would extend the by-name `too_large` path to uncompressed
  frames without buffering them.
- **A lost rejected `Ack` reads as a delivery at the sender** (decision 4). Closing it would take a
  bounded per-identity set of rejected sequences that `HelloAck` could name; out of scope.
- **Operator docs.** `docs/design/wire-protocol.md`'s hop payload and connection protocol,
  `docs/design/internal-telemetry.md`'s `logit_in` and `logit_out` rows, `docs/deploying.md`'s
  native-hop section, and `docs/known-gaps/native-hop.md`'s decode-budget entry describe the
  rejected `Ack`.
