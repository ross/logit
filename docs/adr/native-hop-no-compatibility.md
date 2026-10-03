---
created: 2026-10-01
updated: 2026-10-02
---

# Native hop: two payload shapes named by shape, every hop frame sequenced, and strict control messages

## Status
Accepted. Supersedes in part:

- [ADR `native-hop-identity-and-sequence`](native-hop-identity-and-sequence.md): decision 1's
  "The unsequenced rule", decision 2's "A v1 connection sends unsequenced frames", decision 3's
  "An old spool's records replay unsequenced", step 1 of decision 5's per-frame algorithm, and the
  "The breaking change" consequence.
- [ADR `native-hop-send-window`](native-hop-send-window.md): decision 1's "A `HelloAck.window`
  of 0 reads as 1".
- [ADR `batch-provenance-on-delivered`](batch-provenance-on-delivered.md): the v1-peer case of
  `stamp_relayed`'s backfill, "Negotiation needs no new machinery", the `CODEC_NATIVE_V1` clause
  of "The disk queue's 24-byte trace-context prefix is not widened", and the two consequences
  about builds on different versions still talking and old segments staying readable.
- [ADR `disk-backed-sink-buffer`](disk-backed-sink-buffer.md): "Record evolution" in the
  2026-10-01 amendment, where an older binary replays a record and a record written before the
  change replays unsequenced.
- [ADR `native-transport-handshake-and-ack`](native-transport-handshake-and-ack.md): "Control
  payload"'s "skip-unknown forward compatibility as `native::record`".

Superseded in part on 2026-10-02 by [ADR `native-hop-named-acks`](native-hop-named-acks.md):
decision 4's "`Ack` is the message byte alone". `Ack` carries an identity and a sequence, both
required, under the same strict rules.

## Context

`logit` is pre-release and keeps no compatibility with its own earlier builds
([`docs/plans/lossless-transit.md`](../plans/lossless-transit.md), "Pre-release, no backward
compatibility"). The native `logit_out` to `logit_in` hop and the disk spool still tolerate
older peers and older records:

- `Hello.codecs` offers `[CODEC_NATIVE_V2, CODEC_NATIVE_V1]` and `logit_in` falls back to v1, a
  payload with no trailer, so no provenance and no sender pair, for a peer that offers only `[1]`.
- A frame or spool record without a complete sender pair is "unsequenced": `logit_in` forwards it
  every time it arrives, and `parse_record` replays a `CODEC_NATIVE_V1` record with empty
  provenance and no pair. That rule exists for a v1 peer and for a record spooled before the
  pair existed. It also turns a corrupt or buggy frame into a silent duplicate.
- `Hello.window` and `HelloAck.window` read as 0 when absent, and both ends clamp 0 to 1, for a
  peer that predates the window.
- `Ack::decode` drains any field, so an older `logit_in`'s `Ack { seq }` decodes.
- `Hello`, `HelloAck`, and `Reject` skip an unknown field, documented in `control.rs` as room for
  a later protocol version.
- The "maybe unsequenced" case is a type: `StoreItem`, `Output::observe_batch`, `Output::submit`,
  `parse_record`, and every sink carry `Option<SeqId>`, although the memory store always numbers
  and the disk store writes a pair into every record it pushes.

There will be no v1 peers and no old spool records. Each accommodation is a branch that nothing
reaches in a deployment and that a reviewer must reason about, and the unsequenced rule is a
loss of the effectively-once property for the frames least likely to be legitimate.

One fact shapes the codec decision: the trailer-less payload isn't only a compatibility path.
`NativeEncoder`/`NativeDecoder` write and read it as `file_out`/`stdio_out`'s `format: native`
([ADR `file-output-native-format`](file-output-native-format.md)), the perf harness decodes it
from its telemetry dump (`crates/logit-perf/src/telemetry_leg.rs`), and the benches pin it. A
file has no sender and no hop, so it needs no trailer. The two payloads are two shapes, not two
versions.

## Decision

The native wire has two payload shapes, named by what they carry; every hop frame and every
spool record carries a complete sender pair; a control message carries its defined fields and
nothing else; and the hop negotiates one codec.

### 1. Two shapes, named by shape

- `CODEC_BATCH` (codec byte 1) is a bare batch: dictionary, resource, events. It's the file
  format and the perf dump, written by `NativeEncoder` and read by `NativeDecoder`.
- `CODEC_HOP_BATCH` (codec byte 2) is a batch followed by the trailer: provenance (tags 1 and 2),
  the sender identity (tag 3), and the sequence (tag 4). It's what `logit_out` sends and what the
  disk spool records.
- The byte values don't change. `encode_batch`/`decode_batch` keep their names;
  `encode_batch_v2`/`decode_batch_v2` become `encode_hop_batch`/`decode_hop_batch`.
- The trailer's length prefix stays mandatory, so a hop payload truncated at any byte still fails
  to decode ([ADR `batch-provenance-on-delivered`](batch-provenance-on-delivered.md)'s truncation
  invariant). A provenance field that's empty still writes no entry. An unknown trailer tag is
  still skipped: that's torn-write hygiene for the spool, the same rule `native::record` holds.

### 2. Every hop frame and spool record carries a complete pair

- `decode_hop_batch` returns `(EventBatch, Provenance, SeqId)`. A trailer with tag 3 or tag 4
  missing or repeated, an identity that isn't 16 bytes, a sequence of 0 or with bytes left over
  after its uvarint, is `CodecError::Malformed`.
- `encode_hop_batch` takes a `SeqId`, not an `Option`.
- `StoreItem` is `(Arc<EventBatch>, BatchContext, SeqId)`. `Output::observe_batch` and
  `Output::submit` take a `SeqId`. The store numbers every batch it holds, so nothing in the
  pipeline is unsequenced.
- `logit_in`'s per-frame algorithm has two steps: at or below the mark, count and acknowledge
  without forwarding; above it, forward, raise the mark, and acknowledge. A frame whose decode
  fails is a protocol error, counted in `logit.proto.errors` and ending the connection, as any
  malformed data frame is today.
- `parse_record` decodes `CODEC_HOP_BATCH` only. Any other codec byte, and a record without a
  complete pair, is a corrupt record: skipped and counted through `skip_corrupt`, as a record
  with a bad CRC is.
- `CONTEXT_LEN` still never widens, because a record carries no version of its own and the
  resync arithmetic assumes the prefix. That's a design constraint on the record, not a promise
  about records already on disk.
- `Fanout::stamp_relayed` keeps backfilling `logit_in`'s own id into an empty `origin` or
  `previous`. The case it serves is a peer that carried none, not a peer that couldn't.

### 3. The hop negotiates one codec

- `logit_out` offers `[CODEC_HOP_BATCH]`. `logit_in` answers `Reject{NO_COMMON_CODEC}` to a
  `Hello` that doesn't offer it. A data frame with any other codec byte is a protocol error.
- `Hello.codecs` and `HelloAck.codec` stay as fields. Compression is still negotiated through
  the same handshake, and a one-member list costs nothing.
- The `codec` label on `logit.proto.frames`, in both directions, goes. A label with one value
  says nothing. `logit.proto.errors{reason="codec"}` stays: it counts a frame under the wrong
  codec byte.
- `logit_out` encodes a batch once per attempt, as a hop payload, and runs its pre-connect size
  gate on that payload. A `send` with no preceding `observe_batch` is a programming error, not an
  unsequenced send.

### 4. Strict control messages

- `Hello`, `HelloAck`, and `Reject` decode their defined fields, each required. An absent field
  or an unknown tag is `CodecError::Malformed`.
- `Ack` is the message byte alone. A body is `Malformed`. [Superseded on 2026-10-02 by [ADR
  `native-hop-named-acks`](native-hop-named-acks.md): `Ack` carries a required identity and
  sequence.]
- `window` is at least 1 in both `Hello` and `HelloAck`, enforced on decode. `logit_in` answers
  `min(hello.window, RECEIVER_MAX_WINDOW)`; `logit_out` uses `min(offered, answered)`. Graph rule
  75 keeps the configured `window` at 1 or more, so nothing else clamps.
- `MAX_CONTROL_MESSAGE_BYTES` stays where it is, as headroom for a longer `Reject.message` or
  codec list, not for fields a later version adds.
- An unknown message type stays `Malformed`, as today.

### 5. Breaking change, no path

`PROTOCOL_VERSION`, the frame `VERSION`, and the two codec bytes don't change. A `logit_out` from
before this record offers `[2, 1]` and still talks; one that offers `[1]` alone is rejected; a
`logit_in` from before it answers `window` and `codec` as before and is unaffected. None of that
is a supported deployment, and nothing in the code accounts for it.

### What stays

These look like compatibility code and aren't:

- **Skip-unknown fields in `native::record`, `native::value`, and the hop trailer.** A record
  or a trailer is also what the spool reads back after a crash, and skipping an unknown tag by
  its length is one branch that keeps a torn write from poisoning the walk. The module docs say
  so.
- **Strict `PROTOCOL_VERSION` and frame `VERSION` checks, and the two reserved header bytes.**
  A mismatch is a reject, and reserved room has no code path.
- **`CURSOR_VERSION` in the spool.** A bad or mismatched `cursor.json` falls back to the oldest
  segment, which is corruption handling.
- **`reject_is_permanent` treating an unknown `Reject` code as transient.** It decides what to
  do with a code this build doesn't name, and either answer is defensible; it isn't a path an
  older peer reaches.
- **Other systems' older formats**: collectd's legacy `Time`/`Interval` parts, a dd-trace
  tracer's decimal ids, RFC 3164 syslog, remote-write 1.0. Interop, out of scope.

## Alternatives considered

- **One codec, with the trailer on the file format too.** `format: native` would write an empty
  provenance and no pair, so the codec would still need `Option<SeqId>` and the hop would still
  have to reject `None`. Two shapes let the hop's decoder return a required pair and keep the
  file format as it is.
- **Keep the `V1`/`V2` names.** A smaller diff, but the names describe a negotiation that no
  longer exists and invite a reader to look for the fallback.
- **Keep skip-unknown in control messages, reworded as torn-write hygiene.** A control message
  travels over TCP, which has no torn writes, and a `Hello` or `HelloAck` is read once at
  connection start. One strict rule for every control message is less to reason about than two
  rules with a rationale that doesn't fit one of them.
- **A reject code for a bad `Hello`.** `window >= 1` is a field constraint, so the decoder
  enforces it and a bad `Hello` is a malformed control frame, which already ends the connection.
  No new code.
- **Forward an unsequenced frame as a last resort.** That's the rule being removed: a frame that
  reaches `logit_in` without a pair came from a bug or a corruption, and forwarding it trades
  effectively-once for nothing.

## Consequences

- **Code.** `crates/logit-proto/src/native/mod.rs` (renames, the required pair),
  `crates/logit-proto/src/native/control.rs` (strict decoders), `crates/logit-pipeline`'s
  `queue.rs`/`disk_queue.rs`/`output.rs`/`runtime.rs` (`Option<SeqId>` to `SeqId`, one spool
  codec), `crates/logit-inputs/src/logit.rs` and `crates/logit-outputs/src/logit.rs` (one codec,
  no unsequenced rule, no window clamp, no `codec` label), every sink's `observe_batch`, the fuzz
  targets and seeds, and the perf dump reader. Allocation and size pins that trip are updated
  with `docs/design/memory.md` in the same commit.
- **Telemetry.** `logit.proto.frames` loses its `codec` label in both directions.
  `docs/design/internal-telemetry.md` records the change.
- **Operator docs.** `docs/design/wire-protocol.md` and `docs/deploying.md` describe the hop as
  two shapes and a required pair, and `docs/known-gaps/native-hop.md` drops "a v1 peer" from the
  duplicate sources the mark doesn't cover.
- **Follow-up.** With every frame sequenced, a named cumulative `Ack { id, seq }` becomes a
  strict simplification of the sender's in-flight tracking. Its own record, when it's taken up.
