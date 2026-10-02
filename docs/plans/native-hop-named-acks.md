---
created: 2026-10-02
updated: 2026-10-02
---

# Enabling plan: native hop named acks — a cumulative `Ack { id, seq }`, coalesced at `logit_in`, and a resume mark

## Goal

Make the code do what [ADR `native-hop-named-acks`](../adr/native-hop-named-acks.md) decides:
`Ack` names an identity and a sequence and covers every frame of that identity at or below it;
`logit_in` writes one ack per run of handled frames instead of one per frame; and a reconnect
commits the frames the receiver already holds without resending them. Stream key `cack`.
`cack/w0` is the ADR and this plan, and changes no code.

## Non-goals

- Credit messages, a window that changes after the handshake, or any flow control beyond the
  fixed window ([ADR `native-hop-send-window`](../adr/native-hop-send-window.md)).
- Out-of-order acknowledgment, or per-record ack state in the spool.
- A concurrent receiver: `logit_in` keeps handling one connection's frames serially.
- A time-based ack flush.
- Any compatibility with a peer from before this stream ([ADR
  `native-hop-no-compatibility`](../adr/native-hop-no-compatibility.md)).

## Workstreams

Each is one PR, and each branches from the one before, W1 from W0, so the stack reads W0 to W4; a
child PR targets its parent and is retargeted to `main` once the parent merges.

| WS | Branch | Change | ADR decision |
|---|---|---|---|
| W0 | `cack/w0` | The ADR, this plan, and the "superseded in part" markers | all |
| W1 | `cack/w1` | The wire: `Ack { id, seq }`, `Hello.senders`, `HelloAck.marks`; both ends send and read them with no coalescing and no resume yet; the in-flight list and prefix marking at `logit_out`; the drift check removed | 1, 3, 5 |
| W2 | `cack/w2` | Coalescing at `logit_in`: the pending ack and its four flush points | 2 |
| W3 | `cack/w3` | The resume: identities out in `Hello`, marks back in `HelloAck`, covered frames committed without a send | 4 |
| W4 | `cack/w4` | Operator docs, known gaps, telemetry docs, and measurements | consequences |

### W0: ADR and plan

- [x] `docs/adr/native-hop-named-acks.md` from `docs/adr/TEMPLATE.md`: decisions 1–5, "What
  stays", and the alternatives (a positional cumulative ack, no count cap, a time-based flush, a
  separate `Resume` message, marks resolved per frame, out-of-order acknowledgment, keeping the
  drift check).
- [x] `docs/plans/native-hop-named-acks.md`: this plan.
- [x] "Superseded in part" markers in ADRs `native-hop-identity-and-sequence`,
  `native-hop-send-window`, `native-hop-no-compatibility`, and
  `native-transport-handshake-and-ack`.
- [x] `docs/adr/README.md` and `docs/plans/README.md` index rows.

### W1: the wire and the sender's in-flight list

Files: `crates/logit-proto/src/native/control.rs`, `crates/logit-inputs/src/logit.rs`,
`crates/logit-outputs/src/logit.rs`, `crates/logit-pipeline/src/output.rs`,
`crates/logit-pipeline/src/runtime.rs`, `crates/logit-inputs/src/logit/senders.rs`,
`fuzz/seedgen/src/lib.rs`, `fuzz/seeds/native_control/`, `docs/design/wire-protocol.md`.

- [ ] `control.rs`: `Ack`, `Hello.senders`, and `HelloAck.marks` as the ADR's decisions 1 and 4
  lay them out, under the strict decoding `Hello`/`HelloAck`/`Reject` already use
  (`set_once`/`required`/`unknown_tag`). A `senders` or `marks` field whose length isn't a
  multiple of its entry size, or that holds more than `MAX_HELLO_SENDERS` entries, is
  `Malformed`. Both lists are empty in W1 (`logit_out` sends `[]`, `logit_in` answers `[]`); W3
  fills them.
- [ ] `MAX_CONTROL_MESSAGE_BYTES` stays 4096; update its doc's arithmetic for the two new fields.
- [ ] `logit_in`: `serve_frames` writes one named `Ack` per frame (W2 coalesces). `SenderTable`
  gains a read-only `marks(&self, ids: &[[u8; 16]])` lookup: no insert, no eviction, no
  `last_seen` bump.
- [ ] `logit_out`: `Conn.in_flight` becomes the in-flight list of decision 3; `submit` pushes;
  `read_ack` marks a prefix and fails `Ambiguous` on a shape violation; `await_ack` answers from
  the list before reading the wire; `record_in_flight` reports the list's length. Remove
  `drifted` and the `in_flight != held` check. `logit.output.ack.duration` records one sample per
  wire read.
- [ ] `Output::submit(&mut self, batch, ctx, seq)` loses `in_flight`; `window_round` and the
  `WindowedOutput` test double follow; `docs/design/pipeline-graph.md`'s `deliver_window` row if
  it names the argument.
- [ ] `fuzz/seedgen`: the `hello`, `hello-ack`, and `ack` seeds carry the new fields; regenerate
  with `script/unsafe-check fuzz-seed`.
- [ ] `docs/design/wire-protocol.md`, "Connection protocol": the `Ack` fields and meaning, the
  two new handshake fields, and the identity-order rule.
- [ ] `control.rs` tests: `an_ack_round_trips_its_identity_and_sequence`,
  `an_ack_missing_either_field_is_malformed`, `an_ack_with_a_sequence_of_zero_is_malformed`,
  `hello_senders_and_hello_ack_marks_round_trip`, `a_senders_list_over_the_cap_or_misaligned_is_malformed`;
  the existing table tests (`a_message_missing_any_field_is_malformed`,
  `a_message_repeating_any_field_is_malformed`, `every_valid_message_decodes_from_its_fields`)
  gain the new fields and `Ack`.
- [ ] `logit_in` tests: `every_ack_names_the_frame_it_answers`;
  `pipelined_frames_are_acked_in_frame_order` asserts the names.
- [ ] `logit_out` tests: `an_ack_naming_the_head_commits_it`,
  `a_cumulative_ack_commits_every_frame_of_its_identity_up_to_the_named_sequence`,
  `an_ack_for_an_identity_not_at_the_front_is_ambiguous_and_drops_the_connection`,
  `an_ack_naming_a_sequence_not_in_flight_is_ambiguous`,
  `await_ack_answers_from_the_list_before_reading_the_wire`; delete
  `a_submit_whose_in_flight_disagrees_with_the_connection_is_ambiguous`.
- [ ] Pins: `logit_out: encode + frame, 1 batch` (31) and `logit_in: read + decode, 1 batch` (7)
  expected unchanged; a `VecDeque` push per submit may show in a `logit_out` round pin if one
  exists. A tripped pin is updated with `docs/design/memory.md` in the same commit.

### W2: coalescing at `logit_in`

Files: `crates/logit-inputs/src/logit.rs`, `docs/design/internal-telemetry.md`.

- [ ] `serve_frames` keeps `pending: Option<(SeqId, u32)>` (the last handled frame's pair and the
  frames covered) and flushes it at the four points of the ADR's decision 2. The identity check
  runs after decode and before `is_resend`. The read-would-block flush is one raw `poll_read`
  into a fresh header buffer (`std::future::poll_fn` over `Pin::new(&mut *stream).poll_read`): a
  `Pending` consumes nothing, plain or TLS, so the ack is written and flushed before the real
  wait; a partial fill is passed into `read_header` so no header byte is lost. Not a
  `tokio::select!` over `read_header`, whose dropped future would lose the bytes it had read. The
  flush before a `Reject` lives in `going_away` and `write_reject`; the flush on every other exit
  is best effort and doesn't change the exit.
- [ ] `logit.input.acks` (count), recorded in `docs/design/internal-telemetry.md`'s `logit_in`
  section beside `logit.proto.frames`.
- [ ] Tests, on observables (`TelemetryProbe`, `wait_until`), no sleeps:
  - `a_burst_of_frames_from_one_sender_is_answered_by_one_ack_naming_the_last`
  - `a_frame_from_a_different_sender_flushes_the_pending_ack_first`
  - `a_quiet_sender_gets_its_ack_without_waiting_for_another_frame`
  - `a_run_longer_than_the_coalesce_cap_is_acked_every_cap_frames`
  - `a_pending_ack_is_flushed_before_going_away` (shutdown, idle, and no-consumer cases)
  - `a_pending_ack_is_flushed_before_a_frame_too_large_reject`
  - `resends_below_the_mark_are_acked_by_their_own_sequence_not_the_mark`
  - `a_window_against_logit_in_delivers_every_batch_once_in_order` (existing, now with
    coalesced acks)
  - under TLS: `coalesced_acks_over_tls_reach_a_sender_with_frames_still_buffered`

### W3: the resume

Files: `crates/logit-outputs/src/logit.rs`, `crates/logit-inputs/src/logit.rs`,
`crates/logit-inputs/src/logit/senders.rs`, `crates/logit-cli/tests/logit_round_trip.rs`,
`docs/design/internal-telemetry.md`.

- [ ] `logit_out`: when a connection is dropped with frames in flight, keep the distinct
  identities of its unacked entries as `resend_senders`; the next `handshake` sends them and
  stores the answered marks on the new `Conn`. `submit_frame` applies the ADR's decision 4: with
  `stream == None` and `resend_senders` non-empty it connects and handshakes before encoding,
  then checks the head against the marks; a covered frame is pushed `acked`, counted, and
  returned `Ok` with no encode, no size gate, and no write. With nothing to resend the existing
  order (encode, size gate, connect) is unchanged, and a test pins that an oversized batch still
  never connects on that path.
- [ ] `logit_in`: `handshake` answers `marks: senders.marks(&hello.senders)`.
- [ ] `docs/design/internal-telemetry.md`: `logit.output.batches.resumed`.
- [ ] Tests:
  - `logit_out`: `a_reconnect_lists_the_identities_of_the_frames_it_will_resend`,
    `a_frame_at_or_below_its_mark_is_committed_without_a_write`,
    `a_frame_above_its_mark_is_sent_and_its_identity_stays_listed`,
    `marks_for_an_identity_the_receiver_omits_read_as_zero`,
    `more_than_sixteen_identities_lists_the_first_sixteen_and_resends_the_rest`.
  - `logit_in`: `hello_ack_answers_a_mark_for_each_listed_identity_it_holds`,
    `a_marks_lookup_neither_inserts_nor_evicts`.
  - Integration, `crates/logit-cli/tests/logit_round_trip.rs`: a real pair with a disk spool and
    `window: 32`; kill the listener's connection after N acks; assert the resumed count equals
    the frames acked but not yet committed, `logit.input.batches.resends` is 0 for them, every
    batch is forwarded once, in order.

### W4: operator docs, known gaps, and measurements

- [ ] `docs/design/wire-protocol.md`: "Flow control" and the acknowledgement-point text (one ack
  per run; the resume); `docs/deploying.md`, forwarding section: what an ack means now, the
  resume, and that a reconnect no longer resends acknowledged frames; module docs of both hop
  components; the `RECEIVER_MAX_WINDOW` doc in `crates/logit-inputs/src/logit.rs` carries the
  named ack's size (about 80 KB for 1024 under TLS); `docs/known-gaps.md`: add the 16-identity
  cap residual and reword the `ack_write_stalled` reasoning for coalesced, larger acks;
  `AGENTS.md`'s `logit_out`/`logit_in` rows gain this ADR.
- [ ] Sweep every comment line the stream added for the banned words.
- [ ] `script/cibuild` at the stack's tip, from a private `CARGO_TARGET_DIR`.
- [ ] Measurements in "Findings" below: loopback `native-relay` at `window: 1` and `window: 32`
  before and after, and a container `netem` run at 10 ms RTT, as the send-window plan did. The
  perf VM run is owed and batched with the pending re-baseline.

## Verification

- `script/check` per PR; `script/cibuild` at `cack/w4`.
- The fuzz target `native_control` after W1, under `script/unsafe-check fuzz native_control`, to
  confirm the new fields have no panic path.
- The integration test in W3 is the end-to-end check: a spooled sender, a mid-window fault, and
  a receiver that forwards every batch once with the resumed frames never re-sent.

## Findings

(Recorded as the workstreams land.)
