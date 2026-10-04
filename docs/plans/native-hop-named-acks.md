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

- [x] `control.rs`: `Ack`, `Hello.senders`, and `HelloAck.marks` as the ADR's decisions 1 and 4
  lay them out, under the strict decoding `Hello`/`HelloAck`/`Reject` already use
  (`set_once`/`required`/`unknown_tag`). A `senders` or `marks` field whose length isn't a
  multiple of its entry size, or that holds more than `MAX_HELLO_SENDERS` entries, is
  `Malformed`. Both lists are empty in W1 (`logit_out` sends `[]`, `logit_in` answers `[]`); W3
  fills them.
- [x] `MAX_CONTROL_MESSAGE_BYTES` stays 4096; update its doc's arithmetic for the two new fields.
- [x] `logit_in`: `serve_frames` writes one named `Ack` per frame (W2 coalesces). `SenderTable`
  gains a read-only `marks(&self, ids: &[[u8; 16]])` lookup: no insert, no eviction, no
  `last_seen` bump.
- [x] `logit_out`: `Conn.in_flight` becomes the in-flight list of decision 3; `submit` pushes;
  `read_ack` marks a prefix and fails `Ambiguous` on a shape violation; `await_ack` answers from
  the list before reading the wire; `record_in_flight` reports the list's length. Remove
  `drifted` and the `in_flight != held` check. `logit.output.ack.duration` records one sample per
  wire read.
- [x] `Output::submit(&mut self, batch, ctx, seq)` loses `in_flight`; `window_round` and the
  `WindowedOutput` test double follow; `docs/design/pipeline-graph.md`'s `deliver_window` row if
  it names the argument.
- [x] `fuzz/seedgen`: the `hello`, `hello-ack`, and `ack` seeds carry the new fields; regenerate
  with `script/unsafe-check fuzz-seed`.
- [x] `docs/design/wire-protocol.md`, "Connection protocol": the `Ack` fields and meaning, the
  two new handshake fields, and the identity-order rule.
- [x] `control.rs` tests: `an_ack_round_trips_its_identity_and_sequence`,
  `an_ack_missing_either_field_is_malformed`, `an_ack_with_a_sequence_of_zero_is_malformed`,
  `hello_senders_and_hello_ack_marks_round_trip`, `a_senders_list_over_the_cap_or_misaligned_is_malformed`;
  the existing table tests (`a_message_missing_any_field_is_malformed`,
  `a_message_repeating_any_field_is_malformed`, `every_valid_message_decodes_from_its_fields`)
  gain the new fields and `Ack`. Landed with the misaligned-list test as
  `a_senders_or_marks_list_over_the_cap_or_misaligned_is_malformed`, plus
  `an_ack_with_a_short_identity_is_malformed`.
- [x] `logit_in` tests: `every_ack_names_the_frame_it_answers`;
  `pipelined_frames_are_acked_in_frame_order` asserts the names.
- [x] `logit_out` tests: `an_ack_naming_the_head_commits_it`,
  `a_cumulative_ack_commits_every_frame_of_its_identity_up_to_the_named_sequence`,
  `an_ack_for_an_identity_not_at_the_front_is_ambiguous_and_drops_the_connection`,
  `an_ack_naming_a_sequence_not_in_flight_is_ambiguous`,
  `await_ack_answers_from_the_list_before_reading_the_wire`; delete
  `a_submit_whose_in_flight_disagrees_with_the_connection_is_ambiguous`.
- [x] Pins: `logit_out: encode + frame, 1 batch` (31) and `logit_in: read + decode, 1 batch` (7)
  expected unchanged; a `VecDeque` push per submit may show in a `logit_out` round pin if one
  exists. A tripped pin is updated with `docs/design/memory.md` in the same commit. Landed with
  no pin tripped.

### W2: coalescing at `logit_in`

Files: `crates/logit-inputs/src/logit.rs`, `docs/design/internal-telemetry.md`.

- [x] `serve_frames` keeps `pending: Option<(SeqId, u32)>` (the last handled frame's pair and the
  frames covered) and flushes it at the four points of the ADR's decision 2. The identity check
  runs after decode and before `is_resend`. The read-would-block flush is one raw `poll_read`
  into a fresh header buffer (`std::future::poll_fn` over `Pin::new(&mut *stream).poll_read`): a
  `Pending` consumes nothing, plain or TLS, so the ack is written and flushed before the real
  wait; a partial fill is passed into `read_header` so no header byte is lost. Not a
  `tokio::select!` over `read_header`, whose dropped future would lose the bytes it had read. The
  flush before a `Reject` lives in `going_away` and `write_reject`; the flush on every other exit
  is best effort and doesn't change the exit.
- [x] `logit.input.acks` (count), recorded in `docs/design/internal-telemetry.md`'s `logit_in`
  section beside `logit.proto.frames`.
- [x] Tests, on observables (`TelemetryProbe`, `wait_until`), no sleeps:
  - `a_burst_of_frames_from_one_sender_is_answered_by_one_ack_naming_the_last`
  - `a_frame_from_a_different_sender_flushes_the_pending_ack_first`
  - `a_quiet_sender_gets_its_ack_without_waiting_for_another_frame`
  - `a_run_longer_than_the_coalesce_cap_is_acked_every_cap_frames` (landed as
    `..._acked_at_least_every_cap_frames`)
  - `a_pending_ack_is_flushed_before_going_away` (shutdown, idle, and no-consumer cases; landed as
    `..._on_shutdown` and `..._on_an_idle_close`, with the no-consumer case in
    `a_frame_no_consumer_took_mid_window_is_answered_going_away_and_nothing_after_it_is_forwarded`)
  - `a_pending_ack_is_flushed_before_a_frame_too_large_reject`
  - `resends_below_the_mark_are_acked_by_their_own_sequence_not_the_mark`
  - `a_window_against_logit_in_delivers_every_batch_once_in_order` (existing, now with
    coalesced acks)
  - under TLS: `coalesced_acks_over_tls_reach_a_sender_with_frames_still_buffered`
  - Also landed: `a_sender_that_stops_mid_frame_still_gets_the_acks_for_the_frames_before_it`,
    its `..._after_a_header_...` and `..._mid_header_...` siblings (a `stop_mid_frame_at`
    helper), and `a_stalled_ack_before_going_away_is_counted_and_the_reject_left_out`.

### W3: the resume

Files: `crates/logit-outputs/src/logit.rs`, `crates/logit-inputs/src/logit.rs`,
`crates/logit-inputs/src/logit/senders.rs`, `crates/logit-cli/tests/logit_round_trip.rs`,
`docs/design/internal-telemetry.md`.

- [x] `logit_out`: when a connection is dropped with frames in flight, keep the distinct
  identities of its unacked entries as `resend_senders`; the next `handshake` sends them and
  stores the answered marks on the new `Conn`. `submit_frame` applies the ADR's decision 4: with
  `stream == None` and `resend_senders` non-empty it connects and handshakes before encoding,
  then checks the head against the marks; a covered frame is pushed `acked`, counted, and
  returned `Ok` with no encode, no size gate, and no write. With nothing to resend the existing
  order (encode, size gate, connect) is unchanged, and a test pins that an oversized batch still
  never connects on that path.
- [x] `logit_in`: `handshake` answers `marks: senders.marks(&hello.senders)`.
- [x] `docs/design/internal-telemetry.md`: `logit.output.batches.resumed`.
- [x] Tests:
  - `logit_out`: `a_reconnect_lists_the_identities_of_the_frames_it_will_resend`,
    `a_frame_at_or_below_its_mark_is_committed_without_a_write`,
    `a_frame_above_its_mark_is_sent_and_its_identity_stays_listed` (landed as
    `a_frame_above_its_mark_is_sent`),
    `marks_for_an_identity_the_receiver_omits_read_as_zero`,
    `more_than_sixteen_identities_lists_the_first_sixteen_and_resends_the_rest`.
  - `logit_in`: `hello_ack_answers_a_mark_for_each_listed_identity_it_holds`,
    `a_marks_lookup_neither_inserts_nor_evicts` (landed as
    `..._nor_evicts_nor_touches_last_seen`).
  - Also landed: `an_oversized_head_with_nothing_to_resend_never_connects`,
    `an_oversized_head_with_identities_to_resend_connects_and_keeps_the_connection`,
    `a_hello_ack_mark_for_an_unoffered_identity_is_permanent`, and
    `a_hello_ack_naming_one_identity_twice_does_not_answer_the_hello`.
  - Integration, `crates/logit-cli/tests/logit_round_trip.rs`: a real pair with a disk spool and
    `window: 32`; kill the listener's connection after N acks; assert the resumed count equals
    the frames acked but not yet committed, `logit.input.batches.resends` is 0 for them, every
    batch is forwarded once, in order. Landed by reshaping the existing
    `a_connection_cut_mid_window_resends_the_window_and_logit_in_forwards_each_batch_once` into
    `a_connection_cut_mid_window_resumes_the_window_without_resending_it`.

### W4: operator docs, known gaps, and measurements

- [x] `docs/design/wire-protocol.md`: "Flow control" and the acknowledgement-point text (one ack
  per run; the resume); `docs/deploying.md`, forwarding section: what an ack means now, the
  resume, and that a reconnect no longer resends acknowledged frames; module docs of both hop
  components; the `RECEIVER_MAX_WINDOW` doc in `crates/logit-inputs/src/logit.rs` carries the
  named ack's size (about 80 KB for 1024 under TLS); `docs/known-gaps/`: add the 16-identity
  cap residual and reword the `ack_write_stalled` reasoning for coalesced, larger acks;
  `AGENTS.md`'s `logit_out`/`logit_in` rows gain this ADR.
- [x] Sweep every comment line the stream added for the banned words.
- [x] `script/cibuild` at the stack's tip, from a private `CARGO_TARGET_DIR`.
- [x] Measurements in "Findings" below: loopback `native-relay` at `window: 1` and `window: 32`
  before and after, and a container `netem` run at 10 ms RTT, as the send-window plan did, then
  the same on the perf VM ("W4: the perf VM re-run").

## Verification

- `script/check` per PR; `script/cibuild` at `cack/w4`.
- The fuzz target `native_control` after W1, under `script/unsafe-check fuzz native_control`, to
  confirm the new fields have no panic path.
- The integration test in W3 is the end-to-end check: a spooled sender, a mid-window fault, and
  a receiver that forwards every batch once with the resumed frames never re-sent.

## Findings

### W4: loopback and `netem` measurements

Laptop numbers, not the perf VM: AMD Ryzen AI 9 HX 370 (24 logical CPUs, mixed Zen 5 and Zen 5c
cores), 62 GiB, Linux 7.2.7, on battery under the `powersave` governor, runs unpinned, the
harness run inside the dev container. Binaries: `cack/w3` at 45a45184 (sha256 `b57b7607e612`;
W4 changes only docs and comments) and `main` at efd50c1e (sha256 `5ce36b7d7b63`, built from an
exported tree), each built `--release` and passed to `script/perf run`
with `--logit-bin`; the two arms ran alternately in one session (`main`, `cack`, `main`, `cack`),
so drift on battery falls on both. `generate_in` batches are 100 events.

**Loopback** (`native-relay` and `native-relay-window1`, 7M events, two passes of `--repeat 3`
per arm, medians of the six):

| Arm | Events/s | CPU µs/event | Peak RSS |
|---|---|---|---|
| `main`, `window: 32` | 1,263,103 | 1.379 | ~306 MiB |
| `cack/w3`, `window: 32` | 1,436,457 | 1.375 | ~335 MiB |
| `main`, `window: 1` | 823,314 | 1.599 | ~138 MiB |
| `cack/w3`, `window: 1` | 860,133 | 1.546 | ~143 MiB |

- At window 32, `cack/w3` moves 1.14× the events of `main` at the same CPU per event, and every
  `cack/w3` repeat (1.37M–1.49M) is above every `main` repeat (1.23M–1.34M). That fits the
  ADR's reading that the per-frame write and flush at `logit_in` bounded `native-relay`.
- At window 1 there is nothing to coalesce: every frame waits for its own `Ack`. The 1.04× is
  inside this box's spread at window 1 (both arms' repeats are bimodal, about 0.70M and 0.93M,
  which matches the mixed-core noise seen before), so it is read as no change.
- Peak RSS at window 32 rises about 10%, not attributed.
- What these don't show: the coalescing ratio itself (`logit.input.acks` against
  `logit.proto.frames{direction="in"}` wasn't captured), or the resume, which only a fault
  exercises; W3's integration test is its evidence.

**Latency** (`tc qdisc add dev lo root netem delay 5ms`, a 10 ms nominal RTT, in a sidecar
container whose network namespace the perf container joined; a temporary copy of `native-relay`
with `buffer.max_batches: 64` and 3M events, as the send-window plan's run used; `window: 32`;
two passes of `--repeat 2 --settle 2s` per arm, alternating, medians of the four):

| Arm | Events/s | Batches/s | CPU µs/event |
|---|---|---|---|
| `main`, `window: 32` | 302,993 | ~3,030 | 1.484 |
| `cack/w3`, `window: 32` | 289,970 | ~2,900 | 1.351 |

- The ceiling at 32 frames per 10 ms is 3,200 batches/s. `main` reaches about 95% of it and
  `cack/w3` about 91%; every `cack/w3` repeat (286k–291k) is below every `main` repeat
  (299k–308k), so the 4% gap is likely real on this box. CPU per event is about 9% lower.
- A likely cause, not verified: when the window is full, a burst of frames arrives together and
  `logit_in` acks the burst once, after its last frame, instead of freeing the sender's slots one
  frame at a time, so each cycle adds the burst's handling time to the round trip. The cap of 32
  frames per `Ack` is the window here, so it doesn't cut the burst short. Worth confirming on the
  VM before acting on it; a smaller `ACK_COALESCE_MAX` would trade some of the loopback gain back.
- This run measures a steady stream under latency, not the resume's WAN benefit, which shows only
  after a fault mid-window.

### W4: the perf VM re-run (2026-10-02)

The VM numbers replace the laptop's for every question below. They ran on `Standard_F8as_v6`
([`docs/design/performance.md`](../design/performance.md)'s preamble has the box facts) against
`efd50c1e` (C-, the `main` this plan's laptop run used) and `d1521c5f` (M, with the named acks).
The tables are in `performance.md` §1, "`native-relay` under a 10 ms round trip, and the
coalescing sweep", and its native-relay ladder. This section records what they settle.

- **Loopback, window 32.** M costs 0.858 µs/event against C-'s 0.958 (−10.4%), and runs 2,472,575
  events/s against 1,996,994, pooled over six repeats (twelve for M). That confirms the ADR's
  reading that the per-frame write and flush at `logit_in` bounded `native-relay`. The laptop's
  1.14× at flat CPU per event became a CPU saving here.
- **Loopback, window 1.** There's nothing to coalesce, and M reads +2.3% against C- (1.150 →
  1.177 µs/event; 1,259,807 → 1,205,667 events/s), with non-overlapping repeats (1.148–1.155
  against 1.172–1.183). It's under the 5% gate and unexplained, so it stays a note.
- **Peak RSS at window 32.** The laptop's ~10% rise doesn't hold: pooled medians are 369.1 MiB at
  C- and 287.7 MiB at M. RSS at this scenario is allocator retention and swings by tens of MiB
  between runs (`performance.md` §1, "Peak RSS").
- **Latency, window 32, 10 ms RTT (`netem`).** The gap holds and is smaller than the laptop's: C-
  reaches 99.5% of the 3,174 batches/s ceiling, M 97.2%, and every M repeat (307,571–311,090
  events/s) is below every C- repeat (315,938–316,017), over 8 repeats each. M costs 12.8% less CPU
  per event (0.850 against 0.975).
- **The coalescing ratio explains the gap, and the cap isn't the cause.** M sends one `Ack` per
  2.54 frames on loopback and 4.07 under latency, far below the cap of 32. Smaller caps behave the
  way that predicts: with `ACK_COALESCE_MAX` at 8 and 16, the ratio is 3.10 and 3.74 on loopback and
  3.28 and 4.05 under latency; the share of the ceiling is 98.5% and 97.6% (M: 97.2%); and on
  loopback the variants are inside M's own 12-repeat range (0.868 and 0.838 µs/event against M's
  0.858, range 0.823–0.882). A cap of 8 recovers 1.3 points of the 2.3, outside M's repeat range
  (its four repeats, 312,220–312,645 events/s, all sit above M's highest, 311,090), and gives up
  nothing measurable on loopback; a cap of 16's +0.4 is inside that range. With the whole gap about
  2 points, that isn't worth a change. The suspected cause is burst handling before the coalesced
  `Ack`: a full window's burst of frames arrives together and is acked once, after its last frame,
  which adds the burst's handling time to the round trip. The cap can't cut a burst of about four
  frames short.

**Conclusion.** The latency gap is real, about 2 points of the ceiling at 10 ms RTT.
`ACK_COALESCE_MAX` (32) doesn't bind, and the whole gap is small, so the constant stays. The gap is tracked in [`docs/known-gaps/native-hop.md`](../known-gaps/native-hop.md) with the
variant numbers.
