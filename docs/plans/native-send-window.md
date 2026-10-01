---
created: 2026-10-01
updated: 2026-10-01
---

# Enabling plan: a native hop send window — several frames in flight, acknowledged in frame order

## Goal

Make the code do what [ADR `native-hop-send-window`](../adr/native-hop-send-window.md) decides:
`logit_out` keeps up to a negotiated window of frames in flight on one connection, so the native
hop's throughput is no longer one batch per round trip. Stream key `flow`. `flow/w0` is the ADR
and this plan, and changes no code.

## Non-goals

- Credit messages, or any flow control beyond a fixed window negotiated at the handshake.
- QUIC.
- A concurrent receiver: `logit_in` keeps handling one connection's frames serially.
- Out-of-order acknowledgment.
- End-to-end acknowledgment ([ADR `delivery-semantics`](../adr/delivery-semantics.md), item 3).

## Workstreams

Each is one PR, stacked in order: each branches from the one before it.

| WS | Branch | Change | ADR decision |
|---|---|---|---|
| W0 | `flow/w0` | The ADR, this plan, and the "superseded in part" markers | all |
| W1 | `flow/w1` | The store: `peek_at`, a reserved prefix, `max_in_flight` | 3 |
| W2 | `flow/w2` | The `Output` trait's `window`/`submit`/`await_ack`, and `deliver_window` | 4 |
| W3 | `flow/w3` | `logit_out`, `logit_in`, config, schema, and telemetry docs | 1, 2, 5 |
| W4 | `flow/w4` | Operator docs, known gaps, and measurements | 6 |

### W0: ADR and plan

- [x] `docs/adr/native-hop-send-window.md` from `docs/adr/TEMPLATE.md`: decisions 1–6, and the
  alternatives (credit messages, `Ack` carrying the sequence, out-of-order acknowledgment with
  per-record ack state in the spool, a concurrent receiver, coupling the window to the posture, a
  receiver-side knob, a sender-side read-ahead buffer, a bigger default window).
- [x] `docs/plans/native-send-window.md`: this plan.
- [x] "Superseded in part" markers in ADRs `native-hop-identity-and-sequence`,
  `native-transport-handshake-and-ack`, `delivery-semantics`, and `disk-backed-sink-buffer`.
- [x] `docs/adr/README.md` and `docs/plans/README.md` index rows.

### W1: store

Files: `crates/logit-proto/src/buffer.rs`, `crates/logit-pipeline/src/queue.rs`,
`crates/logit-pipeline/src/disk_queue.rs`, `queue_stress.rs`, `disk_queue_verification.rs`.

- [ ] `Buffer::peek_at(&mut self, n) -> Option<&T>`; `InMemoryBuffer.head_reserved: bool` becomes
  `reserved: usize` (`peek` is `peek_at(0)`, `commit` decrements saturating, eviction starts at
  `reserved`, invariant `reserved <= len`).
- [ ] `BoundedQueue::peek_at` (synchronous, one lock), `SinkQueue::peek_at`, `max_in_flight`.
- [ ] `SinkStore::peek_at` (`async`, never waits for a push) and `SinkStore::max_in_flight`
  (memory `max_batches.max(1)`, disk `usize::MAX`).
- [ ] `DiskQueue.head_cache` becomes `read_ahead: VecDeque<ReadAhead { batch, ctx, seq, seg,
  offset, len }>`, with a next-segment helper that mirrors `roll_read_cursor` without unlinking.
  Every `head_cache` check (`push`'s make-room `nothing_queued`, the `DropOldest` to `DropNewest`
  arm, `evict_oldest`, `skip_corrupt`) becomes a `read_ahead` check.
- [ ] `queue_stress.rs` gains a `PeekAt` consumer op.
- [ ] `disk_queue_verification.rs`: `spool_op()` gains `PeekAt(n)`; the model checks FIFO
  first-commits and replay of uncommitted records.
- [ ] Buffer tests:
  - `peek_at_reserves_every_item_up_to_n_and_drop_oldest_evicts_past_them`
  - `peek_at_past_the_end_returns_none_and_reserves_nothing_new`
  - `commit_releases_one_reservation_and_leaves_the_rest`
  - `with_every_item_reserved_drop_oldest_accepts_one_over_the_bound`
- [ ] Queue tests:
  - `peek_at_never_waits_on_an_empty_open_queue`
  - `drop_oldest_never_evicts_a_batch_reserved_by_peek_at`
  - `take_all_clears_every_reservation_peek_at_made`
- [ ] Disk tests:
  - `peek_at_reads_ahead_across_a_segment_boundary_and_commit_advances_one_record_at_a_time`
  - `peek_at_follows_the_writer_after_the_active_segment_it_caught_up_to_rotates_away`
  - `a_peek_at_cancelled_mid_read_caches_nothing_and_the_next_one_reads_the_same_record`
  - `peek_at_past_corruption_stops_the_read_ahead_until_the_corrupt_span_is_the_head`
  - `drop_oldest_drops_the_newest_while_any_record_is_read_ahead`
  - `make_room_never_rotates_a_spool_with_a_record_read_ahead`
  - `finish_with_records_read_ahead_replays_all_of_them_on_reopen`
- [ ] Pins: `disk_queue_push_one_batch` (33), `disk_queue_peek_cached_costs_nothing` (0), and
  `drain_inbox_single_consumer_owned_batch_costs_exactly_the_arc` (1) expected unchanged. New:
  `disk_queue_peek_at_cached_costs_nothing` (0) and `sink_queue_peek_at_costs_nothing` (0), with
  rows in `docs/design/memory.md` and a note that a spooled `logit_out` holds up to `window`
  decoded batches in `read_ahead`.

### W2: trait and write loop

Files: `crates/logit-pipeline/src/output.rs`, `crates/logit-pipeline/src/runtime.rs`,
`docs/design/pipeline-graph.md`.

- [ ] `Output::window`, `Output::submit`, and `Output::await_ack`, with defaults that keep every
  other sink on `send`.
- [ ] `write_loop` tracks `outstanding` and `observed`; `observe_batch` runs once per batch,
  before its first submission.
- [ ] The fast path: `outstanding == 0 && output.window() <= 1` runs `deliver_with_retry`
  unchanged.
- [ ] `deliver_window`: fill to `min(output.window(), store.max_in_flight())`, the head-only
  classification of a submit `Err`, the unclassified submit `Err` past the head, the
  `at_most_once` whole-window drop on `Ambiguous`, and the grace rules.
- [ ] A `deliver_window` row in `docs/design/pipeline-graph.md`'s "Cancellation points" table.
- [ ] Tests in `runtime.rs`, with a scripted `WindowedOutput` double, `start_paused`, and
  observables, not sleeps:
  - `a_window_has_every_batch_submitted_before_the_first_ack`
  - `a_window_of_one_calls_send_and_never_submit`
  - `observe_batch_runs_once_per_batch_under_a_window_across_resubmits`
  - `an_ambiguous_await_mid_window_under_at_least_once_resubmits_every_unacked_batch_in_order`
  - `an_ambiguous_await_mid_window_under_at_most_once_drops_and_counts_every_unacked_batch`
  - `a_clean_await_mid_window_resubmits_under_both_postures`
  - `a_submit_failure_past_the_head_stops_the_fill_and_drains_the_acks_already_owed`
  - `a_permanent_submit_past_the_head_drops_that_batch_only_once_it_is_the_head`
  - `the_window_never_exceeds_a_memory_stores_max_batches`
  - `a_budget_exhausted_head_drops_only_the_head_under_at_least_once`
  - `a_grace_cut_with_a_window_in_flight_counts_every_outstanding_batch_under_at_most_once_and_leaves_them_under_at_least_once`
  - `every_run_output_exit_path_reconciles_with_a_window_in_flight` (the existing matrix, with the
    windowed double)

### W3: `logit_out`, `logit_in`, config, schema, and telemetry docs

Files: `crates/logit-outputs/src/logit.rs`, `crates/logit-outputs/src/stream.rs`,
`crates/logit-inputs/src/logit.rs`, `crates/logit-config/src/lib.rs`,
`crates/logit-pipeline/src/graph.rs` (rule 74), `crates/logit-cli/src/pipeline.rs`,
`schema/logit.schema.json`, `docs/design/internal-telemetry.md`,
`docs/design/pipeline-graph.md`.

- [ ] `LogitOut.window: u32`, default 32 (`default_logit_out_window`), with its operator doc.
  Exhaustive `LogitOut {..}` patterns gain the field (in `lib.rs`, `graph.rs`, and
  `pipeline.rs`), and `conn_over` gains the new connection fields.
- [ ] Graph rule 74, `1 <= window <= 1024`, with a row in `docs/design/pipeline-graph.md`.
- [ ] `crates/logit-cli/src/pipeline.rs` passes `.with_window(*window)`; `script/schema`
  regenerates `schema/logit.schema.json`.
- [ ] `logit_out`: `Conn` gains `window`, `in_flight`, and `broken`; `submit`, `await_ack`, and
  `send` as `submit` then `await_ack`; the probe only at `in_flight == 0`; `Dial.nodelay`.
- [ ] `logit_out` counters: `submit` counts its `Err`s in `logit.output.requests`, `await_ack`
  every result; `logit.output.ack.duration` per await; gauges `logit.output.in_flight` and
  `logit.output.window`, recorded in `docs/design/internal-telemetry.md`.
- [ ] `logit_in`: `RECEIVER_MAX_WINDOW = 1024`, the clamp in `handshake`, `set_nodelay` on
  accept, and `close_lingering(stream, bound)` after the `Fanout` clone is dropped.
- [ ] `logit_out` tests:
  - `a_negotiated_window_puts_several_frames_on_the_wire_before_the_first_ack`
  - `a_peer_answering_window_one_keeps_one_frame_in_flight`
  - `a_hello_ack_window_of_zero_is_read_as_one`
  - `a_going_away_after_some_acks_is_clean_for_every_unanswered_frame`
  - `an_eof_mid_window_is_ambiguous_and_the_next_submit_reconnects`
  - `a_write_failure_with_frames_in_flight_still_reads_the_acks_already_sent`
  - `a_submit_whose_in_flight_disagrees_with_the_connection_is_ambiguous`
  - `a_cancelled_await_ack_drops_the_connection_and_its_window`
  - `a_window_over_tls_reads_acks_buffered_behind_several_frames`
  - `a_window_against_logit_in_delivers_every_batch_once_in_order`
  - `a_window_resent_after_a_dropped_connection_is_forwarded_once`
- [ ] `logit_in` tests:
  - `hello_ack_answers_the_offered_window_clamped_to_the_receiver_maximum`
  - `pipelined_frames_are_acked_in_frame_order`
  - `a_shutdown_with_frames_still_buffered_reaches_the_peer_as_going_away_not_a_reset`
  - `a_frame_no_consumer_took_mid_window_is_answered_going_away_and_nothing_after_it_is_forwarded`
  - `the_graph_closes_while_a_connection_lingers_after_going_away`
- [ ] Config and graph tests:
  - `logit_out_window_defaults_to_32`
  - `a_logit_out_window_of_zero_or_past_1024_is_rejected`
- [ ] Integration tests in `crates/logit-cli/tests/logit_round_trip.rs`:
  - a real pair through `run_output` with `window: 32` and a disk spool: N batches forwarded
    once, in order;
  - a listener restarted mid-stream with the same `logit_in` table: resends counted, nothing
    forwarded twice.

### W4: operator docs, known gaps, and measurements

- [ ] Rewrite every passage that says one frame is in flight, that `window` stays 1, or that
  `logit_in` answers the one frame outstanding:
  - module docs of `crates/logit-inputs/src/logit.rs` and `crates/logit-outputs/src/logit.rs`;
    the docs in `output.rs`, `runtime.rs`, `buffer.rs`, and `disk_queue.rs`; the
    `request_timeout` field doc;
  - `docs/design/wire-protocol.md`: the connection section, "Flow control", and "Buffering";
  - `docs/deploying.md`, forwarding section: the `window:` field, the posture note, the
    "sequence is never a credit" bullet, and the closing paragraph;
  - `docs/known-gaps.md`: close "Credit-based flow control (`window` > 1)" and "No
    out-of-order/credit-based acknowledgement"; amend "A forward parked past the sender's ack
    timeout can be forwarded twice" and the `ack_write_stalled` reasoning under "`logit_in`'s
    `idle_timeout` bounds reads only";
  - amendments to ADRs `sink-send-path-and-attempt-accounting`, `buffered-sink-delivery`, and
    `disk-backed-sink-buffer`;
  - `docs/plans/native-transport.md`, `docs/plans/buffered-sink-delivery.md`, and the
    `docs/plans/delivery-semantics.md` non-goal;
  - `docs/design/memory.md`;
  - `AGENTS.md`: the `logit_out` row's ADR list and the delivery bullet.
- [ ] Sweep every comment line the stream added for the banned words.
- [ ] `script/cibuild` at the stack's tip, from a private `CARGO_TARGET_DIR`.
- [ ] Record the measurements in "Findings" below, not in `docs/design/performance.md`, which
  holds VM numbers only.

## Verification

- `script/check` on each PR; `script/cibuild` at the stack's tip from a private
  `CARGO_TARGET_DIR`.
- Round trip (W3's integration tests): with `window: 32`, N batches are forwarded once, in
  order. With faults mid-window (a peer close, `GOING_AWAY`, an ack timeout), every batch is
  forwarded once under `at_least_once`, and `logit.input.batches.resends` counts the resent
  prefix. Under `at_most_once` the window is dropped and counted.
- Loopback: `script/perf run --scenario native-relay` before and after, on a development
  machine, recorded as laptop numbers.
- Latency: the release binary in a throwaway container with `--cap-add NET_ADMIN`, `iproute2`
  installed in it (no shipped image has it), and `tc qdisc add dev lo root netem delay 5ms`,
  running `native-relay` at `window: 1` and `window: 32`. The expected gain is about 30 times on
  batch rate at a 10 ms RTT.
- `script/validate` and `every_shipped_config_loads_and_validates` still pass. No shipped config
  sets `window`, so the default covers them.

## Findings

W4 records here:

- Laptop loopback: `native-relay` before and after.
- Container `netem`: `native-relay` at `window: 1` and `window: 32` with 5 ms of delay each way.
- Perf VM: owed, with the pending re-baseline.
