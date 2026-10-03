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

Each is one PR. W0 is docs only, so W1 branches from `main` beside it; W2, W3, and W4 each
branch from the one before.

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

- [x] `Buffer::peek_at(&mut self, n) -> Option<&T>`; `InMemoryBuffer.head_reserved: bool` becomes
  `reserved: usize` (`peek` is `peek_at(0)`, `commit` decrements saturating, eviction starts at
  `reserved`, invariant `reserved <= len`).
- [x] `BoundedQueue::peek_at` (synchronous, one lock), `SinkQueue::peek_at`, `max_in_flight`.
- [x] `SinkStore::peek_at` (`async`, never waits for a push) and `SinkStore::max_in_flight`
  (memory `max_batches.max(1)`, disk `usize::MAX`).
- [x] `DiskQueue.head_cache` becomes `read_ahead: VecDeque<ReadAhead { batch, ctx, seq, seg,
  offset, len }>`, with a next-segment helper that mirrors `roll_read_cursor` without unlinking.
  Every `head_cache` check (`push`'s make-room `nothing_queued`, the `DropOldest` to `DropNewest`
  arm, `evict_oldest`, `skip_corrupt`) becomes a `read_ahead` check.
- [x] `queue_stress.rs` gains a `PeekAt` consumer op.
- [x] `disk_queue_verification.rs`: `spool_op()` gains `PeekAt(n)`; the model checks FIFO
  first-commits and replay of uncommitted records.
- [x] Buffer tests:
  - `peek_at_reserves_every_item_up_to_n_and_drop_oldest_evicts_past_them`
  - `peek_at_past_the_end_returns_none_and_reserves_nothing_new`
  - `commit_releases_one_reservation_and_leaves_the_rest`
  - `with_every_item_reserved_drop_oldest_accepts_one_over_the_bound`
- [x] Queue tests:
  - `peek_at_never_waits_on_an_empty_open_queue`
  - `drop_oldest_never_evicts_a_batch_reserved_by_peek_at`
  - `take_all_clears_every_reservation_peek_at_made`
- [x] Disk tests:
  - `peek_at_reads_ahead_across_a_segment_boundary_and_commit_advances_one_record_at_a_time`
  - `peek_at_follows_the_writer_after_the_active_segment_it_caught_up_to_rotates_away`
  - `a_peek_at_cancelled_mid_read_caches_nothing_and_the_next_one_reads_the_same_record`
  - `peek_at_past_corruption_stops_the_read_ahead_until_the_corrupt_span_is_the_head`
  - `drop_oldest_drops_the_newest_while_any_record_is_read_ahead`
  - `make_room_never_rotates_a_spool_with_a_record_read_ahead`
  - `finish_with_records_read_ahead_replays_all_of_them_on_reopen`
- [x] Pins: `disk_queue_push_one_batch` (33), `disk_queue_peek_cached_costs_nothing` (0), and
  `drain_inbox_single_consumer_owned_batch_costs_exactly_the_arc` (1) expected unchanged. New:
  `disk_queue_peek_at_cached_costs_nothing` (0) and `sink_queue_peek_at_costs_nothing` (0), with
  rows in `docs/design/memory.md` and a note that a spooled `logit_out` holds up to `window`
  decoded batches in `read_ahead`.

### W2: trait and write loop

Files: `crates/logit-pipeline/src/output.rs`, `crates/logit-pipeline/src/runtime.rs`,
`docs/design/pipeline-graph.md`.

- [x] `Output::window`, `Output::submit`, and `Output::await_ack`, with defaults that keep every
  other sink on `send`.
- [x] `write_loop` tracks `outstanding` and `observed`; `observe_batch` runs once per batch,
  before its first submission.
- [x] The fast path: `outstanding == 0 && observed == 0 && output.window() <= 1` runs `deliver_with_retry`
  unchanged.
- [x] `deliver_window`: fill to `min(output.window(), store.max_in_flight())`, the head-only
  classification of a submit `Err`, the unclassified submit `Err` past the head, the
  `at_most_once` whole-window drop on `Ambiguous`, and the grace rules. The head's submit runs
  under the head's remaining attempt time; a submit past the head and every `await_ack` run
  under no attempt time (the sink bounds them), and the budget decides only whether a failed
  round is retried.
- [x] A `deliver_window` row in `docs/design/pipeline-graph.md`'s "Cancellation points" table.
- [x] Tests in `runtime.rs`, with a scripted `WindowedOutput` double, `start_paused`, and
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
  - `a_round_that_outlasts_the_budget_still_delivers_a_head_whose_ack_arrived`
  - `a_grace_cut_with_a_window_in_flight_counts_every_outstanding_batch_under_at_most_once_and_leaves_them_under_at_least_once`
  - `every_run_output_exit_path_reconciles_with_a_window_in_flight` (the existing matrix, with the
    windowed double)

### W3: `logit_out`, `logit_in`, config, schema, and telemetry docs

Files: `crates/logit-outputs/src/logit.rs`, `crates/logit-outputs/src/stream.rs`,
`crates/logit-inputs/src/logit.rs`, `crates/logit-config/src/lib.rs`,
`crates/logit-pipeline/src/graph.rs` (rule 75), `crates/logit-cli/src/pipeline.rs`,
`schema/logit.schema.json`, `docs/design/internal-telemetry.md`,
`docs/design/pipeline-graph.md`.

- [x] `LogitOut.window: u32`, default 32 (`default_logit_out_window`), with its operator doc.
  Exhaustive `LogitOut {..}` patterns gain the field (in `lib.rs`, `graph.rs`, and
  `pipeline.rs`), and `conn_over` gains the new connection fields.
- [x] Graph rule 75, `1 <= window <= 1024`, with a row in `docs/design/pipeline-graph.md`.
- [x] `crates/logit-cli/src/pipeline.rs` passes `.with_window(*window)`; `script/schema`
  regenerates `schema/logit.schema.json`.
- [x] `logit_out`: `Conn` gains `window`, `in_flight`, and `broken`; `submit`, `await_ack`, and
  `send` as `submit` then `await_ack`; with frames in flight the frame written in chunks under
  a per-chunk progress bound of `request_timeout`, a stall marking the connection `broken`;
  the probe only at `in_flight == 0`; `Dial.nodelay`.
- [x] `logit_out` counters: `submit` counts its `Err`s in `logit.output.requests`, `await_ack`
  every result; `logit.output.ack.duration` per await; gauges `logit.output.in_flight` and
  `logit.output.window`, recorded in `docs/design/internal-telemetry.md`.
- [x] `logit_in`: `RECEIVER_MAX_WINDOW = 1024`, the clamp in `handshake`, `set_nodelay` on
  accept, and `close_lingering(stream, bound)` after the `Fanout` clone is dropped.
- [x] `logit_out` tests:
  - `a_negotiated_window_puts_several_frames_on_the_wire_before_the_first_ack`
  - `a_peer_answering_window_one_keeps_one_frame_in_flight`
  - `a_hello_ack_window_of_zero_is_read_as_one`
  - `a_going_away_after_some_acks_is_clean_for_every_unanswered_frame`
  - `an_eof_mid_window_is_ambiguous_and_the_next_submit_reconnects`
  - `a_write_failure_with_frames_in_flight_still_reads_the_acks_already_sent`
  - `a_write_that_stalls_with_frames_in_flight_still_reads_the_acks_already_sent_and_times_out_only_the_parked_frame`
  - `a_submit_whose_in_flight_disagrees_with_the_connection_is_ambiguous`
  - `a_cancelled_await_ack_drops_the_connection_and_its_window`
  - `a_window_over_tls_reads_acks_buffered_behind_several_frames`
  - `a_window_against_logit_in_delivers_every_batch_once_in_order`
  - `a_window_resent_after_a_dropped_connection_is_forwarded_once`
- [x] `logit_in` tests:
  - `hello_ack_answers_the_offered_window_clamped_to_the_receiver_maximum`
  - `pipelined_frames_are_acked_in_frame_order`
  - `a_shutdown_with_frames_still_buffered_reaches_the_peer_as_going_away_not_a_reset`
  - `a_frame_no_consumer_took_mid_window_is_answered_going_away_and_nothing_after_it_is_forwarded`
  - `the_graph_closes_while_a_connection_lingers_after_going_away`
- [x] Config and graph tests:
  - `logit_out_window_defaults_to_32`
  - `a_logit_out_window_of_zero_or_past_1024_is_rejected`
- [x] Integration tests in `crates/logit-cli/tests/logit_round_trip.rs`:
  - a real pair through `run_output` with `window: 32` and a disk spool: N batches forwarded
    once, in order;
  - a listener restarted mid-stream with the same `logit_in` table: resends counted, nothing
    forwarded twice.

### W4: operator docs, known gaps, and measurements

- [x] Rewrite every passage that says one frame is in flight, that `window` stays 1, or that
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
- [x] Sweep every comment line the stream added for the banned words.
- [x] `script/cibuild` at the stack's tip, from a private `CARGO_TARGET_DIR`.
- [x] Record the measurements in "Findings" below, not in `docs/design/performance.md`, which
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

Laptop numbers, not the perf VM: AMD Ryzen AI 9 HX 370 (24 logical CPUs, mixed Zen 5 and Zen 5c
cores), 62 GiB, Linux 7.2.7, on battery under the `powersave` governor, runs unpinned, the
harness run inside the dev container through a bind-mounted private target dir. Binaries: the
`flow/w4` tip including 847c912c, and `main` at 958b93e7. `generate_in` batches are 100 events,
so batches/s is events/s divided by 100. The harness's wall time ends at "generation complete",
so up to one queue of batches is still undelivered; that overstates window 1 by about 3% under
`netem` and every loopback arm by about 1.5%.

**Loopback** (`native-relay`, 7M events, `--repeat 3`, medians):

| Arm | Events/s | CPU µs/event | Peak RSS |
|---|---|---|---|
| `main`, one frame in flight | 688,235 | 1.829 | 130.0 MiB |
| `flow/w4`, `window: 1` (`native-relay-window1`) | 628,860 | 1.968 | 143.9 MiB |
| `flow/w4`, default `window: 32` (`native-relay`) | 876,336 | 1.725 | 230.5 MiB |

- Window 32 against window 1 on the same binary: 1.39× events/s and 12% less CPU per event.
- Window 32 against `main`: 1.27× events/s.
- Window 1 against `main`: 0.91×, inside this box's spread (`main`'s own repeats span ±7%);
  not called a regression without VM numbers.
- Peak RSS rises with the window (230 MiB against 130–144 MiB), consistent with batches held
  along `logit_in`'s path and in the sink queue rather than one at a time; not attributed.

**Latency** (`tc qdisc add dev lo root netem delay 5ms` in a throwaway `logit-dev:local`
container with `--cap-add NET_ADMIN`; measured RTT 10.1/11.9/15.3 ms min/avg/max; `flow/w4`,
`--repeat 2`, `--settle 2s`; temporary copies of `native-relay` with `buffer.max_batches: 64`
so the queue left at "generation complete" drains inside the settle at window 1; 3M events at
window 32 and 200k at window 1):

| Arm | Events/s | Batches/s | CPU µs/event | Peak RSS |
|---|---|---|---|---|
| `window: 32` | 312,734 | ~3,127 | 1.811 | 60.7 MiB |
| `window: 1` | 9,133 | ~91 | 5.724 | 42.3 MiB |

- 34.2× on batch rate at a 10 ms RTT, against the record's expected ~30×. Window 32 reaches
  about 98% of its ceiling of 32 frames per RTT (3,200 batches/s); window 1 about 91% of its one
  per RTT (100 batches/s). The window-1 arm's CPU per event is inflated by idle wake-ups per
  batch.

**The perf VM (2026-10-02).** The VM numbers are in
[`docs/design/performance.md`](../design/performance.md) §1's native-relay ladder. At `99615d2c`
(`flow/w4`), `window: 32` costs 1.176 µs/event and runs 1,451,784 events/s, and `window: 1` costs
1.371 and runs 988,553. `window: 1` is within 1.6% on CPU of `958b93e7`, the last binary before the
window (1.350 µs/event, 1,006,348 events/s), under the 5% gate, so the window-1 regression the
laptop couldn't rule out doesn't show. Peak RSS rises with the window there too, 250.2 MiB at
window 1 against 273.4 at window 32. The VM didn't re-run the `netem` window 1 against window 32
comparison; the laptop figures above stand for it.
