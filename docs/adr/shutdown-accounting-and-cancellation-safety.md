---
created: 2026-09-26
updated: 2026-09-26
---

# Shutdown accounting and cancellation safety: every shutdown loss counted, and one table of cancellation points

## Status
Accepted

## Context

`docs/plans/critical-sections-inventory.md` groups eleven entries as cluster 3, "Shared queue +
shutdown":

- NET-02, NET-03, NET-06, and NET-07 cover the UDP read and decode loops and `BoundedQueue`.
- RT-02, RT-03, RT-04, and RT-07 cover runtime shutdown, `run_output`, `write_loop`, and
  `SinkQueue`.
- DISK-08 covers `DiskQueue`'s `Notify`/`closed` protocol.
- TAIL-06 and TAIL-08 cover the tail driver's shutdown and its `select!`.

Read-only passes over those entries, against tokio 1.53.1's source, found the queue protocol
itself sound. Every wait loop creates its `Notified` before it checks state, no lock is held
across an await, every access to `closed` is Acquire/Release, `pop_many` awaits only on an
iteration that removed nothing, and `close()`'s use of `notify_waiters` matches tokio 1.53.1. What
was missing was executable evidence under real concurrency, and the shutdown accounting had gaps:

- **Shutdown drops that `drain complete` doesn't see (RT-02).** The `drain complete` log's
  `batches_dropped` field reads `shutdown_dropped_batches`, which only `run_output`'s Memory sweep
  adds to. `finish_and_flush`'s drops and `revoke_lua_io`'s drops count
  `batches.dropped{reason="shutdown"}` but never reach it, so the line can log at `info` after a
  thousand batches were dropped.
- **Sweep-taken batches never counted as received (RT-03).** The abandoned-inbox sweep takes
  batches with `inbox.try_recv()` and counts them dropped, but not `batches.received` or
  `events.received`, so a sink can report more dropped than received.
- **A listener's error lost to the backstop (RT-02).** `run_input`'s `select!` is unbiased. When
  a draining input returns `Err` in the same poll as the grace backstop, the backstop can win and
  the error is discarded.
- **A reservation left standing by a grace cut (RT-04).** The inventory's mechanism, a `peek`
  reservation left when grace wins the `NextBatch` race, doesn't exist: `select!` returns on the
  first `Ready` branch, and `peek` reserves only in the poll that returns. A reservation is left
  by another route: `DeliverStep::ShutdownExpired` returns with the peeked head uncommitted. It's
  benign (`SinkStore::finish` commits it; a disk store persists the cursor at the head), but no
  comment argues it.
- **A grace-cut send leaves no trace (RT-04).** A `write_loop` whose in-flight `Output::send` is
  cut by the grace records no fault and leaves the batch uncommitted. A disk sink under
  `delivery: at_most_once` then replays, on the next start, a batch the destination may have
  taken.
- **Unchecked weight sums (NET-06, RT-07).** `BoundedQueue::would_overflow`,
  `InMemoryBuffer::would_overflow`, and `InMemoryBuffer::push` add `u64` weights without a
  saturating add.
- **`read_loop`'s `queue.close()` is a trailing statement (NET-02).** Every path reaches it today,
  but nothing stops a future edit from adding an early return that skips it.
- **Dead `shutdown_grace` copies (TAIL-06).** `TailBatching::shutdown_grace`,
  `UdpListenerConfig::shutdown_grace`, and `TcpListenerConfig::shutdown_grace` are set by
  `logit-cli::pipeline` and read by nothing. The copy the runtime enforces is
  `InputRuntimeConfig::shutdown_grace`.
- **Timer starvation under backlog (TAIL-08).** `Tailer::drain` loops while any file makes
  progress and re-checks timers only at the `select!`. A sustained backlog against a slow
  downstream can starve the poll, flush, and checkpoint ticks. No test covers it.
- **A wrong `!Send` claim (NET-02).** `tcp.rs`'s `read_step` doc says `wait_for`'s `Ref` makes
  the future `!Send`. Its arms don't await while holding the `Ref`, so the reason is wrong, though
  `changed()` may still be the better call, because `wait_for` takes the value's read lock on
  every call.
- **UDP shutdown losses wider than the ADR names (NET-02, NET-03).** [ADR
  `udp-intake-batching-and-socket-visibility`](udp-intake-batching-and-socket-visibility.md) names
  two uncounted losses: a cancelled `push_many`'s remainder, and what `decode_loop` popped but
  didn't decode. When the grace backstop drops a UDP listener's `drive`, it also loses the
  datagrams still in the `ReceiveQueue`, the events in the `BatchAccumulator`, and the batch
  parked in `emit`'s `Fanout::send`. None of them are counted.
- **A send that lands after the sweep (RT-03).** `run_output` never closes `inbox`. After the
  sweep's last `try_recv`, a producer parked on the full channel, such as the close-time flush of
  an upstream `aggregate` or Lua node, can complete its send while `finish_and_flush` awaits
  `SinkStore::finish` and `output.flush()`. That batch dies with the `Receiver`, neither received
  nor dropped.
- **No sink `delivered` counter (RT-03).** Without one, `received == delivered + dropped +
  spooled` can't be checked from telemetry. The one reconciliation test counts deliveries in a
  test double.
- **An untested rotation failure (DISK-08).** `DiskQueue`'s invariant that `segments` is never
  empty holds at all three mutation sites, and two rotation-failure tests exist. What no test
  covers is a make-room rotation whose `create` fails with no leftover file: `push` then falls
  through to an over-bound write under `overflow: block`.
- **A stale gauge (RT-07).** `BoundedQueue::update_gauges` runs outside the lock, so with truly
  parallel callers the last gauge written can be stale. It's benign in production, because both
  halves of every queue run in one task.

## Decision

1. **Shutdown has an accounting contract, and any unexplained remainder is a bug.**
   - Per sink: `logit.component.batches.received == batches.delivered + Σ batches.dropped{reason}
     + batches still spooled`, and the same for events. "Received" includes every batch the
     abandoned-inbox sweep takes from `inbox`. It doesn't recount the `in_hand` batch, which
     `drain_inbox` counted as received before it parked on the store.
   - Per UDP listener: `logit.input.datagrams == datagrams decoded + Σ
     logit.component.datagrams.dropped{reason}`. "Decoded" means every datagram yielded to the
     decoder, including one `decode_into` rejects. Each gets one
     `logit.component.receive.latency` sample; a decode error is a throttled diagnostic, not a
     counter. `logit.input.datagrams.truncated` isn't a drop and never appears in the sum.
   - Four losses are named exceptions, left uncounted and to be listed in `docs/known-gaps.md`:
     - A UDP listener's decoded events that the grace backstop drops, either held in the
       `BatchAccumulator` or parked in `emit`'s `Fanout::send`. Their datagrams already count as
       decoded, so the datagram contract still holds.
     - A batch cut off mid-`Fanout::deliver` at the grace backstop. It reaches a prefix of its
       consumers and counts as `sent` and `receive.flushed`, but never as a drop.
     - A batch sent into a revoked Lua inbox by a permit holder still blocked after
       `REVOKE_DRAIN_TIMEOUT`.
     - A batch sent into a closed sink inbox (decision 9) by a permit holder still blocked when
       the sweep's bound runs out.
2. **`drain complete`'s `batches_dropped` is the sum of every `reason="shutdown"` batch drop at a
   sink or Lua boundary.** One helper, `count_shutdown_drop`, will be the only site that counts
   `batches.dropped`/`events.dropped{reason="shutdown"}` and the only site that adds to
   `shutdown_dropped_batches`. Its callers will be the `run_output` sweep, `finish_and_flush`,
   `write_loop`, and `revoke_lua_io`. `revoke_lua_io` has two callers: `watch_lua_thread`, on a
   wedge at shutdown, and `run_lua`'s Lua OS thread on the `max_memory` failure path, through
   `sweep_runtime.block_on(revoke_lua_io(..))`. So the counter reaches the Lua thread too. A
   `max_memory` failure can land before the drain or after it has begun (`run_lua_loop`'s
   `flush_now` and `check_at_close` verdicts). Either way the revoked inbox's contents are
   dropped because the node is leaving the graph, on or after the drain's start, so the label
   stays `shutdown`.

   The field doesn't include events a `Fanout` drops as `closed_consumer`, UDP datagram drops (a
   different unit), overflow evictions during the drain, or the disk sweep's push failures
   (`frame_too_large`, `disk_full`, `disk_io_error`). `docs/deploying.md` will say so.
3. **A send cut off by the shutdown grace is `Fault::Ambiguous`.** The destination may have
   received it, and the existing `is_retryable(Fault::Ambiguous, posture)` table decides what
   happens:
   - Under `at_least_once`, the batch stays uncommitted. Replay is that posture's contract.
   - Under `at_most_once`, the batch is committed and counted `dropped{reason="shutdown"}`, and
     the sink span is tagged `fault=ambiguous`.

   Today the send is cut when `write_loop`'s `select!` lets `shutdown_grace_expired` win and
   drops `deliver_with_retry`. `write_loop` will track whether a send is in flight with a flag set
   immediately before the `timeout(remaining, output.send(batch))` await and cleared as soon as it
   returns. A grace
   expiry while the flag is clear leaves the batch uncommitted under either posture, as for a
   grace that lands during a backoff sleep. `deliver_with_retry` also checks the anchored
   deadline before every attempt and starts none once it has passed (`Delivery::GraceExpired`),
   so a send never starts after the deadline to be read as cut off.
4. **The UDP shutdown remainders are counted** as `datagrams.dropped` and
   `bytes.dropped{reason="shutdown"}`. There are four:
   - **A cancelled `push_many`'s remainder.** A new `CountedDrain` wrapper around a `Vec`'s drain
     will count, when dropped, whatever it never yielded. It will call only `Telemetry::count`,
     never the queue lock. `push_many` will iterate through it.
   - **A batch `push_many` never started.** `push_many` is an `async fn`, so its drain exists only
     after its first poll. In `read_loop`'s second `select!`, `queue.push_many(&mut batch)` against
     `shutdown.wait_for`, `wait_for` is `Ready` on its first poll once shutdown is set, and
     `select!` starts at a random branch. About half the time `push_many` is never polled, and the
     whole batch stays in `read_loop`'s `Vec`, dropped uncounted on return. `read_loop`'s shutdown
     arm will count whatever `batch` still holds. A polled-then-cancelled `push_many` leaves the
     `Vec` empty and a never-polled one leaves it full, so this count is exact and never overlaps
     `CountedDrain`'s.
   - **What `decode_loop` popped but didn't decode.** `decode_loop` will iterate its popped batch
     through a `CountedDrain`.
   - **The `ReceiveQueue` residual.** A guard declared in `drive` before `read` and `decode` drops
     after both halves' futures are gone. Its `Drop` closes the queue, drains it with `commit()`,
     and counts what it drained. `commit()` takes the queue's mutex, and taking it there is safe:
     no other holder of that queue exists by then, the mutex is never held across an await
     anywhere, and every lock site swallows poisoning.

   Each of these counts is recorded at the grace backstop or during the drain, after `internal`'s
   final drain has run, so it never reaches an exported pipeline. Each guard that counts a
   nonzero remainder will also log a self-log `diag.warn` naming the listener and the count.
5. **Queue verification is a multi-thread randomized stress harness, a sequential proptest, and
   pins against tokio's source.** The stress harness will run `BoundedQueue` and `DiskQueue` on a
   four-worker multi-thread runtime with random producers, consumers, cancellations, and close
   timing, seeded so a failure replays. The proptest will check that batched and single-item calls
   agree under close and cancellation. Unit tests will pin the tokio behavior the queues rely on.
   On a tokio bump, re-check these internals:
   - `sync/notify.rs`: `notify_waiters` wakes only waiters whose `Notified` was created before the
     call, and a registered `Notified` that is assigned a `notify_one` and dropped before
     observing it passes the permit on (`drop_notified`). A `Notified` that consumed a stored
     permit has reached `Done` and forwards nothing.
   - `macros/select.rs`: `select!` returns on the first branch that is `Ready` and drops the rest.
   - `sync/watch.rs`: `wait_for`'s behavior when its future is dropped and re-created.
6. **Grace races prefer the node's own outcome, and a grace arm can't be starved.**
   - `run_input`'s `select!` will be `biased`, with the input's arm first, so a listener's `Err`
     that is ready in the same poll as the backstop is returned, not discarded.
   - `write_loop`'s `DeliverStep` `select!` will be `biased`, with the deliver arm first, so a send
     that completed in the same wake is counted delivered, not ambiguous.
   - Every grace arm, `run_input`'s and both of `write_loop`'s, will be wrapped in
     `tokio::task::unconstrained`. `Sleep::poll_elapsed` and `wait_for` spend the task's coop
     budget, so an input or a delivery that exhausts the budget on every poll would otherwise
     defer the backstop.

   `shutdown_grace_expired` anchors its deadline at the first poll of a grace arm after the
   signal, not at the signal itself. `unconstrained` keeps that first poll within a few wakes of
   the signal, not one: `run_output`'s outer `select!` is unbiased, and `drain_inbox` can spend
   the coop budget before `write_loop` is polled.
7. **A listener's shutdown grace is enforced only by the runtime.**
   `InputRuntimeConfig::shutdown_grace`, read by `run_input`, is the one copy. The three
   per-listener copies (`TailBatching`, `UdpListenerConfig`, `TcpListenerConfig`) will be removed
   with no alias.
8. **`docs/design/pipeline-graph.md`'s "Cancellation points" table is the canonical list of every
   production `select!` and `timeout` on a node's run path.** Each row names the site, its arms,
   what a losing arm drops, and why nothing is lost or what counts it. A new `select!` or
   `timeout` on a run path adds its row in the same PR. Comments at the site point at the table
   and don't repeat it.
9. **`run_output` closes its inbox before the sweep.** It will call `inbox.close()` first, as
   `revoke_lua_io` does, so a later send fails upstream as `closed_consumer` instead of landing
   in a channel nobody reads. The sweep then drains with `recv` until `None`, under a short bound,
   so a send whose permit was reserved before the close still lands and is counted. Closing
   first fixes two things in [ADR `disk-backed-sink-buffer`](disk-backed-sink-buffer.md)'s
   "Shutdown":
   - The batch lost after the sweep contradicts its "the sweep … still drops nothing".
   - Its bound on spool overshoot, "bounded by the channel's fixed capacity", breaks today by a
     different mechanism: the inbox stays open while the sweep awaits each disk `store.push`, so
     producers refill it. A closed inbox can't refill.

## Alternatives considered

- **loom or shuttle.** Rejected. Neither can instrument `tokio::sync::Notify`, which every queue
  waits on, and tokio's own loom build is dev-only, not something a dependent crate can switch
  on. A model checker that can't see the primitive under test would check a stand-in.
- **A hand-rolled model of `Notify`.** Rejected. It would verify the model, not tokio 1.53.1.
  Pinning the tokio behaviors the queues use, in tests that run against the real crate, catches
  a change on the next bump. A model wouldn't.
- **A counting `Drop` inside `push_many` that re-acquires the queue lock.** Rejected. A live
  future may hold that lock, and the remainder can be counted without it: the undrained items are
  in the caller's `Drain`, and `Telemetry::count` takes only its own buffer's lock, which is never
  held across an await or while a `CountedDrain` drops. `impl Drop for Timer` already counts from
  `Drop` this way. The `ReceiveQueue` residual guard in decision 4 does take the queue lock, but
  only after every other user of the queue is gone.
- **A new `shutdown_ambiguous` drop reason.** Rejected. It splits one operator question ("what
  did shutdown drop?") across two reasons, and `drain complete` would have to sum both. The
  span's `fault=ambiguous` tag carries the "may have been received" signal.
- **Counting a grace-cut send as `reason="send_failed"`.** Rejected. No destination failure was
  observed. `send_failed` drives the `degraded` readiness edge and the permanent-failure streak,
  and `drain complete` sums shutdown drops, not send failures.
- **Leaving the shutdown remainders uncounted.** Rejected. [ADR
  `udp-intake-batching-and-socket-visibility`](udp-intake-batching-and-socket-visibility.md)
  accepted them as bounded and shutdown-only, and priced counting against the queue lock. The
  lock isn't needed for the cancelled remainder (see above), and the losses are wider than that
  ADR named, so an operator reconciling counters at shutdown couldn't tell an accepted loss from
  a bug.

## Consequences

- A new `select!` or `timeout` on a node's run path adds a row to `docs/design/pipeline-graph.md`'s
  "Cancellation points" table in the same PR. A reviewer can check that rule against the diff.
- A tokio version bump re-checks the three internals under decision 5. The pins fail if
  `Notify`'s permit forwarding changes.
- New telemetry:
  - `logit.component.batches.delivered` and `logit.component.events.delivered`, counted by a sink
    on each `Delivery::Delivered`.
  - `logit.component.datagrams.dropped` and `logit.component.bytes.dropped` gain
    `reason="shutdown"`.
- A disk-backed sink under `at_most_once` will be able to count
  `batches.dropped{reason="shutdown"}`, for a send the grace cut off. Today a disk sink never
  counts a shutdown drop.
- `drain complete` will log at `warn` whenever any `reason="shutdown"` batch drop happened at a
  sink or Lua boundary, from any of the four sites. Decision 2 lists what the field excludes, and
  `docs/deploying.md` will say the same.
- Removing the three per-listener `shutdown_grace` fields is a wide but mechanical diff across the
  struct literals in `logit-cli`'s tests. It isn't operator-visible: the operator's field for a
  listener's grace, `receive.shutdown_grace` (`ReceiveConfig`), doesn't change.
- The stress tests will run a small seed count in CI (64 for `BoundedQueue`, 16 for `DiskQueue`)
  under a 30 s timeout, so they don't flake on a loaded runner. Long runs will be `#[ignore]`d,
  and `LOGIT_QUEUE_STRESS_SEED` will replay one seed.
- `internal` can't export the counts decision 4 adds. Its `run_until_shutdown` does its final
  drain the instant the signal fires, and every one of those counts is recorded later, at the
  grace backstop or during the drain. They reach a test `Registry`, but not an exported pipeline.
  The self-log `diag.warn` is what an operator sees, and `docs/known-gaps.md` will record the gap.
- Out of this stream's scope, recorded for the sink cluster: `stdio_out`/`file_out` write in
  place with `write_all`, so when a send is cancelled mid-write and `flush()` then completes it,
  the file can hold a torn line or native frame.

## Running it

Each workstream updates its inventory rows in the PR that lands its artifact.

### `drain/w1`: queue protocol (NET-06, NET-07, RT-07, DISK-08)

`drain/w1` lands `CountedDrain`, which `BoundedQueue::push_many` now iterates, so a cancelled
call counts its remainder `items_dropped`/`units_dropped{reason="shutdown"}`. A `push_many`
future dropped before its first poll takes nothing, and the caller still holds every item. Weight
arithmetic in `BoundedQueue::would_overflow` and `InMemoryBuffer` saturates, on the add and the
subtract, so an empty queue always weighs 0. Saturating only the add would underflow: push
weights `u64::MAX` and 5, commit both, and `0 - 5` panics under the lock in a debug build, or wraps
in release and parks every later `Block` push on an empty queue.

It also documents three contracts at the code: `BoundedQueue::peek`/`commit` assume one consumer,
`BoundedQueue::update_gauges` can write a stale gauge under parallel callers, and `DiskQueue`'s
`commit` and `evict_oldest` are check-then-act pairs that are safe only with the producer and
consumer polled from one task. `DiskQueue`'s behavior doesn't change.

Run the long stress modes with:

```sh
script/test -p logit-pipeline --run-ignored only -E 'test(/queue_stress::.*long/)'
```

Replay one seed with `LOGIT_QUEUE_STRESS_SEED=N` (and `--run-ignored all` for a long-mode seed).
The thread schedule isn't replayed, only the scenario the seed picks.

Tests in `crates/logit-pipeline/src/queue_stress.rs`:

- `bounded_queue_under_random_producers_consumers_cancellation_and_close_loses_and_duplicates_nothing`
  (64 seeds in CI) runs one to three producers and one or two consumers as tasks on a four-worker
  runtime, under every overflow policy and bound shape, with random cancellations (including
  cancellers ready at once and futures never polled) and random close timing. Every handed item
  must be popped, still queued, counted dropped (`overflow_oldest`, `overflow_newest`,
  `shutdown`), or known not admitted, in items and units. No item appears twice, each consumer
  sees each producer's items in order, a `peek` consumer's `commit` removes the item it peeked,
  zero-drop runs drop nothing, and the gauges read 0 once the queue is drained.
- `bounded_queue_long_stress` (ignored) runs the same scenario over 20,000 seeds.
- `a_close_while_every_producer_and_consumer_is_parked_wakes_all_of_them` parks three `Block`
  pushers on a full queue, then `pop`, `pop_many`, and `peek` on an empty one, and checks that
  `close()` wakes each of them.
- `disk_queue_under_a_random_producer_consumer_and_close_loses_and_duplicates_nothing` (16 seeds
  in CI) joins one producer and one consumer as two futures in one task, under random spool
  bounds, segment sizes, policies, cancellations, and close timing, then finishes, reopens, and
  drains the spool. Every handed push that wasn't cut off is delivered, counted dropped, or
  replayed; delivery and replay together follow push order with nothing twice; and only the
  trailing cut-off push, whose write `finish` flushed, may reappear on reopen.
- `disk_queue_long_stress` (ignored) runs the same scenario over 1,000 seeds.

Tests in `crates/logit-pipeline/src/queue.rs`:

- `any_sequence_with_close_and_cancelled_calls_agrees_between_batched_and_single_calls` (proptest,
  256 cases) runs random sequences of `push`, `push_many`, `pop`, `pop_many`, and `close` under
  every policy and three bound shapes. Each call is polled once with a no-op waker and dropped if
  `Pending`, or dropped unpolled. Batched and single-item runs must pop and retain the same items
  and count the same overflow drops, and a batched run's `shutdown` count must equal what the
  single-item run never reached.
- `a_notify_one_delivered_to_a_registered_notified_that_is_dropped_unpolled_is_passed_on` and
  `a_notify_one_with_no_waiter_stores_one_permit_not_two` pin the tokio `Notify` behavior under
  decision 5.
- `a_cancelled_push_many_keeps_its_prefix_and_counts_its_remainder_as_shutdown_drops` checks the
  counted remainder, 2 items and their own units.
- `a_push_many_never_polled_before_shutdown_leaves_every_item_in_the_callers_vec_uncounted` pins
  that the caller counts what an unpolled call never took.
- `commit_after_saturated_weights_never_underflows_and_an_empty_queue_has_zero_weight` and
  `would_overflow_saturates_rather_than_wrapping_near_u64_max` cover the saturating arithmetic.
- `counted_drain_counts_only_what_it_never_yielded` counts a peeked-but-unyielded item and
  records nothing for an exhausted drain.

`crates/logit-proto/src/buffer.rs` adds
`saturated_weights_never_underflow_and_an_empty_buffer_weighs_nothing`, through `commit` and
eviction. `crates/logit-pipeline/src/disk_queue.rs` adds
`a_block_push_whose_make_room_rotation_fails_writes_over_bound_rather_than_parking`: a consumed,
full spool whose make-room create fails with no file left behind still completes a `Block` push,
the batch is readable, and `disk.errors{op="create"}` counts 1.

Each harness was checked against a planted bug: counting nothing on cancellation fails the
proptest and the `BoundedQueue` ledger; removing `push_many`'s pre-wait `notify_one` stops a
`BoundedQueue` seed within its 30 s timeout; and removing the `not_full` wake in
`DiskQueue::after_cursor_advance` stops a `DiskQueue` seed within its timeout.

### `drain/w2`: runtime shutdown accounting (RT-02, RT-03, RT-04)

`drain/w2` lands decisions 2, 3, 6, and 9 in `crates/logit-pipeline/src/runtime.rs`, and the new
`batches.delivered`/`events.delivered` counters. `deliver_with_retry` returns
`Delivery::GraceExpired`, starting no send, before any attempt that would begin at or past the
anchored grace deadline. `count_shutdown_drop` counts every
`reason="shutdown"` batch drop at `run_output`'s sweep, `finish_and_flush`, `write_loop`'s
grace-cut send, and both of `revoke_lua_io`'s callers. `run_output` closes its inbox before the
sweep and bounds its `recv` loop with `SWEEP_DRAIN_TIMEOUT` (250 ms); the sweep counts `received`
for each batch it takes from the inbox. The runtime's rows in `docs/design/pipeline-graph.md`'s
"Cancellation points" table land with it.

Tests in `runtime.rs`, on a paused clock unless noted:

- `every_run_output_exit_path_reconciles_received_against_delivered_dropped_and_spooled` covers
  five exit paths (the drain finishing first, a grace expiring during retries, a grace cutting off
  an in-flight send, a permanent-failure exit, and a clean close), under both postures and both
  stores. It reads `received` and `delivered` from telemetry and checks `received == delivered +
  Σ dropped + spooled`, that the `drain complete` total equals `dropped{reason="shutdown"}`, and
  that a disk sink drops for shutdown only a send the grace cut off under `at_most_once`.
- `a_send_cut_off_by_shutdown_grace_is_committed_and_counted_under_at_most_once` (memory and disk)
  checks one `shutdown` drop, an error span tagged `fault=ambiguous`, and no replay on reopen.
- `a_send_cut_off_by_shutdown_grace_stays_queued_for_replay_under_at_least_once` checks that a disk
  spool replays the batch and a memory store's `finish` counts it once.
- `a_head_left_reserved_by_a_grace_cut_delivery_is_dropped_and_counted_by_finish` checks that the
  reserved head and the batch behind it are each counted once.
- `a_grace_expiring_during_backoff_after_a_clean_failure_leaves_the_batch_uncommitted_under_at_most_once`
  checks that a grace landing in a backoff sleep commits nothing, counts nothing, and tags no
  fault. `shutdown_grace_expiry_ends_write_loop_promptly_leaving_the_remainder_for_run_output`
  passes unchanged.
- `a_send_that_completes_in_the_same_wake_as_the_grace_deadline_is_counted_delivered` resolves the
  send at the grace deadline, 16 times; unbiased, about half would read as cut off.
- `a_batch_queued_behind_a_send_that_completes_at_the_grace_deadline_is_not_started_and_stays_uncommitted`
  (memory and disk, at-most-once, 16 times) checks that the next batch starts no send: no
  `ambiguous` tag, a memory store's `shutdown` count comes only from `finish`, and a disk spool
  replays it.
- `a_backoff_ending_at_the_grace_deadline_does_not_start_another_attempt` ends a clean failure's
  backoff at the deadline and checks that no second attempt starts, nothing is tagged, and a disk
  spool replays the batch.
- `drain_complete_reports_every_batch_dropped_for_shutdown_including_those_finish_drops` runs a
  full pipeline into a sink that never delivers and checks the logged `batches_dropped` against
  the telemetry sum, with drops from the sweep, `finish`, and the `at_most_once` cut.
  `a_lua_node_over_max_memory_fails_the_run_as_runtime_naming_it` (multi-thread, real time) checks
  the same for a `max_memory` revoke.
- `a_batch_sent_into_the_inbox_after_the_sweep_began_is_counted_not_silently_lost` parks a
  producer on a one-slot inbox and holds the sink's `flush()` open, then checks that the producer's
  `batches.sent` equals the sink's `received` plus the sends refused upstream as
  `closed_consumer`.
- `an_input_that_burns_its_coop_budget_after_the_signal_is_still_cancelled_at_the_grace_deadline`
  runs an input that sends forever after the signal, advancing the clock by hand because a busy
  runtime never auto-advances, and bounds the wait so a regression fails.
- `an_input_error_at_the_grace_deadline_is_never_swallowed_by_the_backstop` returns an `Err` in the
  backstop's wake, 32 times, and expects every one.
- `a_shutdown_grace_expired_call_polled_after_the_signal_then_dropped_keeps_its_anchor` and
  `a_shutdown_grace_expired_call_never_polled_after_the_signal_anchors_nothing` pin the anchor at
  the first poll after the signal.

Each new ordering was checked against its removal: dropping `biased` from `run_input` fails the
input-error test, dropping it from the deliver `select!` fails the same-wake test, dropping
`inbox.close()` fails the late-send test, dropping `run_input`'s `unconstrained` fails the
coop-budget test, and dropping the pre-attempt deadline check fails both `GraceExpired` tests.

### `drain/w3`: UDP read and decode loops (NET-02, NET-03)

`drain/w3` lands decision 4 in `crates/logit-inputs/src/udp.rs`: every UDP shutdown remainder is
counted `datagrams.dropped`/`bytes.dropped{reason="shutdown"}`, so the per-listener datagram
contract in decision 1 holds on every exit. Three guards do the counting:

- `ReadHalf` owns `read_loop`'s batch and queue. Its `Drop` counts what the batch still holds and
  closes the queue, which covers the never-polled `push_many`, a `break Err`, and the read future
  dropped at the coop-budget yield between its read and its push. That yield is reachable under
  `receive.shutdown_grace: 0s`.
- `decode_loop` iterates its popped batch through a `CountedDrain`.
- `ResidualOnDrop`, declared in `UdpListener::drive` before both halves, closes the queue and
  counts what it holds through the new one-lock `BoundedQueue::take_all`. It makes no telemetry
  call on an empty queue.

Each guard logs a `warn` naming the count when it's nonzero, because `internal`'s final drain has
already run by then. `docs/known-gaps.md` records that gap and the event-level losses decision 1
names.

`decode_loop` and `tcp.rs`'s `serve_connection` now re-read the clock after an interval `emit`.
Before, an `emit` parked past the next deadline left that deadline already due, and the next pop
batch or read flushed again at once. The udp and tcp rows of `docs/design/pipeline-graph.md`'s
"Cancellation points" table land with it.

Tests in `udp.rs`, on real time unless noted:

- `shutdown_while_a_batch_is_mid_push_exits_promptly_and_closes_the_queue` now checks that the
  datagrams read equal those queued plus those counted dropped, in datagrams and bytes.
- `a_read_loop_whose_push_many_was_never_polled_before_shutdown_counts_its_whole_batch` runs
  `read_loop` with shutdown already set until a trial reads a batch and never polls the push, and
  checks the contract on every trial.
- `a_read_loop_dropped_mid_iteration_counts_what_its_batch_held_and_closes_the_queue` leaves the
  read one unit of coop budget, so the push `select!` yields with the batch full, then drops it.
- `read_loop_closes_the_queue_even_when_its_future_is_dropped` (paused) drops a parked read.
- `a_decode_loop_dropped_mid_batch_counts_every_popped_but_undecoded_datagram` (paused) counts
  datagrams 3 to 5, with their bytes, after the second `emit` parks.
- `a_udp_listener_cancelled_by_the_grace_backstop_counts_what_its_queue_still_held` wedges a
  listener behind an unread consumer and a `Block` queue, cuts it off after the signal, and checks
  the contract.
- `a_zero_shutdown_grace_never_breaks_the_datagram_contract` reproduces `run_input`'s `select!`
  at a grace of 0 over 50 iterations with varied traffic and timing.
- `an_interval_emit_that_parks_past_the_deadline_does_not_flush_once_per_pop_batch` (paused)
  checks that no interval flush follows a resumed `emit` at the same instant.
  `tcp.rs`'s `an_interval_emit_that_parks_past_the_deadline_does_not_flush_once_per_read` does the
  same over an in-memory duplex stream.
- `a_batch_cut_off_mid_fan_out_reaches_a_prefix_of_consumers` (paused) pins the partial fan-out
  gap.

`queue.rs` adds
`take_all_removes_everything_in_order_with_one_gauge_update_and_clears_a_reservation`.

Each guard and the clock fix was checked against its removal. Dropping `ReadHalf`'s count fails
the never-polled, dropped-mid-iteration, and grace-0 tests. Dropping `ResidualOnDrop`'s count
fails the grace-backstop and grace-0 tests. Relabeling `decode_loop`'s drain fails the decode
test, and reading the clock before the `emit` fails both interval tests.

### `drain/w4`: tail shutdown and timers (TAIL-06, TAIL-08)

Filled in by `drain/w4`.

### `drain/w5`: close-out

Filled in by `drain/w5`.
