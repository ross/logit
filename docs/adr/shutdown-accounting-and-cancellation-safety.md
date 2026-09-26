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
     abandoned-inbox sweep takes.
   - Per UDP listener: `logit.input.datagrams == datagrams decoded + Σ
     logit.component.datagrams.dropped{reason}`. A decoded datagram is one
     `logit.component.receive.latency` sample.
   - Two losses are named exceptions, left uncounted and listed in `docs/known-gaps.md`. The first
     is a UDP listener's decoded events that the grace backstop drops, either held in the
     `BatchAccumulator` or parked in `emit`'s `Fanout::send`. Their datagrams already count as
     decoded, so the datagram contract still holds. The second is a batch sent into a revoked Lua
     inbox by a permit holder still blocked after `REVOKE_DRAIN_TIMEOUT`.
2. **`drain complete`'s `batches_dropped` is the sum of every `reason="shutdown"` batch drop.**
   One helper, `count_shutdown_drop`, will be the only site that counts
   `batches.dropped`/`events.dropped{reason="shutdown"}` and the only site that adds to
   `shutdown_dropped_batches`. Its callers are the `run_output` sweep, `finish_and_flush`,
   `write_loop`, and `revoke_lua_io`.
3. **A send cut off by the shutdown grace is `Fault::Ambiguous`.** The destination may have
   received it, and the existing `is_retryable(Fault::Ambiguous, posture)` table decides what
   happens:
   - Under `at_least_once`, the batch stays uncommitted. Replay is that posture's contract.
   - Under `at_most_once`, the batch is committed and counted `dropped{reason="shutdown"}`, and
     the sink span is tagged `fault=ambiguous`.

   `write_loop` will track whether a send is in flight with a flag set immediately before the
   `timeout(remaining, output.send(batch))` await and cleared as soon as it returns. A grace
   expiry while the flag is clear leaves the batch uncommitted under either posture. That covers
   a `select!` that never polled the deliver arm, and a grace that lands during a backoff sleep:
   neither is a send in flight.
4. **The UDP shutdown remainders are counted.** A new `CountedDrain` wrapper around a `Vec`'s
   drain will count, when dropped, whatever it never yielded as `datagrams.dropped` and
   `bytes.dropped{reason="shutdown"}`. It will call only `Telemetry::count`, never the queue lock.
   `push_many` and `decode_loop` will iterate through it. The datagrams still in the
   `ReceiveQueue` when the grace backstop drops `drive` will be counted the same way, by a guard
   that closes the queue, drains it, and counts what it drained.
5. **Queue verification is a multi-thread randomized stress harness, a sequential proptest, and
   pins against tokio's source.** The stress harness will run `BoundedQueue` and `DiskQueue` on a
   four-worker multi-thread runtime with random producers, consumers, cancellations, and close
   timing, seeded so a failure replays. The proptest will check that batched and single-item calls
   agree under close and cancellation. Unit tests will pin the tokio behavior the queues rely on.
   On a tokio bump, re-check these internals:
   - `sync/notify.rs`: `notify_waiters` wakes only waiters whose `Notified` was created before the
     call, and dropping a `Notified` that consumed a `notify_one` permit passes the permit on.
   - `macros/select.rs`: `select!` returns on the first branch that is `Ready` and drops the rest.
   - `sync/watch.rs`: `wait_for`'s behavior when its future is dropped and re-created.
6. **`run_input` prefers the listener's own result.** Its `select!` will be `biased`, with the
   input's arm first, so a listener's `Err` that is ready in the same poll as the backstop is
   returned, not discarded.
7. **A listener's shutdown grace is enforced only by the runtime.**
   `InputRuntimeConfig::shutdown_grace`, read by `run_input`, is the one copy. The three
   per-listener copies (`TailBatching`, `UdpListenerConfig`, `TcpListenerConfig`) will be removed
   with no alias.
8. **`docs/design/pipeline-graph.md`'s "Cancellation points" table is the canonical list of every
   production `select!` and `timeout` on a node's run path.** Each row names the site, its arms,
   what a losing arm drops, and why nothing is lost or what counts it. A new `select!` or
   `timeout` on a run path adds its row in the same PR. Comments at the site point at the table
   and don't repeat it.

## Alternatives considered

- **loom or shuttle.** Rejected. Neither can instrument `tokio::sync::Notify`, which every queue
  waits on, and tokio's own loom build is dev-only, not something a dependent crate can switch
  on. A model checker that can't see the primitive under test would check a stand-in.
- **A hand-rolled model of `Notify`.** Rejected. It would verify the model, not tokio 1.53.1.
  Pinning the tokio behaviors the queues use, in tests that run against the real crate, catches
  a change on the next bump. A model wouldn't.
- **A counting `Drop` that takes the queue lock.** Rejected. It would add lock acquisition from
  `Drop`, with poisoning and ordering hazards, on the cancellation path. The count needs no queue
  state: the undrained items are in the caller's `Drain`, and `Telemetry::count` takes only its
  own buffer's lock, which is never held across an await or while a `CountedDrain` drops. `impl
  Drop for Timer` already counts from `Drop` this way.
- **A new `shutdown_ambiguous` drop reason.** Rejected. It splits one operator question ("what
  did shutdown drop?") across two reasons, and `drain complete` would have to sum both. The
  span's `fault=ambiguous` tag carries the "may have been received" signal.
- **Counting a grace-cut send as `reason="send_failed"`.** Rejected. No destination failure was
  observed. `send_failed` drives the `degraded` readiness edge and the permanent-failure streak,
  and `drain complete` sums shutdown drops, not send failures.
- **Leaving the shutdown remainders uncounted.** Rejected. [ADR
  `udp-intake-batching-and-socket-visibility`](udp-intake-batching-and-socket-visibility.md)
  accepted them as bounded and shutdown-only, and priced counting against the queue lock. The
  lock isn't needed (see above), and the losses are wider than that ADR named, so an operator
  reconciling counters at shutdown couldn't tell an accepted loss from a bug.

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
- `drain complete` will log at `warn` whenever any `reason="shutdown"` batch drop happened, from
  any of the four sites, and `docs/deploying.md` will say what the field sums.
- Removing the three per-listener `shutdown_grace` fields is a wide but mechanical diff across the
  struct literals in `logit-cli`'s tests. It isn't operator-visible: the operator's field,
  `buffer.shutdown_grace`, doesn't change.
- The stress tests will run a small seed count in CI (64 for `BoundedQueue`, 16 for `DiskQueue`)
  under a 30 s timeout, so they don't flake on a loaded runner. Long runs will be `#[ignore]`d,
  and `LOGIT_QUEUE_STRESS_SEED` will replay one seed.

## Running it

Each workstream updates its inventory rows in the PR that lands its artifact.

### `drain/w1`: queue protocol (NET-06, NET-07, RT-07, DISK-08)

Filled in by `drain/w1`.

### `drain/w2`: runtime shutdown accounting (RT-02, RT-03, RT-04)

Filled in by `drain/w2`.

### `drain/w3`: UDP read and decode loops (NET-02, NET-03)

Filled in by `drain/w3`.

### `drain/w4`: tail shutdown and timers (TAIL-06, TAIL-08)

Filled in by `drain/w4`.

### `drain/w5`: close-out

Filled in by `drain/w5`.
