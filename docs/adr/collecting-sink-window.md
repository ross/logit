---
created: 2026-10-30
updated: 2026-10-30
---

# Collecting sink window: a windowed sink keeps its window open across arrivals until its own bound, a deadline, store pressure, or shutdown closes it

## Status
Accepted. Built in W2 of [the plan](../plans/s3-object-archive.md); `s3_out` (W3a) is the one
sink that collects. "Implementation notes (2026-10-30)", below, records what W2 decided that this
ADR left open.

Extends [ADR `native-hop-send-window`](native-hop-send-window.md)'s decision 4, where the fill
ends once `peek_at` returns `None`, for a sink that says it's still collecting. A sink that
doesn't say so, every sink today, runs the window as it does now.

## Context

[ADR `s3-object-archive`](s3-object-archive.md) keeps one object open over a time and size window
and counts each batch in it delivered only when the object is complete. Every batch in the open
object has to stay in the sink's store, uncommitted, for as long as the object is open: minutes,
across many arrivals, with gaps between them.

The window path almost carries this. `deliver_window` and `window_round`
(`crates/logit-pipeline/src/runtime.rs`) keep up to a window of batches submitted and
unacknowledged, reserved as a prefix of the store; `submit` stages a batch and `await_ack`
answers the oldest. Commit per head, the at-fault accounting, per-batch spans and
`batches.delivered`, and the grace handling all apply to an object's batches as they stand. What
stands in the way:

- **The fill ends at the first gap.** `window_round` submits while `outstanding` is below the
  window and `peek_at` returns an item, then calls `await_ack`. An empty store between two pushes
  ends the fill, and the head's `await_ack` must then answer, so the sink would have to close its
  object at every gap in arrivals.
- **Racing `await_ack` against arrivals is wrong.** A cancelled `await_ack` means the sink
  dropped its connection and nothing is outstanding (`Output::await_ack`'s doc), so cancelling it
  to submit a new arrival would tell the loop to resubmit the whole object.
- **Neither store can wait for an item past the head.** `BoundedQueue::peek_at` and
  `DiskQueue::peek_at` never wait for a push; only `peek` waits, and only for the head.
- **A long-held prefix meets the store's bounds.** A memory store's `InMemoryBuffer::evict_to_fit`
  never evicts a reserved item, so under `overflow: drop_oldest` with every item reserved, a push
  is accepted one over the bound and the next one evicts it; under `block`, the push waits, and
  every source feeding the sink stalls for as long as the object stays open. A disk store under
  `drop_oldest` sheds the newest record while any record is read ahead
  (`drop_oldest_drops_the_newest_while_any_record_is_read_ahead` in
  `crates/logit-pipeline/src/disk_queue.rs`), and an open object keeps records read ahead nearly
  all the time. The store's `FullWatch` (`crates/logit-pipeline/src/buffer_full.rs`) learns of a
  push that waits or sheds, but not of the memory store's push accepted over its bound with
  nothing evictable: `SinkQueue::push` counts no drop for it and opens no `buffer_full` episode.
- **A round fills before it waits.** `window_round` always runs its fill before its
  `await_ack`, and after the head is answered `write_loop` commits the head alone, so the next
  round fills again before it reads the next answer. A sink whose fill can wait would open a new
  window while the answers for a closed one are still owed.
- **A submit failure past the head is unclassified.** `window_round` stops the fill on it and
  reads the answers already owed (`Output::submit`'s doc), so the sink itself has to carry that
  failure to the head's answer.
- **A disk store holds read-ahead decoded.** `DiskQueue::peek_at` caches each record it reads
  ahead, decoded, until it's committed, and a disk store's `max_in_flight` is `usize::MAX`, so a
  window bounded by nothing else holds every staged batch on the heap.
- **Telemetry would measure collection.** `deliver_window` times each round into
  `logit.component.send.duration`, and `SendLimit` names "sending at its concurrency" in the
  `buffer_full` line when `outstanding` reaches the window. A round that collects for minutes would
  record minutes and read as a sink saturated by its round trip.
- **Shutdown would wait for the object's age.** Nothing tells a collecting sink that shutdown
  started, and `buffer.shutdown_grace` (default `5s`) starts at the signal.
  `drop_cut_off_at_shutdown` counts every batch that left the process as "cut off mid-send", which is wrong
  for a batch staged into an object whose upload never completed: the destination can't have it.

## Decision

A sink may say it is still collecting, with a deadline. While it does, the fill waits for the
store's next item instead of ending at an empty store, and the window closes, and the head's
`await_ack` runs, at the first of: the sink's own bound, the deadline, store pressure, the store
closing, the staged batches' memory, or the shutdown signal. A closed window's answers are all
read before the next window opens.

### 1. The sink's "still collecting" signal and deadline

- **A new `Output` method with a default.** `Output::collecting(&self) -> Option<Instant>`, by
  default `None`. `Some(deadline)` means the sink's open window takes more batches until
  `deadline`; `None` means it takes no more and its head's `await_ack` may be called. `s3_out`
  returns its object's `max_age` deadline while the object is under `max_bytes`, and `None` once
  a staged batch takes it past.
- **`window()` keeps its meaning: the in-flight limit.** While a window collects, or none is
  open, a collecting sink reports `usize::MAX`, so the bound comes from `collecting`, the store's
  `max_in_flight`, and the memory bound (decision 2). The design notes floated `window()`
  returning `staged + 1` while open instead; a separate method makes the deadline part of the
  signal.
- **`await_ack` on the head closes the window.** The sink needs no separate close call: the loop
  calls `await_ack` only once it stops filling, and a collecting sink reads that call as "close
  and complete". The sink answers the head once its window completes.
- **No new window while answers are owed.** From the close until the last batch of the closed
  window is answered, the sink's `window()` reads the number of answers it still owes, and
  `collecting()` reads `None`. After each head is committed, `outstanding` equals that number, so
  `window_round`'s check `outstanding >= window` stops the fill before it peeks or waits, and the
  round goes straight to `await_ack`, which the sink answers `Ok` at once. Once the last answer
  is read, `window()` returns to `usize::MAX` and the next round opens a new window. The loop
  needs no new branch for this, and a batch that arrives during an upload waits in the store
  until every answer is read.
- **A sink that never returns `Some` changes nothing.** `logit_out` and the HTTP slot sinks keep
  the default, so their fill, `await_ack`, telemetry, and grace handling run as today.

### 2. The fill waits on the store

- **The wait primitive.** `SinkStore` gains an `async` wait for its `n`-th item that resolves with
  one of `Item`, `Closed`, `Deadline`, or `Pressure` (decision 3). A memory store waits on its
  `not_empty` notification; a disk store on its own. It reserves nothing until it returns `Item`,
  and is cancellation-safe: a dropped wait leaves the store as it found it. `peek_at` keeps its
  never-waits contract.
- **The fill loop.** At each position, when `peek_at` returns `None` and `collecting()` returns
  `Some(deadline)`, the loop waits for that position until `deadline`, store pressure, the store
  closing, or the shutdown signal, then submits on `Item` and stops the fill on anything else. A
  batch the store already holds is taken without waiting, under pressure too (decision 3). It
  reads `collecting()` again after each submit and stops on `None`. It also stops at
  `min(window, store.max_in_flight())`, so a memory store still caps an open window at
  `buffer.max_batches`.
- **A memory bound, in both stores.** The fill also stops once the staged batches'
  `EventBatch::estimated_heap_bytes` reach `buffer.max_bytes`, the operator's memory budget for
  the sink. Under the memory store the store's own bound already enforces it; under a disk store
  it bounds the decoded read-ahead the window holds, and graph rule 35 will accept a non-default
  `buffer.max_bytes` beside `disk:` on a collecting sink for that one use ([ADR
  `disk-backed-sink-buffer`](disk-backed-sink-buffer.md)'s amendment of 2026-10-30). The default
  64 MiB applies when the operator sets none. A window's size is
  bounded by that budget on either store, and a larger window means raising `buffer.max_bytes`.
- **Every stop goes to `await_ack`**, as a stop does today. The loop records why the window
  closed (decision 4).
- **A fault doesn't change.** A non-`HeadOnly` fault covers every staged batch and the loop
  resubmits from the head, which reopens the window; the sink reopens its object under the same
  head. A `HeadOnly` fault settles the head alone, as [ADR
  `sink-request-concurrency`](sink-request-concurrency.md)'s decision 2 says. A fault resubmits
  without observing again, so `CountGate` keeps a sink's encode-side counts once per batch ([ADR
  `sink-send-path-and-attempt-accounting`](sink-send-path-and-attempt-accounting.md), decision 2).
- **A submit failure past the head poisons the window.** The loop stops the fill and leaves the
  failure unclassified, as today. A collecting sink records it, never completes that window, and
  fails the head's next `await_ack` without `HeadOnly`, with that failure's class. The loop then
  resubmits from the head, failed batch included, into a new window.

### 3. Store pressure closes a collecting window, in both stores

- **The signal.** A store reports pressure from the moment a push finds no room: a `block` push
  starts waiting, a `drop_oldest` or `drop_newest` push sheds an item, or a `drop_oldest` push
  with nothing evictable is accepted over the bound. The first two open a `buffer_full` episode
  in the store's `FullWatch` today; the third opens nothing, and W2 makes it raise the signal
  too, since under a collecting window every staged batch is reserved and the memory store would
  otherwise grow past its bound for as long as the window collects. The signal clears once the
  store is back under its bounds with no push waiting. The
  memory store raises it against `buffer.max_batches` and `buffer.max_bytes`, and the disk store
  against `disk.max_bytes`. The wait primitive returns `Pressure` while it's raised and no item is
  at the position it waits for, so pressure ends only the wait for new arrivals. It never stops
  the window taking a batch the store already holds. That batch grows neither store: a memory
  store already counts it, and the memory bound limits what a disk store reads ahead. Closing
  frees only the one slot the next push takes, so a window that checked pressure before each
  batch it took would close at one batch, and every object would be one batch for as long as the
  overload lasts.
- **Why the window closes on it.** Under `block`, a window held open stalls every source feeding
  the sink until the deadline; closing it starts the upload that commits the staged batches and
  frees the room. Under a drop policy, closing it bounds the shedding to the time the upload
  takes. The store is the bound the operator set, and an object larger than the store holds is
  not one the store can back.
- **What `drop_oldest` does under a collecting window.** The batches in an open or uploading
  window are reserved, so `drop_oldest` sheds the oldest batch behind the window, which arrived
  after the window closed: in effect the newest. A memory store with nothing unreserved accepts
  one push over its bound, as it does under any window, and the next push evicts it. An open
  window takes that batch like any other the store holds and then closes on the memory bound, so
  the store holds at most one batch over its bound. The disk
  store already behaves this way while any record is read ahead. The operator docs say so beside
  `buffer.overflow`; validation doesn't reject the combination, since it runs as written.

### 4. Telemetry

- **`send.duration` measures the completion, not the collection.** A collecting sink returns
  `true` from `Output::send_duration_per_batch`, and `Output::answered_send_duration` reports, for
  the head, the time from the window's close to the head's answer, and `None` for each batch
  answered behind it. A round's own timer is never recorded for it.
- **The `buffer_full` cause.** The loop remembers whether the current window collected (a
  `collecting()` read returned `Some`). While such a window collects, it sets `SendLimit` to a
  limit of 1 with 0 in flight, which adds no cause to the line. From the close to the last answer
  it sets 1 in flight, not the owed count `window()` reads, so a `buffer_full` line written during an upload reads "sending at its concurrency
  of 1" with the last upload's duration, which is the true bound.
- **One new count.** `logit.component.window.closed{cause}` counts each closed collecting window
  by what closed it: `full` (the sink's `collecting()` returned `None`, or `max_in_flight` was
  reached), `deadline`, `pressure`, `memory` (the memory bound), `closed` (the store closed), or
  `shutdown`. A sink whose
  windows close on `pressure` needs a larger buffer or a smaller window, and this count is where
  an operator sees it. [`docs/design/internal-telemetry.md`](../design/internal-telemetry.md)
  records it in W2.

### 5. Shutdown

- **The shutdown signal closes the window.** The fill's wait ends at the signal, not at the grace
  expiry, so the upload starts with the whole grace ahead of it. After the signal, the loop
  doesn't wait to collect: each later window closes at the first empty `peek_at`.
- **The grace counts only what may have reached the destination.** A new `Output` method, whose
  default returns what `InFlight::left_the_process()` returns today (`outstanding`, plus one while
  a submit is in progress), tells the loop how many of the outstanding batches may
  have reached the destination. A collecting sink answers 0 until it sends the request that
  makes its window visible (`s3_out`'s `PutObject` or `CompleteMultipartUpload`), and every staged
  batch after. Under `at_most_once`, `drop_cut_off_at_shutdown` commits and counts only those; the
  rest stay reserved, so a memory store's `finish` counts them as still queued at shutdown and a
  disk store replays them. Its warning keeps the words "cut off mid-send" for the batches it
  counts, which are mid-send.
- **The grace doesn't cut a closed window's owed answers.** They belong to a window the
  destination already stored, and the sink answers them at once, so reading them costs no wait.
  The loop samples `window()` before each round's `select!`. A round that starts with an owed
  count, which only happens after the head's answer, skips `deliver_window`'s grace early return
  and `write_loop`'s grace arms; the loop reads every owed answer, commits each batch as
  delivered, and only then honors the expired grace. The head's own round, which runs the
  upload, starts with `window()` at `usize::MAX`, so the grace still cuts a stuck upload. Without
  this, a grace that passed between the head's answer and the rest would count stored batches as
  shutdown drops, or replay them from a disk store as duplicates.
- **The grace is short for an upload.** Under the memory store, a grace cut counts the open
  window's batches as shutdown drops. `s3_out`'s operator doc will recommend a
  `buffer.shutdown_grace` long enough for one object's upload, and `buffer.disk:`, which replays
  instead.

### 6. One window at a time

A collecting sink has one window open or uploading at a time: the next window opens only once
every batch of the last one is answered (decision 1). Several objects in flight, or several parts of one object
in flight, are a follow-up, measured first; the window protocol's in-flight prefix would carry
them.

## Alternatives considered

- **Race `await_ack` against new arrivals.** Rejected: a cancelled `await_ack` means the sink
  dropped its connection, so the loop would resubmit every staged batch.
- **A sink that owns its accumulation outside the window protocol.** The sink could take each
  batch through `send`, return `Ok` once staged, and upload later. Rejected: `Ok` commits the
  batch, so it would be acknowledged from memory before it's stored, and a crash or a failed
  upload would lose it uncounted. That's the ack-on-staging model [ADR
  `s3-object-archive`](s3-object-archive.md) rejects.
- **`window()` returns `staged + 1` while collecting.** The notes' first form. Rejected for a
  separate method: the deadline needs a home, and `window()` then departs from `usize::MAX` in one
  place only, the owed count after a close.
- **Let pressure backpressure instead of closing.** Rejected: under `block` it stalls every
  source for up to the window's deadline, and under `drop_oldest` the memory store would hold a
  window past its bounds.
- **Close the window at the grace expiry instead of the signal.** Rejected: the upload would
  start with no grace left.
- **Drop each read-ahead batch once it's staged, and read it again on a fault.** This would
  bound a disk store's heap by one batch instead of the window, so an object could outgrow
  memory. Rejected for now: it needs the disk store to forget and re-read a reserved record, and
  the memory store can't re-read at all, while the memory bound covers every object `s3_out`'s
  defaults make. It stays open if objects need to outgrow memory.
- **Open the next window while answers are owed.** Rejected: delivered batches would stay
  reserved for another window, a crash would replay them, and a fault or an `at_most_once` drop
  in the next window would count them again.

## Consequences

- **Code (W2).** `Output::collecting` and the reached-the-destination count with their defaults
  in `crates/logit-pipeline/src/output.rs`; the wait primitive and the pressure signal on
  `SinkQueue`, `DiskQueue`, and `SinkStore` in `crates/logit-pipeline/src/queue.rs`,
  `disk_queue.rs`, and `buffer_full.rs`; the fill's wait, the close cause, the `SendLimit`
  settings, and the grace count in `window_round`, `deliver_window`, and `write_loop`. Tests run
  on a collecting variant of the `WindowedOutput` fake, and each regression test is shown failing
  with its change reverted.
- **Docs (W2).** `docs/design/internal-telemetry.md` gains `logit.component.window.closed`;
  `docs/design/pipeline-graph.md`'s "Cancellation points" gains the wait; `docs/deploying.md`'s
  sink buffering section gains the `drop_oldest` note.
- **No change for existing windowed sinks.** `logit_out` and the six HTTP slot sinks return the
  default `collecting()`, so their fill, acknowledgment, telemetry, and grace counts are as today;
  their existing window tests pin it.
- **`buffer.max_bytes` bounds a window on both stores.** A larger object means a larger
  `buffer.max_bytes`; `buffer.disk:` buys durability across a restart, not size.
  [`docs/design/memory.md`](../design/memory.md) records a collecting sink's cost once W3
  measures it. Graph rule 35's text in [`docs/design/pipeline-graph.md`](../design/pipeline-graph.md)
  changes with W2.

## Implementation notes (2026-10-30)

W2 settled these, which the decisions above leave open:

- **What makes a window a collecting one.** The loop reads `collecting()` after the head's
  submit. A `Some` there opens a collecting window; a `None` leaves the ordinary fill, which ends
  at the first empty `peek_at`, so a sink that closes its window at its first batch gets no wait.
- **The order of the checks before each submit past the head:** `collecting()` reading `None`
  (`full`), the deadline passed, then the memory bound. The deadline is checked even when the
  store has a batch ready, so a store that never empties can't hold a window open past its
  deadline. Pressure isn't among them (decision 3). Only an empty `peek_at` waits (decision 2),
  and after the shutdown signal it closes the window instead (`shutdown`). The wait checks for an
  item before pressure, so a batch that arrives with pressure raised still joins the window.
- **A window a submit failure ends isn't counted** in `logit.component.window.closed`: it never
  completes, and the loop reopens it from the head (decision 2's poisoned window), whose close is
  counted.
- **An owed-count round** is one that starts after a collecting window closed, with some of its
  batches still outstanding, and with `window()` reading other than `usize::MAX`. The loop keeps
  the first fact in `InFlight` and samples `window()` before each round's `select!`.
- **When pressure clears.** A change on either side, a commit or a push that sheds nothing and
  leaves the store within its bounds, clears the signal once utilization is below 1 with no push
  waiting. A store at its bound stays under pressure, so a `block` push that gets room keeps the
  signal raised until the next commit. The memory store raises it for any push it accepts over a
  bound, the batch that can never fit included.
- **`buffer.max_bytes` reaches the loop** as `WriteLoopConfig::window_max_bytes`, set for every
  sink and read only by a collecting window, so neither store carries a bound it doesn't
  enforce.
- **`Output::reached_destination`** takes the count the loop would have cut off, and every grace
  cut passes through it: the `send` path's, the windowed round's, and the cut between rounds.
  Its default returns that count, so a sink that keeps it counts what it counted before.
- **`Output::collecting` returns a `tokio::time::Instant`**, so a test under paused time reaches
  the deadline.
