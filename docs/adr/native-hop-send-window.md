---
created: 2026-10-01
updated: 2026-10-01
---

# Native hop send window: several frames in flight, acknowledged in frame order

## Status
Accepted. Supersedes in part:

- [ADR `native-hop-identity-and-sequence`](native-hop-identity-and-sequence.md): decision 4's
  "With one frame in flight per connection the reply is unambiguous" and "`window` stays 1, and
  a future credit-based flow-control record decides its own acknowledgment form", and the
  rejected alternative "`Ack` echoing the sequence", whose "With one frame in flight" reason no
  longer holds.
- [ADR `native-transport-handshake-and-ack`](native-transport-handshake-and-ack.md): "In-flight:
  one frame per connection, in this plan", and the deferred alternative "Building credit-based
  flow control (window > 1) now".
- [ADR `delivery-semantics`](delivery-semantics.md): item 7's last line, "`window` stays 1.
  Credit-based flow control is separate work and this record doesn't design it."
- [ADR `disk-backed-sink-buffer`](disk-backed-sink-buffer.md): "Corrections to the design
  sketch", item 1's list of the `SinkStore` surface as `push`/`peek`/`commit`/`close`/`finish`.
  `SinkStore` gains `peek_at` and `max_in_flight`.
- [ADR `sink-send-path-and-attempt-accounting`](sink-send-path-and-attempt-accounting.md):
  decision 7's "it isn't under the sink's `request_timeout`", for a `logit_out` write with
  frames in flight, which decision 5 bounds by progress.
- [ADR `buffered-sink-delivery`](buffered-sink-delivery.md): "every attempt, including the
  first, races the remaining budget", which on the window path holds for the head's own submit
  alone (decision 4).

## Context

`logit_out` writes one frame, waits for `logit_in`'s `Ack`, then writes the next. Per connection
that caps throughput at one batch per round trip plus the receiver's decode and forward time.
On a link with a 10 ms round-trip time (RTT) that's about 100 batches/s, and at 100 ms about 10
batches/s, whatever the hardware. On loopback the perf harness already shows `native-relay` as
ack-bound: `docs/design/performance.md`, "Peak RSS: what is live data and what is jemalloc
retention", names `logit_out`'s full sink queue as its peak.

Every earlier record deferred a window above 1, and each named the same blocker: the sink's
store is head-only. `SinkStore` exposes `peek` (the head) and `commit` (pop the head), so a sink
can't have two batches outstanding, and a fault after several frames left would have nothing to
resend them from:

- [ADR `native-transport-handshake-and-ack`](native-transport-handshake-and-ack.md), "In-flight",
  negotiates `window` and keeps one frame in flight until `SinkQueue` can track several.
- [ADR `native-hop-identity-and-sequence`](native-hop-identity-and-sequence.md), decision 4,
  keeps the sequence a deduplication identity, never a credit, and leaves the acknowledgment form
  to a later record.
- [ADR `delivery-semantics`](delivery-semantics.md), item 7, keeps `window` at 1.
- `docs/known-gaps.md` tracks "Credit-based flow control (`window` > 1)" and "No
  out-of-order/credit-based acknowledgement".

What changed: the hop is effectively-once. Every frame carries the `(sender id, seq)` its store
assigned, and `logit_in` acknowledges a frame at or below its sender's high-water mark without
forwarding it ([ADR `native-hop-identity-and-sequence`](native-hop-identity-and-sequence.md),
decision 5). Resending a whole window after a fault is therefore safe at the default
`at_least_once` posture: `logit_in` forwards each batch once.

These facts about the code fix the design:

- **The receiver is one serial task per connection.** `serve_connection` in
  `crates/logit-inputs/src/logit.rs` reads a frame, forwards it, writes its answer, then reads
  the next. Answers leave in the order frames arrived, so acks arrive in frame order.
- **`Ack` is empty.** `control::Ack` carries no fields, so an `Ack` can't name the frame it
  answers.
- **`window` is negotiated and ignored.** `logit_out` offers `window: 1` in `Hello`,
  `logit_in` answers `window: 1` in `HelloAck`, and neither side reads the other's value.
- **The store is head-only.** `SinkStore::peek` returns the head and reserves it against
  `drop_oldest` eviction; `commit` pops it. The memory buffer tracks one reserved item
  (`InMemoryBuffer.head_reserved`), and the disk spool caches one decoded head
  (`DiskQueue.head_cache`).
- **`observe_batch` counts.** Ten sinks (`collectd_out`, `datadog_out`, `datadog_trace_out`,
  `graphite_out`, `influxdb_out`, `otlp_out`, `prometheus_out`'s remote-write sender, `splunk_hec_out`,
  `statsd_out`, `syslog_out`) run `BatchAccounting::observe` in `Output::observe_batch`, which
  the write loop calls once per batch before its first attempt. A second call for the same batch
  would count its encode-side drops twice.
- **`close()` with unread data resets the connection.** On Linux, closing a TCP socket whose
  receive buffer still holds unread bytes sends RST and discards the unsent send queue. A
  `logit_in` that closes with pipelined frames unread loses the `GOING_AWAY` or trailing `Ack`s it
  has written, and the sender reads `ECONNRESET` (`Ambiguous`) instead of `GOING_AWAY` (`Clean`).
  With one frame in flight that needs a narrow race; with a window it's the common case.
- **Neither side sets `TCP_NODELAY`.** Only `otlp_out` does. With small acks and an idle sender,
  Nagle's algorithm and delayed ACK together can add about 40 ms per ack.

## Decision

`logit_out` keeps up to a negotiated window of frames in flight on one connection, and each
`Ack` answers the oldest unanswered frame. The wire doesn't change: no credit messages, no
sequence in `Ack`, and no version bump. The sink's store reserves a prefix of items instead of
the head alone, and a fault resends the window from the head.

### 1. Wire: no message changes

- **Acks arrive in frame order.** `Ack` stays empty. `logit_in` handles one connection's frames
  serially and in order, so the k-th `Ack` on a connection answers the k-th unacknowledged frame.
  `docs/design/wire-protocol.md` states this as normative.
- **A fixed negotiated window, not credits.** No message grants or returns credit; the window is
  set at the handshake and holds for the connection.
- **`logit_in` answers a clamped window.** `HelloAck.window` is
  `hello.window.clamp(1, RECEIVER_MAX_WINDOW)`, with `RECEIVER_MAX_WINDOW = 1024`. At 1024 the
  unread acks a peer can leave in the listener's send buffer are about 47 KB under TLS, under the
  default `tcp_rmem`.
- **`logit_out` uses the smaller of the two.** It offers its configured `window` in `Hello` and
  uses `max(1, min(offered, answered))`. A `logit_in` that answers 1 gets one frame in flight. A
  `HelloAck.window` of 0 reads as 1.
- **`TCP_NODELAY` on both ends.** `logit_out` sets it through `Dial.nodelay` in
  `crates/logit-outputs/src/stream.rs`, and `logit_in` calls `set_nodelay` on accept.
- **`logit_in` closes with a linger.** On every return of `serve_connection` after the
  handshake, `logit_in` drops its `Fanout` clone, shuts the stream down (`close_notify` under
  TLS), then reads and discards until EOF or `handshake_timeout`. That keeps a `GOING_AWAY` or
  trailing `Ack`s from being lost to a reset.
- **No version bump.** `PROTOCOL_VERSION`, the frame `VERSION`, and the codec byte stay as they
  are. `logit` is pre-release, the same posture [ADR
  `native-hop-identity-and-sequence`](native-hop-identity-and-sequence.md) took.

### 2. Config

- **`window` on `logit_out`.** `LogitOut.window: u32`, default 32
  (`default_logit_out_window`). The operator doc says it's the number of frames in flight before
  the oldest must be acknowledged, to raise it on a high-latency link, and that `1` keeps one
  frame in flight.
- **Graph rule 74.** `1 <= window <= 1024`, with a row in `docs/design/pipeline-graph.md`.
- **No `logit_in` field.** The receiver's bound is the constant `RECEIVER_MAX_WINDOW`.
- **Schema.** `crates/logit-cli/src/pipeline.rs` passes `.with_window(*window)`, and
  `script/schema` regenerates `schema/logit.schema.json`.

### 3. Store: a reserved prefix instead of a reserved head

- **`peek_at`.** `SinkStore::peek_at(&self, n: usize) -> Option<StoreItem>` is `async` but never
  waits for a push. It returns the n-th item from the head and reserves items `0..=n` against
  eviction. `None` means not readable now: fewer than n+1 items, or a disk spool that has caught
  up to its writer.
- **`peek` and `commit` keep their contracts.** `peek` waits for the head; `commit` pops it and
  releases one reservation.
- **`max_in_flight`.** `SinkStore::max_in_flight()` is `max_batches.max(1)` for a memory store
  and `usize::MAX` for a disk store.
- **Memory.** `Buffer` (`crates/logit-proto/src/buffer.rs`) gains `peek_at`.
  `InMemoryBuffer.head_reserved: bool` becomes `reserved: usize`, with `reserved <= len` as an
  invariant: `peek` is `peek_at(0)`, `commit` decrements `reserved` saturating at 0, and eviction
  starts at index `reserved`. `DropOldest` with nothing evictable keeps accepting one item over
  its bound; the write loop's cap at `max_in_flight` keeps the store at `max_batches + 1` or
  fewer. `BoundedQueue::peek_at` is synchronous under one lock, and `SinkQueue` gains `peek_at`
  and `max_in_flight`.
- **Disk.** `DiskQueue.head_cache: Option<HeadCache>` becomes
  `read_ahead: VecDeque<ReadAhead { batch, ctx, seq, seg, offset, len }>`. The next read position
  is the back entry's `(seg, offset + len)`, or the commit cursor when `read_ahead` is empty,
  rolling to the next segment when that one is finished and not the active one, without
  unlinking anything.
  - `peek_at(n)` reads one record at that position and appends it only if the position is
    unchanged under the lock, so a cancelled read caches nothing.
  - A corrupt span past the head returns `None`; the head's `peek` handles it through
    `skip_corrupt`, as it does today.
  - `commit` pops the front entry, checks in a `debug_assert` that the commit cursor matches it,
    then advances the cursor by its length. Persist triggers don't change.
  - Every check of `head_cache` (`push`'s make-room `nothing_queued`, the `DropOldest` to
    `DropNewest` arm, `evict_oldest`, `skip_corrupt`) becomes a check of `read_ahead`.
  - `finish` doesn't change: records in `read_ahead` were never committed, so they replay.
- **No explicit release.** After a fault the write loop peeks again from the head, and `commit`
  shrinks the prefix. The store keeps its single producer and single consumer.

### 4. Output trait and write loop

`Output` (`crates/logit-pipeline/src/output.rs`) gains three methods with defaults, so no sink
other than `logit_out` changes:

```rust
fn window(&self) -> usize { 1 }   // in-flight limit; may change after a connection is made
async fn submit(&mut self, batch: &EventBatch, ctx: BatchContext, seq: Option<SeqId>,
                in_flight: usize) -> anyhow::Result<()> { self.send(batch).await }
async fn await_ack(&mut self) -> anyhow::Result<()> { Ok(()) }
```

- **`observe_batch` runs once per batch, before its first submission.** `write_loop` tracks
  `observed`, a count of the store's prefix already observed. `commit` decrements it, and a
  fault doesn't reset it, so a resubmitted batch isn't observed again.
- **The sink drops its connection on every failure of `await_ack`.** Every `Err` from
  `await_ack`, and every cancelled `submit` or `await_ack`, means the sink dropped its connection,
  and `write_loop` resets `outstanding` to 0 on either.
- **`in_flight` is the loop's count.** `submit` receives `outstanding`. A sink whose own count
  differs fails the submit `Ambiguous`, so a drifted count can't let the first `Ack` commit a
  head that was never sent.
- **A fast path at window 1.** When a head starts, if `outstanding == 0`, `observed == 0`, and
  `output.window() <= 1`, `write_loop` runs `deliver_with_retry` unchanged. Every sink but
  `logit_out` stays on this path, with no extra boxed future per batch. The `observed == 0`
  term matters after a mid-window fault: later batches are already observed, `logit_out` has
  no connection so its `window()` reads 1, and `deliver_with_retry`'s `send` would carry the
  pending sequence of the last batch observed rather than the head's, which raises the
  receiver's mark past batches not yet delivered. Such a head goes through `deliver_window`
  with an effective window of 1 and its context and sequence passed explicitly. On the fast
  path `write_loop` calls `observe_batch` before `deliver_with_retry`, as today, and leaves
  `observed` at 0; only `deliver_window` counts what it observes.
- **Otherwise `deliver_window`**, with an effective window of
  `min(output.window(), store.max_in_flight())`:
  - **Fill.** While `outstanding < window`: `peek_at(outstanding)`, `observe_batch` if the batch
    isn't observed yet, then `submit`. An `Ok` increments `outstanding`. The head's submit runs
    under the head's remaining attempt time, as a `deliver_with_retry` attempt does. A submit
    past the head runs under no attempt time: the sink bounds it by progress (decision 5, step
    6), and the window caps how many there are. The head's budget never cancels a write past
    the head, so an `Ack` the receiver already sent is never lost to the budget: a round with a
    slowly draining receiver can outlast the budget, and the head is still delivered when its
    buffered `Ack` is read.
  - **A submit `Err` at the head** is the head's own fault, classified as today.
  - **A submit `Err` past the head classifies nothing.** The fill stops and the loop goes to
    `await_ack`. The sink keeps the connection, marked `broken` (no more writes), so the acks
    already owed are still read. A `Permanent` past the head also stops the fill; that batch
    fails again when it's the head and is dropped then.
  - **Await.** `await_ack` under no attempt time: the sink bounds it by `request_timeout`. `Ok`
    delivers the head. `Err` sets `outstanding` to 0, classifies the fault, and backs off and
    retries while the fault is retryable and the head's retry budget lasts, else drops the head
    with that fault.
  - **One retry budget per head.** Its clock starts when the batch becomes the head, as today.
    On the window path the budget decides whether a failed round is retried, and bounds the
    head's own submit and each backoff; it never cancels anything past the head's own submit. A
    round lasts at most the head's submit, `window - 1` progress-bounded writes, and one
    `request_timeout` ack wait, so a round can overrun the budget by that much where
    `deliver_with_retry` cuts an attempt at the deadline.
  - **A pipelined sink bounds itself.** A sink that reports a window above 1 bounds every
    `submit` past the head and every `await_ack` on its own, since the loop applies no attempt
    time to them.
- **Delivered and dropped.** A delivered head is committed, and on the window path `outstanding`
  and `observed` each drop by 1. A dropped head is committed as dropped. Under `at_most_once` an `Ambiguous` fault
  also commits every other outstanding item and counts each `send_failed`, since each is as
  ambiguous as the head. A `Permanent` fault drops only the head.
- **Grace.** The write loop counts as sending while `outstanding > 0` or a submit is in
  progress, including a `GraceExpired` reached before an attempt with `outstanding > 0`. At a
  grace cut, `at_most_once` commits every outstanding item and counts each as a shutdown drop;
  `at_least_once` leaves them reserved, so a memory store's `finish` counts them and a disk
  store replays them.
- **Telemetry.** `logit.component.send.duration` records one sample per windowed round (fill
  and await). `docs/design/pipeline-graph.md`'s "Cancellation points" table gains a
  `deliver_window` row.

### 5. `logit_out` and `logit_in`

- **State.** `logit_out`'s connection gains `window`, `in_flight`, and `broken`, and
  `LogitOutput` gains `window: u32` and `with_window`.
- **`submit`**, in order:
  1. Encode with the passed context and sequence.
  2. Fail `Ambiguous` if `in_flight` differs from the connection's count (0 with no connection).
  3. Connect and handshake if there's no connection, and record the negotiated window.
  4. Probe a pooled connection only when `in_flight == 0`: the probe consumes a byte, and with
     frames in flight that byte is an `Ack`'s.
  5. Size checks fail `Permanent` and keep the connection.
  6. Write and flush. With `in_flight == 0` the write is bounded by the retry budget alone, as
     [ADR `sink-send-path-and-attempt-accounting`](sink-send-path-and-attempt-accounting.md),
     decision 7, keeps it, and an error is `Clean` and drops the connection. With frames in
     flight the write is bounded by progress: the frame is written in chunks, and a chunk write
     or the final flush that accepts nothing for `request_timeout` is a stall. A large frame on
     a slow link makes progress every chunk and never trips it; a receiver parked on an earlier
     frame does. A stall or an error with frames in flight marks the connection `broken` and
     returns the error unclassified: neither invalidates the acks the receiver already sent,
     and `await_ack` reads them before the window is retried. Nothing more is written on a
     `broken` connection, so the partial frame a stall leaves on the wire ends the connection
     at `logit_in` as a truncated frame once the sender drops it. `Ok` increments `in_flight`.
- **`await_ack`.** With `in_flight == 0` it returns `Ok`. Otherwise it reads one control message
  under the request timeout:
  - `Ack` decrements `in_flight`, and drops a `broken` connection once `in_flight` reaches 0.
  - `Reject{GOING_AWAY}` is `Clean`, for every unanswered frame. `logit_in` writes it only for a
    frame it didn't forward, and reads nothing after writing it, so every frame still unanswered
    on that connection was unforwarded.
  - A permanent reject code is `Permanent`.
  - Any other message, an EOF, a reset, or a timeout is `Ambiguous`.
  - Every `Err` drops the connection. The connection is taken into a local for the read, so a
    cancelled `await_ack` drops it.
- **`send` is `submit` then `await_ack`.** `send` calls `submit` with the pending context and
  sequence and `in_flight` 0, then `await_ack`, and clears the pending sequence on `Ok`. Direct
  callers and fake peers that answer `window: 1` see today's behavior.
- **Counters.** `submit` counts only its `Err`s in `logit.output.requests`, and `await_ack`
  counts every result, so a `send` still counts once. `logit.output.ack.duration` records one
  sample per `await_ack`. New gauges: `logit.output.in_flight` and `logit.output.window`.
- **`logit_in`.** It adds `RECEIVER_MAX_WINDOW`, the clamp in `handshake`, `set_nodelay`, and the
  lingering close (`close_lingering(stream, bound)` after the `Fanout` clone is dropped).
  `serve_connection`'s loop is otherwise unchanged. The loop-top shutdown check bounds the reads
  after shutdown to one frame, and the invariant that `GOING_AWAY` is written only for a frame
  that wasn't forwarded holds with frames pipelined.

### 6. Posture and residuals

- **`at_least_once` (the default).** A fault mid-window costs re-encoding and a resend that
  `logit_in` deduplicates.
- **`at_most_once`.** An `Ambiguous` fault drops the whole window. The operator docs recommend
  `window: 1` under this posture; nothing couples the two.
- **A long downstream stall.** The receiver parks on one frame and stops reading, the sender's
  socket buffers fill, and a frame write makes no progress. The progress bound marks the
  connection `broken`, the acks already received deliver their heads, and the parked frame
  alone times out in `await_ack`, is retried, and is dropped when its budget runs out: the
  batch dropped is the stuck one, as at window 1. Each retry round costs about two
  `request_timeout`s, the stalled write and then the ack wait, where window 1 pays one, and
  each costs `logit_in` one `logit.proto.errors{reason="truncated"}` for the partial frame.
- **A slowly draining receiver.** A receiver that forwards a frame every few seconds makes
  progress, so no write stalls, and a fill of `window - 1` frames can take longer than the
  head's retry budget. The head is delivered when the fill ends and its buffered `Ack` is read;
  the budget bounds retries and the head's own submit, never a step past it. Once the head is
  submitted, only a shutdown grace cancels a round.
- **The parked-forward race grows.** The abandoned connection's task can forward several
  buffered frames that race the new connection's resend, so the worst case is a few duplicates
  instead of one. It limits itself: the first forward raises the mark. The `docs/known-gaps.md`
  entry "A forward parked past the sender's ack timeout can be forwarded twice" says so.
- **The shutdown count grows.** A memory store's `finish` counts outstanding frames as
  `dropped{reason="shutdown"}`, and `flush`'s `shutdown()` can then let `logit_in` forward them.
  The over-count is at most one batch today and at most `window` with this record.

## Alternatives considered

- **Credit messages, or a credit field in `Ack`.** A receiver granting credit can shrink the
  window under load, but `logit_in` already applies backpressure by delaying its `Ack`, and a
  fixed window bounded by `RECEIVER_MAX_WINDOW` caps what it must buffer. Credits add a message
  or a field, a state machine on both ends, and a way to deadlock at zero credit, for no case
  the fixed window fails.
- **`Ack` carrying the sequence.** Acks already arrive in frame order on a serial receiver, so
  the k-th `Ack` names its frame. A sequence in `Ack` would serve only out-of-order
  acknowledgment, rejected next, and would change the wire.
- **Out-of-order acknowledgment, with per-record ack state in the spool.** It would let a slow
  batch not hold up later ones, but `logit_in` forwards in order, so nothing is acknowledged out
  of order to gain from. The spool would need a per-record acknowledged bit, persisted, and
  `commit` would stop being a cursor advance.
- **A concurrent receiver.** Decoding or forwarding several frames of one connection at once
  breaks frame-order acks and in-order forwarding, and the high-water mark assumes a sender's
  frames are forwarded in order. The bottleneck this record removes is the round trip, not the
  receiver's per-frame work.
- **Coupling the window to the delivery posture.** Forcing `window: 1` under `at_most_once`
  hides a trade-off the operator can make: a larger window under `at_most_once` loses more per
  `Ambiguous` fault, and an operator on a lossy link may still want it. The operator docs
  recommend `window: 1` there instead.
- **A receiver-side config knob.** The receiver's only cost per window slot is the acks a peer
  can leave unread, about 47 bytes each under TLS, and a constant of 1024 keeps that under the
  default `tcp_rmem`. A knob with no operator driver adds a field and a rule.
- **A sender-side read-ahead buffer of encoded frames instead of a reserved prefix in the
  store.** A disk spool's crash replay must cover frames in flight, so those frames must stay in
  the store until acknowledged. A buffer outside the store would hold them where a crash loses
  them and an eviction can't see them.
- **A bigger default window.** 32 frames is expected to carry about 30 times the one-frame batch
  rate on a 10 ms RTT link, and under `at_most_once` an `Ambiguous` fault drops at most 32
  batches. A higher-latency link raises `window`, up to 1024.

## Consequences

- **Code.** The store's `peek_at` and `max_in_flight`, the `Output` methods and
  `deliver_window`, `logit_out`'s window and `logit_in`'s clamp and lingering close, the config
  field and graph rule 74, and the tests and allocation pins each workstream adds are listed
  once, in [`docs/plans/native-send-window.md`](../plans/native-send-window.md).
- **Telemetry.** `logit.component.send.duration` records one sample per windowed round, not per
  batch, for a `logit_out` with a window above 1. `logit.output.requests` counts each `submit`
  `Err` and each `await_ack` result. `logit.output.ack.duration` records one sample per
  `await_ack`. Two new gauges, `logit.output.in_flight` and `logit.output.window`, show the
  frames outstanding and the negotiated window. `docs/design/internal-telemetry.md` records all
  four.
- **Known gaps.** In `docs/known-gaps.md`, "Credit-based flow control (`window` > 1)" and "No
  out-of-order/credit-based acknowledgement" close. "A forward parked past the sender's ack
  timeout can be forwarded twice" changes to the several-duplicate worst case of decision 6, and
  the `ack_write_stalled` reasoning under "`logit_in`'s `idle_timeout` bounds reads only"
  changes for a peer with a window of unread acks.
- **Operator docs.** The plan's W4 rewrites every passage that says one frame is in flight:
  module docs, `docs/design/wire-protocol.md`, `docs/deploying.md` (the `window:` field and the
  posture note), and the amended records.
- **Measurement owed.** `native-relay` on the perf VM at `window: 1` and `window: 32`, with the
  pending re-baseline. The plan's "Findings" section records the laptop loopback and container
  `netem` numbers until then.
