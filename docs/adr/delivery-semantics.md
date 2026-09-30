---
created: 2026-09-29
updated: 2026-09-29
---

# Delivery semantics: at-least-once per hop, posture by duplicate harm, and an effectively-once native hop

## Status
Accepted

## Context

`logit` has a delivery mechanism and no delivery target. Three records each decide a part:

- [ADR `buffered-sink-delivery`](buffered-sink-delivery.md) makes every sink's queue
  at-least-once-capable (`peek`/`commit`) and leaves whether a sink uses that to a posture. The
  posture comes from `Output::duplicate_safe()`, and `buffer.delivery:` overrides it. The record
  rejects "a single fixed delivery guarantee for every sink".
- [ADR `native-transport-handshake-and-ack`](native-transport-handshake-and-ack.md) acknowledges
  a frame once `Fanout::send` returns, and numbers frames implicitly, per connection.
- [ADR `shutdown-accounting-and-cancellation-safety`](shutdown-accounting-and-cancellation-safety.md)
  counts every shutdown loss.

None says what `logit` aims for, so behavior follows from defaults nobody chose:

- **`logit_out` is at-most-once by derivation.** Its `duplicate_safe()` is `false` because "the
  receiver has no dedupe identity", so a lost `Ack` drops the batch. No record says `logit`'s own
  transport should lose a batch where it could resend one.
- **Every sink but four defaults to at-most-once.** `influxdb_out`, `graphite_out`,
  `prometheus_out`, and `null_out` report `true`. For the rest, an attempt whose outcome is
  unknown drops the batch, whether a duplicate would cost an extra log line or a corrupted
  counter.
- **`at_most_once` doesn't hold across a crash with `buffer.disk:`.** `commit` moves the read
  cursor in memory, and the cursor reaches disk on `checkpoint_interval`. A crash replays the
  batch in flight and every batch committed since the last cursor write, under either posture.
- **Three sinks duplicate under `at_most_once`.** `otlp_out`, `datadog_out`, and
  `datadog_trace_out` send several requests per batch and report a connect failure `Clean` after
  an earlier request was accepted. The retry resends what was accepted. [ADR
  `sink-send-path-and-attempt-accounting`](sink-send-path-and-attempt-accounting.md)'s Follow-ups
  leave that classification to its own record. This is that record.
- **An input can acknowledge a batch nothing kept.** `Fanout` skips a closed consumer, counts
  `closed_consumer`, and tells its caller nothing, so `logit_in` writes its `Ack` and an HTTP
  listener its `2xx`.
- **"Lossless" has two meanings.** [ADR `lossless-transit`](lossless-transit.md) uses it for field
  fidelity. `docs/OVERVIEW.md`'s "lossless transport between nodes" reads as a delivery claim.

[`docs/plans/delivery-semantics.md`](../plans/delivery-semantics.md) is the workstream breakdown.
This record changes no code.

## Decision

`logit` targets at-least-once delivery on every hop: where it must choose between losing a batch
and delivering it twice, it delivers it twice. The numbered items say where that holds, where a
wire can't support it, and where `logit` does better.

### 1. Vocabulary

| Term | Meaning |
|---|---|
| Hop | One transfer between a sender and the next receiver: a client to a `logit` input, or a `logit` sink to its destination. `logit_out` to `logit_in` is the native hop. |
| Acknowledgment | The receiver's statement to the sender that it accepted a batch. Item 3 says what a `logit` input's means. |
| Loss | A batch a sender handed over, or tried to, that no destination received. |
| Duplicate | A batch a destination received more than once. |
| At-most-once | The sender gives up on a batch whose outcome it doesn't know. No duplicate, possible loss. |
| At-least-once | The sender resends a batch whose outcome it doesn't know. No loss from that cause, possible duplicate. |
| Effectively-once | At-least-once sending with the receiver discarding the resends it recognizes. |
| Lossless | Field fidelity, as [ADR `lossless-transit`](lossless-transit.md) defines it. Never a delivery claim. |

Exactly-once isn't a term `logit` uses. No hop here can provide it.

### 2. The target is at-least-once per hop

A sink resends a batch whose outcome it doesn't know, inside `buffer.retry_budget`. An input
answers its sender so that a sender that retries keeps the batch until `logit` has it.

The target covers the hop, not the process. These losses stay permitted, and item 11 counts each
one a running process can count:

- a batch whose `buffer.retry_budget` ran out, or whose fault is `Permanent`;
- a batch a `drop_oldest` or `drop_newest` overflow policy evicted;
- a batch in a memory queue when the shutdown grace ran out;
- everything in memory when the process dies: channels, memory sink queues, receive queues,
  accumulators, an `aggregate` window, and a Lua script's state.

`buffer.disk:` is how an operator narrows the last one at a sink (item 8). Nothing narrows it
ahead of the sink.

### 3. An input's acknowledgment means accepted into the pipeline

An acknowledgment from a `logit` input means the batch is in every downstream inbox of that
process. It says nothing about a sink. This is `logit_in`'s `Ack`, an HTTP listener's success
status, `splunk_hec_in`'s `/ack` answer of `true`, and the offset `tail_in` and `docker_in`
checkpoint.

An input doesn't acknowledge a batch no consumer took. When every consumer of a batch is closed,
the input answers as it does for a batch it couldn't deliver: `logit_in` writes no `Ack`, and an
HTTP listener answers its protocol's retryable failure.

End-to-end acknowledgment, where an input answers only once every sink delivered, is a non-goal.
A listener has no view of what its fan-out's sinks did, and a stateful transform such as
`aggregate` has no batch to acknowledge.

### 4. In-process edges carry no identity

An edge between two components is a bounded channel. A full one makes the producer wait. It
drops only into a closed consumer, and it never delivers a batch twice. No sequence number or
batch identifier exists on an edge, because there's no loss or duplicate there for one to repair.
Identity belongs where a batch crosses a process boundary (item 7).

### 5. A sink's default posture follows what a duplicate does at its destination

`Output::duplicate_safe() -> bool` becomes a three-way class. The sink reports it, the runtime
derives the default posture, and `buffer.delivery:` overrides it per component.

| Class | A duplicate at the destination | Default posture | Sinks |
|---|---|---|---|
| Idempotent | Overwrites the first copy | `at_least_once` | `influxdb_out`, `prometheus_out`, `graphite_out`, `null_out` |
| Extra record | Is stored as a second record | `at_least_once` | `otlp_out`, `splunk_hec_out`, `datadog_out`, `datadog_trace_out`, `syslog_out`, `stdio_out`, `file_out`, `logit_out` |
| Corrupts a value | Changes what a stored value means | `at_most_once` | `statsd_out`, `collectd_out` |

An extra record is visible and a reader can discard it. A counter incremented twice can't be
told from a counter that counted twice as much, so `statsd_out` and `collectd_out` keep the
posture that can't produce one.

`logit_out` moves to the idempotent class when item 7 is built.

`graphite_out`'s class holds for a Whisper-backed receiver. An operator with another backend
overrides it.

The `Fault` table in [ADR `buffered-sink-delivery`](buffered-sink-delivery.md) is unchanged:
`Clean` retries under both postures, `Ambiguous` under `at_least_once`, and `Permanent` under
neither.

### 6. A wire that can't confirm delivery bounds its hop

Posture decides what a sink does with an outcome it doesn't know. It can't create knowledge the
wire doesn't carry. On these wires a successful send means the kernel or the TLS session took
the bytes, and a receiver that dies afterward loses them with no fault for the sink to retry:

- a datagram, in either direction: the UDP listeners and sinks, and `statsd_in` and
  `statsd_out` over a Unix datagram socket;
- a stream with no application acknowledgment: `syslog_out`, `statsd_out`, and `graphite_out`
  over TCP, `syslog_in`, `statsd_in`, and `graphite_in` over TCP, and `statsd_in` and
  `statsd_out` over a Unix stream socket.

These hops are best-effort, and that's the wire's property, not a gap. A UDP listener's
`drop_oldest` receive queue stays ([ADR `decoupled-listener-io`](decoupled-listener-io.md)).

### 7. The native hop is effectively-once

`logit_out` to `logit_in` is the one hop where `logit` controls both ends, so it's the one hop
where a resend can be recognized. These are the requirements. The wire layout, the size of the
window, and the spool record's layout are a follow-up record, which supersedes [ADR
`native-transport-handshake-and-ack`](native-transport-handshake-and-ack.md)'s "Sequence numbers
are implicit".

- **Sender identity.** Each `logit_out` component presents an identity in its handshake. It's
  the same across reconnects. With `buffer.disk:` it's the same across a restart.
- **A sender that lost its sequence state takes a new identity.** A restart without a spool
  starts a new sequence, and the receiver must not read its first frame as a resend of the old
  sequence's first frame.
- **A sequence per sender, assigned at enqueue.** A batch gets its sequence number when it
  enters the sink's store, and a disk spool persists the number with the record. A resend on a
  new connection and a replay after a crash both carry the number the batch first had.
- **A bounded window at `logit_in`.** `logit_in` remembers, per sender identity, enough to
  recognize a resend inside a bounded window. It acknowledges a recognized resend and doesn't
  forward it.
- **Outside the window, forward.** A frame `logit_in` can't place, from a sender it has
  forgotten or older than its window, is forwarded. The target prefers the duplicate.
- **Identity is advisory.** Under [ADR `deployment-threat-model`](deployment-threat-model.md) it
  protects against accident, not a peer that lies about who it is.

Deduplication covers a resend of one enqueued batch. It absorbs a resend after a lost `Ack`, the
TLS 1.3 `KeyUpdate` residual in [ADR
`sink-send-path-and-attempt-accounting`](sink-send-path-and-attempt-accounting.md)'s decision 6,
and a spool replay. It doesn't cover a duplicate that arrived at the sending process as two
batches, such as a `tail_in` replay or a client's retry.

`window` stays 1. Credit-based flow control is separate work and this record doesn't design it.

### 8. A disk spool is at-least-once across a crash, under either posture

After a crash, a `buffer.disk:` sink replays the batch that was in flight and every batch
committed since the last cursor write, up to `checkpoint_interval` of them. That includes a
batch the sink delivered and a batch it dropped as `Ambiguous`.

`buffer.disk:` with `at_most_once` stays valid. Posture governs the retry of an unknown outcome
while the process runs, and `at_most_once` then holds across a graceful restart, which persists
the cursor. An operator who puts a spool under `statsd_out` or `collectd_out` accepts that a
crash can replay up to `checkpoint_interval` of counters.

### 9. A multi-request sink reports `Ambiguous` once a request was accepted

Once any request of a `send` is accepted, a later failure that would be `Clean` is `Ambiguous`.
This is `splunk_hec_out`'s `after_delivery` rule, and it applies to `otlp_out`, `datadog_out`,
and `datadog_trace_out`. `Clean` means the destination holds nothing of the batch, and after an
accepted request that's false.

Under item 5 these sinks retry an `Ambiguous` fault by default, so the rule costs no batch. An
operator who sets `at_most_once` on one gets what the posture says: no resend of an accepted
request, and the loss of the requests that hadn't gone.

### 10. A replaying input is at-least-once up to the in-memory queues

`tail_in` and `docker_in` checkpoint an offset once its lines are in every downstream inbox. A
crash replays the lines since the last checkpoint and loses the checkpointed lines that were
still in memory. Two records decide the details:
[ADR `file-tailing-and-docker-json-logs`](file-tailing-and-docker-json-logs.md) and
[ADR `tail-discovery-failure-and-resume-identity`](tail-discovery-failure-and-resume-identity.md).

A replaying input ahead of a sink in the "corrupts a value" class can corrupt a value after a
crash. `logit` doesn't reject that graph. The operator decides whether the pipeline tolerates
it.

### 11. Every permitted loss is counted, and so is every replay

A loss a running process causes is counted with a reason, per [ADR
`shutdown-accounting-and-cancellation-safety`](shutdown-accounting-and-cancellation-safety.md)'s
decision 1 and its four named exceptions. Whether a sink's counter counts per batch or per
attempt is [ADR `sink-send-path-and-attempt-accounting`](sink-send-path-and-attempt-accounting.md)'s
decision 1.

A component that can recognize a replay counts it: the disk spool's
`logit.component.buffer.disk.replayed`, and the frames `logit_in` recognizes and doesn't forward.

### Retry ownership

The runtime owns retry, and a sink's `send` is one attempt. The `Output` trait's doc lists the
bounded resends a sink may make inside one attempt. This record adds none.

## Alternatives considered

- **Per-sink posture with no project target.** The state before this record. Each default was
  defensible alone, and together they made `logit`'s own transport the hop most likely to lose a
  batch.
- **Effectively-once on every hop.** Most destinations offer no identity to deduplicate on. A
  target most hops can't meet says nothing about any of them.
- **`at_least_once` for every sink, counters included.** A duplicated `statsd` counter or
  collectd `COUNTER` is wrong data that looks right. Losing the increment is visible as a gap.
- **At-least-once on the native hop with no deduplication.** It needs no wire change. It also
  sends every duplicate downstream, where a sink in the "corrupts a value" class can't absorb
  it, and `logit` controls both ends of this hop.
- **Sequence numbers on in-process edges.** Item 4: an edge neither drops nor duplicates.
- **Rejecting `buffer.disk:` with `at_most_once` in config validation.** It removes a
  combination whose crash behavior surprises. It also takes durable buffering away from
  `statsd_out` and `collectd_out`, where an operator may prefer a bounded replay after a crash
  to losing the queue.
- **Persisting the cursor before each send under `at_most_once`.** It makes the posture hold
  across a crash. It costs an `fsync` per batch and turns a crash during a send into a loss,
  which is the opposite of the target.
- **An opt-in durable acknowledgment**, where an input answers once the batch is in every
  downstream sink's store. It closes the in-memory window when every sink has a spool. It needs
  a signal that travels backward through every transform, and `aggregate` and Lua `flush()`
  emit batches that correspond to no input batch.
- **End-to-end acknowledgment.** Item 3.

## Consequences

Each of these is a workstream in
[`docs/plans/delivery-semantics.md`](../plans/delivery-semantics.md). Until it lands, the gap is
an entry in [`docs/known-gaps.md`](../known-gaps.md).

- `Output::duplicate_safe()` is replaced by a class, `DeliveryPosture`'s derivation changes, and
  eight sinks change their default posture to `at_least_once`. A deployment that relied on the
  old default sets `buffer.delivery: at_most_once`. This is a pre-release break with no alias.
- An `Ambiguous` fault on those eight sinks is retried for up to `buffer.retry_budget`, 60 s by
  default, where it was dropped at once. A sink whose destination answers `5xx` holds its queue
  head for that long, and its `overflow` policy decides what happens behind it.
- `otlp_out`, `datadog_out`, and `datadog_trace_out` gain `splunk_hec_out`'s rule.
- `logit_in`, `prometheus_in`'s remote-write receiver, and the other HTTP listeners need to
  learn from `Fanout` that no consumer took a batch.
- The native hop gains a wire change, per-sender state at `logit_in`, and a field in the disk
  spool's record. `logit` is pre-release, so the wire changes with no dual-read path.
- `docs/deploying.md`, `docs/datadog.md`, and `docs/splunk.md` describe the old defaults until
  the posture change lands, and change with it.
- `docs/OVERVIEW.md`'s "lossless transport" is read as field fidelity.
