---
created: 2026-09-29
updated: 2026-10-05
---

# Delivery semantics: at-least-once per hop, duplicates absorbed by the data model, and an effectively-once native hop

## Status
Accepted. Superseded in part on 2026-10-01 by [ADR `native-hop-send-window`](native-hop-send-window.md):
item 7's last line, "`window` stays 1". Amended on 2026-10-04: the W3 amendment's open per-edge
`on_full` policy is closed as not planned. Amended on 2026-10-05 by [ADR `native-hop-ack-status`](native-hop-ack-status.md): item 3's native-hop
exception gains a second case, a frame `logit_in` refuses by name, and the pair leads the hop
payload rather than riding in the trailer.

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
  unknown drops the batch, whatever a duplicate would have cost at its destination.
- **`at_most_once` doesn't hold across a crash with `buffer.disk:`.** `commit` moves the read
  cursor in memory, and the cursor reaches disk on a later commit, a segment roll, or shutdown.
  A crash replays the batch in flight and every batch committed since the last cursor write,
  under either posture.
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
- a batch dropped on an `Ambiguous` fault under `at_most_once`, where `statsd_out`'s default
  or an operator's `buffer.delivery:` chose that over a duplicate (item 5);
- a batch a `drop_oldest` or `drop_newest` overflow policy evicted;
- a batch a disk spool couldn't write or read back (`frame_too_large`, `disk_full`,
  `disk_io_error`, `disk_corrupt`);
- a batch sent to a consumer that had already closed (`closed_consumer`, items 3 and 4);
- a batch in a memory queue when the shutdown grace ran out;
- everything in memory when the process dies: channels, memory sink queues, receive queues,
  accumulators, an `aggregate` window, and a Lua script's state.

`buffer.disk:` is how an operator narrows the last one at a sink (item 8). Nothing narrows it
ahead of the sink.

### 3. An input's acknowledgment means accepted into the pipeline

[Narrowed for the native hop by "Amendment: W4 decisions (2026-10-01)": `logit_in` also
acknowledges a frame at or below its sender's high-water mark, with no forward. See also
"Amendment: a rejected `Ack` (2026-10-05)".]

An acknowledgment from a `logit` input means the batch is in every open downstream inbox of
that process, and in at least one. It says nothing about a sink. This is `logit_in`'s `Ack`, an
HTTP listener's success status, `splunk_hec_in`'s `/ack` answer of `true`, and the offset
`tail_in` and `docker_in` checkpoint.

A closed consumer is outside the promise: `Fanout` skips it and counts `closed_consumer`, and
the batch is still acknowledged if another consumer took it. An input doesn't acknowledge a
batch no consumer took. When every consumer of a batch is closed, the input answers as it does
for a batch it couldn't deliver: `logit_in` writes no `Ack`, and an HTTP listener answers its
protocol's retryable failure. Once a listener can see that case (the plan's W3; today `Fanout`
reports nothing), a request that decodes to several batches answers that failure even when an
earlier batch of it was taken, and the sender's retry duplicates that prefix. `splunk_hec_in`
is the one listener whose retryable answer promises more: its `503` code 9 means nothing of
the body was taken, so for a later batch it must answer a retryable status that makes no such
promise.

End-to-end acknowledgment, where an input answers only once every sink delivered, is a non-goal.
A listener has no view of what its fan-out's sinks did, and a stateful transform such as
`aggregate` has no batch to acknowledge.

### 4. In-process edges carry no identity

An edge between two components is a bounded channel. A full one makes the producer wait. It
drops only into a closed consumer, and it never delivers a batch twice. No sequence number or
batch identifier exists on an edge, because there's no loss or duplicate there for one to repair.
Identity belongs where a batch crosses a process boundary (item 7).

### 5. Every sink defaults to `at_least_once`; the data model and the receiver absorb the duplicate

`Output::duplicate_safe()` goes away. The runtime's default posture is `at_least_once` for every
sink, `buffer.delivery:` overrides it per component, and one sink, `statsd_out`, declares
`at_most_once` as its own default.

This is what the protocols and their official senders do. OTLP retries 429, 502, 503, 504, and
its retryable gRPC codes, and its specification says a resend "may result in duplicate data on
the server side". Prometheus remote-write senders must retry a 5xx, and version 2.0 requires a
receiver to be idempotent. The Datadog Agent's forwarder retries timeouts and 5xx for every
metric type alike. Splunk's own guidance for HEC is to resend after a missing acknowledgment and
mark the resend as a possible duplicate. The OpenTelemetry Collector, Vector, and Fluentd's
`forward` with acknowledgments are at-least-once, and none of them varies that by signal or by
metric kind.

What those systems do instead is make the data resilient to a duplicate, and that's where
`logit` puts the responsibility too. The data model and the receiver, not the sender, are on
the hook:

- A metric with an identity at its destination overwrites on a resend: a Prometheus or
  InfluxDB sample at its `(series, timestamp)`, a Datadog series point at its timestamp, a
  Whisper point at its second. A cumulative sum with a start time is the same metric whether
  it arrives once or twice. OpenTelemetry SDKs default to cumulative temporality for this
  reason.
- A log or a span arrives as a second record, which a reader can see and discard.
- A kind the receiver aggregates rather than overwrites adds a resend to its total. Where the
  kind is a delta `Sum` or a delta `Histogram`, the remedy is upstream of the sink, not in its
  posture: an `aggregate` with `temporality: cumulative` turns it into a running total that a
  resend repeats rather than adds. `otlp_out` carries both forms, `splunk_hec_out` carries the
  `Sum` and, under `multi_value: expand`, the `Histogram`, and `collectd_out` carries a
  monotonic `Sum` (as `ABSOLUTE`, and as `COUNTER` once `aggregate` makes it cumulative) and
  drops every `Histogram`. `prometheus_out` needs no remedy: it skips a delta
  outright, so cumulative mode decides whether the metric goes out at all, and what goes out
  overwrites. `datadog_out` has no remedy and needs none for its `Sum`: a Datadog `count`
  carries a per-interval value, so its series route skips a cumulative `Sum`, and a resent
  series point was measured to overwrite at its `(series, timestamp)`. No `aggregate` mode
  makes a `Distribution`, `Samples`, or `Set` cumulative; each window's summary is
  self-contained. An `ExponentialHistogram` and a `Summary` pass through `aggregate`
  unchanged, so a delta `ExponentialHistogram` leaves `otlp_out` delta. So a Datadog
  distribution point or sketch, the APM stats `datadog_out` and `datadog_trace_out` relay, and
  a delta `ExponentialHistogram` have no upstream remedy today, and each is assumed to add on
  a resend until measured. Whether a Splunk metrics index adds a resent running total or
  stores it as a second point is unmeasured too. An operator who sends such a kind accepts
  the double count, as every surveyed sender does, or sets `buffer.delivery: at_most_once` on
  that sink.

`statsd_out` is the one exception. The classic statsd grammar has no timestamp, and `statsd_out`
writes a DogStatsD `|T` only when the event arrived with one, so a resent counter usually has
no identity at its destination, and the remedy above doesn't exist for it. It defaults to
`at_most_once` on every transport: a datagram send that fails after earlier datagrams landed is
`Ambiguous` too, and item 6 bounds what a successful send proves, not the posture.
`collectd_out` is not an exception. Its `ABSOLUTE` values are the aggregated case above, a
resend may double-count at a receiving collectd, and the cumulative remedy applies to it.

`logit_out` is at-least-once like the rest, and item 7 makes its duplicates rare. A resend
outside item 7's window is still forwarded as a second record.

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

- **Sender identity.** [Superseded in part by "Amendment: W4 decisions (2026-10-01)": each
  batch carries the identity of the store it entered, a store takes a fresh one every time it
  opens, a replayed spool record keeps its own, and the handshake carries none.] Each
  `logit_out` component presents an identity in its handshake. It's
  the same across reconnects. With `buffer.disk:` it's the same across a restart.
- **A sender that lost its sequence state takes a new identity.** [Superseded in part by
  "Amendment: W4 decisions (2026-10-01)": every store open takes a new identity, with or without
  a spool, and nothing recovers a sequence.] A restart without a spool
  starts a new sequence, and the receiver must not read its first frame as a resend of the old
  sequence's first frame.
- **A sequence per sender, assigned at enqueue.** A batch gets its sequence number when it
  enters the sink's store, and a disk spool persists the number with the record. A resend on a
  new connection and a replay after a crash both carry the number the batch first had.
- **A bounded window at `logit_in`.** `logit_in` remembers, per sender identity, enough to
  recognize a resend inside a bounded window. It acknowledges a recognized resend and doesn't
  forward it.
- **Outside the window, forward.** [Narrowed by "Amendment: W4 decisions (2026-10-01)": a
  frame at or below its sender's mark is not forwarded, even one the sender dropped and a spool
  replayed.] A frame `logit_in` can't place, from a sender it has
  forgotten or older than its window, is forwarded. The target prefers the duplicate.
- **Identity is advisory.** Under [ADR `deployment-threat-model`](deployment-threat-model.md) it
  protects against accident, not a peer that lies about who it is.

Deduplication covers a resend of one enqueued batch that reaches the same `logit_in` process
while the batch is inside its window. There it absorbs a resend after a lost `Ack`, the TLS 1.3
`KeyUpdate` residual in [ADR
`sink-send-path-and-attempt-accounting`](sink-send-path-and-attempt-accounting.md)'s decision 6,
and a spool replay. It doesn't cover a resend that reaches a `logit_in` that restarted in
between, or a different `logit_in` when the sender's `endpoint` resolves to more than one, and
it doesn't cover a duplicate that arrived at the sending process as two batches, such as a
`tail_in` replay or a client's retry. Each of those is forwarded.

[Superseded in part on 2026-10-01 by [ADR `native-hop-send-window`](native-hop-send-window.md):
`window` is negotiated up to 1024, and a fault resends the window, which `logit_in`
deduplicates.] `window` stays 1. Credit-based flow control is separate work and this record
doesn't design it.

### 8. A disk spool is at-least-once across a crash, under either posture

After a crash, a `buffer.disk:` sink replays the batch that was in flight and every batch
committed since the last cursor write. The cursor is written by a commit once
`checkpoint_interval` has passed since the last write, on a segment roll, at open, and at
shutdown, with no timer. So the batches that replay are those committed within
`checkpoint_interval` after the last write, and after an idle period that write can be any
age. The set includes a batch the sink delivered and a batch it dropped as `Ambiguous`.
[Narrowed for the native hop by "Amendment: W4 decisions (2026-10-01)": `logit_in` doesn't
forward a replayed batch at or below its sender's mark.]

`buffer.disk:` with `at_most_once` stays valid. Posture governs the retry of an unknown outcome
while the process runs, and `at_most_once` then holds across a graceful restart, which persists
the cursor. An operator who puts a spool under `statsd_out`, or under a sink whose destination
aggregates a resend, accepts that a crash can replay counters from that window.

### 9. A multi-request sink reports `Ambiguous` once a request was accepted

Once any request of a `send` is accepted, a later failure that would be `Clean` is `Ambiguous`.
This is `splunk_hec_out`'s `after_delivery` rule, and it applies to `otlp_out`, `datadog_out`,
and `datadog_trace_out`. `Clean` means the destination holds nothing of the batch, and after an
accepted request that's false.

Under item 5 these sinks retry an `Ambiguous` fault by default, so the rule costs no batch. An
operator who sets `at_most_once` on one gets what the posture says: no resend of an accepted
request, and the loss of the requests that hadn't gone.

[Narrowed by "Amendment: per-request verdicts (2026-10-04)": a `Permanent` verdict that names one
request no longer stops the send, so the rule above applies to `Clean` and `Ambiguous` faults
only.]

### 10. A replaying input is at-least-once up to the in-memory queues

`tail_in` and `docker_in` checkpoint an offset once its lines are acknowledged as item 3
defines it. A crash replays the lines since the last checkpoint and loses the checkpointed
lines that were still in memory. Two records decide the details:
[ADR `file-tailing-and-docker-json-logs`](file-tailing-and-docker-json-logs.md) and
[ADR `tail-discovery-failure-and-resume-identity`](tail-discovery-failure-and-resume-identity.md).

A replaying input ahead of a sink whose destination aggregates a resend (item 5) can
double-count after a crash. `logit` doesn't reject that graph. The operator decides whether the
pipeline tolerates it.

### 11. Every permitted loss is counted, and so is every replay

A loss a running process causes is counted with a reason, per [ADR
`shutdown-accounting-and-cancellation-safety`](shutdown-accounting-and-cancellation-safety.md)'s
decision 1 and its four named exceptions. Whether a sink's counter counts per batch or per
attempt is [ADR `sink-send-path-and-attempt-accounting`](sink-send-path-and-attempt-accounting.md)'s
decision 1.

A component that can recognize a replay counts it. Today none can: the disk spool's
`logit.component.buffer.disk.replayed` counts every record resumed after the cursor, backlog
and re-delivery alike, because the spool can't tell them apart. The frames `logit_in`
recognizes and doesn't forward (item 7) are the first replays `logit` can count.

### Retry ownership

The runtime owns retry, and a sink's `send` is one attempt. The `Output` trait's doc lists the
bounded resends a sink may make inside one attempt. This record adds none.

## Alternatives considered

- **Per-sink posture with no project target.** The state before this record. Each default was
  defensible alone, and together they made `logit`'s own transport the hop most likely to lose a
  batch.
- **Effectively-once on every hop.** Most destinations offer no identity to deduplicate on. A
  target most hops can't meet says nothing about any of them.
- **A default posture per sink, by what a duplicate does at its destination.** Three classes:
  a duplicate overwrites, is a second record, or corrupts a value, with the last defaulting to
  `at_most_once`. `otlp_out`, `datadog_out`, `datadog_trace_out`, and `splunk_hec_out` each
  carry kinds from two classes (a span is a second record; a delta sum or a sketch is added),
  so the class is a property of the payload, not the sink. No surveyed sender makes the
  distinction, and the remedy for the aggregated kinds is upstream (item 5).
- **A posture per batch, from the kinds the batch carries.** More precise than any surveyed
  sender, at the cost of a check per batch and a posture that changes with content. Where an
  upstream remedy exists it makes the content one a resend repeats instead, and where none
  exists the surveyed senders resend anyway.
- **`at_least_once` for `statsd_out` too.** A statsd counter has no timestamp and no identity,
  so a resend over a stream double-increments with no way to see it. Losing the increment is
  visible as a gap.
- **At-least-once on the native hop with no deduplication.** It needs no wire change. It also
  sends every duplicate downstream, where a `statsd_out` or an aggregated kind can't absorb
  it, and `logit` controls both ends of this hop.
- **Sequence numbers on in-process edges.** Item 4: an edge neither drops nor duplicates.
- **Rejecting `buffer.disk:` with `at_most_once` in config validation.** It removes a
  combination whose crash behavior surprises. It also takes durable buffering away from
  `statsd_out`, where an operator may prefer a bounded replay after a crash to losing the
  queue.
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
an entry in [`docs/known-gaps/README.md`](../known-gaps/README.md).

- `Output::duplicate_safe()` goes away, `at_least_once` becomes the runtime's default posture,
  and nine sinks change default: `otlp_out`, `splunk_hec_out`, `datadog_out`,
  `datadog_trace_out`, `syslog_out`, `stdio_out`, `file_out`, `collectd_out`, and `logit_out`.
  A deployment that relied on the old default sets `buffer.delivery: at_most_once`. This is a
  pre-release break with no alias.
- An `Ambiguous` fault on those nine sinks is retried for up to `buffer.retry_budget`, 60 s by
  default, where it was dropped at once. A sink whose destination answers `5xx` holds its queue
  head for that long, and its `overflow` policy decides what happens behind it.
- A resend to a destination that aggregates the kind it carries double-counts. Each sink's
  operator doc says which kinds, and where `aggregate`'s `temporality: cumulative` is a remedy
  (a delta `Sum` or `Histogram` at `otlp_out` and `splunk_hec_out`; a delta `Sum` at
  `collectd_out`) and where none exists (`datadog_out`'s distribution points and sketches,
  the APM stats `datadog_out` and `datadog_trace_out` relay, a delta `ExponentialHistogram`,
  and a `Distribution`, `Samples`, or `Set` at any receiver that aggregates it rather than
  storing a timestamped point).
- `otlp_out`, `datadog_out`, and `datadog_trace_out` gain `splunk_hec_out`'s rule.
- `logit_in`, `prometheus_in`'s remote-write receiver, and the other HTTP listeners need to
  learn from `Fanout` that no consumer took a batch.
- The native hop gains a wire change, per-sender state at `logit_in`, and a field in the disk
  spool's record. `logit` is pre-release, so the wire changes with no dual-read path.
- `docs/deploying.md`, `docs/datadog.md`, and `docs/splunk.md` describe the old defaults until
  the posture change lands, and change with it.
- `docs/OVERVIEW.md`'s "lossless transport" is read as field fidelity.

## Amendment: W3 decisions (2026-09-30)

The workstream that closes item 3 settled these:

- **`Fanout` reports whether a batch was taken.** Every send returns a `bool`: `true` when the
  batch is in at least one consumer's inbox at send time, `false` for zero consumers or all
  closed. It never means the batch was processed.
- **`logit_in` answers `Reject{GOING_AWAY, "no consumer took the batch"}` and closes** for a
  frame no consumer took, before the sequence advances. `logit_out` already treats `GOING_AWAY`
  as `Clean` and redials. `GOING_AWAY` now has three causes: shutdown, an idle close, and no
  consumer took the frame ([ADR `native-transport-handshake-and-ack`](native-transport-handshake-and-ack.md)).
- **The other HTTP listeners answer their protocol's retryable failure.** `otlp_in` answers `503`
  with `Retry-After: 1` over HTTP and status 14 (`UNAVAILABLE`) over gRPC, never a
  `partial_success`. `prometheus_in`'s receiver answers `503`. `datadog_in` and `datadog_trace_in`
  answer the `503` with `Retry-After: 1` they use for a busy pipeline, counted under their own
  reason.
- **`splunk_hec_in` answers by how much of the body was taken.** A refused first batch is `503`
  code 9 with `Retry-After: 1`. A refused later batch is `500` code 8 with no `ackId`, because code
  9 promises nothing was taken ([ADR `splunk-hec-relay`](splunk-hec-relay.md)).
- **`tail_in` and `docker_in` freeze and stop.** On the first refused batch the driver stops
  writing its checkpoint and returns, and the node finishes, like a finite `generate_in`. The
  frozen checkpoint is the last one written, at or before the last line a consumer took. A restart
  resumes there and may replay lines a consumer already took.
- **Counter.** Every acknowledging listener counts
  `logit.input.batches.dropped{reason="closed_consumer"}`: every batch of the request no consumer
  took, the refused one included. It isn't disjoint from `logit.component.batches.sent` (`Fanout`
  counted the refused batch there, and once per consumer in
  `logit.component.events.dropped{reason="closed_consumer"}`); batches after it that were never
  offered appear only in the input counter. Listeners with nothing to withhold (UDP and TCP stream
  inputs, `internal`, `generate_in`, the scrape path) stay on `Fanout`'s counter.
- **Scope is direct consumers.** A listener refuses a batch only when every consumer directly
  downstream has closed. A sink closing behind an open transform is still acknowledged. Propagating
  a closure as a shutdown signal, and a per-edge `on_full` policy, stay open
  (`docs/design/pipeline-graph.md`, "Open question: a closed downstream").

## Amendment: W4 decisions (2026-10-01)

[ADR `native-hop-identity-and-sequence`](native-hop-identity-and-sequence.md) decides item 7's
wire layout, window, and spool record. It restates item 7's bullets as decided:

- **Identity is per batch, from its store.** A sink's store, memory or disk, takes a fresh
  16-byte identity every time it opens and numbers the batches it holds from 1. The identity and
  the number ride in the batch's v2 trailer, not in the handshake. [Amended on 2026-10-05 by
  [ADR `native-hop-ack-status`](native-hop-ack-status.md): they lead the hop payload, ahead of the batch.] A replayed spool record keeps
  the identity and number it was written with, so nothing recovers a sequence after a restart.
- **The window is a high-water mark.** Each `logit_in` component keeps one mark per sender
  identity. A frame at or below its identity's mark is acknowledged and not forwarded, and a
  frame above it is forwarded and, once a consumer takes it, raises the mark. A batch the sender
  dropped and a spool later replays is at or below the mark once a later batch of its identity
  was taken, so it stays dropped. Item 7's
  "a frame `logit_in` can't place is forwarded" holds for a frame without an identity, from a
  forgotten sender, or reaching a `logit_in` that restarted.
- **The bound.** The table holds `max_connections + max_connections / 4` identities and evicts
  the least recently seen.
- **The residual race.** A forward parked on a full inbox past the sender's ack timeout can be
  followed by a resend on a new connection that reads a mark not yet advanced, and both copies
  are forwarded. The record accepts the duplicate.
- **Cloning is unsupported.** A running process cloned by a VM snapshot or CRIU shares its
  identity with its clone.

Item 3's "accepted into the pipeline" holds for a frame above its sender's mark. A frame at or
below it is acknowledged on the mark alone: for a resend, that repeats the earlier forward's
acknowledgment; for a batch the sender dropped and a spool replayed, it acknowledges a batch no
consumer took, and the sender commits it. The sender gave that batch up before the replay.

Item 8's "The set includes a batch the sink delivered and a batch it dropped" holds at the sink.
On the native hop, a dropped batch the spool replays is at or below its identity's mark once a
later batch of that identity was taken, and `logit_in` doesn't forward it.

Item 11's first countable replays are `logit.input.batches.resends`: frames `logit_in`
recognizes at or below the mark, acknowledges, and doesn't forward.

## Amendment: a per-edge `on_full` policy isn't planned (2026-10-04)

The W3 amendment's "Scope is direct consumers" bullet left a per-edge `on_full: block | drop`
policy open. It's closed as not planned. A sink isolates itself from its siblings with its own
`buffer.overflow: drop_newest | drop_oldest` or `buffer.disk`, and the default `block` keeps
`tail_in`, the TCP listeners, and the native hop lossless because the sender waits. A transform,
Lua node, or router blocks only on its downstream, so with every sink set to drop, an
intermediate backs up only when it is itself the bottleneck. That's a capacity matter. Finding
the consumer that blocks a fan-out is instrumented instead: `logit.component.inbox.full`,
`inbox.blocked.duration`, and `inbox.batches`, recorded under the consumer
(`docs/design/internal-telemetry.md`, "Inbox side: recorded under the consumer";
`docs/design/pipeline-graph.md`, "Backpressure"). Propagating a closure as a shutdown signal
stays open.

## Amendment: per-request verdicts (2026-10-04)

A `send` that issues several requests used to stop at the first failing request. Against a
destination that accepts only some of the signals or routes a batch carries, that made a
well-formed batch fail on every attempt, even though part of it had landed. `otlp_out` fed by
`internal` and sent to Tempo, which accepts traces only, is the case: each batch's traces request
succeeds and its metrics request fails with gRPC `UNIMPLEMENTED`. No `send` ever returned `Ok`, so
[ADR `buffered-sink-delivery`](buffered-sink-delivery.md)'s sustained-permanent-failure guard
(`PERMANENT_FAILURE_WINDOW`, 60 s) ended the process.

`otlp_out`, `datadog_out`, and `datadog_trace_out` now treat each request's verdict on its own:

1. **Accepted** (2xx or gRPC `OK`, `partial_success` included): record that something was
   accepted and go on.
2. **`Permanent` naming the request** (a 3xx, a 4xx other than `datadog_out`'s 403, gRPC
   `INVALID_ARGUMENT`, `UNIMPLEMENTED`, or an unrecognized code; for `otlp_out` this includes
   HTTP 401 and 403 and gRPC `UNAUTHENTICATED` and `PERMISSION_DENIED`): count the request's records as
   `logit.output.records.dropped{reason="rejected"}` on the attempt that got the verdict, remember
   the first such error, and go on to the next request. A `413` keeps its existing
   `reason="oversize"` count.
3. **`Permanent` refusing the sink** (`datadog_out`'s 403 `api_key_rejected`): stop and return
   it. A Datadog API key is org-wide, so every remaining request would get the same answer, and
   continuing only spends requests. A `datadog_out` `events` route sends one request per event.
   An OTLP credential can be scoped per signal (a Grafana Cloud access policy can grant
   `traces:write` without `metrics:write`), so `otlp_out`'s auth answers fall under item 2.
4. **`Clean` or `Ambiguous`**: stop and return it through item 9's `after_delivery` rule,
   unchanged.
5. **End of the send**: if any request was accepted, return `Ok`, even when others were rejected.
   If none was accepted and some were rejected, return the first rejected error, still
   explicitly `Permanent`.

**Why `Clean` and `Ambiguous` still stop.** The runtime retries the whole batch on either, so
attempting more requests first only adds duplicates to the retry.

**Why a sink-wide refusal stops.** A refusal of Datadog's org-wide API key says nothing about
the request that carried it. Going on would repeat the refusal once per remaining request. An
OTLP auth answer can be scoped to one signal, so no answer to one signal says anything about
another; a token refused on every signal still ends the send with the first rejection,
explicitly `Permanent`, at the cost of at most two more requests per batch.

**The guard.** A wholly refused batch still returns an explicitly `Permanent` error, so a bad
token or bucket keeps tripping the guard after 60 s. A partly accepted batch returns `Ok`, so a
signal-partial destination can't trip it. A batch the destination refuses whole, such as a
metrics-only batch into Tempo, still fails and is dropped, and the guard trips only when every
batch for 60 s is one of those. That is the misconfiguration the guard exists to catch. The
operator remedy for a mixed source is `has_signal` or `keep_signals` ahead of the sink, which
`otlp_out` names in its throttled `signal_rejected` warning. The Datadog sinks reuse their
`request_rejected` diagnostic.

**Counting.** `records.dropped{reason="rejected"}` is a server-verdict drop, so it counts per
attempt, whether or not the send ends `Ok` ([ADR `sink-send-path-and-attempt-accounting`](sink-send-path-and-attempt-accounting.md),
decision 1). Layer 2's `events.dropped{reason="send_failed"}` counts the batch only when the whole
send failed. A partly accepted batch is delivered modulo these counted losses, the posture
OTLP `partial_success` and Splunk's code 6 already have.

**Not covered.** `splunk_hec_out` keeps item 9's stop-at-first-failure behavior. Its bodies are
chunks of one route, and its `400` handling is code-specific. Applying the rule there is a
follow-up that has to account for the code 6 resend and the `Clean` rule for a busy answer before
anything is accepted.

There is no config knob.

## Amendment: four fault classes (2026-10-04)

[ADR `sink-fault-classes`](sink-fault-classes.md) replaces `Permanent` with `Rejected` and
`Refused`, removes `buffer.retry_budget`, and removes the process exit. Two items here change:

- Item 2's first permitted loss, "a batch whose `buffer.retry_budget` ran out, or whose fault is
  `Permanent`", becomes "a batch whose fault is `Rejected`": the destination refused that batch for
  its own content. A `Clean`, `Refused`, or (under `at_least_once`) `Ambiguous` fault retries until
  it succeeds or shutdown cuts it, so no loss arises from a budget.
- Item 5's closing paragraph, the `Fault` table: `Clean` and `Refused` retry under both postures,
  `Ambiguous` under `at_least_once`, and `Rejected` under neither. An `Ambiguous` fault under
  `at_least_once` retries indefinitely, `logit_out` included; `at_most_once` remains the opt-out
  that drops it at once.

The per-request verdicts amendment above reads with `Rejected` for "`Permanent` naming the
request" and `Refused` for "`Permanent` refusing the sink"; its guard paragraph describes the
process exit this record removes.

## Amendment: a rejected `Ack` (2026-10-05)

[ADR `native-hop-ack-status`](native-hop-ack-status.md) gives `Ack` a status. `logit_in` answers a frame it reads but won't take (a body
past its decode budget, one that doesn't decode, an oversize frame within the compressed bound)
with a rejected `Ack` naming it, raises its sender's mark with no forward, and keeps the
connection. That is a second native-hop exception to item 3 beside the at-or-below-the-mark case,
and it isn't an acknowledgment of delivery: it tells the sender the batch was dropped, and the
sender drops it as `Rejected` (item 2's permitted loss). A frame no consumer took still gets
`Reject{GOING_AWAY}` and leaves the mark alone. A rejected `Ack` lost before the sender reads it
lets a mark resume commit the frame as delivered; `docs/known-gaps/native-hop.md` tracks that.
