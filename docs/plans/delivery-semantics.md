---
created: 2026-09-29
updated: 2026-09-29
---

# Enabling plan: delivery semantics — at-least-once per hop, and an effectively-once native hop

## Goal

Make the code do what [ADR `delivery-semantics`](../adr/delivery-semantics.md) decides. Stream key
`delivery`. `delivery/w0` is the ADR and this plan, and changes no code.

## Non-goals

- End-to-end acknowledgment, and an acknowledgment that waits for a sink's store (ADR item 3).
- Credit-based flow control (`window` > 1) and QUIC on the native transport.
- Durable buffering ahead of a sink.
- A native `dedup` transform. Deduplication here is the native hop's, on a sender's sequence.

## Workstreams

Each is one PR. W1 lands before W2, and W4 before W5. The rest are independent.

| WS | Change | ADR item |
|---|---|---|
| W1 | A duplicate-harm class replaces `Output::duplicate_safe()`, and the default postures follow it | 5 |
| W2 | `otlp_out`, `datadog_out`, and `datadog_trace_out` report `Ambiguous` once a request was accepted | 9 |
| W3 | An input doesn't acknowledge a batch no consumer took | 3 |
| W4 | A record for the native hop's wire: identity, sequence, window, and spool record | 7 |
| W5 | The native hop's implementation | 7 |
| W6 | Operator docs | all |

### W1: duplicate-harm class and default postures

- Replace `Output::duplicate_safe() -> bool` with a method that returns the class, and
  `DeliveryPosture::from_duplicate_safe` with a derivation from it
  (`crates/logit-pipeline/src/output.rs`).
- Assign every sink in `crates/logit-outputs/src/` its class from the ADR's table, and rewrite
  its doc to say what a duplicate does at its destination.
- `logit_out` takes the "extra record" class here and the idempotent class in W5.
- Tests: one per class that an `Ambiguous` fault is retried or dropped as the class says, and
  one that `buffer.delivery:` overrides each.
- Update the posture text in [ADR `buffered-sink-delivery`](../adr/buffered-sink-delivery.md) by
  amendment, and `docs/known-gaps.md`'s "Sink default postures don't follow ADR
  `delivery-semantics` yet" entry closes.

W1 lands first because W2 turns a `Clean` fault into an `Ambiguous` one, which the old default
drops.

### W2: `Ambiguous` after an accepted request

- Move `after_delivery` out of `crates/logit-outputs/src/splunk.rs` to where the four sinks can
  share it, and apply it in `otlp.rs`, `datadog.rs`, and `datadog_trace.rs`, on both of
  `datadog_trace_out`'s transports and both of `otlp_out`'s.
- Tests: for each sink, a connect failure on the second request of a `send` is `Ambiguous`, and
  the same failure on the first is `Clean`.
- The three `docs/known-gaps.md` entries close.

### W3: no acknowledgment for a batch no consumer took

- `Fanout`'s send reports whether any consumer took the batch
  (`crates/logit-pipeline/src/fanout.rs`). A batch some consumers took is still a success.
- `logit_in` writes no `Ack` for a batch no consumer took. Decide what it writes: `GOING_AWAY`
  is `Clean` at `logit_out`, which resends, and it's written only before a forward today.
- `prometheus_in`'s remote-write receiver, `otlp_in`, `datadog_in`, `datadog_trace_in`, and
  `splunk_hec_in` answer their protocol's retryable failure.
- `tail_in` and `docker_in` don't advance past lines no consumer took.
- Open question: `docs/design/pipeline-graph.md`'s "Open question: a closed downstream" asks
  whether a closed consumer should propagate as a shutdown signal. W3 answers the input's half.
  Settle whether it answers the whole question before the PR.

### W4: the native hop's wire record

A record that supersedes "Sequence numbers are implicit" in [ADR
`native-transport-handshake-and-ack`](../adr/native-transport-handshake-and-ack.md). It decides:

- where the identity travels (`Hello`) and what it is;
- where the sequence travels: a field on the data frame, a control frame ahead of it, or the
  frame header's reserved bytes;
- how a disk spool's record carries the sequence, and what an old spool's records replay as;
- the window's size and bound per sender, the bound on senders, and eviction;
- what `Ack.seq` means once sequences outlive a connection;
- how a restart without a spool takes a new identity;
- the counter for a recognized resend.

The native-transport record rejected an explicit `seq` field so that one frame's bytes serve a
socket and a file. W4 says whether that still holds.

### W5: the native hop's implementation

- `logit_out`, `logit_in`, `crates/logit-proto/src/native/control.rs`, and
  `crates/logit-pipeline/src/disk_queue.rs`, per W4.
- `LogitOutput`'s class becomes idempotent.
- Tests: a resend after a lost `Ack` is forwarded once; a spool replay after a crash is
  forwarded once; a frame outside the window is forwarded; a sender with a new identity isn't
  read as a resend.
- A fuzz target for any new decoder surface, per
  [ADR `out-of-ci-fuzzing`](../adr/out-of-ci-fuzzing.md).
- Measure the native relay on the perf VM before and after.

### W6: operator docs

- `docs/deploying.md`: the default postures, the `logit_out` section, and "Durable buffering"
  gain the crash window under either posture. Two statements are stale today and are fixed
  here: UDP datagrams dropped at the grace deadline are counted, and a disk-backed sink can emit
  `reason="shutdown"`.
- `docs/datadog.md` and `docs/splunk.md`: the default posture.
- `docs/OVERVIEW.md`: "lossless transport" says field fidelity, and delivery gets its own
  sentence.
- `docs/design/wire-protocol.md`: the native hop, after W5.

W6 can land in pieces with the workstream each piece describes.

## Open questions

- **W3:** what `logit_in` writes for a batch no consumer took, and whether a closed consumer
  propagates as a shutdown signal.
- **W4:** everything its list names.
- **W1:** whether `stdio_out` and `file_out` need a class at all. Their write errors carry no
  `Fault`, so they're `Permanent` and never retried. The one exception, `file_out`'s failed
  re-open after a rotation, is `Clean`, which retries under both postures. The posture has
  nothing to decide.
