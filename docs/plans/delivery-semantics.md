---
created: 2026-09-29
updated: 2026-09-30
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

Each is one PR. W1 lands before W2 and W5, and W4 before W5. W6 lands in pieces, each with
the workstream it describes. W3 is independent.

| WS | Change | ADR item |
|---|---|---|
| W1 | `Output::duplicate_safe()` goes away and `at_least_once` becomes the default posture | 5 |
| W2 | `otlp_out`, `datadog_out`, and `datadog_trace_out` report `Ambiguous` once a request was accepted | 9 |
| W3 | An input doesn't acknowledge a batch no consumer took | 3 |
| W4 | A record for the native hop's wire: identity, sequence, window, and spool record | 7 |
| W5 | The native hop's implementation | 7 |
| W6 | Operator docs | all |

### W1: `at_least_once` by default

- Remove `Output::duplicate_safe()` and `DeliveryPosture::from_duplicate_safe`
  (`crates/logit-pipeline/src/output.rs`). The runtime's default is `at_least_once`. Decide how
  `statsd_out` declares its `at_most_once` default: a method on `Output` that returns the
  default posture, with the trait default `at_least_once`, is the smallest shape.
- Rewrite every sink's posture doc in `crates/logit-outputs/src/` to say what a resend does at
  its destination, which kinds the destination aggregates, and the `aggregate`
  `temporality: cumulative` remedy where one applies (a delta `Sum` or `Histogram` at
  `otlp_out` and `splunk_hec_out`; a delta `Sum` at `collectd_out`). `prometheus_out`'s doc
  says it skips a delta, so a resend has nothing to add. `datadog_out`'s doc says the remedy
  doesn't apply to it: its series route skips a cumulative `Sum`, and its sketches and APM
  stats have no remedy. `datadog_trace_out`'s doc says the same of the APM stats it relays.
- `logit_out` defaults to `at_least_once` here, before W5 can deduplicate. Between W1 and W5 a
  resend after a lost `Ack` reaches `logit_in`'s consumers twice, and a `statsd_out` or an
  aggregated kind behind that `logit_in` double-counts it. `buffer.delivery: at_most_once` on
  the `logit_out` avoids that until W5.
- Tests: an `Ambiguous` fault is retried by default and dropped on `statsd_out`, and
  `buffer.delivery:` overrides each.
- Update the posture text in [ADR `buffered-sink-delivery`](../adr/buffered-sink-delivery.md) by
  amendment, and `docs/known-gaps.md`'s "Sink default postures don't follow ADR
  `delivery-semantics` yet" entry closes.

W1 lands before W2 because W2 turns a `Clean` fault into an `Ambiguous` one, which the old
default drops.

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
  `splunk_hec_in` answer their protocol's retryable failure. `splunk_hec_in` answers a `503`
  code 9 only when no batch of the request was taken; for a later batch it needs a retryable
  status that doesn't carry code 9's "nothing taken" promise. Decide which.
- `tail_in` and `docker_in` don't advance past lines no consumer took.
- Open question: `docs/design/pipeline-graph.md`'s "Open question: a closed downstream" asks
  whether a closed consumer should propagate as a shutdown signal. W3 answers the input's half
  only; the rest stays open (answered in the ADR's "Amendment: W3 decisions (2026-09-30)").

### W4: the native hop's wire record

A record that supersedes "Sequence numbers are implicit" in [ADR
`native-transport-handshake-and-ack`](../adr/native-transport-handshake-and-ack.md). It decides:

- where the identity travels (`Hello`) and what it is;
- where the sequence travels: a field on the data frame, a control frame ahead of it, or the
  frame header's reserved bytes;
- how a disk spool's record carries the sequence, and what an old spool's records replay as;
- the window's size and bound per sender, the bound on senders, and eviction;
- what `Ack.seq` means once sequences outlive a connection;
- how a restart without a spool takes a new identity, and how a spool whose records have all
  been unlinked recovers its next sequence number or takes a new identity too;
- the counter for a recognized resend.

The native-transport record rejected an explicit `seq` field so that one frame's bytes serve a
socket and a file. W4 says whether that still holds.

### W5: the native hop's implementation

- `logit_out`, `logit_in`, `crates/logit-proto/src/native/control.rs`, and
  `crates/logit-pipeline/src/disk_queue.rs`, per W4.
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

- **W3 (answered):** `logit_in` writes `Reject{GOING_AWAY}` for a batch no consumer took, and a
  closed consumer doesn't propagate as a shutdown signal: W3 answers the input's half only (ADR
  `delivery-semantics`, "Amendment: W3 decisions (2026-09-30)"). `splunk_hec_in` answers `500`
  code 8 for a later batch, in the same amendment.
- **W4:** everything its list names.
- **W1 (answered):** `stdio_out`'s module doc, in `crates/logit-outputs/src/stdio.rs`, now says
  that a write error is `Permanent` and never retried, that `file_out`'s failed re-open after a
  rotation is `Clean` and retries under both postures, and that posture decides only a write the
  shutdown grace cuts off: under `at_least_once` it stays queued and a `buffer.disk:` spool
  replays it (the file can repeat a block), and under `at_most_once` it's dropped and counted
  `reason="shutdown"`.
