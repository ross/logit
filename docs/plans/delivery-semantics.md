---
created: 2026-09-29
updated: 2026-10-02
---

# Enabling plan: delivery semantics — at-least-once per hop, and an effectively-once native hop

## Goal

Make the code do what [ADR `delivery-semantics`](../adr/delivery-semantics.md) decides. Stream key
`delivery`. `delivery/w0` is the ADR and this plan, and changes no code.

## Non-goals

- End-to-end acknowledgment, and an acknowledgment that waits for a sink's store (ADR item 3).
- Credit-based flow control (`window` > 1) and QUIC on the native transport. (`window` > 1 built
  since: ADR `native-hop-send-window`.)
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

Answered by [ADR `native-hop-identity-and-sequence`](../adr/native-hop-identity-and-sequence.md),
which supersedes "Sequence numbers are implicit" in [ADR
`native-transport-handshake-and-ack`](../adr/native-transport-handshake-and-ack.md). The record
answers each of these:

- [x] where the identity travels and what it is (the v2 batch trailer, not `Hello`; 16 bytes,
  fresh per store open);
- [x] where the sequence travels (the v2 batch trailer, per frame);
- [x] how a disk spool's record carries the sequence (inside the frame's trailer), and what an
  old spool's records replay as (unsequenced, forwarded);
- [x] the window's size and bound per sender (a high-water mark, one number), the bound on
  senders (`max_connections + max_connections / 4`), and eviction (least recently seen);
- [x] what `Ack` means once sequences outlive a connection (the frame is handled; it carries no
  fields);
- [x] how a restart without a spool takes a new identity, and what a spool whose records were all
  unlinked does (takes a new identity; nothing recovers a number);
- [x] the counter for a recognized resend (`logit.input.batches.resends`).

The native-transport record rejected an explicit `seq` field so that one frame's bytes serve a
socket and a file. That no longer binds: `logit_out` re-encodes on every attempt and the spool
decodes every record.

### W5: the native hop's implementation

Per [ADR `native-hop-identity-and-sequence`](../adr/native-hop-identity-and-sequence.md):

- **Trailer tags.** Tag 3 (sender identity, 16 bytes) and tag 4 (sequence, uvarint, first value
  1) in `crates/logit-proto/src/native/mod.rs`, with the unsequenced rule on decode.
- **`SeqId` beside `BatchContext`.** `SeqId { id: [u8; 16], seq: u64 }`, `Copy`, through
  `SinkStore::push`, `peek`, and `Output::observe_batch`, never on `BatchContext` or `Delivered`.
- **Store numbering.** A fresh identity per open and numbering in push order in
  `crates/logit-pipeline/src/queue.rs` and `crates/logit-pipeline/src/disk_queue.rs`.
  `parse_record` returns the pair, and `CONTEXT_LEN`'s doc names a trailer tag alongside a codec
  byte.
- **`Ack`.** Emptied in `crates/logit-proto/src/native/control.rs`, and the
  `Ack.seq == conn.seq` check removed from `logit_out`.
- **The table.** One per `logit_in` component, built in `run_until_shutdown`, with the mark
  rule, the `max_connections + max_connections / 4` bound, and least-recently-seen eviction.
- **Counters.** `logit.input.batches.resends`, `logit.input.senders`, and
  `logit.input.senders.evicted`.
- **Known gap.** A `docs/known-gaps.md` entry for the parked-forward race.
- **Stale text.** `logit_out`'s module doc ("Ack wait" and "Delivery posture"), the module doc
  and `Ack`'s doc in `control.rs`, and the doc on `logit_out`'s `Conn::seq`.
- **Pins**, each updated in the same commit as `docs/design/memory.md`:
  - `disk_queue_push_one_batch`: 33, with the trailer written straight into an output buffer
    sized to fit;
  - `disk_queue_peek_cached_costs_nothing`: stays 0, because `SeqId` is `Copy`;
  - `the_largest_message_of_each_type_fits_the_control_message_cap`'s `Ack` literal;
  - the `ack.seq` assertions and the `control_frame_len(&control::Ack { .. })` call in
    `logit_in`'s tests;
  - the exact-size spool helpers `one_counter_record_len`, `encoded_record_len`, and
    `raw_record`.
- **Tests:**
  - a resend after a lost `Ack` is forwarded once;
  - a spool replay after a crash is forwarded once;
  - an unsequenced frame is forwarded;
  - a new identity after a restart isn't read as a resend;
  - a replayed record keeps its recorded identity;
  - a batch dropped and then replayed stays dropped;
  - a sender evicted from the table is forwarded;
  - two `logit_in` components don't share a table.
- **Fuzzing.** Seeds for tags 3 and 4 in the `native_batch_v2` target, per
  [ADR `out-of-ci-fuzzing`](../adr/out-of-ci-fuzzing.md).
- **Identity independence.** Verify that `random_id_bytes` gives independent identities across
  processes.
- [x] Measure the native relay on the perf VM before and after. Done 2026-10-02
  ([`docs/design/performance.md`](../design/performance.md) §1's native-relay ladder): the
  delivery stack's sender identity and sequence trailer cost nothing measurable. `native-relay` reads
  1.378 µs/event at `4e08c49e`, 1.365 after W3, and 1.350 after W4 through W6 (`958b93e7`), and
  the later steps are the send window, encode-once, and named acks.

### W6: operator docs

Done. W1 through W3 landed their pieces with their own PRs, and W6 completes the rest:

- [x] `docs/deploying.md`: the default postures with the multi-request `Ambiguous` rule, an
  input's acknowledgment and `closed_consumer`, the crash window under either posture and the
  native hop's narrowing of it, the counted UDP shutdown drop, a disk-backed sink's
  `reason="shutdown"`, and the `logit_out` section's sender identity, duplicate cases, and
  counters.
- [x] `docs/datadog.md` and `docs/splunk.md`: the default posture and the `Ambiguous` rule.
- [x] `docs/OVERVIEW.md`: "lossless transport" says field fidelity, delivery gets its own
  sentence, and the native hop is effectively-once.
- [x] `docs/design/wire-protocol.md`: the trailer's sender pair, the spool record's pair, the
  per-identity mark at `logit_in`, the acknowledgment point's two cases, and the sequence as
  never a credit (`window` > 1 built since: ADR `native-hop-send-window`).
- [x] `docs/design/performance.md`: the `native-relay` numbers marked pending re-measurement.
- [x] `docs/design/pipeline-graph.md`, `docs/design/internal-telemetry.md`, and `AGENTS.md`:
  the native hop's acknowledgment without a forward, and the ADR cited beside the
  native-transport record.

## Open questions

- **W3 (answered):** `logit_in` writes `Reject{GOING_AWAY}` for a batch no consumer took, and a
  closed consumer doesn't propagate as a shutdown signal: W3 answers the input's half only (ADR
  `delivery-semantics`, "Amendment: W3 decisions (2026-09-30)"). `splunk_hec_in` answers `500`
  code 8 for a later batch, in the same amendment.
- **W4 (answered):** everything its list names, in [ADR
  `native-hop-identity-and-sequence`](../adr/native-hop-identity-and-sequence.md).
- **W1 (answered):** `stdio_out`'s module doc, in `crates/logit-outputs/src/stdio.rs`, now says
  that a write error is `Permanent` and never retried, that `file_out`'s failed re-open after a
  rotation is `Clean` and retries under both postures, and that posture decides only a write the
  shutdown grace cuts off: under `at_least_once` it stays queued and a `buffer.disk:` spool
  replays it (the file can repeat a block), and under `at_most_once` it's dropped and counted
  `reason="shutdown"`.
