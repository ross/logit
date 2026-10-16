---
created: 2026-10-15
updated: 2026-10-15
---

# Native hop trace context: the hop trailer carries a sampled batch's trace context, and `logit_in` continues it

## Status
Accepted. Amends:

- [ADR `native-hop-ack-status`](native-hop-ack-status.md): decision 1's "The trailer keeps
  provenance only". The trailer also carries an optional trace context, tag 5.
- [ADR `native-hop-no-compatibility`](native-hop-no-compatibility.md): decision 1's
  `CODEC_HOP_BATCH` layout gains tag 5.
- [ADR `native-hop-identity-and-sequence`](native-hop-identity-and-sequence.md): decision 1's
  "the trailer keeps provenance only".
- [ADR `batch-provenance-on-delivered`](batch-provenance-on-delivered.md): the 2026-10-01
  amendment's "the trailer carries provenance only".
- [ADR `internal-span-emission-and-deterministic-sampling`](internal-span-emission-and-deterministic-sampling.md):
  the per-node table's listener row, which `logit_in` no longer follows for a batch that arrives
  with a context, and the sampler's claim that every `logit` process in a split-collection
  topology reaches the same verdict, which a native hop now realizes by carrying the context.

## Context

An instance forwarding its own `internal` telemetry to a central `logit` over the native hop
produced pipeline traces in Tempo that were unrelated across the two hosts. The trace context
restarts at the hop:

- `logit_out` keeps only the provenance from `Output::observe_batch`, and
  `native::encode_hop_batch` takes no trace context.
- `logit_in` calls `Fanout::send_relayed`, which mints `TraceContext::new_root()` for every
  batch it forwards.

So the central instance's spans for a batch start a new trace, and the edge instance's
`logit_out deliver` span has no child on the far side.

The sampler ([ADR `internal-span-emission-and-deterministic-sampling`](internal-span-emission-and-deterministic-sampling.md))
decides from the trace id alone, so two processes with the same `span_sample_rate` already agree
on a trace once they share its id. Carrying the id is what's missing. Carrying it naively couples
the two processes' rates: a receiver that keeps a trace the sender dropped records spans whose
parent no backend ever receives, and Grafana shows the trace as "root span not yet received".

## Decision

`logit_out` writes the batch's incoming trace context into the hop trailer when its own process
records spans for that trace, and `logit_in` sends a batch that arrives with one downstream as a
child of it.

### 1. Wire: trailer tag 5

```
trailer_bytes := (tag: u8, len: uvarint, value: [u8; len])*
                 -- tag 1 origin, tag 2 previous, tag 5 trace context
tag 5 value   := trace_id[16] | span_id[8]
```

- **The value is 24 bytes**, read as `logit_proto::native::HopTrace { trace_id, span_id }`. A tag-5
  field of any other length is `Malformed`, and `logit_in` answers it with
  `Ack{rejected(malformed)}` as it does any malformed body. Only a buggy sender writes one.
- **Any 24-byte value decodes, zeros included.** `Fanout` never mints `TraceContext::default()`,
  so an all-zero context doesn't occur in practice, and reading zeros as "absent" would break the
  module rule that re-encoding a decoded payload reproduces it byte for byte.
- **A repeated tag keeps the last value**, as tags 1 and 2 do.
- **Tags 3 and 4 stay retired** and are skipped as unknown.
- **The trailer length stays mandatory**, so no proper prefix of a hop payload decodes.
- **The field costs 26 bytes** (the tag, a one-byte length, and the 24-byte value) and no
  allocation. A batch with no context costs nothing.

### 2. Sender gate: the tag goes out only for a trace the sender sampled

`logit_out` writes tag 5 only when its own process would record spans for that trace.
`Telemetry::samples_trace(trace_id)`, new in `logit-core`, is true when the handle is live and
`trace_is_sampled(trace_id, span_sample_rate)`, the verdict `write_loop`'s own span guard reaches.
A sender with no `internal` component, or whose sampler dropped the trace, writes no tag.

### 3. The context carried is the batch's incoming context

`logit_out` carries `BatchContext.trace`, the context the batch arrived under, which
`observe_batch` receives and the disk spool's 24-byte record prefix persists. It doesn't carry the
sink's own `deliver` span:

- On the send-window path, `window_round` observes and submits a batch before it's the head, and
  so before its `deliver` span is minted.
- The incoming context is the same on every attempt at a batch, so a resend carries what the
  first attempt did.
- A spool replay restores it from the record prefix.

So `logit_in`'s span is a sibling of the sender's `logit_out deliver` span: both are children of
the span of the node that fed `logit_out`.

### 4. Receiver: `send_relayed` takes a parent

`Fanout::send_relayed(batch, provenance, parent: Option<TraceContext>)`:

| `parent` | Context sent downstream | `logit_in` `send` span's `parent_span_id` |
|---|---|---|
| `Some(p)` | `p.child()`: the same `trace_id`, a fresh `span_id` | `p.span_id` |
| `None` | a fresh root, as before | none |

`logit_in` is the one listener that can inherit a parent. It's the same exception to the listener
rule that `stamp_relayed` is to `stamp` for provenance. The span's `SpanKind` stays `Producer`.

### 5. Spool

`DiskQueue::push` writes no tag 5: the record prefix already holds the context, and `logit_out`
re-encodes the batch with its own gate on every attempt. `parse_record` ignores a tag 5 it finds.

### 6. Not done: the remote-parent flags

`logit_in`'s span doesn't set the OTLP `Span.flags` remote-parent bits
(`CONTEXT_HAS_IS_REMOTE`, `CONTEXT_IS_REMOTE`), so a backend can't tell its parent lives in
another process. `Telemetry::span` has no flags input. `docs/known-gaps/telemetry.md`'s
internal-spans entry tracks it.

### Why the gate removes rate coupling

`trace_is_sampled` is a threshold on the trace id's low bytes, monotone in the rate: a trace kept
at one rate is kept at every higher rate. With `s` the sender's `span_sample_rate` and `r` the
receiver's:

| | The sender kept the trace (tag sent) | The sender dropped it (no tag) |
|---|---|---|
| `r = s` | The receiver keeps it too: one whole trace. | The receiver roots its own trace, sampled independently. |
| `r < s` | The receiver keeps it only if it also passes at `r`: a whole trace, or the sender's subtree alone. | The receiver roots its own trace. |
| `r > s` | Kept at `s` means kept at `r`: a whole trace. | The receiver roots its own trace, kept but disconnected from the sender. |

No cell yields a receiver span whose parent the sender never recorded, so a backend never shows a
cross-host trace as "root span not yet received", provided both processes export to the same
backend. One dangling-parent case remains, and it already exists within one process: a spool
replay after a crash whose upstream span never drained.

The cost: the receiver records spans for more of its traffic than its rate suggests. At `0.1` on
both sides it records about 19% of batches, not 10%, because each batch the sender dropped gets a
fresh root and a fresh roll at the receiver.

A chain through a middle instance with no `internal` component degrades the same way. The middle
instance samples nothing and writes no tag, so the first instance's trace ends at its `deliver`
span and the third instance roots its own.

## Alternatives considered

- **Always carry the context, and send the sender's rate in `Hello` so `logit_in` warns on a
  mismatch.** It detects the coupling instead of removing it, and config on two hosts would then
  have to agree.
- **A span link from a fresh root to the sender's `deliver` span.** It keeps sampling independent,
  but Grafana doesn't stitch links into one trace, and one trace across hosts is the ask.
- **A propagated sampled bit on `TraceContext`.** [ADR
  `internal-span-emission-and-deterministic-sampling`](internal-span-emission-and-deterministic-sampling.md)
  rejected it for `Delivered`'s byte cost. The gate is that bit, paid once per frame on the wire
  instead of on every `Delivered`.
- **Parent on the `deliver` span**, by remembering a per-position child context in `write_loop`'s
  `InFlight` state. It gives a tighter tree at the cost of more state in the send window.

## Consequences

- **API.** `encode_hop_batch` takes an `Option<HopTrace>`; `decode_hop_body` returns
  `(EventBatch, Provenance, Option<HopTrace>)` and `decode_hop_batch` returns
  `(EventBatch, Provenance, Option<HopTrace>, SeqId)`. `Fanout::send_relayed` and
  `send_relayed_blocking` take an `Option<TraceContext>` parent. `Telemetry::samples_trace` is new.
  `LogitOutput` keeps the whole pending `BatchContext` from `observe_batch`, not only its
  provenance.
- **Measured costs.** No `size_of` pin moves, and no allocation pin: the field adds no
  allocation on either side, so [memory.md](../design/memory.md)'s `logit_out` encode and
  `logit_in` decode rows stay 31 and 7.
- **No compatibility path.** `logit` is pre-release, and the codec byte and protocol version don't
  change ([ADR `native-hop-no-compatibility`](native-hop-no-compatibility.md)). A reader that
  doesn't know tag 5 skips it by its length, the rule every unknown trailer tag follows.
- **Sampling.** The receiver's rate decides how much of a sender's trace it continues: every
  trace the sender kept crosses a hop whole when the receiver's rate is at or above the sender's
  ([internal-telemetry.md](../design/internal-telemetry.md)'s "The sampler: deterministic on `trace_id`", and
  [deploying.md](../deploying.md)'s "Forwarding between `logit` nodes" for operators).
- **Docs.** `docs/design/wire-protocol.md`'s hop grammar and trailer,
  `docs/design/internal-telemetry.md`'s per-node table and sampler,
  `docs/design/pipeline-graph.md`'s "Trace context propagation", and
  `docs/known-gaps/telemetry.md`'s internal-spans entry describe the result.
