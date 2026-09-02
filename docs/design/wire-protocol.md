# Native wire protocol

The `logit`-to-`logit` protocol for splitting collection from processing across nodes
([overview](../OVERVIEW.md), [ADR 0004](../adr/0004-native-wire-format-with-otlp-bridge.md)). OTLP
remains available as an interop codec at ingest/egress; this document is specifically the efficient
native path between two `logit` nodes.

## Framing

```
magic (4 bytes)            "LGIT"
version (u16)
flags (u16)
codec (u8)                 which payload encoding follows (native v1, future versions, ...)
compression (u8)           none | lz4 | zstd
uncompressed_len (u32)
compressed_len (u32)
crc32c (u32)                over the (possibly compressed) payload
payload (compressed_len bytes)
```

Fixed, versioned header so a future incompatible payload format can still be framed and rejected
(or, later, negotiated) cleanly rather than corrupting the stream.

## Payload: dictionary-first batches

Telemetry is extraordinarily repetitive — the same attribute keys and often the same values recur
across an entire batch. Before compression even enters the picture, encode each `EventBatch`
([docs/design/data-model.md](data-model.md)) as:

1. A **dictionary**: every interned attribute key `Symbol` used in this batch, plus repeated string
   values worth deduplicating, written once.
2. **Events**, each referencing dictionary entries by `u32` index rather than repeating the string.

This reuses the same interning the in-process `AttrMap` already does
([docs/design/data-model.md](data-model.md)), so building the wire dictionary is close to free —
it's largely the symbol table's contents, filtered to what this batch actually uses.

Compression then runs over the dictionary-encoded payload: **lz4** for low-latency hops (the
sidecar-to-local-aggregator case), **zstd** where bandwidth matters more than latency (a
cross-region hop). Configurable per link.

## Encoding: decide with a benchmark, not up front

Two real candidates, deliberately left open pending a prototype rather than committed to now:

- **`rkyv`** — true zero-copy access: read `Event`s straight out of the received buffer with no
  per-event deserialization allocation. The risk is schema evolution: `rkyv`'s story for "an old
  node talks to a new node with an extra field" is weaker than protobuf's, and this protocol is
  specifically for talking between independently-deployed nodes that won't always be on the same
  `logit` version.
- **A hand-rolled encoder** over the dictionary-first layout above. Full control over forward/backward
  compatibility (explicit field tags, defined skip-unknown behavior), at the cost of writing and
  maintaining the encoder/decoder by hand instead of deriving it.

Prototype both against representative batches (mixed logs/metrics/spans, realistic cardinality),
benchmark encode/decode throughput and allocation count, and record the decision as an ADR once
it's made. **The harness for this now exists**: `crates/logit-bench` (`script/bench`) holds
representative fixtures and reports per-benchmark allocation counts alongside timings via `divan` —
which is what this section originally named `criterion` for, chosen instead because it reports
allocation counts natively, and allocation count is the number that matters most here. Add the
encoding prototypes as benches there rather than standing up a second harness; see
[memory.md](memory.md) for how to read its output — weighted toward the schema-evolution story, since a wire protocol that can't
tolerate mixed versions in the field is a much bigger operational problem than a few percent of
throughput.

## Connection protocol

- **Transport:** TCP first; QUIC is a plausible later upgrade (head-of-line-blocking avoidance
  matters less here than getting the format and node-to-node story right first).
- **TLS:** via `rustls`, not OpenSSL — keeps the "no host toolchain needed" property
  ([ADR 0005](../adr/0005-containerized-development.md)) intact, since `rustls` has no system OpenSSL
  dependency to link against.
- **Handshake:** negotiates protocol version, supported codecs, and supported compression before any
  batch is sent, so a version mismatch fails fast and legibly instead of corrupting a stream.
- **Flow control:** credit-based — the receiver advertises how many in-flight batches/bytes it will
  accept, the sender respects it. Combined with per-batch ACKs, this is what makes at-least-once
  delivery semantics addable later (retransmit unacked batches) without redesigning the transport.

## Buffering

`Buffer<T>` (`logit_proto::buffer`) is a bounded, in-process queue between a producer and a
slower/intermittent consumer, with an ack shape rather than a plain pop — see
`docs/adr/0019-buffered-sink-delivery.md` for the reasoning:

```rust
pub trait Buffer<T> {
    fn push(&mut self, item: T, weight: u64) -> PushOutcome<T>;
    fn peek(&self) -> Option<&T>;      // does not remove
    fn commit(&mut self) -> Option<T>; // removes the head, only once delivery succeeded
    fn len(&self) -> usize;
    fn weight(&self) -> u64;
}
```

`peek`/`commit`, not `push`/`pop`, is the ack mechanism: `Buffer::pop` would remove an item before
delivery is confirmed, so a failed send would already have lost the batch. Instead the head stays
in place across `peek`, retried until whatever the caller does with it succeeds, and only then
removed via `commit`. This is the whole of in-process at-least-once delivery — deliberately
in-order and single-in-flight (one queue, one head); out-of-order acks across several in-flight
batches are a real future need for this native protocol's credit-based flow control, but not
something worth building speculatively ahead of a second caller that needs it.

`push` takes the pushed item's weight in bytes alongside it, so a bounded buffer can weigh a
byte-aware bound (e.g. `EventBatch::estimated_heap_bytes`) as well as an item-count one, and never
has to recompute it later. Overflow is one of two dropping policies (`OverflowPolicy::DropOldest`,
`DropNewest`); `push` returns a `PushOutcome<T>` so an eviction is never silent —
`PushOutcome::Evicted` hands back the displaced item, `PushOutcome::Rejected` hands back the
pushed item unchanged. A third overflow behavior, blocking until space frees up, is deliberately
not a variant here: a synchronous trait can't block usefully, so that's a concern of an async
wrapper layered on top of `Buffer`, not of the trait or its implementations.

`InMemoryBuffer<T>` is the one shipping implementation, ships first, and is what `Buffer<T>` is
currently defined against. A disk-backed implementation (for surviving a restart or a downstream
outage without data loss) is a real future need but not a v1 blocker — the trait boundary is what's
cheap to add now and expensive to retrofit onto call sites that assumed an in-memory queue.

## Open question

Whether the native protocol should be able to carry OTLP-encoded payloads unmodified as a passthrough
codec (a `logit` node relaying OTLP without re-encoding into the native format) is worth
revisiting once the OTLP codec exists — deferred rather than designed now.
