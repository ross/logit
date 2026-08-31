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

Define a `Buffer` trait now, even though only one implementation ships initially:

```rust
trait Buffer {
    fn push(&mut self, batch: EventBatch) -> Result<()>;
    fn pop(&mut self) -> Option<EventBatch>;
    // ack/retry hooks land here once at-least-once delivery is implemented
}
```

Ship an in-memory implementation first (bounded, with a documented overflow policy — drop-oldest by
default). A disk-backed implementation (for surviving a restart or a downstream outage without data
loss) is a real future need but not a v1 blocker — the trait boundary is what's cheap to add now and
expensive to retrofit onto call sites that assumed an in-memory queue.

## Open question

Whether the native protocol should be able to carry OTLP-encoded payloads unmodified as a passthrough
codec (a `logit` node relaying OTLP without re-encoding into the native format) is worth
revisiting once the OTLP codec exists — deferred rather than designed now.
