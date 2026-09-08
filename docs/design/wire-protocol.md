# Native wire protocol

The `logit`-to-`logit` protocol for splitting collection from processing across nodes
([overview](../OVERVIEW.md), [ADR `native-wire-format-with-otlp-bridge`](../adr/native-wire-format-with-otlp-bridge.md)). OTLP
remains available as an interop codec at ingest/egress; this document is specifically the efficient
native path between two `logit` nodes.

## Framing

```
magic (4 bytes)             "LGIT"
version (u16)
flags (u16)
codec (u8)                  which payload encoding follows (native v1, future versions, ...)
compression (u8)            none | lz4 | zstd (reserved, not yet encodable -- see below)
reserved (2 bytes)
uncompressed_len (u32)
compressed_len (u32)
crc32c (u32)                over the (possibly compressed) payload
payload (compressed_len bytes)
```

24 bytes total (`crates/logit-proto/src/frame.rs::HEADER_LEN`) — the two reserved bytes after
`compression` keep every following multi-byte field on a 4-byte boundary, and are spare room for a
future flag or narrow field, not padding to be removed. Fixed, versioned header so a future
incompatible payload format can still be framed and rejected (or, later, negotiated) cleanly rather
than corrupting the stream. `write_frame`/`read_frame` are the shipped implementation, and are
deliberately the same function pair a socket write and a file append both use — see
[ADR `native-wire-format-encoding`](../adr/native-wire-format-encoding.md).

**Only `lz4` is encodable today.** The real `zstd` crate builds C via `zstd-sys`, breaking
[ADR `containerized-development`](../adr/containerized-development.md)'s "no host toolchain needed"
property, and the pure-Rust alternatives aren't yet competitive on ratio or speed — `Zstd = 2`
stays a reserved discriminant `write_frame` and `read_frame` both reject with
`CodecError::Unsupported`, per the same ADR.

`uncompressed_len` is bounded at 64 MiB (`MAX_SANE_UNCOMPRESSED_LEN`) before it is used to size a
decompression buffer — a frame declaring more is rejected as `Malformed` on the header alone.

## Payload: dictionary-first batches

Telemetry is extraordinarily repetitive — the same attribute keys and often the same values recur
across an entire batch. Before compression even enters the picture, each `EventBatch`
([docs/design/data-model.md](data-model.md)) is encoded (`crates/logit-proto/src/native/`) as:

1. A **dictionary**: every interned `Symbol` used in this batch — attribute/span-attribute keys,
   metric names, and metric units — written once as a string.
2. **Events**, each referencing dictionary entries by `u32` index rather than repeating the string.

**v1 dictionary-indexes keys, not string values.** `Value::Str`/`Value::Bytes` payloads (a log
message, a repeated tag *value*) are written inline rather than through the dictionary — the
"repeated string values worth deduplicating" this section originally described for *values* too.
Keys are the dominant repetition in practice (`host`, `env`, `service.name`, ... on nearly every
event, `docs/design/data-model.md`) and dictionary-indexing them costs nothing extra to build (the
process interner already resolved every one of them once); value-deduplication is a real, separable
follow-up once it's clear from real traffic that repeated attribute *values* (not just keys) are
common enough to be worth the added complexity — not designed away, just not v1.

This reuses the same interning the in-process `AttrMap` already does
([docs/design/data-model.md](data-model.md)), so building the wire dictionary is close to free —
it's largely the symbol table's contents, filtered to what this batch actually uses.

Compression then runs over the dictionary-encoded payload: **lz4** for low-latency hops (the
sidecar-to-local-aggregator case), **zstd** where bandwidth matters more than latency (a
cross-region hop). Configurable per link.

## Encoding: decided — hand-rolled

**Settled by [ADR `native-wire-format-encoding`](../adr/native-wire-format-encoding.md), on a four-arm bake-off
(`crates/logit-bench/src/bakeoff/`, run via `script/bench wire_format`) plus a fidelity/version-skew
gate (`crates/logit-bench/tests/wire_format_bakeoff.rs`).** A hand-rolled encoder over the
dictionary-first layout below, shipped as `logit_proto::native`
(`crates/logit-proto/src/frame.rs` + `crates/logit-proto/src/native/`) — full control over
forward/backward compatibility (explicit `tag(1) + len(varint) + payload` framing on every `Value`
and every `Event` field, with a defined, tested skip-unknown behavior for both), which the bake-off
confirmed neither `rkyv` (true zero-copy, but no skip-unknown story over a derived type) nor a
`postcard`/`serde` encoding offers without hand-written support of their own. See the ADR for the
comparison table, the throughput/size numbers, and — the other question this bake-off had to answer
first — the concrete, test-pinned reasons OTLP itself isn't a close enough fit to be the internal
transport at all.

## Connection protocol

- **Transport:** TCP first; QUIC is a plausible later upgrade (head-of-line-blocking avoidance
  matters less here than getting the format and node-to-node story right first).
- **TLS:** via `rustls`, not OpenSSL — keeps the "no host toolchain needed" property
  ([ADR `containerized-development`](../adr/containerized-development.md)) intact, since `rustls` has no system OpenSSL
  dependency to link against.
- **Handshake:** negotiates protocol version, supported codecs, and supported compression before any
  batch is sent, so a version mismatch fails fast and legibly instead of corrupting a stream.
- **Flow control:** credit-based — the receiver advertises how many in-flight batches/bytes it will
  accept, the sender respects it. Combined with per-batch ACKs, this is what makes at-least-once
  delivery semantics addable later (retransmit unacked batches) without redesigning the transport.

## Buffering

`Buffer<T>` (`logit_proto::buffer`) is a bounded, in-process queue between a producer and a
slower/intermittent consumer, with an ack shape rather than a plain pop — see
`docs/adr/buffered-sink-delivery.md` for the reasoning:

```rust
pub trait Buffer<T> {
    fn push(&mut self, item: T, weight: u64) -> PushOutcome<T>;
    fn peek(&mut self) -> Option<&T>;  // does not remove, but reserves the head against eviction
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
