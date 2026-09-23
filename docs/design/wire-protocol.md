# Native wire protocol

The native protocol carries batches between two `logit` nodes when collection and processing run on
different hosts ([overview](../OVERVIEW.md),
[ADR `native-wire-format-with-otlp-bridge`](../adr/native-wire-format-with-otlp-bridge.md)). OTLP
stays available as an interop codec at ingest and egress. The same frames also back `stdio_out`/
`file_out`'s `format: native` and the `buffer.disk:` spool.

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

The header is 24 bytes (`crates/logit-proto/src/frame.rs::HEADER_LEN`). The two reserved bytes
after `compression` keep every later multi-byte field on a 4-byte boundary and leave room for a
future flag or narrow field; don't remove them as padding. The header is fixed and versioned so a
reader can reject an incompatible future payload format cleanly instead of corrupting the stream.
`write_frame`/`read_frame` implement it, and a socket write and a file append use the same pair
([ADR `native-wire-format-encoding`](../adr/native-wire-format-encoding.md)).

**Only `lz4` is encodable.** The real `zstd` crate builds C through `zstd-sys`, which breaks
[ADR `containerized-development`](../adr/containerized-development.md)'s "no host toolchain needed"
property, and the pure-Rust alternatives aren't competitive on ratio or speed. `Zstd = 2` stays a
reserved discriminant that `write_frame` and `read_frame` both reject with
`CodecError::Unsupported`, per the same ADR.

A reader rejects a frame whose `uncompressed_len` exceeds 64 MiB (`MAX_SANE_UNCOMPRESSED_LEN`) as
`Malformed` on the header alone, before the value sizes a decompression buffer.

## Payload: dictionary-first batches

Telemetry is repetitive: the same attribute keys, and often the same values, recur across a batch.
Before compression, `crates/logit-proto/src/native/` encodes each `EventBatch`
([docs/design/data-model.md](data-model.md)) as:

1. A **dictionary**: every interned `Symbol` used in this batch — attribute/span-attribute keys,
   metric names, metric units, metric descriptions, and log `event_name`s — written once as a
   string.
2. **Events**, each referencing dictionary entries by `u32` index rather than repeating the string.

**v1 dictionary-indexes keys, not string values.** `Value::Str`/`Value::Bytes` payloads, such as
a log message or a repeated tag *value*, are written inline. Keys are the dominant repetition
(`host`, `env`, `service.name`, and so on appear on nearly every event,
`docs/design/data-model.md`), and indexing them costs almost nothing: the dictionary reuses the
interning the in-process `AttrMap` already does ([docs/design/data-model.md](data-model.md)), so it
is largely the symbol table filtered to what this batch uses. Deduplicating values is a separate
follow-up, worth building only if real traffic shows repeated attribute values are common enough to
pay for the complexity.

Compression, when enabled, runs over the dictionary-encoded payload. Each writer chooses it with a
`compression: none | lz4` setting (`logit_out`, `stdio_out`/`file_out`'s native format, and
`buffer.disk:`), defaulting to `none`; `logit_in` can negotiate a `logit_out`'s offer down to
`none`.

## `CODEC_NATIVE_V2`: a provenance trailer

`CODEC_NATIVE_V2` carries everything v1 does plus a mandatory, length-prefixed trailer holding the
batch's `Provenance`: the component that created the batch and the one that most recently handled
it (`docs/design/pipeline-graph.md`'s "Provenance propagation",
[ADR `batch-provenance-on-delivered`](../adr/batch-provenance-on-delivered.md)):

```
payload_v2 := dict | resource attrs | uvarint(event_count) | events...
            | uvarint(trailer_len) | trailer_bytes[trailer_len]
trailer_bytes := (tag: u8, len: uvarint, value: [u8; len])*   -- tag 1 = origin, tag 2 = previous
```

`encode_batch_v2`/`decode_batch_v2` call `encode_batch`/`decode_batch` and add the trailer around
them. The v1 encoding itself is not stable across releases: ADR `metrics-model-v2` reshaped every
record to TLV and added the mandatory `Scope` section (see "Record layout" below), so a frame
encoded before that ADR doesn't decode after it. `logit` is pre-release (ADR `lossless-transit`), so
format changes are straight reshapes with no dual-read compatibility path.

**The trailer length is mandatory**, written as a single `0x00` byte when both fields are absent.
An optional trailer would let a payload truncated exactly at the trailer boundary decode as "no
provenance" instead of failing. That breaks the format's truncation-safety invariant: every proper
prefix of a valid encoding must fail to decode, pinned by
`crates/logit-proto/tests/robustness.rs`'s `assert_every_truncation_fails_cleanly`. Trailer values
are inline, not dictionary-indexed, because `origin`/`previous` are at most two strings per batch
with nothing for a dictionary to amortize.

`Hello.codecs`/`HelloAck.codec` (below) negotiate v2 when both sides offer it and fall back to v1,
without provenance, otherwise, so neither side needs a protocol version bump. `DiskQueue`'s
spooled records (`crates/logit-pipeline/src/disk_queue.rs`) carry the same per-record codec byte,
so a v1 record spooled before an upgrade still replays after it.

## Encoding: decided — hand-rolled

`logit_proto::native` (`crates/logit-proto/src/frame.rs` + `crates/logit-proto/src/native/`) is a
hand-rolled encoder over the dictionary-first layout. [ADR
`native-wire-format-encoding`](../adr/native-wire-format-encoding.md) chose it from a four-arm
bake-off (`crates/logit-bench/src/bakeoff/`, run with `script/bench wire_format`) and a
fidelity/version-skew gate (`crates/logit-bench/tests/wire_format_bakeoff.rs`). Hand-rolling gives
full control over compatibility: every `Value` and every `Event` field is framed as
`tag(1) + len(varint) + payload`, with tested skip-unknown behavior. Neither `rkyv` (true zero-copy,
but no skip-unknown story over a derived type) nor a `postcard`/`serde` encoding offers that without
hand-written support. The ADR has the comparison table, the throughput and size numbers, and the
test-pinned reasons OTLP itself doesn't fit as the internal transport.

### Record layout

Every record type in `crates/logit-proto/src/native/record.rs` is TLV-framed like `Event`'s
fields: `tag(u8) + len(uvarint) + payload`, with an unrecognized tag skipped by its declared length.
**Only non-default field values are written.** An absent field decodes to its type's default (`0`,
`None`, empty), so mostly-default records stay small and a round trip is exact either way.

A list of same-typed records (`Event.metrics`, `MetricRecord.exemplars`,
`SpanRecord.events`/`links`) is `uvarint(count)` followed by `count` entries of
`uvarint(len) + body`. A single embedded record gets its boundary from its enclosing TLV frame, but
a list entry needs its own length prefix so the reader stops before its neighbor's bytes.

`ADR metrics-model-v2` introduced this layout for `MetricRecord`/`LogRecord`/`SpanRecord`/
`SpanLink`/`SpanEvent`. Adding a field is a straight reshape of this module, not a
version-negotiated path; skip-unknown framing guards against a torn write, not mixed-version peers.

**`Event`**

| Tag | Field | Payload |
|---|---|---|
| 1 | `timestamp` | `ivarint` (signed varint — the one field here that isn't fixed-width) |
| 2 | `attributes` | attribute map (dictionary-indexed keys) |
| 3 | `log` | nested `LogRecord` TLV |
| 4 | `metrics` | list of `MetricRecord` TLV |
| 5 | `span` | nested `SpanRecord` TLV |

**`MetricRecord`**

| Tag | Field | Payload |
|---|---|---|
| 1 | `name` | dictionary index (symbol) |
| 2 | `unit` | dictionary index (symbol) |
| 3 | `description` | dictionary index (symbol) |
| 4 | `start_timestamp` | `i64` LE |
| 5 | `exemplars` | list of `Exemplar` TLV |
| 6 | `kind` | `MetricKind` payload — kind tag (`u8`) + `len` (uvarint) + kind-specific body, see below |
| 7 | `flags` | `u32` LE — OTLP `DataPointFlags` bitmask (bit 0 = `MetricRecord::FLAG_NO_RECORDED_VALUE`); skipped on the wire when `0`, same as every other default-valued field |

**`Exemplar`**

| Tag | Field | Payload |
|---|---|---|
| 1 | `timestamp` | `i64` LE |
| 2 | `value` | `f64` LE |
| 3 | `trace` | nested `TraceRef`: 16-byte trace id + span-id presence byte (`0`, or `1` + 8 bytes) + a flags byte |
| 4 | `filtered_attributes` | attribute map |

**`LogRecord`**

| Tag | Field | Payload |
|---|---|---|
| 1 | `message` | `Value` |
| 2 | `severity` | `u8` tag: `0`=Trace, `1`=Debug, `2`=Info, `3`=Warn, `4`=Error, `5`=Fatal |
| 3 | `body_format` | `u8` tag: `0`=Raw, `1`=Json, `2`=Structured |
| 4 | `trace` | nested `TraceRef` (see `Exemplar` above) |
| 5 | `event_name` | dictionary index (symbol) |
| 6 | `observed_timestamp` | `i64` LE |
| 7 | `dropped_attributes_count` | `u32` LE |

**`SpanEvent`**

| Tag | Field | Payload |
|---|---|---|
| 1 | `timestamp` | `i64` LE |
| 2 | `name` | `Value` |
| 3 | `attributes` | attribute map |
| 4 | `dropped_attributes_count` | `u32` LE |

**`SpanLink`**

| Tag | Field | Payload |
|---|---|---|
| 1 | `trace_id` | `[u8; 16]` |
| 2 | `span_id` | `[u8; 8]` |
| 3 | `attributes` | attribute map |
| 4 | `flags` | `u32` LE |
| 5 | `trace_state` | raw bytes |
| 6 | `dropped_attributes_count` | `u32` LE |

**`SpanExt`** (`SpanRecord.ext`'s boxed payload)

| Tag | Field | Payload |
|---|---|---|
| 1 | `status_message` | raw bytes |
| 2 | `trace_state` | raw bytes |
| 3 | `dropped_attributes_count` | `u32` LE |
| 4 | `dropped_events_count` | `u32` LE |
| 5 | `dropped_links_count` | `u32` LE |

**`SpanRecord`**

| Tag | Field | Payload |
|---|---|---|
| 1 | `trace_id` | `[u8; 16]` |
| 2 | `span_id` | `[u8; 8]` |
| 3 | `parent_span_id` | `[u8; 8]` |
| 4 | `name` | `Value` |
| 5 | `kind` | `u8` tag: `0`=Internal, `1`=Server, `2`=Client, `3`=Producer, `4`=Consumer |
| 6 | `status` | `u8` tag: `0`=Unset, `1`=Ok, `2`=Error |
| 7 | `events` | list of `SpanEvent` TLV |
| 8 | `links` | list of `SpanLink` TLV |
| 9 | `end_timestamp` | `i64` LE |
| 10 | `flags` | `u32` LE |
| 11 | `ext` | nested `SpanExt` TLV |

**`Resource`**

| Tag | Field | Payload |
|---|---|---|
| 1 | `attributes` | attribute map |
| 2 | `dropped_attributes_count` | `u32` LE |
| 3 | `schema_url` | raw bytes |

**`Scope`**

| Tag | Field | Payload |
|---|---|---|
| 1 | `name` | raw bytes |
| 2 | `version` | raw bytes |
| 3 | `attributes` | attribute map |
| 4 | `dropped_attributes_count` | `u32` LE |
| 5 | `schema_url` | raw bytes |

**`MetricKind` (the `MetricRecord.kind` field's own payload)**: one kind tag byte, `uvarint(len)`,
then the kind's *fixed, sequential* body. A kind's shape is locked to its tag, so there's nothing to
skip inside one. `read_metric_kind` rejects an unrecognized kind tag as `Malformed` instead of
skipping it, because a metric with no interpretable value can't be carried forward.

| Tag | Kind | Payload |
|---|---|---|
| 0 | `Sum` | `value: f64` LE + `temporality: u8` (`0`=Delta, `1`=Cumulative) + `monotonic: u8` |
| 1 | `Gauge` | `f64` LE |
| 2 | `GaugeDelta` | `f64` LE |
| 3 | `Samples` | `uvarint(n)` + `n` × `f64` LE values + `sample_rate: f64` LE |
| 4 | `Distribution` | `DdSketch::to_java_bytes()`'s blob, verbatim — the only lossless view the wrapped sketch crate exposes |
| 5 | `SetMembers` | `uvarint(n)` + `n` × (`uvarint(len)` + member bytes) |
| 6 | `Set` | `HyperLogLog::to_bytes()`'s blob, verbatim — the wrapped `cardinality_estimator::CardinalityEstimator`'s own `serde` form, driven through a small hand-rolled byte codec and canonicalized (the representation tag's low 2 bits only) so two estimators holding the same members serialize identically regardless of allocation address; pinned to this crate's `cardinality-estimator` dependency version, not a portable interchange format like `Distribution`'s `to_java_bytes` |
| 7 | `Histogram` | `uvarint(n)` + `n` × (`bound: f64` LE + `count: uvarint`) + `temporality: u8` + `sum`/`min`/`max`, each an `Option<f64>` (presence byte, then `f64` LE if present) |
| 8 | `ExponentialHistogram` | `scale: ivarint` + `zero_count: uvarint` + `zero_threshold: f64` LE + `positive` buckets (`offset: ivarint` + `uvarint(n)` + `n` × `uvarint` counts) + `negative` buckets (same shape) + `temporality: u8` + `count: uvarint` + `sum`/`min`/`max` (`Option<f64>` each) |
| 9 | `Summary` | `uvarint(n)` + `n` × (`quantile: f64` LE + `value: f64` LE) + `count: uvarint` + `sum: f64` LE |

**Batch grammar**, `crates/logit-proto/src/native/mod.rs`'s `encode_batch`/`decode_batch`:

```
payload_v1 := dict | resource_section | scope_section | uvarint(event_count) | (uvarint(len) + event_body)*
resource_section := uvarint(len) + resource TLV body
scope_section := presence: u8 (0 | 1)  [+ uvarint(len) + scope TLV body]
```

`scope_section` sits right after `resource_section`, never as an optional *trailing* section. A
trailing one would let a payload truncated at that boundary decode as "no scope", breaking
`robustness.rs`'s "no proper prefix of a valid encoding is itself valid" invariant, the same reason
`CODEC_NATIVE_V2`'s trailer length is mandatory. v2 appends the provenance trailer after this.

## Connection protocol

`logit_out`/`logit_in` (`crates/logit-outputs/src/logit.rs` /
`crates/logit-inputs/src/logit.rs`) implement this protocol. This section summarizes it as built;
[ADR `native-transport-handshake-and-ack`](../adr/native-transport-handshake-and-ack.md) has the
decision record.

- **Transport:** TCP, optionally TLS through `rustls`. `rustls` has no system OpenSSL to link,
  which keeps [ADR `containerized-development`](../adr/containerized-development.md)'s "no host
  toolchain needed" property. QUIC isn't implemented.
- **Control frames.** A handshake or ack is an ordinary frame with
  [`FLAG_CONTROL`](../../crates/logit-proto/src/frame.rs) set in the header's `flags`. Its `codec`
  byte is meaningless, and its `compression` is always `none`: the messages are tiny, and
  compression is itself being negotiated. The payload is hand-rolled TLV over `native::varint`
  (`crates/logit-proto/src/native/control.rs`), with the same `tag(u8) + len(uvarint) + payload`
  shape and skip-unknown behavior as a native-v1 `Event`'s fields:

  | Message | Fields | Sent by |
  |---|---|---|
  | `Hello` | `version`, `codecs`, `compressions`, `max_frame_bytes`, `window` | the connecting side, first |
  | `HelloAck` | `version`, `codec`, `compression`, `max_frame_bytes`, `window` | the listener, once, in reply to a valid `Hello` |
  | `Ack` | `seq` | the listener, once per data frame forwarded |
  | `Reject` | `code`, `message` | either side, closing the connection |

- **Handshake.** The connecting side sends `Hello`. The listener replies with `HelloAck` (codec
  and compression negotiated down to what both sides offer, plus its own `max_frame_bytes` and
  `window`) or with `Reject`. A version mismatch or no shared codec is a clean refusal, not a
  corrupted stream.
- **Sequence numbers are implicit.** TCP is ordered, so the Nth data frame on a connection is seq
  N, and `Ack.seq` is the cumulative count the receiver has forwarded. The native payload carries
  no transport fields.
- **Acknowledgement point:** after the batch is in every downstream inbox (`Fanout::send` returns
  on the listener side), not when it decodes. A stalled downstream delays the ack, which stalls the
  sender's next frame. That is the protocol's backpressure, and it's why `logit_in` needs no
  receive-side queue the way a UDP listener does.
- **Flow control: negotiated, not yet used.** `Hello`/`HelloAck` both carry `window`, but the sender
  keeps one frame outstanding (`docs/plans/native-transport.md`'s "In-flight" decision), and
  `LogitOutput`'s `SinkQueue` `peek`/`commit` holds that frame for retransmit. Credit-based flow
  control (several frames outstanding, cumulative acks) isn't built; negotiating `window` now lets
  it land without a wire-format version bump. `docs/known-gaps.md` tracks it.

## Buffering

`Buffer<T>` (`logit_proto::buffer`) is a bounded, in-process queue between a producer and a slower
or intermittent consumer. It acknowledges instead of popping (`docs/adr/buffered-sink-delivery.md`
has the reasoning):

```rust
pub trait Buffer<T> {
    fn push(&mut self, item: T, weight: u64) -> PushOutcome<T>;
    fn peek(&mut self) -> Option<&T>;  // does not remove, but reserves the head against eviction
    fn commit(&mut self) -> Option<T>; // removes the head, only once delivery succeeded
    fn len(&self) -> usize;
    fn weight(&self) -> u64;
}
```

`peek`/`commit` is the ack mechanism. A `Buffer::pop` would remove an item before delivery is
confirmed, so a failed send would lose the batch. Instead the head stays in place across `peek`
and retries until the caller succeeds, and only `commit` removes it. That is all of in-process
at-least-once delivery, deliberately in order with one item in flight. The native protocol's future
credit-based flow control will need out-of-order acks across several in-flight batches, but they
aren't worth building before that caller exists.

`push` takes the item's weight in bytes, so a buffer can enforce a byte bound (for example,
`EventBatch::estimated_heap_bytes`) alongside an item-count bound without recomputing it. Overflow
follows one of two dropping policies (`OverflowPolicy::DropOldest`, `DropNewest`), and `push`
returns a `PushOutcome<T>` so no drop is silent: `PushOutcome::Evicted` hands back the displaced
item, and `PushOutcome::Rejected` hands back the pushed item unchanged. Blocking until space frees
up isn't a policy here, because a synchronous trait can't block usefully; an async wrapper above
`Buffer` owns that.

`InMemoryBuffer<T>` is the only implementation; `SinkQueue`'s `BoundedQueue<T: Queued>` wraps it.
The disk-backed sink buffer (`crates/logit-pipeline/src/disk_queue.rs`, ADR
`disk-backed-sink-buffer`) doesn't implement `Buffer<T>`: the trait's sync, `&mut self`, generic
shape is the wrong seam for real file I/O over a concrete `(Arc<EventBatch>, TraceContext)`, so
`DiskQueue` has its own async surface.

## Open question

Should the native protocol carry OTLP-encoded payloads unmodified as a passthrough codec, so a
`logit` node can relay OTLP without re-encoding it into the native format? The OTLP codec exists
now (`crates/logit-proto/src/otlp/`), but this remains undecided and undesigned.
