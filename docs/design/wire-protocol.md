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
   metric names, metric units, metric descriptions, and log `event_name`s — written once as a
   string.
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

## `CODEC_NATIVE_V2`: a provenance trailer

A second payload codec, `CODEC_NATIVE_V2`, carries everything v1 does plus a mandatory
length-prefixed trailer holding the batch's `Provenance` — which component created it, which
component most recently handled it (`docs/design/pipeline-graph.md`'s "Provenance propagation",
[ADR `batch-provenance-on-delivered`](../adr/batch-provenance-on-delivered.md)):

```
payload_v2 := dict | resource attrs | uvarint(event_count) | events...
            | uvarint(trailer_len) | trailer_bytes[trailer_len]
trailer_bytes := (tag: u8, len: uvarint, value: [u8; len])*   -- tag 1 = origin, tag 2 = previous
```

v1 and v2's *relationship* is untouched — `encode_batch_v2`/`decode_batch_v2` still call
`encode_batch`/`decode_batch` as subroutines and only add the trailer around them.
`encode_batch`/`decode_batch` themselves are not byte-for-byte stable across time, though: ADR
`metrics-model-v2` reshaped every record's own framing to TLV and added the mandatory `Scope`
section described in "Record layout" below, so a frame encoded before that ADR does not decode
after it — `logit` is pre-release (ADR `lossless-transit`), so this is a straight reshape, not a
version-negotiated, dual-read compatibility path. The trailer's length prefix is *mandatory*,
present (as a single `0x00` byte) even when both fields are
absent — deliberately, not an optional convenience: an optional trailer on an otherwise-unchanged
v1 payload would let a payload truncated exactly at the trailer boundary decode as "no provenance"
instead of failing, silently breaking this format's own truncation-safety invariant (every proper
prefix of a valid encoding must fail to decode, pinned by
`crates/logit-proto/tests/robustness.rs`'s `assert_every_truncation_fails_cleanly`). With the
length mandatory, v2 holds the identical invariant v1 does. Each trailer field's value is inline,
not dictionary-indexed: `origin`/`previous` are at most two scalar strings written once per batch,
with no repetition within one payload for a dictionary to amortize.

`Hello.codecs`/`HelloAck.codec` (below) negotiate v2 whenever both sides offer it, falling back to
v1 with provenance simply absent otherwise — no version bump forced on either side. `DiskQueue`'s
spooled records (`crates/logit-pipeline/src/disk_queue.rs`) are self-describing the same way: the
existing per-record codec byte picks v1 or v2 decoding, so an already-spooled v1 record keeps
replaying correctly after an upgrade.

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

### Record layout

Every record type in `crates/logit-proto/src/native/record.rs` is TLV-framed the same way
`Event`'s own fields always were: `tag(u8) + len(uvarint) + payload`, an unrecognized tag skipped
whole by its declared length. **Only non-default field values are written** — an absent field
decodes to that type's own default (`0`, `None`, empty), so a record carrying mostly-default values
stays small on the wire and a round trip is exact whether or not a given field happened to be
present. A list of same-typed records (`Event.metrics`, `MetricRecord.exemplars`,
`SpanRecord.events`/`links`) is `uvarint(count)` followed by `count` length-prefixed entries —
`uvarint(len) + body` each — since, unlike a single embedded record (which gets its boundary for
free from its own enclosing TLV frame), each list entry needs its own length prefix so its reader
knows where to stop instead of consuming its neighbors' bytes. This reshape (`ADR
metrics-model-v2`) replaced `MetricRecord`/`LogRecord`/`SpanRecord`/`SpanLink`/`SpanEvent`'s
previous positional layouts; `logit` is pre-release, so growing a record's field set is a straight
reshape of this module, not a version-negotiated, dual-read path — skip-unknown framing is kept as
hygiene against a torn write, not to support mixed-version readers and writers.

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

**`MetricKind` (the `MetricRecord.kind` field's own payload)** — one kind tag byte, then
`uvarint(len)`, then the kind's own *fixed, sequential* body: unlike a record's fields, a kind
variant's shape is locked to its tag, so there's nothing to skip-unknown inside one. An
unrecognized kind tag is a hard `Malformed` error in `read_metric_kind`, not a skip, since a metric
with no interpretable value can't be meaningfully carried forward.

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

`scope_section` sits right after `resource_section` — never as an optional *trailing* section,
which would let a payload truncated exactly at that boundary decode successfully as "no scope"
instead of failing, breaking `robustness.rs`'s "no proper prefix of a valid encoding is itself
valid" invariant (the same reasoning `CODEC_NATIVE_V2`'s mandatory trailer length above already
follows). v2 appends the unchanged provenance trailer described above, unmodified by this reshape.

## Connection protocol

**Shipped**, as `logit_out`/`logit_in` (`crates/logit-outputs/src/logit.rs` /
`crates/logit-inputs/src/logit.rs`) — see [ADR
`native-transport-handshake-and-ack`](../adr/native-transport-handshake-and-ack.md) for the full
decision record; this section is the as-built summary.

- **Transport:** TCP, optionally TLS via `rustls` (not OpenSSL — keeps the "no host toolchain
  needed" property, [ADR `containerized-development`](../adr/containerized-development.md), intact
  since `rustls` has no system OpenSSL dependency to link against). QUIC remains a plausible later
  upgrade, not attempted here.
- **Control frames.** A control message (handshake or ack) is an ordinary frame with
  [`FLAG_CONTROL`](../../crates/logit-proto/src/frame.rs) set in the header's `flags` — `codec`/
  `compression` are meaningless on one. The payload is hand-rolled TLV over `native::varint`
  (`crates/logit-proto/src/native/control.rs`), the same `tag(u8) + len(uvarint) + payload` shape
  and skip-unknown forward compatibility as a native-v1 `Event`'s own fields:

  | Message | Fields | Sent by |
  |---|---|---|
  | `Hello` | `version`, `codecs`, `compressions`, `max_frame_bytes`, `window` | the connecting side, first |
  | `HelloAck` | `version`, `codec`, `compression`, `max_frame_bytes`, `window` | the listener, once, in reply to a valid `Hello` |
  | `Ack` | `seq` | the listener, once per data frame forwarded |
  | `Reject` | `code`, `message` | either side, closing the connection |

- **Handshake.** The connecting side sends `Hello`; the listener replies `HelloAck` (codec and
  compression negotiated down to the intersection of what both sides offer, its own
  `max_frame_bytes`, its own `window`) or `Reject` — a version mismatch or no shared codec is a
  clean, legible refusal, not a corrupted stream.
- **Sequence numbers are implicit**, not a field on the data frame: TCP is ordered, so the Nth data
  frame on a connection is always seq N, and `Ack.seq` is the cumulative count the receiver has
  forwarded so far. This keeps the native-v1 payload itself untouched by the transport layer.
- **Acknowledgement point:** after the batch is in every downstream inbox (`Fanout::send` returning
  on the listener side), not merely after it decodes. A stalled downstream delays the ack, which
  stalls the sender's next attempt — that *is* this protocol's backpressure, and it's what removes
  the need for a receive-side queue on `logit_in` the way a UDP listener has one.
- **Flow control: negotiated, not yet exercised.** `Hello`/`HelloAck` both carry `window`, but the
  sender only ever has one frame outstanding today (`docs/plans/native-transport.md`'s "In-flight"
  decision) — `LogitOutput`'s `SinkQueue` `peek`/`commit` is the retransmit state for that one
  frame. Credit-based flow control (several outstanding, cumulative acks against them) is real,
  designed-for future work — negotiating `window` now is what lets it land later without a
  wire-format version bump — tracked in `docs/known-gaps.md`, not built yet.

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

`InMemoryBuffer<T>` is the one shipping implementation of this trait, and turns out to be the only
one: a disk-backed sink buffer landed (`crates/logit-pipeline/src/disk_queue.rs`, ADR
`disk-backed-sink-buffer`), but *not* against `Buffer<T>` — that trait's sync/`&mut self`/generic
shape was the wrong seam for an implementation that has to do real file I/O and is concrete over
`(Arc<EventBatch>, TraceContext)`, not generic over `T`. `DiskQueue` implements its own async
surface directly instead. `Buffer<T>`'s role narrows to `InMemoryBuffer<T>` alone; the "cheap to
add now, expensive to retrofit" bet this trait was built on paid off for the *first* buffer this
crate needed (`SinkQueue`'s own `BoundedQueue<T: Queued>` wraps it), just not for the disk-backed
one.

## Open question

Whether the native protocol should be able to carry OTLP-encoded payloads unmodified as a passthrough
codec (a `logit` node relaying OTLP without re-encoding into the native format) is worth
revisiting once the OTLP codec exists — deferred rather than designed now.
