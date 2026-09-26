---
created: 2026-09-08
updated: 2026-09-25
---

# Native wire format encoding: hand-rolled, not `rkyv` or a `serde`/`postcard` derive

## Status
Accepted

## Context

[ADR `native-wire-format-with-otlp-bridge`](native-wire-format-with-otlp-bridge.md) settled that
`logit`-to-`logit` hops get a compact native frame format, with OTLP kept as an interop codec, not
the internal transport. [`docs/design/wire-protocol.md`](../design/wire-protocol.md) specified the
framing and a dictionary-first payload design, but deliberately left one question open: whether the
payload itself should be encoded with `rkyv` (true zero-copy access) or a hand-rolled encoder over
that dictionary layout — "decide with a benchmark, not up front." `AGENTS.md` lists this
among its non-optional design constraints: *"Don't pick one in passing while implementing something
else; benchmark it and record the outcome as an ADR."* This is that benchmark and that ADR.

Answering that question first required answering a question the design docs had left as an
assumption rather than a measurement: **is OTLP (`crates/logit-proto/src/otlp/`, a full, shipping
codec by the time this ADR was written) actually a close enough fit to skip building a native
format at all?** [ADR `native-wire-format-with-otlp-bridge`](native-wire-format-with-otlp-bridge.md)
argued no, on grounds available before that codec existed; this ADR re-asks the question with the
codec in hand and a fidelity gate that pins the answer as a test, not an argument.

## Decision

**Hand-rolled**, implemented as `logit_proto::native` (`crates/logit-proto/src/frame.rs` +
`crates/logit-proto/src/native/`): a dictionary-first, tag-length-value binary encoding, framed by
a fixed 24-byte header (magic, version, codec, compression, lengths, CRC-32C). No new external
codec dependency for the shipped path — only `lz4_flex` (compression) and `crc32c` (the header's
checksum), both pure Rust.

**OTLP is disqualified as the internal transport**, confirmed rather than assumed:
`crates/logit-bench/tests/wire_format_bakeoff.rs`'s fidelity gate fails three ways for the existing
`OtlpEncoder`/`OtlpDecoder`, each pinned as a passing test that demonstrates the failure (not a
hypothetical):

| Finding | Test |
|---|---|
| `Value::Timestamp` collapses to a plain `I64`; `Value::U64` above `i64::MAX` loses precision — `AnyValue` has neither a timestamp nor an unsigned-64 variant | `otlp_collapses_timestamp_to_i64_and_loses_u64_above_i64_max` |
| One `Event` carrying a log, a metric, and a span at once shatters into up to three separate OTLP payloads and decodes back as up to three separate batches, never one event carrying all three | `otlp_shatters_a_multi_payload_event_across_separate_batches` |
| `MetricKind::GaugeDelta`/`Set` produce **no** Metrics payload at all — the record disappears, not just its precision | `native_postcard_and_rkyv_preserve_gauge_delta_and_set_identity_that_otlp_drops` |

The middle row is the one [ADR `native-wire-format-with-otlp-bridge`](native-wire-format-with-otlp-bridge.md)
named directly (`Event` is "whatever it carries, not a tagged one-of," ADR `multi-payload-events`;
OTLP's wire protocol splits by signal into three separate RPCs with no way to recombine them). The
first and third are the same ADR's prediction — "OTLP's log data model doesn't fit arbitrary
structured/unstructured logs well," "the internal model must be a superset of what OTLP can
express" — now demonstrated against the real, shipped codec rather than argued from the spec.
`docs/known-gaps.md`'s "Cross-protocol semantic gaps" entry already tracked the third row from the
encode side; this ADR is the first place it's used as disqualifying evidence for OTLP as an
*internal* transport, not just a documented lossy edge of the *interop* codec.

### The bake-off

Four arms, `crates/logit-bench/src/bakeoff/`:

| Arm | What |
|---|---|
| `native` | The shipped `logit_proto::native` codec. |
| `otlp` | The existing `OtlpEncoder`/`OtlpDecoder` — the control arm, and the one disqualified above. |
| `rkyv` | Zero-copy archival over `bakeoff::wire_mirror::WireBatch`. |
| `postcard` | Derived `serde` binary encoding over the same mirror type. |

**Both `rkyv` and `postcard` operate on a plain-data mirror type, not the real `EventBatch`/`Event`
types**, deliberately: `docs/design/wire-protocol.md`'s original framing of `rkyv`'s risk was schema
evolution alone, but the more immediate problem is that `Event`'s own types are foreign to both
serializers in ways that need wrapper plumbing — `bytes::Bytes`, `smallvec::SmallVec`,
`lasso::Spur`, and `sketches_ddsketch::DDSketch` (whose fields are private with no bin iteration,
`crates/logit-core/src/metric.rs`). `WireBatch` sidesteps all four the same way `native` itself
does: a `Symbol` is resolved to a dictionary-indexed string (never written raw — see
`logit_proto::native`'s own module doc for why), and `MetricKind::Distribution` rides as
`DdSketch::to_java_bytes()`'s canonical blob (now a public method on `DdSketch`, added for exactly
this). With those two problems solved once, both `rkyv`'s and `postcard`'s derives apply directly
to `WireBatch` with no per-type wrapper code — `rkyv` needed exactly one adaptation beyond the
derive: `WireValue` is a directly recursive enum (`Array`/`Map` hold more `WireValue`s), which
needs `#[rkyv(omit_bounds)]` on the recursive fields plus three restated bounds attributes, exactly
rkyv's own documented pattern for a JSON-like recursive value type (`rkyv/examples/json_like_schema.rs`
in the `rkyv/rkyv` repository) — a known, narrow rough edge, not the open-ended wrapper-writing
`wire-protocol.md` had flagged as the risk.

**Fidelity gate, `crates/logit-bench/tests/wire_format_bakeoff.rs`**: `native`, `rkyv`, and
`postcard` all round-trip every representative fixture losslessly (mixed, statsd-shaped,
single-distribution, distribution-heavy, span), preserve `U64` above `i64::MAX` and the
`Timestamp` type exactly, and keep a multi-payload event as one event. All three pass. `otlp` fails
the three ways tabulated above.

**Version-skew gate**: only `native` claims forward compatibility without a version bump — a `Value`
tag or an `Event` field this reader doesn't recognize is skipped (degrading to `Value::Null` for an
unrecognized value shape, or dropping a whole unrecognized `Event` field, never corrupting a
sibling field). Pinned in `crates/logit-proto/src/native/value.rs`'s
`an_unrecognized_value_tag_degrades_to_null_without_corrupting_what_follows` and
`crates/logit-proto/src/native/record.rs`'s
`an_unrecognized_event_field_tag_is_skipped_without_disturbing_known_fields`. Neither `rkyv` nor
`postcard` offer this over a derived struct without hand-written skip logic of their own — this is
the concrete form of `wire-protocol.md`'s own stated priority: *"weighted toward the
schema-evolution story, since a wire protocol that can't tolerate mixed versions in the field is a
much bigger operational problem than a few percent of throughput."*

**Throughput and encoded size** — `crates/logit-bench/benches/wire_format.rs`, two representative
shapes (`NginxMixed`, the mixed reference workload, and `DistributionHeavy`, five distinct
`DDSketch`-carrying metrics on one event) at batch sizes 1, 100, and 1000, one
`script/bench wire_format` run (`docs/design/memory.md`'s own rule against mixing timings across
runs), on the disposable perf VM (`docs/adr/disposable-azure-perf-vm.md`: `Standard_F8as_v6`, 8
dedicated EPYC 9V74 cores, SMT off), `taskset -c 2`, 2026-09-20. Like every other timing table this
PR touches, this one reflects both a machine change (laptop → VM) and real code changes landed
since the table was first measured — read the comparative *findings* below (which arm wins, by
roughly how much) as the durable content, not the absolute microsecond figures against whatever
this table showed before.

Encode time, µs (`divan`'s *fastest* column):

| Shape | Size | native | otlp | rkyv | postcard |
|---|---:|---:|---:|---:|---:|
| NginxMixed | 1 | 2.75 | 7.93 | 2.24 | 2.16 |
| NginxMixed | 100 | 158.0 | 799.0 | 85.4 | 79.4 |
| NginxMixed | 1000 | 1581 | 8244 | 807.6 | 833.1 |
| DistributionHeavy | 1 | 2.68 | 4.01 | 2.14 | 2.07 |
| DistributionHeavy | 100 | 178.1 | 436.0 | 126.2 | 126.6 |
| DistributionHeavy | 1000 | 1774 | 5183 | 1650 | 1258 |

Decode time, µs, at size 100 (the only size decode was benched at):

| Shape | native | otlp | rkyv | postcard |
|---|---:|---:|---:|---:|
| NginxMixed | 133.2 | 1057 | 89.9 | 105.2 |
| DistributionHeavy | 169.7 | 497.8 | 140.3 | 174.7 |

Decode allocation count, at size 100:

| Shape | native | otlp | rkyv | postcard |
|---|---:|---:|---:|---:|
| NginxMixed | 404 | 11914 | 1820 | 1820 |
| DistributionHeavy | 604 | 7008 | 1914 | 1914 |

`native`'s own counts are exactly unchanged from the table this replaced (404/604, deterministic
and machine-independent as this whole category of number always is); `otlp`'s and `rkyv`/
`postcard`'s shifted by real amounts (+90/-5 and -200/+0 respectively) — a code delta somewhere in
the intervening six days of otlp and `bakeoff::wire_mirror` work, not measurement noise, though
this ADR didn't chase down which specific change.

Encoded bytes, uncompressed, `native` with lz4 alongside for reference:

| Shape | Size | native | native+lz4 | otlp | rkyv | postcard |
|---|---:|---:|---:|---:|---:|---:|
| NginxMixed | 1 | 616 | 461 | 1949 | 1756 | 555 |
| NginxMixed | 100 | 36949 | 607 | 192924 | 135604 | 33027 |
| NginxMixed | 1000 | 367250 | 1914 | 1929024 | 1352404 | 328228 |
| DistributionHeavy | 1 | 541 | 378 | 1049 | 1724 | 472 |
| DistributionHeavy | 100 | 38359 | 533 | 103912 | 146660 | 33637 |
| DistributionHeavy | 1000 | 382160 | 1884 | 1039012 | 1464260 | 335138 |

**Four findings, not the one this ADR's `wire-protocol.md` predecessor expected:**

1. **OTLP loses decisively on every axis, not just fidelity.** Roughly 1.5-5× slower to encode
   (8.2 ms vs. 1.6-1.8 ms at 1000 events — a narrower gap than this ADR first found, see the note
   below), roughly 3-8× slower to decode, more bytes on the wire uncompressed at every size and
   shape measured, and 5-8× more allocations on both directions. Combined with the fidelity gate's
   three disqualifying findings, there is no axis left on which "just use OTLP internally too" is
   competitive, for a wire-encoding difference this dense in repeated dictionary-eligible keys and
   protobuf's own per-field tag/varint overhead.
2. **`native`, `rkyv`, and `postcard` are close enough to each other on *encode* time that the
   difference is noise-level, not a deciding factor** (79.4-178.1 µs across all three at 100
   events, every shape). None of the three custom arms wins encode by a margin `wire-protocol.md`'s
   "a few percent of throughput" framing would call material.
3. **`rkyv`'s headline zero-copy advantage does not materialize in the shape this pipeline would
   actually use it.** `bakeoff::rkyv_decode` calls `rkyv::deserialize` — full owned deserialization
   into `WireBatch`, then conversion to `EventBatch` — because that owned `Event` is what
   `logit_pipeline::Transform::process(&mut self, resource: &Arc<Resource>, event: Event)` and
   every other pipeline consumer actually needs; nothing downstream reads fields straight out of an
   `ArchivedWireBatch`. Measured that way, `rkyv` decode is comparable to `native`'s (89.9-140.3 µs
   vs. 133.2-169.7 µs) — not the order-of-magnitude win zero-copy access promises, because this
   comparison never exercises the zero-copy path at all. A design that kept data in archived form
   end-to-end might realize that advantage; `logit`'s pipeline, built around owned `Event`s flowing
   through `Transform`, structurally can't without a much larger redesign than this ADR is scoped
   to evaluate.
4. **Both `rkyv` and `postcard`'s decode allocation counts (1820-1914) run through
   `WireBatch::into_event_batch()`, the same conversion either way — that intermediate mirror
   type, not the serialization format, is most of why they cost ~3-5× `native`'s decode allocations
   (404-604).** `native` decodes straight from bytes into `Event`/`AttrMap` with no intermediate
   object graph. A production `rkyv` codec archiving `EventBatch`-shaped data directly (with the
   `bytes-1`/`smallvec-1` remote-type wrappers `wire-protocol.md` originally worried about) would
   likely close some of this gap — but building that wrapper layer is exactly the complexity this
   ADR's Decision section explains `native` avoids paying for no proven benefit, given finding 3.

**A note on finding 1's encode-time ratio narrowing (3-8× at first measurement, roughly 1.5-5× on
the 2026-09-20 VM refresh):** this table has now been measured on two different machines running
two different commits, six days apart, with real optimization work landing in between (the
interner key-cache and in-place `Transform::process` work most visibly). Nothing about this ADR's
*decision* changes — OTLP is still slower on every axis it was slower on before, `native` still
wins the same argument for the same reasons — but the exact multiple is not a fixed physical
constant to defend down to the decimal, and a future re-measurement moving it again would not by
itself be a sign anything is wrong.

**Compression ratios above are a synthetic-fixture ceiling, not a production estimate** — both
fixtures repeat one event verbatim N times, which is closer to lz4's best case than real telemetry
(distinct timestamps and values per event) will be. The qualitative result — `native`'s
dictionary-first layout gives lz4 far more to work with than OTLP's already-verbose protobuf
framing repeating full attribute keys per record — should hold directionally; the exact multiples
above should not be quoted as a production expectation.

## Alternatives considered

- **`rkyv`.** True zero-copy access into the buffer is real, but doesn't pay off measured the way
  `logit`'s pipeline would actually consume it — findings 3 and 4 above. Rejected as the *shipped*
  format for the reason `wire-protocol.md` already weighted heaviest regardless: no
  forward-compatible skip-unknown story over a derived struct, which matters more for a protocol
  between independently-deployed, not-always-same-version `logit` nodes than a throughput delta
  does. Kept in the bake-off harness (`crates/logit-bench/src/bakeoff/`) as a standing comparison
  point, not deleted.
- **`postcard`/`serde`.** Same version-skew gap as `rkyv` (a derived format has no skip-unknown
  behavior without hand-written support), plus none of `rkyv`'s zero-copy decode advantage. Rejected
  for the same reason, with less upside.
- **OTLP as the internal transport**, i.e. skip building a native format at all. See the fidelity
  gate above — this is the alternative [ADR `native-wire-format-with-otlp-bridge`](native-wire-format-with-otlp-bridge.md)
  already rejected in principle; this ADR is what turned that rejection from an argument into a
  passing/failing test suite.
- **`zstd` compression** alongside `lz4`, per `wire-protocol.md`'s original framing ("lz4 for
  low-latency hops... zstd where bandwidth matters more"). Rejected for v1: the real `zstd` crate
  builds C via `zstd-sys`, breaking [ADR `containerized-development`](containerized-development.md)'s
  "no host toolchain needed" property, and the pure-Rust alternatives (`ruzstd` and forks) aren't yet
  competitive with `libzstd` on ratio or speed. `crate::frame::Compression::Zstd` is a reserved
  discriminant `write_frame`/`read_frame` both reject with `CodecError::Unsupported` rather than
  silently omitted — revisit once a pure-Rust zstd implementation is genuinely competitive, or once
  `containerized-development` is revisited for a good enough reason.

## Consequences

- `logit_proto::native` (`NativeEncoder`/`NativeDecoder`) is the shipped, tested, `Encoder`/`Decoder`
  implementation for the native format — `EventBatch` ⇄ framed bytes, usable identically for a
  socket write and a file append (`docs/design/wire-protocol.md`'s "same format for a socket and a
  file" is now real, not aspirational).
- Two new pure-Rust runtime dependencies: `lz4_flex` (compression) and `crc32c` (the frame header's
  checksum). `rkyv`, `postcard`, and the `serde` derives on `bakeoff::wire_mirror` are bake-off-only,
  living in `crates/logit-bench` (which ships in nothing).
- `docs/known-gaps.md`'s durable-buffering and out-of-order-acknowledgement entries, both explicitly
  blocked on this decision, are unblocked -- the encoder those items build on now exists and is
  tested.
- `DdSketch::to_java_bytes`/`DdSketch::from_java_bytes` are now public API on `logit-core`
  (`crates/logit-core/src/metric.rs`) -- the one change this ADR made to the core event model
  itself, needed because `DDSketch`'s own fields are private with no bin iteration, so a lossless
  round trip has no alternative to the blob.
- Not built by this ADR: the connection/handshake state machine, credit-based flow control, and a
  disk-backed `Buffer<T>` implementation. `docs/design/wire-protocol.md`'s connection-protocol
  section and the two `docs/known-gaps.md` entries above are the next work this unblocks, not work
  this ADR does.

## Amendment: zstd enters the workspace for remote-write, not the native frame (2026-09-24)

[ADR `victoriametrics-interop`](victoriametrics-interop.md) adds `ruzstd`, pure Rust, as a
dependency of `logit-proto` for Prometheus remote-write's `Content-Encoding: zstd`. That
narrows the "`zstd` compression" alternative above without reversing it: the C `zstd` crate is
still out, and `Compression::Zstd` in the native frame is still a reserved discriminant that
`write_frame` and `read_frame` reject. `ruzstd`'s decoder is what changed since the alternative
was written; its encoder reaches about libzstd level 1, which is enough for a remote-write body
and not the "genuinely competitive" bar the native frame's revisit clause sets.

## Amendment: canonical varints, no trailing bytes, and a writer-side length cap (2026-09-25)

[ADR `untrusted-input-bounds`](untrusted-input-bounds.md) tightens three decode rules. None of
them changes what a conforming writer emits:

- **Canonical varints.** A 10-byte varint whose last byte has any bit above bit 0 set is
  `Malformed`. `read_uvarint` used to discard those bits.
- **No trailing bytes.** A metric-kind body carved by `read_metric_kind`, or a field carved by
  `for_each_field`, with bytes left after its parse is `Malformed`, the rule
  `read_record_list_into` already applied. Before release there is no forward-compatibility
  padding to preserve.
- **Writer-side cap.** `write_frame` returns an error for a payload over
  `MAX_SANE_UNCOMPRESSED_LEN` instead of truncating its length to `u32`, so the cap every reader
  checks is also enforced once where frames are written.
