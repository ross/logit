---
created: 2026-10-15
updated: 2026-10-15
---

# Enabling plan: native encode without per-field scratch buffers

## Goal

Make `logit_proto::native`'s encoder write a frame with a fixed, small number of allocations and
one copy of each event's bytes, and leave every byte it writes unchanged. Today it allocates a
scratch buffer for every length-prefixed field and value and copies each event's bytes four or
five times on the way to a socket or a spool file; on `native-relay` that work is about 17% of the
scenario's CPU. Stream key `nenc`. `nenc/w0` is this plan and changes no code.

## Non-goals

- **The decode side.** `read_event` and `decode_hop_body` have their own copy and allocation costs
  (about 7% of `native-relay`'s samples sit in unresolved `libc` leaves under `read_event`), but
  the decoder's rules (the decode budget, consume-whole, canonical re-encoding) make it a separate
  piece of work.
- **Pre-sizing `AttrMap`, or any change to `Event`.**
  [ADR `event-sizing-and-allocation-strategy`](../adr/event-sizing-and-allocation-strategy.md)
  rejected it on measurement.
- **Changing the wire format.** Every frame this encoder writes stays byte-identical, so no
  decoder, spool file, or peer notices the change. A change that needs a new byte (a fixed-width
  length prefix, say) is a wire change and needs its own ADR, per
  [ADR `native-wire-format-encoding`](../adr/native-wire-format-encoding.md).

## Evidence

The 2026-10-15 perf VM session ([`docs/design/performance.md`](../design/performance.md) §1,
"`native-relay` is code-layout-sensitive") captured six `cpu-clock` flamegraphs of `native-relay`
on two release builds. Leaf frames in jemalloc's entry points and in `BytesMut`/`RawVec` growth are
20.6% of samples. Attributed to the innermost `logit` frame above them, 61.2% of that is under
`native::encode_events`, 14.8% under `value::write_value`, and 6.1% under `write_attr_map`: about
17% of total CPU. In the same session `encode-native-devnull` became the most expensive of the three
`/dev/null` encode scenarios (0.969 µs/event, against 0.906 for `format: json`).

The code accounts for it:

- **Every length-prefixed field builds into a temporary.** `record::write_field` allocates a
  `BytesMut`, builds the payload into it, then writes the tag, the length, and a copy of the
  payload into its parent. `write_record_list` does the same per list entry, and
  `write_metric_kind` per kind. A nested value pays one temporary per nesting level, and each level
  copies its bytes again.
- **So does every varint scalar.** `value::write_value` builds an `I64`, `U64`, or `Timestamp`
  value's varint in a fresh `BytesMut` only to learn its length, though `varint::uvarint_len`
  already computes an unsigned varint's length.
- **Each event's body is a fresh buffer.** `record::write_event` returns a new `BytesMut` per
  event, which `encode_events` copies into `events_buf`.
- **The sections are copied again into the output.** `resource_buf`, `scope_buf`, and `events_buf`
  start empty, grow by doubling, and are copied into `out`, which also starts empty and grows. The
  dictionary is written first but built last, so the event bytes can't go straight into `out`
  without a change to how the dictionary is built.
- **The hop and frame layers copy the whole payload twice more.** `encode_hop_batch` sizes its
  output exactly but copies `encode_batch`'s finished payload into it, and
  `frame::write_frame_with_flags` copies the payload into a `Vec` (`payload.to_vec()` under
  `Compression::None`) and then into the framed `BytesMut`.
- **The dictionary builder is per frame.** `DictBuilder` allocates a `Vec` and a `HashMap` with
  `RandomState` for every frame.

`docs/design/memory.md` pins the result: `NativeEncoder::encode` of one nginx event costs 30
allocations, and `kafka_out`'s 100-event native batch costs 2,010, "20 per event for the per-field
TLV scratch buffers, 10 for the frame's dictionary, section buffers, and output".

Every native writer pays this:

| Caller | Path |
|---|---|
| `logit_out` | `encode_hop_batch`, then `write_frame_with_flags` (`crates/logit-outputs/src/logit.rs`) |
| A sink's `buffer.disk:` spool | `encode_hop_batch`, then `write_frame` (`crates/logit-pipeline/src/disk_queue.rs`) |
| `kafka_out`, `format: native` | `encode_events`, then `write_frame`, once per record, and under `record: batch` once per candidate run while it searches for the longest run that fits (`crates/logit-outputs/src/kafka/encode.rs`) |
| `file_out`/`stdio_out`, `format: native` | `NativeEncoder::encode`: `encode_batch`, then `write_frame` (`crates/logit-outputs/src/stdio.rs`) |
| `logit-perf`'s telemetry leg | an `internal → file_out format: native` leg inside the measured process for `udp-statsd*`, `tail*`, and `attribute` runs |

## Workstreams

Each is one PR, stacked: W1 branches from `main`, W2 from W1, W3 from W2. W1 lands the
instruments before any production change, so every number W2 claims has a `main` baseline.

| WS | Branch | Change |
|---|---|---|
| W0 | `nenc/w0` | This plan |
| W1 | `nenc/w1` | Instruments: the byte-identity oracle and test, the five-shape encode bench, a 100-event pin |
| W2 | `nenc/w2` | The encoder rewrite, wire bytes unchanged, with the pins and `memory.md` rows it moves |
| W3 | `nenc/w3` | The perf VM measurement and the write-up |

### W0: plan

- [x] `docs/plans/native-encode-allocations.md`: this plan, and its `docs/plans/README.md` row.

### W1: instruments

Files: `crates/logit-proto/src/native/` (a test-only module),
`crates/logit-bench/benches/wire_format.rs`, `crates/logit-bench/tests/allocations.rs`,
`docs/design/memory.md`.

- [ ] A test-only copy of the current encoder (`encode_events`, `encode_hop_batch`, the `record`
  and `value` writers they call, and `write_frame_with_flags`) as the oracle.
- [ ] A byte-identity test that compares an encoder against the oracle, passing trivially on W1
  (the encoder is the oracle's twin), over:
  - the bench fixtures in `crates/logit-bench/src/fixtures.rs` (the five shapes below, and the six
    survey shapes);
  - `wire_format_bakeoff.rs`'s `representative_batches()`, which cover every `MetricKind`;
  - every native fuzz seed that decodes (`fuzz/seeds/native_batch`, `native_frame`,
    `native_hop_batch`), decoded and then re-encoded by both;
  - batches of 1, 100, and 1000 events, with and without a scope and a provenance trailer, under
    `Compression::None` and `Lz4`.
- [ ] `wire_format.rs`'s native encode benches over the five fixture shapes `memory.md` §0 names:
  mixed (`nginx_batch`), logs-only (`logs_only_syslog_datagram` through the syslog decoder),
  wide-JSON (`wide_json_syslog_datagram` through the syslog decoder and `json`),
  distribution-heavy (`distribution_heavy_event`), and span (`span_event`), each at 1, 100, and
  1000 events. Today the file covers mixed and distribution-heavy only. Record `main`'s numbers,
  pinned (`taskset -c 2`), in the PR.
- [ ] A pin for a warm 100-event `NativeEncoder::encode`, and its `memory.md` row, so a per-event
  cost can't return unseen.

### W2: encoder rewrite

Files: `crates/logit-proto/src/native/{mod.rs,record.rs,value.rs,varint.rs,dict.rs}`,
`crates/logit-proto/src/frame.rs`, the callers above where an API changes,
`crates/logit-bench/tests/allocations.rs`, and `docs/design/memory.md`.

- [ ] Lengths computed, not discovered: an `ivarint_len` beside `uvarint_len`, and a fixed or
  computed length for every scalar field and value, written through `write_scalar_field`'s
  pattern (length first, `debug_assert` that the payload matched it).
- [ ] For nested fields whose length isn't cheap to compute (attribute maps, records, lists),
  either a length pass that mirrors the writer, or one reused scratch buffer per nesting depth,
  chosen per field by W1's micro-bench, not by allocation count alone.
- [ ] Events written straight into the events section: no per-event `BytesMut`.
- [ ] Reusable encode state (scratch buffers and a cleared `DictBuilder`) held by
  `NativeEncoder`, `logit_out`'s connection, `DiskQueue`, and `kafka_out`'s encoder, so a warm
  encode allocates only its output.
- [ ] An output sized once: the hop prefix, payload, and trailer written into one buffer, and
  `write_frame_with_flags` writing the header and an uncompressed payload into one buffer without
  the intermediate `Vec`.
- [ ] W1's byte-identity test passes against the new encoder, unchanged.
- [ ] Every native encode pin moved to its new count in the same commit as `memory.md`'s rows:
  `native_encode_one_event` (30), `logit_out_encode_and_frame_one_batch` (31),
  `disk_queue_push_one_batch` (33), `native_round_trip_survey_shapes`' encode side (16, 24, 24,
  20, 20), `native_round_trip_enriched_resource_batch`'s encode (53), W1's 100-event pin, and
  `kafka_encode_into_100_events`' native rows (3,000 and 2,010).
- [ ] W1's micro-bench re-run on W2, beside `main`'s numbers, in the PR.

### W3: VM measurement

- [ ] Build W2's tip and three baselines with the same behavior and different layout: the `main`
  commit W1 branched from and the two first-parent merges before it.
- [ ] Measure, 20 runs per binary (four rounds of `--repeat 5`, order rotated), every scenario that
  runs the encoder: `native-relay`, `native-relay-window1`, `encode-native-devnull`, `buffered`,
  and `buffered-small-segments`. Measure `passthrough` and `json-parse` as controls, at the suite's
  `--repeat 5`.
- [ ] One `cpu-clock` flamegraph triple of `native-relay` per side, for the allocation-leaf share.
- [ ] Record the numbers in `docs/design/performance.md` and close this plan's checklist.

## Gates

A change lands only if all three hold, in this order of weight:

1. **Allocation counts (primary).** Exact, deterministic, and pinned. A warm encode allocates its
   output and nothing per event or per field. The counts are the evidence that the mechanism
   changed; they aren't the reason to ship it (the event-sizing ADR's decision 3).
2. **Micro-bench per shape.** No shape of the five regresses at any batch size beyond the bench's
   own run-to-run spread. A win on mixed bought with a loss on span doesn't land.
3. **VM end to end.** On every encoder scenario, the change's CPU µs/event clears the spread
   between the three baselines. `native-relay` moves 5–10% between builds from code layout alone,
   so a single before/after pair can't show a move under about 8%. No non-encoder control may move
   past that same spread.

## Risks and open questions

- **Exact sizing can lose to growing.** The event-sizing ADR measured reserving an `AttrMap` early
  as 5–9% slower than growing it late, and couldn't say why on a VM without a PMU. A large,
  exactly sized output buffer is a different allocation (a jemalloc large size class, fresh pages,
  THP `always`), and the one slow M flamegraph spent its extra time in realloc and page faults. W2
  and W3 measure it; nothing here assumes the exact size wins.
- **The dictionary is written before the sections that build it.** Writing events straight into
  the output needs the dictionary first: either a pass that interns every symbol before writing
  (walking the batch twice) or the events written to a reused scratch buffer and copied once. The
  pass must intern in the same order as today's single walk (resource, scope, then events in
  order), or the dictionary indexes change and the bytes with them. The byte-identity test catches
  that; W1 picks the approach by measurement.
- **Where the reusable state lives.** `encode_events` and `encode_hop_batch` are free functions
  today, and `kafka_out`'s `native_frame` calls them per candidate run. State on each caller means
  an API change in four crates; a thread-local scratch avoids that but ties buffer lifetime to
  Tokio's worker threads. Open.
- **A computed length that drifts from what's written desyncs every later field.** It's a bug, not
  bad input: `write_scalar_field`'s `debug_assert` and the byte-identity test both catch it in CI,
  and the fuzz seeds give the identity test more shapes than the fixtures do.
- **The win may be smaller than 17%.** The output buffer and one copy of the bytes stay, and some
  of the attributed time is `BytesMut` growth that exact sizing replaces with a single larger
  allocation. W3's numbers decide what this plan is worth.
- **Control frames share `write_frame_with_flags`.** The frame-layer change touches `Hello`,
  `HelloAck`, and `Ack` writes too. They're small and rare, so the risk is a correctness one only,
  which `frame_fixed_point.rs` and the control-frame tests cover.
