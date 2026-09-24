# Memory model and allocation behavior

`logit` moves many small, short-lived objects through a graph of components. Every hot path is
linear, so at real throughput the limits are **allocation churn** and **bytes copied per event**.
This document records both, measured, and what to do about them. [data-model.md](data-model.md)
defines the types measured here; [pipeline-graph.md](pipeline-graph.md) defines the nodes and
channels events move through.

Everything here is reproducible:

| What | Command |
|---|---|
| Type sizes | `script/test -p logit-core --test type_sizes` |
| Allocation counts | `script/test -p logit-bench --no-capture` |
| Throughput | `script/bench` |

`crates/logit-core/tests/type_sizes.rs` and `crates/logit-bench/tests/allocations.rs` are
**assertions**, so a regression fails CI instead of quietly making this document wrong. If you
change one of their numbers, change the matching table here in the same commit.

> Timings were taken on the disposable perf VM (`docs/adr/disposable-azure-perf-vm.md`:
> `Standard_F8as_v6`, 8 dedicated AMD EPYC 9V74 cores, SMT off), x86-64 Linux, in the dev
> container, `bench` profile (`lto = true`, `codegen-units = 1`), system allocator, `taskset -c 2`,
> 2026-09-20. They are divan's *fastest* column, the estimate least contaminated by noise: use them
> to compare stages with each other, not as throughput ceilings. Don't compare them with the
> pre-2026-09-20 laptop numbers; §2's timing-table note explains why. **Allocation counts are
> exact and machine-independent.**

## 0. What these measurements can and can't tell you

**Read this before acting on anything below.** Most allocation numbers here come from one event
shape: the `examples/nginx-to-influxdb.yaml` reference pipeline, whose events carry a log body
*and* several derived metrics, with ~10 attributes. It's a real config that exercises five
components end to end, but it is **one point in a space `logit` is meant to cover**:

| Workload | Carries | Wasted per event today |
|---|---|---:|
| Logs only (syslog, file tail → forward) | attributes + `log` | 376 B (`MetricList` + `SpanRecord`) |
| Metrics only (statsd, collectd, scrape → aggregate) | attributes + 1 metric | 232 B, plus ~176 of `MetricKind` a `Sum` can't use |
| Traces only (OTLP → forward) | attributes + `span` | 320 B (`LogRecord` + `MetricList`) |
| Mixed (the nginx shape — the first one measured) | all three | least of any shape |

Two consequences for how much weight to put on §8:

- **§1's sizing applies to every workload**, because every hop pays `Event`'s 864 bytes whatever
  the event carries. The single-signal shapes above waste 232-376 bytes on payloads they never
  hold. That argument doesn't depend on the fixture.
- **Several specific *fixes* are workload-dependent, and one flips sign** with the mix. A change
  that is free for a logs-only pipeline can cost a metrics-only one an allocation per event. §8
  marks which is which; don't read its ordering as settled for a workload the fixtures don't cover.

The fixtures (`crates/logit-bench/src/fixtures.rs`) now go beyond the nginx shape: a logs-only
syslog line, a wide-JSON log (28 flat fields), a distribution-heavy event (5 distinct
`Distribution` metrics), a directly-constructed span, and six shapes derived from the
[data-shapes.md](data-shapes.md) survey (§7's "Fixtures" section). They settle the *measurement*;
the *sizing* decisions they feed are §8's.

## 1. The event model's footprint

```
Event                                       864 bytes
├── timestamp: i64                            8
├── attributes: AttrMap                      392   ← SmallVec<[(Symbol, Value); 8]>
├── log: Option<LogRecord>                    88
├── metrics: MetricList                      232   ← SmallVec<[MetricRecord; 1]>
└── span: Option<SpanRecord>                 144
```

with the constituent parts:

| Type | Size | Why |
|---|---:|---|
| `Symbol` (`lasso::Spur`) | 4 | `NonZeroU32`; `Option<Symbol>` is also 4 |
| `Value` | 40 | sized by `Bytes` (4 words) plus an aligned discriminant |
| `(Symbol, Value)` | 48 | 4 bytes of padding after `Symbol` |
| `AttrMap` | 392 | 8 × 48 inline, + 8 of smallvec overhead (`union` feature, below) |
| `DdSketch` | 176 | `sketches_ddsketch::DDSketch` inlined directly (no `Box`): two `Store`s plus a `Config` |
| `Samples` | 168 | `SmallVec<[f64; SAMPLES_INLINE]>` (`SAMPLES_INLINE = 19`) + `sample_rate: f64` -- deliberately sized to sit just under `DdSketch`'s 176, see `MetricKind` below |
| `MetricKind` | 176 | sized by the larger of its two big variants (`Distribution`'s inlined `DdSketch`), with just enough room left over for a real discriminant that `Samples`'s smaller payload doesn't use up -- every other variant (`Sum`/`Gauge`/`GaugeDelta`/`SetMembers`/`Set`/`Histogram`/`ExponentialHistogram`/`Summary`) is far smaller and pays the same 176 regardless |
| `MetricRecord` | 224 | `MetricKind` (176) + `name`/`unit`/`description` (4 each, one padded) + `start_timestamp: i64` (8) + `exemplars: Vec<Exemplar>` (24) + `flags: u32` (4, fills former padding) |
| `MetricList` | 232 | 1 × 224 inline + 8 |
| `TraceRef` | 26 | `[u8;16]` trace id + `Option<[u8;8]>` span id + a flags byte; `Option<TraceRef>` is also 26 -- niche-filled through `Option<[u8;8]>`'s own tag |
| `LogRecord` | 88 | `Value` (40) + `Option<TraceRef>` (26) + `Severity`/`BodyFormat` (2) + `event_name: Option<Symbol>` (4) + `observed_timestamp: i64` (8) + `dropped_attributes_count: u32` (4, padded); `Option<LogRecord>` is also 88 — `Severity`'s niche absorbs `None` |
| `SpanExt` | 80 | boxed off `SpanRecord` (below): `status_message`/`trace_state: Option<Bytes>` (32 each) + three `u32` dropped counts (12, padded to 16) |
| `Option<Box<SpanExt>>` | 8 | `Box`'s non-null-pointer niche absorbs `None` |
| `SpanRecord` | 144 | the pre-`metrics-model-v2` 136 bytes + `flags: u32` (4, padded to 8) + `ext: Option<Box<SpanExt>>` (8); `Option<SpanRecord>` is also 144 — `SpanKind`'s niche absorbs `None` |
| `Resource` | 432 | `AttrMap` (392) + `dropped_attributes_count: u32` (4, padded) + `schema_url: Option<Bytes>` (32, no niche) -- no longer just `AttrMap`'s own size now that it carries these two extra fields |
| `Scope` | 496 | `name`/`version: Bytes` (32 each) + `AttrMap` (392) + `dropped_attributes_count: u32` (4, padded) + `schema_url: Option<Bytes>` (32) |

Three consequences:

**A `SmallVec` costs its inline capacity whether or not it has spilled.** The inline array and the
heap `(ptr, cap)` pair share one slot, sized by the larger. So an event with 13 attributes pays a
heap allocation *and* the full 392 bytes. Inline capacity 8 is not "free up to 8": it is 384 bytes
on every event, and the reference nginx pipeline spills past it anyway.

**A statsd counter costs the same 864 bytes as a fully-populated nginx access log.** `Event` has no
compact form for the common case; it reserves space for attributes, a sketch, and a span
unconditionally. That is the price of "an event is whatever it carries"
([ADR `multi-payload-events`](../adr/multi-payload-events.md)) implemented with inline storage.

**`MetricKind::Distribution` sets the size of every metric.** A `Sum(Sum { value, temporality,
monotonic })` ([ADR `metrics-model-v2`](../adr/metrics-model-v2.md)'s replacement for the old
`Counter(f64)`) needs a fraction of that and still pays 176, because `DDSketch` (two `Store`s and a
`Config`) is inlined into the enum. That is deliberate, per
[ADR `minimize-allocations-over-event-size`](../adr/minimize-allocations-over-event-size.md):
boxing it would save 144 bytes here but cost an allocation on every distribution metric
constructed or cloned, and distributions are common, not rare (see below). `MetricKind::Samples`
(raw statsd timer/histogram observations) is sized to sit *under* this ceiling:
`SAMPLES_INLINE = 19` keeps `size_of::<Samples>()` at 168, just under `DdSketch`'s 176, so
`MetricKind` stays at 176 instead of growing to fit a second large inlined variant.

### What was reclaimed, and what was deliberately not

The trades don't all point the same direction (see §0):

| Change | Saves | Real cost | Outcome |
|---|---:|---|---|
| smallvec's `union` feature | 16 B | none — it's a feature flag | **done** — applied, no tradeoff |
| `Box` `SpanRecord` | 136 B | +1 alloc per event that carries a span | **not done** — see below |
| `Box` the `DdSketch` in `MetricKind::Distribution` | ~168 B | +1 alloc per distribution metric created | **not done** — see below |
| Re-pick `AttrMap`'s inline capacity | up to 192 B | more spills, or (if increased) more bytes | **deferred** — see below |

Only the `union` feature landed, taking `Event` from 792 to 776 bytes at the time. `Event` has
grown since for unrelated reasons: to 800 with `LogRecord::trace`, and to 864 with
[ADR `metrics-model-v2`](../adr/metrics-model-v2.md)'s reshape, which also made `SpanRecord` 8 bytes
bigger (the table's 136 B saving). The two boxing changes were measured, implemented, and then
**reverted**, although the byte savings alone argue for them.

**Both boxing changes trade `Event`'s size for allocation count, and the project's stated priority
for that conflict is: minimize allocations, not size**
([ADR `minimize-allocations-over-event-size`](../adr/minimize-allocations-over-event-size.md)).
`logit`'s deployments aren't constrained by an in-flight footprint of hundreds of bytes per event.
At this scale, copying a few hundred extra bytes is close to free, while an allocation does real,
measurable work even on a fast path. So a trade that adds allocations to save bytes goes the wrong
way unless the payload is rare in its intended workload.

Neither payload is rare:

- **Boxing the `DdSketch` is not free.** A sketch doesn't allocate at construction:
  `sketches_ddsketch`'s `Store::new` starts with `Vec::new()`, and the bins are allocated on the
  first `add`. So the box is a new allocation, not one folded into an existing one. Measured at the
  time, boxing took `kv_metrics` from 3 allocations for 4 metrics (one `MetricList` spill plus one
  bins `Vec` per distribution) to 5, and the reference config, which carries 2 distributions per
  event, from 5 to 7 allocations per ingested line. That is the flagship config, not an edge case.
  (`kv_metrics` has since dropped to 1 by emitting raw `Samples` instead of a per-event sketch,
  [ADR `kv-metrics-semantics`](../adr/kv-metrics-semantics.md); the boxing argument is unchanged.)
- **Boxing `SpanRecord`** was reverted for the same reason. When it was measured, no
  span-producing input existed, but per ADR `minimize-allocations-over-event-size` that was a
  `v0.1` gap, not a property of the workload: a trace-focused deployment populates `span` on most
  events, the way the nginx config populates `metrics` with distributions. `otlp_in` and
  [ADR `internal-span-emission-and-deterministic-sampling`](../adr/internal-span-emission-and-deterministic-sampling.md)'s
  `internal` spans now produce exactly that shape, at the cost this table already prices (864
  bytes inline, 144 of them `SpanRecord`'s).

**`AttrMap`'s inline capacity is the largest single term (384 B), and stays at 8.** Shrinking it
has no measured upside: at capacity 4 the logs-only syslog line (6 attributes) spills, statsd
(0-4) stays inline either way, and the nginx shape (10) and a wide-JSON line (32) spill either way.
Increasing it was measured on the perf VM and rejected too; see §8 item 12.

**`MetricList`'s inline capacity (currently 1 — `SmallVec<[MetricRecord; 1]>`) is an open
question.** Any event with 2+ metrics spills, which includes every nginx reference event (4
metrics) and `kv_metrics` configurations generally. It interacts with the `DdSketch` decision:
with the sketch inlined, `MetricRecord` is 224 bytes (184 before ADR `metrics-model-v2` added
`description`/`start_timestamp`/`exemplars`), so each extra inline slot costs far more bytes than
it would with the sketch boxed. Picking a number needs real per-event metric counts, not more
synthetic fixtures. See §8 item 13.

**What a spill costs is measured, not inferred** (`attr_map_spills_to_double_its_inline_
capacity_then_reallocs`, `crates/logit-bench/tests/allocations.rs`; `docs/plans/event-sizing.md`'s
W1), instead of resting on smallvec 1.x's documented amortized doubling. Building a map one sorted
`insert` at a time:

| Attributes | allocs | reallocs | heap bytes | capacity |
|--:|--:|--:|--:|--:|
| 1, 8 | 0 | 0 | 0 (inline) | 8 |
| 9, 16 | **1** | 0 | 768 | 16 |
| 17, 24, 32 | 1 | **1** | 1536 | 32 |
| 33 | 1 | **2** | 3072 | 64 |

Three consequences:

- **The 9th entry spills to twice the inline capacity**, not to an exactly-sized buffer. A
  9-attribute event holds 768 heap bytes for 432 bytes of entries, on top of the 392 inline bytes
  it already paid for and abandoned.
- **Allocation count is nearly blind to width past 9.** The `allocs` column stays at 1 however wide
  the map gets, because every doubling after the spill is a `realloc`. A 30-attribute access log
  and a 12-attribute application log look the same by count (§2's two `json` rows) and differ only
  in bytes moved.
- **No producer can pre-size a map.** `AttrMap` exposes no `reserve`/`with_capacity`, even where
  the count is known: the native decoder reads an exact count off the wire and discards it
  (`native/value.rs`'s `read_attr_map_at`), while the metric list beside it *does* reserve
  (`native/record.rs`'s `read_record_list_into`).

**Both inline capacities are compile-time constants.** `SmallVec<[T; N]>`'s `N` is a const array
length, monomorphized into the type. Tuning it per deployment means recompiling for one workload's
shape or dropping compile-time inline capacity altogether, so whatever is picked has to serve every
workload the binary ships to.

A plain `Vec` is also an option. A spilled `SmallVec` still occupies its full inline footprint, so
for a consistently wide workload a `Vec` (24 B plus one allocation) is strictly better than a
`SmallVec` that always spills. The wide-JSON shape (32 attributes, one allocation under either
type) is exactly this case. "No inline capacity for this field" is a real alternative to "which
number."

## 2. Where the allocations are

Pinned by `crates/logit-bench/tests/allocations.rs`. Unless a row names another fixture, it
measures the reference pipeline (`examples/nginx-to-influxdb.yaml`,
`syslog_in → json → kv_metrics → keep → aggregate → influxdb_out`) with one real nginx access-log
line.

| Stage | allocs | Notes |
|---|---:|---|
| `syslog_in` decode 1 line | **1** | just the `Vec<Event>`; every field slices the datagram |
| `syslog_in` decode 100 lines | **1** | + 5 reallocs from `Vec` growth |
| `syslog_in` decode 1 logs-only line | **1** | plain-text message, no JSON -- same zero-copy shape |
| `syslog_in` `decode_into` into a warm buffer | **0** | ADR `decoupled-listener-io` -- see below |
| `statsd_in` decode 1 line | **2** | fixed -- see below, tag values now slice the datagram too |
| `statsd_in` `decode_into` into a warm buffer | **1** | ADR `decoupled-listener-io` -- see below |
| `statsd_in` decode 1 distribution line (`ms`/`h`/`d`, unsampled) | **2** | same as `statsd_in` decode 1 line -- `ms`/`h`/`d` decode straight to `MetricKind::Samples` now (ADR `lossless-transit`'s W3), one value fits inline in `Samples`'s own `SmallVec`; no `DdSketch`/`bins` Vec is built at decode time any more |
| `statsd_in` decode 1 sampled distribution line (`@0.1`) | **2** | same as unsampled -- the raw `sample_rate` now rides verbatim on the decoded `Samples`, with no decode-time extrapolation to allocate for |
| `statsd_in` decode 1 set line (`s`) | **3** | 2 as above + 1 `Vec<Bytes>` for `MetricKind::SetMembers`'s members -- unlike `Samples`'s inline `SmallVec`, `SetMembers` has no small-size optimization |
| `statsd_in` decode 1 DogStatsD event line (`_e{...}`, `TEXT` with nothing to unescape) | **2** | same as `statsd_in` decode 1 line -- `parse_event`'s `unescape_event_text` takes its zero-copy `slice_of` path, so an event costs nothing beyond the per-line/per-batch `Vec<Event>` pair every statsd line pays |
| `statsd_in` decode 1 DogStatsD event line (`TEXT` with one `\n` escape) | **3** | 2 as above + 1 -- the decoded length is known up front (each two-byte escape becomes one byte), so `unescape_event_text` sizes its `Vec` exactly and `Bytes::from(Vec<u8>)` takes its `len == capacity` promotion path: one allocation, no realloc, no second eager control-block alloc of the kind a slack-capacity `String::replace` result would cost |
| `statsd_in` decode 1 DogStatsD service check line (`_sc\|...`) | **2** | same as `statsd_in` decode 1 line -- every `statsd.service_check.*` carrier is a zero-copy datagram slice, same shape as an ordinary metric line's tags |
| `statsd_in` decode 1 line with a repeated tag key | **4** | ADR `statsd-output`'s amendment -- 2 as `statsd_in` decode 1 line + 2: `insert_tags` builds the `Value::Array`'s `Vec` spine (`vec![existing, value]`), and `build_event`'s `attributes.clone()` -- run once even on a single-value line -- deep-copies that spine again for the `Event`. A scalar tag's share of that clone is a `Bytes` refcount bump; the `Array` is the one attribute shape whose clone allocates |
| `statsd_in` decode 1 multi-value counter line with a repeated tag key (`name:1:2:3\|c`) | **6** | 2 + 1 (`insert_tags` builds the spine once) + 3 (one deep copy of that spine per value event, via `build_event`'s per-value `attributes.clone()`) -- the measured correction to that clone's "memcpy plus a refcount bump" account, which holds for a scalar tag but not for an `Array`-valued one |
| `collectd_in` decode 1 value list (1 data source) | **1** | just the `Vec<Event>`, same as `syslog_in` -- every identity field slices the datagram, the record name is built into the decoder's reused scratch `String` before interning, and a single-data-source list fits `MetricList`'s inline capacity |
| `collectd_in` `decode_into` into a warm buffer | **0** | ADR `decoupled-listener-io` -- nothing at all is left once the caller's `Vec<Event>` keeps its capacity |
| `collectd_in` decode 1 three-data-source value list (`load`) | **2** | 1 as above + one `MetricList` spill: `MetricList` is a `SmallVec` inlined at 1, so a multi-data-source list moves its records to the heap exactly once, not once per record |
| `collectd_in` decode a 25-list datagram | **1** | + 3 reallocs (`Vec<Event>` growing 4 → 8 → 16 → 32); a collectd datagram has no header naming its value-list count, so `decode_into` cannot size the `Vec` up front |
| `collectd_in` decode 1 three-data-source list, `types_db` resolving its names | **2** | **the same as without a `types.db`** -- the lookup is one `HashMap::get` per Values part returning a borrowed slice, and resolved names go into the same reused scratch `String` before interning |
| `graphite_in` decode 1 plaintext line | **1** | just the `Vec<Event>`, same as `syslog_in`/`collectd_in` -- the path is interned, the value is an `f64` in the record, and one `Gauge` fits `MetricList`'s inline capacity |
| `graphite_in` decode 1 tagged line (2 carbon tags) | **1** | **the same as untagged** -- every tag value is a zero-copy `Bytes::slice` of the datagram (`logit_proto::graphite::decode`'s `slice_of`, the trick `statsd_in`'s own `slice_of` plays) and two entries still fit `AttrMap`'s inline capacity |
| `graphite_in` `decode_into` into a warm buffer | **0** | ADR `decoupled-listener-io` -- nothing at all is left once the caller's `Vec<Event>` keeps its capacity, on the TCP path (the shared `logit-inputs/src/tcp.rs` driver's per-connection `scratch`) as much as the UDP one |
| `graphite_in` decode a 25-line datagram | **1** | + 3 reallocs (`Vec<Event>` growing 4 → 8 → 16 → 32); a carbon datagram has no header naming its line count, so `decode_into` cannot size the `Vec` up front -- the same shape as `collectd_in`'s 25-list row |
| `graphite_in` decode a 100-datapoint pickle frame | **1** | + 5 reallocs (4 → 8 → … → 128). The restricted pickle reader's stack, arenas and memo are decoder *fields*, cleared per frame rather than rebuilt (`logit_proto::graphite::pickle::PickleReader`), so walking a hundred datapoints through the stack machine allocates nothing of its own -- which is exactly why they are fields |
| `prometheus_in` decode 1 scrape (11 series: 2 counter families, 1 gauge, 1 histogram, 1 summary) | **161** | `text::parse_with` + `families_to_events`, no `Decoder` trait (ADR `prometheus-scrape-and-exposition`'s "No `logit_proto::Encoder`") -- ~14.6/series, dominated by one `String`/`AttrMap` per label pair (labels are decoded as owned `String`s, not sliced from the scrape body, unlike syslog/statsd's zero-copy `Bytes` fields) plus one `Vec` per family's series list; not yet optimized the way syslog/statsd's decode paths were, tracked as follow-up work rather than fixed here |
| `prometheus_in` decode 1 remote-write 1.0 request (100 gauge series) | **1526** | `remote_write::decode` alone -- Snappy and HTTP are the receiver's, and `families_to_events` is the scrape row's -- over `fixtures::remote_write_request_v1`. ~15.3/series, the same order as the scrape row above and for the same reason: labels become owned `String`s rather than slices of the request, one `Vec`/`HashMap` entry per series in the shared assembler, and one `Vec` per family. Prost decoding the request into owned protobuf structs is part of it (~200 `String`s for 100 two-label series); a borrowing decoder would be a different codec, not a tuning of this one. Not yet optimized, tracked as follow-up work alongside the two scrape/exposition rows |
| `prometheus_in` decode 1 remote-write 2.0 request (100 gauge series) | **1428** | the same request through 2.0's symbol table, and **cheaper** than 1.0 by ~1/series. Both versions build the same owned label pairs for the model, so the win is upstream of this codec, in what prost materializes: 1.0 decodes a `Label { name, value }` per label per series, 2.0 decodes the table once (~105 `String`s) and the references as plain `u32`s. 2.0's per-series `Metadata` -- repeated on every one of a family's series -- costs nothing, because declarations are deduplicated by family name before any timestamp group opens. This row was 1624 before that dedupe, when each repeat also fired a bogus `skipped{reason="duplicate_metadata"}`; the number and the counter were wrong together |
| `generate_in` render 100 events (all-literal template) | **1** | just the batch's `Vec<Event>` -- nothing at all per event. No placeholder anywhere means one prototype `Event` is rendered once at construction and `clone`d per event with only `timestamp` overwritten, and *this* shape's `Event::clone` is free: one attribute fits `AttrMap`'s 8-entry inline capacity, one metric fits `MetricList`'s inline capacity of 1, and the log body's `Bytes` is a refcount bump rather than a copy (contrast the `Event::clone (nginx shape)` row below, whose attributes have spilled). The generator is effectively free next to whatever a scenario puts downstream of it, which is the point of having this path at all |
| `generate_in` render 100 events (2 templated fields) | **201** | 1 as above + exactly one `Bytes::copy_from_slice` per templated field per event (`{seq%50}` in the log body, `{seq%10}` in one attribute). Nothing else: the scratch `String` every rendering goes through never reallocates once warm (`logit_core::template::Compiled::render`'s own guarantee), and the literal fields alongside them -- the interned metric name, the `Arc`-shared resource -- still cost nothing. This is [ADR `load-test-harness`](../adr/load-test-harness.md)'s named risk, measured rather than feared: a placeholder is a cardinality knob, not decoration, and each one costs an allocation per event forever |
| `json` parse + merge (nginx shape) | **1** | fixed -- see below, was 7 |
| `json` parse + merge (wide-JSON, 28 keys) | **1** | same fix, confirmed to generalize past a small field count |
| `logfmt` parse + merge (go-kit-style, 9 fields) | **1** | hand-rolled scanner, zero-copy by construction -- see `docs/adr/logfmt-and-kv-parsing.md`; the one allocation is `event.attributes` spilling its inline capacity, same shape as `json`'s |
| `logfmt` parse + merge (1 escaped-quote value) | **1** | + 1 realloc; the escaped value is the only path `unescape` can't slice -- `shrink_to_fit` before the final `Bytes::from` keeps that a `realloc` of the already-paid-for buffer rather than a second `alloc` (see "Fixtures" below); 3 fields fit inline, so nothing else allocates |
| `kv` parse + merge (`a=1&b=2&c=hello`) | **0** | 3 fields fit inline, no quoting/escaping to ever allocate |
| `regex` capture into an inline map (3 named groups, empty-attrs event) | **0** | `captures_read` + `haystack.slice` -- zero-copy, no spill |
| `regex` parse 1 event (sshd shape, 3 captures onto 6 existing `syslog.*` attrs) | **1** | spills past `AttrMap`'s 8-entry inline capacity |
| `regex` no match, 1 event | **0** | nothing written, nothing allocated |
| `csv` parse + merge (7-column access line, one quoted-but-unescaped field) | **0** | interned columns, `insert_sym`, `Bytes::slice` throughout -- fits `AttrMap`'s inline capacity |
| `csv` parse + merge (one doubled-quote field) | **1** | `unescape`'s own copy -- the only path in `csv` that allocates (`crates/logit-transforms/src/csv.rs`) |
| `csv` parse + merge (16-column wide row) | **1** | `AttrMap` inline-capacity spill only -- every field itself is still a zero-copy slice |
| `kv_metrics` derive 4 metrics | **1** | the `MetricList` spill, grown once via `reserve`; was 3 while each distribution sketched per event -- they are raw inline `MetricKind::Samples` now ([ADR `kv-metrics-semantics`](../adr/kv-metrics-semantics.md)) |
| `keep` filter to 3 attrs | **0** | 3 attributes fit inline |
| `keep_values` clamp `host`, already allowed and lowercase | **0** | `Clamp::normalize` returns `None` (nothing changed) and the allowed path never calls `insert_sym` -- the steady-state case pays nothing, same property `keep`'s row above pins |
| `keep_values` clamp `host`, `normalize: [lower]` needs to lower it | **1** | the one path that isn't free: an uppercase byte forces a fresh `Bytes` for the write-back. The cardinality win `normalize:` exists for costs exactly one allocation per event that actually needed it, never per event that didn't |
| `shape` measure 1 statsd event | **1** | the `MetricList` spill, and only that: a measurement event carries a dozen-odd records where the incoming one held its single record inline, so `reserve` has to go to the heap ([ADR `shape-observer-component`](../adr/shape-observer-component.md)). The tags fit `AttrMap` inline and every `Samples` is well inside `SAMPLES_INLINE` |
| `shape` measure 1 nginx event | **0** | + 1 realloc, and **cheaper than the statsd row above despite measuring more** -- the inversion is what clearing and refilling `event.attributes`/`event.metrics` in place buys. This event arrives with both already spilled (10 attributes past 8, 4 records past 1), so the tags land in storage that already exists and `reserve` *grows* the existing `MetricList` buffer instead of allocating a new one |
| `shape` measure 1 wide-JSON event (32 attrs) | **3** | 1 `MetricList` spill + 1 each for `logit.shape.key_bytes`/`.value_bytes`, which carry 32 values apiece and so run past `SAMPLES_INLINE`'s 19. Inherent -- a per-attribute measurement of a 32-attribute event *is* 32 numbers -- and the reason `shape` belongs on a tap branch rather than in the flow |
| `flatten` an already-flat event | **0** | the phase-1 scan selects nothing -- no scratch buffer touched, no `Symbol` interned, no `Value` moved ([ADR `flatten-transform`](../adr/flatten-transform.md)) |
| `flatten` pino-http shape (8 attrs, 2 nested `req`/`res`), warm `KeyCache` | **1** | six flat attributes survive untouched plus the eight leaves `req`/`res` expand into -- 14 total, spilling `AttrMap`'s 8-slot inline capacity once. One allocation, not one per inserted entry: `SmallVec`'s first over-capacity push grows straight to 16 slots, which the rest of this event's inserts fit inside |
| `flatten` pino-http shape, cold `KeyCache` (first event of its shape in the process) | **12** | the warm case's spill plus one interner-and-cache-entry cost per distinct path this shape mints (`req.method`, `req.headers.host`, ...) -- exactly the cost `docs/design/data-shapes.md`'s pino-http finding says this shape pays "again" on every event, until its own component's `KeyCache` has seen every path once |
| `sample` `key: trace_id` on a span event | **0** | the 16 id bytes hex-encoded into a stack array, one XXH64 over them ([ADR `consistent-sampling-component`](../adr/consistent-sampling-component.md)); the key name was interned at construction and the decision tally is plain integers emitted once per batch |
| `sample` `key: {attribute: status}` on an `I64` value | **0** | the decimal text is formatted straight into the streaming hasher through `fmt::Write` (`logit_core::sampling`'s `KeyHasher`) -- no intermediate `String`, the reason numeric canonicalization costs nothing |
| `sample` with no key (random draw) | **0** | a per-instance counter mixed with the seed through the same hash -- no RNG, nothing to allocate |
| `sample` `always_keep` hit | **0** | one `get_sym` and a `value_matches` string compare, before the key is looked at |
| `http_access` ignores an event with no log (metric-only event) | **0** | `process` returns before touching anything -- the design's own contract, measured |
| `http_access` normalize a conforming semconv line, warm | **0** | every output is a pre-built `Value` (a config-derived one built once in `HttpAccess::new`, a `static_str`/`format!` cell filled on its first use) cloned by refcount thereafter -- the `shared()` promotion trick (`crates/logit-transforms/src/http_access.rs`'s doc comment) holds exactly as designed: nothing here clones an unpromoted `Bytes` |
| `http_access` normalize a conforming semconv line, cold (fresh component, no warm-up call) | **228** | not `http_access.rs`'s own doing: the `regex` crate lazily builds a per-compiled-pattern, per-thread search cache (`regex-automata`'s pooled `Cache`) on a pattern's *first* `is_match`/`captures_read`, then reuses it for free -- the warm row above is what "scanned with `is_match`, which allocates nothing" (the module doc's own claim) means in steady state. This fixture's UA matches `browser`, the last of the four built-in classifier rules, so all four run cold (176, confirmed in isolation); its path matches none of the four route rules, so all four of those also run cold, plus the one `format!`+`shared()` pair that fills this `(method, route)`'s `span.name` cell (52, confirmed in isolation) -- 176 + 52 = 228. A real process pays this once per compiled pattern, not once per event |
| `http_access` normalize a dashed-alias line, warm | **0** | identical to the dotted-line warm row -- `dealias` renames every dashed key present to its dotted form (`AttrMap::remove_sym` + `insert_sym`, both in-place) before step 1 ever runs, so the rest of the pipeline sees the same attributes either way and the alias path costs nothing extra |
| `http_access` cleans one control byte in `user_agent.original` | **2** | not the module doc's "one exact-size copy" -- warmed only on a clean line, so this component's `scratch: Vec<u8>` (`HttpAccess::new`'s `Vec::new()`) has never grown when the dirty branch first runs. `cap_and_clean`'s clean-up pays for two distinct things here: `scratch.extend` growing that Vec from nothing (a real `alloc`, no prior buffer to `realloc`), then `Bytes::copy_from_slice(scratch)` copying the cleaned bytes out into the new `Value`. Confirmed by isolation: pre-warming `scratch` with one earlier dirty event drops this to exactly **1** -- the steady-state cost, paid once per process (or per query-redaction/control-byte shape) rather than once per event |
| `set` through `process_batch`, attributes only | **0** | nothing at all: `process_batch` is a `Vec::retain_mut` over the batch's own `events` ([ADR `in-place-transform-process`](../adr/in-place-transform-process.md)), and `map_resource` returns `None` immediately, same as `keep` |
| `set.map_resource`, cached (same input `Arc`) | **0** | the one-entry `Arc::ptr_eq` cache hits -- see below |
| `set.map_resource`, cache miss (distinct input `Arc`) | **1** | `Arc::new(Resource { .. })` -- the `AttrMap` clone/insert itself stays inline on an empty resource |
| `has_attributes`/`drop_attributes`, match 1 attribute | **0** | `AttrMap::get_sym` (a `binary_search_by_key`) + `value_matches`' numeric coercion, both stack-only |
| `has_attributes`, resource match, cache hit (same input `Arc`) | **0** | `Matcher`'s one-entry `Arc::ptr_eq` cache, `Set::map_resource`'s idiom applied to a read |
| `has_attributes`, resource match, cache miss (distinct input `Arc`) | **0** | unlike `set.map_resource`'s miss, nothing is rebuilt -- a miss only re-evaluates `get_sym` against the `Arc` already in hand, see below |
| `aggregate` absorb (after `keep`) | **0** | `SeriesKey` clone stays inline |
| `aggregate` absorb (no `keep`) | **4** | one per metric — the map no longer fits inline |
| `aggregate` flush 4 series | **6** | +4 since flush-side trace linking landed (ADR `trace-context-propagation-on-delivered`) — one `Vec<SpanLink>` per series, see below |
| `aggregate` flush 100 retained gauge series (spilled attrs) | **209** | `series_retention > 0` only — see below; the default (`0`) tumbling path above is unaffected |
| `aggregate` flush 100 cumulative sum series (spilled attrs) | **209** | `temporality: cumulative` — identical to the retained-gauge row above, on purpose: see below |
| `aggregate` absorb 1 `Samples` event (`distributions: sketch`, the default) | **0** | every value sketches directly into the series' `DdSketch` via `Samples::sketch`'s weighting -- no raw values are ever retained, so absorbing into an already-open sketch is as free as `distributions_merge_via_ddsketch` already is |
| `aggregate` absorb 25 `Samples` values into one series (`distributions: samples`) | **1** | `SAMPLES_INLINE` is 19 -- a series already holding a few inline values that then absorbs 25 more in one record spills the accumulator's `SmallVec` on that call; the warm/still-inline case (a few values) pays nothing |
| **full ingest chain, 1 line** | **3** | decode → aggregate; was 11 before `json`'s fix, 5 before `kv_metrics` stopped sketching per event |
| `Event::clone` (nginx shape) | **2** | what each extra fan-out branch costs -- the spilled `AttrMap` and `MetricList`; was 4 with a `bins` Vec per `kv_metrics` sketch |
| `Event::clone` (statsd shape) | **0** | fits entirely inline |
| `Event::clone` (distribution-heavy, 5 metrics) | **6** | 1 `MetricList` spill + 1 `bins` Vec per sketch |
| `Event::clone` (span shape) | **2** | 1 per `Vec` (`events`, `links`) -- every `AttrMap` here stays inline |
| `json` parse 12-attribute flat log | **1** | the survey's commonest measured log width (`docs/design/data-shapes.md` §5.3), through the real `tail_in`-shaped leg: one `log.file.path` plus eleven JSON keys. The one allocation is the `AttrMap` spill; every value slices the message `Bytes` |
| `json` parse 10-attribute nested pino-http record | **5** | 1 spill + **4 boxed `Value::Map`s** (`req{}`, `res{}` and the `headers{}` inside each). A *narrower* event than the row above and five times the allocations -- `Value::Map` is `Box<AttrMap>`, so every nested object pays the full 392-byte inline footprint again, whatever its width (§6 of `data-shapes.md`: "nested maps multiply whatever is chosen") |
| `json` parse 30-attribute access log (PostgreSQL `jsonlog`) | **3** | + **2 reallocs** -- the only shipped shape whose map grows twice (8 → 16 → 32, §1's ladder). Only *one* of the three allocations is the map: the other two are a single JSON-escaped value, PostgreSQL quoting the constraint name in a violation message, which cannot be sliced zero-copy |
| `Event::clone` (12-attribute flat log) | **1** | the spilled `AttrMap`, nothing else |
| `Event::clone` (30-attribute access log) | **1** | **the same as the 12-attribute row** -- smallvec clones `len` entries into one fresh buffer, so a spilled map costs exactly one allocation however far past 8 it is. That buffer is **not** exactly sized, as a first draft of this row claimed: `SmallVec::clone` collects through `extend`, whose `reserve` rounds up to the next power of two, so the 12-attribute clone asks for 768 bytes and this one for 1536 (`tests/attr_arms.rs`'s `clone_allocations_and_bytes_by_arm` pins it). An exactly-sized clone was built and measured CPU-neutral on the perf VM, and not adopted (ADR `event-sizing-and-allocation-strategy`). The two shapes differ in entries copied (30 against 12, plus 864 bytes for the `Event` either way): the clearest single demonstration that allocation count and copy cost rank these shapes differently |
| `Event::clone` (10-attribute nested pino-http record) | **5** | 1 spill + 1 per boxed `Value::Map`. The narrowest of the three log shapes and by far the most expensive to clone |
| `Event::clone` (17-attribute server span) | **1** | the spilled map alone -- the exact complement of the `span shape` row above, whose map stays inline and whose two allocations are its `events`/`links` `Vec`s. Measured spans carry neither: 76% of 114,551 demo spans had no events and **none** had a link (`data-shapes.md` §4) |
| `Event::clone` (3-record collectd event, 6 attributes) | **1** | `MetricList`'s spill, not the map's: six attributes fit inline and three records do not. The only measured `MetricList` spill in the survey -- 17.4% of 16,590 collectd events carry 2 records, 0.4% carry 3 (`data-shapes.md` §3) |
| `EventBatch::clone` (5 events, 17-attribute `Resource`) | **6** | 1 `Vec<Event>` + 1 per event: the measured median OpenTelemetry log record carries **9** attributes, one slot past inline, so every event on that leg spills by one. **The 17-attribute resource is not among the six** -- it is `Arc`-shared, so the widest attribute set in the survey is the one place capacity is nearly free (`data-shapes.md` §4, §6) |
| `unwrap_batch` (contended `Delivered::Shared`, that same batch) | **6** | identical, by construction -- the copy-on-write fallback *is* `EventBatch::clone` (§3) |
| `stdio_out` encode 100 events | **102** | ~1/event -- fixed, see below, was 1801; +1 since measured through `Encoder::encode` (`&EventBatch` -> `Bytes`) rather than the inherent `render` (`&EventBatch` -> `String`) directly, ADR `rotating-file-output` -- `Bytes::from(String)`'s own small shared-refcount allocation |
| `influxdb_out` encode 100 events | **230** | 30 of the encoder's own (~0.3/event — see below) + 200 = 2/event re-sketching the fixture's two raw `Samples` distributions (`Samples::sketch`'s `bins` Vec, the same cost the `graphite_out` `Samples` row further down documents). Those 2/event are the allocations `kv_metrics` used to pay for *every* downstream, moved into the one topology that needs a sketch -- an encoder fed straight from `kv_metrics` with no `aggregate` between ([ADR `kv-metrics-semantics`](../adr/kv-metrics-semantics.md)); the reference pipeline's `aggregate` sketches once per series instead |
| receive queue: push then pop, warm | **0** | `BoundedQueue<Datagram>`, ADR `decoupled-listener-io` -- see below |
| receive queue: push_many then pop_many, warm | **0** | the same hop for a whole batch of 8 (ADR `udp-intake-batching-and-socket-visibility`) -- `push_many` drains the caller's `Vec` and `pop_many` appends into one the caller clears, both keeping their capacity, so a batch costs the same nothing per datagram the single-item row above does |
| `disk_queue`: push one batch (encode + write) | **34** | 36 -> 34 once `kv_metrics`'s two distributions became inline `Samples` (no `to_java_bytes` blob per record; same -2 as `NativeEncoder::encode` below); `native::encode_batch_v2` + `frame::write_frame` + one `write_all` -- breaks the zero-clone `Arc<EventBatch>` property by design, see `docs/adr/disk-backed-sink-buffer.md`; 25 -> 27 once `encode_batch_v2` (the provenance trailer, `docs/adr/batch-provenance-on-delivered.md`) replaced `encode_batch` here -- it builds v1's payload as its own `Bytes`, then copies it into a fresh `BytesMut` alongside the trailer rather than extending in place; 27 -> 36 with [ADR `metrics-model-v2`](../adr/metrics-model-v2.md)'s TLV-framed records (same +9 as `NativeEncoder::encode` below) |
| `disk_queue`: peek, cached (no re-decode) | **0** | `write_loop`'s retry loop calls `peek` once per attempt; only the first (uncached) peek after a push touches disk |
| accumulator: absorb into a warm buffer | **0** | `BatchAccumulator::absorb`, ADR `decoupled-listener-io` -- see below |
| `syslog_out` encode_into 100 events | **100** | ~1/event -- reused struct-held scratch buffers, was 401, see below |
| `statsd_out` encode_into 100 events | **0** | measured through the same `FramedEncoder::encode_into` call as the syslog row (ADR `framed-encoder`), over 100 single-counter DogStatsD events: every per-metric buffer was a reused struct field from the start, and a statsd line has no timestamp to format, so a warm `MessageBuf` never touches the allocator |
| `prometheus_out` encode 100 series (1 gauge family) | **414** | `events_to_families` + `text::write`, no `Encoder` trait (same ADR as the decode row above) -- ~4.1/series: one `String` label key/value pair, one `MetricFamily`/`Series` entry, and the rendered text line's own buffer growth per series; not yet optimized, tracked as follow-up work alongside the decode row above |
| `prometheus_out` encode 100 series, remote-write 1.0 | **1223** | `remote_write::encode` alone (the caller Snappy-compresses, and `events_to_families` is the exposition row's) over one 100-series gauge family, `fixtures::remote_write_families`. ~12.2/series: the flattened sample name, the label set rebuilt with `__name__` added and re-sorted, and the prost `TimeSeries`/`Label`/`Sample` structs the request is assembled from before `encode_to_vec` walks it. Lower than the exposition row's 414/100 = 4.1/series would suggest for a "simpler" format because protobuf is built as a tree of owned structs where text is written straight into one growing buffer -- the two are not the same kind of work |
| `prometheus_out` encode 100 series, remote-write 2.0 | **1434** | +211 over 1.0, about +2/series: the symbol table keeps an owned `String` key per distinct string, and this fixture's `shard` label values are distinct by construction, so it interns one per series. The trade is deliberate and the other two thirds of it are elsewhere -- on the wire, where 2.0 sends each string once instead of once per series, and in the decode row above, where the receiver gets that saving back. Interning costs the sender and pays the receiver |
| `collectd_out` encode_into 100 events, warm | **0** | fixed, was 300 (~3/event) -- `CollectdEncoder` (`crates/logit-proto/src/collectd/encode.rs`) already held its own reused `packet`/`list`/`values` scratch, but `pack_list`'s `last.clone_from(cur)` fell through to `Clone`'s *default* `clone_from` (`#[derive(Clone)]` only generates `clone()`), which is `*self = source.clone()` -- a fresh `Vec` allocation per non-empty identity field (host/plugin/type, here), every single list, with the old one dropped right behind it. A hand-written `clone_from` that clears and `extend_from_slice`s each field in place fixed it; `MessageBuf<usize>` (ADR `framed-encoder`) is unrelated to this and was already warm |
| `graphite_out` encode_into 100 plaintext events | **0** | every per-record buffer (`tag_suffix`/`path`/`line`/...) is a reused struct field from the start (`crates/logit-proto/src/graphite/encode.rs`'s own doc comment), so a warm plaintext encode of 100 single-gauge events never touches the allocator |
| `graphite_out` encode_into 100 pickle events | **0** | pickle packing writes into the same reused `frame`/`datapoint` `Vec<u8>` fields, patching the 4-byte length prefix in place rather than copying -- no additional cost over the plaintext row above |
| `graphite_out` encode_into 100 `Distribution` events, `multi_value: expand` | **0** | expanding into `.count`/`.sum`/`.q*` sub-paths reads an already-built `DdSketch` in place (`expand_sketch`) -- no new sketch is built, so this costs exactly what the plaintext row above costs |
| `graphite_out` encode_into 100 `Samples` events, `multi_value: expand` | **100** | not zero, and not meant to be: expanding a raw `Samples` record first calls `Samples::sketch()`, which builds a fresh `DdSketch` accumulator from the record's raw values -- inherent to re-summarizing on the way out, and the identical cost `influxdb_out`'s own `Samples` expansion already pays. `crate::graphite::encode`'s own module doc names this as one of its two deliberate per-record-allocation exceptions; the other, a `SetMembers` expansion's de-duplication `Vec`, has no pinned row here since nothing in this effort's fixtures exercises it, but is called out for the same reason a future fixture would need to account for it too |

And the corresponding times:

| Stage | fastest | per event |
|---|---:|---:|
| `syslog_in` decode, 100 lines | 20.4 µs | 204 ns |
| `statsd_in` decode, 100 lines | 40.1 µs | 401 ns |
| `json` | 411 ns | 411 ns |
| `kv_metrics` | 75.5 ns | 75.5 ns |
| `keep` | 505 ns | 505 ns |
| `aggregate` absorb | 891 ns | 891 ns |
| **full ingest chain** | **2.17 µs** | ~461k lines/s/core |
| `Event::clone` (nginx / statsd / distribution) | 316 / 126 / 97.1 ns | |
| `stdio_out` encode, 100 events | 134.7 µs | 1.35 µs |
| `influxdb_out` encode, 100 events | 257.1 µs | 2.57 µs |
| `syslog_out` encode_into, 100 events | 82.4 µs | 824 ns |
| `lua` (proxy / `to_table`) | 1.61 / 9.03 µs | |

Every timing above comes from **one** `script/bench` run on the disposable perf VM
(`docs/adr/disposable-azure-perf-vm.md`: `Standard_F8as_v6`, 8 dedicated EPYC 9V74 cores, SMT
off), `taskset -c 2`, 2026-09-20. Compare rows *within* this table (`json` against `kv_metrics`,
`stdio_out` against `influxdb_out`); don't compare a row with the pre-2026-09-20 laptop table's.
Between the two, **both the machine and the code changed**. Optimizations landed (the interner key
cache and in-place `Transform::process` are most of why `kv_metrics` dropped 256 ns → 75.5 ns and
`json` 535 ns → 411 ns), while `metrics-model-v2`'s TLV framing and later lossless-transit work
pushed `influxdb_out`/`syslog_out`/`lua to_table` the other way. A single row's delta is a hardware
story, a code story, or both, and reading it as "the VM is N% faster" overclaims.
`docs/design/performance.md`'s preamble carries the same caveat for its numbers.

Many allocation rows have no timing row. Their counts are what these tests pin, in
`crates/logit-bench/tests/allocations.rs`:

- `statsd_in` distribution and set decode: `statsd_decode_one_distribution_line`/
  `statsd_decode_one_sampled_distribution_line`/`statsd_decode_one_set_line`. Decode costs the same
  at any sample rate because ADR `lossless-transit`'s W3 moved sample-rate extrapolation from decode
  into `aggregate`.
- `statsd_in` DogStatsD events and service checks: `statsd_decode_one_event_line`/
  `statsd_decode_one_event_line_with_an_escaped_newline`/`statsd_decode_one_service_check_line`.
- `statsd_in` repeated tag keys: `statsd_decode_one_line_with_a_repeated_tag_key`/
  `statsd_decode_one_multi_value_counter_line_with_a_repeated_tag_key`.
- `statsd_out` (ADR `framed-encoder`): `statsd_encode_into_100_events`. `benches/pipeline.rs` has a
  matching `encode::statsd` arm.
- `collectd_in`: `collectd_decode_one_list`/`collectd_decode_into_a_warm_reused_buffer_costs_nothing`/
  `collectd_decode_one_three_value_list`/`collectd_decode_a_25_list_packet`/
  `collectd_decode_one_list_with_types_db_resolution`.
- `generate_in`: `generate_render_literal_100_events`/`generate_render_templated_100_events`, with
  matching `generate_render_literal`/`generate_render_templated` arms in `benches/pipeline.rs`.
- `graphite_out`: `graphite_encode_into_100_plaintext_events`/
  `graphite_encode_into_100_pickle_events`/
  `graphite_encode_into_100_distribution_events_expanded`/
  `graphite_encode_into_100_samples_events_expanded`.

`value_heap_bytes` (`crates/logit-core/src/event.rs`) counts an `Array`'s element payloads but not
its `Vec` spine (`capacity × size_of::<Value>()`), so it understates a repeated-tag event's heap
footprint by that spine. `syslog.sd`'s `Array` values have the same gap. It was left as is on
purpose: fixing it would move both producers' weights for a reason unrelated to either.

### Listener I/O decoupling: the `decode_into` buffer-reuse win (ADR `decoupled-listener-io`)

`statsd_in decode 1 line` (2) and `statsd_in decode_into into a warm buffer` (1) measure two call
paths, not a contradiction. `decode()`, the trait's provided default and what most tests and
benchmarks call, always hands `decode_into` a fresh `Vec::new()`, and that allocation is real for
that call shape. `logit-inputs::udp::decode_loop`'s hot path instead calls `decode_into` against a
buffer it *reuses* across datagrams, which removes exactly that allocation
([ADR `decoupled-listener-io`](../adr/decoupled-listener-io.md)). `syslog_in`, `collectd_in`, and
`graphite_in` drop from 1 to 0. `statsd_in` drops from 2 to 1, because `parse_line`'s per-line
`Vec<Event>` is internal to `decode_into` and the caller's buffer can't absorb it.

`BatchAccumulator::absorb` is why this matters. It takes `&mut Vec<Event>` and merges with
`Vec::append`, not an owned `EventBatch` merged with `std::mem::take`, so draining the caller's
buffer keeps its capacity instead of replacing it with a capacity-0 one. The
`accumulator: absorb into a warm buffer` row (0) proves it. The
`receive queue: push then pop, warm` row (also 0) covers the queue hop:
`BoundedQueue<Datagram>`'s `push`/`pop` move the datagram through `VecDeque` storage that
`InMemoryBuffer::new`'s `with_capacity(max_len.min(4096))` presizes. So the decode loop's
steady-state cost is: read the socket (1 allocation, `Bytes::copy_from_slice`, §7's
`datagram_copy_is_one_right_sized_allocation`), queue it (0), decode into a reused buffer (0 or 1,
by decoder), accumulate (0). There is no end-to-end number for the loop here; its *behavior* (the
reader keeps reading under backpressure, shutdown drains a backlog) is tested in
`crates/logit-inputs/src/udp.rs`.

### `aggregate` flush now costs one allocation per series, for real trace links

[ADR `trace-context-propagation-on-delivered`](../adr/trace-context-propagation-on-delivered.md)'s
flush-side linking pairs each `Event` that `Transform::flush` emits with a bounded, best-effort
`Vec<SpanLink>` naming its sources (`crates/logit-transforms/src/aggregate.rs`'s
`ContributingContexts`). It took `aggregate_flush_100_series` from 2 to 6 allocations: one
`Vec<SpanLink>` for each of the fixture's 4 series. The fixture never calls
`observe_batch_context`, so each series holds one context (the default, all-zero one), but a
non-empty `Vec` allocates whatever its length, so one per series is the floor, not a worst case.
More distinct sources, up to the 8-per-series cap, don't add allocations: the `Vec` is still built
once.

`run_flush` (`crates/logit-pipeline/src/runtime.rs`) attaches these links to the flush's own span.
The allocation is paid on every flush of any series, whether or not internal spans are sampled or
exported.

### Series retention's own cost, isolated and measured (ADR `aggregation-window-semantics`'s amendments)

`series_retention > 0` (`docs/adr/aggregation-window-semantics.md`'s amendment) adds its own
allocation cost, paid only by retained series. The default (`series_retention: 0`) path is
untouched: `aggregate_flush_100_series` still measures **6**. `aggregate_flush_retained_gauges`
isolates the retained path: 100 distinct gauge series, deliberately not trimmed by `keep` (12
attributes each, past `AttrMap`'s 8-slot inline capacity), retained across a second flush, cost
**209** allocations. Two costs stack, both inherent to retention:

- **`key.attributes.clone()`, once per retained series.** The tumbling path moves `key.attributes`
  into the emitted event and drops the key. A retained series needs its key again next window, so
  the attributes are cloned instead, and a spilled map's clone allocates: ~100 of the 209, one per
  series. `aggregate_flush_100_series` never takes this branch, because its gauge retention is off.
- **Each group's `series` `HashMap` rebuilds its table on every flush that retains anything.**
  `flush` takes the whole map via `mem::take` and re-inserts survivors into the empty replacement,
  so the far more common tumbling path can move `key.attributes` for free instead of cloning every
  series. The trade: a retaining pipeline pays several table-growth allocations every flush for as
  long as it retains. This is an accepted, measured cost of opting in, and why this fixture is
  separate from the default-path number.

`aggregate_flush_cumulative_sums` runs the same 100 series, spilled maps, and steady-state second
flush with `temporality: cumulative` `Sum` series (that ADR's cumulative amendment), and measures
the same **209**. That equality is the finding: a retained `Sum` reports through the same
copy-then-keep path a retained gauge does (`Accumulator::kind_for_retained`, `Copy` fields on both),
so cumulative counters cost a flush nothing beyond gauge retention. A cumulative `Histogram` would
add a bucket-`Vec` clone per series per flush, but no wire producer emits one yet, so there is
nothing honest to fixture it from.

### Encoders: the cost that used to dominate

As first measured, encoding one event for InfluxDB cost **~180 allocations and 4.96 µs**: about
twice the whole ingest chain, and sixteen times an extra fan-out clone. At the time,
[known-gaps.md](../known-gaps.md) named `Arc<EventBatch>` copy-on-write as *the* fix for pipeline
cost, and [pipeline-graph.md](pipeline-graph.md) called the fan-out clone "load-bearing." Both were
second-order next to the encoder.

None of it was the data model. It was all in how lines were built:

- `escape_tag`/`escape_measurement` built **four intermediate `String`s each** via chained
  `.replace()`, on every tag of every point, whether or not any character needed escaping.
- `metric_fields` returned a `Vec<(String, String)>`, with a `format!` per field name and a
  `to_string()` per value.
- The series-identity `HashMap` was keyed by `line.clone()` — a fresh `String` on every call, not
  just on insert.
- `render_tag_suffix` cloned the resource `AttrMap` and re-inserted every event attribute into it,
  per event, paying a `resolve` → `intern` round trip per key.
- `allocate_timestamp` built a fresh `Vec` for its path-compression walk, allocating on every
  timestamp collision — and a statsd multi-value datagram collides on essentially every line.

**The encoder now costs 30 allocations per 100-event batch, down from 18,024: a 600× reduction.**
(The pin reads 230: the other 200 re-sketch the raw `Samples` that `kv_metrics` emits since it
stopped sketching per event; §2's row explains.) The changes stayed inside `influxdb.rs`: escape
and format straight into reused buffers held on the encoder, merge-join the resource and event
attribute maps instead of cloning and re-inserting, borrow the series key for the lookup and
allocate it only on a miss, and reuse the path-compression scratch buffer. `stdio_out` got the same
treatment (§8 item 5): 1801 → 101 allocations per 100 events, ~18×, now 102 with the
`Bytes::from(String)` its `Encoder::encode` path adds.

What's left in both is per-*batch* or per-series, not a growing per-event cost: `influxdb_out` keeps
one `Bytes` for the finished body, one `String` key per distinct series on first sighting, and
growth of the per-series timestamp maps; `stdio_out`'s residual is almost entirely one
`format_rfc3339_utc` call per event. Protect that property: `influx_encode_100_events`/
`stdio_encode_100_events` fail if it regresses.

**Allocation count and wall-clock don't rank the two encoders the same way.** `stdio_out` allocates
~3.4× more than `influxdb_out`'s own 30, yet in §2's timing table it is faster (1.35 µs/event
against 2.57 µs/event). The two metrics are related, not interchangeable, which is why this
document tracks both. Don't read one run as a settled ranking.

**`syslog_out` got a narrower version of the same fix (§8 item 5).** It first measured 401
allocations per 100 events (~4/event): the header and message text, the pre-sanitize render, the
sanitized-message copy, and each sanitized header field were fresh `String`s per event. Three were
function-locals recreated on every `encode_into` call, so warming the call once didn't help: each
call's locals started from empty capacity again, where a struct field wouldn't. Hoisting them into
`SyslogEncoder`'s `line`/`raw_msg`/`scratch` fields, the pattern `InfluxLineEncoder` already used,
brought it to 100 (1/event), the same `format_rfc3339_utc` residual `stdio_out` carries.

**With the encoders fixed, the ingest chain is the cost again, and it dropped too.** `json`'s fix
(§8 item 4) took the full chain from 11 allocations to 5, and `kv_metrics` emitting raw `Samples`
instead of a per-event `DdSketch` ([ADR `kv-metrics-semantics`](../adr/kv-metrics-semantics.md))
took it to 3. Every ingest stage now costs at most 1, comparable to `Event::clone`'s 2 per extra
fan-out branch. §8 is ordered accordingly.

### Runtime: the node loops, not just the components they call

Every row above calls a decoder, transform, or encoder directly. This section measures what
`crates/logit-pipeline/src/runtime.rs`'s node loops (`run_transform`/`run_output`) add on top,
including the `internal` telemetry they carry (`docs/design/internal-telemetry.md`).
`run_transform`'s per-batch body is exported as `process_batch` (synchronous: no channel, no
runtime), and `run_output`'s as `send_batch` (async, like `Output::send`, but callable directly),
so both, plus `unwrap_batch`, are measured in `crates/logit-bench/tests/allocations.rs`'s
"Runtime" section like everything above:

| Path | allocs | Notes |
|---|---:|---|
| `process_batch` through `keep`, telemetry disabled | **0** | nothing at all — `1 → 0` when `Transform::process` went in place ([ADR `in-place-transform-process`](../adr/in-place-transform-process.md), §8 item 14): `process_batch` is a `Vec::retain_mut` over the batch's own `events` now, so there is no `out` `Vec` to collect survivors into |
| `process_batch` through `set` (attributes only) | **0** | identical to `keep` — `Transform::map_resource`'s default `None` return costs nothing beyond the call itself (`crates/logit-pipeline/src/transform.rs`) |
| `set.map_resource`, cached (same input `Arc`) | **0** | the one-entry `Arc::ptr_eq` cache (`crates/logit-transforms/src/set.rs`) hits |
| `set.map_resource`, cache miss (distinct input `Arc`) | **1** | `Arc::new(Resource { .. })` only — cloning/inserting into the fixture's empty, inline `AttrMap` never touches the heap |
| `process_batch` through `has_attributes`, forwarding | **0** | identical to `keep`/`set` — `retain_mut` keeps a forwarded event exactly where it already is; `crates/logit-transforms/src/attributes.rs`'s `Matcher::matches` contributes nothing |
| `process_batch` through `drop_attributes`, dropping every event | **0** | dropping costs no more than forwarding: `retain_mut` drops each rejected event in place — same shape as the fully-absorbed `aggregate` row below, for a filter rather than an accumulator |
| `has_attributes`/`drop_attributes`, resource match, cache hit (same input `Arc`) | **0** | `Matcher`'s one-entry `Arc::ptr_eq` cache — `Set::map_resource`'s caching idiom, applied to a read instead of a rebuild |
| `has_attributes`/`drop_attributes`, resource match, cache miss (distinct input `Arc`) | **0**, not **1** | the one place this diverges from `set.map_resource`'s own cache-miss row above: a miss here only re-evaluates `AttrMap::get_sym` against the `Arc` a caller already passed in, never allocates a replacement `Resource` — `native::decode` mints a fresh `Arc<Resource>` per frame, so the fan-out-after-`logit_in` topology this component exists for (`docs/adr/attribute-filtering-components.md`) always misses this cache, and this row is why that's fine |
| `trace_context`, lifting a valid `trace_id` | **0** | identical to `keep`/`set`'s own rows — nothing on the batch path, and `parse_trace_id` (`logit_core::trace`) works on stack arrays while `AttrMap::remove` (`keep_source: false`) is an in-place `SmallVec` shift |
| `trace_context` with a `span:` block, minting a `SpanRecord` from the convention (`traceparent` + ids + `span.end_s`/`span.duration_s`) | **0** | the same nothing; ids parse to stack arrays, timing is integer arithmetic, the span's `name` is the transform's pre-built `Value` cloned (a `Bytes` refcount bump), `events`/`links` are `Vec::new()`, `SpanRecord` is inline in `Event` (§1) so `event.span = Some(..)` is a 144-byte move, and the ~7 consumed attributes are in-place removes (`docs/adr/trace-context-span-lifting.md`) |
| `run_lua`: `set_resource` + `process` + `take_resource`, script never writes `resource` | **9** | identical to plain `process` (below) — `set_resource`/`take_resource` are field assignments, no allocation |
| `run_lua`: `set_resource` + `process` + `take_resource`, script writes `resource` | **7** | see `crates/logit-script/src/resource.rs` — lower than the row above because this script (unlike `LUA_ENRICH_SCRIPT`) never touches `event.attributes`, skipping its `AttrsProxy` cost; the `+1` here is `take_resource`'s `Arc::new(Resource { .. })` commit |
| `process` reading `event.log.trace_id` (`LogProxy`) | **9** | same total a script touching `event.attributes` instead pays (`crates/logit-bench/tests/allocations.rs`'s `lua_process_one_event`) despite touching no attributes at all — creating and caching the `LogProxy` userdata costs what `AttrsProxy` does there, `to_hex`'s returned `String` costs what an attribute write does; a script that never touches `event.log` pays none of it, unchanged at 9 either way |
| `process` reading `event.metrics[1].value` (`MetricsProxy`/`MetricProxy`) | **11** | not the 9 a naive add-up predicts (4 baseline + 1 `Box` + 3 for `MetricsProxy`'s own first-access create-and-cache + 1 for the "one small allocation" `MetricProxy`'s doc comment assumes a per-index handle costs) — measured directly (`lua_process_one_event_reading_metric_value`) against a script that indexes `event.metrics[1]` but never reads `.value` (still 11, so the field read itself is free, the same reason `event.span.name` below is) and one that indexes it *twice* (14, exactly +3 more) — a fresh, uncached `MetricProxy` costs the same 3 allocations `AttrsProxy`/`LogProxy`/`SpanProxy` pay for create-**and**-cache via a `RegistryKey`, even though it never caches one; see §8 item 6 and `crates/logit-bench/tests/allocations.rs`'s comment for the full finding |
| `process` reading `event.metrics[1].value`, event has a **spilled** (9-attribute) `AttrMap` | **11** | identical to the row above, and to that same script's own passthrough baseline on this fixture (**5**, `lua_process_one_event_passthrough_on_a_spilled_event`) plus 6 — the regression guard for `MetricProxy::event` being a `Weak<RefCell<Event>>` rather than a strong `Rc` (`crates/logit-script/src/proxy.rs`): a strong `Rc` left alive by an uncollected `event.metrics[1]` temporary would make `EventProxy::into_inner`'s `Rc::try_unwrap` fall back to a real `Event::clone` here, a cost `sum_metric_event`'s own empty, inline `AttrMap` could never make visible above (`lua_process_one_event_reading_metric_value_on_a_spilled_event`) |
| `process` reading `#event.metrics` only (`MetricsProxy`, no index) | **8** | 4 baseline + 1 `Box` + 3 for `MetricsProxy`'s first-access create-and-cache, and nothing more — confirms the row above's extra 3 comes entirely from indexing, not from touching `event.metrics` at all (`lua_process_one_event_reading_metric_len`) |
| `process` reading `event.span.name` (`SpanProxy`) | **8** | 4 baseline + 1 `Box` + 3 for `SpanProxy`'s first-access create-and-cache, the same bucket `AttrsProxy`/`LogProxy`/`MetricsProxy` pay for theirs; the `Value::Str` read itself costs nothing (`lua_process_one_event_reading_span_name`) |
| `run_lua`: `set_scope` + `process` + `take_scope`, script never writes `scope` | **9** | identical to plain `process` — `set_scope`/`take_scope` are field assignments, no allocation, the same contract `set_resource`/`take_resource` already have (`lua_process_one_event_with_scope_hooks_but_no_write_costs_the_same_as_process_alone`) |
| `process` reading `scope.name` | **5** | 4 baseline + 1 `Box`, **no** first-access `+3` — unlike every `EventProxy` sub-proxy, `scope`'s `ScopeProxy`/`ScopeAttrsProxy` are installed once in `ScriptWorker::new`, before any measured call, so there is no per-event userdata to create or cache here (`lua_process_one_event_reading_scope_name`) |
| `process` writing `scope.attributes.k` (first write this batch) | **7** | 4 baseline + 1 `Box` + 1 (`lua_to_scope_value`'s `Bytes::copy_from_slice` for the new string) + 1 (`take_scope`'s `Arc::new(Scope { .. })` commit) — **not** a separate cost for `ensure_modified`'s `Scope` clone: the fixture's `name`/`version` are `Bytes::from_static` (never promoted) and `attributes` starts empty and inline, so the clone itself is a plain memcpy (`lua_process_one_event_writing_scope_attribute`) |
| `process` writing `resource.schema_url` | **7** | 4 baseline + 1 `Box` + 1 (`write_schema_url`'s `Bytes::copy_from_slice`) + 1 (`take_resource`'s `Arc::new(Resource { .. })` commit) — same total as writing a resource attribute, for the same reason (`lua_process_one_event_writing_resource_schema_url`) |
| `process` with `scope.name = scope.name` (identity write) | **5** | identical to a plain `scope.name` read above — the no-op check (`scope_name(&state) == s`) catches the identity assignment before `ensure_modified` ever runs, so `take_scope` still returns `None` (`lua_process_one_event_identity_write_to_scope_name_is_free`) |
| `process` returning `Event.new{timestamp = "1", attributes = {env = "prod"}, log = {message = "hi"}}` in place of the incoming event (`crates/logit-script/src/construct.rs`) | **17** | 4 baseline + 6 for the construction itself (a fresh `EventProxy`'s `Rc<RefCell<Event>>` and userdata, plus mlua's own cost for calling a Rust closure and returning its userdata — constructing and discarding lands at 10) + 1 `Box` + 3 for the `attributes` sub-table with one entry (1 for the table, `lua_to_value`'s `Bytes` for `"prod"`, 1 for the map's first insert; each further attribute adds 1) + 3 for the `log` sub-table with its `message` (the same shape; `LogRecord` is inline in `Event`, no box). Additive: every other `lua:` row above is unchanged, so a script that never calls `Event.new` pays nothing (`lua_process_one_event_constructing_a_log_event`) |
| `process` returning `Event.new{timestamp = "1", metrics = {{name = "tick", kind = "gauge", value = 1}}}` in place of the incoming event | **16** | the log row's 4 baseline + 6 construction + 1 `Box` (11) + 1 for the `metrics` array table (an empty list stays inline, 12) + 4 for the one record: 1 for its sub-table, 1 for `validated_sequence_len`'s key `Vec` over the array (once per event; a second metric adds 3 plus the `MetricList` spill, 20 in all), 1 for the `format!`'d `metrics[i]` error-path prefix (the one deliberate per-metric cost; a static path lands at 15), and 1 that every non-empty record sub-table carries beyond its table and fields — the same bucket the log row folds into `message`. `intern("tick")`, the `expect_keys` walk and every nil-defaulted field allocate nothing (`lua_process_one_event_constructing_a_gauge_event`) |
| `process` returning `Event.new{timestamp = "1", span = {trace_id = <hex>, span_id = <hex>, name = "GET /"}}` in place of the incoming event | **14** | the log row's 4 baseline + 6 construction + 1 `Box` (11) + 3 for the `span` sub-table with its three required fields: 1 for the table, 1 for `lua_to_value`'s `Bytes` for the `name` (`name = 1` lands at 13), and the same 1 every non-empty record sub-table carries beyond its table and fields. The two hex ids parse straight into their arrays, `SpanRecord` is inline in `Event`, and `Vec::new()` for `events`/`links` is free. Every defaulted or scalar field adds 0 — `kind`/`status`/`end_timestamp`/`parent_span_id` set explicitly still land at 14, as does an explicit `dropped_events_count = 0` (a default value never earns the `SpanExt` box); `status_message = "boom"` adds 2 (the box and its `Bytes`), an empty `events = {}`/`links = {}` 1 each, one span event with a name 7, one link with just its ids 6 (`lua_process_one_event_constructing_a_span_event`) |
| `process_batch`, fully absorbed (`aggregate`) | **0** | nothing to throw away unused: absorbing every event just empties the batch's own `Vec` in place |
| `process_batch` through `keep`, telemetry live, **steady state** | **0** | identical to disabled — `count`/`timer` update an existing `ComponentBuffer` entry in place, no allocation of their own |
| `process_batch`, **first call after an `internal` drain** | **2** | a `HashMap` table rebuild (1) + a fresh `DdSketch` (1) — see below; this used to be 3, the third being the `out` `Vec` the in-place `process` removed |
| `unwrap_batch` (`Delivered::Owned`) | **0** | no `Arc` was ever involved |
| `unwrap_batch` (`Delivered::Shared`, sole reference) | **0** | `Arc::try_unwrap` succeeds |
| `unwrap_batch` (`Delivered::Shared`, contended) | **3** | falls back to `EventBatch::clone` — 1 for the `Vec<Event>` + `Event::clone`'s 2 (nginx shape) |
| `send_batch` through a no-op `Output`, telemetry disabled | **1** | `#[async_trait]` boxing its future (below) — nothing to do with telemetry |
| `send_batch`, telemetry live, **steady state** | **1** | same 1 as disabled — telemetry adds nothing on top of the box |
| `send_batch`, **first call after an `internal` drain** | **3** | the box (1) + the same `HashMap`/`DdSketch` rebuild as `process_batch`'s (2) |
| `send_batch` through a **failing** `Output`, telemetry disabled | **4** | the box (1) + `anyhow!(..)` constructing the error (1) + `.with_context(..)` (2: the `format!` message, and wrapping into a new boxed `anyhow::Error` node) — see below |
| `send_batch`, failing, telemetry live, **first failure** (success keys already warm) | **5** | the failure baseline (4) + 1 — `logit.component.errors` is a brand-new 4th map key, which can grow the buffer even though the first 3 already fit |
| `send_batch`, **failing**, first call after an `internal` drain | **7** | not simply 4 + 3 — a map absorbing 4 fresh keys (not 3) in one call can need more than one growth step; measured, not derived |

Four findings:

1. **In steady state, telemetry is free.** `process_batch_with_live_telemetry` and
   `send_batch_through_a_noop_output_telemetry_live` match their disabled counterparts exactly:
   `count`/`timer` update an already-resident `ComponentBuffer` entry in place. `Fanout::send`'s
   telemetry is free too (`docs/design/internal-telemetry.md`'s tests); this is the receive side.

2. **The first call after every `internal` drain costs 2 more allocations, and that recurs for as
   long as `internal` runs.** `ComponentBuffer::drain` (`crates/logit-core/src/telemetry.rs`)
   `mem::take`s the whole `points` map on every `internal` tick, so the next `count`/`timer` for
   each key inserts into an empty map instead of updating: 1 for the map's backing table and 1 for
   a fresh `DdSketch` for the timing key. That is on top of the disabled baseline (nothing for
   `process_batch`, the `async_trait` box for `send_batch`), once per drain interval, on every
   component `internal` is attached to.

3. **Every `output.send(..).await` heap-allocates its future**, because `Output` is
   `#[async_trait]` (`crates/logit-pipeline/src/output.rs`). A direct call (no `dyn Output`) and the
   `dyn Output` call `send_batch` makes both cost exactly 1 (16 bytes), so this is `async_trait`'s
   boxing, not dynamic dispatch. It is a per-batch cost on every sink, unrelated to telemetry, and
   not fixed here. A hand-written `Pin<Box<dyn Future<...>>>` method is not a fix: that return type
   needs the same allocation, since the box *is* how a `dyn Trait` object returns a future of
   unknown, implementer-varying size. A real fix gives up `dyn Output` for the call: enum dispatch
   over the closed set of concrete `Output` kinds, or a per-node generic runtime.

4. **A failing send is a distinct allocation shape, not a bigger success number.** A `NoopOutput`
   that always succeeds never exercises `logit.component.errors` or `result.with_context(...)`, so
   `FailingOutput` (always `Err`) backs the three failure rows. The failure baseline decomposes
   exactly, checked against each isolated piece:
   `1 (async_trait box) + 1 (anyhow!) + 2 (.with_context) = 4`. `anyhow::anyhow!(..)` builds the
   error, and `.with_context(..)`'s 2 are the `format!` message and the new boxed `anyhow::Error`
   node.

`crates/logit-bench/benches/pipeline.rs`'s `runtime` module times the same paths, including
`Fanout::send`+`recv` across a real `tokio::sync::mpsc` channel and `send_batch` through
`#[async_trait]`. Both columns are trustworthy despite the channel hop and the trait-object call:
each bench drives a **current-thread** runtime with no `tokio::spawn`, so nothing leaves the one OS
thread divan's `AllocProfiler` watches. Measured this way:

| Path | fastest | allocs |
|---|---:|---:|
| `process_batch` through `keep` | 525 ns | 0 |
| `Fanout::send`+`recv`, 1 consumer | 210 ns | 0 |
| `Fanout::send`+`recv`, 2 consumers | 630 ns | 4 (1 `Arc::new` + the 3-allocation contended `unwrap_batch` above) |
| `send_batch` through a no-op `Output` | 206 ns | 1 (the `async_trait` box) |
| `send_batch` through a **failing** `Output` | 319 ns | 4 (matches the disabled-telemetry failure row above exactly) |

This table is from the same 2026-09-20 perf-VM `script/bench` run as §2's timing table
(`taskset -c 2`), and its timings don't compare with earlier laptop measurements, for the reason
that table's note gives. One laptop result still stands, because it is *relative*: on 2026-09-17,
on the same pinned laptop core (`taskset -c 2`, fastest of three), `process_batch` measured **469 ns
/ 1 alloc** before `Transform::process` went in place and **431 ns / 0** after, an ~8% win from the
removed `out` `Vec` and the removed per-event `Event` move. It wasn't re-derived on the VM, and the
code path hasn't changed since. The 360 ns this row carried before that pair came from an earlier
build and machine state and was never comparable to either number.

### Costing internal spans: the `Delivered` trade, measured

`docs/known-gaps.md`'s internal-spans entry gated carrying trace context on `Delivered` on measured
evidence, per [ADR `minimize-allocations-over-event-size`](../adr/minimize-allocations-over-event-size.md).
This section is that evidence, recorded so the decision didn't have to re-derive it. The decision
itself is [ADR `trace-context-propagation-on-delivered`](../adr/trace-context-propagation-on-delivered.md).

**The prototype.** A 24-byte `TraceContext { trace_id: [u8; 16], span_id: [u8; 8] }` was added to
both `Delivered` variants (`Owned(EventBatch, TraceContext)`,
`Shared(Arc<EventBatch>, TraceContext)`) and minted fresh per `Fanout::send`/`send_blocking` call
from a thread-local SplitMix64: no allocation, no new dependency. It deliberately wasn't
`tracing::span::Id`, which a `Registry` recycles after a span closes, making it unsafe as an
identity. There was no parent propagation and no `run_output` plumbing beyond the match arms the
new field required: this measured the type change's cost, not a span feature. It was built, measured, and reverted in full.

**Size: `size_of::<Delivered>()` went from 32 to 56**, exactly `TraceContext`'s 24 bytes, with no
padding. That is a per-*batch* cost on the channel payload, not a per-event one. ADR
`minimize-allocations-over-event-size` settled that trading a smaller per-event size cost for an
allocation isn't worth it for `Event`; `Delivered` is a different type at a different multiplier
(one per batch), so that conclusion is a contrast here, not the answer.

**Allocations: zero change.** Every `fanout_send_*` constant in
`crates/logit-bench/tests/allocations.rs` (0 / 6 / 1 at the time, including the mixed-consumer
cases) and every "Runtime" constant that existed then held exactly with the prototype in place, and
the full `script/cibuild` suite passed unmodified. Copying 24 bytes into an already-allocated enum
payload doesn't touch the allocator. The Runtime section's later additions (the post-drain cost,
`send_batch` coverage) weren't re-verified against the prototype. By mechanism they shouldn't
interact with `Delivered`'s size, since the post-drain cost lives in `ComponentBuffer`'s map and
sketch and the `async_trait` box in `Output::send`'s call, but that is a code-reading argument, not
a measurement.

**Throughput: no attributable regression.** A naive before/after showed all three `runtime` benches
(`fanout_send_one_consumer`, `fanout_send_two_consumers`, `process_batch_through_keep`) ~40-50%
slower with the prototype. But `process_batch_through_keep` never touches `Delivered`/`Fanout` and
moved by almost the same percentage, and re-running the *unmodified* benches reproduced the
pre-prototype numbers almost exactly. That was cross-run timing noise, the caveat at the top of this
document. A real timing comparison needs a same-session, back-to-back run, which this didn't do.

**What the prototype didn't measure:** propagating an *inherited* context, taking a batch's
incoming `Delivered` as the parent of what the node produces instead of always minting a fresh
root. That touches `run_transform`/`run_output` themselves, a bigger change than the type-and-copy
cost measured here.

**What was built on this evidence.** ADR `trace-context-propagation-on-delivered` implemented
propagation for the two node kinds with an unambiguous parent (`Transform::process`/
`ScriptWorker::process`'s non-flush path, and `run_output`, which needed no new wiring). Every
allocation-count assertion held exactly against the real implementation too. See
`docs/design/pipeline-graph.md`'s "Trace context propagation" section for the per-node-kind
account, and `docs/known-gaps.md`'s internal-spans entry for what's still open.

[ADR `internal-span-emission-and-deterministic-sampling`](../adr/internal-span-emission-and-deterministic-sampling.md)
then built emission (a real `Telemetry::span`/`SpanGuard`, a bounded per-component span buffer, and
`ComponentBuffer::drain`'s span-emitting pass) without changing any number here. Its sampler is
deterministic on `trace_id` (`trace_is_sampled`), so no `sampled` bit is propagated and
`TraceContext`/`Delivered` gained nothing: `size_of::<Delivered>()` stayed 56.

`Delivered` has grown twice since, neither time with an allocation:

- **To 64**, when its second element became `BatchContext`: `TraceContext` plus an 8-byte
  `Provenance` naming the component that created the batch and the one that last handled it
  (`docs/adr/batch-provenance-on-delivered.md`). Two `Option<Symbol>`s, `Copy`, so the cost is
  `CHANNEL_CAPACITY * 8` bytes per inbox, noise against `Event`'s 864.
- **To 72**, with [ADR `metrics-model-v2`](../adr/metrics-model-v2.md): `EventBatch` gained
  `scope: Option<Arc<Scope>>`, one more pointer-sized field (`Arc<Resource>` 8 + `Option<Arc<Scope>>`
  8 + `Vec<Event>` 24 = 40, plus `BatchContext`'s 32). The same `fanout.rs` test pins it.

**Which constants prove the unsampled path is free.** `SpanGuard`'s disabled/unsampled form holds no
state, mirroring `Timer`, so an unsampled span should cost what a disabled `Telemetry` handle does.
But not every allocation constant exercises a `Telemetry::span` call site:

- `fanout_send_*` (`Fanout::send`/`send_blocking`, the listener span site) build their `Fanout` via
  `Fanout::new` with **no** `.with_telemetry(...)` call. That is `Telemetry::default()`, fully
  disabled (`self.0` is `None`), and `Telemetry::span` returns `SpanGuard::disabled()` on its first
  line, *before* calling `trace_is_sampled`. These constants prove the disabled path is free, not a
  live registry sampling below `1.0`.
- `process_batch_*`/`send_batch_*`'s "telemetry live" variants attach a real `Telemetry` from a
  live `Registry`, but the `Telemetry::span` calls live one level up, in
  `run_transform`/`run_flush`/`run_lua`/`write_loop`, which `logit-bench` doesn't drive under
  `CountingAlloc`. They don't exercise the sample decision either.
- `unwrap_batch_*` has no span site on any path.

`fanout_send_one_consumer_with_a_live_unsampled_registry_costs_nothing` closes the gap for the one
span site `logit-bench` drives under `CountingAlloc`. Its `Fanout` carries a real `Telemetry` from
`Registry::with_span_sampling(0.0)`, attached the way `crates/logit-cli/src/pipeline.rs::prepare`
attaches one in production and deterministically never sampled, instead of relying on a fixture's
`trace_id` missing the default 0.1 band. It costs 0 allocations, like the disabled case:
`Telemetry::span` reaches `trace_is_sampled`, gets `false`, and returns the same
`SpanGuard::disabled()`. **Not covered:** the same proof for
`run_transform`'s/`run_flush`'s/`run_lua`'s/`write_loop`'s span sites. They call the same
`Telemetry::span` with the same early return, but that is a code-reading argument for those four
sites, not a measurement. Close it the same way if one becomes benchmarkable on its own.

**What a *sampled* span costs** is measured in `crates/logit-core/src/telemetry.rs`'s own tests,
since it is `logit-core`-local state, not a runtime or channel hop. Two costs,
both on top of whatever the node visit already costs:

- One `PendingSpan` pushed into `ComponentBuffer`'s `Vec` at `finish`/`Drop` time. **This recurs
  once per `internal` drain interval**, like the points map's post-drain cost (the Runtime
  section's "first call after an `internal` drain" rows): `ComponentBuffer::drain`'s span pass takes
  the `Vec<PendingSpan>` with `mem::take`, leaving a zero-capacity `Vec`, so the first sampled span
  after any drain pays a fresh `Vec` growth.
- One `Value::str` (a `String` allocation) built at `ComponentBuffer::drain` time for the span's
  `name` (`"aggregate flush"`, say). It is deferred that far so a span that never reaches a drain
  (still buffered, or dropped past `MAX_SPANS_PER_COMPONENT`) never pays it.

### Zero-copy: where it holds

[data-model.md](data-model.md) commits to "`bytes::Bytes` everywhere strings and blobs appear," so
a field parsed out of a socket read buffer is a refcounted slice of that buffer, not a fresh
allocation. The datagram decoders keep that commitment (§2's decode rows); `statsd_in` broke it
until §8 item 3 fixed it.

`syslog_in` is the exemplar. `slice_of` rebuilds a `Bytes` for each extracted field by pointer
arithmetic back into the datagram, so decoding a line costs exactly one allocation (the `Vec`)
however many fields it yields. `crates/logit-bench`'s `syslog_fields_share_the_datagram_allocation`
asserts this structurally, not just by count. `json` continues it: `ValueSeed` deserializes straight
into `Value` with no intermediate `serde_json::Value` tree, and `borrowed_str_bytes` keeps an
unescaped string a slice of the message buffer, copying only a string serde had to unescape.

`statsd_in` used to build attribute values with `attributes.insert(k, v)` on a `&str`, which went
through `impl From<&str> for Value` → `Value::str` → `Bytes::from(String)`, copying bytes already in
the datagram; `build_event`'s `attributes.clone()` then promoted each to a shared `Bytes`, a second
copy. That was six of the pre-fix eight allocations. It now uses `syslog.rs`'s `slice_of`
reconstruction, asserted structurally by `crates/logit-bench`'s
`statsd_tag_values_share_the_datagram_allocation` (which replaced
`statsd_tag_values_are_copied_not_sliced`). The 2 remaining allocations are a `Vec<Event>` per line
plus one for the batch: the same irreducible pair `syslog_in` has, split across two `Vec`s because
statsd's multi-value form lets one line produce several events.

### Retention: what pins what

Zero-copy slicing trades allocation count for **retention**. Every field of every event decoded
from a datagram references that datagram's buffer, so the buffer lives until the last event derived
from it is dropped. The trade is right here because the buffer is right-sized: the shared UDP
driver (`crates/logit-inputs/src/udp.rs`) does `Bytes::copy_from_slice(&buf[..n])`, allocating
exactly `n` bytes, not the 64 KB receive buffer. The batched `recvmmsg(2)` read
([ADR `udp-intake-batching-and-socket-visibility`](../adr/udp-intake-batching-and-socket-visibility.md))
keeps this: it reads into a slab of `read_batch` such buffers (§5) and still makes one right-sized
copy per datagram out of the slot it landed in.

**Don't "optimize" this copy away.** Reading into a large shared `BytesMut` and `split_to`-ing
each datagram off it would save one allocation per datagram, and would let a single retained log
line pin a 64 KB chunk. When a slow sink can hold events for seconds, that is a far worse failure
mode than one small memcpy per datagram. `datagram_copy_is_one_right_sized_allocation` guards the
current behavior.

### The native wire format (`logit_proto::native`)

The native format is a hop between processes, not a stage in the reference pipeline, so it gets its
own table. `logit_out`/`logit_in` (`docs/plans/native-transport.md`) each do slightly less work
than the raw `NativeEncoder`/`NativeDecoder` pair: `logit_out` skips `NativeEncoder`'s bundling and
calls `encode_batch`/`write_frame_with_flags` directly, to frame with whatever compression the
connection negotiated; `logit_in` has no caller-held scratch buffer to `out.extend` into the way
`NativeDecoder::decode_into` does, since `Fanout::send` takes the `EventBatch` `decode_batch`
returns. (The disk buffer's encode cost is §2's `disk_queue` row.) One event
(`fixtures::nginx_batch(1)`), `crates/logit-bench/tests/allocations.rs`:

| Stage | allocs | Notes |
|---|---:|---|
| `NativeEncoder::encode`, 1 event | **30** | 32 -> 30 once `kv_metrics`'s two distributions became inline `Samples` -- a `Distribution` record serializes its sketch via `to_java_bytes` (one blob each), a `Samples` writes its values directly; dictionary build + the per-field TLV scratch buffers `native::record::write_field` allocates for each of `Event`'s up-to-five fields -- 23 -> 32 with [ADR `metrics-model-v2`](../adr/metrics-model-v2.md), which TLV-framed every record too: the fixture's four metrics each pay one scratch buffer for their `kind` field and one length prefix as a list entry (+8), and the log's `message` one (+1) (`docs/adr/native-wire-format-encoding.md`'s own Decision section notes this as a known, unoptimized cost of the field-level skip-unknown framing) |
| `logit_out`: encode + frame, 1 event | **30** | `encode_batch` + `write_frame_with_flags` directly — same cost as `NativeEncoder::encode` above, since it's the same two steps; pinned separately so a future change to just this sink's path is caught here |
| `NativeDecoder::decode_into`, 1 event | **8** | dictionary re-intern + `AttrMap`/`Event` construction; no intermediate object graph, unlike the bake-off's `rkyv`/`postcard` arms, which run through a `WireBatch` mirror first (`docs/adr/native-wire-format-encoding.md`'s finding 4) |
| `logit_in`: read + decode, 1 event | **7** | `read_frame_with_header` + `decode_batch` directly — one allocation cheaper than `NativeDecoder::decode_into` above: no caller-held `Vec<Event>` to `out.extend` into, since `decode_batch`'s own freshly allocated `Vec` is what `Fanout::send` takes as-is |

The same pair over the six survey-derived shapes (`docs/plans/event-sizing.md` W1), one event per
batch except the last:

| Shape | encode | decode | Notes |
|---|---:|---:|---|
| 12-attribute flat JSON log | **16** | **5** | |
| 10-attribute nested pino-http record | **24** | **9** | decode's extra four are the boxed `Value::Map`s, same cause as `json`'s |
| 30-attribute access log | **24** | **5** | **decode is flat in width** -- the same 5 as the 12-attribute row, with the growth chain showing up as reallocs instead. This is `read_attr_map_at` reading the exact count off the wire and discarding it, then rebuilding the map by 30 sorted `insert_sym`s in the *writer's* symbol order, which dictionary remapping has already made unsorted for the reader |
| 17-attribute server span | **20** | **5** | |
| 3-record collectd event | **20** | **5** | |
| 5 events, 17-attribute `Resource` | **53** | **10** | the one measurement where the resource's own width is paid: `logit_proto::native` writes it once per batch rather than `Arc`-sharing it |

Encode cost tracks the number of *fields* written, not the attribute count: the span and the
three-record collectd event write more structure than the 12-attribute log while carrying fewer
attributes.

The bake-off against `otlp`, `rkyv`, and `postcard` (two shapes, three batch sizes, timing and
encoded bytes) lives in [ADR `native-wire-format-encoding`](../adr/native-wire-format-encoding.md).
Those numbers came from `script/bench wire_format` (`crates/logit-bench/benches/wire_format.rs`):
one-off comparison data, not a per-build assertion.

## 3. Sharing versus copying

What is shared today:

- **`Resource`** — `Arc`-shared across a whole batch, never copied per event.
- **String and blob data** — `Bytes`, refcounted; a clone is an atomic increment.
- **Attribute keys and metric names** — interned to a 4-byte `Symbol` (see §4).

What is copied, and when, depends on fan-out shape (below). One guarantee holds in every shape: a
mutation on one branch of a fan-out is structurally invisible to a sibling branch, with nothing
extra to maintain. `runtime.rs`'s `a_mutation_on_one_fan_out_branch_is_invisible_to_the_sibling_branch`
pins it; no change below may regress it.

When a mutating branch has to copy, it pays a deep clone: for the nginx shape, 2 allocations and an
864-byte memcpy per event per extra branch, 316 ns, ~15% of the ingest chain (§2).

### The `Arc<EventBatch>` copy-on-write change

Channels carry `Arc<EventBatch>`, and each consumer does
`Arc::try_unwrap(batch).unwrap_or_else(|shared| (*shared).clone())`. It landed in three rounds
(`docs/adr/arc-eventbatch-copy-on-write.md`), each correcting an overclaim in the one before,
starting from "strictly no worse anywhere." The measured result, by fan-out shape:

| Fan-out shape (2 consumers) | Allocations | vs. the pre-`Arc` code's flat 3 |
|---|---:|---|
| Single consumer (any kind) | **0** | strictly better |
| Both `Output` | **1** | strictly better |
| One `Output`, one `Transform`/Lua | **1 or 4** | scheduling-dependent, either direction |
| Both `Transform`/Lua-style, no `Output` | **4** | 1 worse, always |

The clone-bearing rows cost 2 less than when this landed (then 6 against a flat 5), because
`Event::clone` on the nginx shape dropped from 4 to 2 once `kv_metrics`'s distributions became
inline `Samples`. The relative story is unchanged.

**Unconditionally better: single-consumer edges and all-`Output` fan-outs.** A single-consumer
edge (every shipped listener's first hop, and every interior edge of a linear chain) costs nothing:
the `Delivered::Owned | Delivered::Shared(Arc<_>)` payload skips the `Arc` when there's only one
consumer. An all-`Output` fan-out is free past the one `Arc::new`, because `Output::send` takes
`&EventBatch`: a read-only sink branch never calls `Arc::try_unwrap`, it borrows through the `Arc`
however many sibling branches still hold a handle. Each read-only branch pays one atomic, not a
clone.

**Racy: one `Output` branch plus one mutating (`Transform`/`ScriptWorker`) branch.** This is the
common shape, the nginx reference config's `tap`/`trimmed` split. It costs **1 or 4**, decided by
tokio scheduling, never in between and never the pre-`Arc` code's flat 3. If the mutating branch's
`unwrap_batch` finds the `Output` branch's handle already gone, it unwraps for free (1); if the
handle is still alive, it clones (4). Two tests pin each ordering
(`fanout_send_mixed_output_and_transform_consumers[_when_output_finishes_first]`).

**4 is the likelier outcome.** `drain_inbox` moves the `Output` branch's handle into the sink's
store (`SinkStore::push`). A `Memory` store holds it until `write_loop` commits the batch after
`output.send`, typically real I/O and slower than a `Transform`'s local work, so the mutating
branch usually finds the handle alive and clones. A `Disk` store drops it once the record is
written, which shortens the window but doesn't close it. That is an expectation about typical
scheduling, not a guarantee; both outcomes stay reachable.

**An `Output` branch pays one more hop past `Fanout::send`.** The table measures `Fanout::send`
alone. A sink's batch then goes through `drain_inbox` (`runtime.rs`,
[ADR `buffered-sink-delivery`](../adr/buffered-sink-delivery.md)), which moves it off the inbox into
the `SinkQueue`. The queue needs an `Arc<EventBatch>`, so a `Delivered::Owned` batch from a
single-consumer edge costs exactly one `Arc::new` there, pinned by
`drain_inbox_single_consumer_owned_batch_costs_exactly_the_arc`
(`crates/logit-bench/tests/allocations.rs`), which drives `drain_inbox` without a full `run`. A
`Delivered::Shared` batch already carries its `Arc`, so the hop costs nothing further.

**Doesn't improve: a fan-out with no `Output` branch** (two `Transform`s, or a `Transform` and a Lua
stage, sharing one node). Both sides mutate, so neither can borrow. This is round one's
`1 + (N-1) × clone`: deterministically 4 for two consumers, one worse than the pre-`Arc` code, with
no racy path to anything better. Closing it would mean widening `Transform`/`ScriptWorker` beyond
mutating or consuming an owned `Event`, which isn't on the table.

**A fix for the racy case was sketched and not taken.** For exactly one consumer of each kind,
making `Fanout` aware of which is which and giving the mutating one an unconditional direct clone
(no `Arc`) would turn 1-or-4 into a fixed 4: predictability instead of the chance at 1. It doesn't
generalize. For 2 borrowing + 2 owning consumers, worked through at the batch clone's cost at the
time (5), the aware design costs 11 with a direct clone per owning consumer, or 12 with a second
dedicated `Arc` for the owning group to race over (the extra `Arc::new` outweighs what the race
saves). At that same clone cost, the racy design already reaches 6 for that shape whenever every
`Output` branch finishes first. So the aware fix would make today's *worst* case the *guaranteed* one once either
group has more than one member. It stays an open design problem; the ADR's Alternatives section has
the full working.

Two properties held through all of this:

- **`Transform::process` never changed for the `Arc` plumbing**: the wrap/unwrap boundary sits
  entirely inside `logit-pipeline`. `Output::send`'s signature (`&EventBatch`, not owned) was the one
  trait-level change.
- **The `Arc` wraps the *batch*, not each `Event`**: one atomic per batch instead of an allocation
  and an atomic *per event*, which would be worse than no `Arc` for a single consumer. Vector's
  `LogEvent` (`Arc<Inner>` with copy-on-write) is prior art for the same choice.

A separate change removed more per-hop cost: `Transform::process(&mut self, &Arc<Resource>, &mut
Event) -> bool` plus `Vec::retain_mut` in `process_batch` saves one 864-byte `Event` memcpy per node
hop and one `Vec` allocation per batch per node. The trait already couldn't emit more than one event
per input, so a bool says everything the `Option<Event>` return did. See
[ADR `in-place-transform-process`](../adr/in-place-transform-process.md) and §8 item 14; the
`process_batch` rows in §2's "Runtime" table are the result.

### Routing: a partition pass instead of N clones (ADR `target-components`)

[ADR `target-components`](../adr/target-components.md) gives the graph a second way to fork a
batch besides `Fanout`'s unconditional fan-out: a `Router` node (`route`, or `lua`/`lua_file` with
`targets:`) moves each event into exactly one of `1 + targets.len()` destination buffers in one
pass over the batch (`crate::runtime::route_batch`, `crates/logit-pipeline/src/router.rs`), then
sends every non-empty one under a single child `BatchContext`. The rule this buys, measured exactly
(`crates/logit-bench/tests/allocations.rs`'s `// Routing` section, all zero per-event allocations,
one `reserve_exact` per used destination):

| Shape (`route`, 64 events, `by: {attribute: stream}`) | Allocations |
|---|---:|
| 2 targets, evenly split (both used) | **3** |
| All 64 to one target (many-to-one `routes:`) | **2** |
| All 64 unrouted (no match — the router's own `Forward` edge) | **2** |

**The rule is `1 + (destinations that received events)`, never per event and never for a
destination nothing was routed to**: 1 for the returned partition list itself
(`Vec::with_capacity(used)`), plus exactly one `reserve_exact` per non-empty destination —
`RouterScratch`'s `marks`/`counts` buffers amortize to zero after warm-up, but each destination's
`Vec<Event>` is handed out by `mem::take` (leaving capacity 0 behind), so its `reserve_exact` is
paid again every batch, by design — that's the whole partition cost, and it's an integer, not a
logarithm.

Against that, the shape a router replaces — an N-way `Fanout` plus N `has_attributes` filters, each
scanning the *whole* batch to keep its own slice — costs `fan_out_plus_two_has_attributes_for_the_
same_split`'s **194** for the identical 64-event, 2-way `stream: host`/`stream: app` split: 1
`Arc::new` (`Fanout::deliver`'s once-per-send wrap) + 193 for the forced `EventBatch` deep clone
that a fan-out with no `Output` branch always pays (1 for the clone's own `Vec<Event>`, plus 64 × 3
for this fixture's own per-event clone cost — see the test's doc comment for why that's 3, not the
reference nginx shape's 2) + 0 for the two `has_attributes` passes (each free per batch now,
`process_batch_through_has_attributes`'s own number; they cost 1 apiece until `Transform::process`
went in place). The two numbers aren't directly comparable
per-event (`route`'s inputs are borrowed, never cloned, and its output partition is exactly sized
to the split; the fan-out's cost is paid whether or not that branch's filter keeps anything) — the
comparison that matters is scaling: routing costs `1 + destinations used`, flat in event count and
branch count; the filter-chain shape costs `branches × events` (the fan-out clone) plus `branches ×
1` (the filter passes), rising with both.

## 4. Interning: the bargain, and its bounds

`logit_core::interner` maps every attribute key and metric name through a process-global
`lasso::ThreadedRodeo` to a 4-byte `Symbol`. This pays for itself several times over: `AttrMap`
compares and sorts `u32`s, `SeriesKey` hashes them, and the native wire format's dictionary
encoding ([wire-protocol.md](wire-protocol.md)) is backed by the same table.

**`ThreadedRodeo` never evicts and never frees.** Every *distinct* string ever interned stays for
the life of the process, at a measured **~94-124 bytes each** for a 40-character name: roughly 2.4×
the string's length, the rest map and index overhead.

Two facts bound how much that matters:

- **Re-interning a string the table already holds allocates nothing.** 1000 repeat interns cost
  zero allocations (`re_interning_an_existing_string_is_free`). A pipeline whose keys and metric
  names come from a fixed schema reaches steady state and stays flat.
- **Only keys and metric names are interned, not values.** `AttrMap::insert` interns the key and
  stores the value as a `Value::Str(Bytes)`. The high-cardinality dimension in telemetry is almost
  always the value side (host, request id, user agent, URL path, trace id), and none of it touches
  the interner. The classic cardinality explosion is not an interner problem here.

So the exposure is narrow: **a string used in key or metric-name position that never repeats.** In
practice that means:

- **statsd metric names** (`intern(name)` in `statsd.rs`'s `build_event`), which are
  client-controlled and where putting an id in the name is a well-worn anti-pattern:
  `user.<id>.logins`, `deploys.<sha>`, `orders.<order_id>.latency`. One such client is enough.
- Secondarily, **JSON object keys** from log bodies (`json.rs`) when a producer puts data in key
  position (`{"req_a1b2c3": {...}}`), and **DogStatsD tag keys** for the same reason. Both are
  schema-shaped in normal use and unbounded only when abused.

At ~94 bytes each, a million distinct metric names is ~94 MB retained with no way to reclaim it.
The `internal` component's `logit.process.interner.strings` gauge (`interner::len()`) is the only
thing that shows it happening.

### Accepted, with the premise written down

**No work is planned here, deliberately.** The reasoning, so it can be re-checked instead of
re-argued:

- **Listeners are private.** `logit`'s deployment shapes — sidecar, host agent, central aggregator
  fed by other `logit` nodes ([OVERVIEW.md](../OVERVIEW.md)) — all put the listener inside a trust
  boundary. The metric namespace is therefore *user*-controlled, not attacker-controlled. That is
  the load-bearing assumption; everything below follows from it.
- **A user can still name metrics badly**, but embedding an id in a metric name is a well-known
  anti-pattern with well-known consequences, and designing to accommodate it is not this project's
  job.
- **`logit` is not what breaks first, or even second.** The metric store goes long before: a
  million distinct measurement names is a million-plus series, which is squarely where InfluxDB's
  index falls over, against 94 MB here. And `logit`'s *own* first failure under the same abuse
  isn't the interner either — `aggregate`'s window holds a `SeriesKey` (408 bytes, almost entirely
  its `AttrMap`) plus an `Accumulator` (184) per series, so roughly **600 bytes per series per
  window** against the interner's ~94 bytes once. That one already has a documented mitigation
  (`keep` in front of `aggregate`, this section's closing note), and it would bite ~6× harder and
  sooner.

**What would change the calculus:** a listener that stops being private: a public or multi-tenant
ingest endpoint, or a hosted aggregator taking traffic from parties the operator doesn't control.
If that ships, revisit this section first, because the retrofit is expensive: `Symbol` is `Copy`
and `resolve` *panics* on an unknown symbol, so `AttrMap`, `MetricRecord`, `SeriesKey`, the Lua
proxy, and the native wire dictionary are all written against "symbols are eternal."

### What was *not* the risk: failed lookups — fixed anyway

`AttrMap::get` used to intern instead of probing, so in principle a miss added a key no event
carries. It was never a growth path in practice. There were three production `get` call sites:

- `kv_metrics.rs` (twice), keyed by `m.field` -- a **config** string, fixed at startup. Hit or
  miss, it's interned once and never again. (Since moved off `get` entirely: the field is interned
  at construction and read through `AttrMap::get_sym`, no per-event probe of the interner at all.)
- `proxy.rs`'s `AttrsProxy::__index`, keyed by whatever a Lua script indexes -- normally a literal
  in the script, so also a bounded set. Unbounded only for a script that builds keys out of event
  data, which is unusual and is trusted config besides.

So `AttrMap::get`'s interning was a **CPU** problem, not a memory one: a hash plus a
concurrent-map probe on the hot path. Fixed anyway (§8 item 2): `AttrMap::get`/`remove` use
`interner::lookup`, a non-interning probe, and fall through to `binary_search_by_key` only on a hit.
No behavior change; `insert` still calls `intern`, since it may need to mint a symbol.

### The per-parser key cache: a second copy, deliberately bounded

A parser whose keys come from its *input* has the same CPU problem: `json` can't intern its object
keys once at construction the way `set`/`csv`/`kv_metrics` do, so every key of every line cost a
hash plus a shard lock on the global table. That was the largest remaining cost in the `json-parse`
load-test scenario ([performance.md](performance.md)). Each `JsonParser` owns a
`logit_core::interner::KeyCache`: a `&str -> Symbol` memo in first-seen order with a cursor, so on a
schema-shaped stream every key after the first line is one `memcmp` and never touches the interner.
It is a pure fast path: `get_or_intern(s)` always equals `intern(s)`, and the cache holds no symbol
the table doesn't.

Unlike the interner, it is **capped** (64 entries, keys ≤ 128 bytes). The reasoning above accepts
the interner's unbounded growth once, process-wide; an uncapped per-node copy would double that
exposure per parser under the same abuse (`{"req_a1b2c3": …}`). Past the cap a new key is interned
but not cached, after a bounded scan. The interner still retains every distinct key, and
`interner::len()` still reports it.

### The other unbounded structure

`Aggregator`'s per-resource `HashMap<SeriesKey, Accumulator>` grows with tag cardinality within a
window. This is deliberate, with an operator-facing mitigation the reference config uses: put
`keep` in front of `aggregate`, so config, not input, bounds the tag set `SeriesKey` keys on.
`logit_transforms::keep`'s module docs say so. It also saves allocations: with `keep`, absorbing an
event allocates nothing; without it, the 10-attribute map spills and costs one allocation *per
metric per event*.

## 5. Bounds on in-flight memory

`CHANNEL_CAPACITY` is 64 (`runtime.rs`), and it counts **batches, not bytes or events**. An ordinary
transform-to-transform edge has no byte bound, so total in-flight memory scales with the number of
graph edges times whatever a batch weighs.

Batch size is bounded where batches are assembled. A listener that builds batches from a stream of
datagrams, lines, or file records does it through a `BatchAccumulator` bounded by config-visible
`batch_max_events`/`batch_max_bytes`: a UDP listener's `receive.batch_max_events`/`batch_max_bytes`,
each connection on the shared TCP stream driver (`crates/logit-inputs/src/tcp.rs`), and each file
`tail_in`/`docker_in` tracks (`docs/adr/file-tailing-and-docker-json-logs.md`). So one 65 KB syslog
datagram that decodes to hundreds of events, or a TCP read of any size, still leaves the listener as
bounded batches. A listener that receives a whole batch per request or frame (`otlp_in`,
`logit_in`) passes on what the sender sent, sized by the sender.

**Sinks bound their queues by bytes.** The byte-aware bound is `EventBatch::estimated_heap_bytes()`
(`crates/logit-core/src/event.rs`), a deliberately approximate O(events) walk:

- **The dominant term is the `Vec<Event>` backing storage**,
  `events.capacity() * size_of::<Event>()`, 864 bytes per event (§1) before any nested heap
  payload. Without it, a batch of numeric-only metrics would estimate close to zero while holding hundreds of bytes per event.
- **On top of that:** attribute values, log bodies, span-owned data (name, every
  `SpanEvent`/`SpanLink`'s backing storage and attributes, a boxed `SpanExt`), and metric records
  (exemplars, a spilled `Samples`'s heap capacity, a `Set`'s `HyperLogLog::heap_bytes()`,
  `SetMembers`'s member byte lengths, and `Histogram`/`ExponentialHistogram`'s bucket `Vec`s). The
  `Resource`'s and any `Scope`'s attributes count once per batch, since both are `Arc`-shared.
- **Interned `Symbol`s count for nothing** (attribute keys, metric names/units/descriptions, a log's
  `event_name`). On the event they are 4-byte handles; the bytes they name live in the process-wide
  interner (§4) and dropping a batch frees none of them. Resolving and counting them by length, as
  an earlier version did, cost an interner probe per key per event on every queue push (~30% of the
  `json-parse` load-test scenario's samples, more than either transform) and billed shared bytes
  once per event per hop.

It is an admission-control estimate, not allocator accounting, so unlike §1's numbers it is
deliberately *not* asserted exactly: a `MetricKind::Distribution`'s `DDSketch` is a fixed constant
instead of a bin-by-bin walk, and `Value`'s inline numeric/bool/null variants contribute nothing.
Every sink's in-memory queue (`crates/logit-pipeline/src/queue.rs`,
`docs/adr/buffered-sink-delivery.md`, `docs/plans/buffered-sink-delivery.md`) bounds itself on
batch count and this estimate, whichever trips first.

A sink opted into `buffer.disk:` (`docs/adr/disk-backed-sink-buffer.md`) bounds on-disk bytes
instead: `crates/logit-pipeline/src/disk_queue.rs`'s per-record encoded frame length, summed over
every segment still on disk. The two figures differ on purpose: disk usage tracks exactly what was
written, while the in-memory figure is an estimate. A sink's queue is in memory *or* on disk, never
both, and only one bound applies: `buffer.max_batches`/`max_bytes` are rejected alongside a
non-default `buffer.disk` (`crates/logit-pipeline/src/graph.rs` rule 35).

**Listeners bound undecoded bytes too.** [ADR `decoupled-listener-io`](../adr/decoupled-listener-io.md)
generalizes `SinkQueue` into `BoundedQueue<T: Queued>` and weighs a UDP listener's receive queue
(`logit-inputs::udp::ReceiveQueue`) by `Datagram::weight()`: `bytes.len()` plus the struct's inline
footprint. `receive.max_bytes` (default 32 MiB) bounds it. `receive.batch_max_bytes` (default 1 MiB)
is the independent bound one layer downstream, on `BatchAccumulator`'s not-yet-sent events.

### The batched read's slab: a fixed per-listener cost, mostly virtual

The `recvmmsg(2)` read half ([ADR `udp-intake-batching-and-socket-visibility`](../adr/udp-intake-batching-and-socket-visibility.md))
needs one receive buffer per message the syscall may return, so a UDP listener owns a slab of
`receive.read_batch` × 65,507-byte slots (`BatchReader::slots`, `crates/logit-inputs/src/udp.rs`):
one allocation, made when the read loop starts (after bind), never resized, replacing the single
65,507-byte buffer the `recv_from` loop held. 65,507 is IPv4's maximum payload; the ADR says why the
slots weren't grown to IPv6's 65,527. It is a fixed per-listener cost, independent of load, and the
only figure in this document where *virtual* versus *resident* is the whole point:

| `read_batch` | slab, virtual | resident at allocation | resident once every slot has held a small datagram | resident if every slot holds a maximum-size datagram |
|---|---|---|---|---|
| 1 (the pre-ADR shape) | 64 KiB | ~0 | 4 KiB | 64 KiB |
| **64** (the default) | **4.0 MiB** | **+52 KiB** | **+308 KiB** | **+4.1 MiB** |
| 1024 (the rule 57 ceiling) | 64.0 MiB | +96 KiB | ~4.1 MiB | ~64 MiB |

**Untouched pages are never faulted in.** Under the jemalloc global allocator (§6), `vec![0u8; n]`
is `alloc_zeroed`, which at this size is a fresh `mmap` of zero pages. Allocating the slab costs
address space and a few tens of KiB of allocator bookkeeping, not its nominal size. Resident cost
then tracks what traffic writes: one 4 KiB page per slot for any datagram up to 4 KiB, which covers
every statsd or syslog datagram in practice. So the default's realistic steady state is the
~256-308 KiB column, not the 4 MiB one.

The table comes from a direct probe under the release profile, and the whole process agrees: across
the `read_batch` sweep
([ADR `udp-intake-batching-and-socket-visibility`](../adr/udp-intake-batching-and-socket-visibility.md)),
the `udp-statsd-small` scenario's peak RSS is **21.7 MiB at every one of `read_batch` 16, 32, 64, 128
and 256**. The slab's nominal size grows sixteenfold, from 1 MiB to 16 MiB, and resident memory
doesn't move. A resident slab couldn't do that.

**What did move is not the slab.** Peak RSS on the two large-datagram scenarios (`udp-statsd`,
`udp-statsd-packed`) rose by several MiB once the read was batched at all: `udp-statsd-packed` went
from 44.4-47.5 MiB across five repeats to 50.4-62.3 MiB. The rise appears at `read_batch: 16` and
doesn't grow from there to 256, which rules the slab out. It is more datagram bytes in flight per
turn of the read loop, at those scenarios' ~1.4 KB datagrams. `receive.max_bytes` (32 MiB by
default) governs it, and was never reached.

Two consequences. Many UDP listeners multiply the *virtual* figure, which is free on a 64-bit
host, and the resident one, which is not: a host agent with four listeners at the default pays
roughly a megabyte of real memory for its slabs, once. And raising `read_batch` to its ceiling on a
small-datagram workload is a ~4 MiB decision, not a 64 MiB one. `docs/deploying.md`'s "Listener
intake" section points operators at the `logit.input.datagrams / logit.input.reads` ratio that says
whether raising it would help at all.

**None of this holds under `THP=always`.** The figures above were measured in the dev container,
where transparent huge pages are `madvise`/`never`. On an Azure VM with THP set to `always`, peak
RSS *rose* with `read_batch` at 128/256 (`docs/design/performance.md` §7): touching one 4 KiB page
per slot faults in its whole 2 MiB huge page, making the entire slab resident. A same-box,
same-binary repeat with `transparent_hugepage` forced to `madvise` (`docs/design/performance.md` §7,
2026-09-20) confirmed THP as the cause: peak RSS stayed flat in a 14.4–16.8 MiB band across the
entire `read_batch` range. So under `THP=always`, the shipped default `read_batch: 64` can cost up
to ~4 MiB of real resident memory per UDP listener.

## 6. The allocator

`logit` runs a multi-threaded tokio runtime plus one OS thread per Lua component, allocating and
freeing small objects continuously for weeks. glibc's `malloc` — what `debian:bookworm-slim` ships
— handles that shape poorly on two counts: per-thread arenas fragment when allocation and
deallocation happen on different threads (which is what a pipeline of channel-connected nodes does
by construction), and it returns memory to the OS reluctantly, so RSS drifts upward over days
while the working set stays flat.

So `logit` uses **jemalloc** by default
([ADR `jemalloc-global-allocator`](../adr/jemalloc-global-allocator.md)), behind a default-on
`jemalloc` feature on `logit-cli`. `--no-default-features` still builds against the system
allocator, which keeps the comparison available.

**Allocation counts are allocator-independent** (they count calls), so every count in this document
holds under either allocator. Timings don't.

### Profiling recipes

Neither of these runs in CI; both are for a specific investigation.

**jemalloc's own heap profiler** — available on the shipped binary, no rebuild:

```
MALLOC_CONF=prof:true,prof_prefix:/tmp/jeprof,lg_prof_interval:30 logit run config.yaml
jeprof --show_bytes --pdf $(which logit) /tmp/jeprof.*.heap > heap.pdf
```

**heaptrack**, for a full allocation trace with call stacks — heavier, better for "what is
allocating 180 times per event":

```
script/console
heaptrack cargo run --release -p logit-cli -- run examples/nginx-to-influxdb.yaml
heaptrack_print heaptrack.*.zst | head -50
```

## 7. Instrumentation

Three layers, separated by how noisy they are, plus one targeted bench:

**`crates/logit-core/tests/type_sizes.rs`**: exact `size_of` assertions on the event model. The
highest value per line here: no dependencies, deterministic, runs in CI, and catches the day someone
adds a field that costs every in-flight event another 200 bytes. Exact equality, not an upper bound,
on purpose: a `<=` would absorb exactly what it exists to catch.

**`crates/logit-bench/tests/allocations.rs`**: exact allocation counts per stage, via
`CountingAlloc`, a `GlobalAlloc` wrapper installed only in that test binary. They are ordinary
`#[test]`s, so `script/test` runs them in CI and an allocation regression fails the build. Two
things make them deterministic: counters are **thread-local**, so nothing another thread does leaks
in, and `cargo nextest` runs each test in its own process. Every measurement warms its subject
first, because a cold call folds in one-time initialization and reports a number that never
reproduces.

**`crates/logit-bench/benches/pipeline.rs`**: divan throughput benches, run by hand with
`script/bench`. Deliberately **not** in `script/cibuild`, because wall-clock benchmarking on a
shared CI runner measures the runner. divan's `AllocProfiler` reports allocation counts alongside
timings, so the two layers cross-check each other.

**`crates/logit-bench/benches/size_vs_alloc.rs`** answers what the three layers above can't: *what
does an allocation cost, against what the bytes it saves cost?*
[ADR `minimize-allocations-over-event-size`](../adr/minimize-allocations-over-event-size.md)
assumes that ratio without measuring it; [`event-sizing.md`](../plans/event-sizing.md)'s W2 is the
measurement. It isolates a jemalloc alloc/free pair at the sizes `AttrMap`'s growth ladder asks for,
same-thread and **cross-thread** (a spilled buffer is built on a listener or transform task and
freed on a sink's); the 768→1536→3072 realloc ladder against one exact allocation; the O(k²) sorted
build against append-then-sort; an `Event`-sized move and a 1000-event batch scan at every candidate
`size_of::<Event>()`; and `AttrMap::clone` inline against spilled.

It is the **only** bench target that installs real jemalloc as its `#[global_allocator]` instead
of a counting wrapper. That is the point: `AllocProfiler` wraps the *system* allocator and counts
every request inside the timed region, which suits a bench whose output is a count and not one
whose output is what a jemalloc call costs. So it reports no allocation column, which is fine since
counts are allocator-independent and pinned in `allocations.rs`. Its numbers aren't recorded here:
this document and [`performance.md`](performance.md) take recorded figures from the perf VM, and
`event-sizing.md`'s W2 section names the commands that reproduce them there.

**Before adding a bench, know that divan's `AllocProfiler` only counts allocations on threads it
controls.** Almost every bench calls decoders, transforms, and encoders **directly**, never touching
the tokio runtime or the channels between nodes. The exception is `pipeline.rs`'s `runtime` module,
which drives `Fanout::send`/`recv` across a real channel. That is safe because it never calls
`tokio::spawn`: a `current_thread` runtime's `block_on` runs everything on the calling thread, the
one divan watches. The constraint is **no cross-thread hop** (a `tokio::spawn`, a multi-thread
runtime, a real OS thread), not "no channel." `crates/logit-bench/tests/allocations.rs`'s
`fanout_send_*`/`unwrap_batch_*` tests (thread-local `CountingAlloc`, same reasoning) independently
confirm the numbers this module reports.

What a full multi-node graph costs end to end, across the worker threads and OS threads
`run_with_shutdown` spawns, needs a load generator, not a microbenchmark. The out-of-CI load-test
harness (`crates/logit-perf`, `script/perf`) measures it; see [`performance.md`](performance.md)
for its methodology and recorded runs against the real release binary. Capacity planning for a
given deployment's traffic is a separate, open question ("Open questions", below).

**Spans are only partly covered by these layers.** Most span-adjacent constants (`fanout_send_*`)
use a disabled `Telemetry` and never reach the sample decision;
`fanout_send_one_consumer_with_a_live_unsampled_registry_costs_nothing` covers the live, unsampled
path through `Fanout::send`'s span site; and what a *sampled* span costs is measured in
`crates/logit-core/src/telemetry.rs`'s own tests. "Costing internal spans" (§2) has the full
account.

### Fixtures: synthetic inputs, no external services

**Nothing in the test or bench suite may depend on a running nginx, InfluxDB, or any other
service.** `fixtures.rs` holds `const` wire-format literals, components are called directly, and
even `influxdb_out`'s retry tests bind an in-process `TcpListener` on `127.0.0.1:0` instead of
talking to a real server. Keep it that way: fixtures that stand up services quickly get slow, flaky,
and large, and stop being runnable in CI.

The pattern for a new shape, in order of preference:

1. **A `const` byte literal** for anything with a wire format: one representative record plus a
   `count` multiplier for volume (as `nginx_syslog_datagram(n)` does), not a recorded corpus.
2. **Directly-constructed `Event`s** where there's no wire format to record from, or the shape is a
   model rather than a capture. `span_event` and the survey-derived shapes below are examples;
   `SpanRecord` fixtures predate `otlp_in`, and a captured OTLP payload could now replace them.

Synthetic doesn't mean guessed. A literal should carry **provenance**: which software and config
produced this shape, and when it was last checked against the real thing. `NGINX_SYSLOG_LINE` was
derived from the `access_json_syslog` format `examples/nginx/nginx.conf` used before it became
`access_semconv` (the fixture keeps the older shape so the pins stay comparable) and confirmed
against a live nginx run (the emitted `syslog.facility=23`/`severity=6` match its `<190>` priority exactly).
Exploring real software is the right way to *inform* a fixture; the fixture is what gets committed.

**Warm a directly-constructed `Event`'s message `Bytes`, or the count measures the fixture, not the
code.** `bytes::Bytes` defers its atomically-refcounted, shared representation until a buffer is
*first* cloned or sliced. Built fresh (`Bytes::from(String)`/`Bytes::copy_from_slice`, both through
`From<Vec<u8>>`), it is a plain, unshared pointer+len+capacity triple, and the first
`.clone()`/`.slice()` pays a real, `#[cold]` allocation (one `Box<Shared>`, 24 bytes on a 64-bit
target) to promote it. Decoder-sourced fixtures (`nginx_syslog_datagram`, ...) get this for free:
the test holds one base `Bytes` and clones it for both the warm-up and the measured call
(`decoder.decode(datagram.clone())`, twice, against the same `datagram` binding), so the promotion
lands on the warm-up. `crates/logit-bench/src/fixtures.rs`'s `logfmt_event`/`kv_event`/
`logfmt_escaped_event` build an `Event`, not a raw `Bytes`, and have nowhere to hold a shared base
across two calls. Each memoizes its message in a function-local `static OnceLock<Bytes>`, so every
call after the first returns a `.clone()` of the same, already-promoted buffer, without changing the
zero-arg `-> Event` signature. Skipping this makes a transform's first touch of a message look like
it costs an allocation it doesn't, in every measurement.

**The six survey-derived shapes** (`crates/logit-bench/src/fixtures.rs`, added 2026-09-21 by
[`docs/plans/event-sizing.md`](../plans/event-sizing.md)'s W1) are what
[`data-shapes.md`](data-shapes.md) §7's follow-up 2 asked for. Its §6 found the existing fixtures
held up better than their caveats suggested, but had nothing at the commonest measured log width,
no nested record, and no span at the measured ceiling:

| Fixture | Width | The row it sits on |
|---|---|---|
| `flat_json_log_event` | 12 attributes, flat | §5.3's p50 for Go `log/slog` and structlog in their documented production configuration |
| `pino_http_log_event` | 10 attributes, 4 nested maps, depth 2 | §5.3's pino-http row (9 / max 10, 4 / 3 / 2) |
| `access_log_event` | 30 attributes | §2's PostgreSQL `jsonlog` (29 keys), the desk-counted 15–34 access/audit class |
| `wide_server_span_event` | 17 attributes, no events, no links | §4's HTTP-server convention (16) at the demo's measured p90 (17) |
| `collectd_three_record_event` | 6 attributes, 3 metric records | §3's collectd width (p50 = max = 6) and its 3-record tail |
| `enriched_resource_batch` | 5 events × 9 attributes, 17-attribute `Resource` | §3's median collector batch carrying §4's median resource |

All six are **modeled, not captured**, and each doc comment says which part is the model's own.
The survey reports counts and pooled percentiles, not key names, so the names come from the
conventions or the format while the counts are measured. Two choices matter when reading a number
off one: `collectd_three_record_event`'s sixth attribute is the fixture's, not collectd's (the
survey measures record count and attribute width over the same corpus but doesn't say they co-occur
on one list), and `pino_http_log_event`'s per-map widths are a choice consistent with the measured
median of 3, not a recorded shape.

**Three of the six are built through the leg that really produces them**: a `tail_in`-shaped log
event (one `log.file.path` attribute and a JSON body) handed to the real `json` transform, not
constructed attribute by attribute. So the code under measurement produces the width these
fixtures pin, at the total count §5.3 measured on that leg. It also gives
`perf/scenarios/json-parse-app-log`, `-nested-log` and `-access-log` something to render: each
scenario's `generate_in` template is the matching fixture body verbatim plus the same path
attribute, so a scenario and an allocation pin measure the same event.

**Their directly-constructed attribute values are `Bytes::from_static`, never `Value::str`**
(`fixtures::sstr`). That is the promotion rule above, applied to attribute values: a `Value::str`
fixture pays one `Box<Shared>` promotion per string in whatever region first clones it, which for
these shapes is the `Event::clone` measurement itself. `Bytes::from_static` is what a value that
arrived off the wire already is.

**`unescape`'s allocation count depends on the same `Vec`/`Bytes` conversion, in the other
direction.** `Bytes::from(Vec<u8>)` defers promotion only when `vec.len() == vec.capacity()`;
otherwise it allocates the `Shared` control block *inside the conversion*, a second allocation
immediately instead of one deferred to the first clone. Every escape `unescape` resolves consumes
two source bytes and emits one, so a worst-case `Vec::with_capacity(bytes.len())` always has spare
capacity when there was anything to unescape. `unescape` calls `out.shrink_to_fit()` before the
final `Bytes::from(out)` to turn that second `alloc` into a `realloc` of the buffer it already paid
for; see §2's `logfmt` escaped-value row, pinned by `logfmt_parse_escaped_value_event`.

## 8. Recommendations

Ordered by **how much the evidence supports them**, not by the raw nginx numbers; §0 says why those
differ. An item that helps every workload with no tradeoff outranks a bigger saving that might
regress a workload the fixtures don't cover. The numbering is stable because code comments and
other docs cite items by number.

### Done

1. ~~**Fix the InfluxDB encoder's allocation churn.**~~ **Done**: 18,024 allocations per 100-event
   batch to 30 (§2). It was the single largest cost in the pipeline and is now smaller than ingest.
   Workload-independent: it helps any config with an `influxdb_out`.
2. ~~**Make `AttrMap::get` non-interning.**~~ **Done**: `AttrMap::get`/`remove` probe via
   `interner::lookup` instead of `intern`, closing both the CPU cost and the theoretical growth
   path (§4). It also added `interner::len()`, needed to test the fix.
3. ~~**Give `statsd_in` the `slice_of` treatment.**~~ **Done**: 8 → 2 allocations per line (§2's
   zero-copy section). `statsd_in` keeps the same zero-copy promise `syslog_in` does.
4. ~~**Trim `json`'s allocations.**~~ **Done, further than scoped**: 7 → 1 for the nginx shape,
   and 1 for a 28-field wide-JSON line too (§2). The plan was a checkpoint-and-rollback scheme over
   the intermediate `AttrMap`. Measuring first showed the real cost was `collect_attrmap`'s per-key
   owned `String` (`next_key::<String>()`), not the intermediate map, so the fix interns keys
   straight off the deserializer. The guess from the nginx number alone had the mechanism wrong;
   measuring against a wider shape caught it.
5. ~~**Give `stdio_out` and `syslog_out` the treatment `influxdb_out` got.**~~ **Done**. `stdio_out`:
   1801 → 101 allocations per 100 events, ~18× (§2), by merge-joining the resource/event attribute
   maps the way `influxdb_out` does and formatting straight into reused buffers instead of
   `format!` per value. `syslog_out`: 401 → 100, ~4×, by holding `line`/`raw_msg`/`scratch` as
   reused `SyslogEncoder` fields instead of fresh `String`s per event (§2's encoder section has the
   mechanism).
6. ~~**Reduce the Lua boundary's allocations.**~~ **Done**: 21 → 9 per event round trip, by caching
   `process`/`flush` (via `mlua::RegistryKey`, resolved once at load instead of looked up from `_G`
   per call), caching the `AttrsProxy` userdata per event instead of rebuilding it per attribute
   access, and taking `mlua::String` instead of an owned `String` in both metamethods. Two edge
   cases from review were closed, not left as caveats: a script that stashes `event.attributes`
   across a return boundary fails loudly (in this crate's voice, not mlua's raw error) instead of
   silently working on a disconnected copy, and a `flush` global that isn't a function is a
   load-time error, matching `process`'s `MissingProcess`, instead of being treated as "no
   `flush()`" and silently losing every flush tick's events.

   **The newer Lua surfaces (`event.metrics`, `event.span`, `scope`) mostly follow the same
   shape:**

   - `SpanProxy` (read-only) and `MetricsProxy` (its `#`/`Len` form) cost the same first-access
     `+3` `AttrsProxy`/`LogProxy` do, cached the same `RegistryKey` way, once per event (§2's
     `event.span.name`/`#event.metrics` rows).
   - `scope` costs *less* to read (`+0` beyond the call baseline): unlike every `EventProxy`
     sub-proxy, it's a batch-lifetime global installed once in `ScriptWorker::new`, not per-event
     userdata, so a `process()` call has nothing to create or cache. Writing through it
     (`scope.attributes.k`, `resource.schema_url`) costs what the `resource` write rows already
     show: one allocation for the new value and one for the `take_*`-time `Arc::new(..)` commit.
     The copy-on-write clone itself measured free when the fixture's `Bytes` fields are `'static`
     and its `attributes` map starts empty and inline.
   - **The exception is `event.metrics[i]` indexing.** `MetricProxy`'s doc comment says a per-index
     handle isn't worth caching because it's "one small allocation," but it measures the *same* 3
     allocations a cached, registry-keyed proxy costs, paid on *every* index. It is reported, not
     fixed, per this file's rule to report an avoidable-looking cost in `logit-script` instead of
     fixing it here: a per-event cache trades a `RegistryKey` slot for scripts that never touch
     `event.metrics` against one for scripts that index it repeatedly, a real design trade-off.

   Separately, `MetricProxy`'s event field was once a *strong* `Rc<RefCell<Event>>`, so an
   uncollected `event.metrics[i]` temporary could still hold a second strong reference when
   `EventProxy::into_inner` ran, forcing its `Rc::try_unwrap` fast path into a real `Event::clone`.
   No fixture showed it, because `sum_metric_event`'s empty attributes and one inline metric make
   that clone free. The field is now a `Weak<RefCell<Event>>` (`crates/logit-script/src/
   proxy.rs`), guarded by a row on a *spilled* (9-attribute) `AttrMap` whose clone would allocate:
   it measures the same as the plain fixture (§2's
   `lua_process_one_event_reading_metric_value_on_a_spilled_event`), so a regression to a strong
   `Rc` would show as a rise in that row.
7. ~~**`Arc<EventBatch>` copy-on-write on channels.**~~ **Done, with real caveats** (§3), landed
   over three rounds (`docs/adr/arc-eventbatch-copy-on-write.md`), each correcting an overclaim in
   the one before. Single-consumer edges and all-`Output` fan-outs are strict wins (0 and 1
   allocations). A fan-out mixing one `Output` branch with one mutating branch is racy: 1 or 4,
   decided by scheduling, never the pre-`Arc` code's flat 3. A fan-out with no `Output` branch
   doesn't improve: 4, one worse than that code, deterministically. Read §3 before citing a number
   from this item; which one applies depends on fan-out shape.
8. ~~**Re-pick `AttrMap`'s inline capacity — down.**~~ **Decided: don't shrink** (§1). Capacity
   8 → 4 only ever costs an allocation across every shape measured, never saves one. Increasing it
   is item 12.
9. ~~**`Box` `SpanRecord`.**~~ **Decided: don't box** (§1,
   [ADR `minimize-allocations-over-event-size`](../adr/minimize-allocations-over-event-size.md)).
   Measured (construction 11 → 12 allocations, clone 2 → 3, for the span fixture), implemented, and
   reverted: the byte saving (128 bytes then, 136 now) trades against an allocation that a
   trace-focused deployment pays on most events. The policy judges it against that workload, which
   `otlp_in` now serves, not against the lack of a span input at the time.
10. ~~**`Box` the `DdSketch`.**~~ **Decided: don't box** (§1, ADR
    `minimize-allocations-over-event-size`). Measured on the single-distribution and
    distribution-heavy fixtures, implemented, and reverted: boxing saved 144 bytes but took the
    reference config's full ingest chain from 5 to 7 allocations. Distributions are common, not the
    rare case the byte saving alone would justify boxing.
11. ~~**Enable smallvec's `union` feature.**~~ **Done**: 16 bytes off every `Event`, no tradeoff,
    as predicted (792 → 776 bytes at the time; `Event` has since grown to 864 for unrelated reasons,
    and the saving still applies).

### Deferred — needs real production data, not more synthetic measurement

Both items are `SmallVec` inline-capacity choices: compile-time-fixed, trading bytes against
allocations depending on how wide events are in practice. Four synthetic fixtures were enough to
rule out shrinking `AttrMap` (item 8) but not to pick a number here, which needs a real
distribution of attribute and metric counts.

**That distribution now exists, short of production.** [`data-shapes.md`](data-shapes.md) is a desk
survey plus live captures of real third-party software measured by the `shape` component. Its §6
states what it implies: per-event width is bimodal by signal (metric events at 0–6 attributes,
parsed structured logs at 9 and up with 100% of them past 8, spans across both), and live collectd
puts 17.7% of its events at 2–3 metrics and none higher. It decides nothing itself, and its §7 is
explicit that none of it is production traffic.

**So does a baseline.** [`docs/plans/event-sizing.md`](../plans/event-sizing.md)'s W1 added the six
shapes data-shapes.md §6 asked for, pinned their numbers (§2's `json`/`Event::clone`/native rows),
and measured the growth ladder §1 used to infer. Two of those numbers shape how these items should
be argued: a spilled `AttrMap` clones in **one** allocation whatever its width, so allocation count
barely separates a 12-attribute log from a 30-attribute one; and the *nested* shape, at 10
attributes, is five times more expensive to clone than either. An argument denominated in
allocations alone ranks those three shapes in an order bytes moved does not.

12. ~~**`AttrMap`'s inline capacity, increased rather than shrunk.**~~ **Measured; no change**
    ([ADR `event-sizing-and-allocation-strategy`](../adr/event-sizing-and-allocation-strategy.md),
    `performance.md` §8). Measured on the perf VM in both directions: N=16 buys the wide-log legs
    3–10% and costs every narrow leg 6–18%; N=0 and N=4 cost the narrow legs 7–17%. Pre-sizing the
    spill instead (one exact allocation, no growth chain) was built and measured 8–17% *slower* end
    to end on `json`, because real keys arrive in interning order (so per-key `insert_sym` is
    already an append) and because reserving early lost to growing late for reasons not yet
    established. 8 stands, and §1's growth ladder is what ships. The one open lead isn't `Event`'s
    map: `aggregate` ran 18% faster at N=0, which is the `AttrMap` inside every `SeriesKey`.
13. **`MetricList`'s inline capacity (currently 1).** Any event with 2+ metrics spills: always for
    the nginx reference config (4 metrics), and for `kv_metrics` configurations generally. It
    interacts with item 10: with `DdSketch` inlined, `MetricRecord` is 224 bytes (184 before ADR
    `metrics-model-v2` added `description`/`start_timestamp`/`exemplars`), so each extra inline slot
    costs far more than it would with the sketch boxed.

### Later — needs a reason first

14. ~~**`Transform::process(&mut Event) -> bool`.**~~ **Done**; see §2's "Runtime" table and the
    `Transform::process`/`process_batch` doc comments
    ([ADR `in-place-transform-process`](../adr/in-place-transform-process.md)). It removes an
    864-byte memcpy per node hop *and* the per-batch `out` `Vec` that `process_batch` collected
    survivors into: `process_batch` is a `Vec::retain_mut` over the batch's own `events`, so every
    `process_batch` row dropped by exactly 1, to **0** for the ordinary forward/filter/absorb
    cases. It was applied while there were still only ~20 transforms to change, as this item asked.
15. **`AttrMap` accessors keyed by `Symbol`,** eliminating the remaining `resolve` → `intern` round
    trips. `influxdb_out`, `stdio_out` (both merge-join now), and `json` (which merges by
    `insert_sym`) no longer make them. What's left is `keep`/`remove`'s rebuild (`filtered` in
    `crates/logit-transforms/src/keep.rs`), which resolves each key to test it and re-inserts by
    `&str`.
16. **Byte-aware channel bounds** (§5). An ordinary transform-to-transform edge is bounded only by
    `CHANNEL_CAPACITY` batches. Every batch-assembling listener, UDP, TCP, and file alike, already
    bounds batch size, so this needs a workload that shows the gap first.
17. **~~Bound the interner~~ — accepted as-is, see §4.** Listeners are private, so the namespace is
    user-controlled; the metric store and `logit`'s own aggregation window both fail earlier and
    harder under the same abuse. Revisit only if a listener stops being private.

## Open questions

- **What is the real attribute and metric-count distribution in production?** Everything short of
  production is measured: [`data-shapes.md`](data-shapes.md) measures real third-party producers
  with the `shape` component and counts the rest from pinned sources, and §7's fixtures cover its
  commonest log width, a nested-map record, and a span at the 16–17-attribute ceiling. It also
  shows the wide-JSON fixture's 32 attributes is an access-log or audit-log width, not an
  application-log one (every logging library measured landed at 9–15). That settled `AttrMap`'s
  capacity (§8 items 8 and 12) but not `MetricList`'s (§8 item 13). What remains is the production
  distribution itself; `shape` exists so an operator can measure it from traffic that can't leave
  its environment.
- **What should capacity planning assume?** The load-test harness ([`performance.md`](performance.md),
  §7) measures what a full multi-node graph costs end to end on the real runtime. Sizing a
  deployment still depends on its own traffic shape, the question above.
- **Does jemalloc actually flatten RSS for this workload?** Partly answered. A short soak of the
  reference config against the real nginx stack (60,000 requests through
  `syslog_in → json → kv_metrics → {stdio_out, keep → aggregate → influxdb_out}`) held RSS at
  11.2 MB ± 3%, finishing slightly *below* where it started, with aggregated windows landing in
  InfluxDB throughout. That rules out a leak and shows pages being returned. It does **not**
  isolate jemalloc from glibc: the soak hasn't been run with `--no-default-features`, and the drift
  ADR `jemalloc-global-allocator` is about takes days, not minutes, to show. The feature flag keeps
  that comparison one build away. Relatedly, over a 5–10 s `script/perf` run, jemalloc's default
  10 s `dirty_decay_ms` means peak RSS carries roughly as much freed-but-unpurged memory as live
  data for any scenario whose sink keeps up; `performance.md` §1's "Peak RSS" sub-section has the
  paired default/immediate-purge table.
- **Is there a compact `Event` representation** worth having, one that doesn't reserve span and
  sketch space on a bare log line? Boxing the rare variants (§8 items 9-10) is the cheap answer, but
  it pays only where the variant really is rare, and that depends on the workload: a sketch is the
  common case in a statsd-timing pipeline and absent from a logs-only one. If no single boxing
  choice wins across shapes, this needs a representational answer, not a tuning one.
