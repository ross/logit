# Memory model and allocation behavior

`logit`'s job is to move a lot of small, short-lived objects through a graph of components. At any
interesting throughput, what limits it is not algorithmic complexity — every hot path here is
linear — but **allocation churn** and **how many bytes get copied per event**. This document
records what those actually are today, measured, and what to do about them.

Two companion documents: [data-model.md](data-model.md) defines the types this measures, and
[pipeline-graph.md](pipeline-graph.md) defines the node/channel structure the events move through.
This one is about what those cost.

Everything here is reproducible:

| What | Command |
|---|---|
| Type sizes | `script/test -p logit-core --test type_sizes` |
| Allocation counts | `script/test -p logit-bench --no-capture` |
| Throughput | `script/bench` |

The measurements live in code, not just in this file: `crates/logit-core/tests/type_sizes.rs` and
`crates/logit-bench/tests/allocations.rs` are **assertions**, so a regression fails CI rather than
quietly making this document wrong. If you change one of those numbers, change the matching table
here in the same commit.

> Numbers below were taken on x86-64 Linux, in the dev container, `bench` profile (`lto = true`,
> `codegen-units = 1`), system allocator, on an otherwise-busy laptop. Timings are divan's
> *fastest* column — the least noise-contaminated estimate available — and are useful for comparing
> stages against each other, not as absolute throughput ceilings. Allocation counts are exact and
> machine-independent.

## 0. What these measurements can and can't tell you

**Read this before acting on anything below.** Every allocation number in this document comes from
one event shape: the `examples/nginx-to-influxdb.yaml` reference pipeline, whose events carry a log
body *and* several derived metrics, with ~10 attributes. That was the right place to start — it's a
real config, exercising five components end to end — but it is **one point in a space `logit` is
explicitly meant to cover**:

| Workload | Carries | Wasted per event today |
|---|---|---:|
| Logs only (syslog, file tail → forward) | attributes + `log` | 336 B (`MetricList` + `SpanRecord`) |
| Metrics only (statsd, collectd, scrape → aggregate) | attributes + 1 metric | 184 B, plus ~176 of `MetricList` a `Counter` can't use |
| Traces only (OTLP → forward) | attributes + `span` | 248 B (`LogRecord` + `MetricList`) |
| Mixed (the nginx shape — the first one measured) | all three | least of any shape |

Two consequences that matter for how much weight to put on §8:

- **Everything in §1 (sizing) applies to every workload**, because `Event`'s 792 bytes are paid on
  every hop whatever the event carries. Every shape above wastes 180-340 bytes on payloads it never
  holds. That argument doesn't depend on the fixture at all.
- **Several specific *fixes* are workload-dependent, and one flips sign** depending on the mix. A
  change that is free for a logs-only pipeline can cost a metrics-only one an allocation per event.
  §8 marks which is which; don't read the ordering as settled for a workload the fixtures don't
  cover.

**Update: the fixture matrix has been broadened.** `crates/logit-bench/src/fixtures.rs` now
has a logs-only syslog fixture, a wide-JSON log fixture (28 flat fields), a distribution-heavy
metrics fixture (5 distinct `Distribution` metrics on one event), and a directly-constructed span
fixture — closing the gap this section used to describe. That unblocks the *measurement* half of
items 7-9 in §8; it doesn't by itself settle the *sizing* decisions those items still need to make
(a follow-up pass, still pending). See §1 and §8 for what the new numbers already show.

## 1. The event model's footprint

```
Event                                       776 bytes
├── timestamp: i64                            8
├── attributes: AttrMap                      392   ← SmallVec<[(Symbol, Value); 8]>
├── log: Option<LogRecord>                    48
├── metrics: MetricList                      192   ← SmallVec<[MetricRecord; 1]>
└── span: Option<SpanRecord>                 136
```

with the constituent parts:

| Type | Size | Why |
|---|---:|---|
| `Symbol` (`lasso::Spur`) | 4 | `NonZeroU32`; `Option<Symbol>` is also 4 |
| `Value` | 40 | sized by `Bytes` (4 words) plus an aligned discriminant |
| `(Symbol, Value)` | 48 | 4 bytes of padding after `Symbol` |
| `AttrMap` | 392 | 8 × 48 inline, + 8 of smallvec overhead (`union` feature, below) |
| `MetricKind` | 176 | almost entirely the inlined `DDSketch` |
| `MetricRecord` | 184 | `MetricKind` + name + unit |
| `MetricList` | 192 | 1 × 184 inline + 8 |
| `LogRecord` | 48 | `Option<LogRecord>` is also 48 — `Severity`'s niche absorbs `None` |
| `SpanRecord` | 136 | `Option<SpanRecord>` is also 136 — `SpanKind`'s niche absorbs `None` |

**Three things about this are worth internalizing.**

**A `SmallVec` costs its inline capacity whether or not it has spilled.** The inline array and the
heap `(ptr, cap)` pair share one slot, sized by the larger. So an event with 13 attributes pays a
heap allocation *and* the full 392 bytes. Inline capacity 8 is therefore not "free up to 8" — it is
384 bytes on every event, forever, and the reference nginx pipeline spills past it anyway.

**A statsd counter costs the same 776 bytes as a fully-populated nginx access log.** `Event` has no
compact representation for the common case; the space for attributes, a sketch, and a span is
reserved unconditionally. That is the price of "an event is whatever it carries"
([ADR 0012](../adr/0012-multi-payload-events.md)) implemented with inline storage.

**`MetricKind::Distribution` sets the size of every metric.** A `Counter(f64)` needs 8 bytes and
pays 176, because `DDSketch` (two `Store`s and a `Config`) is inlined into the enum — deliberately,
per [ADR 0017](../adr/0017-minimize-allocations-over-event-size.md): boxing it would save 144 bytes
here but cost an allocation on every distribution metric actually constructed or cloned, and
distributions are a shipping, commonly-populated feature (`kv_metrics`, statsd's `ms`/`h`/`d`), not
a rare one — see below.

### What was reclaimed, and what was deliberately not

Sized here so the trade is visible, and the trades are not all in the same direction (see §0):

| Change | Saves | Real cost | Outcome |
|---|---:|---|---|
| smallvec's `union` feature | 16 B | none — it's a feature flag | **done** — applied, no tradeoff |
| `Box` `SpanRecord` | 128 B | +1 alloc per event that carries a span | **not done** — see below |
| `Box` the `DdSketch` in `MetricKind::Distribution` | ~168 B | +1 alloc per distribution metric created | **not done** — see below |
| Re-pick `AttrMap`'s inline capacity | up to 192 B | more spills, or (if increased) more bytes | **deferred** — see below |

Only the `union` feature landed; `Event` is 792 → 776 bytes from that alone. The other two boxing
changes were measured, implemented, and then **reverted** — worth explaining why, since the numbers
alone would suggest taking them.

**Both boxing changes trade `Event`'s size for allocation count, and this project now has a stated
priority for that exact conflict: minimize allocations, not size**
([ADR 0017](../adr/0017-minimize-allocations-over-event-size.md)). `logit`'s deployments are not
constrained by the in-flight footprint at stake here (hundreds of bytes per event); a heap
allocation is the more expensive resource by a wide margin at this scale — copying a few hundred
extra bytes is close to free, while an allocation does real, measurable work even on a fast path.
So a trade that adds allocations to save bytes goes the wrong way by default, unless the payload in
question is genuinely rare in its intended workload.

Neither is. **`Box`ing the `DdSketch` is not free, and an earlier draft of this document said it
was** — the reasoning was that a sketch "already allocates," but it doesn't, at construction;
`sketches_ddsketch`'s `Store::new` starts with `Vec::new()`, and the bins are allocated on the
first `add`. Measured: `kv_metrics` costs 3 allocations for 4 metrics (one `MetricList` spill plus
one bins `Vec` per distribution); boxing made that 5, and on the project's own reference config —
which carries 2 distributions per event — the headline ingest number this document tracks went
from 5 to 7 allocations per line. That's not a rare-workload edge case; it's the flagship config.
**`Box`ing `SpanRecord`** was reverted for the same reason applied consistently rather than
selectively: no OTLP (or other span-producing) input exists yet, but per ADR 0017 that's a `v0.1`
gap, not a property of the workload — a trace-focused deployment will populate `span` on most
events the same way the nginx config already populates `metrics` with distributions, once that
input exists. Treating spans as safe to box because nothing constructs one *yet* would just be
deferring the same mistake to whenever that input lands. That prediction is now partly realized
without any external input at all:
[ADR 0025](../adr/0025-internal-span-emission-and-deterministic-sampling.md) makes `internal`
itself a real, if low-volume by default, producer of `span`-carrying events — a drained span
event costs exactly what this table already prices (776 bytes inline, `SpanRecord`'s 136 of it),
no new type and no change to this row's reasoning, just the first real caller of the shape this
section was already sized for.

**`AttrMap`'s inline capacity is the largest single term (384 B), and is left exactly as it is —
deliberately deferred, not decided.** Four shapes are measured: statsd (0-4 attributes, inline
either way), the nginx mixed shape (10, spills at both 8 and 4), a plain logs-only syslog line (6,
inline at capacity 8, would spill at 4), and a wide-JSON log line (32, spills regardless of 8 or
4). That's enough to say **shrinking to 4 has no measured upside** — it only ever costs an
allocation (the logs-only case) and never saves one. It is *not* enough to decide the opposite
question — whether to *increase* capacity to reduce spills on wider shapes — because that decision
needs a real distribution of attribute counts across production traffic, which four synthetic
fixtures can gesture at but not substitute for. Recorded as an open knob rather than pushed to a
guess in either direction: see §8.

**`MetricList`'s inline capacity (currently 1 — `SmallVec<[MetricRecord; 1]>`) is the same open
question, never yet asked.** Any event with 2+ metrics spills — which includes the nginx reference
config's event (4 metrics) unconditionally, and `kv_metrics` configurations generally, by design.
Worth noting a real interaction with the `DdSketch` decision above: `MetricRecord` is 184 bytes
with the sketch inlined (per ADR 0017), so widening `MetricList`'s capacity is considerably more
expensive in bytes per additional slot than it would have been if the sketch had stayed boxed (40
bytes/slot). The two decisions aren't independent of each other. Also recorded as an open knob,
same reasoning as `AttrMap`'s: real per-event metric-count data is needed before picking a number,
not more synthetic-fixture measurement. See §8.

**Both inline capacities are compile-time constants** — `SmallVec<[T; N]>`'s `N` is a const array
length, monomorphized into the type, with no runtime equivalent. There is no way to tune this per
deployment without either recompiling for a specific workload's shape or moving to a design with
no compile-time-fixed inline capacity at all. Whatever gets picked has to serve every workload this
binary ships to.

Worth noting on the topic of alternatives to inlining at all: a `SmallVec` that has spilled still
occupies its full inline footprint, so for a consistently-wide workload a plain `Vec` (24 B plus
one allocation) is strictly better than a `SmallVec` that always spills — the wide-JSON shape (32
attributes, one allocation either way under either type) is exactly this case. Worth keeping in
mind for whoever eventually does have the real-world data to make this call: "wider inline
capacity" and "no inline capacity for this field" are both on the table, not just "which number."

## 2. Where the allocations are

Measured over the reference pipeline (`examples/nginx-to-influxdb.yaml`,
`syslog_in → json → kv_metrics → keep → aggregate → influxdb_out`) with one real nginx access-log
line — `crates/logit-bench/tests/allocations.rs`.

| Stage | allocs | Notes |
|---|---:|---|
| `syslog_in` decode 1 line | **1** | just the `Vec<Event>`; every field slices the datagram |
| `syslog_in` decode 100 lines | **1** | + 5 reallocs from `Vec` growth |
| `syslog_in` decode 1 logs-only line | **1** | plain-text message, no JSON -- same zero-copy shape |
| `statsd_in` decode 1 line | **2** | fixed -- see below, tag values now slice the datagram too |
| `json` parse + merge (nginx shape) | **1** | fixed -- see below, was 7 |
| `json` parse + merge (wide-JSON, 28 keys) | **1** | same fix, confirmed to generalize past a small field count |
| `kv_metrics` derive 4 metrics | **3** | `MetricList` spill + one `bins` Vec per sketch |
| `keep` filter to 3 attrs | **0** | 3 attributes fit inline |
| `aggregate` absorb (after `keep`) | **0** | `SeriesKey` clone stays inline |
| `aggregate` absorb (no `keep`) | **4** | one per metric — the map no longer fits inline |
| `aggregate` flush 4 series | **6** | +4 since flush-side trace linking landed (ADR 0020) — one `Vec<SpanLink>` per series, see below |
| `aggregate` flush 100 retained gauge series (spilled attrs) | **209** | `gauge_retention > 0` only — see below; the default (`0`) tumbling path above is unaffected |
| **full ingest chain, 1 line** | **5** | decode → aggregate; was 11 before `json`'s fix |
| `Event::clone` (nginx shape) | **4** | what each extra fan-out branch costs |
| `Event::clone` (statsd shape) | **0** | fits entirely inline |
| `Event::clone` (distribution-heavy, 5 metrics) | **6** | 1 `MetricList` spill + 1 `bins` Vec per sketch |
| `Event::clone` (span shape) | **2** | 1 per `Vec` (`events`, `links`) -- every `AttrMap` here stays inline |
| `stdio_out` encode 100 events | **101** | ~1/event -- fixed, see below, was 1801 |
| `influxdb_out` encode 100 events | **30** | ~0.3/event — see below |
| `syslog_out` encode_into 100 events | **100** | ~1/event -- reused struct-held scratch buffers, was 401, see below |

And the corresponding times:

| Stage | fastest | per event |
|---|---:|---:|
| `syslog_in` decode, 100 lines | 23.1 µs | 231 ns |
| `statsd_in` decode, 100 lines | 33.2 µs | 332 ns |
| `json` | 535 ns | 535 ns |
| `kv_metrics` | 256 ns | 256 ns |
| `keep` | 339 ns | 339 ns |
| `aggregate` absorb | 581 ns | 581 ns |
| **full ingest chain** | **2.08 µs** | ~481k lines/s/core |
| `Event::clone` (nginx / statsd / distribution) | 228 / 87 / 51 ns | |
| `stdio_out` encode, 100 events | 124 µs | 1.24 µs |
| `influxdb_out` encode, 100 events | 159 µs | 1.59 µs |
| `syslog_out` encode_into, 100 events | 41.5 µs | 415 ns |
| `lua` (proxy / `to_table`) | 1.07 / 2.02 µs | |

> Every row above comes from **one** `script/bench` run, deliberately: runs on a busier machine have
> come out uniformly ~20% slower across every unchanged benchmark, so mixing rows across runs would
> invent differences that aren't there. Compare rows within the table freely; treat absolute values
> as this machine on this day. This table was refreshed once, in one sitting, after landing the
> `json`/`statsd_in`/`stdio_out`/Lua-boundary fixes below -- every row reflects the current code.
> The `syslog_out` row is the one exception, added in a later, separate `script/bench` run rather
> than refreshing the whole table again for one new row -- its allocation count is exact and
> comparable regardless (deterministic, not machine-dependent), but don't read its wall-clock
> figure as directly comparable to the others' down to the nanosecond, per this same caveat.

### `aggregate` flush now costs one allocation per series, for real trace links

[ADR 0020](../adr/0020-trace-context-propagation-on-delivered.md)'s flush-side linking widened
`Transform::flush` to pair each emitted `Event` with the bounded, best-effort `Vec<SpanLink>` that
attributes it (`crates/logit-transforms/src/aggregate.rs`'s `ContributingContexts`). Re-measured,
not assumed, per this file's own rule: `aggregate_flush_100_series` moved from 2 to 6 allocations —
exactly the 4 series in that fixture, each now allocating its own one-element `Vec<SpanLink>` on
flush (the fixture never calls `observe_batch_context`, so every series ends up with exactly one
distinct contributing context — the default, all-zero one — but a non-empty `Vec` always allocates
regardless of element count, so one per series is the honest floor, not a worst case). A series fed
by more distinct sources within the 8-per-series cap doesn't cost more allocations for it — the
`Vec<SpanLink>` is still built once, from however many contexts `ContributingContexts` ended up
holding.

Nothing downstream reads this yet — `run_flush` (`crates/logit-pipeline/src/runtime.rs`) discards
the links on the way out, since nothing turns them into a real `SpanRecord` yet
(`docs/known-gaps.md`'s internal-spans entry, item 2). This cost is paid regardless, the moment
`aggregate` flushes any series at all, whether or not a config ever routes anything to look at the
result.

### Gauge retention's own cost, isolated and measured (ADR 0008's amendment)

`gauge_retention > 0` (`docs/adr/0008-aggregation-window-semantics.md`'s amendment) adds a real,
separate allocation cost on top of the flush numbers above, paid only by series that are actually
retained -- the default (`gauge_retention: 0`) path above is untouched, confirmed by
`aggregate_flush_100_series` re-measuring at exactly the same **6** it was before retention existed.
`aggregate_flush_retained_gauges` isolates the retained path itself: 100 distinct, deliberately
un-`keep`ed gauge series (12 attributes each, past `AttrMap`'s 8-slot inline capacity) retained
across a second flush, measured **209** allocations. Two costs stack here, both inherent to what
retention has to do, not incidental:

- **`key.attributes.clone()`, once per retained series.** A retained series' map key has to survive
  to become its own key again next window, so (unlike the tumbling path, which moves `key
  .attributes` into the emitted event and drops the key) the attributes have to be cloned instead.
  With a spilled (non-inline) map, that clone is a genuine heap allocation -- ~100 of the 209, one
  per series. `aggregate_flush_100_series`'s `keep`-trimmed fixture never pays this, on purpose: its
  gauge retention is off, so nothing takes this branch at all.
- **The per-group `series` `HashMap` rebuilds its backing table on every flush that retains
  anything.** `flush` takes each group's whole `series` map via `mem::take` and re-inserts survivors
  into the now-empty replacement -- deliberately, so the *far* more common tumbling/drop path can
  move `key.attributes` for free (see above) rather than paying a clone on every series, retained or
  not. The tradeoff is that a `gauge_retention > 0` pipeline pays a full table-growth cost -- several
  allocations, not just one -- every flush, for as long as it keeps retaining the same series. This
  is accepted as a real, measured cost of opting into retention (not a bug), and is exactly why this
  fixture exists as its own measurement rather than folding into the default-path number above.

### `aggregate` flush now costs one allocation per series, for real trace links

As first measured, encoding one event for InfluxDB cost **~180 allocations and 4.96 µs** — roughly
twice what ingesting it cost end to end, and sixteen times what cloning it for an extra fan-out
branch cost. That was not the expected answer. [known-gaps.md](../known-gaps.md) had been carrying
`Arc<EventBatch>` copy-on-write as *the* identified fix for pipeline cost, and
[pipeline-graph.md](pipeline-graph.md) called the fan-out clone "load-bearing." Both were true, and
both were second-order next to the encoder.

None of it was about the data model. It was all in how lines were built:

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

**Now 30 allocations per 100-event batch, from 18,024 — a 600× reduction.** The changes were
mechanical and stayed inside `influxdb.rs`: escape and format straight into reused buffers held on
the encoder, merge-join the resource and event attribute maps instead of cloning and re-inserting,
borrow the series key for the lookup and only allocate it on a miss, and reuse the path-compression
scratch buffer. `stdio_out` got the identical treatment shortly after (§2's table, item 5 in §8):
1801 → 101 allocations per 100 events, ~18×.

What's left in both is per-*batch*, not per-event: `influxdb_out` keeps one `Bytes` for the finished
body, one `String` key per distinct series on first sighting, and growth of the per-series
timestamp maps; `stdio_out`'s residual 101 is almost entirely one `format_rfc3339_utc` call per
event, outside this file's scope. Neither scales with event count any more, which is the property
to protect — `influx_encode_100_events`/`stdio_encode_100_events` will catch it if that stops being
true.

On allocation count the two are no longer close (30 vs. 101), but **wall-clock is closer, and not
always in the direction the allocation count would suggest**: in the run §2's table comes from,
`stdio_out` (1.24 µs/event) actually edged out `influxdb_out` (1.59 µs/event) despite allocating
~3.4× more — a reminder that allocation count and wall-clock time are related, not interchangeable,
and this doc tracks both for exactly that reason. Don't read either single run as a settled ranking
between the two encoders.

**`syslog_out` got a narrower version of the same treatment, caught by review rather than found
independently.** A first version measured at 401 allocations per 100 events (~4/event): the
encoder's header/message text, the pre-sanitize render, the sanitized-message copy, and each
sanitized header field were all fresh `String`s allocated per event, three of them as
function-locals recreated on *every* `encode_into` call — which is why warming the call once (per
this file's own discipline) didn't help: the very next call's locals started from empty capacity
again, the same mistake a plain local makes that a struct field doesn't. Hoisting those into
`SyslogEncoder`'s own `line`/`raw_msg`/`scratch` fields — mirroring `InfluxLineEncoder`'s existing
scratch buffers rather than inventing a new pattern — brought it to 100 (exactly 1/event), the same
`format_rfc3339_utc`-per-call residual `stdio_out` already carries and already documents above.

**With the encoder no longer dominant, the ingest chain is the cost again — and it dropped too**:
`json`'s own fix (item 4) took the full ingest chain from 11 allocations to 5. `kv_metrics` is now
the single most allocation-hungry ingest-side stage at 3, and `Event::clone`'s 4 per extra fan-out
branch is comparable to or larger than any individual ingest stage. The recommendations in §8 are
ordered accordingly.

### Runtime: the node loops, not just the components they call

Every row above measures a decoder, transform, or encoder called directly. Nothing above measured
what `crates/logit-pipeline/src/runtime.rs`'s node loops (`run_transform`/`run_output`) add on top
— a gap `internal` telemetry's own PR (`docs/design/internal-telemetry.md`) opened without closing,
since it instrumented both loops and added no coverage of what that instrumentation costs. Closed
here: `run_transform`'s per-batch body is exported as `process_batch` (a plain synchronous
function — no channel, no runtime), and `run_output`'s as `send_batch` (async, since `Output::send`
is, but still callable directly), specifically so both — and `unwrap_batch` — can be measured
directly in `crates/logit-bench/tests/allocations.rs`'s "Runtime" section, the same as everything
above.

**`send_batch` coverage, and the corrected "telemetry is free" claim, both landed in review of the
first draft** — recorded here as findings, not silently folded in, since both change what the first
draft actually established:

| Path | allocs | Notes |
|---|---:|---|
| `process_batch` through `keep`, telemetry disabled | **1** | `Vec::with_capacity(batch.events.len())` for `out` — this is the whole cost |
| `process_batch`, fully absorbed (`aggregate`) | **1** | the same `Vec`, built before any event is processed, thrown away unused when nothing survives |
| `process_batch` through `keep`, telemetry live, **steady state** | **1** | identical to disabled — `count`/`timer` update an existing `ComponentBuffer` entry in place, no allocation of their own |
| `process_batch`, **first call after an `internal` drain** | **3** | the `out` `Vec` (1) + a `HashMap` table rebuild (1) + a fresh `DdSketch` (1) — see below |
| `unwrap_batch` (`Delivered::Owned`) | **0** | no `Arc` was ever involved |
| `unwrap_batch` (`Delivered::Shared`, sole reference) | **0** | `Arc::try_unwrap` succeeds |
| `unwrap_batch` (`Delivered::Shared`, contended) | **5** | falls back to `EventBatch::clone` — 1 for the `Vec<Event>` + `Event::clone`'s 4 (nginx shape) |
| `send_batch` through a no-op `Output`, telemetry disabled | **1** | `#[async_trait]` boxing its future (below) — nothing to do with telemetry |
| `send_batch`, telemetry live, **steady state** | **1** | same 1 as disabled — telemetry adds nothing on top of the box |
| `send_batch`, **first call after an `internal` drain** | **3** | the box (1) + the same `HashMap`/`DdSketch` rebuild as `process_batch`'s (2) |
| `send_batch` through a **failing** `Output`, telemetry disabled | **4** | the box (1) + `anyhow!(..)` constructing the error (1) + `.with_context(..)` (2: the `format!` message, and wrapping into a new boxed `anyhow::Error` node) — see below |
| `send_batch`, failing, telemetry live, **first failure** (success keys already warm) | **5** | the failure baseline (4) + 1 — `logit.component.errors` is a brand-new 4th map key, which can grow the buffer even though the first 3 already fit |
| `send_batch`, **failing**, first call after an `internal` drain | **7** | not simply 4 + 3 — a map absorbing 4 fresh keys (not 3) in one call can need more than one growth step; measured, not derived |

**Two findings, not one, once the first draft's claim was checked properly:**

1. **Steady state, telemetry really is free.** `process_batch_with_live_telemetry` and
   `send_batch_through_a_noop_output_telemetry_live` both match their disabled counterparts exactly
   — `count`/`timer` update an already-resident `ComponentBuffer` entry in place. `Fanout::send`'s
   telemetry was already known to be free (`docs/design/internal-telemetry.md`'s own tests); this
   is the same result for the receive side.

2. **But "telemetry live" only ever measured steady state — the first call after every `internal`
   drain costs more, and recurs forever.** `ComponentBuffer::drain` (`crates/logit-core/src/telemetry.rs`)
   `mem::take`s the whole `points` map on every `internal` tick, so the next `count`/`timer` call for
   each key is a fresh insert into an empty map, not an update. Concretely: the map's backing table
   (first insert since the reset) plus a fresh `DdSketch` for the timing key (no prior sample to
   merge into) — 2 allocations, on top of whatever the disabled baseline already costs (`process_batch`'s
   `out` `Vec`, or `send_batch`'s `async_trait` box). This is not a one-time cost: it happens once per
   `internal` drain interval, for as long as `internal` runs, on every component it's attached to.

**A third, unrelated finding, found the same way: `Output` is `#[async_trait]`
(`crates/logit-pipeline/src/output.rs`), and every call to `output.send(..).await` heap-allocates its
future** — confirmed by measuring a direct call (no `dyn Output`, no vtable) alongside the `dyn
Output` call `send_batch` actually makes; both cost exactly 1 (16 bytes), so this is `async_trait`'s
boxing, not dynamic dispatch. A real, previously unmeasured, per-batch cost on every output sink in
production, unrelated to telemetry or to this section's `internal-spans` question — worth its own
follow-up, not fixed here. (`docs/known-gaps.md`'s entry for this originally suggested a
hand-written `Pin<Box<dyn Future<...>>>` method as a workaround — wrong, corrected in review: that
return type requires the identical allocation to construct, whether a macro or a person wrote the
method, since the box *is* how a `dyn Trait` object returns a future of unknown, implementer-varying
size. A real fix means giving up `dyn Output` for the call — enum dispatch over the closed set of
concrete `Output` kinds, or a per-node generic runtime — not a differently-spelled boxed future.)

**A fourth finding, from a second round of review: every `send_batch` test above used a
`NoopOutput` that always succeeds, so none of them exercised `logit.component.errors` or
`result.with_context(...)`'s error-only work — a distinct allocation shape, not just a bigger
version of the success number.** Closed with `FailingOutput` (always `Err`) and three more tests,
the three failure rows in the table above. The failure baseline (4) is fully decomposed, not just
measured as one opaque number: `anyhow::anyhow!(..)` alone costs 1, and `.with_context(..)` alone
(given an already-built error) costs 2 more — `1 (async_trait box) + 1 (anyhow!) + 2
(.with_context) = 4`, confirmed against the isolated pieces, not asserted from the total alone.

`crates/logit-bench/benches/pipeline.rs`'s `runtime` module gives the wall-clock view of the same
paths, including `Fanout::send`+`recv` across a real `tokio::sync::mpsc` channel and `send_batch`
through `#[async_trait]` — safe to trust on *both* columns (timing and allocations) despite the
channel hop and the trait-object call, because both drive a **current-thread** runtime with no
`tokio::spawn` anywhere in the loop, so nothing here leaves the one OS thread Divan's
`AllocProfiler` is watching. Measured this way:

| Path | fastest | allocs |
|---|---:|---:|
| `process_batch` through `keep` | 360 ns | 1 |
| `Fanout::send`+`recv`, 1 consumer | 190 ns | 0 |
| `Fanout::send`+`recv`, 2 consumers | 458 ns | 6 (1 `Arc::new` + the 5-allocation clone above) |
| `send_batch` through a no-op `Output` | 189 ns | 1 (the `async_trait` box) |
| `send_batch` through a **failing** `Output` | 238 ns | 4 (matches the disabled-telemetry failure row above exactly) |

### Costing internal spans: the `Delivered` trade, measured

`docs/known-gaps.md`'s internal-spans entry gates carrying trace context on `Delivered` on measured
evidence, per [ADR 0017](../adr/0017-minimize-allocations-over-event-size.md). The coverage above
is what made that measurement possible; this is the measurement itself. **Not a decision** -- ADR
0017 asks for evidence before the trade is decided, and this is that evidence, recorded so the
decision (its own ADR, when someone takes it) doesn't have to re-derive it.

**The prototype.** A 24-byte `TraceContext { trace_id: [u8; 16], span_id: [u8; 8] }` added to both
`Delivered` variants (`Owned(EventBatch, TraceContext)`, `Shared(Arc<EventBatch>, TraceContext)`),
minted fresh per `Fanout::send`/`send_blocking` call via a thread-local SplitMix64 (no allocation,
no new dependency -- and deliberately not `tracing::span::Id`, which a `Registry` recycles after a
span closes, making it unsafe as a source of identity here). No parent propagation, no `run_output`
plumbing beyond the match arms `Delivered`'s extra field requires -- this measures the type change's
cost, not a working span feature. Built, measured, and reverted in full; every file it touched is
back to this table's pre-existing state except the one line below.

**Size: `size_of::<Delivered>()` goes from 32 to 56 -- exactly `TraceContext`'s 24 bytes, no padding
overhead.** This is a per-*batch* cost, on the channel payload, not a per-event one: contrast with
`Event`'s 776 bytes, where ADR 0017 already settled that a much smaller per-event size cost is
worth avoiding an allocation. `Delivered` isn't `Event` -- this is a different type, on a different
part of the pipeline, at a different multiplier (one per batch, not one per event within it), so
0017's conclusion doesn't transfer here by default; it's cited for contrast, not as the answer.

**Allocations: zero change, across every existing exact-equality assertion.** Every
`fanout_send_*` constant in `crates/logit-bench/tests/allocations.rs` (0 / 6 / 1, including the
mixed-consumer cases) and every "Runtime" constant *as it existed at the time* held exactly, with
the prototype in place -- confirmed by the full `script/cibuild` suite passing unmodified, not just
spot-checked. This is the expected result stated plainly: copying 24 bytes into an already-allocated
enum payload doesn't touch the allocator.

*(Caveat added after this measurement: the "Runtime" section above was corrected and extended in
review -- the post-drain cost and `send_batch` coverage weren't part of the baseline this prototype
ran against, and weren't separately re-verified against it. Neither change is expected to interact
with `Delivered`'s size: the post-drain cost lives in `ComponentBuffer`'s map/sketch, and the
`async_trait` box lives in `Output::send`'s call, both orthogonal to what `TraceContext` on
`Delivered` touches -- but "expected" is a claim about mechanism, not a re-measurement, and should
be treated as such by whoever takes the actual decision.)*

**Throughput: no attributable regression, but only once run-to-run noise is accounted for.** A
naive before/after comparison showed all three `runtime` benches (`fanout_send_one_consumer`,
`fanout_send_two_consumers`, `process_batch_through_keep`) slower by a uniform ~40-50% with the
prototype in place -- which would be a real finding, except `process_batch_through_keep` never
touches `Delivered`/`Fanout` at all and moved by almost exactly the same percentage as the two that
do. That's this doc's own "runs on a busier machine come out uniformly ~20% slower" caveat (§2)
firing, not a cost of the change -- confirmed by re-running the *unmodified* prototype benches a
second time, which reproduced the original (pre-prototype) numbers almost exactly. Comparing
benchmark timings across separate `script/bench` invocations remains unreliable, as already
documented; a same-session, back-to-back comparison (not done here) is what a real decision should
use if the allocation numbers above ever turn out not to be the deciding factor on their own.

**What's left unmeasured, deliberately, because it needs a different kind of prototype:**
propagating an *inherited* context (reading a batch's own incoming `Delivered` as the parent for
what it produces, rather than always minting a fresh root) touches `run_transform`/`run_output`
themselves, not just `Fanout`/`Delivered` -- a materially bigger change than the type-and-copy cost
measured here, and the actual shape a real internal-spans feature would need.

**Decided and built, on this evidence:** [ADR 0020](../adr/0020-trace-context-propagation-on-delivered.md)
took the measurements above as its basis and implemented real propagation for the two node kinds
with an unambiguous parent (`Transform::process`/`ScriptWorker::process`'s non-flush path, and
`run_output`, which needed no new wiring at all). Every allocation-count assertion from before that
change held exactly, re-confirmed against the real implementation, not just the reverted prototype
-- see `docs/design/pipeline-graph.md`'s "Trace context propagation" section for the resulting
per-node-kind account, and `docs/known-gaps.md`'s internal-spans entry for what's still open
(flush's *n*-to-1 problem, `SpanRecord` emission, sampling).

**Emission itself landed next, on the same "measure, don't assume" basis, and changed nothing in
this section's numbers.** [ADR 0025](../adr/0025-internal-span-emission-and-deterministic-sampling.md)
built the piece this section's "what's left unmeasured" line named -- a real `Telemetry::span`/
`SpanGuard`, a bounded per-component span buffer, and `ComponentBuffer::drain`'s span-emitting pass
-- and the deliberately deterministic-on-`trace_id` sampler (`trace_is_sampled`) is *why* it changed
nothing here: no `sampled` bit needed propagating, so `TraceContext`/`Delivered` gained nothing
beyond what this section already measured. `size_of::<Delivered>()` stays exactly 56.

`SpanGuard`'s own "disabled/unsampled holds no state" shape (mirroring `Timer`'s) is what's *meant*
to make the unsampled path free the same way a disabled `Telemetry` handle already is -- but stated
precisely, not every existing `crates/logit-bench/tests/allocations.rs` constant that held unmodified
actually exercises a `Telemetry::span` call site, and it matters which do:

- `fanout_send_*` (`Fanout::send`/`send_blocking`, the listener span site) build their `Fanout` via
  `Fanout::new` with **no** `.with_telemetry(...)` call -- `Telemetry::default()`, fully disabled
  (`self.0` is `None`), which returns `SpanGuard::disabled()` from `Telemetry::span`'s very first
  line, *before* `trace_is_sampled` is ever called. These constants holding unmodified proves the
  disabled path is free; it says nothing about a *live* registry sampling below `1.0`.
- `process_batch_*`/`send_batch_*`'s "telemetry live" variants attach a real `Telemetry` from a live
  `Registry` -- but `process_batch`/`send_batch` are the per-batch bodies `run_transform`/
  `write_loop` call *into*; the actual `Telemetry::span` calls (ADR 0025) live one level up, in
  `run_transform`/`run_flush`/`run_lua`/`write_loop` themselves, none of which `logit-bench` drives
  directly under `CountingAlloc`. These constants holding unmodified is expected (nothing about them
  changed), but it does not exercise the sample-decision branch either.
- `unwrap_batch_*` has no span site at all, on any path.

**`fanout_send_one_consumer_with_a_live_unsampled_registry_costs_nothing` closes that specific gap,
directly, for the one span site `logit-bench` can and does drive under `CountingAlloc`:** a `Fanout`
carrying a real `Telemetry` handle from `Registry::with_span_sampling(0.0)` -- attached the same way
`crates/logit-cli/src/pipeline.rs::prepare` attaches one in production, deterministically never
sampled rather than relying on a fixture's `trace_id` happening to miss the default 0.1 band. `0`
allocations, matching the disabled case exactly: `Telemetry::span` reaches `trace_is_sampled`, gets
`false`, and returns `SpanGuard::disabled()` -- the same value, built the same way, as the disabled
path takes on line one. **What this does not cover:** the equivalent live-unsampled proof for
`run_transform`'s/`run_flush`'s/`run_lua`'s/`write_loop`'s own span sites, since none of those are
driven directly by `logit-bench` today (`process_batch`/`send_batch` are measured instead, and
neither one contains a span site) -- the code path is structurally identical (the exact same
`Telemetry::span` function, the exact same early return), but that is a code-reading argument, not a
measured one, for those four call sites specifically. Worth closing the same way if one of them ever
becomes independently benchmarkable.

**What a *sampled* span costs, measured directly in `crates/logit-core/src/telemetry.rs`'s own test
module (not `logit-bench`, since this is `logit-core`-local state, not a runtime/channel hop):** one
`PendingSpan` pushed into `ComponentBuffer`'s `Vec` at `finish`/`Drop` time, and one `Value::str` (a
`String` allocation) built at `ComponentBuffer::drain` time for the span's `name` (`"aggregate
flush"`, say) -- deliberately deferred that far, so a span that never survives to a drain (still
sitting in the buffer, or dropped past `MAX_SPANS_PER_COMPONENT`) never pays it. **The `Vec` push is
not a one-time, amortized-over-the-process-lifetime cost, the same way the points `HashMap`'s isn't**
(the "first call after an `internal` drain" finding in the Runtime section above): `ComponentBuffer::
drain`'s span pass takes the buffer's `Vec<PendingSpan>` with `mem::take`, exactly like the points
pass does with its `HashMap` -- which replaces it with a fresh, zero-capacity `Vec`, discarding the
old backing allocation along with everything it held. So the very next sampled span recorded after
*any* drain pays a fresh `Vec` growth, not a reuse of already-grown capacity; this recurs once per
`internal` drain interval for as long as spans keep getting sampled, the same recurring (not
one-time) shape the points map's post-drain cost already has. Both costs -- the `Vec` push and the
`name` `String` -- are strictly additional to whatever the surrounding node visit already paid
(`process_batch`'s `out` `Vec`, `send_batch`'s `async_trait` box, ...) -- spans ride alongside
existing work, they don't replace any of it.

### Zero-copy: where it holds

[data-model.md](data-model.md) commits to "`bytes::Bytes` everywhere strings and blobs appear," so
that a field parsed out of a socket read buffer is a refcounted slice of that buffer rather than a
fresh allocation. Measured, that commitment is **now kept by both inputs** — it used to be broken
by `statsd_in`, fixed since (item 3).

`syslog_in` is the exemplar, and was the reference implementation for `statsd_in`'s fix. `slice_of`
reconstructs a `Bytes` for each extracted field by pointer arithmetic back into the datagram, so
decoding a line costs exactly one allocation (the `Vec`) no matter how many fields it yields.
`crates/logit-bench`'s `syslog_fields_share_the_datagram_allocation` asserts this structurally, not
just by count. `json` continues it: `ValueSeed` deserializes straight into `Value` with no
intermediate `serde_json::Value` tree, and `borrowed_str_bytes` keeps an unescaped string a slice of
the message buffer (falling back to a copy only for a string serde had to unescape, which genuinely
lives elsewhere).

`statsd_in` used to build attribute values with `attributes.insert(k, v)` on a `&str`, which went
through `impl From<&str> for Value` → `Value::str` → `Bytes::from(String)` — a fresh copy of bytes
already sitting in the datagram — and then `build_event`'s `attributes.clone()` promoted each to a
shared `Bytes`, copying a second time. That was six of the eight allocations in the pre-fix row.
Now it uses the same `slice_of` pointer-arithmetic reconstruction `syslog.rs` does, and
`crates/logit-bench`'s `statsd_tag_values_share_the_datagram_allocation` asserts it structurally,
the same way the syslog test does — it replaced the old
`statsd_tag_values_are_copied_not_sliced`, exactly as that test's own doc comment said would happen
once someone fixed it. The 2 remaining allocations are a `Vec<Event>` per line plus one for the
batch, the same irreducible pair `syslog_in` has, just split across two `Vec`s due to a grammar
difference (statsd's multi-value form).

### Retention: what pins what

Zero-copy slicing trades allocation count for **retention**. Every field of every event decoded
from a datagram holds a reference to that one datagram buffer, so the buffer lives until the last
event derived from it is dropped. This is the right trade here because the buffer is right-sized:
`SyslogInput::run` and `StatsdInput::run` do `Bytes::copy_from_slice(&buf[..n])`, allocating exactly
`n` bytes, not the 64 KB of the reusable receive buffer.

**The obvious "optimization" here is a trap, and is deliberately not taken.** Reading straight into
a large shared `BytesMut` and `split_to`-ing each datagram off it would save that one allocation per
datagram — and would let a single retained log line pin a 64 KB chunk. For a pipeline where a slow
sink can hold events for seconds, that is a far worse failure mode than one small memcpy per
datagram. `datagram_copy_is_one_right_sized_allocation` guards the current behavior. Don't
"fix" this.

## 3. Sharing versus copying

What is shared today:

- **`Resource`** — `Arc`-shared across a whole batch, never copied per event.
- **String and blob data** — `Bytes`, refcounted; a clone is an atomic increment.
- **Attribute keys and metric names** — interned to a 4-byte `Symbol` (see §4).

What is copied, and when, is no longer a flat rule — it depends on fan-out shape (below). What never
changes regardless: a mutation on one branch of a fan-out is structurally invisible to a sibling
branch, with nothing extra to design or maintain for that guarantee. `runtime.rs`'s
`a_mutation_on_one_fan_out_branch_is_invisible_to_the_sibling_branch` is the test that pins it, and
it's the thing every change described below was built to never regress — including through three
rounds of correcting an initial performance claim, per that section.

For scale: the deep clone this section used to describe unconditionally (4 allocations, a 792-byte
memcpy per event per extra branch, 228 ns, ~11% of the ingest chain) is still exactly what a
mutating branch pays when it has to.

### The `Arc<EventBatch>` copy-on-write change — done, and genuinely more subtle than first assumed

The design this section originally recommended: put `Arc<EventBatch>` on the channels, and have
each consumer do `Arc::try_unwrap(batch).unwrap_or_else(|shared| (*shared).clone())`. Landed in
three rounds (`docs/adr/0016-arc-eventbatch-copy-on-write.md`, PR #33) — worth reading in full for
how much the initial "strictly no worse anywhere" framing had to be corrected against real
measurement. The honest result, by fan-out shape:

| Fan-out shape (2 consumers) | Allocations | vs. `main`'s flat 5 |
|---|---:|---|
| Single consumer (any kind) | **0** | strictly better |
| Both `Output` | **1** | strictly better |
| One `Output`, one `Transform`/Lua | **1 or 6** | scheduling-dependent, either direction |
| Both `Transform`/Lua-style, no `Output` | **6** | 1 worse, always |

**What's unconditionally better**: a single-consumer edge — the common case, every shipped
listener's first hop, and every interior edge of a linear chain — costs nothing, via a
`Delivered::Owned | Delivered::Shared(Arc<_>)` payload that skips the `Arc` entirely when there's
only one consumer. This fixed a regression the first draft introduced (wrapping in `Arc`
unconditionally, which cost a single-consumer edge one allocation for nothing). An all-`Output`
fan-out also becomes unconditionally free past the one `Arc::new`: `Output::send` was changed to
take `&EventBatch` instead of an owned one (round two), so a read-only sink branch never calls
`Arc::try_unwrap` at all — it just borrows through the `Arc`, regardless of how many sibling
branches still hold their own handle. This *is* the "every read-only branch pays one atomic, not a
clone" saving originally claimed, delivered — for this shape.

**What's genuinely racy, not deterministic in either direction**: a fan-out with one `Output`
branch and one mutating (`Transform`/`ScriptWorker`) branch — the actually-common shape, matching
the nginx reference config's `tap`/`trimmed` split — costs **1 or 6**, decided by real tokio
scheduling, never something in between and never `main`'s flat 5. Whether the mutating sibling's
`unwrap_batch` call finds the `Output` branch's handle already gone (free, cost 1) or still alive
(clone, cost 6) depends on which finishes first — genuinely reachable both ways, confirmed by two
tests that manually pin each ordering
(`fanout_send_mixed_output_and_transform_consumers[_when_output_finishes_first]`).

**A further hop past `Fanout::send` exists for an `Output` branch, and it is not free.** The table
above measures `Fanout::send` alone; a sink's batch then takes one more step,
`drain_inbox` (`runtime.rs`, [ADR 0021](../adr/0021-buffered-sink-delivery.md)), which moves it off
the component's inbox into its `SinkQueue`. For a single-consumer edge, `Fanout::send` itself costs
0 (the table's first row) — but `drain_inbox` always needs an `Arc<EventBatch>` to hand to the
queue, so a `Delivered::Owned` batch costs exactly one `Arc::new` there, previously paid nowhere on
this path at all. `drain_inbox_single_consumer_owned_batch_costs_exactly_the_arc`
(`crates/logit-bench/tests/allocations.rs`) pins this directly, driving `drain_inbox` on its own
rather than through a full `run`. A `Delivered::Shared` batch (a real fan-out) already carries its
`Arc`, so this hop costs `drain_inbox` nothing further beyond what the table above already counts.

**This likelihood flipped with [ADR 0021](../adr/0021-buffered-sink-delivery.md).** Before it,
`run_output` held its `Arc` handle for the full duration of `output.send` — typically real I/O,
measurably slower than a `Transform`'s local processing — so 6 was the likelier practical outcome
despite 1 being reachable. After the drain/write split, `drain_inbox` drops its handle the instant
it matches the received `Delivered`, immediately on receipt and entirely decoupled from how long
the paired `write_loop`'s `output.send` takes — so the race is now between two comparably cheap,
local operations on each side, no longer one side waiting on I/O. **1 is now the likelier practical
outcome**, though — as before — this is an expectation about typical scheduling, not something the
design guarantees; the two pinned-ordering tests above still exist specifically because both
outcomes remain genuinely reachable.

**What doesn't close at all**: a fan-out with no `Output` branch — two `Transform`s, or a
`Transform` and a Lua stage, sharing one node. Both sides need to mutate, so neither can borrow;
this is exactly round one's `1 + (N-1) × clone`, deterministically 6 for two consumers — one
allocation worse than `main`, with no racy path to anything better, since nothing in this shape
ever finishes without competing for the free unwrap. Closing it would need widening
`Transform`/`ScriptWorker` past what they need to do their job (mutate/consume an owned `Event`),
which isn't on the table.

**A fix for the racy case's raciness was sketched and deliberately not taken — and it's narrower
than it first looks.** For exactly one consumer of each kind, making `Fanout` aware of which is
which and giving the mutating one an unconditional direct clone (bypassing `Arc` entirely) would
turn 1-or-6 into a fixed 6 — trading the chance at 1 for predictability. That does **not**
generalize past two consumers: worked through directly for 2 borrowing + 2 owning, the "aware"
design's own cost turns on an unresolved internal choice (direct clone per owning consumer costs
11; a second dedicated `Arc` for the owning group to race over costs 12, worse, since that second
`Arc::new` outweighs what the race saves) — while *today's* racy design already reaches as low as 6
for that same shape, whenever every `Output` branch happens to finish first. So an aware fix would
fix the current *worst* case as the *guaranteed* one, not strictly dominate what exists, once
either group grows past one member. Left as an open design problem, not a specified direction — see
the ADR's Alternatives for the full working.

Two things that didn't change through any of this:

- **`Transform::process` never had to change** for the `Arc` plumbing itself — the wrap/unwrap
  boundary sits entirely inside `logit-pipeline`. `Output::send`'s signature did change
  (`&EventBatch`, not owned), the one trait-level change this design needed.
- Granularity: putting the `Arc` around the *batch*, not each `Event`, costs one atomic per batch
  rather than one allocation and one atomic *per event* — worse than what it replaces for the
  single-consumer case. Prior art for the batch-level choice: Vector's `LogEvent` is an
  `Arc<Inner>` with copy-on-write for the same reason.

A second, separable change: `Transform::process(&mut self, &Arc<Resource>, &mut Event) -> bool`
plus `Vec::retain_mut` in `run_transform` would remove one full 792-byte `Event` memcpy per node
hop and one `Vec` allocation per batch per node. Nothing is lost — the trait already can't emit
more than one event per input. Deserves its own ADR; gets more expensive to make with every
transform that lands (§8 item 14).

## 4. Interning: the bargain, and its bounds

`logit_core::interner` maps every attribute key and metric name through a process-global
`lasso::ThreadedRodeo` to a 4-byte `Symbol`. This pays for itself several times over: `AttrMap`
compares and sorts `u32`s, `SeriesKey` hashes them, and the planned wire format's dictionary
encoding ([wire-protocol.md](wire-protocol.md)) is backed by the same table.

**`ThreadedRodeo` never evicts and never frees.** Every *distinct* string ever interned is
retained for the life of the process, at a measured **~94-124 bytes each** (for a 40-character
name -- roughly 2.4x the string's own length, the rest being map and index overhead).

Two facts bound how much that matters, and they're worth stating before the risk:

- **Re-interning a string the table already holds allocates nothing.** Measured: zero allocations
  for 1000 repeat interns (`re_interning_an_existing_string_is_free`). A pipeline whose keys and
  metric names come from a fixed schema reaches steady state and stays flat forever. For its
  intended use, this design is not just acceptable, it's free.
- **Only keys and metric names are interned. Values are not.** `AttrMap::insert` interns the key
  and stores the value as a `Value::Str(Bytes)`. This matters more than it sounds: the
  high-cardinality dimension in telemetry is almost always the *value* side -- host, request id,
  user agent, URL path, trace id -- and none of it touches the interner. The classic cardinality
  explosion is not an interner problem here.

So the exposure is narrow and specific: **a string that is real, is used in key or metric-name
position, and never repeats.** In practice that is one thing above all --

- **statsd metric names** (`intern(name)` in `statsd.rs`'s `build_event`), which are
  client-controlled and where putting an id in the name is a well-worn anti-pattern:
  `user.<id>.logins`, `deploys.<sha>`, `orders.<order_id>.latency`. One such client is enough.
- Secondarily, **JSON object keys** from log bodies (`json.rs`) when a producer puts data in key
  position (`{"req_a1b2c3": {...}}`), and **DogStatsD tag keys** for the same reason. Both are
  schema-shaped in normal use and unbounded only when abused.

At ~94 bytes each, a million distinct metric names is ~94 MB retained with no way to reclaim it,
and nothing anywhere reporting that it happened.

### Accepted, with the premise written down

**No work is planned here, deliberately.** The reasoning, so it can be re-checked rather than
re-litigated:

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
  (`keep` in front of `aggregate`, §4's closing note), and it would bite ~6× harder and sooner.

**What would change the calculus:** a listener that stops being private — a public or
multi-tenant ingest endpoint, or a hosted aggregator taking traffic from parties the operator
doesn't control. If that ever ships, revisit this section first, because the retrofit is expensive:
`Symbol` is `Copy` and `resolve` *panics* on an unknown symbol, so `AttrMap`, `MetricRecord`,
`SeriesKey`, the Lua proxy, and the planned wire dictionary are all written against "symbols are
eternal."

`interner::len()` now exists (added alongside the `AttrMap::get` fix below, to let a test
assert directly that a miss doesn't grow the table) — if a diagnostics facility lands (the
`tracing` migration in [known-gaps.md](../known-gaps.md)), wiring it into a gauge is now a small
addition rather than a new one.

### What was *not* the risk: failed lookups — fixed anyway

`AttrMap::get` used to intern rather than do a lookup-only probe, so in principle a miss added a
key no event carries. In practice this was never a growth path. There were exactly three
production `get` call sites in the tree:

- `kv_metrics.rs` (twice), keyed by `m.field` -- a **config** string, fixed at startup. Hit or
  miss, it's interned once and never again.
- `proxy.rs`'s `AttrsProxy::__index`, keyed by whatever a Lua script indexes -- normally a literal
  in the script, so also a bounded set. Unbounded only for a script that builds keys out of event
  data, which is unusual and is trusted config besides.

So `AttrMap::get`'s interning was always a **CPU** problem, not a memory one -- a hash plus a
concurrent-map probe on the hot path for a lookup that could be cheaper. Fixed regardless (§8 item
3): `AttrMap::get`/`remove` now use `interner::lookup`, a non-interning probe, falling through to
the existing `binary_search_by_key` only on a hit. Pure efficiency win, no behavior change --
`insert` still calls `intern`, since it may legitimately need to mint a new symbol.


### The other unbounded structure

`Aggregator`'s per-resource `HashMap<SeriesKey, Accumulator>` grows with tag cardinality within a
window. This one is deliberate and has an operator-facing mitigation that the reference config
uses: put `keep` in front of `aggregate`, so the tag set `SeriesKey` keys on is bounded by config
rather than by input. `logit_transforms::keep`'s module docs say so, and the numbers here show the
second-order effect too — with `keep`, absorbing an event allocates nothing; without it, the
10-attribute map spills and costs one allocation *per metric per event*.

## 5. Bounds on in-flight memory

`CHANNEL_CAPACITY` is 64 (`runtime.rs`), and it counts **batches, not bytes or events**. Batch size
is unbounded: one 65 KB syslog datagram can decode to hundreds of events, so a single edge can hold
tens of megabytes with nothing in the config saying so, and total in-flight memory scales with the
number of graph edges.

This has not bitten anything yet because the datagram size caps batch size in practice. It becomes
a real problem with a TCP or file-tail input, where nothing caps how many events one read produces.

The byte-aware bound is `EventBatch::estimated_heap_bytes()` (`crates/logit-core/src/event.rs`): a
deliberately approximate, O(events) walk. The dominant term, added after an initial pass
undercounted it, is the `Vec<Event>` backing storage itself --
`events.capacity() * size_of::<Event>()` -- which every event pays (776 bytes each, §1) *before*
any nested heap payload; a batch of numeric-only metrics with no string attributes would otherwise
estimate close to zero despite genuinely holding hundreds of bytes per event. On top of that: a
batch's attribute keys/values, log bodies, span-owned data (name, and every `SpanEvent`/`SpanLink`'s
own backing storage and attributes), and metric records, plus its `Resource`'s attributes counted
once per batch rather than once per event (the resource is `Arc`-shared, not copied per event). It
is an admission-control estimate, not an allocator-accounting figure — unlike §1's numbers, it is
*not* asserted exactly anywhere, and is deliberately exempt from `type_sizes.rs`/`allocations.rs`'s
exact-equality discipline: a `MetricKind::Distribution`'s `DDSketch` is approximated with a fixed
constant rather than walked bin-by-bin, and `Value`'s numeric/bool/null variants (stored inline, no
heap component) contribute nothing. It is consumed by the buffered sink-delivery work
(`docs/plans/0004-buffered-sink-delivery.md`, `docs/adr/0021-buffered-sink-delivery.md`): every
sink's `SinkQueue` (`crates/logit-pipeline/src/sink_queue.rs`) bounds itself on both batch count
and this estimate, whichever trips first.

## 6. The allocator

`logit` runs a multi-threaded tokio runtime plus one OS thread per Lua component, allocating and
freeing small objects continuously for weeks. glibc's `malloc` — what `debian:bookworm-slim` ships
— handles that shape poorly on two counts: per-thread arenas fragment when allocation and
deallocation happen on different threads (which is what a pipeline of channel-connected nodes does
by construction), and it returns memory to the OS reluctantly, so RSS drifts upward over days
while the working set stays flat.

`logit` therefore uses **jemalloc** by default —
[ADR 0015](../adr/0015-jemalloc-global-allocator.md) — behind a default-on `jemalloc` feature on
`logit-cli`, so `--no-default-features` still builds against the system allocator and the
comparison stays available.

Note the division of labour when reading numbers here: **allocation counts are
allocator-independent** (they count calls), so every count in this document holds under either.
Timings do not.

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

Three layers, deliberately separated by how noisy they are:

**`crates/logit-core/tests/type_sizes.rs`** — exact `size_of` assertions on the event model. The
highest value per line in this whole effort: no dependencies, deterministic, runs in existing CI,
and catches the day someone adds a field that costs every in-flight event another 200 bytes. Exact
equality rather than an upper bound, on purpose — a `<=` would absorb exactly what it exists to
catch.

**`crates/logit-bench/tests/allocations.rs`** — exact allocation counts per stage, via
`CountingAlloc`, a `GlobalAlloc` wrapper installed only in that test binary. Ordinary `#[test]`s,
so `script/test` runs them in normal CI and an allocation regression fails a build. Two things make
them deterministic: counters are **thread-local** (so nothing another thread does leaks in), and
`cargo nextest` runs each test in its own process. Every measurement warms its subject first,
because a cold call folds in one-time initialization and reports a number that never reproduces.

**`crates/logit-bench/benches/pipeline.rs`** — divan throughput benches, run by hand with
`script/bench`. Deliberately **not** in `script/cibuild`: wall-clock benchmarking on a shared CI
runner measures the runner. divan's `AllocProfiler` reports allocation counts alongside timings, so
the two layers cross-check each other.

One constraint worth knowing before adding benches: divan's `AllocProfiler` only counts allocations
on threads it controls. Almost every bench here sidesteps the question entirely by calling
decoders, transforms, and encoders **directly**, never touching the tokio runtime or the channels
between nodes. The one deliberate exception is `pipeline.rs`'s `runtime` module, which *does* drive
`Fanout::send`/`recv` across a real channel — safely, because it never calls `tokio::spawn`: a
`current_thread` runtime's `block_on` runs everything on the calling thread, the one thread Divan is
already watching, so nothing is handed to a worker thread it can't see. The constraint that actually
matters is **no cross-thread hop** (a `tokio::spawn`, a multi-thread runtime, a real OS thread), not
"no channel" — `crates/logit-bench/tests/allocations.rs`'s own `fanout_send_*`/`unwrap_batch_*`
tests (thread-local `CountingAlloc`, same reasoning) independently confirm the same numbers this
module reports. What a full multi-node graph costs end to end, spread across the real worker
threads and OS threads `run_with_shutdown` actually spawns, is still a separate question needing a
load generator, not a microbenchmark.

**Spans are the one live-registry cost only partly covered by either of the two layers above.**
Most of `crates/logit-bench/tests/allocations.rs`'s span-adjacent constants (`fanout_send_*`) use
`Telemetry::default()` (disabled), which returns `SpanGuard::disabled()` before `Telemetry::span`
ever reaches its sample-decision branch; `fanout_send_one_consumer_with_a_live_unsampled_registry_
costs_nothing` is the one exception, a real `Registry::with_span_sampling(0.0)` proving the *live,
deterministically-unsampled* path through `Fanout::send`'s own span site is equally free (see
"Costing internal spans" in §2 for the account of which other span sites this does and doesn't
cover). Neither says anything about what a span that *does* get sampled costs. That measurement
lives directly in `crates/logit-core/src/telemetry.rs`'s own test module instead (no
`CountingAlloc` harness needed to state it precisely): a sampled span costs one `PendingSpan`
pushed into `ComponentBuffer`'s `Vec` at `SpanGuard::finish`/`Drop` time, plus one `Value::str`
built at `ComponentBuffer::drain` time for the span's `name` — deferred that far specifically so a
span that never survives to a drain (still buffered, or dropped past `MAX_SPANS_PER_COMPONENT`)
never pays it, and recurring once per `internal` drain interval rather than amortized once, since
`drain`'s `mem::take` discards the `Vec`'s capacity along with its contents. See "Costing internal
spans" (§2) for the full account.

### Fixtures: synthetic inputs, no external services

**Nothing in the test or bench suite may depend on a running nginx, InfluxDB, or any other
service.** Today it doesn't: `fixtures.rs` holds `const` wire-format literals, the components are
called directly, and even `influxdb_out`'s retry tests bind an in-process `TcpListener` on
`127.0.0.1:0` rather than talking to a real server. Keep it that way — fixtures that stand up
services get slow, flaky, and large very quickly, and they stop being runnable in CI.

The pattern for a new shape, in order of preference:

1. **A `const` byte literal** for anything with a wire format, one representative record plus a
   `count` multiplier for volume (as `nginx_syslog_datagram(n)` does) — not a recorded corpus.
2. **Directly-constructed `Event`s** where no input codec exists yet to record from. `SpanRecord`
   is the current example: there is no OTLP input, so a span fixture has to be built in Rust. When
   a decoder lands, a captured payload can replace it.

Synthetic doesn't mean guessed. A literal should carry **provenance** — which software and config
produced this shape, and when it was last checked against the real thing. `NGINX_SYSLOG_LINE` was
derived from `examples/nginx/nginx.conf`'s `access_json_syslog` format and confirmed against a live
nginx run (the emitted `syslog.facility=23`/`severity=6` match its `<190>` priority exactly). A
one-off exploration against real software is the right way to *inform* a fixture; the fixture is
what gets committed.

## 8. Recommendations

Ordered by **how much the evidence supports them**, not by the raw nginx numbers — see §0 for why
those differ. An item that helps every workload with no tradeoff outranks a bigger saving that
might regress a workload the fixtures don't cover.

### Done

1. ~~**Fix the InfluxDB encoder's allocation churn.**~~ **Done** — 18,024 allocations per 100-event
   batch to 30 (§2). Was the single largest cost in the pipeline; now smaller than ingest.
   Workload-independent: it helps any config with an `influxdb_out`.
2. ~~**Make `AttrMap::get` non-interning.**~~ **Done** — `AttrMap::get`/`remove` now probe via
   `interner::lookup` instead of `intern`, closing both the CPU cost and the theoretical growth
   path in one change (§4). Also added `interner::len()` as a side effect, needed to test the fix.
3. ~~**Give `statsd_in` the `slice_of` treatment.**~~ **Done** — 8 → 2 allocations per line (§2's
   zero-copy section). `statsd_in` now keeps the same zero-copy promise `syslog_in` always has.
4. ~~**Trim `json`'s allocations.**~~ **Done, further than scoped** — 7 → 1 for the nginx shape,
   confirmed to hold at 1 for a 28-field wide-JSON line too (§2). The original plan was a
   checkpoint-and-rollback scheme over the intermediate `AttrMap`; measuring first showed the real
   cost was `collect_attrmap`'s per-key owned `String` allocation (`next_key::<String>()`), not the
   intermediate map itself — so the actual fix interns keys straight off the deserializer instead.
   Worth internalizing: the guess this section made from the nginx number alone (properly
   caveated at the time as a guess) was wrong about the mechanism, and measuring the fix against a
   wider shape is what caught that.
5. ~~**Give `stdio_out` the same treatment `influxdb_out` got.**~~ **Done** — 1801 → 101
   allocations per 100 events, ~18× (§2). Merge-joins the resource/event attribute maps the same
   way `influxdb_out` does, and formats straight into reused buffers instead of `format!` per
   value.
6. ~~**Reduce the Lua boundary's allocations.**~~ **Done** — 21 → 9 per event round trip. Caching
   `process`/`flush` (via `mlua::RegistryKey`, resolved once at load rather than looked up from
   `_G` per call), caching the `AttrsProxy` userdata per event instead of rebuilding it per
   attribute access, and taking `mlua::String` instead of an owned `String` in both metamethods.
   Two real edge cases came out of review and were closed rather than left as caveats: a script
   that stashes `event.attributes` across a return boundary now fails loudly (in this crate's own
   voice, not mlua's raw error) instead of silently working with a disconnected copy, and a
   `flush` global that exists but isn't a function is now a load-time error — matching
   `process`'s existing `MissingProcess` — instead of being silently treated as "no `flush()`" and
   quietly losing every flush tick's events forever.
7. ~~**`Arc<EventBatch>` copy-on-write on channels.**~~ **Done, with real caveats** (§3) — landed
   over three rounds (`docs/adr/0016-arc-eventbatch-copy-on-write.md`), each correcting an
   overclaim the previous one made. Single-consumer edges and all-`Output` fan-outs are
   unconditionally better (0 and 1 allocations respectively, both strict wins). A fan-out mixing
   one `Output` branch with one mutating branch is genuinely racy — 1 or 6, decided by scheduling,
   never `main`'s flat 5 either way. A fan-out with no `Output` branch at all doesn't improve —
   still 6, one worse than `main`, deterministically. Read §3 in full before citing a single number
   from this item; which one applies depends entirely on fan-out shape.
8. ~~**Re-pick `AttrMap`'s inline capacity — down.**~~ **Decided: don't shrink** (§1). Dropping
   capacity 8 → 4 only ever costs an allocation across every shape measured, never saves one.
   Whether to go the *other* direction (increase it) is a separate, still-open question — see the
   new "Deferred" bucket below.
9. ~~**`Box` `SpanRecord`.**~~ **Decided: don't box** (§1,
   [ADR 0017](../adr/0017-minimize-allocations-over-event-size.md)). Measured first (construction
   11 → 12 allocations, clone 2 → 3, for the span fixture) before implementing and then reverting:
   the 128-byte saving trades against an allocation cost that a trace-focused deployment would pay
   on most events once a span-producing input exists — evaluated against that eventual workload,
   not against `v0.1`'s current lack of one, per the new policy.
10. ~~**`Box` the `DdSketch`.**~~ **Decided: don't box** (§1, ADR 0017). Measured both the
    single-distribution and distribution-heavy fixtures before implementing and then reverting:
    boxing saved 144 bytes but cost the project's own reference config a real allocation increase
    (full ingest chain 5 → 7) — distributions are a shipping, commonly-populated feature, not the
    rare case the byte saving alone would suggest trading for.
11. ~~**Enable smallvec's `union` feature.**~~ **Done** — 16 bytes off every `Event`, no tradeoff,
    exactly as predicted. `Event`: 792 → 776 bytes.
12. ~~**Give `syslog_out` the same treatment `influxdb_out`/`stdio_out` got.**~~ **Done** — 401 →
    100 allocations per 100 events, ~4× (§2). `SyslogEncoder` now holds `line`/`raw_msg`/`scratch`
    as reused struct fields instead of allocating fresh `String`s per event (three of them as
    function-locals recreated on every call, the same mistake the other two encoders had already
    moved past). Found by review, not independently — see §2's own writeup for the mechanism.

### Deferred — needs real production data, not more synthetic measurement

Both of these are the same shape of question `AttrMap`'s "should we shrink it" already had an
answer for (item 8): a `SmallVec` inline-capacity choice, compile-time-fixed, that trades bytes
against allocations depending on how wide events actually are in practice. Four synthetic fixtures
were enough to rule out shrinking `AttrMap`; they are not enough to pick a number for either of
these, because that needs a real distribution of attribute/metric counts across production
traffic, which doesn't exist yet and can't be synthesized honestly.

12. **`AttrMap`'s inline capacity, increased rather than shrunk.** Would reduce spills on wider
    shapes (the nginx config's 10 attributes, wide-JSON's 32), at the cost of a larger `AttrMap` —
    and therefore `Event` — for every event, paid whether or not the wider shape is common in a
    given deployment. Not a guess to make without the data.
13. **`MetricList`'s inline capacity (currently 1).** Any event with 2+ metrics spills — always
    true for the nginx reference config (4 metrics) and for `kv_metrics` configurations generally,
    by design. Note the interaction with item 10 above: with `DdSketch` staying inlined,
    `MetricRecord` is 184 bytes, so widening this capacity costs considerably more per additional
    slot than it would have if the sketch had been boxed — the two decisions aren't independent.

### Later — needs a reason first

14. **`Transform::process(&mut Event) -> bool`.** Removes a 792-byte memcpy per node hop. Gets more
    expensive to decide with every transform that lands, so decide it early even if applied late.
    Touches `runtime.rs`, the same file item 7's three rounds just settled — a fresh reason to
    check `unwrap_batch`'s current shape before starting, not a blocker any more.
15. **`AttrMap` accessors keyed by `Symbol`,** eliminating the remaining `resolve` → `intern` round
    trips. Narrower than it used to be: `influxdb_out`'s and `stdio_out`'s are both gone now (both
    encoders merge-join instead of clone-and-reinsert). What's left is `json`'s final merge into
    `event.attributes` (the per-key intern step itself is already gone; only the map insertion
    still takes `&str`) and `keep`'s rebuild.
16. **Byte-aware channel bounds** (§5), before a TCP or file-tail input makes batch size unbounded
    in practice.
17. **~~Bound the interner~~ — accepted as-is, see §4.** Listeners are private, so the namespace is
    user-controlled; the metric store and `logit`'s own aggregation window both fail earlier and
    harder under the same abuse. Revisit only if a listener stops being private.

## Open questions

- **What is the real attribute/metric-count distribution** across the inputs `logit` will
  actually see? Partly answered: four representative shapes are now measured (statsd 0-4, nginx
  10, logs-only 6, wide-JSON 32), enough to rule out shrinking `AttrMap`'s inline capacity (§1, §8
  item 8) but not enough to decide whether to *increase* it, or to pick `MetricList`'s (§8 items
  12-13). That needs real production telemetry, not more synthetic fixtures — recorded as
  deliberately deferred rather than guessed, per the direction settled when `DdSketch`/`SpanRecord`
  were measured and then not boxed for the same reason (§1, [ADR 0017](../adr/0017-minimize-allocations-over-event-size.md)).
- **What do the unmeasured workload shapes actually cost?** Answered, for allocation and clone
  cost: logs-only, wide-JSON, distribution-heavy metrics, and spans are all fixtured and measured
  (§0, §2), and that evidence is what drove §8 items 8-10's decisions (one confirmed-unchanged, two
  measured-then-reverted). The two capacity questions above are what's left open, and they need a
  different kind of evidence than this pass can generate on its own.
- **Does jemalloc actually flatten RSS for this workload?** Partly answered. A short soak of the
  reference config against the real nginx stack — 60,000 requests through
  `syslog_in → json → kv_metrics → {stdio_out, keep → aggregate → influxdb_out}` — held RSS at
  11.2 MB ± 3%, finishing marginally *below* where it started, with aggregated windows landing in
  InfluxDB throughout. That rules out a leak and shows pages are being returned. It does **not**
  isolate jemalloc from glibc: the same soak has not been run with `--no-default-features`, and
  the drift ADR 0015 is really about takes days, not minutes, to show up. The escape hatch exists
  so that comparison stays one build away.
- **Is there a compact `Event` representation** worth having — one that doesn't reserve span and
  sketch space on a bare log line? Boxing the rare variants (§8 items 9-10) is the cheap answer, but
  it only pays where the variant really is rare, and "rare" is workload-dependent: a sketch is the
  common case in a statsd-timing pipeline and absent entirely from a logs-only one. If the broader
  fixtures show no single boxing choice wins across shapes, that's the signal this needs a
  representational answer rather than a tuning one.
