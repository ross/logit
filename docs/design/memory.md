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

## 1. The event model's footprint

```
Event                                       792 bytes
├── timestamp: i64                            8
├── attributes: AttrMap                      400   ← SmallVec<[(Symbol, Value); 8]>
├── log: Option<LogRecord>                    48
├── metrics: MetricList                      200   ← SmallVec<[MetricRecord; 1]>
└── span: Option<SpanRecord>                 136
```

with the constituent parts:

| Type | Size | Why |
|---|---:|---|
| `Symbol` (`lasso::Spur`) | 4 | `NonZeroU32`; `Option<Symbol>` is also 4 |
| `Value` | 40 | sized by `Bytes` (4 words) plus an aligned discriminant |
| `(Symbol, Value)` | 48 | 4 bytes of padding after `Symbol` |
| `AttrMap` | 400 | 8 × 48 inline, + 16 of smallvec overhead |
| `MetricKind` | 176 | almost entirely the inlined `DDSketch` |
| `MetricRecord` | 184 | `MetricKind` + name + unit |
| `MetricList` | 200 | 1 × 184 inline + 16 |
| `LogRecord` | 48 | `Option<LogRecord>` is also 48 — `Severity`'s niche absorbs `None` |
| `SpanRecord` | 136 | `Option<SpanRecord>` is also 136 — `SpanKind`'s niche absorbs `None` |

**Three things about this are worth internalizing.**

**A `SmallVec` costs its inline capacity whether or not it has spilled.** The inline array and the
heap `(ptr, cap)` pair share one slot, sized by the larger. So an event with 13 attributes pays a
heap allocation *and* the full 400 bytes. Inline capacity 8 is therefore not "free up to 8" — it is
384 bytes on every event, forever, and the reference nginx pipeline spills past it anyway.

**A statsd counter costs the same 792 bytes as a fully-populated nginx access log.** `Event` has no
compact representation for the common case; the space for attributes, a sketch, and a span is
reserved unconditionally. That is the price of "an event is whatever it carries"
([ADR 0012](../adr/0012-multi-payload-events.md)) implemented with inline storage.

**`MetricKind::Distribution` sets the size of every metric.** A `Counter(f64)` needs 8 bytes and
pays 176, because `DDSketch` (two `Store`s and a `Config`) is inlined into the enum.

### What could be reclaimed

Not done — this pass measures rather than optimizes — but sized here so the trade is visible:

| Change | Saves | Cost |
|---|---:|---|
| `Box` the `DdSketch` in `MetricKind::Distribution` | ~168 B/event | one indirection on distribution metrics only |
| `Box` `SpanRecord` (`Option<Box<_>>` is 8 B) | 128 B/event | one indirection on spans only |
| smallvec's `union` feature | 16 B/event | none — it's a feature flag |
| Re-pick `AttrMap`'s inline capacity | up to 192 B/event | more spills on wide events |

Together these take `Event` from 792 bytes to roughly 300, without changing what it can represent.
The distribution and span boxes are nearly free: both variants are rare, and both already cost a
heap allocation of their own when present.

## 2. Where the allocations are

Measured over the reference pipeline (`examples/nginx-to-influxdb.yaml`,
`syslog_in → json → kv_metrics → keep → aggregate → influxdb_out`) with one real nginx access-log
line — `crates/logit-bench/tests/allocations.rs`.

| Stage | allocs | Notes |
|---|---:|---|
| `syslog_in` decode 1 line | **1** | just the `Vec<Event>`; every field slices the datagram |
| `syslog_in` decode 100 lines | **1** | + 5 reallocs from `Vec` growth |
| `statsd_in` decode 1 line | **8** | see below — tag values are copied, not sliced |
| `json` parse + merge | **7** | intermediate `AttrMap`, then the merge spills the event's |
| `kv_metrics` derive 4 metrics | **3** | `MetricList` spill + one `bins` Vec per sketch |
| `keep` filter to 3 attrs | **0** | 3 attributes fit inline |
| `aggregate` absorb (after `keep`) | **0** | `SeriesKey` clone stays inline |
| `aggregate` absorb (no `keep`) | **4** | one per metric — the map no longer fits inline |
| `aggregate` flush 4 series | **2** | |
| **full ingest chain, 1 line** | **11** | decode → aggregate |
| `Event::clone` (nginx shape) | **4** | what each extra fan-out branch costs |
| `Event::clone` (statsd shape) | **0** | fits entirely inline |
| `stdio_out` encode 100 events | **1801** | ~18/event |
| `influxdb_out` encode 100 events | **18024** | **~180/event** |

And the corresponding times:

| Stage | fastest | per event |
|---|---:|---:|
| `syslog_in` decode, 100 lines | 28.9 µs | 289 ns |
| `statsd_in` decode, 100 lines | 43.6 µs | 436 ns |
| `json` | 748 ns | 748 ns |
| `kv_metrics` | 304 ns | 304 ns |
| `keep` | 404 ns | 404 ns |
| `aggregate` absorb | 721 ns | 721 ns |
| **full ingest chain** | **2.61 µs** | ~383k lines/s/core |
| `Event::clone` (nginx / statsd / distribution) | 272 / 99 / 63 ns | |
| `stdio_out` encode, 100 events | 236 µs | 2.36 µs |
| `influxdb_out` encode, 100 events | 496 µs | **4.96 µs** |

### The headline result: the bottleneck is the output encoder, not the event model

**Encoding one event for InfluxDB costs ~180 allocations and 4.96 µs — roughly twice what
ingesting it costs (11 allocations, 2.61 µs), and sixteen times what cloning it for an extra
fan-out branch costs.** That was not the expected answer.
[known-gaps.md](../known-gaps.md) has been carrying `Arc<EventBatch>` copy-on-write as *the*
identified fix for pipeline cost, and [pipeline-graph.md](pipeline-graph.md) calls the fan-out
clone "a real cost." Both are true, and both are second-order next to this.

The cost is entirely in how lines are built, not in anything about the data model:

- `escape_tag` and `escape_measurement` build **four intermediate `String`s each** via chained
  `.replace()`, on every tag of every point, whether or not any character needs escaping.
- `metric_fields` returns a `Vec<(String, String)>`, with a `format!` per field name and a
  `to_string()` per value.
- The series-identity `HashMap` is keyed by `line.clone()` — a fresh `String` allocated on every
  call, not just on insert.
- `render_tag_suffix` clones the resource `AttrMap` and re-inserts every event attribute into it,
  per event.

None of that needs the event model to change. `escape_*` returning `Cow<str>` (or writing straight
into the output buffer), a reused line buffer, and `raw_entry` for the series map would take the
large majority of it. **This is where an optimization pass should start.**

For contrast, `stdio_out` costs ~18 allocations per event doing a comparable amount of formatting —
because it appends into one shared buffer instead of building per-line `String`s.

### Zero-copy: where it holds, where it doesn't

[data-model.md](data-model.md) commits to "`bytes::Bytes` everywhere strings and blobs appear," so
that a field parsed out of a socket read buffer is a refcounted slice of that buffer rather than a
fresh allocation. Measured, that commitment is **kept by `syslog_in` and broken by `statsd_in`**.

`syslog_in` is the exemplar. `slice_of` reconstructs a `Bytes` for each extracted field by pointer
arithmetic back into the datagram, so decoding a line costs exactly one allocation (the `Vec`) no
matter how many fields it yields. `crates/logit-bench`'s `syslog_fields_share_the_datagram_allocation`
asserts this structurally, not just by count. `json` continues it: `ValueSeed` deserializes
straight into `Value` with no intermediate `serde_json::Value` tree, and `borrowed_str_bytes` keeps
an unescaped string a slice of the message buffer (falling back to a copy only for a string serde
had to unescape, which genuinely lives elsewhere).

`statsd_in` does not. It builds attribute values with `attributes.insert(k, v)` on a `&str`, which
goes through `impl From<&str> for Value` → `Value::str` → `Bytes::from(String)` — a fresh copy of
bytes already sitting in the datagram. Then `build_event`'s `attributes.clone()` promotes each to a
shared `Bytes`, allocating again. **Two allocations per tag**, which is six of the eight in that
row above; the other two are a `Vec<Event>` per line plus one for the batch. The fix is to give
`statsd.rs` the `slice_of` treatment `syslog.rs` already has.
`statsd_tag_values_are_copied_not_sliced` pins this as a currently-true fact so it can't be
assumed away; when someone fixes it, that test should fail and be inverted.

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

What is copied: **everything else, per fan-out branch**. `Fanout::send` deep-clones the whole
`EventBatch` for every consumer but the last. For the nginx shape that is 4 allocations plus a
792-byte memcpy per event per extra branch — 272 ns, about 10% of the ingest chain.

That clone is not incidental; it is what makes branch isolation free. Two branches of a fan-out
never share an `Event`, so a mutation on one is structurally invisible to the other, with nothing
to design or maintain for that guarantee. `runtime.rs`'s
`a_mutation_on_one_fan_out_branch_is_invisible_to_the_sibling_branch` is the test that pins it.

### The `Arc<EventBatch>` copy-on-write change

Recommended, not implemented. Put `Arc<EventBatch>` on the channels and have each consumer do:

```rust
let batch = Arc::try_unwrap(batch).unwrap_or_else(|shared| (*shared).clone());
```

The properties that make this the right shape:

- **A single-consumer edge — the common case — becomes free.** `try_unwrap` succeeds when nobody
  else holds a reference, handing back the owned batch with no copy at all.
- **A read-only consumer on a fan-out branch stops copying entirely.** Every sink is read-only:
  both encoders take `&EventBatch`. In the reference config, the `tap`/`trimmed` fan-out would go
  from one full batch clone to one atomic increment.
- **A mutating branch pays exactly what it pays today.** There is no case where this is worse.
- **`Transform::process` does not have to change**, so no component is touched.
- Prior art: Vector's `LogEvent` is an `Arc<Inner>` with copy-on-write for the same reason.

Granularity matters here. Putting the `Arc` around the *batch* costs one atomic per batch. Putting
it around each `Event` would cost an allocation and an atomic *per event* — worse than what it
replaces for the single-consumer case. Don't do that.

A second, separable change: `Transform::process(&mut self, &Arc<Resource>, &mut Event) -> bool`
plus `Vec::retain_mut` in `run_transform` would remove one full 792-byte `Event` memcpy per node
hop and one `Vec` allocation per batch per node. Nothing is lost — the trait already can't emit
more than one event per input. Both changes deserve their own ADR; the signature one gets more
expensive to make with every transform that lands.

## 4. Interning: the bargain, and the risk

`logit_core::interner` maps every attribute key and metric name through a process-global
`lasso::ThreadedRodeo` to a 4-byte `Symbol`. This pays for itself several times over: `AttrMap`
compares and sorts `u32`s, `SeriesKey` hashes them, and the planned wire format's dictionary
encoding ([wire-protocol.md](wire-protocol.md)) is backed by the same table.

**`ThreadedRodeo` never evicts and never frees.** Every distinct string ever interned is retained
for the life of the process. That is fine for a bounded key space and is a genuine problem for an
unbounded one — and several strings reaching it come straight off the network:

- statsd metric names (`statsd.rs`), fully client-controlled.
- DogStatsD tag keys (`statsd.rs`), likewise.
- JSON object keys from log bodies (`json.rs`) — bounded for a fixed `log_format`, unbounded for
  any producer that puts an id in a key.

**Worse: `AttrMap::get` interns.** A lookup that *misses* still permanently adds its key to the
table. `kv_metrics` calling `attributes.get(field)` on an event that lacks that field is a normal,
expected path (nginx's `$upstream_response_time` is empty on non-proxied requests), and it grows a
process-global table. `lasso` has a non-interning `get()`; a lookup should use it. A key that isn't
in the table can't be on any event, so returning `None` without inserting is not just cheaper, it's
more correct.

### Threat model

A single client emitting `orders.<uuid>:1|c` grows the interner without bound, with no eviction, no
limit, and nothing that reports it. RSS climbs until the process is OOM-killed. `Spur` is a `u32`,
so the hard ceiling is ~4 billion strings — memory runs out first, by a wide margin.

### Why this is the hardest thing here to retrofit

`Symbol` is `Copy`, and `interner::resolve` **panics** on an unknown symbol. Every component in the
tree is written against the assumption that symbols are eternal and always resolvable. Changing the
representation later means touching `AttrMap`, `MetricRecord`, `SeriesKey`, the Lua proxy, and the
wire format's dictionary — the exact "expensive to change once components depend on it" shape this
pass exists to find.

**Recommended now** (cheap, no representation change): make `AttrMap::get` use a non-interning
lookup; expose `interner::len()`; warn on a throttled threshold. **Recommended before a
public-facing listener ships:** an ADR choosing between a bounded table with an inline-string
fallback for overflow, and narrowing what is allowed to be interned at all. Not a decision to make
in passing.

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
A byte- or event-aware bound is the eventual answer; recorded here and in
[known-gaps.md](../known-gaps.md) rather than designed now.

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
on threads it controls, so every bench here calls decoders, transforms, and encoders **directly**
rather than driving the tokio runtime and the channels between nodes. Anything measured across a
channel hop would report allocation numbers that are quietly wrong. What a full multi-node graph
costs end to end is a separate question needing a load generator, not a microbenchmark.

## 8. Recommendations

Ranked by measured value, not by how interesting they are.

### Now — contained, no design decision required

1. **Fix the InfluxDB encoder's allocation churn.** ~180 allocations/event, the single largest cost
   in the pipeline. `escape_tag`/`escape_measurement` returning `Cow<str>`, one reused line buffer,
   and a `raw_entry` series map. No effect on any other component.
2. **Make `AttrMap::get` non-interning.** A one-line change that closes an unbounded-growth path
   reachable from ordinary input.
3. **Give `statsd_in` the `slice_of` treatment.** Removes 6 of its 8 allocations per line and
   restores the zero-copy property the data model claims.
4. **`Box` the `DdSketch` and `SpanRecord`; enable smallvec's `union`.** Takes `Event` from 792
   bytes toward ~300 with no representational change.

### Next — worth an ADR each

5. **`Arc<EventBatch>` copy-on-write on channels.** Strictly no worse anywhere; frees every
   read-only fan-out branch. Second-order next to item 1, which is worth saying out loud given
   this was previously assumed to be the main event.
6. **`Transform::process(&mut Event) -> bool`.** Removes a 792-byte memcpy per node hop. Gets more
   expensive to decide with every transform that lands, so decide it early even if it's applied
   late.
7. **Bound the interner, or narrow what may enter it.** The one item here that is genuinely hard to
   retrofit. Needs a decision before a public-facing listener ships.
8. **`AttrMap` accessors keyed by `Symbol`,** eliminating the `resolve → intern` round trips in
   `json`, `keep`, and both encoders.

### Later — needs a reason first

9. **Re-pick `AttrMap`'s inline capacity** from a measured distribution of real attribute counts.
   8 is currently wrong in both directions: statsd carries 0–4, the nginx path carries 10 and
   spills anyway while still paying the full 384 bytes.
10. **Byte-aware channel bounds** (§5), before a TCP or file-tail input makes batch size unbounded
    in practice.
11. **Reduce the Lua boundary's 21 allocations/event** — cache the `process` function instead of a
    `_G` lookup per event, cache the `AttrsProxy` userdata instead of building one per attribute
    access, take `mlua::String` rather than `String` in the metamethods. Worth doing when a Lua
    stage is actually on a hot path.

### Settled by this work

[known-gaps.md](../known-gaps.md) recorded that the event proxy was chosen over full table
conversion on reasoning alone, with a benchmark outstanding. Measured: **1.51 µs/event through the
proxy against 2.63 µs for `to_table`** — the proxy is ~1.7× faster, and the gap widens for scripts
that touch few attributes, since `to_table` converts everything whether the script reads it or not.
The design decision in [lua-api.md](lua-api.md) stands, now with a number behind it.

## Open questions

- **What is the real attribute-count distribution** across the inputs `logit` will actually see?
  Every `AttrMap` sizing argument here is reasoning from two examples.
- **Does jemalloc actually flatten RSS for this workload?** Partly answered. A short soak of the
  reference config against the real nginx stack — 60,000 requests through
  `syslog_in → json → kv_metrics → {stdio_out, keep → aggregate → influxdb_out}` — held RSS at
  11.2 MB ± 3%, finishing marginally *below* where it started, with aggregated windows landing in
  InfluxDB throughout. That rules out a leak and shows pages are being returned. It does **not**
  isolate jemalloc from glibc: the same soak has not been run with `--no-default-features`, and
  the drift ADR 0015 is really about takes days, not minutes, to show up. The escape hatch exists
  so that comparison stays one build away.
- **Is there a compact `Event` representation** worth having — one that doesn't reserve span and
  sketch space on a bare log line — or is boxing the rare variants (item 4) enough? Worth revisiting
  only if item 4's numbers turn out not to be.
