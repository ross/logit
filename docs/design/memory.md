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
| Mixed (the nginx shape — **the only one measured**) | all three | least of any shape |

Two consequences that matter for how much weight to put on §8:

- **Everything in §1 (sizing) applies to every workload**, because `Event`'s 792 bytes are paid on
  every hop whatever the event carries. Every shape above wastes 180-340 bytes on payloads it never
  holds. That argument doesn't depend on the fixture at all.
- **Several specific *fixes* are workload-dependent, and one flips sign** depending on the mix. A
  change that is free for a logs-only pipeline can cost a metrics-only one an allocation per event.
  §8 marks which is which; don't read the ordering as settled for a workload the fixtures don't
  cover.

`crates/logit-bench/src/fixtures.rs` has no logs-only fixture and **no span fixture at all**, so
nothing here has measured the span path. Broadening that matrix — small synthetic inputs and
directly-constructed events, no external services — is the prerequisite for settling the items §8
flags as workload-dependent.

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

Not done — this pass measures rather than optimizes. Sized here so the trade is visible, **and the
trades are not all in the same direction** (see §0):

| Change | Saves | Real cost | Verdict |
|---|---:|---|---|
| smallvec's `union` feature | 16 B | none — it's a feature flag | safe everywhere |
| `Box` `SpanRecord` (`Option<Box<_>>` is 8 B) | 128 B | +1 alloc per event **that carries a span** | free for logs/metrics; unmeasured for traces |
| `Box` the `DdSketch` in `MetricKind::Distribution` | ~168 B | +1 alloc per **distribution metric created** | wins for logs/traces, loses for distribution-heavy metrics |
| Re-pick `AttrMap`'s inline capacity | up to 192 B | spills for events in the new gap | depends on the attribute-count distribution |

All four together take `Event` from 792 bytes to roughly 290. The first two are worth taking on
their own merits; the last two need evidence the fixtures don't currently provide.

**`Box`ing the `DdSketch` is not free, and an earlier draft of this document said it was.** The
reasoning was that a sketch "already allocates" — it doesn't, at construction.
`sketches_ddsketch`'s `Store::new` starts with `Vec::new()`, and the bins are allocated on the
first `add`. The measurement confirms it: `kv_metrics` costs 3 allocations for 4 metrics, which is
one `MetricList` spill plus exactly **one** bins `Vec` per distribution. Boxing makes that two. In
a `kv_metrics` or statsd-timing pipeline, distributions are the common case, not the rare one — so
this trades 168 bytes per event for an extra allocation per distribution, and which side wins
depends entirely on the workload.

**`AttrMap`'s inline capacity is the largest single term (384 B) and the easiest to get wrong.**
Dropping 8 → 4 saves 192 bytes and looks free against the two shapes measured — statsd carries 0-4
tags (still inline), the nginx path carries 10 (spills either way). But **a plain syslog pipeline
with no JSON parsing carries 4-6 attributes**, which is inline at 8 and spilling at 4: that change
would add an allocation per event to an entire workload class the fixtures don't cover. The real
input is a distribution of attribute counts across representative shapes, which doesn't exist yet.

Worth noting while re-picking it: a `SmallVec` that has spilled still occupies its full inline
footprint, so for a consistently-wide workload a plain `Vec` (24 B plus one allocation) is strictly
better than a `SmallVec` that always spills. "Smaller inline capacity" and "no inline capacity" are
both on the table; the distribution decides.

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
| `influxdb_out` encode 100 events | **30** | ~0.3/event — see below |

And the corresponding times:

| Stage | fastest | per event |
|---|---:|---:|
| `syslog_in` decode, 100 lines | 22.3 µs | 223 ns |
| `statsd_in` decode, 100 lines | 34.7 µs | 347 ns |
| `json` | 584 ns | 584 ns |
| `kv_metrics` | 250 ns | 250 ns |
| `keep` | 323 ns | 323 ns |
| `aggregate` absorb | 561 ns | 561 ns |
| **full ingest chain** | **2.07 µs** | ~484k lines/s/core |
| `Event::clone` (nginx / statsd / distribution) | 232 / 85 / 52 ns | |
| `stdio_out` encode, 100 events | 188 µs | 1.88 µs |
| `influxdb_out` encode, 100 events | 153 µs | 1.53 µs |

> Every row above comes from **one** `script/bench` run, deliberately: an earlier run of the same
> benchmarks on a busier machine was uniformly ~20% slower (`json` 748 ns, `keep` 404 ns, the full
> chain 2.61 µs), so mixing rows across runs would invent differences that aren't there. Compare
> rows within the table freely; treat absolute values as this machine on this day.

### The headline result: the output encoder *was* the bottleneck, and has been fixed

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

**Now 30 allocations per 100-event batch, from 18,024 — a 600× reduction, and 2.6× faster** (1.53 µs
per event against 4.96 µs, adjusted for the ~20% run-to-run shift the note above describes).
`influxdb_out` is now *faster* than the `stdio_out` debug sink, where it used to be 2.1× slower.
The changes were mechanical and stayed inside `influxdb.rs`: escape and format straight into reused
buffers held on the encoder, merge-join the resource and event attribute maps instead of cloning
and re-inserting, borrow the series key for the lookup and only allocate it on a miss, and reuse
the path-compression scratch buffer.

What's left is per-*batch*, not per-event: one `Bytes` for the finished body, one `String` key per
distinct series on first sighting, and growth of the per-series timestamp maps. Nothing scales with
event count any more, which is the property to protect —
`crates/logit-bench`'s `influx_encode_100_events` will catch it if that stops being true.

**With that gone, the ingest chain is the cost again**: 11 allocations and 2.07 µs to take one
access-log line from datagram to aggregated window, against 0.3 allocations and 1.53 µs to encode
it. `json` is now the single most allocation-hungry stage at 7, and `Event::clone`'s 4 per extra
fan-out branch is no longer dwarfed by anything. The recommendations in §8 are ordered accordingly.

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

If a diagnostics facility lands anyway (the `tracing` migration in
[known-gaps.md](../known-gaps.md)), an `interner::len()` gauge is nearly free to expose at that
point and would make this observable instead of silent. Worth doing *then*, not worth a change of
its own now.

### What is *not* the risk: failed lookups

`AttrMap::get` interns rather than doing a lookup-only probe, so in principle a miss adds a key no
event carries. In practice this is not a growth path. There are exactly three production `get` call
sites in the tree:

- `kv_metrics.rs` (twice), keyed by `m.field` -- a **config** string, fixed at startup. Hit or
  miss, it's interned once and never again.
- `proxy.rs`'s `AttrsProxy::__index`, keyed by whatever a Lua script indexes -- normally a literal
  in the script, so also a bounded set. Unbounded only for a script that builds keys out of event
  data, which is unusual and is trusted config besides.

So `AttrMap::get`'s interning is a **CPU** problem, not a memory one: it pays a hash plus a
concurrent-map probe on the hot path for a lookup that could be cheaper, and `lasso`'s
non-interning `get()` would avoid it. Worth fixing, but as an efficiency cleanup and a
defence-in-depth tightening -- not as the memory fix this section is really about. (An earlier
draft of this document had that backwards.)


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
   batch to 30, and 2.6× faster (§2). Was the single largest cost in the pipeline; now smaller than
   ingest. Workload-independent: it helps any config with an `influxdb_out`.

### Safe regardless of workload mix

No tradeoff to measure — each is a strict improvement for every shape in §0's table.

2. **Enable smallvec's `union` feature.** 16 bytes off every `Event` for a one-line feature flag.
3. **Make `AttrMap::get` non-interning.** Drops a hash and a concurrent-map probe from every
   attribute lookup on the hot path, in every component that reads an attribute. Also closes a
   theoretical interner growth path, though every production call site is config- or script-keyed
   today, so that half is defence in depth rather than a live bug (§4).
4. **`Arc<EventBatch>` copy-on-write on channels** (§3). Strictly no worse anywhere; frees every
   read-only fan-out branch. Ranks higher than it did on nginx evidence alone, for two reasons: it
   is entirely payload-shape-independent, and it is worth *most* to the workload least represented
   in the fixtures — a `SpanRecord` holds `Vec<SpanEvent>`/`Vec<SpanLink>` and each `SpanEvent`
   carries its own 400-byte `AttrMap`, so span-bearing events are far more expensive to deep-clone
   than anything measured here. Needs an ADR, not because it's contentious but because it changes a
   trait-adjacent contract.
5. **Trim `json`'s 7 allocations** — the most allocation-hungry single stage, and broader than its
   nginx origin suggests: JSON-bodied log lines are common to most log-oriented pipelines, not a
   quirk of this one. Most of it is the intermediate `AttrMap` built so a malformed object can't
   half-populate the event, plus the spill when the merged set passes 8 entries. The intermediate is
   load-bearing for correctness, so this wants a checkpoint-and-rollback or a reusable scratch map
   rather than deletion.
6. **Give `statsd_in` the `slice_of` treatment.** Removes 6 of its 8 allocations per line and
   restores the zero-copy property the data model claims (§2). Strictly an improvement; narrower
   reach than the items above only because it helps statsd inputs specifically.

### Blocked on a broader fixture matrix

Each of these could regress a workload the current fixtures can't see. §0 describes what's missing;
the prerequisite is small synthetic inputs and directly-constructed events covering logs-only,
wide-JSON logs, metrics-only, distribution-heavy metrics, and spans — **no external services**.

7. **`Box` `SpanRecord`.** 128 bytes off every event, free for logs-only and metrics-only. Needs a
   span fixture to confirm the cost for a tracing pipeline is what it looks like (one allocation on
   an event that already allocates for its span's `events`/`links`).
8. **`Box` the `DdSketch`.** 168 bytes off every event, but +1 allocation per distribution created
   — a win for logs and traces, a loss for distribution-heavy metrics. Needs both shapes measured
   before it can be decided at all (§1).
9. **Re-pick `AttrMap`'s inline capacity.** The largest single term at 384 bytes, and the easiest
   to get wrong: 8 → 4 looks free against the measured shapes and would add an allocation per event
   to a plain-syslog pipeline. Needs an attribute-count distribution across shapes, and should
   consider dropping inline storage entirely for consistently-wide workloads (§1).

### Later — needs a reason first

10. **`Transform::process(&mut Event) -> bool`.** Removes a 792-byte memcpy per node hop. Gets more
    expensive to decide with every transform that lands, so decide it early even if applied late.
11. **`AttrMap` accessors keyed by `Symbol`,** eliminating the `resolve` → `intern` round trips
    still left in `json`, `keep`, and `stdio_out`. (`influxdb_out`'s are gone — the merge-join in
    `render_tag_suffix` removed them.)
12. **Byte-aware channel bounds** (§5), before a TCP or file-tail input makes batch size unbounded
    in practice.
13. **Reduce the Lua boundary's 21 allocations/event** — cache the `process` function instead of a
    `_G` lookup per event, cache the `AttrsProxy` userdata instead of building one per attribute
    access, take `mlua::String` rather than `String` in the metamethods. Worth doing when a Lua
    stage is actually on a hot path.
14. **Give `stdio_out` the same treatment `influxdb_out` just got** — at ~18 allocations per event
    it is now the more wasteful of the two encoders, and the fix is nearly the same. Deliberately
    low: it's a debug sink for a human reading a terminal, not a throughput path.
15. **~~Bound the interner~~ — accepted as-is, see §4.** Listeners are private, so the namespace is
    user-controlled; the metric store and `logit`'s own aggregation window both fail earlier and
    harder under the same abuse. Revisit only if a listener stops being private.

### Settled by this work

[known-gaps.md](../known-gaps.md) recorded that the event proxy was chosen over full table
conversion on reasoning alone, with a benchmark outstanding. Measured: **1.51 µs/event through the
proxy against 2.63 µs for `to_table`** — the proxy is ~1.7× faster, and the gap widens for scripts
that touch few attributes, since `to_table` converts everything whether the script reads it or not.
The design decision in [lua-api.md](lua-api.md) stands, now with a number behind it.

## Open questions

- **What is the real attribute-count distribution** across the inputs `logit` will actually see?
  Every `AttrMap` sizing argument here reasons from two examples, and §1 shows that's not enough to
  decide even the direction of the change. The largest single term in `Event` is blocked on this.
- **What do the unmeasured workload shapes actually cost?** Logs-only, wide-JSON logs,
  distribution-heavy metrics, and traces are all in scope (§0) and none are in the fixtures. Three
  of §8's sizing items can't be settled until they are, and the span path has no coverage at all.
- **Does jemalloc actually flatten RSS for this workload?** Partly answered. A short soak of the
  reference config against the real nginx stack — 60,000 requests through
  `syslog_in → json → kv_metrics → {stdio_out, keep → aggregate → influxdb_out}` — held RSS at
  11.2 MB ± 3%, finishing marginally *below* where it started, with aggregated windows landing in
  InfluxDB throughout. That rules out a leak and shows pages are being returned. It does **not**
  isolate jemalloc from glibc: the same soak has not been run with `--no-default-features`, and
  the drift ADR 0015 is really about takes days, not minutes, to show up. The escape hatch exists
  so that comparison stays one build away.
- **Is there a compact `Event` representation** worth having — one that doesn't reserve span and
  sketch space on a bare log line? Boxing the rare variants (§8 items 7-8) is the cheap answer, but
  it only pays where the variant really is rare, and "rare" is workload-dependent: a sketch is the
  common case in a statsd-timing pipeline and absent entirely from a logs-only one. If the broader
  fixtures show no single boxing choice wins across shapes, that's the signal this needs a
  representational answer rather than a tuning one.
