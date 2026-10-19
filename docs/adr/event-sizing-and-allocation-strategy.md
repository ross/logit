---
created: 2026-09-21
updated: 2026-10-18
---

# Event sizing: `AttrMap`'s inline capacity stays 8, and attribute maps are not pre-sized

## Status
Accepted

## Context

[`docs/design/memory.md`](../design/memory.md) §8 deferred two sizing questions — `AttrMap`'s
inline capacity (8 entries × 48 B, 384 B of every 864 B `Event`) and whether to go further —
until a real distribution of event shapes existed. [`docs/design/data-shapes.md`](../design/data-shapes.md)
supplied one, and a plan turned it into a bake-off: fixtures and scenarios at the surveyed widths, instruments for the allocation-versus-copy
ratio, candidate designs as real binaries and as bench mirrors, all measured on the perf VM
([ADR `disposable-azure-perf-vm`](disposable-azure-perf-vm.md)) before anything was decided.

The survey's headline is that per-event width is bimodal by signal: scraped metrics need 0–2
inline slots to hold 90% of events, library JSON logs 9–14 (≈0% fit in 8), spans 17, access logs
15–34. No single N covers the population, so the question the plan started from — "should the
defaults be allocation-free for 90% of events, or 50%?" — has no good answer in its own terms. The
plan replaced it with per-signal-class invariants and a lead candidate: keep N small, and make the
one allocation a wide event needs exactly sized and free of a realloc chain (pre-sizing, "arm P").

**The measurements did not support the lead candidate, or any other change to `Event`.** That is
the decision this ADR records, with the numbers, so the question is not re-opened on intuition.

All figures below are CPU µs/event, median of 6 (two interleaved rounds × 3 repeats) on the perf
VM (8 × AMD EPYC 9V74, 2026-09-21); run-to-run spread was 0.4–6%, round-to-round drift nil.
[`docs/design/performance.md`](../design/performance.md) §8 has the full tables.

## Decision

1. **`AttrMap`'s inline capacity stays 8.** Measured on both sides, against an otherwise identical
   tree:

   | Scenario | N=0 | N=4 | N=16 |
   |---|--:|--:|--:|
   | `passthrough` | +16.6% | +7.1% | +10.9% |
   | `fanout` | +12.9% | +9.6% | +8.2% |
   | `route` | +6.9% | +7.7% | +18.0% |
   | `aggregate` | −18.3% | −8.2% | +6.1% |
   | `json-parse-app-log` (12 attributes) | +9.7% | +4.5% | −6.2% |
   | `json-parse-access-log` (30) | +3.6% | +2.7% | −3.0% |

   N=16 buys the wide-log legs 3–10% and costs every narrow leg 6–18% — past the plan's
   pre-registered kill criterion (≥5% on a narrow-metric leg). N=0 and N=4 cost the narrow legs
   too, because even a one-attribute event then reaches for the heap. Neither deployment shape in
   `docs/OVERVIEW.md` may pay for the other, so 8 stands.

2. **Attribute maps are not pre-sized, and there is no bulk-build path.** Producers keep building
   maps with per-key `insert_sym`. `AttrMap` gains no `reserve`/`with_capacity`/bulk API. On the
   `json` scenarios, against the per-key loop on the same tree:

   | `json`'s merge | 12 attributes | 30 attributes |
   |---|--:|--:|
   | loop + `reserve_exact(n)` up front | +7.6% | +6.4% |
   | loop + `reserve(n)` (next power of two) up front | +8.5% | +4.6% |
   | one-sort bulk build, exact reservation, in-order fast path | +9.6% | +14.8% |
   | same, power-of-two reservation | +9.5% | +12.8% |

   Against `main`, the bulk build cost the `json` scenarios 8–17% end to end (8.1–16.7% and
   8.2–16.1% in two separate passes, with its in-order fast path; 12–20% before it), gained
   `logfmt-parse` 6–7%, and left `native-relay` flat; one 6–7% win at one call site does not
   carry ~300 lines of subtle code in `logit-core`. Two facts explain most of why a design that
   won its micro-benchmark lost in the pipeline:

   - **Real keys arrive already sorted.** The interner numbers keys in first-seen order, so a
     source with a stable key order — every logging library — presents keys in ascending `Symbol`
     order, and each `insert_sym` is a binary search plus an *append*. The O(k²) shuffle the
     micro-bench measured (keys fed in a fixed unsorted order) does not occur.
   - **Reserving early loses to growing late**, by 5–9%, whether the reservation is exact or
     rounded. The size class is not the variable; *when* the map leaves its inline storage is. Why
     is not established — the VM exposes no PMU — and is recorded as open rather than guessed at.

3. **The allocation count of an event is not an optimization target in itself.** The plan's
   invariant "a wide event costs at most one exactly-sized allocation, no realloc chain" is
   achievable and was achieved; it made the pipeline slower. What stays pinned is what was already
   pinned: exact `size_of`s and exact per-stage allocation counts, as regression tripwires, not as
   a score. A change that moves one must carry an end-to-end VM measurement per signal class, not
   a micro-benchmark.

4. **[ADR `minimize-allocations-over-event-size`](minimize-allocations-over-event-size.md)'s
   premise now has numbers**, and they are narrower than it assumed. Under jemalloc on the VM: an
   alloc/free pair is ~5 ns on the thread-cache fast path, 6–25 ns per block with 64 live, and
   31–92 ns per block when freed on another thread (the real lifecycle of a spilled map); a realloc
   growth step is 53–81 ns; moving an `Event` costs ~7 ns more per +384 B and cloning a batch ~9 ns
   more per event per +384 B; a 1000-event batch scan is flat in `size_of::<Event>()`. "An
   allocation costs tens of nanoseconds, a few hundred bytes of copy single digits" holds for
   cross-thread frees and overstates the fast path by several times. That ADR's direction is
   unchanged; its tie-breaker should no longer be applied without measuring.

## Alternatives considered

- **Larger static N (12/16/24).** Rejected on the table in (1). Also pushes `Event` past 1024 B,
  which flips `Vec`'s minimum non-zero capacity and makes byte-denominated bounds
  (`receive.batch_max_bytes`, `buffer.max_bytes`) trip before event-count ones.
- **Smaller or zero static N.** Rejected on the same table. Its one win is real and points
  elsewhere — see "Consequences".
- **Pre-sizing and a one-sort bulk build (arm P).** Built for real, reviewed, adopted at every
  producer that knew its width, measured, rejected in (2). An exactly-sized `AttrMap::clone` came
  out of the same work and measured CPU-neutral (±0.6%); it saves bytes, not time, and is not
  adopted either — nothing here changes `logit-core` on the strength of a number that isn't a
  clear win.
- **Learned or configured capacity hints.** Moot: every producer already has its width in hand
  (a count or a scratch buffer) except Lua tables, and using the width is what (2) rejects. A
  config hint (`expect_attributes:`) would have been the same mechanism with a knob.
- **Recycling the batch `Vec<Event>`.** Dropping a whole 0.5–1.6 MB batch buffer measured ~0.14 ns
  per event. Nothing to win.
- **Per-batch arenas, per-signal `Event` types, columnar batches.** Not prototyped: events are
  `'static` and move between batches (`route`, `aggregate`);
  [ADR `multi-payload-events`](multi-payload-events.md) rules out per-signal types; the median
  OTLP resource/scope group is 5 events.
- **A shared key-set plus a values vector, a thin unboxed `Value::Map`, a faster clone path.**
  Bench-only mirrors (`crates/logit-bench/src/bakeoff/attr_arms/`). On the VM: the key-set beat the
  bulk build by 19% on the 12-attribute log's build+clone — and lost by 58% on the survey's
  196-key-set gateway under a 64-entry cache; the thin nested map cloned the pino-http record 56%
  faster; no clone candidate won at realistic string shares. **None of this is evidence for
  adoption.** The central lesson of this workstream is that a micro-benchmark win (arm P had one)
  did not predict the pipeline; these numbers say where a future prototype might look, no more.

## Bake-off arms and instruments

The bake-off replaced "X% of events allocation-free", which pools a bimodal population into one
number and optimizes allocation count, a proxy that inverts against copy cost at this magnitude,
with three pinned invariants:

- **I1.** A metric-class event costs 0 attribute allocations. True today; defended.
- **I2.** A log, span, or access-log event costs at most one attribute allocation, sized to fit,
  per building stage, with no realloc chain, at any width. False today past 16 attributes.
- **I3.** `size_of::<Event>()` changes only on a measured per-signal-class CPU µs/event result,
  clone-inclusive, on the perf VM, with no class regressing beyond noise.

The arm letters that bench sources and comments cite are:

- **P: exact and learned pre-sizing.** `AttrMap::reserve`/`with_capacity` plus a bulk build from a
  sortable scratch, which also retires the O(k²) build. Count-knowing decoders reserve their count;
  `json`/`logfmt`/`kv` reserve at the merge; declared-width components (`csv`, `regex`, `set`,
  `keep`, prometheus, collectd) reserve at construction; anything else keeps a per-instance
  high-water hint. No config, no representation change, and a wrong guess degrades to today's
  behavior.
- **S: static N sweep.** N of 0 (heap-only, `Event` under 500 B), 4, 8, and 16, each with P
  applied. Pre-registered kill criterion: N=16 costing the statsd leg 5% or more CPU µs/event ends
  large static N.
- **E: per-embedding N** (`AttrMap<const N: usize>`). `Scope`, `Resource`, `SeriesKey`,
  `Value::Map`, `SpanEvent`, and `SpanLink` each sized independently of `Event`'s own map. No kill
  criterion; the const-generic question was answered as an assessment of how many signatures and
  crates it would ripple through, not an implementation. `Resource` is `Arc`-shared per batch, so P
  alone may be the answer for it.
- **K: shared key-set plus a values vector**, bench-only. A per-source learned `Arc<KeySet>`,
  looked up once per event at the merge. The case against: interned 4-byte symbols already banked
  most of the win, and clone cost barely moves. Pre-registered kill criterion: a win under 10% on
  the 12-attribute log's build plus clone, against P's bulk build, drops it. It also had to hold
  on the 196-key-set gateway shape and preserve sorted-`Symbol` `iter()` and borrowed `&Value`
  (`attrs::merged`, `SeriesKey`, `keep`, the native encoder, Lua's `AttrsProxy`).
- **R: recycling the batch `Vec<Event>`** (864 KB at 1000 events: a jemalloc large allocation,
  freed cross-thread). Orthogonal to the rest and measured as an add-on. Not spill pooling, because
  jemalloc's thread cache already covers small same-size allocations.
- **C: the clone path**, added during the bake-off. The clone, not the build, is what a fan-out
  pays, and `AttrMap::clone` measured an order of magnitude over its byte cost. Four candidate
  replacements for `SmallVec`'s element-at-a-time clone: a write loop sized to fit, a one-branch
  scalar clone, a detected bitwise copy, and a per-map "every value is a scalar" flag, plus floor
  and measurement-shape controls. No kill criterion: it either explains the clone cost with a fix
  or shows the number was an artifact.

Arms K, E, and C are bench-only mirrors under `crates/logit-bench/src/bakeoff/attr_arms/`, built
on the native-wire-format bake-off's pattern: a local mirror of the shipped type, every
simplification stated in the mirror's module doc, nothing changed in `logit-core`.
`crates/logit-bench/tests/attr_arms.rs` gates the timings: every arm must produce the same sorted
`(Symbol, Value)` sequence the shipped `AttrMap` does from the same input, duplicate keys included
(last write wins). Writing it showed that a bulk build equals repeated `insert` only if it sorts
stably and collapses each run of equal keys to its last entry, which `size_vs_alloc.rs`'s simpler
`append_then_sort` mirror does not do.

**Fixtures (W1).** `crates/logit-bench/src/fixtures.rs` holds six survey-derived shapes: a
12-attribute flat JSON log, a pino-http nested record, a 17-attribute server span, a 30-field access
log, a 3-record collectd event, and a 17-attribute resource over a 5-event batch. Each cites the
survey row it models and says what is modelled rather than captured.
`perf/scenarios/json-parse-{app-log,nested-log,access-log}.yaml` render the same bodies through
`generate_in`. Two results shaped the later work:

- A spilled `AttrMap` clones in one allocation whatever its width, so allocation count barely
  separates the 12-attribute and 30-attribute logs.
- The nested 10-attribute pino-http record costs five allocations to build and five to clone, more
  than either flat log. The allocation-count proxy inverts on measured shapes, not only in
  principle.

**Instruments (W2).** None changed production code.

- `crates/logit-bench/benches/size_vs_alloc.rs` isolates the allocation-versus-copy ratio under real
  jemalloc installed as that bench binary's own `#[global_allocator]` ([ADR
  `jemalloc-global-allocator`](jemalloc-global-allocator.md)), not `benches/pipeline.rs`'s
  `divan::AllocProfiler`, which wraps the system allocator and counts every request from inside the
  timed region. That is the right trade for an allocation-count bench and the wrong one when the
  cost of an allocation is the question, so the file reports no allocation column. Its six groups
  are an alloc/free pair at the sizes `AttrMap`'s ladder asks for (432, 576, 768, 1440, 1536, 3072
  bytes), same-thread and cross-thread in steady state; the 768, 1536, 3072 realloc ladder against
  one exact allocation; today's sorted `insert` against an append-then-sort mirror at k of 9, 12,
  17, and 30; one `Event`-sized move at each candidate `size_of::<Event>()` (496 to 1632, a padded
  struct); a 1000-event batch scan at each stride over 8 rotated batches, plus dropping and cloning
  the batch buffer; and `AttrMap::clone` inline (8) against spilled (9, 12).
- `logit-perf flamegraph --folded PATH` tees `inferno-collapse-perf`'s output beside the SVG, and
  `perf/folded_share.py` reports the share of samples whose stack contains a jemalloc, memcpy,
  `AttrMap`/`SmallVec`, drop-glue, clone, interner, or `serde_json` frame. It counts containment
  once per stack, so the groups overlap.

To reproduce on the perf VM (`script/vm up`, then in the checkout there):

```sh
# 1. The micro-benches, pinned to one core.
taskset -c 2 cargo bench -p logit-bench --bench size_vs_alloc

# 2. Share of samples under the allocator, per scenario.
for s in json-parse-app-log json-parse-nested-log json-parse-access-log passthrough; do
    script/perf flamegraph --scenario "$s" --folded "perf/results/$s.folded"
done
python3 perf/folded_share.py perf/results/*.folded
python3 perf/folded_share.py perf/results/passthrough.folded --explain alloc

# 3. Arms C, E, and K, then their allocation counts.
taskset -c 2 cargo bench -p logit-bench --bench attr_arms
cargo nextest run -p logit-bench --test attr_arms --no-capture
```

Two caveats apply to anything these produce:

- `perf stat` measures the whole process, and divan's per-stride setup (8 batches of 1000
  elements) dominates it. Attributing cache counters to the scan alone needs `perf record -e
  cache-misses` plus symbol filtering. Azure's guest exposes no virtualized PMU ([ADR
  `disposable-azure-perf-vm`](disposable-azure-perf-vm.md)), so on the VM the cache counters can
  read `<not supported>`, and the stride comparison has to come from the micro-bench's own timings.
- The `memmove` column reads 0.00% on every capture, because LLVM inlines `Event`-sized copies into
  SIMD stores attributed to their caller. The release binary never calls libc `memcpy`, so the copy
  side of the ratio is invisible to a flamegraph by construction. That is why the micro-bench
  exists.

Pre-sizing was built for real on two branches (`sizing/w3a`, #270, and `sizing/w3c`, #272) and
both are closed unmerged. An independent review of the first found no correctness bug but found
that eager reservation spilled maps that fit inline, which became a lazy reservation. Neither
survived the VM session.

## Consequences

- `Event`, `AttrMap`, and every producer are unchanged. `memory.md` §8's two deferred sizing items
  close as "measured; no change".
- What this workstream leaves behind is the means to ask again properly: six survey-derived
  fixtures and three `json-parse-*` scenarios at measured widths, `benches/size_vs_alloc.rs` under
  real jemalloc, `logit-perf flamegraph --folded` with `perf/folded_share.py`, and the bench-only
  arms. The allocation baseline for the survey shapes stays pinned.
- **A sizing change now needs a real binary measured per signal class on the VM.** Micro-benchmarks
  choose what to prototype; they do not decide. Fixtures for attribute maps should present keys in
  interning order unless they are specifically modelling a source that doesn't.
- **Future investigations**, none started:
  - **`SeriesKey`'s embedded `AttrMap`.** `aggregate` ran 18% faster at N=0 and 8% faster at N=4 —
    the one end-to-end result here that points at a win. It is evidence about the map embedded in
    each series key (and by extension `Scope`, median 0 attributes, 392 B per batch), not about
    `Event`'s own N. Worth its own small workstream: a real binary, the VM, then a decision.
  - Why early reservation loses to late growth (needs a box with a PMU).
  - Cross-thread free cost, 3–15× the same-thread pair, as a lever in its own right.
  - `MetricList`'s inline capacity (collectd: 17% of events carry 2 records); `Value`/string
    inlining; a `KeyCache` on the OTLP decode path; a wire-level key-set dictionary for
    `logit_proto::native`; and the captures `data-shapes.md` §7 still wants — none of this
    survey's traffic was production traffic.
