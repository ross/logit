---
created: 2026-09-21
updated: 2026-09-21
---

# Event sizing: `AttrMap`'s inline capacity stays 8, and attribute maps are not pre-sized

## Status
Accepted

## Context

[`docs/design/memory.md`](../design/memory.md) §8 deferred two sizing questions — `AttrMap`'s
inline capacity (8 entries × 48 B, 384 B of every 864 B `Event`) and whether to go further —
until a real distribution of event shapes existed. [`docs/design/data-shapes.md`](../design/data-shapes.md)
supplied one, and [`docs/plans/event-sizing.md`](../plans/event-sizing.md) turned it into a
bake-off: fixtures and scenarios at the surveyed widths, instruments for the allocation-versus-copy
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
