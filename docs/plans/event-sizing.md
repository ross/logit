---
created: 2026-09-21
updated: 2026-09-21
---

# Enabling plan: `Event` sizing and allocation strategy — a measured bake-off, then an ADR

## Context

[`docs/design/memory.md`](../design/memory.md) §8 items 12–13 deferred two sizing decisions —
`AttrMap`'s inline capacity (8 × 48 B = 384 B of every 864 B `Event`) and `MetricList`'s — until a
real distribution of event shapes existed. [`data-shape-survey`](data-shape-survey.md) collected
one ([`docs/design/data-shapes.md`](../design/data-shapes.md)) and named this work as its
follow-up 1. This plan turns that data into an ADR governing `Event`'s sizing defaults and
allocation behaviour, optimizing CPU, allocations, and memory.

The question as first posed was "should the defaults handle 90% of situations without an
allocation, or 50%, or can `Event` learn the shape after a warm-up or take configured hints?" The
survey answers the first half by dissolving it — see "Target metric" below — and the code answers
the second half with a finding nobody had gone looking for: **`AttrMap` cannot be pre-sized at
all**, by anyone, even where the count is already in hand.

Stream key **`sizing`**. Branches `sizing/w0`…, a linear stack — each branch cut from its
parent's, its PR targeting that branch, retargeted to `main` once the parent merges
(`AGENTS.md`'s "Branches and PR titles"). PR stack only: nothing here merges on its own
initiative; Ross directs merging.

## Settled decisions

- **Scope is open, and measured.** Constants, exact/learned pre-sizing, and a change of
  representation (a shared key-set) all compete; the ADR picks what the numbers support.
- **Neither deployment wins.** The narrow-metric edge agent and the wide-log/span central collector
  (`docs/OVERVIEW.md`) are both first-class; a design that regresses either is out.
- **A bake-off on the perf VM precedes the ADR**, the way
  [ADR `native-wire-format-encoding`](../adr/native-wire-format-encoding.md)'s did. The operator
  runs `script/vm up`/`down` ([ADR `disposable-azure-perf-vm`](../adr/disposable-azure-perf-vm.md)).
  Workstation numbers are for iteration only, pinned to one core.
- **In scope: per-event attributes and resource attributes.** `MetricList`'s N, a ceiling on
  queue/RSS growth, and `Value`/string inlining are **recorded as future investigations** by the
  ADR, not decided by it.
- **No config knob unless learning measurably fails.** A hint like `expect_attributes:` is a
  per-component field for something the component can observe for itself.

## What the evidence says

### The survey: no single N

Inline capacity needed to hold a given share of events, read off `data-shapes.md` §2–§5 and the
capture tables behind it (nearest-rank percentiles; the `demo` producer excluded; none of this is
production traffic — `data-shapes.md` §0):

| Signal | 50% | 90% | 99% |
|---|---|---|---|
| Scraped metric series | 0–1 | 0–2 | 2–7 |
| statsd / collectd / Telegraf | 2–6 | 2–8 | max 8 |
| JSON logs through a logging library | 9–14 | 9–14 | max 15 (≈0% fit in 8) |
| OTLP spans (n = 114,551) | 8 | 17 | 17 (max 18) |
| Access/audit logs (desk-counted only) | fixed 8–34 per format; journald median 27, p90 33 | | |

Metrics want ≤6, logs ≥14, spans 17. Resource attributes are ≤6 everywhere except behind a
collector (median 17, p90 28, max 29, over a median batch of 5 events). Key-sets repeat heavily
per source — a logging library's top key-set is ~97% of its events — but not on a mixed OTLP
gateway (9% top-1, 196 sets, 264 distinct keys against a 64-entry per-parser `KeyCache`).

### The code: what N costs, and what nobody can do today

`AttrMap` is `SmallVec<[(Symbol, Value); 8]>` (`crates/logit-core/src/attrs.rs`): 48 B per entry,
sorted by `Symbol`, binary-search lookup, positional insert — so building a k-attribute map one
`insert` at a time moves O(k²) bytes. `size_of::<Event>()` is `48·N + 480`: 672 at N=4, 864 today,
1056 at 12, 1248 at 16, 1632 at 24.

- **`AttrMap`'s public surface has no `reserve`, no `with_capacity`, and no bulk build.** Every
  producer that knows the count discards it: the native decoder reads an exact count and then
  loops `insert` (`logit_proto::native::value::read_attr_map_at` — while the metric list beside it
  *does* reserve, `native::record::read_record_list_into`); OTLP has `kvs.len()`; `csv` and `regex`
  know their width at construction; `json`/`logfmt` know `scratch.len()` at the merge.
- Today: k ≤ 8 → 0 allocations; 9–16 → 1; 17–32 → 1 plus a growth realloc; 33–64 → 1 plus two.
  Every parsed structured log spills; wide spans and access logs chain. **Verified in W1** rather
  than inferred from smallvec's documentation
  (`attr_map_spills_to_double_its_inline_capacity_then_reallocs`,
  [`memory.md`](../design/memory.md) §1's ladder table): the spill goes to **twice** the inline
  capacity, not to an exactly-sized buffer, and every doubling after it is a `realloc` — so the
  `alloc` column is nearly blind to width past 9, which is the proxy problem "Target metric" below
  describes, now with a number behind it.
- **N is not paid per channel hop** — only the `EventBatch` handle moves. It is paid in the
  `Vec<Event>` buffer (cache density of every batch scan), in each copy-on-write
  `EventBatch::clone` (`memory.md` §3), and **everywhere `AttrMap` is embedded**: `Resource`,
  `Scope` (median 0 attributes — 392 B dead per batch), `SeriesKey` (+384 B per `aggregate` series
  at N=16), `SpanEvent`, `SpanLink`, and each boxed `Value::Map` (a pino-http record carries four).
- N ≥ 12 takes `Event` past 1024 B, which flips `Vec`'s minimum non-zero capacity from 4 to 1
  (three exact pins in `crates/logit-bench/tests/allocations.rs` depend on it) and makes
  byte-denominated bounds (`receive.batch_max_bytes`, `buffer.max_bytes`) trip before
  `batch_max_events`.

### The measurements: the ratio this all turns on has never been taken

`script/perf attribute` breaks time down per node, not per operation; `performance.md` disclaims
its older malloc/memcpy flamegraph percentages. [ADR
`minimize-allocations-over-event-size`](../adr/minimize-allocations-over-event-size.md) flags its
own premise — an allocation costs tens of ns, a few hundred bytes of copy single digits — as never
benchmarked here. Modelled, it stops holding at this magnitude: +384 B per event is ~12–25 ns of
copy across the moves and clones an event actually takes, the same order as the jemalloc pair it
saves. The one spot measurement on record ([`in-place-transform-process`](in-place-transform-process.md),
9-attribute logfmt) put the spill at ~0.6% of the process and dropping the spilled `Vec` at ~2.6% —
**the lever is probably small**, which is itself a result the ADR should be able to state with a
number.

Six fixtures `data-shapes.md` §7 asks for don't exist, including the commonest measured log shape
(12 flat attributes); the span fixture is knowingly under 8 attributes; and no existing comparison
bench clones, where VRL's own crossover moved from ~128 fields isolated to ~16 cloned
(`data-shapes.md` §6).

## Target metric

"X% of events allocation-free" pools a bimodal population into one number and optimizes a proxy —
allocation count — that inverts against copy cost at exactly this magnitude. The ADR is asked to
adopt three pinned invariants instead:

- **I1.** A metric-class event costs **0** attribute allocations. True today; defended.
- **I2.** A log, span, or access-log event costs **at most one, exactly-sized** attribute
  allocation per building stage — no realloc chain — at *any* width. False today past 16.
- **I3.** `size_of::<Event>()` changes only on a measured per-signal-class CPU µs/event result,
  clone-inclusive, on the perf VM, with no class regressing beyond noise.

## Bake-off arms

- **P — exact and learned pre-sizing** (lead candidate). `AttrMap::reserve`/`with_capacity` plus a
  bulk build from a sortable scratch (which also retires the O(k²) build). Count-knowing decoders
  reserve exactly; `json`/`logfmt`/`kv` reserve at the merge; declared-width components (`csv`,
  `regex`, `set`, `keep`, prometheus, collectd) at construction; anything else keeps a
  per-instance high-water hint on `&mut self`. No config, no representation change, and a wrong
  guess degrades to today's behaviour.
- **S — static N sweep**, N ∈ {0 (heap-only, `Event` just under 500 B), 4, 8, 16}, each with P applied.
  **Pre-registered kill criterion:** N=16 costing the statsd leg ≥5% CPU µs/event ends large
  static N.
- **E — per-embedding N** (`AttrMap<const N: usize>`): `Scope`, `Resource`, `SeriesKey`,
  `Value::Map`, `SpanEvent`, `SpanLink` each sized independently of `Event`'s own map. Resource
  sizing lands here — though `Resource` is `Arc`-shared per batch, so P alone may be the answer.
- **K — shared key-set plus a values vector**, bench-only. A per-source learned `Arc<KeySet>`,
  looked up once per event at the merge (a parser has the whole key-set in hand by then, so
  per-insert shape transitions aren't the cost they'd be in a general object model). The case
  against is real — interned 4-byte symbols already banked most of the win, and clone cost barely
  moves — so: **kill criterion**, less than a 10% win on the 12-attribute log's build+clone drops
  it. Must hold up on the 196-set gateway shape and preserve sorted-`Symbol` `iter()` and borrowed
  `&Value` (`attrs::merged`, `SeriesKey`, `keep`, the native encoder, Lua's `AttrsProxy`).
- **R — recycling the batch `Vec<Event>`** (864 KB at 1000 events: a jemalloc large allocation,
  freed cross-thread). Orthogonal to the rest; measured as an add-on. Not spill pooling — jemalloc's
  thread cache already covers small same-size allocations.

Not prototyped, reasons recorded in the ADR: per-batch arenas (`'static` events; `route` and
`aggregate` move events between batches), per-signal `Event` types
([ADR `multi-payload-events`](../adr/multi-payload-events.md)), columnar batches (a median OTLP
resource/scope group is 5 events).

## Workstreams

- **W0 — this plan.**
- **W1 — fixtures and a baseline. Landed.** `crates/logit-bench/src/fixtures.rs` gained the six
  survey-derived shapes (12-attribute flat JSON log, pino-http nested record, 17-attribute server
  span, 30-field access log, 3-record collectd event, 17-attribute resource over a 5-event batch),
  each citing the survey row it models and each stating what is modelled rather than captured.
  Divan benches for build, lookup (hit and miss), mutate, **clone**, a 1000-event scan, native
  encode/decode, plus `process_batch` (the `retain_mut` path) and `route_batch` — `drain_inbox` is
  reachable but returns only when its inbox closes, so it stays an allocation count in
  `allocations.rs` rather than a bench. `allocations.rs` pins build/clone/encode/decode at today's
  constants; `perf/scenarios/json-parse-{app-log,nested-log,access-log}.yaml` render the same
  bodies through `generate_in`. smallvec's growth policy verified first (see above).
  `memory.md` §1/§2/§7/§8 updated.
  - Two results worth carrying into W2–W5: a spilled `AttrMap` clones in **one** allocation
    whatever its width, so allocation count barely separates the 12-attribute and 30-attribute
    logs; and the *nested* 10-attribute pino-http record costs **five** to build and five to clone,
    more than either. The proxy this plan warns about inverts on measured shapes, not just in
    principle.
- **W2 — the unmeasured ratio.** A micro-bench of jemalloc alloc/free against an `Event` move at
  each candidate size; `script/perf flamegraph` on W1's scenarios for a current malloc / realloc /
  memmove / clone share; `perf stat` cache misses over a 1000-event batch scan. May prune arms.
- **W3 — the arms**, under `crates/logit-bench/src/bakeoff/` beside `wire_mirror.rs`. P is small
  enough to build for real in `logit-core`; S and E through a const-generic mirror; K and R
  bench-only.
- **W4 — the VM session.** `script/vm build <ref>` per real-binary arm, `script/perf run
  --logit-bin` / `compare` across every scenario class; summary into `docs/design/performance.md`.
- **W5 — the ADR**, `docs/adr/event-sizing-and-allocation-strategy.md`: the decision and its
  numbers, the arms that lost and why, I1–I3, an amendment note on
  `minimize-allocations-over-event-size`, and the future-investigations list — `MetricList` N
  (collectd: 17% of events carry 2), a queue/RSS ceiling against `size_of::<Event>()` and the
  byte-denominated bounds, `Value`/string inlining, a `KeyCache` on the OTLP decode path, a
  wire-level key-set dictionary for `logit_proto::native`, and the captures `data-shapes.md` §7
  still wants. Closes `memory.md` §8 items 12–13; updates `data-shapes.md` §7 and `known-gaps.md`.
- **W6+ — landing the chosen design**, with `type_sizes.rs`/`allocations.rs` constants and
  `memory.md`'s tables changed in the same commits. Planned in detail after W5.

## Verification

- `script/check` per PR and `script/cibuild` before each opens; `script/validate` covers the new
  scenarios.
- W1: the new pins pass at today's constants, and each fixture's width matches its cited row.
- W3/W4: every arm reports allocations/event, bytes/event, and ns/op for build, clone, and scan,
  per signal class; VM runs pinned and repeated; kill criteria applied as registered above.
- W5: an independent, number-by-number fact-check of the ADR against W4's output — the survey's
  own found ten wrong numbers in a careful draft.
