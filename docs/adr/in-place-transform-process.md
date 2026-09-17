---
created: 2026-09-17
updated: 2026-09-17
---

# `Transform::process` transforms in place: `&mut Event -> bool`, not `Event -> Option<Event>`

## Status

Accepted

## Context

`Transform::process` (`crates/logit-pipeline/src/transform.rs`) took an owned `Event` and returned
an `Option<Event>`: `Some` forwards, `None` means the transform absorbed the event into internal
state. `Event` is **864 bytes** — `crates/logit-core/tests/type_sizes.rs` pins the exact `size_of`
and exists precisely because "what one event costs to move between two pipeline nodes" is a number
worth catching drift in. So every transform hop memcpy'd 864 bytes into the callee and 864 bytes
back out, per event, on the hot path, to express a verdict the trait could already have spelled as
a bool: **`process` has never been able to emit more than one event per input**, so the `Option`'s
payload was, in every implementation that ever shipped, the same event the caller had just handed
over.

`process_batch` (`crates/logit-pipeline/src/runtime.rs`) paid a second cost for the same shape. It
could not mutate the batch's own `Vec<Event>` in place — the events had to be moved out to be
passed by value — so it built a `Vec::with_capacity(batch.events.len())` of survivors on every
batch, at every transform node, whether or not anything was absorbed. `docs/design/memory.md` §2's
"Runtime" table pinned that `Vec` as *the whole cost* of an ordinary transform hop: **1**
allocation for `keep`, for `set`, for `has_attributes`, for `trace_context`, and 1 even for
`aggregate` absorbing every event (a `Vec` built before any event is processed and thrown away
unused).

[ADR `arc-eventbatch-copy-on-write`](arc-eventbatch-copy-on-write.md) deliberately did not take
this on. It changed `Output::send` to borrow and left `Transform::process`/`ScriptWorker::process`
alone, on the reasoning that both "genuinely need ownership to do their job." That is true of the
Lua worker and false of `Transform`: a transform needs *mutable access*, which a `&mut` gives, not
ownership. `memory.md` §3 records the deferral in those terms — "a second, separable change …
Nothing is lost … Deserves its own ADR" — and §8 item 14 files it under "Later — needs a reason
first" with a standing warning attached: *"Gets more expensive to decide with every transform that
lands, so decide it early even if applied late."* Twenty `Transform` implementors ship today
(`crates/logit-transforms`), and every new component kind adds one. That warning is the reason to
take it now rather than later.

The precedent is already in the codebase, one trait over. `Router::route`
(`crates/logit-pipeline/src/router.rs`) takes `&Event` and answers a two-byte `Destination`, and
its doc comment gives exactly this ADR's argument: "`Event` is large … a by-value
`(Destination, Event)`-shaped return would memcpy every event through an enum on the hot path for
nothing — the routing verdict is a two-byte answer to a question about the event, not a
transformation of it." `Transform::process`'s verdict is a one-*bit* answer to a question about the
event. The two node traits were gratuitously different in the one place they should have matched.

**Why this came up now, and what it is a step toward.** It fell out of evaluating whether adjacent
transform hops should be *fused onto one thread* — `docs/design/pipeline-graph.md`'s "Fusing a
linear run" paragraph names that as a real future optimization once thread count in practice
warrants it. Fusing is the larger, riskier change: it alters the concurrency shape of the graph,
the flush scheduling, and the backpressure story. This one is strictly cheaper and preserves
semantics exactly, and it is worth doing *first* for a reason beyond its own numbers: it separates
the two terms in the per-hop budget. What a hop costs today is memcpy and allocation on one side,
channel and scheduler on the other. Removing the first term outright leaves the second measured in
isolation, which is the input the fusion decision actually needs.

## Decision

**`Transform::process` borrows the event and answers a bool:**

```rust
fn process(&mut self, resource: &Arc<Resource>, event: &mut Event) -> bool;
```

`true` means "forward this downstream" — possibly unchanged, possibly mutated through the `&mut`.
`false` means the transform absorbed the event into internal state (an aggregator accumulating a
mergeable metric kind) or dropped it (a filter rejecting it), and **the caller discards the event**
rather than forwarding it. Nothing else about the trait changes: `observe_scope`,
`observe_batch_context`, `observe_provenance`, `end_batch`, `flush_interval` and `flush` keep their
signatures and their contracts.

**`process_batch` drives it with `Vec::retain_mut` over the batch's own `events`:**

```rust
let EventBatch { resource, scope, mut events } = batch;
// ...
let before = events.len();
events.retain_mut(|event| transform.process(&resource, event));
let absorbed = (before - events.len()) as u64;
```

There is no `out` `Vec` to collect survivors into, and a forwarded event is never moved at all — it
stays in the slot it already occupied. Telemetry is unchanged in what it reports: the received
counts still read `events.len()` before the loop, and `logit.component.events.dropped{reason=
"absorbed"}` is now the length `retain_mut` removed rather than a counter incremented per `None`.
`process_batch` still returns `None` for an emptied batch, so a fully-absorbing node still sends
nothing downstream.

**An absorbing transform moves the payload it wants out through the borrow and leaves the shell
behind.** `std::mem::take` on the field it needs is the idiom, and `Aggregator` already did exactly
this before the change — it takes `event.metrics` with `mem::take`, merges what it can, and pushes
the unmergeable records back — which is why its verdict line became a plain
`!(event.metrics.is_empty() && event.log.is_none() && event.span.is_none())` with no other change
to its body. The husk left behind is what `retain_mut` drops.

**Lua is out of scope.** `ScriptWorker::process` (`crates/logit-script`) genuinely needs ownership:
it is a 0..N-out interface — a script may drop an event, return it, or construct a different one
entirely ([ADR `lua-event-constructor`](lua-event-constructor.md)) — so a bool cannot express its
outcome and `run_lua` still calls `unwrap_batch` to get an owned batch. `Input::run` and
`Output::send` are likewise untouched; the latter was already settled by
[ADR `arc-eventbatch-copy-on-write`](arc-eventbatch-copy-on-write.md)'s round two.

**The per-transform benches move from divan's `bench_local_values` to `bench_local_refs`**, so the
harness owns the input `Event` and the transform mutates it in place — the shape the trait now has.
This is not cosmetic; it changes what the microbenchmark numbers are comparable to, and the
Consequences below say so explicitly.

### Measured

2026-09-17, on the dev box, pinned per the repo's bench convention. Before is `main` at `b5c820a`,
after is the W1 branch as measured (its tree is what PR #233 carries; the branch was later
rewritten to fold two commits, byte-identical tree).

**Microbenchmarks** (divan, `taskset -c 2`, fastest of three runs):

| bench | before | after |
|---|---:|---:|
| `process_batch` through `keep` | 469 ns, 1 alloc | **431 ns, 0 alloc** |
| `keep` | 440 ns | 420 ns |
| `json_parse` | 349 ns | 329 ns |
| `logfmt_parse` | 349 ns | 320 ns |
| `aggregate_absorb` | 721 ns | 671 ns |
| `full_chain` | 1891 ns | 1771 ns |
| `fanout_send_one_consumer` (control, untouched code) | 260 ns | 270 ns |
| `send_batch` through a no-op `Output` (control, untouched code) | 227 ns | 228 ns |

**Only `process_batch` through `keep`, `full_chain` and the two controls are clean same-harness
comparisons.** The per-transform rows also changed harness in the same commit
(`bench_local_values` → `bench_local_refs`), which changes divan's own per-iteration overhead —
`kv_metrics` showed roughly −40% ns with a completely unchanged body, which is the size of the
harness effect, not of this change. Treat the per-transform rows as indicative of direction only.
The controls agree within noise, as they must: they exercise code this ADR does not touch.

**End-to-end** (`script/perf run --repeat 3`, CPU µs/event, median, `taskset -c 0-3,12-15`):

| scenario | before | after |
|---|---:|---|
| `passthrough` (control, no transform) | 0.369 | 0.365 |
| `json-parse` | 0.789 | **0.678** (−14%) |
| `json-parse-x3` | 2.326 | **2.003** (−14%) |
| `logfmt-parse` | 0.790 | 0.805 (+2%, noise) |
| `aggregate` | 0.259 | 0.251 |

**Per-node attribution** (`script/perf attribute`, µs/event): `json-parse-x3`'s three `json` nodes
0.497/0.505/0.512 → 0.405/0.402/0.418 (−18% to −20%); `json-parse`'s single `json` node 0.421 →
0.359 (−15%); `logfmt-parse`'s `logfmt` node 0.820 → 0.827 (noise). Peak RSS on `json-parse` fell
139 → 80 MiB (the before figure's own repeat spread was 122–143 MiB); every other scenario stayed
within a few MiB.

**What the numbers do and don't say, honestly.** The gain on a `json` node is larger than an
864-byte memcpy per hop alone explains, and it coincides with the RSS drop, so part of it is
allocator churn rather than memcpy: the removed per-batch survivors `Vec` was 64 events × 864 B ≈
55 KB of fresh allocation per batch per node, and not paying it changes the allocator's working set
as well as its instruction count. Against that, **`logfmt-parse` shows no end-to-end gain**, within noise, at both the scenario and
the node level. Flamegraphs of both scenarios before and after (same day, same box) show the two
transforms got the *same* underlying improvement: allocator call share fell ~25% in each and the
large-allocation class halved in each. The difference is only what share of each node's total
that fixed saving is — `logfmt`'s per-event cost is dominated by per-attribute work (nine interned
keys, an `AttrMap` spilled past its eight inline slots, nine string `Value` drops) that this change
never touched, so the same absolute saving lands inside its noise. Not a caveat on the mechanism,
just on which workloads it is visible in. Run-to-run drift on this box is 1–2% (`logfmt-parse`'s three interleaved
before/after pairs gave +1.9%/−0.9%/+2.0%, and one `json-parse` baseline pair drifted to 0.682), so
every magnitude above is approximate and the small rows (`passthrough`, `aggregate`) are inside
that drift.

## Alternatives considered

- **Keep the by-value `Event -> Option<Event>` shape.** The status quo, and it is not absurd: an
  owned event is the simplest thing to reason about, and `memory.md` filed this under "Later —
  needs a reason first" for a year's worth of transforms. Rejected because nothing is lost by
  borrowing — the trait could never emit more than one event per input, so the `Option`'s payload
  never carried information the caller didn't already have — and because the cost of *deciding* is
  monotonically increasing: every component kind that lands adds another implementor to migrate.
  §8 item 14's own instruction was to decide it early even if applied late.
- **An enum verdict (`Forward` / `Absorb`) instead of a bool.** Self-documenting at the call site,
  and the same shape `Router::route`'s `Destination` uses. Rejected: it would not match
  `Vec::retain_mut`'s predicate, so `process_batch` would need a closure mapping the enum back to a
  bool — reintroducing a conversion at exactly the place this change exists to simplify — and it
  adds nothing the two names don't already say in `process`'s doc comment, which spells out what
  `true` and `false` mean in the first sentence. `Destination` is an enum because it genuinely
  carries a payload (`To(u16)`); a forward/absorb verdict does not.
- **A batch-level `fn process_batch(&mut self, &Arc<Resource>, &mut Vec<Event>)` trait method**,
  letting each transform drive its own loop. Strictly more expressive — a transform could reorder,
  split, or emit — and it would remove the runtime's loop entirely. Rejected: the per-event shape is
  deliberate, for the reasons `transform.rs`'s module doc already gives, and this ADR is not the
  place to reopen it. It would also hand every implementor the ability to get batch-level telemetry
  accounting wrong, which is currently the runtime's single responsibility and correct by
  construction.
- **Thread-fusing adjacent transform hops**, removing the channel and scheduler cost per hop
  instead of the memcpy and allocation cost. Deferred, not rejected — it is a separate decision with
  a much larger blast radius (`docs/design/pipeline-graph.md`'s "Fusing a linear run"). This ADR's
  numbers are an *input* to it: with the memcpy/allocation term removed, what remains in a per-hop
  budget is the channel/scheduler term, measured in isolation.

## Consequences

- **Every `process_batch` allocation pin in `crates/logit-bench/tests/allocations.rs` dropped by
  exactly one**, and `docs/design/memory.md` §2's "Runtime" table with it: through `keep`, through
  `set` (attributes only), through `has_attributes`, dropping every event, fully absorbed
  (`aggregate`), with live telemetry, and both `trace_context` rows all go **1 → 0**. The first call
  after an `internal` drain goes **3 → 2** (what remains is the `HashMap` table rebuild and the
  fresh `DdSketch`, which is what that test was always about), and
  `fan_out_plus_two_has_attributes_for_the_same_split` goes **196 → 194**, one per filter pass. The
  per-event `*_one_event` pins are unchanged, as expected: they call `process` directly and never
  allocated a batch `Vec` to begin with.
- **An ordinary transform hop now allocates nothing per batch.** Not "less" — nothing. That is a
  property worth stating as such, because it makes any future non-zero pin on a transform node a
  visible regression rather than a change in degree.
- **Every future transform is written in the borrowing shape**, and the trait no longer offers the
  by-value option. A transform that wants to replace an event wholesale mutates the one it was
  given; a transform that wants to *keep* one moves the payload out with `mem::take` and returns
  `false`.
- **A test double that buffers whole events needs the `mem::replace` idiom.** `Event` has no
  `Default`, so `runtime.rs`'s `WindowingTransform` — which stands in for `Aggregator` in a crate
  that can't depend on `logit-transforms` — pushes
  `std::mem::replace(event, Event::empty(0, AttrMap::new()))` onto its buffer and leaves the husk
  for `retain_mut` to drop. Anything else that needs to *own* an event from inside `process` does
  the same.
- **Filters now take a `_event: &mut Event` they never touch.** `has_attributes`, `drop_signals`,
  `has_provenance` and their siblings read the resource or a cached matcher and answer a bool
  without looking at the event at all, so the mutable borrow is unused. That's a small cosmetic
  cost of one uniform signature across twenty implementors, accepted rather than split into two
  traits.
- **`Transform` and `Router` now agree.** Both borrow the event and answer a verdict; neither moves
  one. `Router::route`'s doc comment already carried the reasoning, and `Transform::process`'s now
  carries the matching version pointing at the same `type_sizes.rs` pin.
- **`docs/design/memory.md` §8 item 14 is closed**, and §3's deferral paragraph now records the
  result instead of the intent.
- **Follow-on: the fusion question stays open.** This ADR removes the memcpy/allocation term from
  the per-hop budget and deliberately does not touch the channel/scheduler term. Whether fusing a
  linear run of adjacent nodes onto one thread is worth its complexity is still undecided, and now
  has cleaner inputs.
- **Follow-on: the win is per transform node and scales with how much of that node is
  `process_batch` overhead.** A `json` node gained 15–20%; a `logfmt` node, dominated by
  per-attribute work, gained nothing measurable from the same removed `Vec` and memcpy. Where the
  rest of a wide event's time goes (`AttrMap` inline capacity, interning, `Value` drops) is a
  separate question with its own notes, not part of this decision.