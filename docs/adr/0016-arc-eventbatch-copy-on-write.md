# 0016. `Arc<EventBatch>` copy-on-write on channels

## Status

Accepted.

## Context

`Fanout::send`/`send_blocking` (`crates/logit-pipeline/src/fanout.rs`) deep-clone the whole
`EventBatch` for every consumer but the last, before handing the batch to the last one. That clone
is not incidental — it is what makes branch isolation free: two branches of a fan-out never share
the same `Event` value, so a mutation one branch's transform makes is structurally invisible to a
sibling branch reading the same upstream event, with nothing extra to design or maintain for that
guarantee (`docs/adr/0012-multi-payload-events.md`, proven by
`crates/logit-pipeline/src/runtime.rs`'s
`a_mutation_on_one_fan_out_branch_is_invisible_to_the_sibling_branch`). But most fan-out consumers
never mutate what they receive — every `Output`/sink is exactly this case, since both implemented
encoders take `&EventBatch` — so the clone buys them a guarantee they never needed.

`docs/design/memory.md` §3 measures the cost: for the nginx-shaped reference event, cloning an
`EventBatch` is 4 allocations and a 792-byte memcpy per event per extra fan-out branch (272 ns).
`docs/design/pipeline-graph.md`'s "Backpressure" section calls this clone "load-bearing" and, since
[ADR 0012](0012-multi-payload-events.md) let one event carry a log and several metrics at once,
notes the average per-branch cost went up from there too.

The relative significance of this cost has moved. Before PR #26 reworked the InfluxDB encoder
(`influxdb_out: 600x fewer allocations per batch, byte-identical output`), `docs/known-gaps.md`'s
fan-out entry and `memory.md` both point at the encoder as the dominant cost — encoding one event
cost roughly sixteen times what cloning it for an extra fan-out branch did, so copy-on-write here
was true but second-order. With the encoder now down to ~0.3 allocations per event (`memory.md`
§2), that ordering has flipped: `known-gaps.md`'s entry, as it stands today, says plainly that the
fan-out clone is no longer the pipeline's main cost — the encoder was — but is now, in its own
words, "one of the larger remaining costs," and that the copy-on-write change is "still worth
making," being "strictly no worse anywhere" and freeing "every read-only sink branch entirely."
`memory.md` §8 ranks this item accordingly: higher than the nginx numbers alone would justify, both
because it is payload-shape-independent (§0's workload table shows every shape pays the full
`Event` clone) and because it is worth *most* to the workload least represented in the fixtures —
a `SpanRecord` carries `Vec<SpanEvent>`/`Vec<SpanLink>`, each with its own 400-byte `AttrMap`, so a
span-bearing event is far more expensive to deep-clone than anything actually measured so far.

## Decision

Change the channel payload between graph nodes from a bare `EventBatch` to `Delivered`, a
two-variant enum, confined entirely to `crates/logit-pipeline/src/{fanout.rs,runtime.rs}`:

```rust
pub enum Delivered {
    Owned(EventBatch),
    Shared(Arc<EventBatch>),
}
```

- `Fanout`'s `consumers: Vec<mpsc::Sender<EventBatch>>` becomes `Vec<mpsc::Sender<Delivered>>`, and
  `Fanout::new` takes that type.
- `Fanout::send`/`send_blocking` still take an owned `EventBatch` — every existing call site
  constructs one exactly as before — but now branch on how many consumers that `Fanout` has:
  - **Exactly one consumer** (a linear chain, and every shipped listener's first hop): the batch
    moves through as `Delivered::Owned`, with **no `Arc` involved at all**. This is the fast path an
    earlier draft of this ADR was missing — see "What changed after review" below.
  - **More than one consumer** (a real fan-out): wrap the batch in an `Arc` once, then clone the
    `Arc` (a refcount bump, not a deep clone) for every consumer but the last, which gets it moved —
    the same `split_last`-based structure as before, just operating on the `Arc`, and saving one
    atomic increment/decrement pair rather than conferring any structural advantage on the "last"
    branch (see the next point).
- `runtime.rs`'s `unwrap_batch` turns either variant back into an owned `EventBatch` at each
  consumption point — `run_output`'s `inbox.recv().await`, `run_transform`'s, and `run_lua`'s —
  right before handing the batch to `Output::send`, iterating it for a `Transform`, or handing its
  events to `ScriptWorker::process`:

  ```rust
  fn unwrap_batch(batch: Delivered) -> EventBatch {
      match batch {
          Delivered::Owned(batch) => batch,
          Delivered::Shared(shared) => Arc::try_unwrap(shared).unwrap_or_else(|shared| (*shared).clone()),
      }
  }
  ```

  `Delivered::Owned` is already the owned batch: free, unconditionally. `Delivered::Shared` unwraps
  via `Arc::try_unwrap`, which succeeds with no clone whenever this is the only remaining strong
  reference. In practice that means whichever fan-out branch happens to drop its own reference last
  at runtime — **not a guaranteed "one free branch."** Nothing about `Fanout::send` privileges one
  branch's handle over another's (moving the `Arc` to the last-sent consumer is purely an efficiency
  detail, saving one atomic operation, and has no bearing on which branch's `try_unwrap` succeeds),
  and two branches racing to unwrap concurrently can both still observe a strong count above 1 and
  both fall back to cloning. It falls back to a real deep clone whenever a sibling branch still
  holds its own reference at the moment of unwrapping — exactly the case that needs an independent
  copy for mutation safety, whether or not that's the *only* case in a given run.

**No trait changes — in round one.** `Input::run`, `Output::send`, `Transform::process`, and
`ScriptWorker::process` all kept taking/returning owned `EventBatch`/`Event` exactly as before. The
wrap/unwrap boundary sat entirely between `Fanout` (produces) and the node runtime's receive loops
(consume), so no crate outside `logit-pipeline` — `logit-inputs`, `logit-outputs`,
`logit-transforms`, `logit-cli` — needed to change at all. (A listener's own inbox is never fed at
all — arity rules out a `sources` entry pointing at one — so `Input` never receives a `Delivered`
in the first place; that stayed true in round two too.) **Round two (below) does take on one trait
change** — `Output::send` — once the numbers below showed it was the one that actually mattered;
`Transform`/`ScriptWorker`/`Input` are still untouched.

**Branch isolation is preserved, not just assumed.** Whenever `Fanout::send` takes the fan-out path,
every branch holds *some* reference until it's consumed, keeping the strong count above 1 until
all-but-one branch has dropped theirs. `try_unwrap` therefore fails for every branch that still has
a live sibling, forcing a real clone there — the same isolation outcome today's unconditional clone
gives, just skipped when nothing needs it. `a_mutation_on_one_fan_out_branch_is_invisible_to_the_sibling_branch`
exercises exactly this and needed no change to keep passing; two smaller tests alongside it
(`a_single_consumer_fanout_delivers_the_batch_owned_with_no_arc_involved` and
`a_shared_batchs_arc_is_uniquely_held_only_once_every_sibling_handle_is_dropped`) pin the two
mechanisms — the fast path, and the strong-count property `try_unwrap` depends on — directly.

### What this change actually saves, measured

The first version of this ADR and its implementation put `Arc::new` on every send,
unconditionally, including a single-consumer edge — which previously moved the batch for free.
Review caught this: it regressed every shipped listener's first hop and all three single-consumer
edges of the v0.1 reference config (`examples/statsd-to-influxdb.yaml`, a pure linear chain) by
roughly one allocation per event per hop, for zero benefit. The `Delivered::Owned` fast path above
is the fix, and `fanout_send_one_consumer_costs_nothing` (`crates/logit-bench/tests/allocations.rs`)
measures it at **zero** additional allocations — a genuine, unconditional restoration of what the
pre-`Arc` code already did for free.

Review also asked for the equivalent number on the fan-out (2+ consumer) side, expecting it to show
a saving. Measuring it honestly (`fanout_send_two_consumers_costs_one_clone_plus_one_arc`,
same file) instead surfaces a real limit of this design that neither the original ADR draft nor the
review fully priced in: **a real fan-out does not cost less than the pre-`Arc` code, and costs
exactly one allocation more.** Working through why: `unwrap_batch` runs immediately on receipt, at
the top of every node's loop, before any of that node's own processing — so which branch's
`Arc` handle is still alive when a sibling calls `try_unwrap` is decided purely by scheduler timing,
not by how much work either branch does per event. Traced through the reference case, N consumers
processed one at a time (no two `unwrap_batch` calls genuinely concurrent) always produces exactly
`N - 1` real clones and one free unwrap — measured directly at `N = 2` (one clone, 5 allocations for
this shape: 1 for the cloned `Vec<Event>`, 4 for the one `Event` inside it, matching
[`clone_one_event`]) and `N = 3` (two clones, 10 allocations) during this review round. That is the
*same* clone count the pre-`Arc` code already paid for the same `N` (`Fanout::send` clones every
consumer but the last, unconditionally) — this design does not reduce it. What it adds is
`Arc::new`'s one allocation per send, paid regardless of `N`. So the honest total is `1 + (N - 1) ×
5` against the old code's `(N - 1) × 5`: one allocation worse, at every fan-out width, in the best
(fully sequential) case. Under genuine concurrency — two branches' `unwrap_batch` calls actually
overlapping on different cores of the multi-thread runtime, which the review confirmed happens in
practice against `examples/nginx-to-influxdb.yaml`'s `tap`/`trimmed` fan-out — more than one branch
can fail to unwrap and clone, which is *worse* than the deterministic case above, never better.

**The originally-hoped saving — "every read-only branch pays one atomic, not a clone" — needed the
`Output::send(&EventBatch)` trait change this ADR's first round deliberately didn't make.** Without
it, every consumer, read-only or not, had to materialize an *owned* `EventBatch` to satisfy the
existing trait signatures, giving up its `Arc` handle immediately on receipt — exactly when sharing
would otherwise have paid off. `Delivered::Shared`'s best-effort savings were real (whichever
branch's handle survives longest gets a free unwrap instead of a guaranteed clone), but didn't, on
their own, beat the code this PR replaces for any fan-out measured. The unconditional win in round
one was scoped to `Delivered::Owned`: the single-consumer edge. See the next section for what
changed once that trait change was actually taken on.

### Round two: `Output::send` takes `&EventBatch`, closing the gap for `Output` branches

Once every other Wave-1 workstream had merged, the trait change round one deferred was revisited
(see the former "Change `Output::send` to take `&EventBatch`" Alternative, folded into this
Decision below). `Output::send` now takes `&EventBatch`:

```rust
pub trait Output {
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()>;
}
```

Both implementers (`InfluxDbOutput`, `StdioOutput`) only ever read via `&EventBatch` internally
already, so each change is signature-only. The part that actually captures the saving is
`runtime.rs`'s `run_output`, which no longer calls `unwrap_batch` at all — it borrows straight out
of whichever `Delivered` variant it received and hands that reference through:

```rust
async fn run_output(/* ... */) -> anyhow::Result<()> {
    while let Some(delivered) = inbox.recv().await {
        let batch: &EventBatch = match &delivered {
            Delivered::Owned(batch) => batch,
            Delivered::Shared(shared) => shared, // &Arc<EventBatch> derefs to &EventBatch
        };
        output.send(batch).await?;
    }
    Ok(())
}
```

`run_transform` and `run_lua` are unchanged: `Transform::process`/`ScriptWorker::process` still
need an owned `Event` to mutate or consume, so they still call `unwrap_batch` exactly as in round
one.

Measured (`crates/logit-bench/tests/allocations.rs`), for a 1-event nginx-shaped batch, against
`main`'s (pre-PR) flat, unconditional **5** allocations for *any* 2-consumer fan-out regardless of
consumer kind:

| Fan-out shape (2 consumers) | Allocations | vs. `main`'s flat 5 |
|---|---:|---|
| Both `Output` (`fanout_send_two_output_consumers_costs_only_the_arc`) | **1** | strictly better |
| One `Output`, one `Transform` — `Transform` unwraps while `Output`'s handle is still alive (`fanout_send_mixed_output_and_transform_consumers`) | **6** | 1 worse |
| One `Output`, one `Transform` — `Output` finishes and drops first (`fanout_send_mixed_output_and_transform_consumers_when_output_finishes_first`) | **1** | strictly better |
| Both `Transform`-style, no `Output` at all (round one's number, unchanged) | 6 | 1 worse |

**This genuinely closes the gap for an all-`Output` fan-out, unconditionally.** Neither branch ever
calls `Arc::try_unwrap`, so there's no race to win or lose — `Output::send` doesn't compete for the
free unwrap at all, it just borrows. This *is* the "every read-only branch pays one atomic, never a
clone" saving `docs/design/memory.md` §8 item 4 originally recommended, delivered, measured at
exactly 1 allocation regardless of how many `Output` branches share the fan-out.

**For the mixed case — the actually-common shape, matching the nginx reference config's
`tap`/`trimmed` split — the result is genuinely bimodal, not a fixed number in either direction.**
An earlier version of this section claimed the `Transform` branch "reliably" or "deterministically"
clones here; that overclaimed. `run_output`'s loop (`runtime.rs`) drops its own `Delivered` the
moment `output.send` returns, immediately before its next receive — it does not hold the `Arc`
handle for the rest of its loop, and it never itself calls `try_unwrap`. So whether the `Transform`
branch's `unwrap_batch` call succeeds for free comes down to real tokio scheduling: whichever
happens first, `Output` completing its `send` and dropping its handle, or the `Transform` branch's
unwrap actually running. Both orderings are reachable and both are measured above — 6 if `Transform`
gets there first (`Output`'s handle still alive, forcing a clone), 1 if `Output` finishes first
(nothing left to contend with, `try_unwrap` succeeds free). **There is no path to landing on
`main`'s flat 5 either way** — `Arc::new` is paid unconditionally the moment there are 2+ consumers
— so the reachable range is 1 or 6, never in between.

In production, `Output::send` typically performs real I/O (a network write, a file append), which
tends to be measurably slower than a `Transform`'s local, in-process work — so 6 (one allocation
worse than `main`) is the *likelier* practical outcome for this shape, not the *only* one. A fast or
local `Output` implementation, or a `Transform` doing enough work before its own unwrap that
`Output` finishes first, could just as easily land on 1. **This means the mixed case is not a
strict improvement over `main`, and not reliably worse either — it's genuinely racy**, and this ADR
does not claim to know, or control, which outcome a given deployment gets. What *is* structural,
not racy: `Output` itself never pays anything beyond its share of the one `Arc::new` in either
outcome, and a mutating branch's copy is always independent before it can be mutated regardless of
which path `try_unwrap` took (`a_mutation_on_one_fan_out_branch_is_invisible_to_the_sibling_branch`).

**What still doesn't close: a fan-out where more than one branch needs an owned copy** (two
`Transform`s, or a `Transform` and a Lua stage, off one node, with no `Output` involved). Neither
side of that fan-out can borrow, so it's exactly round one's `1 + (N - 1) × clone`, deterministically
— one allocation worse than the pre-`Arc` code, with no racy path to anything better, since nothing
in this shape ever finishes without competing for the unwrap. `Output::send(&EventBatch)` doesn't
touch this case; it isn't an `Output` case. Left as-is: the same reasoning round one's
"Alternatives" gave for wrapping each `Event` individually still applies
(`Transform::process`/`ScriptWorker::process` genuinely need ownership to do their job), and
widening *those* traits to something reference-based would remove their ability to mutate or
consume the event at all.

**A fix for the mixed case's raciness was sketched and is deliberately not taken here — and it's
narrower than it first looks.** For exactly one `Output` and one owning consumer, making `Fanout`
aware of which is which and giving the owning one an unconditional direct clone (bypassing
`Arc`/`try_unwrap` entirely) while sharing the `Arc` only with the `Output` side would turn 1-or-6
into a fixed 6 (`main`+1, always, no race) — trading away the chance at 1 for predictability. That
part generalizes cleanly.

**It does not generalize cleanly to more than one consumer on either side, worked through directly
rather than assumed.** With M borrowing and K owning consumers sharing one fan-out (M, K ≥ 1), the
"aware" design's own cost depends on a further, undetermined choice — do the K owning consumers
each get an unconditional direct clone (no sharing among them), or share a *second*, dedicated
`Arc` and race each other the way an all-owning fan-out already does today? For M=2, K=2, the first
costs `1 + K×5 = 11`; the second costs more (`1 + 5 + 1 + (K-1)×5 = 12` — the second `Arc::new` to
set up the owning group's own race outweighs what racing them saves). Meanwhile *today's* actual
design, racy as it is, has a wider reachable range for that same shape than either — from 6 (if
every `Output` branch happens to finish before either owning branch attempts its unwrap, reducing
to the pure two-owning-consumer race) up to 11 (if any `Output` is still pending throughout). So an
"aware" fix doesn't strictly dominate what exists today once either group has more than one member:
it fixes the *worst* case as the *guaranteed* case, foreclosing the chance of landing on today's
best case (6) in exchange for never landing on today's worst case being a surprise. Whether that
trade is worth it — and which internal strategy for the owning group to use — needs its own design
work informed by real consumer-kind distributions across shipped configs, not a two-line addendum
here. Recorded as an open problem, not a specified fix — see Alternatives.

## Alternatives

- **Wrap each `Event` in its own `Arc`, not the batch.** Rejected on `memory.md` §3's own reasoning
  about granularity: for a real fan-out, this design pays one `Arc::new` per *event*, scaling with
  batch size, where the batch-level design pays exactly one `Arc::new` per *send* regardless of how
  many events the batch holds — the same `1 + (N - 1) × clone` accounting worked out above still
  applies, just with the constant "1" multiplied by the event count instead of staying fixed. A
  single-consumer edge could in principle get an equivalent `Owned`-style fast path too, so that
  case isn't the deciding factor; the batch-size scaling on every real fan-out is. Batches are the
  unit `logit` already moves events in (`docs/design/data-model.md`), and this is the granularity
  that keeps the `Arc` overhead independent of batch size.
- **A routing primitive to avoid fan-out entirely** (named outlets, edge predicates, a pub-sub
  broker). Already considered and rejected by [ADR 0009](0009-component-graph-configuration.md):
  filter components chained via `sources` are the one branching mechanism the config model has, and
  a second one on top would be two ways to express the same thing, plus (for a broker) losing the
  natural backpressure a bounded `mpsc` gives for free. This ADR doesn't reopen that; fan-out stays
  the normal shape, and this is a cost fix for it, not a way around it.
- **Change `Output::send` to take `&EventBatch` instead of an owned `EventBatch`.** Round one's
  draft of this ADR listed this as a rejected, out-of-scope alternative — the trait-signature
  change this whole design existed specifically to avoid needing. It no longer is: round two (above)
  takes it on, once every other Wave-1 workstream had merged and widening scope stopped competing
  with anything else in flight. It was the change that actually closed the gap round one's own
  numbers exposed, not a nice-to-have layered on an already-complete saving — recorded here as
  history (why it was rejected the first time) rather than removed, since the reasoning was sound
  for round one's scope at the time.
- **Do nothing.** Leaves the pre-`Arc` code in place: an unconditional clone for every consumer but
  the last, on every send, listener-to-sink or fan-out alike — a flat, predictable 5 allocations for
  any 2-consumer fan-out, regardless of consumer kind. Round one's measurements left this a
  genuinely open question — without the `Output::send(&EventBatch)` change, doing nothing was at
  least as cheap, and for a genuine fan-out one allocation *cheaper*, than what round one shipped.
  Round two changes the comparison but doesn't settle it outright: an all-`Output` fan-out now
  strictly beats doing nothing (1 versus 5), and a fan-out with no `Output` branch at all is still
  strictly worse than doing nothing (6 versus 5, deterministically). The mixed case sits on neither
  side reliably — it's 1 (better than doing nothing) or 6 (worse), decided by scheduling neither
  this design nor "do nothing" controls. So "does this beat doing nothing" no longer has one answer
  across configs; it depends on the fan-out shapes a given deployment actually has, and, for the
  mixed shape specifically, on scheduling this ADR doesn't govern.
- **Make `Fanout` aware of which consumers are borrowing (`Output`) vs. owning
  (`Transform`/`ScriptWorker`) at construction time**, so it can give owning consumers a direct,
  unconditional clone while sharing the `Arc` only among borrowing ones. Clean for exactly one
  consumer of each kind: turns 1-or-6 into a fixed 6, trading the chance at 1 for predictability.
  **Does not clearly generalize once either group has more than one member** — worked through
  directly for M=2 borrowing/K=2 owning: an "aware" fix's own cost depends on an unresolved choice
  (direct clone per owning consumer, `1 + 5K = 11`; or a second dedicated `Arc` for the owning group
  to race over, `12` — worse, since that `Arc::new` costs more than the race saves), and *today's*
  actual (racy) design already reaches as low as 6 for that same shape when every `Output` branch
  happens to finish first. So an "aware" fix would fix the current *worst* case as the *guaranteed*
  one, not strictly beat what exists — whether that trade is worth it isn't decided by the 2-and-2
  case alone. Not taken here, and not fully specified even as a direction: it needs threading
  node-kind information from `runtime.rs` down into `Fanout::new`'s construction *and* a real design
  pass on the >2-consumer case, a scope expansion well past correcting this ADR's claims to match
  what's measured. Left as an open problem for whoever picks this up next, not a specified fix.

## Consequences

- The channel type change (`EventBatch` → `Delivered`) is internal to `logit-pipeline`. The
  `Output::send(&EventBatch)` trait change is not internal — every implementer's signature changes
  — but both shipped ones (`InfluxDbOutput`, `StdioOutput`) needed only the signature updated, no
  logic change, since each already only read via `&EventBatch` internally.
- A single-consumer edge — a linear chain, every shipped listener's first hop — costs nothing extra,
  measured at zero additional allocations (`fanout_send_one_consumer_costs_nothing`). This is a fix
  for a regression this same PR introduced in its first round, not an improvement over `main` before
  this work started.
- **An all-`Output` fan-out (two or more sinks off one node) now costs exactly one allocation total
  — the `Arc::new` — regardless of how many `Output` branches share it**, measured at
  `fanout_send_two_output_consumers_costs_only_the_arc`. This is the "every read-only branch pays
  one atomic, never a clone" saving `docs/design/memory.md` §8 item 4 originally recommended,
  delivered — round one's design alone didn't get here; `Output::send(&EventBatch)` (round two,
  above) is what closes it.
- **A mixed fan-out (`Output` branches plus exactly one mutating `Transform`/Lua branch) — the
  nginx reference config's `tap`/`trimmed` shape — lands on 1 or 6 allocations, decided by real
  scheduling, not a fixed number.** 6 if the mutating branch's `unwrap_batch` call runs before
  `Output` finishes its `send` and drops its handle (`fanout_send_mixed_output_and_transform_consumers`
  — the same total as an all-mutating fan-out, but concentrated on the mutating branch, none of it
  on `Output`); 1 if `Output` finishes first
  (`fanout_send_mixed_output_and_transform_consumers_when_output_finishes_first` — just the
  `Arc::new`, nothing else, since nothing is left to contend with). `Output::send` never competes
  for the free unwrap either way (it never calls `try_unwrap`), but *when* the mutating branch's own
  unwrap runs relative to `Output` completing is scheduling, which this design doesn't control. An
  earlier version of this ADR called this outcome "deterministic" or something `Output` "reliably"
  forces — that overclaimed; corrected here.
- **A fan-out with no `Output` branch at all (two `Transform`s, or a `Transform` and Lua, sharing
  one node) still costs one allocation more than the pre-`Arc` code, deterministically, and possibly
  more under genuine concurrency** — unchanged from round one
  (`fanout_send_two_consumers_costs_one_clone_plus_one_arc`). `Output::send(&EventBatch)` doesn't
  touch this case; it isn't an `Output` case. A branch that mutates pays exactly what it paid
  before, via the `try_unwrap` fallback, in every shape above.
- `a_mutation_on_one_fan_out_branch_is_invisible_to_the_sibling_branch` remains the correctness
  guard for all of this: it must keep passing, unweakened, for any future change that touches
  `Fanout`, `Output`, or the node runtime's receive loops — confirmed unmodified through every
  round, including this one, which is docs/tests only and changes no behavior.
  `a_single_consumer_fanout_delivers_the_batch_owned_with_no_arc_involved` and
  `a_shared_batchs_arc_is_uniquely_held_only_once_every_sibling_handle_is_dropped`
  (`crates/logit-pipeline/src/runtime.rs`) guard the two mechanisms this design rests on;
  `crates/logit-bench/tests/allocations.rs`'s five `fanout_send_*` tests put real allocation counts
  on every shape above, including both reachable outcomes of the mixed one.
- `docs/design/memory.md` §8 item 4 and `docs/known-gaps.md`'s fan-out entry recommended this fix
  expecting a fan-out-side saving. Round one found `Delivered` alone didn't deliver it for any
  shape measured; round two shows it now does for an all-`Output` fan-out unconditionally, and
  *can* for a mixed one depending on scheduling this design doesn't control — worth reflecting
  `memory.md`/`known-gaps.md`'s framing to match precisely, separately, outside this ADR.
