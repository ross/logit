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

Change the channel payload between graph nodes from `EventBatch` to `Arc<EventBatch>`, confined
entirely to `crates/logit-pipeline/src/{fanout.rs,runtime.rs}`:

- `Fanout`'s `consumers: Vec<mpsc::Sender<EventBatch>>` becomes
  `Vec<mpsc::Sender<Arc<EventBatch>>>`, and `Fanout::new` takes that type.
- `Fanout::send`/`send_blocking` still take an owned `EventBatch` — every existing call site
  constructs one exactly as before — but wrap it in an `Arc` once, then clone the `Arc` (a refcount
  bump, not a deep clone) for every consumer but the last, which gets it moved. This is the same
  `split_last`-based structure as today, just operating on the `Arc`.
- `runtime.rs` unwraps back to an owned `EventBatch` at each consumption point — `run_output`'s
  `inbox.recv().await`, `run_transform`'s, and `run_lua`'s — right before handing the batch to
  `Output::send`, iterating it for a `Transform`, or handing its events to `ScriptWorker::process`:

  ```rust
  let batch = Arc::try_unwrap(batch).unwrap_or_else(|shared| (*shared).clone());
  ```

  `try_unwrap` succeeds, with no clone, whenever this is the only remaining strong reference — the
  common single-consumer-per-edge case, and also whichever fan-out branch happens to consume last
  (that one got the `Arc` moved by `Fanout::send`, not cloned). It falls back to a real deep clone
  only when a sibling branch still holds its own reference to the same batch — exactly the case
  that needs an independent copy for mutation safety.

**No trait changes.** `Input::run`, `Output::send`, `Transform::process`, and `ScriptWorker::process`
all keep taking/returning owned `EventBatch`/`Event` exactly as before. The wrap/unwrap boundary
sits entirely between `Fanout` (produces) and the node runtime's receive loops (consume), so no
crate outside `logit-pipeline` — `logit-inputs`, `logit-outputs`, `logit-transforms`, `logit-cli` —
needs to change at all.

**Branch isolation is preserved, not just assumed.** As long as `Fanout::send` clones the `Arc` for
every consumer-but-last, every branch holds *some* reference until it's consumed, keeping the
strong count above 1 until all-but-one branch has dropped theirs. `try_unwrap` therefore correctly
fails for every branch except whichever drops its reference last, forcing a real clone there —
the same outcome as today's unconditional clone, just skipped when nothing needs it.
`a_mutation_on_one_fan_out_branch_is_invisible_to_the_sibling_branch` exercises exactly this and
needed no change to keep passing.

## Alternatives

- **Wrap each `Event` in its own `Arc`, not the batch.** Rejected on `memory.md` §3's own reasoning
  about granularity: an `Arc` per event costs an allocation and an atomic *per event*, which is
  worse than what it replaces for the ordinary single-consumer case — the common case would go from
  free (no clone at all once `try_unwrap` succeeds on a whole-batch `Arc`) to paying an allocation
  it doesn't need. Batch-level granularity costs one atomic per *batch*, not per event, and batches
  are the unit `logit` already moves events in (`docs/design/data-model.md`).
- **A routing primitive to avoid fan-out entirely** (named outlets, edge predicates, a pub-sub
  broker). Already considered and rejected by [ADR 0009](0009-component-graph-configuration.md):
  filter components chained via `sources` are the one branching mechanism the config model has, and
  a second one on top would be two ways to express the same thing, plus (for a broker) losing the
  natural backpressure a bounded `mpsc` gives for free. This ADR doesn't reopen that; fan-out stays
  the normal shape, and this is a cost fix for it, not a way around it.
- **Do nothing.** Leaves the clone in place. Defensible while the encoder was the dominant cost, but
  `memory.md`'s own numbers are the reason to stop deferring it: the fix is strictly no worse
  anywhere (a mutating branch pays exactly what it pays today), it is payload-shape-independent, and
  it disproportionately benefits the span-bearing workload the current fixtures under-represent.

## Consequences

- The channel type change (`EventBatch` → `Arc<EventBatch>`) is internal to `logit-pipeline`; no
  downstream crate's public surface or implementation changes.
- Every read-only fan-out branch — every sink, and any transform that only reads what it's given —
  now costs one atomic refcount operation per extra branch instead of a full clone. A branch that
  mutates pays exactly what it paid before, via the `try_unwrap` fallback.
- `a_mutation_on_one_fan_out_branch_is_invisible_to_the_sibling_branch` remains the correctness
  guard for this: it must keep passing, unweakened, for any future change that touches `Fanout` or
  the node runtime's receive loops.
- `docs/design/memory.md` §8 item 4 and `docs/known-gaps.md`'s fan-out entry describe this as the
  recommended fix; this ADR is the record of actually making it.
