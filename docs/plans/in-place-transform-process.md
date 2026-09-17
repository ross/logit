---
created: 2026-09-17
updated: 2026-09-17
---

# Enabling plan: `Transform::process` in place — `&mut Event -> bool`

## Context

[ADR `in-place-transform-process`](../adr/in-place-transform-process.md) decides the shape:
`Transform::process` borrows the event (`&mut Event`) and answers a bool instead of taking an owned
`Event` and returning `Option<Event>`, and `process_batch` drives it with `Vec::retain_mut` over the
batch's own `events` rather than collecting survivors into a second `Vec`. This plan is the
build-out: what lands in which order, in which files, and how it is verified. Read the ADR first —
this document doesn't repeat its reasoning or its numbers, only its consequences.

Stream key **`inplace`**: branches `inplace/w0`…`inplace/w1`, a strictly linear stack, each PR based
on and targeting its parent's branch, brought up to date with `git merge origin/main` (never
rebase).

## Decisions already settled

| Question | Decision |
|---|---|
| Signature | `fn process(&mut self, resource: &Arc<Resource>, event: &mut Event) -> bool` |
| Verdict type | A bool, not a `Forward`/`Absorb` enum — it is what `Vec::retain_mut`'s predicate wants, and the doc comment names both outcomes |
| `true` / `false` | `true` forwards the (possibly mutated) event; `false` means absorbed into internal state or dropped, and the caller discards it |
| Batch driver | `Vec::retain_mut` over the batch's own `events` — no `out` `Vec`, no event moved on the forward path |
| Absorbing transforms | Move the payload out through the borrow with `std::mem::take` and leave the drained shell for `retain_mut` to drop (`Aggregator` already did this) |
| Test doubles that buffer whole events | `std::mem::replace(event, Event::empty(..))` — `Event` has no `Default` |
| Telemetry | Unchanged in what it reports: received counts still read `events.len()` before the loop; `dropped{reason="absorbed"}` is the length `retain_mut` removed |
| Lua | Out of scope. `ScriptWorker::process` is 0..N-out and genuinely needs ownership; `run_lua` still calls `unwrap_batch` |
| `Input` / `Output` | Untouched — `Output::send(&EventBatch)` was already settled by ADR `arc-eventbatch-copy-on-write` |
| Batch-level trait method | Rejected (ADR's Alternatives); the per-event shape is deliberate |
| Benches | Per-transform benches move `bench_local_values` → `bench_local_refs`; the harness owns the event and the transform mutates it |
| Thread-fusing adjacent hops | Deferred, separate decision. This work's numbers are an input to it |
| Landing | PR stack only. Nothing merged by this workstream; Ross directs merging |

## Design

Everything below is what landed on `inplace/w1`, not a sketch.

### `crates/logit-pipeline/src/transform.rs` (W1)

The trait method changes and its doc comment is rewritten to carry the reasoning:

```rust
fn process(&mut self, resource: &Arc<Resource>, event: &mut Event) -> bool;
```

The comment states the verdict semantics, then why the event is borrowed — `Event` is 864 bytes
(`crates/logit-core/tests/type_sizes.rs` pins the exact `size_of`), `process` has never been able to
emit more than one event per input, and `Router::route` already borrows for exactly the same reason
— and finally that an absorbing transform gets everything it needs from the `&mut` via
`std::mem::take`, pointing at `aggregate.rs` as the live instance. Every other method on the trait
(`observe_scope`, `observe_batch_context`, `observe_provenance`, `end_batch`, `flush_interval`,
`flush`) is unchanged.

### `crates/logit-pipeline/src/runtime.rs` — `process_batch` (W1)

The batch is destructured up front and `retain_mut` replaces the collect loop:

```rust
let EventBatch { resource, scope, mut events } = batch;
transform.observe_scope(scope.clone());
let resource = transform.map_resource(&resource).unwrap_or(resource);

let process_timer = telemetry.timer("logit.component.process.duration");
let before = events.len();
events.retain_mut(|event| transform.process(&resource, event));
transform.end_batch();
drop(process_timer);
let absorbed = (before - events.len()) as u64;
```

`end_batch` stays inside the timer for the reason its own doc comment gives. The tail is unchanged
in behaviour: `None` for an emptied batch, otherwise `Some(EventBatch { resource, scope, events })`
— now the same `Vec` that arrived. `process_batch`'s doc comment gains the `retain_mut` rationale.

### `crates/logit-transforms/src/*.rs` — the implementors (W1)

Twenty `Transform` impls move with the signature. None of them ever constructed a *different*
event, so each is mechanically `mut event: Event` → `event: &mut Event`, `Some(event)` → `true`,
`None` → `false`. The three that needed more than that:

- **`aggregate.rs`** — `Aggregator::process`'s inherent method takes the same new shape and the
  `Transform` impl stays pure delegation. It already `mem::take`s `event.metrics` and pushes the
  unmerged records back, so the only real change is the verdict line becoming
  `!(event.metrics.is_empty() && event.log.is_none() && event.span.is_none())`. Its test module
  gains a `feed(agg, resource, event) -> Option<Event>` helper so the existing assertions, written
  against `Some`/`None`, don't each need a `let mut` and an `assert!`.
- **`attributes.rs` / `provenance.rs`** — their shared `forward()` helpers drop the now-unused
  `Event` parameter.
- **`signals.rs`** — `strip()` takes `&mut Event`.

Filters (`has_attributes`, `drop_signals`, `has_provenance`, …) end up with a `_event: &mut Event`
they never read; that is accepted rather than split into a second trait.

`lib.rs`'s `chained_pipeline_test` — the crate's end-to-end
`json -> scale -> kv_metrics -> keep -> keep_values -> aggregate` walk — keeps one `mut event`
across the whole chain and `assert!`s each hop forwarded, which is a closer read of what the runtime
does now than the old rebind-per-stage shape was.

### `crates/logit-pipeline/src/runtime.rs` — in-crate test doubles (W1)

Seven `Transform` impls in `runtime.rs`'s own test module move the same way.
`WindowingTransform` — the local stand-in for `Aggregator`, since `logit-pipeline` can't depend on
`logit-transforms` — buffers whole events, so it is the one that needs
`std::mem::replace(event, Event::empty(0, AttrMap::new()))`, leaving the husk for `retain_mut` to
drop. Its doc comment explains why `mem::replace` rather than `mem::take`.

### `crates/logit-cli`, `crates/logit-inputs` — test call sites (W1)

Everything that drives a transform directly rather than through `process_batch` binds a `mut event`
and `assert!`s the verdict instead of rebinding what `process` handed back: `logit-cli`'s
round-trip tests (`collectd_round_trip.rs`, `graphite_round_trip.rs`, `prometheus_round_trip.rs`,
`statsd_round_trip.rs`), `logit-inputs`' `statsd_to_aggregate.rs`, and `logit-cli/src/pipeline.rs`'s
own test module (the `trace_context`/`scale` build-and-run cases). No production code in either
crate calls `Transform::process` directly, so nothing outside a test module moves here.

### `crates/logit-bench` — benches and pins (W1)

`src/fixtures.rs`: `nginx_event()` builds its event into a `mut` binding and asserts both hops
(`json`, `kv_metrics`) forwarded, instead of threading the returned `Option` through.

`benches/pipeline.rs`: the per-transform benches (`json_parse`, `json_parse_wide`, `logfmt_parse`,
`kv_parse`, `kv_metrics`, `keep`, `aggregate_absorb`) move from `bench_local_values` to
`bench_local_refs`. `aggregate_absorb`'s `with_inputs` closure and `full_chain`'s loop build their
event into a `mut` binding and `assert!` the hop forwarded.

`tests/allocations.rs`: every `process_batch` pin drops by exactly one — through `keep`, through
`set` (attributes only), through `has_attributes`, dropping every event, fully absorbed, with live
telemetry, and both `trace_context` rows all go **1 → 0**. The first call after an `internal` drain
goes **3 → 2**; `fan_out_plus_two_has_attributes_for_the_same_split` goes **196 → 194**. Each
affected doc comment explains the `0` instead of explaining the `Vec`. The per-event `*_one_event`
pins are unchanged: they call `process` directly and never allocated a batch `Vec`.

### Docs (W1)

`docs/design/memory.md`: §2's "Runtime" table rows and their notes, §2's fastest/allocs table,
§3's deferral paragraph (now "since landed", pointing at the ADR), §3's routing comparison (196 →
194), and §8 item 14 struck through as **Done**. `docs/design/internal-telemetry.md`: the
`dropped{reason="absorbed"}` row's trigger becomes "`Transform::process` returned `false`".

## Workstreams

| # | PR | Branch → target | Files | Depends |
|---|---|---|---|---|
| W0 | **ADR and plan.** | `inplace/w0` → `main` | `docs/adr/in-place-transform-process.md` (new, + row atop `docs/adr/README.md`); `docs/plans/in-place-transform-process.md` (new, + row atop `docs/plans/README.md`) | — |
| W1 | **The trait, the batch loop, every implementor, the benches, the pins, and the `memory.md` rows.** | `inplace/w1` → `inplace/w0` | `crates/logit-pipeline/src/transform.rs`; `crates/logit-pipeline/src/runtime.rs`; `crates/logit-transforms/src/*.rs` (15 files, `lib.rs` included); `crates/logit-cli/src/pipeline.rs`; `crates/logit-cli/tests/*_round_trip.rs`; `crates/logit-inputs/tests/statsd_to_aggregate.rs`; `crates/logit-bench/benches/pipeline.rs`; `crates/logit-bench/src/fixtures.rs`; `crates/logit-bench/tests/allocations.rs`; `docs/design/memory.md`; `docs/design/internal-telemetry.md` | W0 |

Landing order: **W0 → W1**, strictly linear, each PR based on and targeting its parent's branch and
brought up to date with `git merge origin/main` (never rebase). One implementation PR, not several:
`Transform` is a trait, so a signature change that lands without every implementor doesn't compile
— there is no smaller unit that builds. The bench repin travels with it for the same reason
(`allocations.rs` asserts exact counts, so it fails the moment the `out` `Vec` is gone), as do the
`memory.md` rows those pins are the source of truth for.

### Status (2026-09-17)

W1 is implemented and measured (the ADR's "Measured" section carries the numbers, taken against
`main` at `b5c820a`). Both workstreams are open as a linear stack of PRs, each targeting its parent:
W0 PR #232 (`inplace/w0` → `main`), W1 PR #233 (`inplace/w1` → `inplace/w0`). Nothing merged; Ross
directs merging.

### Per-workstream detail

**W0** — Done when: `docs/adr/in-place-transform-process.md` and
`docs/plans/in-place-transform-process.md` exist, both README index tables have their row dated
2026-09-17 and in created-date order, and every cross-reference resolves.

**W1** — Tests: no new test *cases* are owed; the change is semantics-preserving, so the existing
suite is the guard, and the work is to keep it green through a trait signature change that touches
twenty implementors. Specifically, these must pass unmodified in intent:
`a_mutation_on_one_fan_out_branch_is_invisible_to_the_sibling_branch` (branch isolation is not this
change's to weaken); the shutdown-triggered close-time flush test that `WindowingTransform` exists
for (an absorbing transform must still buffer whole events and emit them from `flush`); every
`aggregate` window test, via the `feed` helper; and `crates/logit-core/tests/type_sizes.rs`, which
must not change at all — `Event` is the same 864 bytes, it is simply no longer copied. Allocation
pins are re-pinned to the new counts, never relaxed. Done when: `script/cibuild` green, the
allocation pins assert **0** for the ordinary `process_batch` rows, and `docs/design/memory.md`
matches what `allocations.rs` asserts line for line.

## Verification

- `script/cibuild` before each PR — the whole suite, including `allocations.rs`'s exact counts and
  `type_sizes.rs`.
- `script/bench runtime` for the node-runtime module's throughput and allocation columns
  (`process_batch_through_keep`, and the `fanout_send_*`/`send_batch_*` controls beside it), plus
  the per-transform benches. Run pinned (`taskset -c 2`) and read the *fastest* column, per
  `docs/design/memory.md`'s own convention; fastest of three runs is what the ADR's table quotes.
- `script/perf run --repeat 3 --scenario passthrough --scenario json-parse --scenario
  json-parse-x3 --scenario logfmt-parse --scenario aggregate`, on both sides of the change, pinned
  (`taskset -c 0-3,12-15`), and `script/perf compare <before.json> <after.json>` for the diff.
  `passthrough` is the control: it has no transform node, so it must not move.
- `script/perf attribute --scenario json-parse` / `--scenario json-parse-x3` / `--scenario
  logfmt-parse` for the per-node µs/event breakdown — the number that says whether a gain is
  actually on the transform node rather than somewhere else in the pipeline.
- Interleave before/after runs rather than batching them: run-to-run drift on the dev box is 1–2%,
  which is the same order as the smaller scenario deltas.

## Open risks

- **The per-transform microbenchmark numbers are not same-harness comparisons.** Moving
  `bench_local_values` → `bench_local_refs` changes divan's own per-iteration overhead (`kv_metrics`
  showed ~−40% ns with an unchanged body). Only `process_batch_through_keep`, `full_chain` and the
  untouched controls compare cleanly; the ADR says so, and anyone quoting the other rows should
  too.
- **The `json`-versus-`logfmt` asymmetry is unexplained.** A `json` node gained 15–20% end to end
  and a `logfmt` node gained nothing measurable, from the same removed `Vec` and the same removed
  memcpy. Worth a `script/perf flamegraph` on both before anyone builds a model of per-hop cost on
  these numbers.
- **Part of the end-to-end gain is allocator churn, not memcpy.** The removed survivors `Vec` was
  ~55 KB per batch per node (64 × 864 B), and peak RSS on `json-parse` fell 139 → 80 MiB alongside
  the CPU drop. That makes the win real but its attribution approximate, and it means the result may
  not transfer cleanly to a deployment with a different batch size or allocator.
- **Filters carry an unused `&mut Event`.** A cosmetic cost of one uniform signature across twenty
  implementors, accepted deliberately; if a future reader reads it as an invitation to mutate in a
  filter, the component's own doc comment is the only thing saying otherwise.
- **The fusion question stays open.** This removes the memcpy/allocation term from the per-hop
  budget and deliberately leaves the channel/scheduler term untouched. Nothing here decides whether
  fusing a linear run of adjacent nodes onto one thread is worth its complexity.
