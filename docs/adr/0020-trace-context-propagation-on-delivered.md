# 0020 — Propagate real trace context on `Delivered`, for the node kinds with one unambiguous parent

## Status

Accepted.

## Context

`docs/known-gaps.md`'s internal-spans entry gated carrying trace context on `Delivered`
(`crates/logit-pipeline/src/fanout.rs`) on measured evidence, per
[ADR 0017](0017-minimize-allocations-over-event-size.md): a hot-path type change must be decided on
its own evidence, not folded into a metrics change. A dedicated costing exercise (PR #39) built a
throwaway `TraceContext` prototype, measured it against real allocation and throughput coverage of
the node runtime (`crates/logit-bench/tests/allocations.rs`/`benches/pipeline.rs`'s "Runtime"
section, `docs/design/memory.md`), and reverted it. The result: `size_of::<Delivered>()` goes from
32 to 56 (24 bytes, per batch, not per event — a different multiplier than the `Event`/`SpanRecord`
trade ADR 0017 actually settled), zero change to any allocation-count assertion, no attributable
throughput regression once run-to-run noise was accounted for.

That measurement answered "is the type change itself cheap" — yes. It didn't answer, and wasn't
meant to, whether the type is *useful* on its own: the prototype minted an unrelated root context on
every single `Fanout::send` call, with zero relationship between an incoming batch's context and
whatever a node emitted downstream. A trace with no propagation isn't a trace — every hop shows an
unrelated `trace_id`, and nothing stitches a batch's path through the graph back together.

## Decision

**Build real propagation, for the two node kinds where it's unambiguous; leave the rest as a
documented, deliberate gap.**

- **`Transform::process`/`ScriptWorker::process` (the non-flush path).** `process_batch`/`run_lua`'s
  loop calls this once per event within *one* incoming batch, then sends *one* outgoing batch built
  from whatever survived. One incoming batch, one context, one unambiguous parent for everything
  that batch produces — no fan-in problem. `Delivered::context()` (a new, cheap `&self` accessor,
  read before `unwrap_batch` consumes the batch) supplies the parent; `Fanout::send_with_context`
  (new, alongside the existing `send`) mints a [`TraceContext::child`] — same `trace_id`, fresh
  `span_id` — instead of always calling `new_root()`.

- **`run_output`.** Already borrows `&Delivered` without unwrapping (`Output::send(&EventBatch)`,
  [ADR 0016](0016-arc-eventbatch-copy-on-write.md)), so the incoming context is already there to
  read. No new state, no signature change to `Output` itself — there is nothing yet for a sink to
  *do* with the context (see "What this doesn't do," below).

- **`Transform::flush`/Lua's timer-driven `flush()` — deliberately left minting a fresh root, not
  fixed here.** A flush drains state accumulated from however many incoming batches arrived since
  the last tick. `Aggregator`'s `Accumulator` holds only the merged value, with no memory of which
  batches' contexts fed into it — an *n*-to-1 relationship, not 1-to-1, and there is no single
  correct parent to propagate. `SpanRecord.links: Vec<SpanLink>` already exists in the data model
  for exactly this shape (OTel's answer to "influenced by several spans, not descended from one"),
  so the honest design is a bounded set of contributing contexts recorded as links — new state on
  `Accumulator`, new bookkeeping on every `process()` call, a cardinality question of its own. Real
  work, not done in this change. Lua's `flush()` is worse: no accumulator to inspect at all, the
  same shape `docs/known-gaps.md` already accepts for `Resource` stamping
  (`last_resource` tracks whichever resource a Lua component most recently saw, not the correct
  one for a flush-driven emission either).

`Fanout::send`/`send_blocking` keep their existing signatures and behavior (mint a root) — every
listener (`Input::run` never receives a `Delivered`, so has no parent to inherit) and every
flush-driven call site is unaffected by this change, no call-site updates needed. Only the two new
methods, `send_with_context`/`send_blocking_with_context`, and their two call sites, are new.

## What this doesn't do

- **No `SpanRecord` is emitted anywhere.** Propagation gets the *right* context threaded to the
  *right* place; nothing yet decides when a node's visit to a batch becomes a real
  `SpanRecord`-carrying `Event`. That's a separate, mostly-independent piece — analogous to how
  `internal`'s `ComponentBuffer`/drain mechanism turns counters into events
  (`docs/design/internal-telemetry.md`) — routed through the same `internal` component once built
  ([ADR 0018](0018-internal-telemetry-as-pipeline-events.md) already named it free to grow this
  way, no rename needed).
- **No sampling.** Span volume would be a different shape than metric volume once emission exists —
  potentially one span per node-visit per batch. `internal` will likely need its own knob for this,
  separate from its drain `interval`.
- **The flush *n*-to-1 problem is not solved**, per the Decision section above — tracked in
  `docs/known-gaps.md`'s internal-spans entry, not silently dropped.
- **No Lua span proxy, no `otlp_out`.** Both downstream of whether spans get emitted at all.

## Alternatives considered

- **Propagate everywhere, including flush, by picking an arbitrary contributing batch as "the"
  parent.** Rejected: silently wrong is worse than visibly incomplete. An arbitrary pick would look
  like a real parent to anything consuming it later, with no signal that the relationship is
  fabricated — exactly the kind of approximation this project's stance (see the interner and
  Lua-resource-stamping gaps in `docs/known-gaps.md`) treats as worth naming, not hiding.
- **Change `Fanout::send`/`unwrap_batch`'s existing signatures** rather than adding
  `send_with_context`/`Delivered::context()` alongside them. Rejected: would force every existing
  call site (every `Input` impl, every flush path, every test constructing a `Delivered` or calling
  `unwrap_batch`) to thread a context through whether or not it has one to propagate, for a change
  that only two call sites actually need. Additive methods keep the blast radius to exactly the
  code that changed behavior.
- **Defer this whole change until `SpanRecord` emission is designed**, so propagation and emission
  land together. Rejected: propagation is independently measurable and testable (proven — see
  Verification), emission is a separate, larger design question (sampling, the `internal` wiring,
  the flush *n*-to-1 problem), and there's no correctness reason the two must land in one change.
  Landing propagation first, with its own tests proving `trace_id` survives a hop and `span_id`
  changes at each one, de-risks emission's eventual design by settling this question first.

## Consequences

- `size_of::<Delivered>()` is now permanently 56 (was 32) — pinned by
  `crates/logit-pipeline/src/fanout.rs`'s own test, not just measured once and left unguarded.
- `Fanout` gains `send_with_context`/`send_blocking_with_context`; `Delivered` gains `context()`.
  All additive — no existing public signature changed.
- `run_transform`/`run_lua`'s process (non-flush) paths now propagate; their flush paths, and
  every `Input` impl, are textually unchanged and still mint fresh roots.
- `crates/logit-pipeline/src/fanout.rs`'s test module directly verifies the propagation contract
  (`child` keeps `trace_id`, mints a fresh `span_id`; a real fan-out gives every branch the
  identical child context); `crates/logit-pipeline/src/runtime.rs`'s test module verifies
  `run_transform`'s wiring specifically, both the propagating and the deliberately-non-propagating
  path, by calling the private `run_transform` fn directly — nothing yet surfaces a propagated
  context past this crate (`Output::send` still takes `&EventBatch`, not `&Delivered`), so an
  end-to-end test through `run()` has nowhere to observe it.
- Every existing allocation-count assertion in `crates/logit-bench/tests/allocations.rs` (from
  before this change) held exactly, re-confirmed against the real (not prototype) implementation —
  the same numbers PR #39's evidence predicted.
