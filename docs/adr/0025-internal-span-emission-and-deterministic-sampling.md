# 0025 — Internal span emission, one span per node-visit, and deterministic-on-`trace_id` sampling

## Status

Accepted.

## Context

[ADR 0020](0020-trace-context-propagation-on-delivered.md) put a real `TraceContext` on every
`Delivered` and gave `Transform::process`/`ScriptWorker::process` (the non-flush path) and
`run_output` a real parent to propagate. A follow-up PR then gave `Transform::flush` a bounded,
best-effort `Vec<SpanLink>` per emitted event (`Aggregator`'s `ContributingContexts`). Neither PR
emits a `SpanRecord`. `docs/known-gaps.md`'s internal-spans entry named the two remaining pieces
explicitly: (1) nothing turns a `(context, node, batch)` visit into a real `SpanRecord`-carrying
`Event`, and (2) span volume needs its own sampling knob, separate from `internal`'s drain
`interval`. This ADR closes both.

Two questions had to be settled before "emit a span" was well-defined at all: **what counts as one
span**, and **how does a sink's span (the one that matters most — it is where `SpanStatus::Error`
and a retry count actually live) get the context it needs, given `run_output`'s inbox is already
decoupled from delivery through a `SinkQueue`** ([ADR 0021](0021-buffered-sink-delivery.md)).

## Decision

### One span is one node's minted `TraceContext` — not one span per batch

The natural-sounding "one span per node's processing of one batch" is wrong in exactly two cases
that matter: `Transform::flush` (an *n*-to-1 emission — `aggregate`'s `process_batch` returns
`None`, so no context is minted on the way in) and a flush that emits several resource groups
(which used to mint several *separate* roots, one per group). The correct invariant is: **the
runtime mints a context unconditionally, exactly once per unit of work, and uses that one context
both as the span's identity and as the context the emission is sent under.**

That needs an additive method on `Fanout` rather than a return value, because a node has to *own*
the minting — it may record a span without sending anything downstream at all (an absorbing
transform visit), and the span's `span_id` and the outgoing `Delivered`'s `span_id` must be the
*same* id:

```rust
pub async fn send_with_own_context(&self, batch: EventBatch, ctx: TraceContext);
pub fn send_blocking_with_own_context(&self, batch: EventBatch, ctx: TraceContext);
```

`send_with_context(b, parent)` is now defined as exactly `send_with_own_context(b, parent.child())`
— the two aren't independent behaviors, just two ways of arriving at the context a send actually
goes out under. `Fanout::send`/`send_blocking` mint a root, open this node's own `SpanKind::Producer`
span around the call, and delegate to `send_with_own_context` — since every other caller that used
to go through `send`/`send_blocking` for a flush-driven emission now mints its own root and calls
`send_with_own_context` directly (see the per-node-kind table below), **a genuine listener is the
only remaining caller of `send`/`send_blocking`**, which is what makes "one call to `send`" and
"one listener emission" the same event, with one span for it.

| Node | `trace_id` | `span_id` | `parent_span_id` | `SpanKind` | Recorded in | Window measured |
|---|---|---|---|---|---|---|
| Listener | fresh root | that root's | none | `Producer` | `Fanout::send`/`send_blocking` | the `send` call only |
| `Transform::process` | inherited | `parent.child()`, minted in `run_transform` | incoming | `Internal` | `run_transform` | `process_batch` + send |
| Lua `process` | inherited | same, minted in `run_lua` | incoming | `Internal` | `run_lua` | `process()` + blocking send |
| `Transform::flush` | fresh root | that root's | none | `Internal` | `run_flush` | `flush()` + every group's send |
| Lua `flush()` | fresh root | that root's | none | `Internal` | `run_lua`'s `flush_now` | `flush()` + send |
| `run_output` | inherited | `ctx.child()`, minted then discarded | incoming | `Client` | `write_loop` | the whole `deliver_with_retry` |

A fan-out (one batch, several downstream consumers) does not multiply spans: a fan-out is one
`send_with_own_context` call by one node producing one context, so the node records one span and
the *N* consumers each mint their own child later, on their own visit. The span belongs to the
*emission*, not the edge.

**Two deliberate behavior changes, each worth stating explicitly:**

- `run_flush` used to mint a fresh root **per resource group**. It now mints **one root before
  `transform.flush(now)`** and sends every group via `send_with_own_context` under that one root —
  one flush is one unit of work, and *N* resource groups are an internal detail of `aggregate`'s
  per-resource windowing ([ADR 0008](0008-aggregation-window-semantics.md)), not *N* hops. The
  links each group's events carry (`Transform::flush`'s `Vec<SpanLink>`) are unioned onto this one
  flush span, bounded the same way any other span's links are (`MAX_LINKS_PER_SPAN`).
- The listener span's window is the `send` call, not decode-to-send. `Fanout::send` genuinely
  cannot know when the listener began building the batch — `Input::run` is a free-form loop, and
  fabricating a start time it doesn't have would be worse than naming the gap. Tracked as a
  residual, not fixed here (see "What this doesn't do").

### The sink span forces `SinkQueue` to carry the context

The only span that can carry `SpanStatus::Error` and be tagged with a `Fault` — the whole point of
instrumenting a sink at all — is the one around `deliver_with_retry`. But `run_output`'s
`drain_inbox` calls `unwrap_batch_arc`, which discarded the `TraceContext` before this PR, and
`write_loop` only ever saw the batch, not what arrived with it. So `SinkQueue`'s entries now carry
the context alongside the batch:

```rust
pub async fn push(&self, batch: Arc<EventBatch>, ctx: TraceContext);
pub async fn peek(&self) -> Option<(Arc<EventBatch>, TraceContext)>;
```

`TraceContext` is `Copy`, 24 bytes — it rides inline in the existing `(item, weight)` queue entry
with **no new allocation**, and `SinkQueue`'s byte accounting (`EventBatch::estimated_heap_bytes`)
is unaffected, since it only ever looks at the batch. `drain_inbox` reads `delivered.context()`
before `unwrap_batch_arc` consumes it — the same idiom `run_transform` already used for its own
parent context. `write_loop` mints `ctx.child()` as this span's own identity (parented on `ctx`
itself) and then discards that child — there is nothing further downstream of a sink to propagate
it to (`Output::send` still takes `&EventBatch`, not `&Delivered` — see "What this doesn't do").

**Confirmed: no `Output` trait change.** `write_loop` gets the context from the queue;
`Output::send(&EventBatch)` is untouched, and `logit-outputs` is not modified by this PR.

### The emit API and the bounded span buffer

Follows [ADR 0018](0018-internal-telemetry-as-pipeline-events.md)'s shape: spans ride the same
`ComponentBuffer` → `Registry::drain` → `internal` path as metrics, so nothing downstream needs to
know it's looking at telemetry rather than user data. A guard type mirrors `Timer` exactly — the
`None`-when-disabled trick is what makes the unsampled path free:

```rust
impl Telemetry {
    pub fn span(
        &self, op: &'static str, kind: SpanKind,
        trace_id: [u8; 16], span_id: [u8; 8], parent_span_id: Option<[u8; 8]>,
    ) -> SpanGuard;
}

#[must_use = "a SpanGuard records nothing until it is dropped or finished"]
pub struct SpanGuard { /* ... */ }
impl SpanGuard {
    pub fn events(&mut self, n: u64);
    pub fn link(&mut self, link: SpanLink);
    pub fn links(&mut self, links: impl IntoIterator<Item = SpanLink>);
    pub fn tag(&mut self, k: &'static str, v: &'static str);
    pub fn error(&mut self);
    pub fn ok(&mut self);
    pub fn finish(self);   // explicit; Drop does the same
}
```

The sample decision (below) is made inside `Telemetry::span`, from `trace_id` alone, before any
span-shaped state exists — an unsampled trace gets the identical `SpanGuard` a disabled handle's
`timer()` returns: every method an immediate no-op, no allocation, no second clock read beyond the
one `trace_is_sampled` comparison.

`ComponentBuffer` gains a **separate**, unkeyed structure for spans — a point's `PointKey` map
(capped at `MAX_KEYS_PER_COMPONENT`) coalesces repeats at the same key, but two spans never share
an identity to coalesce on, so a keyed map is the wrong shape:

```rust
const MAX_SPANS_PER_COMPONENT: usize = 512;   // a volume bound, not a cardinality one -- spans
                                               // never coalesce, so nothing else bounds this
                                               // except drain interval × sample rate
const MAX_LINKS_PER_SPAN: usize = 32;
```

Over-cap is counted, never silent: `logit.internal.spans.dropped{reason="buffer_full"}` on the
buffer (drained alongside the metric pass's own `logit.internal.points.dropped`),
`logit.internal.span.links.dropped{reason="cardinality"}` recorded immediately on the guard —
mirroring `logit.internal.points.dropped{reason="cardinality"}`'s existing shape.

`ComponentBuffer::drain(now)` gained a second pass, emitting `Event::span` alongside its existing
`Event::metric`s. **The one place it must ignore `now`:** a span event is stamped with the span's
own `start`, not the drain time — `Event::timestamp` *is* the span's start (`SpanRecord`'s own doc
comment) — stamping it with the drain time would make every span drift later than reality by
however long it sat in the buffer. Attributes: `logit.node.op`
(`"process"|"flush"|"send"|"deliver"`), `events` as a non-interned `Value::I64` (never a tag, since
a per-span count has no cardinality to bound), then the same `component`/`kind`/`role` identity
attributes the metric pass already stamps. `name` (`"aggregate process"`, `"influxdb_out deliver"`)
is built at drain time — off the hot path, the one place this touches a `String`, since it only
happens for a span that both got sampled and survived to a drain. `kind` is `SpanKind::Internal`
for a transform/Lua visit, `SpanKind::Producer` for a listener, `SpanKind::Client` for a sink's
`deliver` span; `status` defaults `Ok` and becomes `Error` only when a call site's own error path
fires (`write_loop`'s `Delivery::Dropped`, a Lua flush error) — a node visit that completes with no
explicit error is a success, the same default every shipped call site relies on.

`InternalInput::tick` needs **no structural change** — `Registry::drain` already returns
`Vec<Event>`, and span events ride the same batch as metric events. That is exactly what ADR 0018's
naming section promised ("`internal` may grow logs and spans later without a rename"). One counting
change: `tick` used to count its whole drained batch as `logit.internal.points.emitted`, accurate
back when every drained event was a metric point; now that a drain can carry span events too, that
name would misdescribe a span as a "point." `tick` now splits the drained batch by shape and
reports `logit.internal.points.emitted`/`logit.internal.spans.emitted` separately, each only when
nonzero — symmetric with `points.dropped`/`spans.dropped`'s existing shape.

### The sampler: deterministic on `trace_id`, no propagated bit

Deterministic on `trace_id`, so every node — and every `logit` process in a split-collection
topology (`docs/OVERVIEW.md`) — reaches the same keep/drop verdict independently, with **no
propagation and no extra bytes on `TraceContext`/`Delivered`**: a kept trace is kept at every hop,
a dropped one dropped at every hop, without any node ever telling another its answer. Same shape as
OTel's `TraceIdRatioBased` sampler:

```rust
pub fn trace_is_sampled(trace_id: &[u8; 16], rate: f64) -> bool {
    if !(rate < 1.0) { return true; }   // also catches NaN -- keep, never silently drop everything
    if rate <= 0.0 { return false; }
    let x = u64::from_be_bytes(trace_id[8..16].try_into().expect("8 bytes"));
    (x >> 11) < (rate * (1u64 << 53) as f64) as u64
}
```

The top 53 bits of the low 8 `trace_id` bytes, not all 64: `rate * 2f64.powi(64)` loses precision
near 1.0, which would reject traces it should keep; 53 bits is `f64`'s exact-integer range, so the
comparison is exact, not an approximation of one.

The rate lives on `Registry` (process-wide, set once — graph rule 13 already guarantees at most one
`internal` component) and is copied into each `ComponentBuffer` at construction, so `span` never
needs a second lock:

```rust
impl Registry {
    pub fn new() -> Arc<Self>;                          // unchanged: rate = DEFAULT_SPAN_SAMPLE_RATE
    pub fn with_span_sampling(rate: f64) -> Arc<Self>;   // new
}
pub const DEFAULT_SPAN_SAMPLE_RATE: f64 = 0.1;
```

`crates/logit-cli/src/pipeline.rs::prepare` (which already gates building a `Registry` at all on an
`Internal` component being present) now reads `span_sample_rate` off that component and calls
`Registry::with_span_sampling` instead of always taking the default.

Config, on `ComponentKind::Internal`:

```rust
Internal {
    interval: Duration,
    #[serde(default = "default_span_sample_rate")]   // logit_core::DEFAULT_SPAN_SAMPLE_RATE
    span_sample_rate: f64,
}
```

Named `span_sample_rate`, not `sample_rate` — there is already a `ComponentKind::Sample` transform,
and `internal` may grow other sampling knobs later. Below `1.0` by default: span volume is a
different shape than metric volume (one span per node-visit per batch, where a metric point
coalesces between drains).

**Graph validation rule 16** (`crates/logit-pipeline/src/graph.rs::resolve`, next after rule 15):
rejects a non-finite or out-of-`[0, 1]` `span_sample_rate` as a config error, not something to
clamp silently — `trace_is_sampled` treats NaN as "keep everything," which would be a surprising
thing to get from a typo rather than a deliberate "sample everything" choice.

## What this doesn't do

- **The listener span's window is `send` only, not decode-to-send** — `Fanout::send` has no
  visibility into how long a listener spent building the batch it's about to send
  (`docs/known-gaps.md`'s "delivery I/O is not decoupled" entry names the listener-side half of
  this as still open).
- **Lua `flush()` still gets a link-less root.** There is no accumulator on the Lua side `logit`
  can inspect for contributing contexts, the same limitation `docs/known-gaps.md` already accepts
  for `Resource` stamping (`last_resource`) — this PR gives it a real span, but not real links.
- **A `SinkQueue` entry is 24 bytes larger** — `TraceContext` inline, same trade `Delivered` itself
  already made and measured (`docs/design/memory.md`'s "Costing internal spans" section).
- **No OTLP.** This closes item 1 of `docs/known-gaps.md`'s internal-spans list (emission); nothing
  here exports a span anywhere. `stdio_out` (which already renders a `SpanRecord` in full) is the
  only consumer today.

## Alternatives considered

- **One span per node's processing of one batch, uniformly.** Rejected: wrong for `Transform::flush`
  (an *n*-to-1 emission with no single incoming batch) and for a multi-resource-group flush (which
  would otherwise mint *N* unrelated roots for what is really one unit of work) — see "One span is
  one node's minted `TraceContext`" above.
- **A propagated `sampled` flag on `TraceContext`, OTel-W3C-style.** Rejected in favor of a
  deterministic sampler: a flag would grow `TraceContext`/`Delivered` further (`Delivered` already
  grew once, per ADR 0020) for no benefit here — a deterministic-on-`trace_id` sampler gets the same
  "every hop agrees" property for free, at zero additional bytes, precisely because every node can
  compute the same answer independently.
- **Emitting the sink span from `drain_inbox` instead of `write_loop`.** Rejected: `drain_inbox`
  never sees the delivery outcome (success, retry count, `Fault`) — only `write_loop`, wrapping
  `deliver_with_retry`, has what the sink span exists to record in the first place.

## Consequences

- `size_of::<Delivered>()` stays exactly 56 (unchanged from ADR 0020) — the whole point of the
  deterministic-on-`trace_id` sampler is that it needs no propagated bit, so `TraceContext`/
  `Delivered` gain nothing from this PR.
- Every allocation-count assertion in `crates/logit-bench/tests/allocations.rs` (`fanout_send_*`,
  `unwrap_batch_*`, `process_batch_*`, `send_batch_*`) held exactly, unmodified, re-confirmed
  against the real implementation — every `SpanGuard` on a disabled or unsampled handle is
  `Option::None`, so the unsampled path allocates nothing beyond what those tests already measured.
- `SinkQueue`'s queue entry grows by `TraceContext`'s 24 bytes; `push`/`peek`'s signatures change
  (additive in spirit, but not source-compatible — both call sites in this crate were updated).
- `Fanout` gains `send_with_own_context`/`send_blocking_with_own_context`; `send_with_context`/
  `send_blocking_with_context` are redefined in terms of them, same observable behavior.
- `docs/known-gaps.md`'s internal-spans entry closes items 1 and 2 (emission, sampling); the "What
  this doesn't do" residuals above are recorded there as the new open list.
