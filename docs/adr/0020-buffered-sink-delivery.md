# 0020 — Buffered, decoupled sink delivery

## Status
Accepted

## Context

`docs/known-gaps.md` names two entries that turn out to be one problem. **Output buffering**:
`crates/logit-proto/src/buffer.rs` defines a `Buffer` trait, deliberately written ahead of any
caller, with no implementation, no ack/retry hooks, and no delivery guarantee. **Delivery I/O is
not decoupled from event processing within a node**: `run_output`
(`crates/logit-pipeline/src/runtime.rs`) awaits `Output::send` inline in its drain loop, so a slow
or retrying sink stops draining its own inbox for as long as `send` takes.

A bounded `Buffer` sitting in front of an inline `send` is not a meaningful step toward the first
entry, because it is indistinguishable in practice from the `mpsc(64)` inbox `run_output` already
has: both fill when the sink is slow, both then backpressure the producer, and a buffer's only
extra capability — dropping instead of blocking — is pure data loss with nothing bought when
nothing is concurrently making progress on delivery while the buffer holds data. The buffer's
entire value comes from the consumer being asynchronous with respect to the producer. ADR 0013
anticipated this directly, naming "Implement `Buffer` now" as the rejected-for-scope alternative
and calling it *"the actual fix for the retry-vs-backpressure tension."* This ADR is that fix,
scoped to the sink side of the decoupling gap — the listener side (`StatsdInput::run` still
interleaving `recv_from`/decode/`Fanout::send`) is untouched and stays a documented, open gap.

`docs/plans/0003-buffered-sink-delivery.md` is the workstream breakdown; this ADR records the
decisions that breakdown depends on.

## Decision

### The queue is shared state, not a second channel

`run_output` splits into a drain half (moves `Delivered` off the inbox into a queue) and a writer
half (delivers from the queue, with retry), joined as two futures in one task. A second `mpsc`
cannot serve as the queue: `OverflowPolicy::DropOldest` requires evicting the *head* from the
producer side, which no `mpsc::Sender` operation can do, and the ack shape below (`peek` without
removing) has no channel equivalent either. The queue is therefore `Arc<Mutex<InMemoryBuffer>>`
plus `tokio::sync::Notify` (`SinkQueue`, `crates/logit-pipeline/src/sink_queue.rs`) —
`std::sync::Mutex`, not tokio's, since every critical section is a `VecDeque` push/pop with no
`.await` inside it.

### The item held is `Arc<EventBatch>`

`Output::send` takes `&EventBatch`, not an owned one, specifically so `run_output` never needs
`Arc::try_unwrap`/clone on a `Delivered::Shared` branch (ADR 0016's copy-on-write design). A
buffer holding owned `EventBatch` would force exactly that clone back in. `Arc<EventBatch>`
preserves the zero-clone property for a fan-out sink: `Delivered::Owned` costs one new `Arc::new`
(previously zero — a real, measured cost, tracked in Consequences below);
`Delivered::Shared(Arc<EventBatch>)` costs a refcount bump, exactly as today.

### The ack shape is `peek`/`commit`, not `push`/`pop`

`Buffer::pop` removes an item before delivery is confirmed — if `send` then fails, the batch is
already gone. The reshaped trait separates looking at the head from removing it:

```rust
pub trait Buffer<T> {
    fn push(&mut self, item: T) -> PushOutcome<T>;
    fn peek(&self) -> Option<&T>;      // does not remove
    fn commit(&mut self) -> Option<T>; // removes the head, only once delivery succeeded
    fn len(&self) -> usize;
    fn weight(&self) -> u64;
}
```

This is the whole of the "ack/retry hooks" the trait's existing `TODO` names, and the whole of
in-process at-least-once: a batch stays at the head, retried, until `send` returns `Ok`. It is
deliberately in-order and single-in-flight (one queue, one head) — out-of-order acks across
several in-flight batches are a real future need for the native wire protocol's credit-based flow
control (`docs/design/wire-protocol.md`), but there is exactly one writer per sink queue here, so
building that generality now would be speculative. `push` also changes shape:
`Result<(), EventBatch>` cannot express a `DropOldest` eviction (the evicted item vanishes with no
way to count it), so it becomes a three-way `PushOutcome<T> { Accepted, Evicted(T), Rejected(T) }`.
`OverflowPolicy::Block` leaves the trait entirely — a sync trait cannot block usefully — and
becomes a `SinkQueue`-level concern layered on top of a trait that implements only the two
dropping policies.

### Delivery posture is a per-sink policy, chosen in three layers

Observability data is usually fine best-effort; audit-shaped logging is not. Rather than pick one
guarantee for every sink, the mechanism is always at-least-once-capable (the `peek`/`commit` shape
above), and whether a given sink *uses* that is a policy decision made in three layers, each owning
what only it can know:

- the **sink** reports a fact, `Output::duplicate_safe() -> bool` (default `false`) — `true` for
  `InfluxDbOutput`: its line-protocol encoder derives point timestamps from `event.timestamp`
  (`influxdb.rs`) and its per-batch collision-disambiguation map is reset on every `encode` call,
  so re-encoding and re-sending a buffered batch is byte-for-byte identical to the first attempt,
  and InfluxDB treats an identical `(measurement, tag set, field key, timestamp)` write as an
  idempotent overwrite, not a second point. `false` for `StdioOutput` — a duplicated line is a
  duplicated line, no destination-side idempotency exists to lean on;
- the **runtime** derives a default posture from that fact: `duplicate_safe` → `at_least_once`,
  otherwise `at_most_once`;
- **config** (`buffer.delivery:`, workstream F) overrides it per component, for an operator who
  knows their specific destination's behavior better than the sink's own default.

Posture decides which failures are worth retrying, which is what actually bounds duplicate risk —
not a blanket "retry everything" or "retry nothing." Only the sink can tell whether a failure means
the destination provably never received the batch, so that classification travels back out of
`send` as an `anyhow` context marker (`Fault`, attached via `.context(...)`, read back via
`anyhow::Error::downcast_ref::<Fault>()` — not `err.chain().find_map(|e| e.downcast_ref::<Fault>())`,
which looks like the obvious spelling but doesn't work: each link `chain()` yields is a `&dyn
std::error::Error` whose *concrete* type is anyhow's own internal context-wrapper, not `Fault`
itself, so the standard `dyn Error::downcast_ref` never matches. `anyhow::Error::downcast_ref` is a
different, anyhow-specific inherent method that knows how to look inside its own context wrapper,
including through further `.context(...)` layers stacked on top later) rather than a signature
change — `StdioOutput` needs no changes beyond its default `duplicate_safe() -> false`:

```rust
pub enum Fault { Clean, Ambiguous, Permanent }
```

| Fault | Meaning | Retried under `at_most_once` | Retried under `at_least_once` |
|---|---|---|---|
| `Clean` | destination never saw it (connect refused, DNS failure) | yes | yes |
| `Ambiguous` | may have committed before the response was lost (timeout, 5xx, 429) | no | yes |
| `Permanent` | a config error (4xx other than 429) | no | no |
| unclassified | — | no | no |

An unclassified error defaults to `Permanent`: never retry a failure the sink didn't recognize.
This is also why an `at_most_once` sink is not "no retry at all" — the common outage shape (a
destination restarting) surfaces as `Clean`, and is retried under both postures with zero
duplicate risk, since the connection never succeeded in the first place.

### Retry moves behind the queue boundary and its budget widens

Retry relocates from `InfluxDbOutput::send`'s own loop into a generic `deliver_with_retry` in
`logit-pipeline`, driven by the writer per the resolved posture and `Fault`. `InfluxDbOutput` keeps
its classification helpers (`is_retryable_status`, `attempt_timeout_for`, `backoff_for` — already
split out and independently tested) but loses the loop itself and its `RetryPolicy`/`with_retry`
builder; every current and future sink gets retry for free rather than reimplementing it.

**This revises, without superseding, ADR 0013's retry-budget decision.** 0013 set the default
`total_budget` to ~5 seconds *specifically because* delivery wasn't decoupled: a longer stall
reached the drain loop, then the inbox, then backpressured the listener, and the kernel started
dropping UDP datagrams. With the queue absorbing that stall instead, the same reasoning now argues
the other way — a **60-second** default rides out a real destination restart instead of only a
blip, without touching intake at all. 0013's shutdown-signal and diagnostics decisions are
unaffected and remain in force; only its retry-budget rationale is revised, noted at that ADR's
retry section rather than marking it Superseded.

### Failure handling: the process no longer exits on a sink failure by default

Today, any `send` error `run_output` can't classify as retryable ends `logit run` outright — every
buffered batch across every sink in the node is lost with it, which is indefensible once a sink is
deliberately holding unwritten data. New rule:

- **Retryable** (per `Fault` and posture): retried within `retry_budget`; the batch stays at the
  queue's head throughout.
- **Budget exhausted:** the batch is committed off the queue (dropped), counted
  (`batches.dropped{reason="send_failed"}`), warned via `Diagnostics::warn_throttled`, and the
  writer moves to the next batch. No process exit — a sink that cannot reach its destination
  degrades to dropping, it does not take every other sink in the node down with it.
- **Permanent:** drop and continue, *except* exit the process if permanent failure has been the
  sole outcome for a fixed ~60-second window with no intervening success. A genuinely misconfigured
  sink (bad token, bad bucket) still fails loudly enough for a restart-policy supervisor to notice;
  one malformed batch cannot kill an otherwise-healthy pipeline.

### `Output::flush` and a bounded shutdown grace

`Output` gains `async fn flush(&mut self) -> anyhow::Result<()> { Ok(()) }`, default no-op — this
closes ADR 0013's residual "no `Output` close hook" gap, now load-bearing since a sink can hold
unwritten data at shutdown, without requiring any existing sink to implement anything. The writer
takes the same `watch::Receiver<bool>` shutdown signal `run_input` already races against; once it
flips, the writer's remaining drain time is capped at a configured `shutdown_grace` (default 5s),
so a permanently-down sink cannot hang process exit indefinitely under SIGTERM. On expiry: log the
undelivered count, count `batches.dropped{reason="shutdown"}`, call `output.flush()`, return `Ok`
— an incomplete drain on shutdown is expected behavior, not a pipeline failure.

### Bounding is both batches and bytes

`docs/design/memory.md` §5 already names the `CHANNEL_CAPACITY=64` inbox's exact problem: it
bounds batches, not bytes, and batch size is unbounded. A queue several times deeper needs the
byte-aware bound the inbox never got. `EventBatch::estimated_heap_bytes()` (new,
`crates/logit-core`) is a deliberately approximate O(events) walk, computed once at push time and
stored beside the item — an admission-control heuristic, explicitly exempt from the exact-`size_of`/
exact-allocation-count discipline `type_sizes.rs`/`allocations.rs` enforce elsewhere, which is
about what `Event` costs to move, not about bounding a queue's rough footprint.

## Alternatives considered

- **At-most-once only, no ack hooks.** Rejected: if the writer retries at all, it must hold the
  in-flight batch across attempts somewhere — either in a local variable (`pop`-then-retry) or in
  the queue (`peek`-then-`commit`). Same bytes, same lifetime; the only difference is who owns the
  bookkeeping, and buffer-ownership is strictly better since the in-flight batch then shows up in
  depth telemetry and in both drain paths (sibling failure, shutdown) for a few dozen extra lines.
  Building the weaker case would also be the wrong irreversible choice — retrofitting an ack
  boundary onto call sites that assumed fire-and-forget is the expensive direction this trait was
  already written ahead of time to avoid.
- **A single fixed delivery guarantee for every sink.** Rejected: observability data and
  audit-shaped logging have genuinely different loss tolerances, and only the sink (or the
  operator, via config) can say which a given destination is. A fixed choice would either force
  duplicate risk onto a sink that can't absorb it or force unnecessary retry cost onto one that
  doesn't need the guarantee.
- **Durable, disk-backed buffering now.** Rejected for this ADR: it needs an `EventBatch`
  serialization, and the payload encoding (`rkyv` vs. hand-rolled,
  `docs/design/wire-protocol.md`) is an explicit, separate, benchmark-gated decision that
  `AGENTS.md` says must not be settled in passing while implementing something else. Left as a
  narrowed, still-open `docs/known-gaps.md` entry, and plausibly config-optional even once it
  lands, since not every deployment needs cross-restart durability.
- **Keep today's fail-fast on any unretryable error.** Rejected: a buffer's entire point is
  surviving what fail-fast doesn't; keeping it would mean a single malformed batch discards every
  other sink's buffered work in the same node.
- **Never exit on a sink failure, under any circumstance.** Rejected: a misconfigured token or
  bucket would then be visible only in metrics and logs, never in the exit code a restart-policy
  supervisor actually watches. The ~60s permanent-failure window is the compromise: fails loudly
  enough to notice, slowly enough that one poison batch can't take down a healthy pipeline.
- **A generic `ack(id)` API supporting several in-flight, out-of-order batches per sink.** Rejected
  as premature: there is exactly one writer per `SinkQueue`, so in-order commit-the-head is the
  honest shape for what exists today. Out-of-order acknowledgement is real future scope for the
  native wire protocol's credit-based flow control, not this boundary.

## Consequences

- `crates/logit-proto/src/buffer.rs`: `Buffer<T>` reshaped to `push`/`peek`/`commit`/`len`/`weight`;
  `PushOutcome<T>`; `OverflowPolicy` narrowed to `DropOldest`/`DropNewest` (`Block` moves to
  `SinkQueue`). `docs/design/wire-protocol.md`'s §Buffering rewritten to match.
- `crates/logit-core`: new `EventBatch::estimated_heap_bytes()`; `docs/design/memory.md` §5 gains
  it as the buffer's bound.
- `crates/logit-pipeline`: new `sink_queue.rs`; `runtime.rs`'s `run_output` splits into
  `drain_inbox`/`write_loop`, raced via `tokio::select!` rather than `tokio::join!` — `write_loop`
  can return early (a permanent send failure) while `drain_inbox`'s inbox is still open, which
  every real listener's is, so `join!` would hang the task waiting on a `drain_inbox` with nothing
  left to learn that its consumer gave up; `select!` lets `run_output` return as soon as
  `write_loop` finishes, dropping a still-pending `drain_inbox` (and its inbox) with it, exactly as
  the pre-split single loop did the moment `output.send` failed permanently. `run_with_telemetry`'s
  join loop stops aborting siblings on the first task error: it records only the *first* error,
  triggers the same shutdown signal SIGTERM already drives, and keeps `join_next`ing until every
  task has actually exited before returning it — a healthy sibling's already-queued `SinkQueue`
  batches are delivered through the ordinary shutdown-grace path rather than discarded by an
  aborted `JoinSet`. New `logit-proto` dependency (cycle-free — `logit-proto` depends only on
  `logit-core`).
- `crates/logit-pipeline/src/output.rs`: `Output` gains `flush` (default no-op) and
  `duplicate_safe` (default `false`); doc comment rewritten — buffering is now the runtime's
  responsibility, not the output's, though a sink still owns fault classification and its
  duplicate-safety fact.
- `crates/logit-outputs/src/influxdb.rs`: loses its retry loop and `RetryPolicy`/`with_retry`
  builder, keeps and reuses its classification helpers; gains `duplicate_safe() -> true`.
- `crates/logit-config`: new `BufferConfig`/`OverflowPolicy`/`DeliveryPosture`, hoisted onto
  `Component`; `schema/logit.schema.json` regenerated.
- Measured cost: one `Arc::new` per batch on the previously-zero-allocation `Delivered::Owned`
  single-consumer path — `crates/logit-bench/tests/allocations.rs`'s constant and
  `docs/design/memory.md`'s table updated in the same commit, per this repo's exact-equality
  discipline.
- `docs/known-gaps.md`: the "Output buffering" entry closes; "Delivery I/O is not decoupled"
  narrows to name only the still-open listener half; three new, narrower entries record what
  remains — no durable buffering, no end-to-end acknowledgement, no out-of-order/credit-based acks.
- `docs/adr/0013-service-lifecycle-and-output-retry.md`: gains a note that this ADR revises its
  retry-budget rationale; 0013's other decisions are unaffected.
