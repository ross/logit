# Closing plan: buffered, decoupled sink delivery

`docs/known-gaps.md` carries two entries side by side: **output buffering**
(`crates/logit-proto/src/buffer.rs`'s `Buffer` trait has no implementation) and **delivery I/O is
not decoupled from event processing within a node** (`run_output` awaits `Output::send` inline,
so a slow or retrying sink stops draining its own inbox). Investigating the first turns up that it
cannot be closed without the second: a bounded buffer in front of an inline `send` is
behaviourally the same thing one hop deeper, since nothing is concurrently making progress on
delivery while the buffer holds data. This plan closes both together — the sink half of the second
entry; the listener half (`StatsdInput::run` still interleaving `recv_from`/decode/`Fanout::send`)
is untouched and stays open.

**Scope.** A sink keeps accepting batches while a write is in flight or backing off, up to a
configured bound; what happens on overflow and on failure is an explicit, per-sink, config-visible
choice; a transient destination outage is ridden out instead of ending the process. It stops at
in-process delivery — durable/disk-backed buffering (surviving a restart) is explicitly deferred,
both because `wire-protocol.md`'s rkyv-vs-hand-rolled encoding decision is a separate,
benchmark-gated ADR this plan must not settle in passing, and because it is a large enough
extension to warrant its own plan once the in-memory boundary exists to build it behind.

Each workstream below is independently reviewable (its own PR, `AGENTS.md`'s branch-and-PR
workflow); later ones depend on earlier ones per the graph below. Read the design docs a workstream
references before starting it — this plan sequences work, it doesn't re-derive design that belongs
in an ADR or a design doc.

## The core design decision: posture is per-sink, not fixed

Observability data is usually fine best-effort — drop it and move on. Audit-shaped logging is not.
Rather than pick one guarantee for every sink, the internals are built for the *stronger* case (a
batch held until delivery is confirmed) and individual sinks default to the weaker one only when
duplicates would actually hurt them. Building the internals best-effort would be the wrong
irreversible choice: retrofitting an ack/retry boundary onto call sites that assumed fire-and-forget
is exactly the expensive direction the `Buffer` trait was written ahead of time to avoid.

Three layers, each owning what only it can know:

- the **sink** reports a fact, `Output::duplicate_safe() -> bool` — `true` for `InfluxDbOutput`
  (its line-protocol encoder makes a re-delivered batch an idempotent overwrite of the same
  `(measurement, tags, timestamp)`), `false` for `StdioOutput` (a duplicated line is a duplicated
  line);
- the **runtime** derives a default posture from it (`duplicate_safe` → `at_least_once`,
  otherwise `at_most_once`);
- **config** (`buffer.delivery:`) overrides it per component.

Posture, in turn, decides which failures are worth retrying — which is what actually bounds
duplicate risk, not a blanket "retry or don't." Only the sink can tell whether a failure means the
destination provably never saw the batch, so that classification travels back out of `send`:

```rust
// crates/logit-pipeline/src/output.rs
pub enum Fault { Clean, Ambiguous, Permanent }
```

| Fault | Meaning | Retried under `at_most_once` | Retried under `at_least_once` |
|---|---|---|---|
| `Clean` | destination never saw it (connect refused, DNS) | yes | yes |
| `Ambiguous` | may have committed (timeout, 5xx, 429) | no | yes |
| `Permanent` | config error (4xx other than 429) | no | no |
| unclassified | — | no | no |

An `at_most_once` sink still rides out the common outage shape — a destination restarting, which
surfaces as connection-refused — with zero duplicate risk, because `Clean` retries under both
postures.

## Decisions already settled

| Question | Decision |
|---|---|
| Item type held in the buffer | `Arc<EventBatch>` — an owned `EventBatch` would force the `Arc::try_unwrap`/clone ADR 0016 exists to avoid on a shared `Delivered::Shared` branch |
| Ack shape | `peek`/`commit` (head stays until delivery returns `Ok`), not `push`/`pop` — in-order, single in-flight batch per sink; out-of-order acks stay deferred to the wire protocol's credit-based flow control |
| Where the queue lives | `Arc<Mutex<InMemoryBuffer>> + Notify`, not a second `mpsc` — `DropOldest` needs to evict the head from the producer side, and `peek`-without-remove has no channel equivalent |
| `Block` | not part of the `Buffer` trait (sync trait, can't block) — a `SinkQueue`-level concern; the trait implements only the two dropping policies |
| Retry ownership | relocates from `InfluxDbOutput::send` into a generic writer loop in `logit-pipeline`; sinks keep only fault *classification* |
| Retry budget | widens from 5s to 60s (ADR 0013's ~5s was deliberately tight *because* delivery wasn't decoupled) |
| Config placement | `buffer:` hoisted onto `Component`, not repeated per sink `ComponentKind` variant |
| Durable buffering | out of scope this plan; left for future exploration, and likely config-optional even then |

## Gaps this plan exists to schedule

| Gap | Consequence |
|---|---|
| `Buffer` (`crates/logit-proto/src/buffer.rs`) has no implementation | `docs/known-gaps.md`'s "Output buffering" entry |
| `run_output` awaits `Output::send` inline | the sink half of "Delivery I/O is not decoupled"; a slow/retrying sink backpressures every branch sharing its upstream |
| `InfluxDbOutput`'s retry budget is ~5s, deliberately tight | ADR 0013: "riding out a real outage without dropping intake needs delivery decoupled from the drain loop" |
| A permanent `send` error ends `logit run` | one malformed batch or misconfigured sink takes the whole node down, discarding every other sink's in-flight state |
| `run_with_telemetry`'s join loop drops the `JoinSet` on first error | siblings' buffered batches would be lost mid-flight once buffering exists — today theoretical, becomes real here |
| `Output` has no close/flush hook | ADR 0013's residual gap; becomes load-bearing once a sink can hold unwritten data at shutdown |
| `CHANNEL_CAPACITY` bounds batches, not bytes | `docs/design/memory.md` §5; the new buffer is ~16× deeper, so it needs the byte-aware bound the inbox never got |

## Reference architecture

What workstream C builds, replacing `run_output`'s single inline loop:

```
        inbox (mpsc<Delivered>)
              │
              ▼
        drain_inbox ──push──▶  SinkQueue  ◀──peek/commit── write_loop ──▶ Output::send
        (async task)          (Arc<Mutex<                 (async task,      (with retry,
                                InMemoryBuffer>>            same run_output   Fault-classified)
                                + Notify)                   future, raced
                                                             via tokio::select!
                                                             -- not join!, so
                                                             write_loop return-
                                                             ing early drops a
                                                             still-open drain_
                                                             inbox rather than
                                                             hanging on it)
```

`drain_inbox` never awaits `Output::send`; `write_loop` never touches the inbox. The only shared
state is the queue, so a slow or backing-off write no longer stops the drain from moving batches
out of the bounded inbox and into the (deeper, byte-aware) buffer.

## Workstream dependency graph

```
A ──┬── C ──┬── D ──┐
B ──┘        └── E ──┼── F ── G
                      ┘
```

A (trait reshape) and B (byte estimator) are independent and can start in parallel. C depends on
both. D (retry/fault/flush/shutdown) and E (drain-all-on-error) each depend only on C and can run
in parallel. F (config/schema) needs D and E landed. G (telemetry + known-gaps) needs F.

---

## A. `Buffer` trait reshape + in-memory implementation

**Goal:** a tested, tokio-free bounded buffer with an ack shape a decoupled writer can actually use.

**Depends on:** nothing.

**Decisions to record:** draft **ADR 0021** here (Status: Proposed) so later workstreams can cite
it; rewrite `docs/design/wire-protocol.md`'s §Buffering to the reshaped trait.

**The change.** `crates/logit-proto/src/buffer.rs`:

```rust
#[must_use]
pub enum PushOutcome<T> {
    Accepted,
    /// Accepted, but the head was evicted to make room (`OverflowPolicy::DropOldest`).
    Evicted(T),
    /// Not accepted; the item is handed back (`OverflowPolicy::DropNewest`).
    Rejected(T),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy { DropOldest, DropNewest }

pub trait Buffer<T> {
    fn push(&mut self, item: T) -> PushOutcome<T>;
    /// The head, without removing it. `None` iff empty.
    fn peek(&self) -> Option<&T>;
    /// Removes the head, only once delivery has actually succeeded.
    fn commit(&mut self) -> Option<T>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool { self.len() == 0 }
    /// Approximate bytes held, for byte-aware bounding (workstream B).
    fn weight(&self) -> u64;
}

pub struct InMemoryBuffer<T> {
    items: VecDeque<(T, u64)>,
    max_len: usize,
    max_weight: u64,
    weight: u64,
    overflow: OverflowPolicy,
}
```

`push` takes the item's weight alongside it (a second parameter, or a `(T, u64)` tuple — pick
whichever reads cleaner once B's estimator exists) so the buffer never recomputes it. `Block` is
deliberately absent: a sync trait cannot block, so `OverflowPolicy::Block` is a `SinkQueue`
(workstream C) concept layered on top, not a variant this trait's impl has to understand.

**Files:** `crates/logit-proto/src/buffer.rs`, `crates/logit-proto/src/lib.rs`,
`docs/design/wire-protocol.md`, `docs/adr/0021-buffered-sink-delivery.md` (new, drafted here).

**Test list:** push/peek/commit ordering; peek does not remove; commit on an empty buffer is a
no-op returning `None`; `DropOldest` evicts the head and returns it via `PushOutcome::Evicted`;
`DropNewest` rejects and hands the pushed item back unchanged; the length bound and the weight
bound each trip independently of the other; `weight()` stays exact across a push/evict/commit
sequence.

**Done when:** `script/test` and `script/lint` are clean, and `Buffer` has no `pop`.

## B. `EventBatch::estimated_heap_bytes`

**Goal:** a byte figure the buffer can bound on, since `docs/design/memory.md` §5 is explicit that
counting batches alone is meaningless once batch size is unbounded — and this buffer will default
to holding far more batches than the 64-deep inbox ever did.

**Depends on:** nothing; can run parallel to A.

**The change.** `crates/logit-core/src/event.rs`, a deliberately approximate O(events) walk:
attribute keys/values' byte lengths, log body length, one entry's worth of size per metric record,
`Arc<Resource>` counted once per batch (not once per event — it's shared). Document prominently
that this is an admission-control estimate, not an allocator-accounting figure, so it is exempt
from the exact-`size_of`/exact-allocation-count discipline `type_sizes.rs`/`allocations.rs` enforce
elsewhere — those tests are about what `Event` actually costs to move, this is a cheap heuristic
for "roughly how much is this buffer holding."

**Files:** `crates/logit-core/src/event.rs`, `docs/design/memory.md` §5.

**Test list:** an empty batch; a batch with attributes only, log only, metrics only; monotonic
growth as events are added; a batch's resource contributes once regardless of event count.

**Done when:** `EventBatch::estimated_heap_bytes()` exists, is unit-tested, and memory.md §5 names
it as the buffer's bound rather than repeating "not designed yet."

## C. `SinkQueue` + split `run_output`

**Goal:** decouple a sink's inbox drain from its delivery, per the reference architecture above.

**Depends on:** A, B.

**Decisions to record:** rewrite `crates/logit-pipeline/src/output.rs`'s doc comment — it
currently claims buffering is the *output's* responsibility; after this it is the runtime's, and
`Output` only ever sees one batch at a time. Add a sink-buffering subsection to
`docs/design/pipeline-graph.md`'s backpressure section.

**The change.** New `crates/logit-pipeline/src/sink_queue.rs`:

```rust
pub struct SinkQueue {
    inner: Arc<Mutex<InMemoryBuffer<Arc<EventBatch>>>>,
    not_empty: Notify,
    not_full: Notify,
    closed: AtomicBool,
    overflow: OverflowPolicy,
    block_when_full: bool,
    telemetry: Telemetry,
}
```

`std::sync::Mutex`, not tokio's — every critical section is a `VecDeque` push/pop with no
`.await` inside it. `push` is `async` only because `Block` awaits `not_full`; the two drop
policies return immediately after taking the lock once. `peek`/`commit` are the async ack pair
`write_loop` drives.

`run_output` (`crates/logit-pipeline/src/runtime.rs`) becomes:

> **Superseded below, as shipped:** this sketch predates implementation and gets two things
> wrong, both corrected during review and recorded in `docs/adr/0021-buffered-sink-delivery.md`'s
> Consequences section — `run_output` races `drain_inbox`/`write_loop` via `tokio::select!`, not
> `tokio::join!` (a `join!` would hang the task forever if `write_loop` returns early while the
> inbox is still open, which every real listener's is), and the real signature also threads a
> `SinkQueueConfig`, a `WriteLoopConfig`, and a shutdown `watch::Receiver<bool>` rather than one
> bundled `options` struct. Kept current here rather than only in the implementing PR, matching
> `docs/plans/0002-nginx-integration.md`'s workstream D precedent for the same situation.

```rust
async fn run_output(
    id: String,
    output: Box<dyn Output + Send>,
    inbox: mpsc::Receiver<Delivered>,
    telemetry: Telemetry,
    options: SinkOptions,   // pipeline-local; NOT logit-config's type (no logit-config dep here)
) -> anyhow::Result<()> {
    let queue = SinkQueue::new(options.buffer, telemetry.clone());
    let (drain_result, write_result) = tokio::join!(
        drain_inbox(inbox, queue.clone(), telemetry.clone()),
        write_loop(id.clone(), output, queue, telemetry, options.retry, options.shutdown),
    );
    drain_result?;
    write_result.with_context(|| format!("component '{id}'"))
}
```

`drain_inbox` moves each `Delivered` into the queue (`Delivered::Owned` → one `Arc::new`;
`Delivered::Shared` → a refcount bump, no clone) and calls `queue.close()` when the inbox closes.
`write_loop` (fleshed out in D) loops `queue.peek().await` until it returns `None`, which happens
only once the queue is both closed and empty — that single condition is what makes shutdown drain
the tail correctly with no separate close-detection logic.

**Files:** `crates/logit-pipeline/src/sink_queue.rs` (new), `crates/logit-pipeline/src/runtime.rs`,
`crates/logit-pipeline/src/output.rs`, `crates/logit-pipeline/Cargo.toml` (new `logit-proto`
dependency — confirmed cycle-free, `logit-proto` depends only on `logit-core`),
`docs/design/pipeline-graph.md`.

**Test list:** extend `runtime.rs`'s existing private `RecordingOutput` test double with a
`SlowOutput`/`FailingOutput` pair in the same module — assert the inbox keeps draining while a
`send` is parked on a test-controlled gate; `DropOldest`/`DropNewest` evict-and-count under a full
buffer; `Block` backpressures the drain and resumes once the writer catches up; a batch whose
first delivery attempt fails is redelivered rather than lost (the at-least-once property); inbox
close drains the queue's tail before the joined future resolves. `tokio::time::pause()` for every
timing-sensitive assertion.

**Benches:** `crates/logit-bench` gains a direct-call bench for `InMemoryBuffer::push`/`peek`/
`commit`, including the evict-under-`DropOldest` path — called directly, never through the
runtime, since divan's allocation profiler only counts allocations on threads it controls and
would misreport anything crossing a channel hop.

**Allocation accounting:** `crates/logit-bench/tests/allocations.rs`'s constant for the
single-consumer output path grows by one (`Arc::new` on `Delivered::Owned`) — update the constant
and `docs/design/memory.md`'s table in the same commit; do not relax the assertion to `<=`.
**Residual, found during implementation:** no existing test in `allocations.rs` actually exercises
the `Delivered -> drain_inbox` hop end to end (the existing `fanout_send_*` tests measure
`Fanout::send` directly, stopping short of `run_output`/`drain_inbox`) — there was no constant to
update. Add one before this plan is considered done: an allocation test driving `drain_inbox`
directly (not through a full `run`) on a single-consumer `Delivered::Owned` batch, asserting
exactly one allocation (the new `Arc::new`), plus a `docs/design/memory.md` table row for it.

**Done when:** a sink under `Block` with a slow write no longer stops its own inbox from draining
(directly assertable in a test), and `script/cibuild` passes.

## D. Retry relocation, `Fault` classification, `Output::flush`, shutdown grace

**Goal:** move retry behind the queue boundary, generic over any `Output`, with duplicate risk
bounded by fault classification and delivery posture rather than a blanket policy; give a sink a
shutdown hook; bound how long a dead sink can hold up process exit.

**Depends on:** C.

**Decisions to record:** finalize **ADR 0021** to Accepted, including the failure-handling
decision below; add a short "Revised by ADR 0021" note to ADR 0013's retry section (0013 is not
superseded — its shutdown and diagnostics decisions stand unchanged).

**The change.**

```rust
// crates/logit-pipeline/src/output.rs
pub enum Fault { Clean, Ambiguous, Permanent }

#[async_trait::async_trait]
pub trait Output {
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()>;
    /// Called once after the last batch has been delivered (or dropped) and no more will
    /// follow. Default no-op — closes ADR 0013's residual "no close hook" gap without touching
    /// any sink that doesn't need one.
    async fn flush(&mut self) -> anyhow::Result<()> { Ok(()) }
    /// Whether re-delivering an already-delivered batch is safe for this destination. Drives
    /// the default `buffer.delivery` posture; config can still override it.
    fn duplicate_safe(&self) -> bool { false }
}
```

`Fault` travels out of `send` as an `anyhow` context marker (`.context(Fault::Ambiguous)`, read
back via `err.chain().find_map(|e| e.downcast_ref::<Fault>())`, unclassified defaulting to
`Permanent`) rather than a signature change, so `StdioOutput` needs no changes at all beyond
opting into `duplicate_safe() -> false` (its default). `InfluxDbOutput::send` keeps its existing,
already-tested classification helpers (`is_retryable_status`, `attempt_timeout_for`,
`backoff_for`) but loses its own retry loop and its `RetryPolicy`/`with_retry` builder — those
move into the generic `deliver_with_retry` in `logit-pipeline`, driven by `write_loop` per the
resolved posture and `Fault`; `duplicate_safe() -> true` for line-protocol's idempotent-overwrite
property (verified fact, see plan header).

**Failure semantics — the process no longer exits on a sink failure by default:**

- **Retryable** (per `Fault` and posture, table above): retried within `retry_budget`; the batch
  stays at the queue's head throughout, visible to depth telemetry.
- **Budget exhausted:** `queue.commit()` (drop the batch), count it
  (`batches.dropped{reason="send_failed"}`), warn via `Diagnostics::warn_throttled`, continue to
  the next batch. No process exit — a sink unable to reach its destination degrades to dropping,
  it does not take every other sink in the node down with it.
- **Permanent:** drop and continue, **except** exit the process if permanent failure has been the
  *only* outcome for a fixed ~60s window with no intervening success — a genuinely misconfigured
  sink (bad token, bad bucket) still fails loudly enough for a restart-policy supervisor to notice,
  while one malformed batch cannot kill an otherwise-healthy pipeline. Tracked with a simple
  rolling `(last_success: Option<Instant>, first_permanent_since: Option<Instant>)` pair in
  `write_loop`, no new dependency.

**Shutdown grace:** `run_output` takes the same `watch::Receiver<bool>` inputs already get
(`run_input`, `runtime.rs`). Once it flips, `write_loop` caps its remaining drain time at
`shutdown_grace` (default 5s); on expiry it logs the undelivered count, counts
`batches.dropped{reason="shutdown"}`, calls `output.flush()`, and returns `Ok(())` — not an error,
an incomplete drain on shutdown is expected behavior, not a pipeline failure.

**Retry budget widens** from 5s to 60s default, since a stall here no longer reaches the drain
loop or the listener behind it — the queue absorbs it instead.

**Verify during implementation:** confirm `reqwest::Error::is_connect()` reliably separates
`Clean` from `Ambiguous` failures; the duplicate-risk argument for `at_most_once` depends on that
distinction actually holding. If it doesn't, `at_most_once` should degrade to no-retry-at-all
rather than silently risking a duplicate.

**Files:** `crates/logit-pipeline/src/runtime.rs`, `crates/logit-pipeline/src/output.rs`,
`crates/logit-outputs/src/influxdb.rs`, `crates/logit-outputs/src/stdio.rs`,
`docs/adr/0021-buffered-sink-delivery.md`, `docs/adr/0013-service-lifecycle-and-output-retry.md`.

**Test list:** influx's existing in-process `TcpListener` retry tests adapt to assert
*classification* (429/503 → `Ambiguous`, connection-refused → `Clean`, 400/401 →
`Permanent`) rather than loop behavior, since the loop moved; new `logit-pipeline` tests under
`tokio::time::pause()` for the backoff schedule, budget exhaustion → drop-and-continue, posture
selecting retryability per `Fault` (both postures against all three `Fault` values), the
60s-permanent-failure-window exit, and shutdown-grace expiry counting the drop and returning `Ok`.

**Done when:** an in-process fake sink that always returns a `Clean`-classified error is retried
indefinitely without duplicating; the same fake returning `Ambiguous` is retried only under
`at_least_once`; a sustained `Permanent` failure exits after ~60s; `script/cibuild` passes.

## E. Drain all tasks on first error

**Goal:** stop `run_with_telemetry`'s join loop from discarding siblings' buffered data.

**Depends on:** C (independent of D — can run in parallel with it).

**The change.** `crates/logit-pipeline/src/runtime.rs`'s `run_with_telemetry` join loop currently
`break`s on the first `Err` and drops the `JoinSet`, aborting every other task mid-flight — cheap
before this plan, a real data-loss path after it, since a sibling sink may be mid-drain with
batches still queued. Record the first error, trigger `shutdown_tx.send(true)` (the same signal
SIGTERM already drives), keep `join_next`ing until every task has exited via the normal cascade,
then return the recorded error.

**Files:** `crates/logit-pipeline/src/runtime.rs`.

**Test list:** two sinks, one fails immediately; assert the healthy sink's already-buffered
batches were delivered (or flushed within its shutdown grace) before `run` returns, and that the
returned error is the first one recorded, not whichever task happened to finish last.

**Done when:** a failing component no longer silently discards a healthy sibling's buffered work.

## F. Config, schema, examples

**Goal:** make buffer depth, overflow policy, delivery posture, retry budget, and shutdown grace
operator-visible, since decoupling is what makes a generous, tunable budget meaningful in the
first place.

**Depends on:** D, E.

**The change.** `crates/logit-config/src/lib.rs`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, default)]
pub struct BufferConfig {
    pub max_batches: usize,
    pub max_bytes: u64,
    pub overflow: OverflowPolicy,     // config-layer enum; converted in the registry (see below)
    pub delivery: Option<DeliveryPosture>,  // None -> derive from the sink's duplicate_safe()
    #[serde(with = "humantime_serde_duration")]
    #[schemars(with = "String")]
    pub retry_budget: Duration,
    #[serde(with = "humantime_serde_duration")]
    #[schemars(with = "String")]
    pub retry_max_delay: Duration,
    #[serde(with = "humantime_serde_duration")]
    #[schemars(with = "String")]
    pub shutdown_grace: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OverflowPolicy { Block, DropOldest, DropNewest }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryPosture { AtLeastOnce, AtMostOnce }
```

Hoisted onto `Component` next to `sources` (`#[serde(default)] pub buffer: BufferConfig`), not
repeated per sink `ComponentKind` variant — `Component` flattens `ComponentKind` already, so a
sibling field produces identical YAML with none of the four-way duplication a per-variant field
would need, and a future fifth sink kind costs nothing extra. Every field defaults (`Block`,
1024 batches, 64MiB, `None` delivery, 60s/10s/5s), so an omitted `buffer:` block is exactly
today's behavior plus decoupling. `crates/logit-pipeline/src/graph.rs` rejects a non-default
`buffer:` on a non-sink component alongside the existing arity rules.
`crates/logit-cli/src/pipeline.rs`'s registry converts the config-layer `OverflowPolicy` to
`logit_proto::buffer::OverflowPolicy` when building each `SinkOptions` (kept as two small enums
with a `From` impl, not unified — `logit-config` must not depend on `logit-proto`).

`max_bytes` as a bare `u64` means writing raw byte counts in YAML; add a small
`humanbytes_serde`-style codec mirroring the existing `humantime_serde_duration` shape
(`#[serde(with = ...)] #[schemars(with = "String")]`) so `64MiB` round-trips, rather than shipping
the uglier bare integer.

```yaml
components:
  influx_out:
    type: influxdb_out
    sources: [windowed]
    url: !env INFLUXDB_URL
    org: logit
    bucket: metrics
    token: !env INFLUXDB_TOKEN
    buffer:
      max_batches: 4096
      max_bytes: 128MiB
      overflow: drop_oldest
      retry_budget: 120s
      shutdown_grace: 10s
```

**Files:** `crates/logit-config/src/lib.rs`, `crates/logit-cli/src/pipeline.rs`,
`crates/logit-pipeline/src/graph.rs`, `schema/logit.schema.json` (via `script/schema`),
`examples/nginx-to-influxdb.yaml` or `examples/statsd-to-influxdb.yaml` (a commented `buffer:`
block on the InfluxDB sink), `docs/deploying.md`.

**Test list:** defaults round-trip through serde with an empty `buffer: {}`; each `overflow`/
`delivery` variant deserializes; an unknown field under `buffer:` is rejected
(`deny_unknown_fields`); a non-default `buffer:` on a listener or transform is a graph-validation
error with a clear message; the byte-size codec round-trips `64MiB`/`128MiB`/a bare integer.

**Docs:** `docs/deploying.md` gains a sink-buffering section: the new non-exit failure semantics,
the `max_bytes` × sink-count memory implication worth sizing for, and what to alert on
(`buffer.utilization`, `batches.dropped`).

**Done when:** `script/schema` diff is clean, `script/cibuild` passes, and the example config's
`buffer:` block is exercised by `script/server`.

## G. Telemetry + `known-gaps.md` rewrite

**Goal:** make buffer state observable via the existing internal-telemetry framework, and record
what actually closed versus what narrows.

**Depends on:** F.

**The change.** New metrics, following the existing `logit.component.*` (runtime-owned, every
component) vs `logit.output.*` (sink-specific) split — these are all `logit.component.*` because
`run_output`/`SinkQueue` own them, superseding influx's now-removed `logit.output.retries`:

| Metric | Type | Tags | Meaning |
|---|---|---|---|
| `logit.component.buffer.batches` | gauge | — | batches currently queued |
| `logit.component.buffer.bytes` | gauge | — | estimated bytes currently queued |
| `logit.component.buffer.utilization` | gauge | — | max of the two bounds' fill ratios |
| `logit.component.buffer.push.blocked.duration` | timing | — | how long a `Block` push waited for room |
| `logit.component.retries` | count | — | replaces `logit.output.retries` |
| `logit.component.batches.dropped` | count | `reason`: `overflow_oldest` \| `overflow_newest` \| `send_failed` \| `shutdown` | new `reason` values on the existing metric |
| `logit.component.events.dropped` | count | same `reason` values | ditto |

> **Two rows in this table's original draft didn't ship, as implemented:** `buffer.wait.duration`
> (push-to-commit latency) and an `outcome`-tagged `send.attempts` breakdown. `buffer.batches`/
> `.bytes`/`.utilization` already answer the operative question — is this sink's queue backing up
> — so these were left as a plausible future addition rather than built for their own sake; see
> `docs/design/internal-telemetry.md`'s catalog for what actually shipped. The metric name is also
> `logit.component.retries`, not `send.retries` as first sketched here.

All tag values `&'static str`, well under `MAX_KEYS_PER_COMPONENT` (1024) per sink. Exercisable
via `examples/internal-telemetry.yaml`. `logit.component.errors` keeps its existing meaning
(incremented once per permanently-dropped batch, not once per retry attempt).

Then rewrite `docs/known-gaps.md`: delete the **Output buffering** entry entirely, and delete the
sink half of **Delivery I/O is not decoupled from event processing within a node** — narrow that
entry to name only the still-open listener half (`StatsdInput::run` interleaving `recv_from`/
decode/`Fanout::send`). Add three new, narrower entries: **no durable/disk-backed buffering**
(buffered batches are lost on SIGKILL, OOM, or the process's fatal-error exit path — nothing
survives a restart, and it's plausibly config-optional even when it lands); **no end-to-end
acknowledgement** (the guarantee is in-process only — UDP listeners still lose datagrams before
anything reaches a buffer, so end-to-end delivery stays best-effort regardless of sink posture);
**no out-of-order/credit-based acks** (deferred to the native wire protocol, where multiple
in-flight batches per link is the real shape). Also fold the new metrics into
`docs/design/internal-telemetry.md`'s existing catalog.

**Files:** `docs/known-gaps.md`, `docs/design/internal-telemetry.md`, plus the emit sites in
`crates/logit-pipeline/src/sink_queue.rs` and `runtime.rs`'s `write_loop`.

**Test list:** each new metric fires under the scenario that should trigger it (an overflow test
asserting the `reason` tag, a retry test asserting `send.attempts{outcome="retryable"}`, a
shutdown-grace-expiry test asserting `reason="shutdown"`).

**Done when:** `examples/internal-telemetry.yaml` run against a config with a small `max_batches`
shows `buffer.utilization` climbing and `batches.dropped` incrementing under sustained load.

---

## Verification, across the whole plan

- Per workstream: `script/cibuild` — the exact sequence CI runs.
- `script/bench` before/after, with the new buffer-bench numbers recorded here once run;
  `crates/logit-bench/tests/allocations.rs`'s constant and `docs/design/memory.md`'s table updated
  in the same commit as C, never relaxed to a `<=` bound.
- `script/schema` diff clean after F.
- Soak against the compose stack (`compose.yaml`, `script/server`): start the pipeline,
  `docker stop` InfluxDB for 90s, confirm no process exit, confirm
  `logit.component.buffer.batches` climbs and drains to zero after InfluxDB restarts with no data
  loss under the default `Block` policy; then, with a small `max_batches` and
  `overflow: drop_oldest`, confirm `batches.dropped{reason="overflow_oldest"}` appears while
  intake never stalls.
- Duplicate check, the point of the exercise: with `delivery: at_least_once` against InfluxDB,
  force an *ambiguous* failure (kill the TCP connection mid-response) and confirm the retried
  write is an overwrite, not a double-counted point. With `delivery: at_most_once` on `stdio_out`,
  confirm the same failure produces no repeated line, and that a *clean* failure (connection
  refused) still retries and eventually delivers exactly once.
- SIGTERM with a slow-but-alive sink mid-drain: confirm delivery completes within
  `shutdown_grace`, and confirm a permanently-wedged sink still lets the process exit at the grace
  deadline rather than hanging.
