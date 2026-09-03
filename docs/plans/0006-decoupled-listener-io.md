# Closing plan: decoupled listener I/O

`docs/known-gaps.md` carried an entry describing **"Delivery I/O is not decoupled from event
processing within a node"** with two halves: [ADR 0021](../adr/0021-buffered-sink-delivery.md)
closed the sink half; this plan closes the other. `StatsdInput::run`/`SyslogInput::run`
(`crates/logit-inputs/src/{statsd,syslog}.rs`) were the same loop, byte for byte apart from the
decoder type — `recv_from`, decode, `Fanout::send`, all on one path:

```rust
loop {
    let (n, _peer) = socket.recv_from(&mut buf).await?;
    let bytes = Bytes::copy_from_slice(&buf[..n]);
    match decoder.decode(bytes) {
        Ok(batch) if !batch.events.is_empty() => sink.send(batch).await,
        Ok(_) => {}
        Err(err) => self.diag.warn_throttled("bad_datagram", err),
    }
}
```

`Fanout::send` blocked unbounded on a full 64-deep inbox, so downstream backpressure stopped
`recv_from`, and the kernel then dropped datagrams silently and uncounted.

**Scope.** A UDP listener keeps reading its socket regardless of downstream backpressure; what it
loses when it can't keep up is dropped in userspace, counted, and attributable, instead of dropped
by the kernel and invisible. Decoded events amortize across datagrams into bounded batches. Both are
config-visible per listener. It stops at the socket: reading more efficiently (`recvmmsg`), reading
in parallel (`SO_REUSEPORT`), and observing the kernel's own drop counter are three separately
motivated extensions recorded as new `docs/known-gaps.md` entries, not built here.

Each workstream below landed as its own commit on this branch (A–E together, F, then G); read the
design docs a workstream references before touching that area again — this plan sequences work, it
doesn't re-derive design that belongs in [ADR 0027](../adr/0027-decoupled-listener-io.md) or a
design doc.

## What established implementations do

Researched before designing, because the intended outcome was to match the field rather than
invent — see [ADR 0027](../adr/0027-decoupled-listener-io.md)'s Context section for the full
table and citations (Telegraf, gostatsd, DogStatsD, rsyslog, syslog-ng, and Vector as the
instructive counter-example). The short version: every mature UDP listener researched decouples
read from parse via a bounded queue, and every one of them drops on overflow by default rather than
blocking the reader — never `logit`'s pre-existing shape.

## Decisions already settled

| Question | Decision |
|---|---|
| Overflow default | **`drop_oldest`, counted** — never block the reader. Deliberately differs from `buffer:`'s `block`: blocking a sink's producer backpressures an in-process drain that can wait; blocking a UDP reader backpressures the kernel, which cannot, and which discards into a counter this process doesn't read |
| Batch assembly | **In scope**, bounded by `batch_max_events` / `batch_max_bytes` / `batch_flush_interval` |
| "No accumulation" | `batch_max_events: 1` — the accumulator flushes when a bound is *reached or exceeded* and never splits a decoded batch, so this falls out of the bound semantics with no magic value |
| `SO_RCVBUF` | **In scope** as `receive_buffer_bytes`, via `socket2` (already in `Cargo.lock` at 0.6.5 transitively via tokio/hyper — a promotion to direct dependency, not a new tree entry) |
| Queue implementation | **Generalize `SinkQueue`** over a `Queued` trait (weight + drop-unit count per item), not closures |
| Timestamp correctness | Decode moving off the read path means `event.timestamp` would silently become decode time under backlog, breaking `syslog.rs`'s documented "receipt time" contract. `Decoder` widens to take an explicit `received_at` |
| Where the pieces live | **Split.** The generic queue and batch accumulator are transport-agnostic — `logit-pipeline`, alongside `SinkQueue`. The UDP socket bind/`recv_from`/`SO_RCVBUF` loop is protocol-impl shaped — `logit-inputs`, per `docs/design/pipeline-graph.md`'s crate-layout rule |
| Config surface | A new **`receive:`** block on `Component`, listener-only, flat — following `BufferConfig`'s flat precedent, not nested |
| Kernel-drop visibility, `recvmmsg`, `SO_REUSEPORT` | **Out of scope**, recorded as new `known-gaps.md` entries |

## The constraint everything is designed around

`Input::run(&mut self, sink: Fanout)` takes its `Fanout` **by value**, and cancel-by-drop is the
only shutdown mechanism in the system. `run_input` races `input.run(fanout)` against a
`watch::Receiver<bool>`; when shutdown wins, dropping the future drops the `Fanout`, dropping the
last `Sender` into every downstream inbox, closing them and cascading the close-time flush through
transforms, Lua nodes and sinks ([ADR 0013](../adr/0013-service-lifecycle-and-output-retry.md)).

**If any spawned task or `Arc` holds a `Fanout` clone past that drop, the graph never closes and
`run` hangs forever.** So the split is two futures in *one* task joined by `select!` — `run_output`'s
shape — never `tokio::spawn`.

### Shutdown drain must not regress ADR 0013's accepted loss

Today SIGTERM loses one in-flight datagram, and ADR 0013 accepts that explicitly on the premise
that "dropping a UDP-listener future mid-datagram is already an accepted loss." A receive queue
holding thousands of datagrams plus an accumulator invalidates that premise. The fix
([ADR 0027](../adr/0027-decoupled-listener-io.md) has the full design) is an additive, defaulted
trait method whose default body **is** ADR 0013's own `select!`, relocated onto the trait, so a
non-overriding listener still resolves at the exact instant shutdown fires — zero added latency —
while `run_input` races the (possibly overridden) result against a grace-delayed backstop. This
*revises* ADR 0013's rejection rationale for cooperative shutdown; it does not supersede the ADR.

## Reference architecture, as shipped

```
        UDP socket (SO_RCVBUF set via socket2, granted value gauged)      logit-inputs
              │  recv_from -> Bytes::copy_from_slice   (unchanged: 1 right-sized alloc)
              ▼
        read_loop ──push──▶ ReceiveQueue ──pop──▶ decode_loop ──▶ Accumulator ──▶ Fanout::send
        (borrows            (BoundedQueue<           (Decoder::decode_into,       (one send per
         &socket,            Datagram>,                received_at threaded        accumulated
         races push AND      drop_oldest                through; malformed          batch, not
         recv_from            default, counted)         datagrams diagnosed          per datagram)
         against shutdown)                               and skipped)
        ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ──
                                                                             logit-pipeline
        (BoundedQueue<T: Queued> and BatchAccumulator are transport-agnostic and live here,
         alongside SinkQueue; the socket and the loop wiring them together are logit-inputs)
```

`read_loop` never awaits decode or `Fanout::send`; `decode_loop` never touches the socket. The only
shared state is the queue. Both run as two futures in one task, raced via `select!` — never
`tokio::spawn`.

## Workstream dependency graph

```
A ──┐
B ──┤
C ──┼── E ── F ── G
D ──┘
```

A (queue generalization), B (batch accumulator), C (`Decoder` timestamp widening), and D
(`Input::run_until_shutdown` + `run_input`'s grace backstop) were mutually independent. E (the UDP
driver, wiring A–D together) needed all four. F (config, schema, validation) needed E. G (telemetry
catalog, docs, `known-gaps.md`, ADR 0027 to Accepted) needed F. Landed as three commits: A–E
together, then F, then G.

---

## A. Generalize `SinkQueue` into `BoundedQueue<T: Queued>`

**Goal:** one tested bounding/overflow/`Notify`/close implementation serving both the sink and the
receive side, with zero behaviour change on the sink side.

**As shipped.** `crates/logit-pipeline/src/sink_queue.rs` → `queue.rs`. A `Queued` trait
(`weight()`/`units()`) and a `QueueMetrics` bundle of `&'static str` names (resolved once at
construction, never formatted) let one `BoundedQueue<T: Queued>` serve both `SinkQueue` (now a type
alias, `SINK_QUEUE_METRICS`) and the receive side's `ReceiveQueue`. Gained `pop()` — an atomic
remove-on-read — because `peek()` then `commit()` at the call site is cancellation-unsafe: `peek()`
reserves the head against `drop_oldest` eviction, and if the awaiting task is dropped between
`peek()` and `commit()` (exactly what a shutdown-grace cancellation does), that reservation never
clears. `InMemoryBuffer::new` (`crates/logit-proto/src/buffer.rs`) now presizes its `VecDeque` via
`with_capacity(max_len.min(4096))`, removing the ~14 warm-up reallocations both queues used to pay.

**Test list, as run:** every pre-existing `sink_queue.rs` test passes unmodified against
`BoundedQueue<Arc<EventBatch>>`. New: a `BoundedQueue<TestItem>` proving the two bounds trip
independently and the dropped-units metric reports units, not item count; `pop()` FIFO ordering,
`None` on closed-and-empty, and (driven under `tokio::time::pause()` with a dropped mid-await call)
leaving no reservation behind so a later `drop_oldest` push still evicts.

**Done:** `script/cibuild` clean; `sink_queue.rs`'s tests unchanged apart from the module path; no
metric name constructed at runtime anywhere in `queue.rs`.

## B. `BatchAccumulator`

**Goal:** amortize the channel hop across datagrams, bounded three ways, without silently merging
events across resources.

**As shipped.** `crates/logit-pipeline/src/accumulator.rs`, transport-agnostic and socket-free.

> **Superseded from the original sketch, as shipped:** `absorb` takes `resource: Arc<Resource>,
> events: &mut Vec<Event>`, not an owned `EventBatch`, and merges via `Vec::append`, not
> `std::mem::take`. This is not a stylistic choice — it's what actually realizes the allocation win:
> `events` comes from a `Decoder::decode_into` call against a buffer the decode loop reuses across
> datagrams, and `Vec::append` drains it while leaving its capacity intact. `mem::take` on a `Vec`
> replaces it with a fresh, capacity-0 one, silently undoing the reuse. Caught by writing the test
> (`absorb_drains_the_callers_buffer_via_append_leaving_its_capacity_intact`) before wiring the real
> decode loop in workstream E — not by measurement after the fact.

> **Superseded again, post-review:** the first shipped cut of weight tracking recomputed
> `EventBatch::estimated_heap_bytes` from scratch on every `absorb`, via a zero-copy swap of the
> accumulator's own `resource`/`events` into a throwaway `EventBatch`. Code review caught this as a
> genuine O(n²) cost over one accumulation cycle (a full O(everything held) walk on every datagram,
> not once per flush) and, independently, a resource-attribute double-count on every call instead
> of once per batch. Replaced with true incremental tracking: `resource_weight` (cached, updated
> only on a resource change), the capacity term read live off `self.events.capacity()` (`O(1)`),
> and `events_weight` (a running sum, updated by adding just the incoming slice's per-event
> contribution). The three reproduce `estimated_heap_bytes` exactly, by construction — see ADR
> 0027. Caught by a whitebox test (`incremental_weight_matches_a_full_recompute_after_many_absorbs`)
> that absorbs many slices under one shared resource and compares the incremental total against a
> from-scratch recompute of the merged batch.

**Test list, as run:** `max_events: 1` emits once per absorbed batch, never splitting a multi-event
decode; reaching the bound exactly and exceeding it both emit; the byte bound trips independently;
`take()` on empty is `None`; a resource-`ptr_eq` mismatch flushes the old batch (carrying the old
resource) and the new accumulation survives with the new one; two batches sharing one
`Arc<Resource>` merge into a batch holding that same `Arc`; the buffer-capacity-preservation test
above; the incremental-vs-recomputed weight test above.

**Done:** `BatchAccumulator` unit-tested with no tokio, no socket, no `Decoder` — confirmed (9
tests, `crates/logit-pipeline/src/accumulator.rs`'s own module).

## C. `Decoder` timestamp widening

**Goal:** keep `timestamp` meaning *receipt* time once decode runs on a separate loop from the
socket read.

**As shipped.** `logit_proto::Decoder`'s core method becomes:

```rust
fn decode_into(&mut self, bytes: Bytes, received_at: i64, out: &mut Vec<Event>)
    -> Result<Arc<Resource>, CodecError>;
```

with a provided default `decode(&mut self, bytes: Bytes) -> Result<EventBatch, CodecError>`
stamping "now" and calling `decode_into` with a fresh `Vec::new()`. This is what kept the change
zero-test-churn: all ~28 existing call sites across `logit-inputs`/`logit-bench` kept compiling and
passing unmodified, since `decode()` is still there — it's just no longer the trait's core method.

**Test list, as run:** every existing decoder test unmodified (the default `decode()` path); two new
tests per decoder (`decode_into_stamps_events_with_the_callers_received_at_not_the_current_time`,
`decode_into_appends_to_an_already_populated_out_buffer_rather_than_replacing_it`).

**Done:** `now_nanos()` has no caller inside either decoder's `decode_into`; `script/cibuild` clean.

## D. `Input::run_until_shutdown` and `run_input`'s grace backstop

**Goal:** make a cooperative drain possible with zero added latency for every listener that has
nothing to drain.

**As shipped.** `crates/logit-pipeline/src/input.rs` gains the defaulted `run_until_shutdown`
exactly as designed in [ADR 0027](../adr/0027-decoupled-listener-io.md). `NodeSpec::Input` becomes
`Input(Box<dyn Input + Send>, InputRuntimeConfig)`; `run_input` gains a `shutdown_grace: Duration`
parameter and races `input.run_until_shutdown(...)` against `shutdown_grace_expired` (the existing
helper `write_loop` already used, reused unchanged). `shutdown.clone()` in the first arm means the
two `select!` arms touch disjoint receivers, so — unlike `run_output` — no `Box::pin`/`Option` dance
was needed.

**Test list, as run:** under `tokio::time::pause()` — a default-impl `Input` (`ForeverInput`, never
returns on its own) returns at t=shutdown with **zero elapsed virtual time**, not after the grace
(the regression this workstream exists to prevent); an overriding `Input`
(`DrainingInput`) completing within its grace delivers its batch and returns before the deadline; an
overriding `Input` that never finishes is cancelled at exactly the grace deadline; the default
impl's dropped `run` future still closes every downstream inbox; an `Input` erroring before any
shutdown still propagates with its `component '{id}'` context. All seven tests live in
`crates/logit-pipeline/src/runtime.rs`'s test module, alongside the pre-existing shutdown tests,
which pass with unchanged timing.

**Done:** every pre-existing `runtime.rs` shutdown test passes unchanged; the new no-added-latency
test pins the property directly.

## E. The shared UDP listener driver

**Goal:** one implementation of the read/decode split, in `logit-inputs`, that `statsd_in` and
`syslog_in` both reduce to.

**As shipped.** `crates/logit-inputs/src/udp.rs`: `Datagram` (implements `Queued`: weight is
`bytes.len()` plus inline footprint, units is `bytes.len()` so `bytes.dropped` reports the size of
what was lost), `RECEIVE_QUEUE_METRICS`, `UdpListenerConfig`, `UdpListener<D: Decoder + Send>`.
`StatsdInput`/`SyslogInput` became thin wrappers holding an `inner: UdpListener<...>`, gaining
`with_receive(UdpListenerConfig)`; neither contains a `recv_from` call or a `UdpSocket` import any
more. `InternalInput` untouched.

> **A correction made during E's own implementation, not foreseen in the original design sketch:**
> the reference architecture initially assumed only `read_loop`'s `recv_from` needed to race
> `shutdown`. In fact `read_loop`'s `queue.push` must race it too — under `overflow: block`, a
> graceful shutdown would otherwise have to wait for downstream decode to make room before the
> reader could even notice it should stop. Cancelling a blocked push mid-wait drops the one datagram
> it was holding, uncounted — bounded to exactly one, the same scope ADR 0013 already accepted.

**`SO_RCVBUF`, and the doubling trap.** Binds via `socket2::Socket`, `set_recv_buffer_size`,
converts to a tokio socket. Linux doubles the requested value for its own bookkeeping, so the
warning threshold is `granted < 2 * requested` on Linux, `granted < requested` elsewhere, naming
`net.core.rmem_max`. The granted value is always gauged, whether or not an override was requested.

**Test list, as run — five integration tests in `udp.rs`'s own module**, added as a follow-up within
this same commit once the driver code itself was reviewed and found to need them (the original
per-workstream test list called for this; it landed slightly out of order relative to the rest of
the workstream's code, in the same commit):

- `the_reader_keeps_reading_while_the_downstream_fanout_is_never_drained` — the central property.
  Proven by direct queue inspection after both loops are stopped, not a timing guess: the `Fanout`'s
  one consumer has capacity 1 and is never drained, so `decode_loop` blocks forever on its second
  send (the first fits in the empty channel) — deterministic regardless of scheduling, since a
  blocked send means no further `pop()` calls happen either. Draining the queue directly afterward
  must find exactly its configured depth (4), never 0.
- `a_backlog_queued_before_shutdown_is_still_decoded_and_delivered`.
- `a_malformed_datagram_is_skipped_without_stopping_the_decode_loop`.
- `shutdown_drains_the_queue_and_delivers_every_already_queued_datagram` — the empty-queue happy
  path, driven through `UdpListener::run_until_shutdown` end to end via `tokio::spawn` (fully
  `'static`, unlike the other four).
- `bind_socket_reports_the_granted_receive_buffer_even_when_unset`.

> **A real testing-approach correction, made while writing these tests:** the original test-list
> sketch assumed `tokio::spawn`/`spawn_local` could drive `read_loop`/`decode_loop` concurrently
> with the test's own driver logic. Both require `'static`, which a stack-local `socket` or `&mut
> decoder` can't satisfy — and `spawn_local` requires it too (only `LocalSet::run_until`'s own
> future may borrow). Resolved with `tokio::pin!` plus `select!`/`join!`: both loops run as plain,
> unspawned futures within the test's own task, raced (`select!`, for the deliberately-stuck
> scenario) or joined (`tokio::join!`, for the two that genuinely terminate) against the test's own
> driver logic.

**Done:** neither `statsd.rs` nor `syslog.rs` contains a `recv_from` call or a `UdpSocket` import;
the central test proves the reader keeps reading through a parked downstream consumer; all five
tests run stably across 5 consecutive repetitions with no flakes observed.

## F. Config, validation, schema, examples

**Goal:** make queue depth, overflow, batching bounds, socket buffer, and shutdown grace
operator-visible, following `BufferConfig`'s exact idioms.

**As shipped.** `crates/logit-config/src/lib.rs`: `ReceiveConfig`, mirroring `BufferConfig` exactly
(`deny_unknown_fields`, `human_bytes`/`humantime_serde_duration` codecs, flat rather than nested). A
new `human_bytes::option` submodule (mirroring the pre-existing `humantime_serde_duration::option`)
for `receive_buffer_bytes: Option<u64>`. Defaults: `max_datagrams` 10,000, `max_bytes` 32MiB,
`overflow` `drop_oldest`, `batch_max_events` 1,000, `batch_max_bytes` 1MiB, `batch_flush_interval`
100ms, `receive_buffer_bytes` `None`, `shutdown_grace` 5s — see
[ADR 0027](../adr/0027-decoupled-listener-io.md) for the numeric derivation against the field's own
tuning figures.

**Graph validation** (`crates/logit-pipeline/src/graph.rs`), rules 17 and 18 (rule 16, added
separately by `internal`'s `span_sample_rate` validation, sits between this plan's two): rule 17
rejects a non-default `receive:` on any kind that isn't a datagram listener — a dedicated
`is_datagram_listener` predicate (`StatsdIn | SyslogIn` today), deliberately not `role() !=
Role::Listener`, since `internal` is a listener by role but has no socket/queue/decoder. Rule 18
rejects a zero `max_datagrams`/`max_bytes`/`batch_max_events` (the twin of rule 15) but accepts
`batch_flush_interval: 0s`, which means "no timer."

`crates/logit-cli/src/pipeline.rs::build_spec` derives both `UdpListenerConfig` (via new
`receive_config`) and `InputRuntimeConfig` (via new `input_runtime_config`) from
`component.receive` — `input_runtime_config` applies uniformly to every `NodeSpec::Input` arm
including `internal`, safe because rule 17 already guarantees a non-datagram-listener's `receive`
is `ReceiveConfig::default()` by the time a resolved graph reaches `build_spec`.

`UdpListener`/`StatsdInput` gained a `config()`/`receive_config()` getter purely for test
introspection, mirroring how `NodeSpec::Output`'s `SinkQueueConfig`/`WriteLoopConfig` are directly
inspectable.

**Test list, as run:** 9 new `graph.rs` tests (rules 16/17, each zero-bound field individually,
`batch_flush_interval: 0s` validating fine, `internal` specifically rejected with its own message);
8 new `logit-config` tests (omitted/empty/fully-specified `receive: {}`, each `overflow` variant,
`deny_unknown_fields`, `receive_buffer_bytes` round-tripping a quoted size / explicit null /
omitted-as-`None`, unknown-field rejection); the `human_bytes::option` submodule's own 4 tests.

**Done:** `script/schema` diff clean (75 lines added, all `ReceiveConfig`-shaped); `script/validate`
passes every shipped config including the new commented `receive:` block in
`examples/statsd-to-influxdb.yaml`.

## G. Telemetry catalog, `known-gaps.md` rewrite, docs

**Goal:** make intake state observable through the existing framework, and record precisely what
closed versus what remains.

**As shipped — the metric set:**

| Metric | Kind | Tags |
|---|---|---|
| `logit.component.receive.datagrams` | gauge | — |
| `logit.component.receive.bytes` | gauge | — |
| `logit.component.receive.utilization` | gauge | — |
| `logit.component.receive.push.blocked.duration` | timing | — |
| `logit.component.receive.latency` | timing | — |
| `logit.component.datagrams.dropped` / `.bytes.dropped` | count | `reason`: `overflow_oldest` \| `overflow_newest` |
| `logit.component.receive.flushed` | count | `reason`: `max_events` \| `max_bytes` \| `interval` \| `resource_change` \| `shutdown` |
| `logit.input.receive_buffer.bytes` / `.requested.bytes` | gauge | — |

Naming: drops are `logit.component.*` (the same generic `BoundedQueue` code as the sink side's
`batches.dropped`), not `logit.input.*`, so an operator alerting on data loss doesn't union two
namespaces; the pre-existing `logit.input.datagrams`/`.datagram.bytes` arrival counters stay under
`logit.input.*`, genuinely impl-known. Accumulator emissions are `receive.flushed`, not an
unqualified `batches.flushed`, because `logit.component.flush.events`/`.flush.duration` already
mean "a stateful transform's window flush."

**`docs/known-gaps.md` rewrite, as done:** the old "Delivery I/O is not decoupled…" entry deleted
entirely (both halves now closed). "No durable/disk-backed buffering" amended to name the receive
queue alongside `SinkQueue`. "No end-to-end acknowledgement" amended: the receive-side loss it named
narrowed to kernel-side-only. "Channel depth is bounded in batches, not bytes" amended: a
listener's own outbound edge now has a config-visible byte bound (`batch_max_bytes`); every other
edge still doesn't. "A syslog event's timestamp is receipt time" amended to describe `received_at`
threaded explicitly through `decode_into`, not a `now_nanos()` call at decode time. Three new
entries added: no kernel-drop visibility (`/proc/net/udp[6]`, Linux-only, no new dependency — noting
the field mostly tells operators to run `netstat -su` themselves); single-datagram reads
(`recvmmsg`, needs raw-fd `try_io` + `libc`); single reader per listener (`SO_REUSEPORT`, deferred
partly because it collides with the cancel-by-drop shutdown cascade's one-`Fanout`-per-listener
assumption).

Also done: a "Every UDP listener also gets a `ReceiveQueue`" subsection in
`docs/design/internal-telemetry.md` (mirroring the sink-side "Every sink also gets a `SinkQueue`"
one); a "Listener intake" section in `docs/deploying.md` (mirroring "Sink delivery buffering"'s
three-part shape: failure semantics, sizing, what to watch); `AGENTS.md`'s "Current state" paragraph
updated; `docs/design/pipeline-graph.md`'s crate-layout section and validation-rules list updated
(rules 15–17 were missing from that doc's own copy of the list even before this plan — backfilled
while touching it); every stale `crates/logit-pipeline/src/sink_queue.rs` reference in *living*
reference docs (not historical ADRs/plans, which keep the path that was accurate when written)
updated to `queue.rs`.

> **A verification gap found and closed while writing this workstream's docs, not foreseen in the
> original per-workstream plan:** AGENTS.md requires `crates/logit-bench/tests/allocations.rs` and
> `docs/design/memory.md` to be updated *empirically*, not asserted in prose, whenever an allocation
> count changes. The ADR draft claimed specific numbers (`statsd_in`/`syslog_in` decode dropping
> from 2/1 to 1/0 allocations once a caller reuses its output buffer) before any bench test actually
> measured them. Added four new `logit-bench` tests to verify this directly:
> `syslog_decode_into_a_warm_reused_buffer_costs_nothing` (0, confirmed),
> `statsd_decode_into_a_warm_reused_buffer_costs_one_not_two` (1, confirmed),
> `receive_queue_push_then_pop_costs_nothing`, and `accumulator_absorb_into_a_warm_buffer_costs_
> nothing` (both 0, confirmed). The receive-queue test failed on its first run at 2 allocations, not
> 0 — not a real regression, but a test-construction bug: its `warm()` closure re-invoked
> `fixtures::statsd_datagram(1)` *inside* the measured region on every call, and that fixture
> function itself allocates a fresh `String` per call. Fixed by constructing the `Bytes` payload
> once outside the measured closure and cloning it (a refcount bump, not a copy) per push — exactly
> the "when one fails, read the printed actual/expected line before changing the constant" discipline
> AGENTS.md asks for, applied to a test bug rather than a real allocation regression. Both
> `docs/design/memory.md` §2 (new rows plus a new "Listener I/O decoupling" subsection explaining
> why the same decoder has two different allocation counts depending on call path) and §5 (the
> receive queue as a second consumer of the byte-aware bounding idea) were updated in the same
> commit.

**Done:** `examples/internal-telemetry.yaml` is the config the new metrics are exercisable through;
[ADR 0027](../adr/0027-decoupled-listener-io.md) is Accepted.

---

## Verification, as run across the whole plan

- Per workstream: full `script/cibuild`-equivalent sequence run manually (`cargo clippy --workspace
  --all-targets -D warnings`, `cargo fmt --all -- --check`, `script/schema`, `script/audit`,
  `script/validate`, `cargo nextest run --workspace` via `script/test`) — clean at every checkpoint,
  culminating in **560 tests passing, zero failures, zero skips** (up from 535 before this plan's
  first commit).
- `script/audit`: `socket2` 0.6.5 confirmed already present transitively (via tokio/hyper) before
  this plan promoted it to a direct `logit-inputs` dependency — no new entry in the dependency tree,
  advisories/bans/licenses/sources all clean.
- Allocation accounting verified empirically, not assumed — see workstream G's account of the test
  bug found and fixed along the way. Final, confirmed numbers: `syslog_in decode_into` into a warm
  buffer 0 allocations, `statsd_in decode_into` 1, `BoundedQueue` push+pop (warm) 0,
  `BatchAccumulator::absorb` into a warm buffer 0.
- The five `udp.rs` integration tests were run 5 consecutive times with no flakes, specifically
  because they're the ones carrying real timing (`tokio::time::sleep`) and real loopback sockets.

**Not run in this pass** (infrastructure/scale, deliberately deferred rather than silently
skipped): the headline compose-stack soak (flood `statsd_in`, stop InfluxDB, confirm
`logit.input.datagrams` keeps climbing while `/proc/net/udp`'s kernel drop counter stays flat under
the default policy and climbs under `overflow: block`); the `SO_RCVBUF` clamp demonstration against
a temporarily-lowered `net.core.rmem_max`. Both are real, valuable end-to-end confirmations of this
plan's central claim, exercising the running `compose.yaml` stack and host-level sysctls rather than
the crate test suite — worth doing before or shortly after this lands somewhere it matters
operationally, using the exact steps `docs/deploying.md`'s new "Listener intake" section and
[ADR 0027](../adr/0027-decoupled-listener-io.md) already describe.
