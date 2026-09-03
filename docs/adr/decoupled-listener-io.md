---
created: 2026-09-02
updated: 2026-09-02
---

# Decoupled listener I/O

## Status
Accepted

Note: this record was drafted and reviewed as `0026`, the next free number at the time. By the
time this branch merged with `main`, `0026` had independently been taken by
[`relative-gauge-adjustments.md`](relative-gauge-adjustments.md) — the same
numbering-collision pattern earlier renumberings in this project's history describe (see that
record's own Context section) — resolved the same way: renumbered to the next free number, `0027`,
after the merge.

## Context

`docs/known-gaps.md`'s **"Delivery I/O is not decoupled from event processing within a node"**
entry was half-closed by [ADR `buffered-sink-delivery`](buffered-sink-delivery.md): `run_output` split into a
`drain_inbox` half and a `write_loop` half sharing a `SinkQueue`, so a slow or retrying sink no
longer stops draining its own inbox. That ADR named the other half explicitly as untouched:
`StatsdInput::run`/`SyslogInput::run` (`crates/logit-inputs/src/{statsd,syslog}.rs`) still
interleave `recv_from`, decode, and `Fanout::send` in one loop, byte for byte identical apart from
the decoder type:

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

`Fanout::send` blocks unbounded on a full 64-deep inbox, so downstream backpressure stops
`recv_from` from ever being called again, and the kernel then drops datagrams **silently and
uncounted** — the failure mode ADR `buffered-sink-delivery`'s fix made rarer (a slow sink's own queue absorbs more
before backpressure reaches this far) but did not remove.

Every mature UDP listener in the field solves this the same way, and none of them do it by
blocking. Researched before designing, specifically to match the field rather than invent:

| Tool | Read → queue → parse | Queue knob (default) |
|---|---|---|
| Telegraf `statsd`/`syslog` | UDP reader goroutine → channel → parser workers | `allowed_pending_messages` (10000) |
| gostatsd | N readers → queue → aggregation workers | `--max-queue-size` (10000), `--receive-batch-size` (50) |
| DogStatsD (Datadog Agent) | listener → PacketAssembler → PacketsBuffer → workers | `dogstatsd_queue_size` (1024), `dogstatsd_packet_buffer_flush_timeout` (100ms) |
| rsyslog `imudp` | N input threads → ruleset queue → queue workers | `batchSize` (128 in their reference config), `queue.dequeueBatchSize` (4096) |
| syslog-ng | source fetch → log path window → destination fifo | `log-fetch-limit` (10, tuned to 100–10k) |

syslog-ng states the reasoning most directly: flow control on a UDP source "will not prevent
message loss… and the only thing it can cause is that your syslog-ng source will not read messages
from the kernel buffer, leading to packet drops." Blocking a UDP reader does not prevent loss — it
relocates loss from a place this process can count and bound to a place it cannot see at all. Every
one of the tools above bounds a receive-side queue and drops on overflow by default rather than
stopping the read.

**Vector is the instructive counter-example**, and the closest architectural analogue (Rust, tokio,
source→sink topology): it has *no* per-source queue, and its own docs state "a source only sends
events as fast as the slowest sink." That is `logit`'s pre-existing design, and Vector's own UDP
guidance is to raise `receive_buffer_bytes` and lean on sink buffers rather than fix the listener
side — prior art for the problem this ADR closes, not for the solution.

This ADR closes the listener half of the gap ADR `buffered-sink-delivery` left open, applying the same
decouple-the-I/O-from-the-work shape that ADR `buffered-sink-delivery` established, adapted to a producer (the kernel
socket buffer) that cannot be asked to wait.

## Decision

### Never block the reader by default — `drop_oldest`, counted

A UDP listener's receive queue defaults to `drop_oldest`, not `buffer:`'s `block`. This is
deliberate, not an oversight: `buffer:`'s producer is `drain_inbox`, an in-process task that *can*
wait for a slow sink. A UDP listener's producer is the kernel's socket receive buffer, which
*cannot* wait — once full, it silently discards the arriving datagram into a counter this process
never reads. Blocking the read half doesn't prevent that loss, it just makes it more likely to
happen, since the queue in front of it fills too. `drop_oldest` over `drop_newest`: the datagrams
already queued are the stale ones under sustained overload, and `drop_newest` has a further
pathology once the queue is full — it stays full, so an entire burst's tail is lost rather than a
spread sample. All three policies remain configurable (`receive.overflow`); an operator who has
sized their kernel buffer and genuinely wants the backpressure signal to propagate can still choose
`block`.

### Generalize `SinkQueue` into `BoundedQueue<T: Queued>`

Rather than a near-copy of `SinkQueue`'s tested bounding/overflow/`Notify`/close machinery,
`crates/logit-pipeline/src/queue.rs` (renamed from `sink_queue.rs`) generalizes it over a `Queued`
trait:

```rust
pub trait Queued: Send + Sync + 'static {
    fn weight(&self) -> u64;
    fn units(&self) -> u64;
}
```

and a `QueueMetrics` bundle of `&'static str` names, resolved once at construction and never
formatted — a name built at runtime would be exactly the interner-growth mistake
`docs/design/internal-telemetry.md`'s cardinality convention exists to prevent. `SinkQueue` becomes
`type SinkQueue = BoundedQueue<Arc<EventBatch>>` with its own `SINK_QUEUE_METRICS`; every sink-side
metric name, default, and existing test is unchanged by this refactor.

A closed's queue's `peek()`/`commit()` ack pair (`SinkQueue`'s existing contract, needed because a
sink retries a failed delivery) has no analogue on the receive side: a datagram that fails to
decode is diagnosed and dropped, never retried. `BoundedQueue` gains `pop()` — an atomic
remove-on-read — specifically because `peek()` then `commit()` at the call site is
**cancellation-unsafe**: `peek()` reserves the head against `drop_oldest` eviction, and if the
awaiting task is dropped between `peek()` and `commit()` (exactly what a shutdown-grace
cancellation does, see below), that reservation never clears, permanently exempting the head from
eviction and letting the queue grow past its configured bound. `pop()` never awaits between
reserving and removing, so a consumer cancelled mid-call can never leave one dangling.

### `BatchAccumulator`: datagram→batch assembly, and why weight tracking is exact, not approximate

`crates/logit-pipeline/src/accumulator.rs` amortizes many decoded datagrams into fewer, larger
batches before one `Fanout::send` — every field tool above does the same, for the same reason: one
send per datagram wastes a channel hop and a `Delivered` construction on payloads that are
routinely a few hundred bytes.

```rust
pub fn absorb(&mut self, resource: Arc<Resource>, events: &mut Vec<Event>) -> Option<(EventBatch, FlushReason)>
```

Two design points worth recording:

- **`absorb` takes `&mut Vec<Event>`, not an owned `EventBatch`, and merges via `Vec::append`.**
  This is what actually realizes the allocation win described below: `events` comes straight from a
  `Decoder::decode_into` call against a buffer the decode loop reuses across datagrams, and
  `Vec::append` drains `events` into the accumulator's own buffer while leaving `events` empty
  **with its allocated capacity intact**. An earlier draft of this design took ownership of a whole
  `EventBatch` and merged via `std::mem::take` — which silently undoes the reuse, since
  `mem::take` on a `Vec` replaces it with a fresh, capacity-0 one, not an emptied one.
- **Weight (for the byte bound) is tracked incrementally, in O(incoming events) per `absorb` —
  not recomputed from scratch.** An earlier draft recomputed `EventBatch::estimated_heap_bytes`
  once per `absorb` via a zero-copy swap of the accumulator's own fields into a throwaway
  `EventBatch`. That was caught in review as a real O(n²) cost over one accumulation cycle — a
  `batch_max_events: 1000` listener under steady load pays a full O(everything held so far) walk on
  *every* datagram, not once per flush — and, separately, would have double-counted the resource's
  own contribution on every call instead of once per batch. `estimated_heap_bytes` is three terms —
  `resource.estimated_heap_bytes()` (once per batch, unaffected by event count),
  `events.capacity() * size_of::<Event>()` (`Vec::capacity` is O(1) to read), and a per-event sum
  (`Event::estimated_heap_bytes()`, additive) — each cheap to reproduce without re-walking events
  already accounted for, so `BatchAccumulator` caches the first as `resource_weight` (updated only
  on a resource change), reads the second live off `self.events.capacity()`, and maintains the
  third as a running `events_weight` updated by adding just the incoming slice's contribution.
  The three together equal `estimated_heap_bytes` exactly, by construction — this is not the usual
  approximate-for-speed trade `docs/design/memory.md` §5 describes for the formula itself, just
  doing the same arithmetic without re-deriving inputs that hadn't changed.
- **`batch_max_events: 1` is the exact, magic-value-free spelling of "no accumulation."**
  `absorb` flushes once a bound is *reached or exceeded* and never splits a decoded batch, so every
  non-empty decode immediately reaches a `max_events: 1` bound — one send per datagram, byte for
  byte the pre-ADR `decoupled-listener-io` behavior. `0` stays rejected as an impossible bound (graph rule 18, the
  twin of rule 15's `buffer.max_batches: 0` check).
- **Never merges across a resource change.** An accumulated batch carries one `Arc<Resource>`; if
  the incoming resource isn't `Arc::ptr_eq` to what's held, whatever was held is flushed first
  (`FlushReason::ResourceChange`) before starting a fresh accumulation. Every decoder shipped today
  constructs one `Arc::new(Resource::default())` per instance and stamps every decoded batch with
  it, so this never trips in practice — it exists to make that assumption load-bearing rather than
  latent, the same *n*-to-1 hazard [ADR `aggregation-window-semantics`](aggregation-window-semantics.md) already
  documents for a Lua component's `flush()`.

### `Decoder::decode_into`: receipt time travels explicitly, and the allocation win it buys

Once decode runs on its own loop rather than immediately after `recv_from`, "now" at decode time
can run arbitrarily behind arrival under backlog. `syslog_in`'s own module doc already promises
`timestamp` is *receipt* time, not decode time — a promise a bare `now_nanos()` call inside decode
would silently break. `logit_proto::Decoder` widens to make the receipt instant an explicit
parameter, and to let the caller supply the output buffer:

```rust
pub trait Decoder {
    fn decode_into(&mut self, bytes: Bytes, received_at: i64, out: &mut Vec<Event>)
        -> Result<Arc<Resource>, CodecError>;

    // Convenience default over decode_into, stamping "now" -- test/bench call sites only.
    fn decode(&mut self, bytes: Bytes) -> Result<EventBatch, CodecError> { ... }
}
```

The default `decode()` method is what kept this a zero-test-churn change: all ~28 existing call
sites across `logit-inputs`/`logit-bench` kept compiling and passing unmodified, since `decode()`
is still there, it's just no longer the trait's core method.

Threading `out: &mut Vec<Event>` through — combined with `BatchAccumulator::absorb`'s
`Vec::append`-based merge above — is a genuine allocation improvement, not merely a neutral
refactor: with the decode loop's scratch buffer reused (cleared, never replaced) across datagrams,
`statsd_in`'s per-line allocation count drops from 2 to 1 and `syslog_in`'s from 1 to 0 in steady
state (`docs/design/memory.md` §2 has the updated table and the accounting for why).

### `Input::run_until_shutdown`: a bounded, opt-in cooperative drain

Today SIGTERM loses one in-flight datagram, and [ADR `service-lifecycle-and-output-retry`](service-lifecycle-and-output-retry.md)
accepts that explicitly on the premise that "dropping a UDP-listener future mid-datagram is already
an accepted loss." A receive queue holding thousands of datagrams plus an accumulator invalidates
that premise — cancel-by-drop would now discard everything queued, not one datagram.

The fix is an additive, defaulted trait method — the same shape ADR `buffered-sink-delivery` used for `Output::flush`:

```rust
#[async_trait::async_trait]
pub trait Input {
    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()>;

    async fn run_until_shutdown(
        &mut self, sink: Fanout, mut shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        tokio::select! {
            result = self.run(sink) => result,
            _ = shutdown.wait_for(|&due| due) => Ok(()),
        }
    }
}
```

The default body **is** ADR `service-lifecycle-and-output-retry`'s cancel-by-drop `select!`, relocated onto the trait from
`run_input` — it resolves at the exact instant `shutdown` fires, for every listener that doesn't
override it, with zero added latency. `run_input` (`crates/logit-pipeline/src/runtime.rs`) then
races this against a *grace-delayed* backstop rather than `shutdown` directly:

```rust
async fn run_input(..., shutdown_grace: Duration) -> anyhow::Result<()> {
    let mut deadline: Option<tokio::time::Instant> = None;
    tokio::select! {
        result = input.run_until_shutdown(fanout, shutdown.clone()) => result,
        () = shutdown_grace_expired(&mut shutdown, &mut deadline, shutdown_grace) => Ok(()),
    }
}
```

Reusing `shutdown_grace_expired` (already written for `write_loop`'s own grace). The default impl's
inner race always resolves before the grace backstop can, so nothing pays added latency; an
overriding listener (`UdpListener`, below) wins if it drains in time, and is cancelled by drop
(losing, and not counting, whatever remains) if it doesn't. `NodeSpec::Input` gains a second field,
`InputRuntimeConfig { shutdown_grace }`, mirroring `NodeSpec::Output`'s `SinkQueueConfig`/
`WriteLoopConfig` — a config-derived runtime knob the runtime needs, not something the boxed
`Input` itself can carry.

**This revises ADR `service-lifecycle-and-output-retry`, it does not supersede it** — the same treatment ADR `buffered-sink-delivery` gave its retry
budget. 0013's "no `Input` trait change, and no cooperation required from any listener
implementation" survives verbatim: the change is additive and defaulted, so cancel-by-drop remains
the behavior for every implementation that doesn't override. What's revised is 0013's *rejection
rationale* for cooperative shutdown, which priced it against "the exact byte in flight" on the
premise that the loss was bounded to one datagram — a premise this ADR's own receive queue
invalidates. 0013 stays **Accepted**.

**A shape considered and rejected:** inject a `watch::Receiver<bool>` at construction via the
existing `with_diagnostics`/`with_telemetry` builder idiom, so a listener races it internally with
no trait signature change. This does not address the actual blocker: the load-bearing change is
`run_input`'s own race becoming grace-delayed, and the moment it is, `run_input` needs to know
*whether this input drains* to avoid paying the grace unconditionally — a builder-injected receiver
conveys nothing to `run_input` for that purpose, so it would still need a second signal (e.g. `fn
drains_on_shutdown() -> bool`) to avoid regressing non-cooperative listeners' latency. Two
mechanisms where the defaulted trait method is one, and a worse contract besides: a listener that
forgets to wire the builder silently loses its drain, where an unimplemented `run_until_shutdown`
visibly falls back to documented, tested behavior.

### `UdpListener<D: Decoder>`: the shared driver, and why it lives in `logit-inputs`

`crates/logit-inputs/src/udp.rs` is the one implementation `StatsdInput`/`SyslogInput` both reduce
to now, generic over the decoder — the only thing their `run` loops ever differed in. Its
`run_until_shutdown` is `run_output`'s exact shape: bind, build one `Arc<BoundedQueue<Datagram>>`,
`Box::pin` a `read_loop` and a `decode_loop`, race them, drive whichever is still running to
completion. Three properties, each mirroring a bug ADR `buffered-sink-delivery`'s sink-side split hit and corrected in
review:

1. **Two futures in one task, never `tokio::spawn`.** `decode_loop` owns the `Fanout`; a spawned
   clone surviving past this task's drop would keep every downstream inbox open and hang the
   shutdown cascade the hard constraint above depends on.
2. **`queue.close()` is called by the read half, only once it has stopped reading** — the one
   condition `decode_loop`'s `pop()` needs to discover closed-and-empty and return; no second
   close-detection signal.
3. **The final accumulator flush happens only after the decode half can no longer receive
   anything** — simpler here than `run_output`'s `finish_and_flush`, since the accumulator is owned
   by (not shared with) `decode_loop`: the flush is the last statement of that loop's own body,
   after `pop()` returns `None`.

`read_loop` races **both** `recv_from` and `queue.push` against `shutdown` — not just `recv_from` —
so a graceful shutdown under `overflow: block` doesn't have to wait for downstream decode to make
room before the reader notices it should stop. Cancelling a blocked push mid-wait drops the one
datagram it was holding, uncounted — bounded to exactly one, the same scope ADR `service-lifecycle-and-output-retry` already
accepted.

**Crate placement: the generic queue and accumulator live in `logit-pipeline`; the socket lives in
`logit-inputs`.** `docs/design/pipeline-graph.md`'s crate layout is explicit that `logit-inputs`
holds only impls, and a UDP socket bind, an `SO_RCVBUF` setsockopt, and a `recv_from` loop are
unambiguously protocol-impl shaped — putting them in `logit-pipeline` would drag `socket2` and a
wire concern into the one crate whose job is to know nothing about protocols. What must be uniform
across listeners (queue bounding/overflow, accumulator flush decisions and their reason tags) is
already runtime-owned by the generic types above; what's left in `logit-inputs` is genuinely
impl-shaped: socket mechanics and the loop wiring them together.

`InternalInput` is untouched — no socket, no `receive:` block, keeps the default
`run_until_shutdown` (cancel-by-drop, unchanged). Not generalized toward `UdpListener`.

### `SO_RCVBUF`, and the Linux doubling trap

`tokio::net::UdpSocket` exposes no socket-option setters, so binding goes through `socket2::Socket`
(already present in `Cargo.lock` at 0.6.5, transitively via tokio/hyper — this promotes it to a
direct `logit-inputs` dependency, not a new tree entry), then converts to a tokio socket. Linux
doubles the requested value for its own bookkeeping, so a successful request routinely reports back
roughly 2× what was asked — a naive `granted < requested` check would never fire there. The warning
fires only when `granted < 2 * requested` on Linux (`granted < requested` elsewhere), naming
`net.core.rmem_max` (212992 B stock) as the sysctl actually doing the clamping. The granted value is
always gauged (`logit.input.receive_buffer.bytes`), whether or not an override was requested, so an
operator can see the kernel default without having to set anything first.

`receive_buffer_bytes` defaults to `None` deliberately: a nonzero default would exceed most stock
kernels' `net.core.rmem_max`, producing the clamp warning on every first run and training operators
to ignore the one warning that matters.

## Alternatives considered

- **`block` as the receive-queue default**, matching `buffer:`. Rejected — the core argument above;
  it relocates loss into the kernel rather than preventing it, and every mature UDP listener
  researched treats this the same way.
- **A builder-injected shutdown receiver instead of a trait method.** Rejected — see the dedicated
  paragraph above.
- **Closures instead of the `Queued` trait** for per-item weight/units. Rejected: a closure field
  would be `Box<dyn Fn(&T) -> u64>` — an allocation per queue, no inlining on the hottest loop in
  the program, and a type that no longer derives `Debug`. A trait call on a known type monomorphizes
  and inlines like any other method.
- **`tokio::spawn`/`spawn_local` for `read_loop`/`decode_loop`** (considered while writing this
  module's own tests, not just its production code). Rejected for production: it's the shutdown
  hazard named above. Rejected even for tests, where `'static` is the concrete blocker (a stack-local
  socket or `&mut decoder` can't satisfy it) — resolved instead with `tokio::pin!` plus `select!`/
  `join!`, running both loops as plain futures within the test's own task.
- **A defaulted `decode_at` method alongside the existing `decode`**, rather than widening `Decoder`
  itself. Rejected: it would leave the timestamp hazard opt-in per decoder rather than closing it at
  the trait boundary, and the buffer-reuse allocation win specifically needs the `&mut Vec<Event>`
  parameter, not just a receipt instant.
- **`recvmmsg` batched reads and `SO_REUSEPORT` multi-reader fan-in.** Real, field-precedented
  techniques (rsyslog `batchSize`, gostatsd `--max-readers`), but out of scope here: `recvmmsg`
  needs raw-fd work via `try_io` plus `libc` since tokio's `UdpSocket` doesn't expose it, and
  `SO_REUSEPORT` fan-in collides with the single-`Fanout`-owns-shutdown constraint (N readers each
  holding a `Fanout` clone). Recorded as new `docs/known-gaps.md` entries.
- **Sampling the kernel's own per-socket drop counter** (`/proc/net/udp[6]`'s drops column).
  Genuinely valuable — almost no tool in the field does this in-process, they all tell operators to
  run `netstat -su` — but Linux-only and a separate, self-contained addition; recorded as a new
  `docs/known-gaps.md` entry rather than folded in here.

## Consequences

- `crates/logit-pipeline/src/sink_queue.rs` → `queue.rs`: generalized into `BoundedQueue<T: Queued>`;
  `SinkQueue`/`SinkQueueConfig` become type aliases/conversions, zero behavior change on the sink
  side (every existing test passes unmodified). Gains `pop()`.
- `crates/logit-pipeline/src/accumulator.rs` (new): `BatchAccumulator`, `FlushReason`.
- `crates/logit-proto/src/lib.rs`: `Decoder::decode_into` is now the trait's core method;
  `decode()` becomes a provided default. `crates/logit-inputs/src/{statsd,syslog}.rs`'s decoders
  updated; no other call site needed to change.
- `crates/logit-pipeline/src/input.rs`: `Input::run_until_shutdown` (defaulted), `InputRuntimeConfig`.
  `crates/logit-pipeline/src/runtime.rs`: `run_input` gains a `shutdown_grace` parameter and races
  the grace backstop; `NodeSpec::Input` becomes a two-tuple.
- `crates/logit-inputs/src/udp.rs` (new): `UdpListener<D>`, `Datagram`, `RECEIVE_QUEUE_METRICS`,
  `UdpListenerConfig`. `StatsdInput`/`SyslogInput` become thin wrappers with an added
  `with_receive` builder; neither contains a `recv_from` call any more.
- `Cargo.toml`/`crates/logit-inputs/Cargo.toml`: `socket2` promoted to a direct dependency (already
  present transitively).
- `crates/logit-config/src/lib.rs`: new `ReceiveConfig`, a sibling field of `Component::buffer`;
  new `human_bytes::option` codec. `crates/logit-pipeline/src/graph.rs`: validation rules 16
  (datagram-listener-only) and 17 (zero-bound rejection, `batch_flush_interval: 0s` exempted).
  `crates/logit-cli/src/pipeline.rs`: `build_spec` derives `UdpListenerConfig`/`InputRuntimeConfig`
  from `component.receive`.
- New telemetry: `logit.component.receive.{datagrams,bytes,utilization,push.blocked.duration,latency,flushed}`,
  `logit.component.{datagrams,bytes}.dropped`, `logit.input.receive_buffer.{bytes,requested.bytes}`
  — see `docs/design/internal-telemetry.md`'s catalog.
- `docs/known-gaps.md`: the "Delivery I/O is not decoupled…" entry is deleted (both halves closed);
  new entries record kernel-drop visibility, `recvmmsg`, and `SO_REUSEPORT` as still open.
- Measured allocation change: `statsd_in`/`syslog_in` per-line decode drops from 2/1 to 1/0
  allocations in steady state (`crates/logit-bench/tests/allocations.rs`, `docs/design/memory.md`
  §2) — a strict improvement, not merely a neutral refactor, because the accumulator's own need for
  a reusable buffer forced the `decode_into` signature that makes it possible.
