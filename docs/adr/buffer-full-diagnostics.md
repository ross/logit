---
created: 2026-10-21
updated: 2026-10-25
---

# Full-buffer diagnostics: a full sink buffer or receive queue logs once a minute, and a gauge reads a blocked push live

## Status
Accepted

## Context
A 4.5-hour network outage left a `logit_out` sink's in-memory buffer (`overflow: block`,
`max_batches: 1024`) full for about 4 hours 15 minutes, and another sink's buffer full on
`max_bytes`. The only log lines were the sink's `retrying` errors, once a minute, and a final
`recovered`. Nothing said that a buffer was full, that intake was blocked, or, under a `drop_*`
policy, that data was being shed. During such an outage the edge's telemetry can't reach its
metrics backend, so its own log is the only live signal.

Two gaps caused that:

- **A full buffer logs nothing.** `BoundedQueue::count_dropped` and `DiskQueue::count_dropped`
  count `batches.dropped`/`events.dropped{reason=overflow_oldest|overflow_newest}`, and a `block`
  push waits, both without a line.
- **The blocked-push timer reads zero during a stall.**
  `logit.component.buffer.push.blocked.duration` records one sample when a wait ends, so a
  four-hour wait shows nothing until it ends, then one spike.

The same `BoundedQueue` backs a datagram listener's receive queue
([ADR `decoupled-listener-io`](decoupled-listener-io.md)), which has both gaps too.

## Decision
Every sink buffer (in memory or on disk) and every datagram listener's receive queue tracks a
**full episode**, logs it, and exposes a live blocked-push gauge. `crates/logit-pipeline/src/buffer_full.rs`
implements it once for all three; its module doc is the canonical statement of the rules.

1. **An episode opens when a push finds no room**: a `block` push starts waiting, or an
   `overflow_oldest`/`overflow_newest` drop sheds an item. That includes `block`'s fallbacks,
   which evict: a batch that could never fit, and a push racing `close`. The line describes what
   happened, not the configured policy alone: `overflow block is shedding the oldest batch(es)`
   for the never-fits fallback.
2. **Three keys.** `buffer_full` (warn) on opening, naming the bound hit with its configured value
   (`buffer.max_batches 1024`, `buffer.max_bytes 64MiB`, `buffer.disk.max_bytes 1GiB`,
   `receive.max_datagrams 10000`, `receive.max_bytes 32MiB`) and the `overflow` policy. It repeats
   at most once a minute while the episode stays open, with the items and units shed so far, or
   how long intake has been held. `buffer_recovered` (info) when an announced episode ends, with its
   duration and totals. `buffer_full_at_shutdown` (info) when the queue's owner finishes with an
   announced episode still open.
   A sink's `buffer_full` also names its concurrency and its last send's duration when every
   batch the concurrency allows is in flight ("Amendment: the line names a saturated
   concurrency (2026-10-25)").
3. **The minute repeat under `block` comes from the waiting push.** No new push arrives while one
   waits, so the waiting push awaits its `Notified` under `tokio::time::timeout_at`, writes the
   line on the first wake at or past its due time, and goes around its wait loop as after any
   wakeup. The wake re-checks the queue and changes nothing else. Decision 6 sets the wake's
   period.
4. **An episode ends below `recover_below`.** After a consumer-side change (a `commit`, `pop`,
   `pop_many`, or `take_all`, or the spool's `commit` or corrupt-record skip), if utilization, the
   same `max(items ratio, bytes ratio)` the `utilization` gauge reports, is below `recover_below`
   and no push is waiting, the episode ends. A producer-side change never ends one. `recover_below`
   is a new field on `buffer:` and `receive:`, default `0.75`, valid in the open interval `(0, 1)`
   (graph rules 15 and 18). `0` would never end an episode. `1` is rejected because it gives no
   hysteresis: a queue held at its bound would end and reopen an episode on every pop and push.
5. **One clock per queue bounds the line rate.** At most one `buffer_full` line a minute, whichever
   episode it's for. Hysteresis can't help when one item fills more than `1 - recover_below` of a
   bound (`max_batches` of 4 or less at `0.75`, or a batch over a quarter of `max_bytes`): each pop
   then drops below the threshold and the next push finds no room again, an episode per item. An
   episode that opens inside the minute stays unannounced until the clock allows a line. If it
   ends first it writes no `buffer_recovered`, and its counts ride on the next `buffer_full` as
   `N shorter episode(s) since the last line shed ...`.
6. **A gauge reads a blocked push live.** `logit.component.buffer.push.blocked` and
   `logit.component.receive.push.blocked` read `1` while a `block` push waits for room, else `0`.
   A drop guard held across the wait lowers it however the wait ends: room, `close`, or the push
   future's cancellation. The `*.push.blocked.duration` timers are unchanged: one sample per wait,
   recorded when the wait's `Timer` drops, which on cancellation records the time to cancellation.

   **The waiting push re-writes the `1` every 500 ms.** `ComponentBuffer::drain` takes every
   point, so a gauge written only on its transitions shows in one `internal` drain window and then
   vanishes for the rest of a multi-hour stall, and an exposition endpoint expires it after five
   minutes (internal-telemetry.md's "Why a constant is re-emitted"). The re-write rides the same
   wake as the minute repeat: decision 3's `timeout_at` deadline is 500 ms ahead instead of the
   next repeat's due time, and each wake re-writes the gauge, writes a repeat if one is due, and
   goes around the wait loop. A queue doesn't know `internal`'s `interval`, which any positive
   duration can set, so the period is a constant: half the shortest interval a shipped config uses
   (`1s`), so each window of 1 s or longer holds a write. Each deadline is set 500 ms after the
   wake that sets it, and tokio rounds a deadline up, so writes land a little over 500 ms apart and
   a 500 ms window can miss one; an `interval` under 1 s can see windows without it. The deadline
   is always 500 ms past the wake, never a due time already passed, so a repeat that isn't written
   can't make the wait spin. That happens when `finish` closes the episode while a push still
   waits; recovery can't close it then, since `utilization` does nothing while a push waits.
7. **Shutdown isn't a recovery.** `SinkStore::finish` and the UDP listener's residual drain end an
   open episode before they empty the queue, so the emptying they do (counted `shutdown`, or
   spooled for the next start) never reads as `buffer_recovered`.
8. **No counter, no throttle.** The lines go through `Diagnostics::warn_keyed` and
   `Diagnostics::info`: unthrottled, keyed, and uncounted in `logit.component.diagnostics`, like
   `write_loop`'s `retrying`. Time paces them, so the power-of-two `warn_throttled` (which never
   resets) is the wrong tool. The `*.dropped` counters stay the record of loss
   ([ADR `telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md): a diagnostic beside an
   existing `*.dropped` counter, never in place of one).

## Alternatives considered
- **Episode lines with no shared clock.** Exact per-episode lines, but a coarse bound (decision 5)
  writes two lines per batch for as long as the queue is overloaded, the flood the repeat interval
  exists to prevent.
- **A minimum episode length before `buffer_recovered`.** Recovery would need a later event to
  confirm it, and an idle queue has none, so a real recovery could go unlogged.
- **Recovery on every gauge update, producer side included.** A `drop_newest` push rejected on the
  byte bound at 60% utilization would end its own episode in the same call. Ending only on the
  consumer side matches what recovery means: the consumer drained it.
- **Logging from `write_loop`.** It sees a held head, not a full queue, and the receive queue has
  no `write_loop`.
- **A spawned task per queue for the repeat and the gauge.** A task to supervise and cancel at
  shutdown, for a wake the waiting push can take itself.
- **Re-writing the gauge from an existing periodic sampler.** The only per-component sampler,
  `sockstat`'s once-a-second read, runs on UDP listeners alone; sinks have none.
- **A gauge that survives a drain.** A held-value kind in `ComponentBuffer` would fix every
  transition-only gauge at once, but changes the drain contract every component relies on, for one
  new metric.

## Consequences
- An operator sees `buffer_full` within the push that fills a buffer, then one line a minute, then
  `buffer_recovered`, with no metrics backend.
- `recover_below` joins `buffer:` and `receive:`, and `schema/logit.schema.json` with it.
- **A waiting `block` push needs tokio's time driver.** Every runtime `logit run` builds has it.
  With no runtime in context (a test polling by hand) the push awaits room with no repeat; a
  runtime built without `enable_time` panics when a push first waits.
- **A spool recovers in whole segments.** `DiskQueue`'s `total_bytes` falls only when a consumed
  segment is deleted, so a spool whose `segment_bytes` is a large share of `disk.max_bytes` ends an
  episode late, by the same reading its `utilization` gauge gives.
- **A spool reopened full opens no episode** until a push finds no room.
- **An unannounced episode that's never followed by another line** leaves its counts only in the
  `*.dropped` counters.
- The consumer side pays one atomic load per gauge update while no episode is open. No allocation
  pin moved: the episode's bookkeeping runs only when a push finds no room or an episode is open.

## Amendment: the line names a saturated concurrency (2026-10-25)

[ADR `sink-request-concurrency`](sink-request-concurrency.md), decision 2, asks the `buffer_full`
line to name the cause when a sink at its default `concurrency: 1` can't keep up with the round
trip. A sink's `buffer_full` line, the first and each repeat, gains one suffix:

| Suffix | Written when |
|---|---|
| `; sending at its concurrency of N, the last send took D` | the write loop has `N` batches in flight and `N` is the most it may: `concurrency:`, or 1 on a sink without the field. `D` is the last answered send's duration, from its submit or attempt to its answer, success or failure |
| `; sending at its concurrency of N, no send answered yet` | as above, before any send was answered |

`write_loop` records the limit, the batches it waits on, and each answered send's duration in the
store's `SendLimit` (`crates/logit-pipeline/src/buffer_full.rs`), which the line reads when it's
written. A loop in a retry backoff waits on no send, so a held head never reads as saturated:
`retrying` names that cause. A receive queue has no write loop and gets no suffix. The line
names the limit, not the remedy: raising `concurrency:` reorders batches at the destination, which
each sink's docs weigh.
