---
created: 2026-11-04
updated: 2026-11-04
---

# `datadog_out` leaves off, and counts, a series point whose series and second an earlier accepted request sent

## Status
Accepted.

## Context
Datadog keeps one point per series per second. Within one batch, `datadog_out` already plans
for that: of the points of one series in one second, it sends the latest and counts the rest
`logit.output.metrics.dropped{reason="duplicate_timestamp"}`, or `normalized` for a state equal to
the point sent ([ADR `dedup-timestamps-transform`](dedup-timestamps-transform.md), "The sink-side
counter"). Two such points in two batches both left, in two requests, and Datadog kept the later.
The sink counted the earlier as delivered, and its loss went uncounted
([`docs/known-gaps/datadog.md`](../known-gaps/datadog.md)).

The soak scenario `delta-datadog` met that pair on a generator restart. The generator's
`aggregate` sent a full 5 s window for each of 20 series and, on shutdown, a 0.5 s partial window
in the same whole second. The two windows reached the system under test's `datadog_out`, one
process life, in two batches, so both points left in two requests. No configuration avoids it: the
flush's offset into a second depends on when the process stops. `dedup_timestamps` doesn't reach
it either, since it holds no state between batches (that ADR's "Scope: one batch, and no added
latency").

### Verified destination behavior (2026-11-04)

Four probes against a Datadog trial org on `datadoghq.com`, one metric name per probe
(`logit.probe.samesec.f5c1aedc.<probe>`, tagged `probe:f5c1aedc`). Each sent two `POST
/api/v2/series` JSON requests about 4 s apart, each one point at the same timestamp,
`1793765277`. Every request drew `202 {"errors":[]}`. A `GET /api/v1/query` over 60 s either side
read the stored point at a 1 s rollup 10 s after the second write. It read the same value twice
more over the next minute and again 7 minutes later.

| Probe | First request | Second request | Query | Stored |
|---|---|---|---|---|
| a | `count` 10 | `count` 3 | `sum:<name>{*}.as_count()` | 3 |
| b | `count` 3 | `count` 10 | `sum:<name>{*}.as_count()` | 10 |
| c | gauge 10 | gauge 3 | `max:<name>{*}` | 3 |
| d | `rate` 10, `interval: 10` | `rate` 3, `interval: 10` | `sum:<name>{*}`, and `.as_count()` | 3, and 30 |
| e | one request: `count` 10, then `count` 3 | -- | `sum:<name>{*}.as_count()` | 10 |

What this verifies, and what it doesn't:

- **Verified:** across two requests, the later write replaces the earlier for a `count`, a gauge,
  and a `rate`, whichever is higher (probes a and b). Datadog doesn't sum two counts. Within one
  request, the higher of two `count` points is kept (probe e), as the 2026-10-21 trial found.
- **Inferred:** that the protobuf body `datadog_out` sends behaves as the JSON body did. Both are
  the same v2 series route. The same is inferred for a histogram's `.bucket`, `.count`, `.sum`,
  `.min`, and `.max` series and a summary's `.quantile` series, which leave as `count` and gauge
  points. It's also inferred that "later" means the later to arrive, since only one order of
  arrival was sent per pair.

## Decision

1. **The sink remembers the latest second it sent each series in.** It records a series point
   once Datadog accepted the request carrying it. A later batch's series-route point of that
   series and second is left off. A series is the within-batch collapse's key: the metric name,
   the tag set, and the host, device, and resources.
2. **The point left off is counted as the within-batch collapse counts one:**
   `logit.output.metrics.dropped{reason="duplicate_timestamp"}`, or `normalized` for a state (a
   gauge not written as a `rate`, or a cumulative kind) equal to the point sent: the same value by
   its bits, wire type, unit, interval, source type, and origin. Two equal delta counts or `rate`s
   are two increments, so one left off stays `dropped`. The reason is the same because the fact
   is the same: Datadog holds one point per series per second. The diagnostic is its own,
   `duplicate_timestamp_sent`, because no batch-local remedy reaches this pair. It names one
   point per series per second upstream: an `aggregate` window, or a `dedup_timestamps` over
   batches that hold the whole second.
3. **The earlier point stays.** It's already stored, and `logit.output.records` already counted it
   delivered. The sink doesn't carry a total forward or merge values: a sink doesn't work around
   its destination's semantics, and summarization is opt-in and named
   ([ADR `lossless-transit`](lossless-transit.md)). So for a gauge, Datadog keeps the older
   reading of the second, where within one batch the sink sends the later. The loss is counted
   either way.
4. **The kinds are every series-route point:** a gauge, a `count`, a `rate`, and the series a
   histogram or summary expands to. The probe shows those overwrite. Distribution points and
   sketches aren't remembered or left off: Datadog keeps both of two distribution points in one
   second (2026-10-21), and a sketch is assumed to add.
5. **Only an accepted request is recorded.** That's a `2xx` whose body names no series dropped.
   A `Rejected` request stored nothing. An `Ambiguous` one may have, but the batch's retry resends
   it, and Datadog overwrites it with the same point. A `202` whose `errors` name a dropped series
   doesn't say which point, so none of its request's points is recorded: leaving a later point off
   after an earlier one was never stored would lose both. A request a shutdown cut mid-flight
   records nothing.
6. **The verdict is fixed once per batch.** The batch's first plan reads the memory, and every
   later attempt at the batch reuses that plan's set of points left off. A retry that read the
   memory again would find the points its own first attempt delivered and leave them off. That
   changes the request cuts, and with them the request numbers the settled-request memory reads
   ([ADR `sink-send-path-and-attempt-accounting`](sink-send-path-and-attempt-accounting.md)).
   The counts are the plan's, unit 0 of the sink's `BatchAccounting`, so they count once per batch
   through the gated handles, however many attempts and bisections the batch takes. A batch sent
   with no `observe_batch` reads the memory on each send.
7. **The memory is bounded in size and age.** It's two generations of at most 57,344 series
   each, a hash of the series to its latest second and a hash of the point's state. A lookup reads
   both generations, and a record writes the current one. The current becomes the previous, and the
   previous is forgotten, once the current is full or has been open for an hour of send time. An
   hour is the series route's stale window, past which a point of an older second is dropped
   `stale` before the memory is read. Each generation's table holds at most 65,536 buckets of
   25 bytes, so the memory is at most about 3.2 MiB.

## Alternatives considered
- **Carry the stored total forward** (option f): add the earlier point's value into the later and
  send the sum. It makes Datadog hold the second's total for a `count`, and works around the
  destination's overwrite. Rejected: a sink doesn't repair its destination's semantics. The
  operator places the summarization upstream, where the graph shows it.
- **Send the later point and count the earlier as lost.** Datadog would then hold the later
  reading, as the within-batch collapse leaves it. But the earlier point is already counted in
  `logit.output.records`, so its loss would be reported after its delivery. Leaving the later
  point off keeps every count final when it's made.
- **Count the pair and send both.** A point counted dropped that still reaches Datadog misstates
  both counters.
- **Remember every second sent, not the latest.** It catches a repeat of an older second, an
  out-of-order arrival, at a table entry per series per second. The pairs seen in practice repeat
  a series' newest second: a shutdown flush after the last full window.
- **Persist the memory across restarts.** It would catch a pair split across two lives of the
  sink's own process. That needs the disk state and recovery rules of a spool for a pair a restart
  makes rarely, and a disk-buffered batch replayed after a restart is a new batch anyway.
- **An exact series key instead of a hash.** The key owns the metric name, every tag, the host,
  device, and resources, which is hundreds of bytes per series. Two distinct series whose 64-bit
  hashes collide read as one, with a chance near `2^-64` per pair. The worst case is a point left
  off and counted as a duplicate.

## Consequences
- **The soak's pair is counted, not lost silently.** Two batches through one `datadog_out`
  process life, the `delta-datadog` case, leave the later point off with a nonzero
  `metrics.dropped{reason="duplicate_timestamp"}`. The soak harness's `ledger.value` reads that
  counter as a delta lost short of the stand-in, and `ledger.standin`'s same-second `WARN` now
  names only the pairs the sink can't catch.
- **A restart forgets.** A pair whose two points leave from two lives of the `datadog_out`
  process is both sent, and Datadog keeps the later, uncounted. That's the case when the process
  that restarts is the one running the sink: an SUT restart, not a generator restart.
- **Above `concurrency: 1`, a pair in flight together is missed.** A batch's points are recorded
  when `await_ack` applies its answer. A batch planned while the earlier one is still in flight
  doesn't see it, and the two can land in either order.
- **A series forgotten by the bounds is missed:** one not sent for two generations, or one beyond
  57,344 series a generation when a flow carries more.
- **Only the newest second is caught.** A repeat of an older second than the series' latest sent
  is sent, and overwrites.
- **An upstream resend of a `count` or `rate` reads as a loss.** An identical increment in the
  same second from a later batch counts `dropped`, as within one batch. If it's a resend of the
  same point, as when an upstream hop retries an ambiguous request, Datadog holds the same value
  either way and nothing was lost. The sink can't tell a resend from a second increment.
- **A `datadog_out` pointed at a `datadog_in` leaves the same points off.** The sink models
  Datadog's intake whatever answers it, as its within-batch collapse does, so a relay chain or a
  test through `datadog_in`, which keeps every point, loses the repeats too, counted. The soak
  scenario `datadog-ingress` spreads its points over 4,000 series, and the `datadog-relay` perf
  scenario gives each point its own series, so neither repeats a series in a second.
- **Every batch with a series point runs the duplicate plan**, not only a batch with two points,
  since its points must be recorded. That's a key per point and a table per batch on the series
  route. `datadog_out` has no allocation pin in `crates/logit-bench/tests/allocations.rs`, and
  [`docs/design/memory.md`](../design/memory.md) §5 records the memory's bound.
- [ADR `dedup-timestamps-transform`](dedup-timestamps-transform.md) gets an amendment pointing
  here, and `docs/known-gaps/datadog.md`'s entry narrows to the cases above.
