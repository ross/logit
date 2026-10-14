---
created: 2026-10-13
updated: 2026-10-13
---

# Stale events are filtered by a `drop_stale`/`has_stale` transform pair

## Status

Accepted

## Context

Nothing between a sender's buffer and `aggregate` limits an event's age. `logit_in` accepts or
deduplicates a frame by sender identity and sequence
([`native-hop-named-acks`](native-hop-named-acks.md)), never by timestamp. So after an outage,
everything a `logit_out`'s `buffer:` or `buffer.disk:` held is delivered, however old it is.

That's correct for a pass-through sink, but wrong in front of `aggregate`. Its windows are
wall-clock: `Aggregator::process` merges each event into whatever has accumulated since the last
flush tick, and `flush(now)` stamps every output with `now`
([`aggregation-window-semantics`](aggregation-window-semantics.md)). A replayed hour of counters
lands in the current 10-second window and produces a plausible, wrong value. Restamping the event
with `timestamp` doesn't help, because `aggregate` never reads the timestamp to pick a window.
The same is true of other consumers that keep state across events: a Lua `flush()`, `shape`, and
`prometheus_out`'s exposition registry.

Two sinks judge age on their own, at the far end of the pipeline:

- `datadog_out` drops points older than the intake accepts and counts them as
  `logit.output.records.dropped{reason="stale"}`.
- `kafka_out` stamps each record with its write time, so Kafka's future-timestamp rule never
  applies to it.

Neither protects anything upstream of it.

## Decision

Two transform kinds judge an event's age against the wall clock. `drop_stale` forwards fresh
events and discards stale ones. `has_stale` forwards stale events and discards fresh ones. Given
the same configuration, the two are exact complements: every event leaves through one of them.
The pair follows the `has_*`/`drop_*` shape of
[`attribute-filtering-components`](attribute-filtering-components.md) and
[`provenance-filtering-components`](provenance-filtering-components.md).

```yaml
fresh:
  type: drop_stale
  sources: [central_in]
  max_age: 5m
  max_ahead: 1m   # optional; off by default
archive:
  type: has_stale
  sources: [central_in]
  max_age: 5m
  max_ahead: 1m
```

The module doc of `crates/logit-transforms/src/stale.rs` is the canonical statement of the rules
below.

**The instant judged.** It is the time the event finished:

- the span's `end_timestamp` when the event has a span and that field is non-zero;
- `event.timestamp` otherwise.

`event.timestamp` is a log's time and a metric's point time, since the model has no separate point
time, and a span's start. A long span that started before the bound but ended inside it is fresh.
`observed_timestamp` is never read, because most decoders leave it at 0. `start_timestamp` is never
read either, because a cumulative series' start is legitimately old. The verdict is one per event.
Payloads are never stripped, which keeps the pair an exact partition.

**The bounds.**

- An event is stale when `now - instant > max_age`. An age equal to `max_age` is fresh.
- With `max_ahead` set, an event is also stale when `instant - now > max_ahead`.
- `max_age` is required. `max_ahead` is off by default, because a future stamp doesn't damage an
  `aggregate` window.
- Both must be greater than zero. Graph rule 87 enforces this.

**Unknown time.** An instant of 0 means unknown in the data model, so the event is never judged
stale. `drop_stale` forwards it and `has_stale` discards it, keeping the partition exact.

**The reference clock.** The filter compares against the wall clock, read once per batch, so every
event in a batch gets the same `now`. A receive time isn't an option: decoders fold the time they
received a datagram or request into `event.timestamp` and keep no separate copy, and `EventBatch`
carries none. Comparing against the wall clock is also what makes the filter work at a central
tier. The native hop carries `event.timestamp` across unchanged, so a statsd line stamped at the
edge keeps its edge time through a replay. The operator sizes `max_age` to cover clock skew between
tiers, one flush interval, and normal pipeline latency.

**Accounting.** This follows [`telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md): a
discard here is by design, not loss.

- The runtime counts each discard from either kind as `logit.component.events.discarded` under the
  filter's id. Nothing is counted as `*.dropped`.
- Both kinds add a counter that says why, which the runtime's count can't:
  `logit.transform.stale.exceeded{bound="max_age"|"max_ahead"}` for each event classed stale, and
  `logit.transform.stale.unjudged` for each event with an unknown time. Both are emitted only when
  nonzero.
- `drop_stale` also warns through a throttled diagnostic (key `stale_event`) with the observed age
  and the bound it crossed. `has_stale` doesn't, because discarding fresh events is its normal
  work.

**Placement.**

- Put `drop_stale` immediately before `aggregate`, or before any other consumer that keeps state
  across events.
- Put it after `timestamp` when one resolves the record's own time. A backlog that `tail_in`
  re-reads after a restart carries its read time unless `timestamp` restamps it, and an event
  carrying its read time is fresh.
- Archive what `has_stale` forwards to an ordinary sink. A `file_out` with `format: native` keeps
  every field.

## Alternatives considered

- **A `max_age` option on `aggregate`.** It couldn't be forgotten, and it could default to a
  multiple of `interval`. Rejected for three reasons:
  - It puts event-time policy into the one component whose ADR rules out event time.
  - It protects only `aggregate`, not a Lua `flush()` or `prometheus_out`'s registry.
  - It gives an operator no way to keep the stale events.

  A separate transform composes with every consumer and supports archiving.
- **Routing stale events with `route`.** `route` matches attribute equality and directs only into
  `target` kinds, so it can't express an age comparison. Two filters reading the same source split
  the stream with no new routing machinery, the same way `has_attributes` and `drop_attributes` do.
- **Stripping stale payloads and keeping the rest of the event**, as `drop_signals` strips one
  signal. Only a span-carrying event can have payloads with different times, and an access-log
  span ends milliseconds after its log line. Stripping would cost the exact-partition property for
  a case that doesn't occur in practice.
- **One component with a `mode:` field.** The `has_*`/`drop_*` pairs already establish the naming
  an operator expects, and a pair keeps each config to the fields it needs.
- **A reason tag on `component.events.discarded`.** The loss-vocabulary ADR keeps that counter
  untagged. The "why" belongs in a component counter, which `stale.exceeded{bound}` is.

## Consequences

- **Cumulative temporality trades a correct total for a clean rate.** Under
  `temporality: cumulative`, a replayed delta that `aggregate` merges late leaves the running total
  correct: `rate()` spikes, but `increase()` over the range is right. Discarding it leaves the total
  permanently low by the counted amount. For delta windows and gauges, discarding is the correct
  answer. Before placing `drop_stale` ahead of a cumulative `aggregate`, an operator chooses
  between the two. The field docs say so, and nothing enforces either choice.
- **A resend after `logit_in` evicts a sender's identity is usually stale**, so `drop_stale` also
  discards those duplicates. A fresh duplicate still passes, as
  [`native-hop-identity-and-sequence`](native-hop-identity-and-sequence.md) accepts.
- **Each batch reads the clock once.** A batch that takes longer than its own spread of ages to
  process is judged against its first event's `now`. That's the same trade `datadog_out` makes.
- **The filters read only event fields that `trace_context` and the decoders already set**, so no
  decoder or the event model changes.
