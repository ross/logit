---
created: 2026-10-16
updated: 2026-10-23
---

# `route_signals`: a native signal router that splits a mixed event into one part per target

## Status
Accepted

## Context

Configs that send each signal somewhere different (logs to Loki, metrics to VictoriaMetrics,
spans to Tempo) spell it, since [ADR `target-components`](target-components.md)'s 2026-10-16
amendment, as `route by: signal`, one `target` per signal, a `keep_signals` gate behind each target,
and a `null_out` for the events no route claims. Four kinds of component for one idea, and the gates
are there because `route` forwards a mixed event whole: a log-and-metrics event reaches the logs
target with its metrics attached, and only a gate strips them.

`route` can't strip. The `Router` contract is that a router never mutates or absorbs an event:
`Router::route(&Event)` borrows the event and returns a mark, and the partition moves or shares it.
Bending that contract so one router kind can rewrite events would put a mutation on every router's
path, and the shared-`Arc` fast path depends on events going out unchanged.

The other half of the cost is the else-branch. Rule 50 requires a router to have an ordinary
consumer, so a `route by: signal` with all three signals routed still needs a `null_out` for the
payload-less events no signal claims. That rule exists because an unrouted event is a config
choice the graph should show ([ADR `telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md)'s
"Count unrouted events as an intended outcome" alternative). For signal routing the config already
shows it: an event survives only if it carries a signal the operator listed, as with
`keep_signals`' list.

## Decision

**A new native kind, `route_signals`, sends each signal an event carries to that signal's target,
and by default splits a mixed event so each target receives only its own signals' payloads.**

```yaml
by_signal:
  type: route_signals
  sources: [remote_in]
  strip: true            # the default
  signals:               # signal -> target id
    metrics: metric_events
    logs: log_events
    traces: log_events   # two signals may share a target
```

### Config

`signals:` is a struct with three optional fields, `logs`, `metrics`, and `traces`, each one target
id, with unknown keys rejected. Any of them may be unset, all of them included. A value is one id,
never a list: a signal that should reach two places is a `route` concern (a list value) or a fan-out
downstream of the target, and one id per signal keeps every part single-destination. `strip` is a
boolean that defaults to `true`.

### Graph

`route_signals` is a transform by role. Its router -> target edges come from `signals:`, keyed by
the signal name, so `logit graph` labels each dashed edge `logs`, `metrics`, or `traces`, and
`targets_of` orders its slots `logs`, `metrics`, `traces`, first occurrence wins. Rules 5 (cycles),
47 (no `targets:` list), 48 (every id resolves to a `target`), and 49 (a target needs a directing
router) apply as to any router.

**It has no ordinary edge.** New rule 92 rejects any component that lists a `route_signals` in
`sources:`, and rule 7 doesn't ask it for a consumer. Rule 50 doesn't apply: an event no listed
signal claims is discarded, not forwarded, so there is no else-branch to give a consumer. Rule 92
also rejects an empty target id. A `route_signals` with no signal set discards every event (see
"Amendment: no signal set is legal" below).

**Reconvergence depends on `strip`.** Under `strip: true` a component may list several targets of
one `route_signals`: each part carries different payloads, so nothing is counted twice at the
payload level. The event-level count downstream sees one event per part, the same as an
operator-placed diamond of two `keep_signals` gates: `events.received`, a field-less `kv_metrics`
counter, or `shape` reading both targets counts a log-and-metrics event twice. That's what the
config says, and the remedy is in it: send both signals to one target to keep them on one event.
Under `strip: false` each target receives the whole event, so rule 92 rejects a component listing
two of its targets, as rule 51 does for a `route` list.

### Runtime

A new `NodeSpec::RouteSignals` arm, not a `Router` (it mutates) and not a `Transform` (it fills
targets). Its loop has `run_router`'s shape: one child `BatchContext` and one `"process"` span per
incoming batch, every partition sent under that context. It never sends to slot 0, since it has no
ordinary edge.

The per-batch body, `route_signals_batch`, keeps `route_batch`'s four passes and its
`reserve_exact` allocation shape, sharing the count, reserve, and partition-assembly code. The
eight-entry table of marks by carried-signal combination, which `route by: signal` used, is now
`logit_pipeline::signal_masks`, and `route_signals` uses it with an empty mark that means
discard.

**`strip: false`** routes as the removed `route by: signal` did with every signal routed: each target
receives the whole event, an event bound for several targets is cloned into each but the highest
slot, and a batch whose every event carries one multi-target mark goes out whole behind one shared
`Arc`. An unlisted payload rides along inside a listed signal's event.

**`strip: true`** splits. The semantics, also in `crates/logit-pipeline/src/route_signals.rs`'s
module doc:

1. An unlisted signal's payload is cleared from every event before routing.
2. An event's metric records move whole: they're one signal.
3. Signals that share a target stay on one part, so an event yields at most one part per target.
4. The part for the highest slot is the original event, with its own `AttrMap`. Every other part
   clones the attributes and takes its payloads by move, never by clone.
5. Each target receives its parts in arrival order.
6. Every part keeps the original's timestamp, its resource `Arc`, and its scope, and goes out under
   the node's one child context, counted in its one span.
7. A `Delivered::Shared` batch arriving from a fan-out pays `unwrap_batch`'s copy-on-write clone
   before the split, as every mutating node does. There's no shared path out: each part is a new
   event.
8. A log's `TraceRef` lives on the log record, so a metrics or span part doesn't carry it. A
   `sample` with `key: trace_id` downstream reads a metrics part as missing the key and applies its
   `missing:` verdict. To give a log, its metrics, and its span one verdict, place the `sample`
   ahead of `route_signals`, where it keeps or drops the whole event. The private `_logit.*`
   carriers are attributes and reach every part.

A batch whose every event goes to one target stays on the move path either way.

### Telemetry

- **`logit.component.events.copied`** counts every delivery past an event's first: under
  `strip: false`, the extra whole copies; under `strip: true`, the parts past the first, one per
  extra target. A part is a new event sharing the original's attributes, and counting it keeps one
  identity for both modes: `events.received + events.copied − events.discarded` equals the events
  across the partitions sent. That's the targets' `events.sent` only when no other router feeds
  those targets.
- **`logit.component.events.discarded`** counts an event carrying no listed signal, an empty event
  included. Under `strip: true` it counts the event stripped to nothing.
- **`logit.transform.payloads.stripped{signal}`** counts each payload slot `strip: true` cleared,
  the counter `keep_signals` uses.
- Never `*.dropped`: the `signals:` map names the discard, as `keep_signals`' list names its own.
  With all three signals listed, only an empty event is discarded, as `has_signal` and
  `keep_signals` also do.

All three are tallied per batch and recorded only when nonzero.

## Alternatives considered

- **Widen the `Router` contract so a router may mutate.** Every router would pay for a hook only
  one kind uses, and the shared-`Arc` path would need a mode check. A separate node arm costs one
  `NodeSpec` variant and a loop that reuses the partition code.
- **A `strip:` field on `route by: signal`.** A field that's only valid for one `by:` form, which
  also changes whether `route` mutates and whether it needs an ordinary consumer. `route`'s other
  forms read a key and never touch payloads; signal routing is a different operation.
- **Keep the `keep_signals` gate behind each target.** Correct, and what configs do today, but each
  gate behind a `route` target is another node, another channel hop, and another component in the
  config to say what `signals:` already says. A split pays one attribute clone per extra part where
  the gate shape pays a whole-event clone per extra branch, or ships unwanted payloads to a sink.
- **Require an ordinary consumer, as rule 50 does for `route`.** With every listed signal routed,
  the only events left are those carrying no listed signal, and the operator listed which signals
  to keep. A mandatory `null_out` would restate the `signals:` map.
- **Reject reconvergence under `strip: true` too.** Parts are disjoint by payload, so a consumer
  reading two targets receives no payload twice. Rejecting it would forbid a legal and useful
  shape (one archive sink reading every target) to protect event-level counts that the config
  already makes visible.
- **A list per signal** (`logs: [loki, archive]`). It would make one signal's part
  multi-destination, which brings back the whole-copy question this kind exists to settle. A
  fan-out from the target does it with the shapes that exist.
- **Name it `split_signals` or `fork_signals`.** `route_signals` says it fills targets, as `route`
  does, and reads next to `keep_signals`.

## Consequences

- `ComponentKind::RouteSignals { signals: SignalTargets, strip: bool }`; schema regenerated.
- Graph: `target_edges`/`targets_of` arms, rule 92, and rule 7's exemption.
- Runtime: `NodeSpec::RouteSignals`, `run_route_signals`, `logit_pipeline::SignalRoutes` and
  `route_signals_batch`; `route_batch`'s pass 2-3 and partition assembly become shared helpers with
  no change to its allocation counts.
- `crates/logit-bench/tests/allocations.rs` pins four rows, mirrored in `docs/design/memory.md`:
  a single-signal batch moved into one target (2), a log-and-metrics batch split into two targets
  (3 plus one attribute clone per event), a `strip: false` batch shared whole (2), and a batch
  every event of which is discarded (0).
- The soak harness's `split = "route"` ledger refuses a `route_signals`, under either `strip:`
  value: its parts or copies land in several sinks, so the sinks' totals don't add up.
- `route by: signal` is removed, and the shipped configs that paired it with `keep_signals` gates
  and a `null_out` use `route_signals`. `route` keeps list-valued `routes:` values.
- Amendments, dated 2026-10-16: [ADR `target-components`](target-components.md),
  [ADR `telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md), and
  [ADR `signal-filtering-components`](signal-filtering-components.md).

## Amendment: a `none` target for an event that carries no signal (2026-10-21)

**`signals:` takes an optional fourth key, `none`, the target for an event that arrives carrying no
log, no metrics, and no span.** This decision assumed a payload-less event carries nothing any sink
can act on, so discarding it lost nothing. The InfluxDB relay made that false: a point whose fields
are all strings decodes to an event with only its `_logit.influxdb.*` carriers
([ADR `influxdb-line-protocol-relay`](influxdb-line-protocol-relay.md)), and `influxdb_out` writes
it. A `logit_in -> route_signals -> target -> influxdb_out` relay discarded every such point, a
printer's `filament v="PLA"` among them.

```yaml
remote_by_signal:
  type: route_signals
  sources: [remote_in]
  signals: {logs: remote_logs, metrics: remote_metrics, traces: remote_spans, none: remote_metrics}
```

- **The test reads the event on arrival**, before any strip. An event left empty only because
  `strip: true` cleared its unlisted signals carried a signal the operator chose to drop, so it's
  still discarded. Under `strip: false` an event carrying only unlisted signals is discarded too.
- **`none` may share a target with another signal**, as two signals may. A payload-less event
  goes to its target whole, once, as one part: it's routed, so it counts neither
  `events.discarded` nor `events.copied`.
- **It's opt-in.** Unset, an event that arrives empty is discarded and counted, as before. The
  router doesn't read `_logit.influxdb.*` or any other protocol's carriers: a generic component
  deciding by one protocol's private attributes would break
  [ADR `private-attribute-namespace`](private-attribute-namespace.md)'s ignore rule. `none` names
  the case by its shape instead, and covers any source that produces it.
- **Graph.** `none`'s edge is keyed `none` in `logit graph`, and its target takes the last slot,
  after `logs`, `metrics`, and `traces`. Rule 92 accepts a table that sets only `none`, or none of
  the four, and rejects an empty `none` id as it does any other. Rule 92's reconvergence clause
  leaves `none`'s target out of the co-marked set under `strip: false`: an event that reaches it
  reaches no other target. When `none` shares a signal's target, that target is in the set through
  the signal.
- **Telemetry.** "With all three signals listed, only an empty event is discarded" now holds only
  when `none` is unset; with all four listed, `route_signals` discards nothing.

The `signals:` vocabulary is shared with the three signal filters, which take `none` too
([ADR `signal-filtering-components`](signal-filtering-components.md)'s 2026-10-21 amendment).

## Amendment: no signal set is legal (2026-10-21)

Rule 92 no longer rejects a `route_signals` whose `signals:` sets no entry. No event carries a
listed signal, so every event is discarded and counted as `logit.component.events.discarded`.
Validation rejects a config only when it can't run, or when it would lose or duplicate data the
operator didn't ask to lose or duplicate; this loss is one the config states. Rule 92 keeps its
empty-id, `sources:`, and `strip: false` reconvergence clauses.

## Amendment: a part is a view, not a new event (2026-10-23)

[ADR `index-partition-router`](index-partition-router.md) replaces `strip: true` rules 1, 4, and 7.
Nothing is cleared before routing: each target receives a view of the shared batch, an index list
plus a signal mask, so a part clones no attributes and moves no payloads, and a batch arriving
shared goes out shared with no `unwrap_batch` clone first. The masked event that a view's iterator
yields is where the unlisted-signal contract lives: its `log()`, `metrics()`, and `span()` show
only the target's signals, and the payload fields aren't reachable through it.
`logit.transform.payloads.stripped{signal}` still counts each payload of an unlisted signal (one
no target's mask shows), as `strip: true` counts it today, and
`logit.component.events.copied` is renamed `logit.component.events.multicast`, with the same
arithmetic.

The amendment covers both `strip:` values. Under either, each target receives a view, so these
statements above no longer hold: the `strip: false` paragraph's "cloned into each but the highest
slot", "A batch whose every event goes to one target stays on the move path either way", the
Runtime section's reason that `route_signals` is not a `Router` because it mutates, and the
Telemetry bullet's "A part is a new event sharing the original's attributes". A part is a view,
and `route_signals` clones and moves nothing. The node arm stays its own `NodeSpec` because it
fills targets with no ordinary edge.
