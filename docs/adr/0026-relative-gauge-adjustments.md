# 0026 — Relative gauge adjustments (`+`/`-` in statsd)

## Status
Accepted

## Context

`docs/known-gaps.md` documents that `statsd_in` rejects any leading `+`/`-` on a gauge value with a
clear decode error: per the statsd and DogStatsD specs, a leading sign means "adjust the previous
value by this much," not "set the value to this negative/positive number." Applying a relative
adjustment needs state the decoder doesn't have — the running value of that gauge, which belongs to
whichever component aggregates it, not to the wire decoder. `crates/logit-transforms::Aggregator`
now exists and is the natural place to hold that state, but until this change lands there is no
representation in `logit_core::MetricKind` for "this is a delta, not an absolute value" to hand it.

`docs/design/data-model.md` is explicit that aggregation state belongs to the `aggregate` processor,
not to `Event`/`MetricRecord`. A statsd gauge is sticky by protocol, though — the sender transmits
only on change and expects the last value to persist — so a delta in window *N+1* has to apply
against window *N*'s final value. Carrying gauge state across a flush is a real design decision with
its own risk, worked out separately in [ADR 0008](0008-aggregation-window-semantics.md)'s amendment;
this record is scoped to the *representation* question — what a decoded relative adjustment looks
like on the wire between `statsd_in` and `aggregate` — and to the resolution semantics `aggregate`
applies to it.

Note: this record was drafted as `0024`, the next free number at the time, alongside `docs/adr/`'s
pre-existing collision at `0020` (`0020-demo-stack-separate-from-dev-stack.md` and
`0020-trace-context-propagation-on-delivered.md`). By the time this branch merged with `main`,
`0024` had independently been taken by `0024-hand-rolled-grpc-over-hyper.md` (and `0023` by
`0023-committed-pregenerated-otlp-protobuf.md`) — a second instance of the same numbering-collision
pattern, resolved the same way as the `0020` one: renumbered to the next free number, `0026`, after
the merge. Resolving the `0020` collision remains not this record's job.

## Decision

### A new `MetricKind::GaugeDelta(f64)` variant, not `Gauge { value: f64, relative: bool }`

A new variant forces every exhaustive match over `MetricKind` to make an explicit decision about a
relative adjustment, rather than letting a `Gauge { value, .. }` pattern silently treat a delta as
an absolute value — the exact bug class this feature exists to avoid. The four exhaustive matches
this touches: `logit-core::event::metric_record_heap_bytes`, `logit-outputs::influxdb::render_fields`,
`logit-outputs::stdio::render_metric`, and `logit-transforms::aggregate`'s `Accumulator::new_for` /
`into_kind` / merge match.

Size is safe to add: `MetricKind` is 176 bytes, almost entirely the `Distribution` variant's inlined
`DDSketch` (`crates/logit-core/tests/type_sizes.rs`) — a seventh variant carrying one `f64` fits the
discriminant word's existing slack, confirmed by that same test staying numerically unchanged
(176/184/192/776) after this change landed.

**Recorded fallback, not taken:** if `MetricKind`'s size assertion ever fails because of this variant
(it did not), the fallback is to fold the flag into `Gauge { value, relative: bool }` instead and
accept the weaker guarantee — a wider `MetricKind` costs every event in every pipeline forever, for a
feature only statsd uses, so growing the type is not an acceptable trade to make silently. This
tradeoff is recorded here specifically so a future change that *does* need to touch `MetricKind`'s
size has this precedent to weigh against, not just a `git blame`.

### Any leading sign means relative — no config escape hatch

Both the statsd and DogStatsD specs say a leading `+`/`-` on a gauge value is a relative adjustment;
neither has wire syntax for setting a gauge to a negative absolute value, and both document the
zero-then-set workaround the mainstream client libraries implement for exactly that case. A
`statsd_in: negative_gauge: delta | absolute` config toggle was considered (see Alternatives) and
rejected — decisively, because **today `-5|g` is a hard decode error**, so there is no existing
working behavior this change needs to preserve. Nothing downstream has ever seen a negative absolute
gauge from this decoder; adding a permanent config surface to keep re-explaining a spec-defined
wire encoding buys nothing a plain reading of the spec doesn't already give.

### Resolution belongs to `aggregate`

`statsd_in` only decodes; it never resolves a delta against a running value. `MetricKind::GaugeDelta`
is explicitly **unresolved** — its own doc comment says so — and must never reach a sink. The
`aggregate` transform is the only component with a resolution rule and the running gauge state to
apply it against:

- an ordinary `Gauge` keeps today's last-write-wins-by-source-timestamp rule, comparing only against
  the `at` most recently set by another absolute;
- a `GaugeDelta` applies to the running value **in arrival order** and **never advances `at`**.

**These two rules are asymmetric on purpose.** Mixing "deltas in arrival order" with "absolutes by
last-write-wins" is undefined the moment they interleave unless one of them is pinned independently
of the other's tiebreak — the same three datagrams processed in a different order must not silently
produce a different window value. Pinning deltas to arrival order and forbidding them from touching
`at` is what keeps an absolute's LWW rule meaningful regardless of how many deltas land between two
absolutes, and keeps a delta's effect deterministic regardless of how many absolutes preceded it.
`crates/logit-transforms/src/aggregate.rs`'s merge match implements this exactly; see its own test
module for the interleaving cases this rule is pinned against.

A `GaugeDelta` that reaches a sink with no `aggregate` on its path is a real, if narrow, failure
mode: it degrades from a clear statsd-level decode error today to a throttled, per-metric drop at
the encoder (`influxdb_out`'s `render_fields`, under its own `gauge_delta_unresolved` diagnostic key
rather than the generic `encode_error`) once this lands. See Consequences.

## Alternatives considered

- **`statsd_in: negative_gauge: delta | absolute` config toggle.** Rejected. A permanent config
  surface for a spec-defined wire encoding has to be explained forever, and there is no working
  behavior today it would be preserving — `-5|g` is a hard decode error as of this record, so
  "absolute" was never a real, previously-reachable choice for this decoder to begin with.
- **Fold the flag into `Gauge { value: f64, relative: bool }` instead of a new variant.** Considered
  and not taken now — see the Decision section above for the reasoning and the condition under which
  this fallback should be revisited.
- **Reject `+`/`-` unconditionally, forever** (i.e. do nothing). Rejected: both specs define this
  syntax, `docs/known-gaps.md` already named it as a known, closable gap, and a spec-compliant
  DogStatsD client emitting `conns:+1|g` for a connection-count gauge is common enough that silently
  dropping every such line is a real, avoidable data-loss gap, not a theoretical one.

## Consequences

- `GaugeDelta` is a new, compiler-enforced case on every exhaustive `MetricKind` match in the
  codebase; a future metric kind consumer that pattern-matches `MetricKind` without a wildcard will
  be forced to decide what a relative gauge adjustment means to it, the same way this change was
  forced to decide for the four existing ones.
- A misconfigured pipeline (a `statsd_in` reaching an output with no `aggregate` on the path) now
  fails later and more quietly than before this change: at the encoder, per metric, throttled —
  instead of at decode, for the whole line, with a message naming statsd directly. The encoder-side
  message is written to name the fix (`"add an aggregate component between the statsd input and
  this output"`) specifically to offset that loss of immediacy.
- A `logit validate` graph check — "a statsd input reaches an output with no `aggregate` on the
  path" — is implementable (`logit-pipeline::graph` already walks the resolved graph) but has a real
  false-positive case: resolving a `GaugeDelta` downstream, in a separate collector this `logit`
  instance forwards to, is legitimate and not visible to a single config's graph. `logit validate`
  also has no warning channel today, only pass/fail. **Deferred**, not silently skipped — tracked in
  `docs/known-gaps.md`.
