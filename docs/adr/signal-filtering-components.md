---
created: 2026-09-03
updated: 2026-09-03
---

# Signal filtering is two transform components, not a sink field

## Status
Accepted

## Context
`docs/plans/otlp-logs-and-resource-identity.md`'s workstream E found that `otlp_out` has no way to
say "logs only" (or metrics-only, or traces-only) at the sink — it sends whatever signals a batch's
events happen to carry. This is more than a demo inconvenience: `demo/logit.yaml`'s `trace_out`
(Tempo, a traces-only OTLP receiver) is fed from `internal`, whose drains also carry this process's
own `logit.*` metrics. `OtlpOutput::send` (`crates/logit-outputs/src/otlp.rs`) issues one request
per non-empty signal and aborts the rest on the first failure; Tempo's traces request succeeds and
its metrics request fails with `grpc-status: 12` (`UNIMPLEMENTED`), and `write_loop`'s
sustained-permanent-failure guard eventually ends the whole process — recorded in full in
`docs/known-gaps.md`'s "`otlp_out` aborts an entire batch's `send`..." entry, which names "a
config-layer way to filter an event stream by which payload it carries" as the real fix.

The obvious fix — a `signals: [traces]` field on `otlp_out` — was rejected. Filtering by signal
type is pipeline composition, not sink configuration: the same need would recur on every future
`_out` component, each reinventing its own copy of the same filtering logic, when ADR
`component-graph-configuration` already settled that a chain of filter components ahead of a sink
is the graph's one branching mechanism ("Adding a second branching mechanism on top would be two
ways to do the same thing").

Under ADR `multi-payload-events`, a single `Event` carries a `log`, `metrics`, and a `span` at once
— a single nginx access-log event that `json` and `kv_metrics` have already run over is a real
example, carrying both a log and derived metrics. That means "filter by signal" is not one
operation. It's two, and conflating them would be wrong for at least one of the two real cases this
ADR was written to unblock:

- **Drop events that don't carry a wanted signal**, without touching whatever a matched event
  carries. `internal` (`crates/logit-core/src/telemetry.rs`'s `drain`) emits *single-payload*
  events — metric-only points and span-only span events — never a mixed one, so `trace_out`'s fix
  is exactly this: forward the span-only drains, drop the metric-only ones, untouched either way.
- **Strip disallowed payloads off an event that carries several**, keeping the rest. The nginx
  access-log case: a logs-only sink downstream of `json` + `kv_metrics` needs the derived metrics
  removed, but must still receive the log.

A single component can't cleanly be both — "forward if it matches" and "strip what doesn't
match" disagree on what to do with a *matched* event that also carries something un-listed, and
silently picking one behavior would surprise whichever use case needed the other.

## Decision
Three new native transforms in `crates/logit-transforms/src/signals.rs`, each implementing
`logit_pipeline::Transform` the same way `keep`/`remove` do — no Lua VM, no `Diagnostics` (nothing
about matching or clearing a fixed signal set can fail):

- **`has_signal`** — drops an event that doesn't carry a wanted signal. Never mutates a forwarded
  event: under `mode: any_of` (the default), an event carrying at least one listed signal is
  forwarded exactly as it arrived, including any signal not listed. `mode: only` additionally
  requires the event carry nothing outside the listed set, dropping a mixed event instead of
  trimming it.
- **`keep_signals`** — an allowlist. Clears every payload slot not listed, keeping the rest.
- **`drop_signals`** — a denylist, the mirror of `keep_signals`.

`keep_signals`/`drop_signals` mirror `keep`/`remove`'s existing allowlist/denylist split for
attributes, deliberately: same relationship, same reason (an allowlist can't be silently defeated
by a signal nobody thought to name).

**Config vocabulary is OTLP's** — `logs`, `metrics`, `traces` (`logit_config::Signal`, mirroring
`logit_proto::Signal`) — not `Event`'s field names. `traces` names `event.span`. This is the
vocabulary an operator configuring a signal filter is actually thinking in (a traces-only backend
like Tempo, a logs-only backend like Loki), and it's the tag value `logit.output.requests{signal=…}`
already uses elsewhere in the codebase.

**All three drop an event that ends up carrying nothing.** For `has_signal`, that's just "matched
no listed signal." For `keep_signals`/`drop_signals`, stripping can leave an event with no payload
at all — such an event carries nothing any sink could act on, so forwarding it would be pure waste,
not a meaningful "empty event" the way `Event::empty` is elsewhere. `Transform::process`'s `None`
already means "don't forward" (`crates/logit-pipeline/src/transform.rs`), and an all-dropped batch
sends nothing downstream (`crates/logit-pipeline/src/runtime.rs`'s `process_batch`) — no runtime
change was needed for this; these are simply the first native transforms to return `None` for a
reason other than `aggregate`'s accumulation.

**Config validation (rule 19, `logit-pipeline::graph::resolve`).** An empty `signals:` list is
rejected on all three — the same silent-black-hole failure rule 7 already guards against elsewhere.
`keep_signals`/`drop_signals` additionally reject naming all three signals, since either shape can
only ever drop every event. `has_signal` naming all three signals is deliberately *not* rejected:
under `mode: only` that's a real, if permissive, "forward anything with a payload" filter, not a
no-op — unlike the other two, `has_signal` never mutates, so there's no equivalent of "strips
everything" to catch.

## Alternatives considered
- **A `signals:` field on `otlp_out` (and every future `_out`).** Rejected as the motivating
  problem above describes: it's sink-scoped state for a pipeline-scoped concern, would be
  reinvented per sink, and duplicates the graph's existing filter-component branching mechanism.
- **One component instead of two**, either always stripping or always dropping. Rejected: the two
  real cases this ADR unblocks (`internal` → `trace_out`, and `json`/`kv_metrics` → a logs-only
  sink) need different behavior on a matched-but-mixed event, and there's no single default that's
  correct for both without a mode flag that would just reinvent the has/keep split under a
  different name.
- **A single `mode: forward | strip` flag on one component**, instead of three separate kinds.
  Considered close to the chosen design. Rejected in favor of separate kinds because
  `keep_signals`/`drop_signals` needed their own allowlist/denylist split anyway (mirroring `keep`/
  `remove`), which a `mode` flag alone wouldn't give without a second flag — three small, clearly
  named components read better in a pipeline than one component with two orthogonal flags.
- **Deriving "matches" from `event.has_log`/`has_metrics`/`has_span` in Lua instead of a native
  component.** Works today (`crates/logit-script/src/proxy.rs`'s `EventProxy`), but requires
  hand-written Lua for a filter shape common enough to warrant a first-class, schema-validated
  config surface — the same reasoning that motivated `keep`/`remove` existing as native transforms
  rather than leaving attribute filtering to Lua.

## Consequences
- `demo/logit.yaml`'s `trace_windowed` — an `aggregate` node whose only job was absorbing metrics
  before `trace_out` (Tempo) saw them — is replaced by `has_signal { signals: [traces] }`. The
  workaround becomes the intended mechanism.
- Any future OTLP-native or signal-partial backend (a logs-only or metrics-only receiver) gets a
  documented, schema-validated way to be fed correctly, without ever touching that sink's own
  config.
- `has_signal`/`keep_signals`/`drop_signals` are the first native transforms to drop an event for a
  reason other than `aggregate`'s accumulation — `process_batch`'s `logit.component.events.dropped
  {reason="absorbed"}` counter is now imprecise for these too (already true of nothing else before
  this), tracked but not fixed here; each component records its own more specific counters
  instead (`docs/design/internal-telemetry.md`).
- `docs/known-gaps.md`'s `otlp_out` entry loses its "no signal filter" clause; the other listed
  gaps (custom headers, compression, gRPC TLS, hardcoded paths, `observed_time_unix_nano`) are
  unaffected and remain filed.
