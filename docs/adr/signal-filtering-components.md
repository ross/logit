---
created: 2026-09-03
updated: 2026-10-21
---

# Signal filtering is two transform components, not a sink field

## Status
Accepted

## Context
Checking `otlp_out` against Loki found that it has no way to
say "logs only" (or metrics-only, or traces-only) at the sink — it sends whatever signals a batch's
events happen to carry. This is more than a demo inconvenience: `demo/logit.yaml`'s `trace_out`
(Tempo, a traces-only OTLP receiver) is fed from `internal`, whose drains also carry this process's
own `logit.*` metrics. `OtlpOutput::send` (`crates/logit-outputs/src/otlp.rs`) issues one request
per non-empty signal and aborts the rest on the first failure; Tempo's traces request succeeds and
its metrics request fails with `grpc-status: 12` (`UNIMPLEMENTED`), and `write_loop`'s
sustained-permanent-failure guard eventually ends the whole process — recorded in full in
`docs/known-gaps/otlp.md`'s "`otlp_out` aborts an entire batch's `send`..." entry, which names "a
config-layer way to filter an event stream by which payload it carries" as the real fix.
[Closed 2026-10-04 by ADR `delivery-semantics`'s "Amendment: per-request verdicts (2026-10-04)":
the gap entry is deleted, and the guard no longer trips on a signal-partial backend.]

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

**No config validation on `signals:`.** An empty list and a list naming all three signals are
legal on every kind, and each does what it says (the 2026-10-21 amendments below). `has_signal`
naming all three signals is a real, if permissive, "forward anything with a payload" filter under
`mode: only`.

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
- `docs/known-gaps/otlp.md`'s `otlp_out` entry loses its "no signal filter" clause; the other listed
  gaps (custom headers, compression, gRPC TLS, hardcoded paths, `observed_time_unix_nano`) are
  unaffected and remain filed.

## Amendment: discards are counted by the runtime (2026-10-13)

[ADR `telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md) supersedes the Consequences bullet on `absorbed`. A filter's removed event is now
`logit.component.events.discarded` (untagged), which `process_batch` counts for every transform
that returns `false`, so the count is precise for filters as well as `aggregate`.
`logit.transform.events.filtered` is removed: it duplicated `events.discarded`. `has_signal` records
nothing of its own. `keep_signals` and `drop_signals` keep
`logit.transform.payloads.stripped{signal}`, because the event survives a strip.

## Amendment: `route_signals` (2026-10-16)

[ADR `route-signals`](route-signals.md) adds `route_signals`, which sends each signal an event
carries to its own target and, under `strip: true`, gives each target only its own signals'
payloads. A config that fanned one source into a `keep_signals` per signal-specific sink can use
one `route_signals` instead. It counts what it strips and discards with the counters the filters
use: `logit.transform.payloads.stripped{signal}` and `logit.component.events.discarded`. The three
filters are unchanged and remain the answer for one signal-specific sink, or a filter on one
branch.

## Amendment: the `none` selector (2026-10-21)

"All three drop an event that ends up carrying nothing" rested on "such an event carries nothing
any sink could act on." The InfluxDB relay made that false for an event that *arrives* empty: a
point whose fields are all strings is one, and `influxdb_out` writes it
([ADR `influxdb-line-protocol-relay`](influxdb-line-protocol-relay.md)). `logit_config::Signal`
gains a fourth value, `none`, which selects an event that arrived with no log, no metrics, and no
span. It's opt-in: a list without it behaves as before. The same type is `route_signals`'
vocabulary ([ADR `route-signals`](route-signals.md)'s 2026-10-21 amendment), so each kind that
reads it decides what `none` means:

- **`has_signal`** forwards a payload-less event when `none` is listed, under either mode.
  `mode: only` stays "carries nothing outside the list": `[metrics, none]` forwards an event
  carrying only metrics, or nothing. `[none]` forwards only payload-less events.
- **`keep_signals`** forwards an event that arrived with no payload when `none` is listed, and
  drops it otherwise, as before. An event its own strip emptied is still dropped: it carried a
  payload the list chose to clear.
- **`drop_signals`** accepts `none` and it changes nothing: `drop_signals` already drops an event
  that carries nothing. (The 2026-10-21 amendment below lifted the validation error.)

No component reads a protocol's `_logit.*` carriers to decide this.

## Amendment: no validation on `signals:`; rule 21 is retired (2026-10-21)

The original rule 21 rejected an empty `signals:` on all three kinds and
`keep_signals`/`drop_signals` naming all three signals, and the `none` selector's amendment added
`none` on `drop_signals`. All of it is gone. Each rejected list is well defined, so the component
does what the list says, even where the result is odd; `logit` can't protect an operator from
every odd config.

- **`has_signal: []`** forwards nothing: no listed signal can match.
- **`keep_signals: []`** strips every payload and drops every event.
- **`drop_signals: []`** clears nothing and forwards every event that carries a payload. An event
  that arrived with none is still dropped.
- **`keep_signals`** naming `logs`, `metrics`, and `traces` keeps every payload and drops an event
  that arrived with none, which no other list expresses short of `has_signal`.
- **`drop_signals`** naming all three drops every event.
- **`drop_signals`** naming `none` changes nothing: its complement never selects an event that
  arrived empty.

The number 21 is not reused, so the other rules keep theirs.
