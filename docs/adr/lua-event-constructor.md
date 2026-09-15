---
created: 2026-09-15
updated: 2026-09-15
---

# `Event.new(t)`: Lua constructs events from the table shape `event:to_table()` already emits

## Status
Accepted

## Context

A Lua script has exactly one way to produce a second event: `event:clone()`, an independent copy
of an event it was handed ([`lua-api.md`](../design/lua-api.md), "Exposure: a proxy, not a
converted table"). Everything a script can put *into* an event is a mutation of a copy: attributes,
the timestamp, a log record's trace context, a `sum`/`gauge`'s value. There is no way to build a
log from scratch, no way to build a metric of any kind, and no way to build a span at all —
`event.span` is a read-only proxy precisely because "there is still no script-visible way to
construct or mutate a span" ([`lua-api.md`](../design/lua-api.md)'s "Reading `event.span`",
[`known-gaps.md`](../known-gaps.md)'s "narrowed to span writes/minting from Lua" entry).

The constructor was deferred, twice, under the same "design pass once a consumer needs it"
posture `event.metrics` and `event.span` took before they landed. That posture was right for
*access*; for *construction* it hid the actual scope. An `Event.new{timestamp = ..., attributes =
...}` that could only produce a payload-less event is legal under
[ADR `multi-payload-events`](multi-payload-events.md) but renders as nothing anywhere. The useful
constructor is the one that builds the payloads, and building a payload means specifying, for
every field of `LogRecord`, every `MetricKind` variant, and `SpanRecord` with its events and
links, what a script may write and what a malformed write looks like. That is the design pass
this ADR is.

Two things make it tractable now. First, `event:to_table()` already defines one canonical plain
Lua table for every payload (`crates/logit-script/src/proxy.rs`'s `to_table`, `log_to_table`,
`metric_to_table`, `span_to_table` and their sub-table helpers): decimal-digit strings for
nanosecond timestamps (the 2^53 rule in `lua-api.md`), lowercase hex strings for trace/span ids,
lowercase names for every enum, arrays for `metrics`/`buckets`/`quantiles`/`events`/`links`.
Every question a constructor's input shape would have to answer, `to_table` has already answered
in the other direction. Second, a script has deliberately had no clock (`lua-api.md`'s "Emitting
telemetry from a script"), which was fine while every event a script emitted was derived from
one it received. A script minting an event inside `flush()` — the stateful-processor shape —
has no incoming event to copy a timestamp from, and the runtime already holds the tick time it
hands the native `aggregate` transform (`transform.flush(now)` in
`crates/logit-pipeline/src/runtime.rs`'s `flush_now`).

## Decision

**`Event.new(t)` is the inverse of `event:to_table()`.** A new `Event` global (a table with one
function, `new`) is installed on every worker's VM beside `telemetry`/`trace`/`resource`/`scope`/
`provenance`, before the script's top-level code runs. `Event.new(t)` takes one table in exactly
the shape `to_table()` returns — same keys, same encodings, same nesting — and returns an ordinary
event handle (an `EventProxy` userdata) that can be mutated, `clone()`d, marked with `to()`, and
returned from `process()` or included in a `flush()` table like any other. `Event.new(e:to_table())`
round-trips every lossless shape; the not-round-trippable cases are enumerated below, not left to
be discovered.

Concretely:

- **Strict keys.** An unknown key anywhere in the input — top level or any sub-table — is a
  runtime error naming the dotted path (`Event.new: log.severty is not a field`). The derived keys
  `has_log`/`has_metrics`/`has_span` are accepted (they are in `to_table()`'s output) and must be
  booleans if present, but their values are ignored: the payload keys are the truth. Table access
  is raw (`raw_get`/`raw_pairs`), so a metatable cannot make the key check and the field reads
  disagree.
- **Required minimums, and defaults only where core already documents one.** `timestamp` is
  required, as a decimal-digit string (a Lua number is the same error `event.timestamp = 1` is).
  `attributes` defaults to empty. A `log` needs `message`; `body_format` defaults to `raw`,
  `observed_timestamp`/`dropped_attributes_count` to 0, `severity`/`event_name`/trace context to
  absent. A metric needs `name` and `kind` plus that kind's own fields; `start_timestamp`/`flags`
  default to 0, `exemplars`/`unit`/`description` to empty/absent; a `sum` without `temporality`/
  `monotonic` is `MetricKind::counter` (delta, monotonic); `samples` without `sample_rate` is
  `Samples::new`'s 1.0. A histogram's or exponential histogram's `temporality` has no core default
  and is required. A span needs `trace_id`, `span_id` and `name`; `kind` defaults to `internal`,
  `status` to `unset`, `end_timestamp` to the event's `timestamp`, every `dropped_*` count to 0,
  `events`/`links` to empty. An `end_timestamp` before `timestamp` is an error, the same rule
  `trace_context`'s `span:` block applies to a lifted span.
- **`is_no_recorded_value` is sugar for a flag bit.** `true` ORs `MetricRecord::FLAG_NO_RECORDED_VALUE`
  onto `flags`; `false` leaves `flags` untouched, so a round-trip of a flagged record is exact.
- **Not constructible: `distribution`, `set`, `gauge_delta`.** `to_table()` is deliberately lossy
  for the two sketches (a `count`, an `estimate` — never the DDSketch or HyperLogLog state), so there
  is no shape to invert. `gauge_delta` is `aggregate`'s private intermediate and must never reach a
  sink ([`data-model.md`](../design/data-model.md)). Each names its reason in the error and points a
  script at the raw kind (`samples`/`set_members`) that `aggregate` will summarize.
- **Ids and values go through the parsers that already exist.** `trace_id`/`span_id`/
  `parent_span_id` use `logit_core::trace::parse_trace_id`/`parse_span_id` (hex, exact length,
  not all-zero); a log's or exemplar's `span_id`/`trace_flags` without a `trace_id` is the same
  error the `event.log` proxy raises. `message`, span and span-event `name`s, and every attribute
  value go through `lua_to_value`. **A constructed value is flattened the way any fresh Lua value
  is**: a Lua string becomes `Str` (or `Bytes` only if it is not valid UTF-8), a Lua integer
  becomes `I64`, an empty table becomes an empty `Map`. The no-op-assignment identity rule of
  [ADR `lua-value-identity-preservation`](lua-value-identity-preservation.md) needs an existing
  value to compare against and a constructor has none, so `U64`/`Timestamp`/UTF-8 `Bytes`
  attribute values do not round-trip through `Event.new(e:to_table())`; that ADR's residual list
  gains this case.
- **Enum names live in `logit-core`.** `Severity`, `BodyFormat`, `Temporality`, `SpanKind` and
  `SpanStatus` gain `as_str()`/`from_name()`/`NAMES` (exact lowercase), and `MetricKind` gains
  `name()`. The five duplicated name tables in `crates/logit-script/src/proxy.rs` and
  `crates/logit-outputs/src/stdio.rs`, and the private name parsers in
  `crates/logit-transforms/src/trace_context.rs`, collapse onto them. OTLP's capitalised
  `severity_text` in `crates/logit-proto` keeps its own table: it is a wire convention, not ours.
- **`SpanExt` is boxed only when needed**, the rule `crates/logit-proto`'s `ext_from_wire` already
  applies: a span whose `status_message`/`trace_state`/`dropped_*` are all default has `ext: None`,
  so a minimal constructed span costs what a minimal decoded one does.
- **`flush(now)`.** The Lua `flush` function receives one argument: the runtime's tick time as a
  decimal-nanos string, the same value `flush_now` already passes the native `aggregate`. A script
  declaring `function flush()` ignores it (Lua semantics; no shim). This is *not* a clock: nothing
  is exposed to `process()`, and the no-clock posture in `lua-api.md` stands with this one named
  exception.
- **A constructed event's routing follows the worker's target list at call time**, not at script
  load. `ScriptWorker`'s target table is shared through a cell the constructor reads on each call,
  so `Event.new(...):to("x")` works inside `process()` and `flush()`. An `Event.new` at script top
  level sees the empty list, exactly as a top-level `resource` write sees the pre-first-batch
  state; documented, not prevented.
- **In-place writes to `event.log.message`/`severity`/`body_format` stay read-only.** The reason
  given for that ("a later design pass, once a concrete need shows up") no longer holds once a
  constructor can set them, but they are a separate, small change that shares this ADR's parsers,
  and folding them in would widen a stack that is already six code PRs. Meanwhile
  `Event.new(e:to_table())` with the field edited is the documented way to rewrite one.

## Alternatives considered

- **Keep deferring; `event:clone()` plus mutation is enough.** It covers derived events and
  stateful `flush()` re-emission of stashed clones, which is why nothing has blocked on this. It
  cannot mint a log line, a histogram, or a span, and every "prepare attributes for `trace_context`
  to lift" workaround is a script encoding a record as attributes so a later component can decode
  it back — the opposite of what a scripting escape hatch is for.
- **Typed constructors per payload (`Log.new{...}`, `Metric.gauge{...}`, `Span.new{...}`).**
  Nicer per call, but three or more new globals, three shapes to document, and no relation to
  `to_table()` — a script that reads an event as a table and wants to rebuild it would translate
  between two conventions. One constructor with one shape, already documented as the output of
  `to_table()`, is the smaller surface. Convenience wrappers can be Lua-side sugar later.
- **Lenient keys (ignore unknown ones).** A typo in `severity` would silently produce an unset
  severity; the proxies already reject unknown field names on read and write, and a constructor
  that is looser than the proxy would be the one place a mistake goes quiet.
- **A `Value` shape tag (`{type = "u64", value = "..."}`) so attribute variants round-trip.**
  It would make `to_table()` emit tagged values too, or `Event.new` accept a shape `to_table()`
  never produces — either breaks the symmetry this ADR is built on, for a case (an exact `U64`
  above 2^53 or a UTF-8 `Bytes` preserved through a rebuild) no consumer has. Recorded as a
  residual gap instead.
- **Expose a general clock (`now()`) rather than `flush(now)`.** A clock in `process()` invites
  timestamps that disagree with the event's own, and the only shape that genuinely lacks a
  timestamp source is the flush-driven emission; giving that path the value the runtime already
  computed adds no capability to the rest of the script.
- **Accept the sketches by re-summarizing (`distribution` from a `count` alone).** It would
  construct a sketch that lies about its own contents. The raw kinds exist for exactly this; the
  error says so.
- **Cumulative-`sum` or `unset`-status defaults chosen by the constructor.** Every default in
  this ADR is one core already documents (`MetricKind::counter`, `Samples::new`, `SpanExt`'s
  zeros). Inventing a default for `histogram.temporality` would be the constructor deciding a
  question the data model left open; it is required instead.

## Consequences

- `crates/logit-core`: `as_str`/`from_name`/`NAMES` on five enums, `MetricKind::name`. No field
  changes; `tests/type_sizes.rs` is untouched.
- `crates/logit-script`: a new `construct.rs` holding the table-to-record parsers and the
  `Event` global's `install`; `ScriptWorker.targets` becomes a shared cell; `flush` takes `now`;
  `value.rs` exposes an `AttrMap`-from-table helper extracted from its existing Map arm;
  `exemplar_to_table` gains `trace_flags` so exemplars round-trip. A script that never calls
  `Event.new` pays nothing: every existing `lua:` allocation pin in
  `crates/logit-bench/tests/allocations.rs` is unchanged, and the one new pin is additive.
- `crates/logit-pipeline`: `flush_now` passes its `now` to the worker.
- `crates/logit-outputs`, `crates/logit-transforms`: rendering and parsing of enum names go
  through core; `stdio_out`'s text is byte-identical.
- Docs: `lua-api.md` gains a "Constructing events" section, `flush(now)` in the script contract,
  and `Event` in Sandboxing/Costs; its "no `Event.new(...)`-style constructor" sentence goes.
  `known-gaps.md`'s span entry narrows from "no way to create or mutate a span" to "no in-place
  mutation". [ADR `trace-context-span-lifting`](trace-context-span-lifting.md)'s "Lua gains no new
  capability here" consequence is amended, dated, to point here.
- Residual, recorded rather than fixed: a `Value::Null` log `message` or span `name` reaches
  `to_table()` as an absent key and is rejected as missing on the way back; a `u64` count above
  `i64::MAX` is emitted `as i64` by `to_table()` and cannot be rebuilt exactly; `U64`/`Timestamp`/
  UTF-8 `Bytes` attribute values flatten as described above, an `I64` past ±2^53 comes back `Str`
  (`to_table()` emits it as a decimal string), and an integral `F64` such as `3.0` comes back
  `I64` (LuaJIT canonicalizes it to an integer); a `sum`/`gauge`/`samples`/exemplar value that is
  NaN or an infinity is rejected by the finiteness rule rather than rebuilt (`prometheus_in`'s
  OpenMetrics `NaN`/`+Inf` and `otlp_in`'s unfiltered `AsDouble` both admit such a point). In-place
  `event.log.message`/
  `severity`/`body_format` writes are the named follow-up.
- Landed by [plan `lua-event-constructor`](../plans/lua-event-constructor.md), stream key `mint`.
