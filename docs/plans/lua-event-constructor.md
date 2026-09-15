---
created: 2026-09-15
updated: 2026-09-15
---

# Enabling plan: `Event.new(t)` — constructing events from Lua

## Context

[ADR `lua-event-constructor`](../adr/lua-event-constructor.md) decides the shape: one `Event.new(t)`
global whose input is exactly the table `event:to_table()` returns, strict about unknown keys,
defaulting only where core already documents a default, refusing the two sketch kinds and
`gauge_delta`, and pairing it with `flush(now)` so a flush-driven emission has a timestamp without a
general clock. This plan is the build-out: what lands in which order, in which files, and how each
piece is verified. Read the ADR first — this document doesn't repeat its reasoning, only its
consequences.

Stream key **`mint`**: branches `mint/w0`…`mint/w6`, a strictly linear stack, each PR based on and
targeting its parent's branch, brought up to date with `git merge origin/main` (never rebase).

## Decisions already settled

| Question | Decision |
|---|---|
| Contract | `Event.new(t)` accepts exactly `to_table()`'s shape and returns an `EventProxy`, so `take_event`/`events_from_table` are untouched. Unknown key anywhere → `Event.new: <path> is not a field`. |
| Derived keys | `has_log`/`has_metrics`/`has_span` accepted, must be booleans, values ignored. `is_no_recorded_value = true` ORs the flag bit; `false` is a no-op. |
| Metric kinds | `sum`, `gauge`, `samples`, `set_members` (W3); `histogram`, `exponential_histogram`, `summary` (W4). `distribution`/`set`/`gauge_delta` → not-constructible error naming why. |
| Required / defaults | Per the ADR: `timestamp`, `log.message`, metric `name`+`kind`+kind fields, span `trace_id`+`span_id`+`name`; defaults only where core documents one; `histogram`/`exponential_histogram` `temporality` required; span `end_timestamp < timestamp` errors. |
| Ids / values | `parse_trace_id`/`parse_span_id`, `TraceRef`; `lua_to_value`; flattening is inherent and documented. |
| Enum names | `as_str`/`from_name`/`NAMES` in `logit-core`; proxy/stdio/trace_context collapse (W1). |
| `flush(now)` | Decimal-nanos string of the runtime's `now_unix_nanos()`; lands in W2 with the constructor. |
| Targets | `ScriptWorker.targets` becomes `Rc<RefCell<Rc<TargetTable>>>`; constructor reads it per call; top-level `Event.new` sees the empty list. |
| Out of scope | In-place `event.log.message`/`severity`/`body_format` writes (follow-up sharing the parsers). |

## Design

### `crates/logit-core` (W1)

`impl Severity { pub const NAMES: [&str; 6]; pub fn as_str(self) -> &'static str; pub fn from_name(&str)
-> Option<Self> }`, the same for `BodyFormat` (`lib.rs`), `Temporality` (`metric.rs`), `SpanKind` and
`SpanStatus` (`span.rs`); `impl MetricKind { pub fn name(&self) -> &'static str }`. Exact lowercase
matching. Consumers collapsed onto them: `proxy.rs`'s `severity_name`/`body_format_name`/
`metric_kind_name`/`temporality_name`/`span_kind_name`/`span_status_name` and `parse_temporality`
(keeps its `event.metrics[i]`-prefixed message); `stdio.rs`'s `severity_label`/`span_kind_label`/
`span_status_label`; `trace_context.rs`'s `span_kind`/`span_status`. `otlp/logs.rs` keeps its
capitalised OTLP table.

### `crates/logit-script/src/construct.rs` (new; W2–W5)

```rust
pub(crate) fn install(lua: &Lua, targets: Rc<RefCell<Rc<TargetTable>>>) -> mlua::Result<()>
pub(crate) fn event_from_table(t: Table) -> mlua::Result<Event>                   // W2
fn log_from_table(t: Table, path: &str) -> mlua::Result<LogRecord>                // W2
fn metric_from_table(t: Table, path: &str) -> mlua::Result<MetricRecord>          // W3, W4 arms
fn exemplar_from_table(t: Table, path: &str) -> mlua::Result<Exemplar>            // W3
fn span_from_table(t: Table, path: &str, start: i64) -> mlua::Result<SpanRecord>  // W5
fn span_event_from_table(..) / span_link_from_table(..)                           // W5
```

Shared helpers (W2): `expect_keys(&Table, allowed, path)` over `raw_pairs`; `nanos_string`
(the `event.timestamp` rule and wording); hex ids via `parse_trace_id`/`parse_span_id`;
`trace_ref_from_fields` (the log proxy's trace_id-before-span_id rule); integer readers for
`i64`/`u32`/`u64` (Lua integer or integral float, negatives rejected for unsigned fields);
`finite` (today's `require_finite_number`, made `pub(crate)`); `enum_name` driven by each enum's
`NAMES`/`from_name`; `sequence` via `value::validated_sequence_len`; `attributes` via a new
`value::lua_table_to_attrmap` extracted, behaviour-preserving, from `lua_table_to_value`'s Map arm.

Errors are `Event.new: <path> ...`: `is not a field`, `is required`, `must be <expected>, got <lua
type>`, `must be one of a, b, c (or nil), got "x"`. Paths: `timestamp`, `attributes`, `log.<f>`,
`metrics[i].<f>`, `metrics[i].exemplars[j].<f>`, `span.<f>`, `span.events[i].<f>`, `span.links[i].<f>`.

`install` follows `telemetry::install`: `create_table`, `new = create_function(move |_, t: Table|
{ let targets = cell.borrow().clone(); event_from_table(t).map(|e| EventProxy::with_targets(e,
targets)) })`, `globals().set("Event", table)`. A non-table argument is `Event.new(t) takes a table,
got <type>`.

Field sets, exactly `to_table()`'s: event `timestamp, attributes, log, metrics, span, has_*`; log
`trace_id, span_id, trace_flags, message, severity, body_format, event_name, observed_timestamp,
dropped_attributes_count`; metric common `name, kind, unit, description, start_timestamp, flags,
is_no_recorded_value, exemplars` plus per kind — `sum`: `value, temporality, monotonic`; `gauge`:
`value`; `samples`: `values, sample_rate`; `set_members`: `members`; `histogram`: `buckets` (rows
of exactly `{bound, count}`), `temporality`, `sum/min/max`; `exponential_histogram`: `scale,
zero_count, zero_threshold, positive{offset, counts}, negative{offset, counts}, temporality, count,
sum/min/max`; `summary`: `quantiles` (rows of `{quantile, value}`), `count, sum`; exemplar
`timestamp, value, trace_id, span_id, trace_flags, attributes`; span `trace_id, span_id,
parent_span_id, name, kind, status, status_message, trace_state, end_timestamp, flags,
dropped_attributes_count, dropped_events_count, dropped_links_count, events, links`; span event
`timestamp, name, attributes, dropped_attributes_count`; span link `trace_id, span_id, trace_state,
flags, dropped_attributes_count, attributes`. `SpanExt` is boxed only when a field is non-default.

### Targets cell and `flush(now)` (`lib.rs`, `runtime.rs`; W2)

`ScriptWorker.targets: Rc<RefCell<Rc<TargetTable>>>` (the `resource_state` shape). `new()` creates
the cell and hands a clone to `construct::install` after `provenance::install` and before
`lua.load(source).exec()`; `with_targets` writes through it; `process` borrows and bumps the `Rc`
(no allocation, so the `lua: process 1 event` pin does not move). `with_targets`'s doc comment is
reworded: the constructor reaches the table through the cell at call time.

`ScriptWorker::flush(&self, now: i64)` calls `flush.call(now.to_string())`; `flush_now` in
`runtime.rs` passes the `now_unix_nanos()` it already holds. Test call sites get a literal.

### Docs

W2 rewrites `lua-api.md`'s "no `Event.new(...)`-style constructor" sentence, adds `## Constructing
events` after "Reading `event.span`" (shape table, required/defaults, error formats, `to()`,
`flush(now)` example, "mint inside `process()`/`flush()`, not at top level"), shows `function
flush(now)` in the script contract, names `Event` under Sandboxing, adds a Costs row, and gives the
telemetry section's "no clock" sentence its one exception. W3–W5 extend the section per kind; W5
retitles "Reading `event.span`" to say constructible-via-`Event.new`, read-only in place, and narrows
`known-gaps.md`'s span entry. W6 updates `AGENTS.md`'s current-state and constraint bullets and
`docs/design/memory.md`.

## Workstreams

| # | PR | Files | Depends |
|---|---|---|---|
| W0 | **Docs.** The ADR and this plan; dated amendment to the ADR this reopens. Docs only, no `script/cibuild`. | `docs/adr/lua-event-constructor.md` (+ row atop `docs/adr/README.md`); `docs/plans/lua-event-constructor.md` (+ row atop `docs/plans/README.md`); `docs/adr/trace-context-span-lifting.md` (dated Consequences bullet, `updated:` bumped) | — |
| W1 | **Enum names in core.** `as_str`/`from_name`/`NAMES` on five enums, `MetricKind::name`; proxy/stdio/trace_context collapse. | `crates/logit-core/src/{lib,metric,span}.rs`; `crates/logit-script/src/proxy.rs`; `crates/logit-outputs/src/stdio.rs`; `crates/logit-transforms/src/trace_context.rs` | W0 |
| W2 | **`Event.new` with `timestamp`/`attributes`/`log`; install; targets cell; `flush(now)`; docs.** | `crates/logit-script/src/construct.rs` (new), `lib.rs`, `proxy.rs`, `value.rs`; `crates/logit-pipeline/src/runtime.rs`; `docs/design/lua-api.md`; `crates/logit-bench/tests/allocations.rs` (+1 additive pin); `docs/design/memory.md` | W1 |
| W3 | **`sum`/`gauge`/`samples`/`set_members` + exemplars**; `trace_flags` added to `exemplar_to_table`. | `construct.rs`; `proxy.rs`; `lua-api.md` | W2 |
| W4 | **`histogram`/`exponential_histogram`/`summary`.** | `construct.rs`; `lua-api.md` | W3 |
| W5 | **Span with events and links**; span docs and `known-gaps.md` narrowing. | `construct.rs`; `proxy.rs` (doc comments); `lua-api.md`; `docs/known-gaps.md` | W4 |
| W6 | **Closeout.** `AGENTS.md` current state and constraints; `memory.md`; this plan's Status. | `AGENTS.md`; `docs/design/memory.md`; `docs/plans/lua-event-constructor.md` | W5 |

Landing order: **W0 → W1 → W2 → W3 → W4 → W5 → W6**, strictly linear. `flush(now)` sits in W2
rather than last because W2's flush-path test (`Event.new{timestamp = now, ...}:to("x")` returned
from `flush(now)`) proves the targets cell and the argument at once, and the script-contract section
is edited once.

### Status (2026-09-15)

All seven workstreams are built and reviewed as a linear stack of PRs, each targeting its parent:
W0 #219 (`mint/w0` → `main`), W1 #220, W2 #221, W3 #222, W4 #223, W5 #224, and W6 (this closeout)
on `mint/w6` → `mint/w5`. Nothing is merged. Allocation pins landed at **17** (log),
**16** (gauge) and **14** (span) for a constructed event, every pre-existing `lua:` pin unchanged.

### Per-workstream detail

**W0** — Done when: both docs follow `TEMPLATE.md`'s headings, every relative link resolves, both
README indexes gain a top row dated 2026-09-15, the span-lifting ADR carries a dated amendment.

**W1** — Tests: per enum, `from_name(as_str(v)) == Some(v)` for every variant and
`from_name("Warn") == None`; `MetricKind::name` exhaustive. Existing proxy/stdio/trace_context tests
pass unchanged (the collapse is behaviour-preserving). Done when: `script/cibuild` green;
`type_sizes.rs` untouched.

**W2** — Tests (`construct.rs` and `lib.rs`): `Event.new(event:to_table())` round-trips the
`log_record_with_everything` fixture by whole-event `assert_eq!`; `{timestamp = "1"}` yields an
empty event; an `Event.new` returned alongside the incoming event in `{event, new}` emits two;
`Event.new(...):to("a")` carries the mark in `process()` and in `flush(now)` on a `routing_worker`;
a top-level `Event.new` then `to("a")` errors naming the empty list; `flush(now)` receives exactly
`tostring(now)`; a `function flush()` script still works. Error tests: non-table argument;
numeric and non-digit `timestamp`; unknown top-level and `log.*` keys; missing `log.message`; bad
`severity`/`body_format`; `span_id` without `trace_id`; `trace_flags = 256`; non-string attribute
key; `has_log = "yes"`. Sandbox: `Event ~= nil`, `type(Event.new) == "function"`, the existing
`assert_global_is_nil` tests unchanged. Allocations: every existing `lua:` pin unchanged; one new
additive pin (`lua: Event.new log event from a literal table`) with its `memory.md` row. Done when:
`script/cibuild` and `script/test -p logit-bench` green.

**W3** — Tests: round-trip per kind with the `metric_record(sum_kind()/samples_kind()/
set_members_kind())` fixtures plus a gauge, each carrying the everything-exemplar (now including
`trace_flags`); `{kind = "sum", value = 1}` equals `MetricKind::counter(1.0)`;
`is_no_recorded_value = true` sets the bit. Errors: unknown kind; the `distribution`/`set`/
`gauge_delta` message; `histogram` "not yet constructible"; NaN/inf `value`; non-sequence `values`;
non-string member; exemplar `span_id` without `trace_id`; unknown exemplar key. Done when:
`script/cibuild` green; `lua-api.md`'s exemplar field list shows `trace_flags`.

**W4** — Tests: round-trip `histogram_kind()`/`exp_histogram_kind()`/`summary_kind()`;
`sum/min/max = nil` → `None`. Errors: a bucket row with an extra key; negative `count`; `positive`
missing `counts`; missing `temporality`. The W3 "not yet" arms are gone. Done when: `script/cibuild`
green.

**W5** — Tests: round-trip `span_record_with_everything()` and a minimal `{trace_id, span_id,
name}` span (defaults applied, `ext == None`); `status_message` alone boxes `ext`; a mixed
log+metrics+span event round-trips. Errors: `end_timestamp` before `timestamp`; all-zero
`trace_id`; bad `parent_span_id`; `events[1]` missing `name`; `links[1]` missing `span_id`; unknown
`links[1].*` key. Done when: `script/cibuild` green; `known-gaps.md` narrowed; no remaining "no
script-visible way to construct a span" claim outside history.

**W6** — Done when: `AGENTS.md` mentions `Event.new` in the current-state narrative and the Lua
constraint bullet; the `memory.md` row and the `lua-api.md` Costs row agree with `allocations.rs`;
this plan's Status paragraph lists PR numbers.

## Verification

- `script/cibuild` before every code PR; `script/test -p logit-script` in the loop; `script/format`
  and `script/lint` are enforced, not advisory.
- `script/test -p logit-bench`: every existing `lua:` allocation pin unchanged — a script that never
  calls `Event.new` pays nothing; the new pin is additive.
- `crates/logit-core/tests/type_sizes.rs` must not move (no model field changes).
- Round-trip tests are whole-event `assert_eq!` against the input fixture, not field spot-checks.
- Manual smoke at W6: a `lua` component with `interval:` whose `flush(now)` returns
  `{Event.new{timestamp = now, metrics = {{name = "tick", kind = "gauge", value = 1}}}}` into
  `stdio_out`. **Done 2026-09-15** (`generate_in` → `lua` with `interval: 1s` → `stdio_out`, in
  the dev container): each tick rendered one event carrying the `now` timestamp, the flush-built
  log line, an attribute and the gauge, with the tick timestamp advancing per flush and a final
  flush on shutdown.

## Open risks

- A `Value::Null` log `message` or span `name` reaches `to_table()` as an absent key and is rejected
  as missing on the way back; a `u64` count above `i64::MAX` cannot be rebuilt exactly. Both are in
  the ADR's residual list, not fixed here.
- The `sum` counter default is a convenience beyond the bare `to_table()` contract; if review
  prefers "everything `to_table()` emits is required", drop it (round-trip tests are unaffected).
- The W1 collapse touches `stdio_out` rendering; its text must stay byte-identical.
