# Lua scripting API

Scripts are the reason `logit` exists rather than a config-only tool. This document covers how an
`Event` ([docs/design/data-model.md](data-model.md)) is exposed to Lua, the script contract, and the
concurrency rules that fall out of embedding `mlua`.

## Exposure: a proxy, not a converted table

The consequential choice: converting each `Event` into a plain Lua table on entry to a script stage
and back on exit is the obvious approach and the wrong one at any real throughput. A typical script
reads and writes two or three fields out of a couple dozen; paying a full table-conversion cost
(and a full re-validation cost on the way back) on every event, at every stage, for fields nobody
touched, is pure waste — and it's the kind of design mistake that's very expensive to undo once
scripts exist that depend on the table shape.

Instead, `Event` is exposed as **`mlua` userdata with `__index`/`__newindex` metamethods**
(implemented in `crates/logit-script/src/proxy.rs`) that read through to the underlying Rust event
lazily, and copy-on-write only the fields a script actually assigns to:

```lua
function process(event)
  event.attributes.env = "prod"     -- __newindex on attributes: writes through, nothing else copied
  local host = event.attributes.host -- __index: reads through, no allocation
  return event
end
```

`event.attributes` is itself a second userdata sharing the same underlying event (not a copy), so
chained access like the above works without materializing anything beyond what's read or written.
Since an event can carry a log, several metrics, and a span all at once
([ADR `multi-payload-events`](../adr/multi-payload-events.md)), the proxy exposes presence, not a classification:
`event.has_log` / `event.has_metrics` / `event.has_span` (read-only booleans). There is deliberately
no `event.type` — an earlier design tried a single `"log"`/`"metric"`/`"span"` string with a
precedence rule for the multi-payload case, and rejected it: a summary string is strictly lossy
versus checking the specific thing a script actually cares about, and a script branching on
`event.type == "metric"` would silently skip the metrics on a log-carrying event, exactly the shape
a transform like `kv_metrics` produces. `event:clone()` (an independent deep copy, needed for
fan-out — see the script contract below) rounds out the proxy's surface for now.

**`event.log` is the first typed record access**, read/write on its trace context
(`trace_id`/`span_id`/`trace_flags`) and on `event_name`/`observed_timestamp`, read-only on
`message`/`severity`/`body_format`/`dropped_attributes_count` — see "Reading and writing
`event.log`" below. **`event.metrics`** and **`event.span`** followed the same path once a
concrete consumer needed them (W7 of
[`docs/plans/lossless-transit.md`](../plans/lossless-transit.md)): `event.metrics` is a
read/write, array-like proxy over the event's metric list — read/write only on the handful of
fields a script can legitimately mutate in place without breaking a kind's own invariants (a
`sum`/`gauge`'s `value`, a `sum`'s `temporality`/`monotonic`), read-only everywhere else — see
"Reading and writing `event.metrics`" below. `event.span` is entirely read-only, the same posture
`provenance` takes, since there is still no script-visible way to construct or mutate a span — see
"Reading `event.span`" below. Same for any `Event.new(...)`-style constructor: still not built,
still the same "design pass once a consumer needs it" posture this section originally took for
metrics and span themselves.

No `__pairs`: it isn't available under LuaJIT. `mlua::MetaMethod::Pairs` requires Lua 5.2+, and
LuaJIT is Lua 5.1 semantics — this was in the original version of this section and is wrong.
`event:to_table()` is the answer instead: a real, disconnected Lua table with `timestamp`,
`attributes`, `log` (itself a table -- see "Reading and writing `event.log`" below -- or `nil`),
`metrics` (an array of tables, one per `event.metrics[i]` -- see "Reading and writing
`event.metrics`" below -- always present, empty when the event carries none), `span` (itself a
table -- see "Reading `event.span`" below -- or `nil`), `has_log`, `has_metrics`, and `has_span`,
for anything the proxy doesn't expose directly —
including full attribute iteration
(`for k, v in pairs(event:to_table().attributes) do ... end`, native `pairs()` on a real table) and
building new structures or logging for debugging. The cost is opt-in and visible at the call site
rather than paid unconditionally.

One more thing the proxy design ran into once actually implemented: **`event.timestamp` is a Lua
*string*, not a Lua number.** Lua's only numeric type is an IEEE-754 double, exact only up to 2^53
(~9e15) — and a unix-nanos timestamp is routinely ~1.7e18, nearly 200x past that. This was
verified empirically, not just reasoned about: an early version exposed it as a Lua integer, and a
script that did nothing but read `event.timestamp` and write it back unchanged already came back
wrong (`tostring` showed `"1.7e+18"`). A decimal-digit string round-trips exactly; a script that
needs real arithmetic on it can `tonumber()` at whatever precision it actually needs (millisecond
granularity, for instance, comfortably fits a Lua number).

The same 2^53 limit applies to ordinary `Value::I64`/`Value::U64` attribute values, not just
timestamps — also found by review, against the real implementation: `event.attributes.x =
event.attributes.x` on a value one past 2^53 silently changed it, and `u64::MAX` wrapped negative
through the naive cast that used to sit here. Unlike a timestamp (always large), an ordinary
integer attribute is usually small (`retry_count = 3`), where a real Lua number is genuinely more
useful to a script than a string. So `crates/logit-script/src/value.rs` checks each I64/U64
individually against the exact-integer boundary and only falls back to a string when a value
doesn't fit — small values stay real, arithmetic-capable Lua numbers; `Timestamp` values are large
enough in practice to always take the string branch anyway, and share the same logic rather than a
separately maintained rule.

**Variant identity survives an unmodified round-trip, via a no-op-assignment rule, not a tagged
value.** A plain Lua string or number genuinely can't carry which `Value` variant it came from, so
an identity round-trip through a script (`event.attributes.x = event.attributes.x`, or the very
ordinary "read every attribute via `to_table()` and copy it back while tagging the event with
something else") would otherwise silently change a value's variant even though its content never
changed — a real behavioral consequence, since `logit-outputs::influxdb`'s tag handling treats
`Bytes` and `Str` differently. `AttrsProxy::__newindex` (`crates/logit-script/src/proxy.rs`) closes
this by treating such an assignment as a no-op when it's byte-for-byte (or number-for-number) what
`value_to_lua` would already produce for the attribute's current content (`value.rs`'s
`lua_value_matches`) — the stored `Value`, variant included, is left untouched; anything that
changes content still converts exactly as before. See
[`lua-value-type-preservation.md`](lua-value-type-preservation.md) for the full mapping, why a
tagged userdata wrapper was considered and rejected, and every known residual gap (cross-key
copies, nested container elements, empty-container ambiguity) — each deliberate and
regression-tested, not an oversight.

**A criterion benchmark against plain table conversion is still outstanding** — tracked as a
follow-up now that the proxy above exists to benchmark against a baseline. The design commits to
the proxy on the reasoning above; the benchmark is to confirm the expected win with numbers, not to
leave the choice open.

## Script contract

```lua
-- required
function process(event)
  ...
  return event        -- pass through, possibly mutated
  -- return nil        -> drop the event
  -- return {a, b}      -> fan out into multiple events
end

-- optional, for stateful processors (e.g. the built-in `aggregate`)
function flush()
  ...
  return {event1, event2, ...}  -- events to emit at this flush tick
end
```

`process` runs once per event. `flush` runs on the interval configured for that pipeline stage and
is how the aggregator ([docs/design/data-model.md](data-model.md)'s mergeable metric kinds) turns
accumulated state into emitted events — this is why the flush/timer contract needs to exist in the
pipeline design now rather than being bolted on when aggregation is implemented.

**An event handle — and its `event.attributes` handle — is consumed once the event is returned
from `process()` or included in a `flush()` table** — don't keep using a Lua variable referencing
either after handing the event back that way. A Lua userdata is a reference type, so a variable a
script stashed elsewhere (`pending = event`, or `pending_attrs = event.attributes`) can be the
*exact same* underlying object as the one returned (or reached through it), not an independent
copy; extracting the returned event invalidates every other reference to it — including a stashed
`event.attributes` handle, since one is cached per event and reused for every access rather than
rebuilt each time (`crates/logit-script/src/proxy.rs`) — and using a stashed alias afterward is a
clear error, not silently wrong data. If a script genuinely needs to both emit an event now and
keep something for later (a stateful `flush()` re-emitting it, say), stash `event:clone()` — an
independent copy — instead of `event` (or `event.attributes`) itself.

`return {a, b}` must be a proper array-like table (keys exactly `1..=n`, matching Lua's own
notion of a sequence) — a malformed table (non-contiguous keys) is a clear error, not a silently
incomplete or empty result.

`process`/`flush` are resolved once, when the script loads, not looked up from `_G` again on every
event or flush tick — a deliberate cost/behavior trade (`crates/logit-script/src/lib.rs`): a script
that reassigns `_G.process`/`_G.flush` mid-run has no effect on what actually runs from that point
on. Reassigning either isn't a pattern this contract ever documented or exercised, so this is very
unlikely to change real behavior, but it is a real (if narrow) restriction worth knowing about. A
`flush` global that exists but isn't a function (or `nil`) is rejected at load time, the same as a
missing `process` — not silently treated as "no `flush()`" and left to quietly emit nothing at
every tick.

## Emitting telemetry from a script

A script can emit its own metrics via a `telemetry` global, callable from `process()` or
`flush()`:

```lua
function process(event)
  telemetry.count("orders.total_value", event.attributes.amount, {status = "completed"})
  telemetry.gauge("queue.depth", 42)
  return event
end
```

`telemetry.count(name, n, tags?)` / `telemetry.gauge(name, v, tags?)` -- `tags`, if given, is a
plain table of string keys to string values. No `timing()`: scripts have no clock exposed in the
sandboxed stdlib (`table`/`string`/`math` only, "Sandboxing" below), so there's no way for a
script to produce a duration.

This is the same self-observability mechanism `logit` uses on itself
(`docs/design/internal-telemetry.md`), extended one level further: a component's Rust code can
only instrument what it can see, but a script often knows something about the domain (an order
value, a custom business counter) no amount of Rust-side instrumentation could infer. Points a
script emits go through the same buffer, the same `internal` component, and the same downstream
tools (`aggregate`, any sink) as everything else -- nothing script-specific to configure beyond the
call itself. If no config uses an `internal` component, `telemetry` calls are no-ops, same as
every other telemetry call site in the codebase.

**A metric name or tag value should be a fixed literal in the script's own source, not built from
event data.** `telemetry.count("orders.total", 1)` is fine, called as often as you like.
`telemetry.count(event.attributes.order_id, 1)` compiles and runs, but leaks one process-wide
interner entry per distinct order id, forever -- cardinality safety for script-authored telemetry
is the script author's responsibility, not something the type system checks for you the way it
does for the Rust call sites `internal-telemetry.md` documents. See
[ADR `lua-authored-telemetry-cardinality`](../adr/lua-authored-telemetry-cardinality.md) for the full reasoning.

**Metric names starting with `logit.` are reserved** for `logit`'s own internal metrics
(`docs/design/internal-telemetry.md`) -- `telemetry.count("logit.component.events.received", 1)`
is a clear call-time error, not a silent merge into the runtime's own counter. Pick a name outside
that namespace for anything script-specific.

**A tag keyed `component`, `kind`, or `role` is rejected the same way** -- those identify which
component emitted a point and can't be set as a tag; `telemetry.count("m", 1, {kind = "x"})` is a
clear error, not a silent no-op and not a point quietly misattributed to another component.

## Reading trace context

A `trace` global gives `process()` read access to the incoming batch's trace context, as lowercase
hex strings:

```lua
function process(event)
  event.attributes["trace.id"] = trace.trace_id
  return event
end
```

`trace.trace_id` (32 hex chars, 16 bytes) and `trace.span_id` (16 hex chars, 8 bytes) -- the same
`TraceContext` every node in the graph carries on its inbound batch
(`docs/adr/trace-context-propagation-on-delivered.md`), set once per incoming batch before any
of its events reach `process()`, so every event in one call to `process()` sees the same value.
Both start at the all-zero placeholder (`"00...0"`) before any batch has arrived.

**This is `logit`'s own pipeline identity, not an application's.** `trace` names which node-visit
processed a batch -- it has nothing to do with `event.log.trace_id`
(below), the *application's* trace context a log line was emitted under. A script copying one onto
the other (`event.log.trace_id = trace.trace_id`, stamping a log with "which `logit` run handled
this line") is a deliberate choice a script can make; `logit` never does it on its own -- see
[ADR `log-record-trace-context`](../adr/log-record-trace-context.md).

**Stale during `flush()`.** `trace` is *not* updated around a `flush()` call -- it keeps whatever
the most recently processed batch set (see this crate's own known-gaps entry). A `flush()`-driven
emission has no single incoming batch to attribute itself to -- however many batches contributed to
whatever a stateful script is about to flush, `logit` has no way to know, and doesn't try to guess.
A script that wants better than "whichever batch was last seen" needs to track contributing
contexts itself, inside its own `process()` -- the values are genuinely there to read, `logit` just
doesn't aggregate them on the script's behalf the way
`docs/adr/trace-context-propagation-on-delivered.md`'s flush-side linking does for the native
`aggregate` transform. `resource`, below, has the same default staleness at a flush tick, but --
unlike `trace` -- a script can override it explicitly.

## Reading provenance

A `provenance` global gives `process()` read-only access to which component created the incoming
batch, which component most recently handled it, and this worker's own component id
(`docs/design/pipeline-graph.md`'s "Provenance propagation",
[ADR `batch-provenance-on-delivered`](../adr/batch-provenance-on-delivered.md)):

```lua
function process(event)
  if provenance.origin == "nginx_in" then
    event.attributes["source.origin"] = provenance.origin
  end
  return event
end
```

| Field | Meaning |
|---|---|
| `provenance.origin` | The component that created this batch (normally a listener); `nil` only if unset |
| `provenance.previous` | The component this batch was received from -- the node feeding *this* one |
| `provenance.component` | This worker's own component id |

Unlike `trace` and `resource`, **`provenance` is genuinely read-only, enforced, not by
convention.** `provenance.origin = "x"` raises `provenance.origin is read-only` rather than
silently succeeding and being clobbered on the next batch -- the same "read-only, name it or say
no field" split `event.has_log`/`event.log`'s fields already use, so a write to an unknown field
(`provenance.bogus = 1`) raises `provenance has no field 'bogus'` instead, and a caller can tell
"this isn't for you to write" from "you mistyped this."

`provenance.origin`/`.previous` are set once per incoming batch, before any of its events reach
`process()` -- the same timing `trace` uses. `provenance.component` is fixed for this worker's
whole lifetime, set once when the pipeline starts. **Stale during `flush()`**, the same way `trace`
is: `provenance.origin`/`.previous` keep whatever the most recently processed batch set, since a
flush-driven emission has no single incoming batch to attribute itself to (see "Reading trace
context" above for the full reasoning, and `docs/known-gaps.md`).

`provenance` carries no application meaning on its own -- a script that wants it in the outgoing
data copies it into an attribute explicitly (`event.attributes["source.origin"] =
provenance.origin`, as above); `logit` never stamps it there itself.

## Reading and writing `resource`

A `resource` global gives `process()` (and `flush()`) read *and write* access to the incoming
batch's resource -- the same `Arc<Resource>` `EventBatch::resource` carries
([data-model.md](data-model.md)), proxied the same way `event.attributes` is:

```lua
function process(event)
  resource["service.name"] = "nginx"
  resource["service.namespace"] = "demo"
  return event
end
```

Reads and writes go through `resource["key"]` exactly like `event.attributes["key"]`; enumerate
every key with `resource:to_table()` (no `__pairs` under LuaJIT, same reason `event.attributes`
needs `event:to_table()`). Assigning `nil` stores a null value, not a removal -- there is no way to
delete a resource attribute from Lua, the same rule `event.attributes` follows.

**Per batch, not per event.** A write inside `process()` applies to the whole outgoing batch,
including any events already processed earlier in the same batch -- `resource` is batch-scoped
state, not per-event. A script that wants a per-event identity uses `event.attributes` instead.
Writes made at a script's top level, before any batch has arrived, are silently discarded once the
first batch's `set_resource` call resets `resource` -- write inside `process()`/`flush()`, not at
load time. `resource` starts empty, the same all-clear starting point `trace` has.

**Copy-on-write, so a script that never touches `resource` costs nothing.** Reading or writing
`event.attributes` on an event a script doesn't otherwise touch is already the common case this
proxy design exists to keep cheap; `resource` follows the identical shape
(`crates/logit-script/src/resource.rs`) -- see [memory.md](memory.md) for the measured cost of both
the no-write and write paths.

**A write inside `flush()` gives a stateful script's flush-driven emission a real identity**,
where `trace` (above) has none to offer -- the one asymmetry between the two globals. It doesn't
resolve the underlying *n*-to-1 problem (`logit` still can't attribute a flush automatically to one
of several upstream resources), it just gives the script a way to state the answer itself when it
knows one. See `docs/known-gaps.md`'s entry and
[ADR `operator-declared-resource-attributes`](../adr/operator-declared-resource-attributes.md).

The `set` native transform (`logit_config::ComponentKind::Set`) offers the same capability without
writing Lua at all, for the common case of stamping a handful of constant values -- see that ADR
for why an operator reaches for a graph component here rather than a per-input config field.

**`schema_url` and `dropped_attributes_count` round out `resource`** (W7 of
[`docs/plans/lossless-transit.md`](../plans/lossless-transit.md), mirroring OTLP's own `Resource`
fields). `schema_url` is read/write, a string or `nil` (`nil` clears it, the same "`nil` means
removal for this one field only" exception `event.log.trace_id` below also has);
`dropped_attributes_count` is read-only -- OTLP's own count of attributes a *producer* dropped
before the resource ever reached `logit`, not something a Lua-side write could meaningfully
change, the same "read-only, name it" rule `provenance` already follows
(`resource.dropped_attributes_count = 5` raises `resource.dropped_attributes_count is read-only`).

**A named field takes precedence over an attribute of the same name.** `resource["schema_url"]`
and `resource["dropped_attributes_count"]` always resolve to the named field above, never to an
attribute literally keyed `schema_url`/`dropped_attributes_count` -- such an attribute still shows
up in `resource:to_table()` (a flat attribute snapshot: `resource:to_table()["schema_url"]`), it
just isn't reachable through `resource[...]` indexing. The
same trade `event`'s own fixed fields (`timestamp`, `attributes`, `log`, ...) already make against
an attribute of the same name, documented rather than guarded against.

## Reading and writing `scope`

A `scope` global gives `process()` (and `flush()`) read *and* write access to the incoming batch's
OTLP instrumentation scope (`EventBatch::scope: Option<Arc<Scope>>`, added alongside batch-level
scope grouping in W4), proxied the same way `resource` is above (`crates/logit-script/src/scope.rs`):

```lua
function process(event)
  scope.name = "nginx-otel-module"
  scope.version = "1.0.0"
  scope.attributes["deployment.environment"] = "prod"
  return event
end
```

| Field | Type | Read/write |
|---|---|---|
| `name` | string | read/write -- no `nil` meaning; reads `""` before any batch/write |
| `version` | string | read/write -- same |
| `schema_url` | string or `nil` | read/write -- `nil` clears it |
| `dropped_attributes_count` | integer | read-only |
| `attributes` | sub-object, open map | read/write per key -- same `__index`/`__newindex` shape as `event.attributes` |

**Unlike `resource`, `scope`'s attributes live behind their own `scope.attributes` sub-object, not
directly on `scope[key]`.** `resource["service.name"]` indexes the resource's attribute map
directly (there is no `resource.attributes` sub-object); `scope["k"]` does not fall through to an
attribute the way `resource["k"]` does -- a script writes `scope.attributes["k"]`, mirroring
`event.attributes` rather than `resource`. `scope.attributes = ...` itself is read-only (the
sub-object can't be replaced wholesale, only written into per key).

**Unlike `resource`, a batch may carry no scope at all.** `EventBatch::scope` is `Option`al, so
reading any field before a write reports the all-clear value `resource` doesn't need to (`""` for
`name`/`version`, `nil` for `schema_url`, `0` for `dropped_attributes_count`, an empty table for
`attributes`) -- the same values `logit_core::Scope::default()` itself carries. A write on such a
batch starts `modified` from `Scope::default()` rather than erroring, exactly the way `resource`'s
write path starts from an empty `Resource` when nothing has stamped one yet.

**Per batch, not per event, with the same blast radius `resource` has -- widened.** A `scope`
write inside `process()`/`flush()` applies to the whole outgoing batch, and **regroups OTLP
output the same way a `resource` write does**: `otlp_out` groups outgoing events by their
`(Resource*, Scope*)` pair (W4), so writing `scope` mid-batch changes which wire
`InstrumentationScope` every event in that batch -- not just the one being processed -- ends up
under. `scope` starts at the same kind of all-clear starting point `resource` does -- no batch's
scope seen yet, reading as the defaults above (`base: None`, unlike `resource`'s own `base`, which
is always a real, if empty, `Arc<Resource>`, never absent) -- before any batch has arrived, and is
reset once per incoming batch before any of its events reach `process()`, the same timing
`resource` uses.

**Copy-on-write, and stale during `flush()`, the same way and for the same reason as `resource`.**
A script that never touches `scope` costs nothing (`crates/logit-script/src/scope.rs`); a `flush()`
tick sees whatever the most recently processed batch's scope was, unless the script writes `scope`
inside `flush()` itself, which gives that flush-driven emission a real identity the same way a
`resource` write does (`crates/logit-pipeline/src/runtime.rs`'s `run_lua` commits a `scope` write
the same way it commits a `resource` write, for both the per-batch and the `flush()` path). See
`docs/known-gaps.md`'s Lua-`flush()`-staleness entry, which now covers both globals.

`scope:to_table()` is the enumeration escape hatch, as `resource:to_table()` is (no `__pairs`
under LuaJIT), but its shape differs: `resource:to_table()` is the flat attribute map, while
`scope:to_table()` returns the named fields with the attributes nested --
`{name=, version=, schema_url=, attributes={...}, dropped_attributes_count=}`, `schema_url`
present only when set.

## Reading and writing `event.log`

`event.log` is `nil` on an event with no log (`event.has_log == false`); otherwise it's a proxy
onto the log record, `trace_id`/`span_id`/`trace_flags` read+write, `message`/`severity`/
`body_format` read-only for now:

```lua
function process(event)
  if event.log and event.log.trace_id == nil then
    event.log.trace_id = "4bf92f3577b34da6a3ce929d0e0e4736"
  end
  return event
end
```

**`trace_id`/`span_id` are lowercase hex strings, `nil` when absent** -- 32 characters (16 bytes)
and 16 characters (8 bytes) respectively, the same shape the `trace` global above uses. Assigning
a valid hex string to `trace_id` sets it, replacing the whole trace context: a log with no trace
context gets a fresh one, and a log that already had one gets a fresh one too, `span_id`/
`trace_flags` reset along with it -- an old span belongs to the old trace, not the new one.
Assigning `nil` likewise clears the *whole* trace context, `span_id`/`trace_flags` included, since
OTLP's own contract is that a span only means something alongside a trace. `span_id` and `trace_flags`
can only be set once `trace_id` is -- assigning either first is a clear error, not a silent no-op,
since there would be nothing for them to attach to. `trace_flags` is a plain integer, 0-255 (the
low 8 bits OTLP's `LogRecord.flags` actually carries); bit 0 is the W3C `SAMPLED` flag. An invalid
hex string, or a `trace_flags` outside `0..=255`, is a runtime error naming the field, the same
strictness `event.timestamp`'s parse has.

**`message`/`severity`/`body_format` are read-only for now** -- a later design pass, once a
concrete need for writing them shows up, not an oversight (same reasoning as the record-field gap
noted above). `message` reads as whatever `Value` the log body holds (a string, most commonly);
`severity` reads as a lowercase name (`"trace"`/`"debug"`/`"info"`/`"warn"`/`"error"`/`"fatal"`,
matching `stdio_out`'s own rendering) or `nil` if the record carries none; `body_format` reads as
`"raw"`/`"json"`/`"structured"`. Assigning to any of the three is a clear "read-only for now"
error.

**`event_name`, `observed_timestamp`, and `dropped_attributes_count` round out the record** (W7 of
[`docs/plans/lossless-transit.md`](../plans/lossless-transit.md), following W4's addition of all
three as real `LogRecord` fields). `event_name` is read/write, a plain string or `nil` --
`event.log.event_name = "request.completed"` interns the string the same way a string-valued
attribute write does. **Prefer a name from a fixed, bounded vocabulary in the script's own
source, not one built from event data** -- the same cardinality caution `telemetry.count`'s metric
name argument already carries ("Emitting telemetry from a script" above): a name built from a
request id or order id leaks one process-wide interner entry per distinct value, forever
(`docs/known-gaps.md`'s interner entry). `observed_timestamp` is read/write, a decimal-digit
string, not a Lua number -- the same 2^53 precision reasoning `event.timestamp` documents above,
since this is also a unix-nanos value; `0` (OTLP's own "unset" convention) reads back as the
string `"0"`, not `nil` -- unlike `event.log` itself, there's no "unset means absent" convention
for this particular field. `dropped_attributes_count` is read-only: OTLP's own count of
attributes a *producer* dropped before the record ever reached `logit`, not something a Lua-side
write could meaningfully change -- the same "read-only, name it" rule `provenance` already
follows (`event.log.dropped_attributes_count = 5` raises
`event.log.dropped_attributes_count is read-only`), not an oversight.

The `trace_context` native transform (`logit_config::ComponentKind::TraceContext`) offers the
common case -- lifting a trace id already sitting in an attribute (a JSON log body's own
`trace.id` field, or a W3C `traceparent`) onto the log record -- without writing Lua, the same
relationship `set` has to `resource`/`event.attributes`. See `docs/adr/log-record-trace-context.md`.
Its `span:` block goes one step further than any script can today: it mints a `SpanRecord` from an
access line's ids and timing (`docs/adr/trace-context-span-lifting.md`,
`docs/design/data-model.md`'s "Well-known attribute names"). A script can *read* a span once one
exists -- `event.span`, read-only, see "Reading `event.span`" below -- and can *prepare* the
attributes a `trace_context` placed after it needs (compute `span.start` from whatever the line
carries, say), but still has no way to *create* or *mutate* a span itself. Narrowed in
`docs/known-gaps.md` (W7): the remaining gap is span writes/minting specifically, not span access
as a whole.

## Reading and writing `event.metrics`

An `event.metrics` global gives `process()` (and `flush()`) access to the event's metric list --
an indexable, array-like proxy over `logit_core::MetricList`
(`crates/logit-script/src/proxy.rs`), always present, even for an event with no metrics at all
(`#event.metrics == 0` is a normal, valid read -- unlike `event.log`/`event.span`, there's no
`nil` gate on the container itself, only on what indexing into it returns):

```lua
-- derive an attribute from a metric
function process(event)
  if event.has_metrics and event.metrics[1].kind == "gauge" then
    event.attributes["metric.value"] = event.metrics[1].value
  end
  return event
end

-- adjust a gauge in place
function process(event)
  for i = 1, #event.metrics do
    local m = event.metrics[i]
    if m.kind == "gauge" and m.name == "queue.depth" then
      m.value = m.value + 1
    end
  end
  return event
end
```

`#event.metrics` (`MetaMethod::Len`) and 1-based `event.metrics[i]` (`MetaMethod::Index`) are the
container's whole surface -- no way to add, remove, or reorder metrics from Lua. Indexing out of
range (including `<= 0`) reads `nil`, the same as an ordinary Lua array read past its end; only a
genuinely non-integer key is a hard error. Each `event.metrics[i]` access mints a small, fresh
`MetricProxy` rather than reusing a cached one the way `event.attributes`/`event.log` do -- a
metric list is typically short and usually read once per access, so this trades a per-access
allocation for not holding a registry slot per index for the event's whole lifetime; measured in
`crates/logit-bench/tests/allocations.rs`, see [`memory.md`](memory.md) §2. **Robust to a stale
index**: nothing in today's Lua surface can shrink `event.metrics` mid-script, but every access
checks the index against the current list length anyway, raising `event.metrics[i] no longer
exists` rather than trusting a handle a script held onto past a point that could someday
invalidate it (cheap insurance against a future surface that *can* shrink the list, e.g. an
eventual `event.metrics:remove(i)`).

Every metric kind is named by `kind`: `"sum"`, `"gauge"`, `"gauge_delta"`, `"samples"`,
`"distribution"`, `"set_members"`, `"set"`, `"histogram"`, `"exponential_histogram"`, `"summary"`
(`crates/logit-core/src/metric.rs`'s `MetricKind`, [`docs/plans/lossless-transit.md`](../plans/lossless-transit.md)'s
target model). Every field is readable regardless of kind (`nil` when the current kind doesn't
carry it); **only a handful are writable, and only on the kind that makes them meaningful**:

| Field | Type | Read/write | Present on |
|---|---|---|---|
| `name` | string | read/write | every kind |
| `unit` | string or `nil` | read/write | every kind |
| `description` | string or `nil` | read/write | every kind |
| `start_timestamp` | nanos-string | read/write | every kind |
| `flags` | integer | read-only | every kind |
| `is_no_recorded_value` | boolean | read-only | every kind (derived from `flags`) |
| `kind` | string (list above) | read-only | every kind |
| `exemplars` | table, array of exemplar tables | read-only | every kind (empty when none) |
| `value` | number | read on `sum`/`gauge`/`gauge_delta`; **write only on `sum`/`gauge`** | see note below |
| `temporality` | `"delta"` or `"cumulative"` | read on `sum`/`histogram`/`exponential_histogram`; **write only on `sum`** | see note below |
| `monotonic` | boolean | read/write on `sum` only | `sum` |
| `values` | table, array of numbers | read-only | `samples` |
| `sample_rate` | number | read-only | `samples` |
| `members` | table, array of strings | read-only | `set_members` |
| `estimate` | integer | read-only | `set` |
| `buckets` | table, array of `{bound=, count=}` | read-only | `histogram` |
| `sum` | number or `nil` | read-only | `histogram`/`exponential_histogram` (optional), `summary` (always a number) |
| `min` | number or `nil` | read-only | `histogram`/`exponential_histogram` |
| `max` | number or `nil` | read-only | `histogram`/`exponential_histogram` |
| `count` | integer | read-only | `distribution` (the sketch's own observation count, `DdSketch::count()`), `exponential_histogram`, `summary` |
| `scale` | integer | read-only | `exponential_histogram` |
| `zero_count` | integer | read-only | `exponential_histogram` |
| `zero_threshold` | number | read-only | `exponential_histogram` |
| `positive` | table, `{offset=, counts=[...]}` | read-only | `exponential_histogram` |
| `negative` | table, `{offset=, counts=[...]}` | read-only | `exponential_histogram` |
| `quantiles` | table, array of `{quantile=, value=}` | read-only | `summary` |
| `:quantile(q)` | method, returns a number or `nil` | -- | meaningful only on `distribution`; `nil` on every other kind |

`count` is a plain read-only integer field, not a method -- unlike `:quantile(q)`, there's no
argument that changes its meaning, so there's no reason to make a script write `m:count()` instead
of `m.count`. A write to any field a metric's current kind doesn't allow -- either an
unconditionally read-only field (`flags`, `kind`, `exemplars`, `values`, `sum`, `count`, ...) or a
kind-specific one (`value`, `temporality`, `monotonic`) on a kind that doesn't support writing it
-- raises `event.metrics[i].<field> is read-only on a <kind> metric`, naming the kind so a script
knows *why*, since the same field name is legitimately writable on a different kind. **A write to
`value` on `gauge_delta` is rejected the same way**, even though `value` is readable there --
`gauge_delta` is explicitly *unresolved* state (`docs/known-gaps.md`'s relative-gauge-adjustments
entry: it must never reach a sink un-resolved), so there's no meaningful in-place adjustment for a
script to make to it.

**Only `sum`/`gauge` are mutable in place; every other kind stays entirely read-only** -- a script
can adjust a counter or a gauge, or rename/retag/re-time any metric regardless of kind, but can't
mint a sketch or a cardinality estimate by hand. This mirrors `AGENTS.md`'s "metric kinds must
stay mergeable" rule for the Rust side of this model: `distribution` (`DdSketch`) and `set`
(`HyperLogLog`) carry real merge invariants a naive field write could violate, and `samples`/
`set_members` are raw pre-aggregation collections `aggregate` still needs to fold correctly --
none of these have a script-safe partial-write surface today, so none get one.

`exemplars` is a read-only snapshot table, one entry per `logit_core::Exemplar`: `{timestamp=
<nanos-string>, value=<number>, trace_id=<hex-or-nil>, span_id=<hex-or-nil>, attributes=<table>}`
-- there is no way to add, remove, or mutate an individual exemplar from Lua, only to read the
whole list as it currently stands.

## Reading `event.span`

`event.span` is `nil` on an event with no span (`event.has_span == false`); otherwise it's a
proxy onto the span record, entirely read-only -- there is no script-visible way to construct or
mutate a span, only to read one `trace_context`'s `span:` block already minted
([ADR `trace-context-span-lifting`](../adr/trace-context-span-lifting.md)) or a wire codec already
decoded (`crates/logit-script/src/proxy.rs`):

```lua
function process(event)
  if event.has_span and event.span.status == "error" then
    event.attributes["span.status_message"] = event.span.status_message
  end
  return event
end
```

| Field | Type | Notes |
|---|---|---|
| `trace_id` | string, 32 hex chars | |
| `span_id` | string, 16 hex chars | |
| `parent_span_id` | string (16 hex) or `nil` | |
| `name` | whatever `Value` the span name holds (a string, most commonly) | |
| `kind` | string: `"internal"`/`"server"`/`"client"`/`"producer"`/`"consumer"` | |
| `status` | string: `"unset"`/`"ok"`/`"error"` | |
| `status_message` | string or `nil` | `nil` when the span carries no `ext` (the common case -- see below) |
| `trace_state` | string or `nil` | `nil` when the span carries no `ext` |
| `end_timestamp` | nanos-string | same string-not-number rule as `event.timestamp` above |
| `flags` | integer | |
| `dropped_attributes_count` | integer | `0` when the span carries no `ext` |
| `dropped_events_count` | integer | `0` when the span carries no `ext` |
| `dropped_links_count` | integer | `0` when the span carries no `ext` |
| `events` | table, array of span-event tables | see below |
| `links` | table, array of span-link tables | see below |

`SpanRecord.ext` (`crates/logit-core/src/span.rs`) is boxed and `None` on the common case -- a
span with no error status message and no W3C tracestate -- so `status_message`/`trace_state` read
`nil` and every `dropped_*_count` reads `0` rather than the proxy erroring or fabricating a `Some`.
`events[i]` is `{timestamp=<nanos-string>, name=<value>, attributes=<table>,
dropped_attributes_count=<integer>}`; `links[i]` is `{trace_id=<hex>, span_id=<hex>,
trace_state=<string-or-nil>, flags=<integer>, dropped_attributes_count=<integer>,
attributes=<table>}`.

**Note what's *not* here: `event.span` has no `attributes` field of its own.** A `SpanRecord` has
no attribute map separate from the event's -- a span-carrying event's attributes are
`event.attributes`, the same single attribute set every event has, so there's nothing for
`event.span.attributes` to be.

Unlike every other proxy in this module, `event.span`'s write path doesn't distinguish an unknown
field from a known-but-read-only one -- any assignment at all, to any key, raises the flat
`event.span is read-only`, since there's no field-specific case worth naming when nothing on a
span is writable.

## Config shape

A Lua transform is one component in the pipeline's component graph
(`docs/design/pipeline-graph.md`, `docs/adr/component-graph-configuration.md`) — it names its
own `sources` and is available as a source to anything downstream, rather than sitting in a fixed
per-pipeline `transforms:` chain. Lua can be inline in YAML (block scalar) or referenced from a
file:

```yaml
components:
  metrics_in:
    type: statsd_in
    bind: 0.0.0.0:8125

  windowed:
    type: aggregate
    sources: [metrics_in]
    interval: 10s

  enrich:
    type: lua
    sources: [windowed]
    script: |
      function process(event)
        event.attributes.env = event.attributes.env or "unknown"
        return event
      end

  enrich_more:
    type: lua_file
    sources: [enrich]
    lua_file: ./scripts/enrich.lua
    interval: 30s

  influx_out:
    type: influxdb_out
    sources: [enrich_more]
    url: http://influxdb:8086
    org: logit
    bucket: metrics
    token: !env INFLUXDB_TOKEN
```

A `lua`/`lua_file` component's `interval` is optional and drives that component's own `flush()` the
same way `aggregate`'s does (see `docs/adr/aggregation-window-semantics.md`) -- omitted, the
common case, the component never ticks, same as a script with no `flush()` at all. A zero interval
is rejected at config-validation time, on either kind of component.

Built-in native processors (no Lua involved) handle the common structured-parsing cases without
per-event VM overhead: `json`, `logfmt`, `kv`, `regex`/`grok`, `csv`, `rename`/`remove`/`copy`,
`filter`, `sample`, `throttle`, `dedup`, `aggregate`. Each is a transform-kind component like `lua`
above; nothing about being native rather than scripted changes how a component wires into the
graph. These are meant to sit in front of user Lua — "parse the JSON body, then run my logic" —
rather than being an either/or with scripting: a native transform names a Lua component as its
source, or vice versa, same as any other edge.

## Concurrency

**`mlua::Lua` is neither `Send` nor `Sync`.** This is a hard constraint from the embedded VM, not a
design preference, and it shapes the pipeline's threading model directly:

- One Lua VM per pipeline worker. Workers do not share VM state.
- A script has **no implicit shared mutable state** across workers — two invocations of the same
  script on different workers see independent Lua globals.
- Anything that genuinely needs to be shared (a lookup/enrichment table loaded once and read by
  every worker, an aggregation window that must see all events regardless of which worker handled
  them) goes through an **explicit host-provided store** — a Rust-side structure the proxy exposes
  read/write access into, with its own concurrency semantics (e.g. `dashmap`, or a sharded design
  keyed so that related events land on the same worker in the first place). This needs a defined
  API before the aggregator is implemented, since `aggregate` is the first consumer of it.

Getting this constraint wrong — reaching for a naively shared `Lua` instance — is the single easiest
way to end up with a design that cannot be parallelized without a rewrite.

## Sandboxing

Each script's VM is built with an explicit `StdLib` allowlist — `TABLE | STRING | MATH`
(`crates/logit-script/src/lib.rs`) — rather than trusting `mlua::Lua::new()`'s "safe" default's
exact composition. That matters concretely for LuaJIT: its `ffi` library is a genuine sandbox
escape (raw memory access, arbitrary C calls) if left enabled, and mlua's docs don't commit to
`Lua::new()` excluding it. No `PACKAGE`, so no `require`, either — scripts transform data, they
don't get ambient access to the host or to files. (Core language functions like `pairs`/`type`/
`tostring` are always available and aren't gated behind a `StdLib` flag at all — there's no `BASE`
flag to include.)

**`StdLib` selection alone isn't the whole sandbox — Lua 5.1's base library isn't gated by any
`StdLib` flag at all**, found by review against the real implementation: `loadfile ~= nil` and
`dofile ~= nil` both held true in a worker built with only `TABLE | STRING | MATH` selected,
meaning a script could read and execute arbitrary files readable by this process despite the
documented sandbox. `remove_unsandboxed_base_globals` (`crates/logit-script/src/lib.rs`) nils out
six base globals after VM creation: `loadfile`/`dofile` (the reproduced file-access issue),
`load`/`loadstring` (dynamic execution of arbitrary constructed strings — not file I/O, but
undermines "only the configured script source ever runs"), and `getfenv`/`setfenv` (Lua
5.1-specific, well documented in the wider Lua community as sandbox-escape-adjacent tools for
tampering with a function's environment).

Verified with real scripts, not just configured and assumed: `os`, `io`, `ffi`, `require`,
`loadfile`, `dofile`, `load`, `loadstring`, `getfenv`, and `setfenv` are all confirmed absent
(`crates/logit-script/src/lib.rs`'s tests) — ten checks, each its own test, not one combined
assertion, so a regression in any single one fails on its own.

## Costs

| Surface | Where to look |
|---|---|
| `event.attributes`, `event:to_table()` (proxy vs. table conversion) | [`memory.md`](memory.md) §2, §8 |
| `resource`, `scope` (copy-on-write, read vs. write path) | [`memory.md`](memory.md) §2 |
| `event.metrics`, `event.span` (per-access `MetricProxy`, `to_table()` growth) | [`memory.md`](memory.md) §2 |

Every number for the surfaces above is measured in `crates/logit-bench/tests/allocations.rs`, not
estimated here -- this table intentionally carries none, so it can't drift out of date the moment
a benchmark changes. See [`memory.md`](memory.md) §2 for the current figures.
