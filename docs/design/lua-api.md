# Lua scripting API

Scripts are why `logit` is more than a config-only tool. This document is both the reference for
script authors and the design record for how an `Event` ([docs/design/data-model.md](data-model.md))
reaches Lua, what a script must define, and the concurrency rules that come from embedding `mlua`.

What a script can reach:

| Surface | Access | Section |
|---|---|---|
| `event.timestamp` | read/write, a decimal-nanos string | "Timestamps are strings" |
| `event.attributes` | read/write per key | same |
| `event.has_log`, `event.has_metrics`, `event.has_span` | read-only booleans | same |
| `event:to_table()`, `event:clone()` | methods | "No `__pairs`: use `event:to_table()`", "Exposure: a proxy, not a converted table" |
| `event:to(id)` | method, marks an event for a target | "Routing to a target" |
| `event.log` | some fields read/write | "Reading and writing `event.log`" |
| `event.metrics` | some fields read/write | "Reading and writing `event.metrics`" |
| `event.span` | read-only | "Reading `event.span`" |
| `Event.new(t)` | constructor | "Constructing events" |
| `telemetry` | `count`/`gauge` | "Emitting telemetry from a script" |
| `trace` | read (writes aren't enforced, but are overwritten each batch) | "Reading trace context" |
| `provenance` | read-only | "Reading provenance" |
| `resource` | read/write, per batch | "Reading and writing `resource`" |
| `scope` | read/write, per batch | "Reading and writing `scope`" |

## Exposure: a proxy, not a converted table

`Event` reaches Lua as **`mlua` userdata with `__index`/`__newindex` metamethods**
(`crates/logit-script/src/proxy.rs`), not as a plain Lua table. The proxy reads through to the
underlying Rust event lazily and copies only the fields a script assigns to:

```lua
function process(event)
  event.attributes.env = "prod"     -- __newindex on attributes: writes through, nothing else copied
  local host = event.attributes.host -- __index: reads through, no allocation
  return event
end
```

The obvious alternative, converting each event to a table on entry and back on exit, is wrong at
any real throughput. A typical script reads and writes two or three fields out of a couple dozen.
Converting and re-validating every field of every event at every stage is pure waste, and once
scripts depend on the table shape, the choice is expensive to undo.

`event.attributes` is a second userdata over the same underlying event, not a copy, so chained
access like the example above materializes only what it reads or writes.

**A table assigned to an attribute converts raw and at most 128 levels deep.** Conversion reads
the table without its metatable, so no `__index`, `__len`, or other metamethod runs while it does.
A table nested past 128 levels, a self-referencing one such as `t.self = t` included, is an error
naming the attribute:
`event.attributes.loop: can't use a table nested more than 128 levels deep as an event attribute
value (does a table contain itself?)`. The cap is the native wire format's own nesting limit, so
any value a script builds also decodes on a `logit_in` peer. `resource`, `scope.attributes`, and
`Event.new` share the conversion and the cap.

**Presence, not a type.** An event can carry a log, several metrics, and a span at once
([ADR `multi-payload-events`](../adr/multi-payload-events.md)), so the proxy exposes
`event.has_log` / `event.has_metrics` / `event.has_span` (read-only booleans). There is
deliberately no `event.type`. An earlier design used a single `"log"`/`"metric"`/`"span"` string
with a precedence rule, and it was rejected: a summary string is strictly lossier than checking
the thing a script cares about, and a script branching on `event.type == "metric"` would silently
skip the metrics on a log-carrying event, which is exactly the shape a transform like `kv_metrics`
produces. `event:clone()` returns an independent deep copy, which fan-out needs (see "Script
contract" below).

**Typed record access.** Each payload has its own proxy:

- `event.log` is read/write on its trace context (`trace_id`/`span_id`/`trace_flags`) and on
  `event_name`/`observed_timestamp`, and read-only on
  `message`/`severity`/`body_format`/`dropped_attributes_count`. See "Reading and writing
  `event.log`" below.
- `event.metrics` is an array-like proxy over the event's metric list. It is writable only on the
  fields a script can change without breaking a kind's invariants (a `sum`/`gauge`'s `value`, a
  `sum`'s `temporality`/`monotonic`) and read-only everywhere else. See "Reading and writing
  `event.metrics`" below. Both it and `event.span` were added once a concrete consumer needed them
  ([`docs/plans/lossless-transit.md`](../plans/lossless-transit.md)).
- `event.span` is entirely read-only in place, the same posture `provenance` takes. A script that
  wants a different span builds a whole one with `Event.new`. See "Reading `event.span`" below.

**`Event.new(t)` builds an event from scratch** from a table in exactly the shape
`event:to_table()` returns, so the two are inverses. It constructs
`timestamp`/`attributes`/`log`, every constructible metric kind, and a `span` with its `events`
and `links`. See "Constructing events" below.

### No `__pairs`: use `event:to_table()`

LuaJIT has Lua 5.1 semantics, and `mlua::MetaMethod::Pairs` requires Lua 5.2+, so the proxy
can't be iterated with `pairs()`. `event:to_table()` is the escape hatch: a real, disconnected Lua
table for anything the proxy doesn't expose directly, including full attribute iteration
(`for k, v in pairs(event:to_table().attributes) do ... end`, native `pairs()` on a real table),
building new structures, and debug logging. Its keys:

- `timestamp` and `attributes`.
- `log`: a table (see "Reading and writing `event.log`" below), or `nil`.
- `metrics`: an array of tables, one per `event.metrics[i]` (see "Reading and writing
  `event.metrics`" below). Always present; empty when the event carries none.
- `span`: a table (see "Reading `event.span`" below), or `nil`.
- `has_log`, `has_metrics`, and `has_span`.

The conversion cost is opt-in and visible at the call site instead of paid on every event.

### Timestamps are strings

**`event.timestamp` is a Lua *string*, not a Lua number.** Lua's only numeric type is an
IEEE-754 double, exact only up to 2^53 (~9e15), and a unix-nanos timestamp is routinely ~1.7e18,
nearly 200x past that. An early version exposed it as a Lua integer, and a script that only read
`event.timestamp` and wrote it back unchanged corrupted it (`tostring` showed `"1.7e+18"`). A
decimal-digit string round-trips exactly. A script that needs arithmetic can `tonumber()` at the
precision it needs; millisecond granularity fits a Lua number comfortably.

The same 2^53 limit applies to `Value::I64`/`Value::U64` attribute values. A naive cast used to
change a value one past 2^53 on `event.attributes.x = event.attributes.x`, and wrapped `u64::MAX`
negative. Unlike a timestamp, an integer attribute is usually small (`retry_count = 3`), and a
real Lua number is more useful to a script than a string. So `crates/logit-script/src/value.rs`
checks each I64/U64 against the exact-integer boundary and falls back to a string only when the
value doesn't fit. `Timestamp` values share the same logic and, being large in practice, always
take the string branch.

### Unmodified values keep their variant

**Variant identity survives an unmodified round-trip, via a no-op-assignment rule, not a tagged
value.** A plain Lua string or number can't carry which `Value` variant it came from. Without this
rule, an identity round-trip (`event.attributes.x = event.attributes.x`, or the ordinary pattern
of reading every attribute through `to_table()` and copying it back while tagging the event)
would change a value's variant without changing its content. That has real consequences:
`logit-outputs::influxdb`'s tag handling treats `Bytes` and `Str` differently.

`AttrsProxy::__newindex` (`crates/logit-script/src/proxy.rs`) treats an assignment as a no-op
when it's byte-for-byte (or number-for-number) what `value_to_lua` would already produce for the
attribute's current content (`value.rs`'s `lua_value_matches`). The stored `Value`, variant
included, stays untouched. An assignment that changes content converts as usual. See
[`lua-value-type-preservation.md`](lua-value-type-preservation.md) for the full mapping, why a
tagged userdata wrapper was rejected, and the known residual gaps (cross-key copies, nested
container elements, empty-container ambiguity), each deliberate and regression-tested.

### Measured cost

`script/bench`'s `lua::proxy`/`lua::to_table` divan arms
(`crates/logit-bench/benches/pipeline.rs`) measure the proxy against full table conversion
directly. The proxy wins, by more for scripts that read few attributes, because `to_table`
converts everything regardless of what the script touches. See
[`docs/known-gaps.md`](../known-gaps.md) for the closed follow-up and [`memory.md`](memory.md)
§2/§8 for the allocation side of the same comparison.

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
function flush(now)
  ...
  return {event1, event2, ...}  -- events to emit at this flush tick
end
```

`process` runs once per event. `flush` runs on the component's configured `interval` (see "Config
shape" below) and is how a stateful script turns accumulated state into emitted events, the same
contract the native aggregator ([docs/design/data-model.md](data-model.md)'s mergeable metric
kinds) runs on.

**`flush()` runs in a root context** ([ADR `lua-flush-root-context`](../adr/lua-flush-root-context.md)).
A flush is the result of no one event or batch, so `trace`, `provenance`, `resource` and `scope`
are reset before every call: a fresh trace root, this component as its own provenance, an empty
resource, and no scope. They are not left at whatever the last `process()` batch set. Each
global's section below says what this means for it.

**`flush` receives one argument, `now`: the runtime's tick time as a decimal-nanos string.** It
uses the same encoding as `event.timestamp` (and isn't a Lua number for the same 2^53 reason), and
it's the same `now_unix_nanos()` value the runtime hands the native `aggregate`'s own flush. It
exists so a flush-driven `Event.new{timestamp = now, ...}` ("Constructing events" below) has a
timestamp without a general clock. A script that declares `function flush()` with no parameter
ignores it, per ordinary Lua semantics.

**Don't use an event handle after you hand the event back.** An event handle, and its
`event.attributes` handle, is consumed once the event is returned from `process()` or included in
a `flush()` table. Lua userdata is a reference type, so a variable a script stashed elsewhere
(`pending = event`, or `pending_attrs = event.attributes`) can be the *exact same* object as the
one returned, not a copy. Extracting the returned event invalidates every other reference to it,
including a stashed `event.attributes` handle, because one is cached per event and reused for
every access (`crates/logit-script/src/proxy.rs`). Using a stale alias is a clear error, not
silently wrong data. To emit an event now and keep something for later (a stateful `flush()`
re-emitting it, say), stash `event:clone()` instead of `event` (or `event.attributes`).

`return {a, b}` must be a proper array-like table (keys exactly `1..=n`, Lua's own notion of a
sequence). A malformed table (non-contiguous keys) is a clear error, not a silently incomplete or
empty result.

**`process`/`flush` are resolved once, when the script loads**, not looked up from `_G` on every
event or flush tick, a deliberate cost/behavior trade-off (`crates/logit-script/src/lib.rs`). A
script that reassigns `_G.process`/`_G.flush` mid-run doesn't change what runs. This contract
never documented that pattern, so the restriction is narrow, but it's real. A `flush` global that exists but isn't a
function (or `nil`) is rejected at load time, the same as a missing `process`, rather than being
treated as "no `flush()`" and emitting nothing at every tick.

## Routing to a target

A `lua`/`lua_file` component that declares `targets:` is a **router** ([ADR
`target-components`](../adr/target-components.md)): each event it emits can name *one* of those
targets as its destination, and downstream components read a target by listing it in `sources:`
like any other component.

```yaml
components:
  central_in:
    type: logit_in
    bind: 0.0.0.0:5150

  split:
    type: lua
    sources: [central_in]
    targets: [host_stream, app_stream]
    script: |
      function process(event)
        if event.attributes.stream == "host" then
          return event:to("host_stream")
        elseif event.attributes.stream == "app" then
          return event:to("app_stream")
        end
        return event
      end

  host_stream: {type: target}
  app_stream:  {type: target}

  windowed:
    type: aggregate
    sources: [host_stream]
    interval: 60s

  untagged_out:
    type: stdio_out
    sources: [split]        # the component's own consumers: everything no `to` claimed
    target: stderr
```

`event:to(id)` **marks** an event; it doesn't emit it. The event still has to be returned from
`process()` (or included in a `flush()` table) like an unrouted one. `to` returns the handle it
was called on, so `return event:to("host_stream")` is the idiomatic one-liner, and
`local e = event:to("x")` leaves `e` and `event` as the same event, not two.

- **`event:to(nil)` clears the mark.** An event marked earlier in `process()` goes back to being
  unrouted.
- **An id not in this component's `targets:` is a script error**, counted like every other script
  error (`logit.component.errors{reason="process"}`) and naming the ids that *are* configured. It
  is never a silent forward. The same applies to `event:to("x")` on a component with no
  `targets:`.
- **An unmarked event goes to the component's own consumers**, the components that list it in
  `sources:`. That ordinary outbound edge is the else-branch; you don't need a chain of
  complementary filters. A router with targets and **no** ordinary consumers is a legal config,
  and it drops and counts its unmarked events as
  `logit.component.events.dropped{reason="unrouted"}` (`docs/design/internal-telemetry.md`). This
  includes unmarked events from `flush()`: an `interval:`-bearing router with no ordinary
  consumers loses any unmarked output its `flush()` emits.
- **`event:clone()` copies the mark**, so a script fanning a routed event out gets two events
  headed the same way; `copy:to(nil)` (or `copy:to("other")`) makes them diverge. The mark is
  per-*event*, not per-call: `return {a:to("x"), b}` sends `a` to `x` and `b` to the component's
  own consumers.
- **`flush()` honors marks the same way `process()` does.** A flush-built event is usually an
  `event:clone()` stashed during `process()`, and a clone carries the target list with it, so
  `e:to("x")` works inside `flush()` too.

One incoming batch is one hop however many ways it forks. The runtime partitions the events by
mark and sends one batch per destination that received any, all under the same trace context and
the same `process` span. Downstream of a target, `provenance.previous` is the **target's** id, not
this component's (`docs/design/pipeline-graph.md`); `provenance.origin` is unchanged.

To route on an equality check alone, use the native `route` component instead: it covers
`{attribute: ..}`/`{resource: ..}`/`{provenance: ..}` equality with no VM in the path. Use a `lua`
router when the decision needs something `route`'s deliberately narrow `by:` can't express.

## Emitting telemetry from a script

A `telemetry` global lets a script emit its own metrics from `process()` or `flush()`:

```lua
function process(event)
  telemetry.count("orders.total_value", event.attributes.amount, {status = "completed"})
  telemetry.gauge("queue.depth", 42)
  return event
end
```

`telemetry.count(name, n, tags?)` / `telemetry.gauge(name, v, tags?)`. `tags`, if given, is a
plain table of string keys to string values. There is no `timing()`: the sandboxed stdlib
(`table`/`string`/`math` only, see "Sandboxing" below) exposes no clock, so a script can't
produce a duration. `flush(now)`'s tick time ("Script contract" above) is the one exception: a
value the runtime already computed, handed only to the path with no incoming event to take a
timestamp from. `process()` can't read it, and two reads of it can't be subtracted into a
duration.

This is `logit`'s own self-observability mechanism (`docs/design/internal-telemetry.md`), extended
to scripts. A component's Rust code can only instrument what it can see, but a script often knows
domain facts (an order value, a business counter) Rust-side instrumentation can't infer. Script
points go through the same buffer, the same `internal` component, and the same downstream tools
(`aggregate`, any sink) as everything else, with nothing script-specific to configure. If no
config uses an `internal` component, `telemetry` calls are no-ops, like every other telemetry call
site.

**Use a fixed literal from the script's own source for a metric name or tag value, never event
data.** `telemetry.count("orders.total", 1)` is fine, called as often as you like.
`telemetry.count(event.attributes.order_id, 1)` runs, but leaks one process-wide interner entry
per distinct order id, forever. Cardinality safety for script-authored telemetry is the script
author's responsibility; the type system doesn't check it the way it does for the Rust call sites
`internal-telemetry.md` documents. See
[ADR `lua-authored-telemetry-cardinality`](../adr/lua-authored-telemetry-cardinality.md) for the
reasoning.

**Metric names starting with `logit.` are reserved** for `logit`'s own internal metrics
(`docs/design/internal-telemetry.md`). `telemetry.count("logit.component.events.received", 1)` is
a call-time error, not a silent merge into the runtime's own counter.

**A tag keyed `component`, `kind`, or `role` is rejected the same way.** Those tags identify which
component emitted a point, so a script can't set them: `telemetry.count("m", 1, {kind = "x"})` is
an error, not a silent no-op or a point misattributed to another component.

## Reading trace context

A `trace` global gives `process()` read access to the incoming batch's trace context, as lowercase
hex strings:

```lua
function process(event)
  event.attributes["trace.id"] = trace.trace_id
  return event
end
```

`trace.trace_id` is 32 hex characters (16 bytes) and `trace.span_id` is 16 (8 bytes). They hold the
`TraceContext` every node in the graph carries on its inbound batch
(`docs/adr/trace-context-propagation-on-delivered.md`), set once per incoming batch before any of
its events reach `process()`, so every event in one `process()` call sees the same value. Both
start at the all-zero placeholder (`"00...0"`) before any batch has arrived.

**This is `logit`'s own pipeline identity, not an application's.** `trace` names the node visit
that processed a batch. It is unrelated to `event.log.trace_id` (below), the *application's* trace
context a log line was emitted under. A script can deliberately copy one onto the other
(`event.log.trace_id = trace.trace_id`, stamping a log with the `logit` run that handled it), but
`logit` never does so itself. See
[ADR `log-record-trace-context`](../adr/log-record-trace-context.md).

**Inside `flush()`, `trace` is the fresh root the flushed batch is sent under**, not the last
processed batch's context ([ADR `lua-flush-root-context`](../adr/lua-flush-root-context.md)): a new
`trace_id`, and the `span_id` of this node's own `flush` span. A flush-driven emission has no
single incoming batch to attribute itself to, and `logit` can't know which batches contributed to
what a stateful script flushes, so it doesn't guess. To relate a flush to the batches that fed it,
track contributing contexts yourself inside `process()`, where they're readable. `logit` doesn't
aggregate them for a script the way
`docs/adr/trace-context-propagation-on-delivered.md`'s flush-side linking does for the native
`aggregate` transform.

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

**Unlike `trace` and `resource`, `provenance` is enforced read-only.** `provenance.origin = "x"`
raises `provenance.origin is read-only` instead of succeeding and being overwritten on the next
batch. A write to an unknown field (`provenance.bogus = 1`) raises
`provenance has no field 'bogus'` instead, so a caller can tell "this isn't yours to write" from
"you mistyped this". `event.has_log` and `event.log`'s fields use the same split.

`provenance.origin`/`.previous` are set once per incoming batch, before any of its events reach
`process()`, the same timing `trace` uses. `provenance.component` is fixed for the worker's
lifetime, set once when the pipeline starts.

**Inside `flush()`, `origin` and `previous` are both this component**:
`provenance.origin == provenance.previous == provenance.component`. That is the root a
flush-driven emission runs in, and what this component's outbound edge stamps on the flushed batch
([ADR `lua-flush-root-context`](../adr/lua-flush-root-context.md)). An event the flush marked for a
`target` (`event:to("a")`) takes one more hop, and that target rewrites `previous` to its own id,
as on the `process()` path ([ADR `target-components`](../adr/target-components.md)); `origin` stays
this component. "Reading trace context" above states the same rule for `trace`.

`provenance` carries no application meaning on its own. To put it in the outgoing data, copy it
into an attribute explicitly (`event.attributes["source.origin"] = provenance.origin`, as above);
`logit` never stamps it there itself.

## Reading and writing `resource`

A `resource` global gives `process()` and `flush()` read *and write* access to the incoming
batch's resource: the same `Arc<Resource>` `EventBatch::resource` carries
([data-model.md](data-model.md)), proxied like `event.attributes`:

```lua
function process(event)
  resource["service.name"] = "nginx"
  resource["service.namespace"] = "demo"
  return event
end
```

Read and write with `resource["key"]`, exactly like `event.attributes["key"]`. To enumerate every
key, use `resource:to_table()`: there is no `__pairs` under LuaJIT, for the same reason
`event.attributes` needs `event:to_table()`. Assigning `nil` stores a null value; it doesn't
remove the key. Lua can't delete a resource attribute, the same rule `event.attributes` follows.

**Per batch, not per event.** A write inside `process()` applies to the whole outgoing batch,
including events already processed earlier in the same batch. For a per-event identity, use
`event.attributes` instead.

**Write inside `process()`/`flush()`, not at load time.** Writes at a script's top level, before
any batch has arrived, are silently discarded when the first batch's `set_resource` call resets
`resource`. `resource` starts empty, the same all-clear starting point `trace` has, and is reset
to empty before every `flush()` call (see below).

**Copy-on-write, so a script that never touches `resource` pays nothing for it.** It follows the
same shape as `event.attributes` (`crates/logit-script/src/resource.rs`); see
[memory.md](memory.md) for the measured cost of the no-write and write paths.

**Inside `flush()`, `resource` starts empty, and writing it there is the only way a flush-driven
emission carries a resource** ([ADR `lua-flush-root-context`](../adr/lua-flush-root-context.md)). A
flush is the result of no one batch, so nothing carries over from the last processed batch:
`logit` can't attribute a flush to one of several upstream resources, and doesn't pretend to by
reusing the last one it saw. A script that knows the answer writes it:
`resource["service.name"] = "..."` inside `flush()` lands on the flushed batch just as a
`process()`-time write lands on its batch. A script that doesn't emits under the empty resource.

To stamp constant values without Lua, use the `set` native transform
(`logit_config::ComponentKind::Set`) downstream. See
[ADR `operator-declared-resource-attributes`](../adr/operator-declared-resource-attributes.md),
which also explains why this is a graph component rather than a per-input config field.

**`schema_url` and `dropped_attributes_count` mirror OTLP's own `Resource` fields**
([`docs/plans/lossless-transit.md`](../plans/lossless-transit.md)):

- `schema_url` is read/write, a string or `nil`. Assigning `nil` clears it, the same "`nil` means
  removal for this one field" exception `event.log.trace_id` below has.
- `dropped_attributes_count` is read-only. It is OTLP's count of attributes a *producer* dropped
  before the resource reached `logit`, which a Lua write can't meaningfully change.
  `resource.dropped_attributes_count = 5` raises `resource.dropped_attributes_count is read-only`,
  the same "read-only, name it" rule `provenance` follows.

**A named field takes precedence over an attribute of the same name.** `resource["schema_url"]`
and `resource["dropped_attributes_count"]` always resolve to the fields above, never to an
attribute keyed `schema_url`/`dropped_attributes_count`. Such an attribute still appears in
`resource:to_table()` (a flat attribute snapshot: `resource:to_table()["schema_url"]`); it just
isn't reachable through `resource[...]` indexing. `event`'s own fixed fields (`timestamp`,
`attributes`, `log`, ...) make the same trade against a same-named attribute. This is documented,
not guarded against.

## Reading and writing `scope`

A `scope` global gives `process()` and `flush()` read *and* write access to the incoming batch's
OTLP instrumentation scope (`EventBatch::scope: Option<Arc<Scope>>`), proxied like `resource`
above (`crates/logit-script/src/scope.rs`):

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

**`scope`'s attributes live under `scope.attributes`, not directly on `scope[key]`.** This differs
from `resource`, where `resource["service.name"]` indexes the attribute map directly and there is
no `resource.attributes` sub-object. `scope["k"]` doesn't fall through to an attribute the way
`resource["k"]` does; write `scope.attributes["k"]`, mirroring `event.attributes`.
`scope.attributes = ...` is read-only: you can write into the sub-object per key but can't replace
it.

**A batch may carry no scope at all**, because `EventBatch::scope` is `Option`al. Before any write,
reads return the all-clear values, the same ones `logit_core::Scope::default()` carries: `""` for
`name`/`version`, `nil` for `schema_url`, `0` for `dropped_attributes_count`, and an empty table
for `attributes`. A write on such a batch starts `modified` from `Scope::default()` instead of
erroring, just as `resource`'s write path starts from an empty `Resource` when nothing has stamped
one yet.

**Per batch, not per event, and a write regroups OTLP output.** A `scope` write inside
`process()`/`flush()` applies to the whole outgoing batch. `otlp_out` groups outgoing events by
their `(Resource*, Scope*)` pair, so writing `scope` mid-batch changes which wire
`InstrumentationScope` every event in the batch lands under, not just the one being processed. A
`resource` write regroups output the same way.

Before any batch has arrived, `scope` reads as the defaults above (`base: None`; `resource`'s own
`base`, by contrast, is always a real, if empty, `Arc<Resource>`). It is reset once per incoming
batch before any of its events reach `process()`, the same timing `resource` uses.

**Copy-on-write, and reset before `flush()`, the same way and for the same reason as `resource`.**
A script that never touches `scope` pays nothing for it (`crates/logit-script/src/scope.rs`). A
`flush()` call starts with no scope at all (the all-clear defaults above), not the last processed
batch's, and a `scope` write inside `flush()` is the only way a flush-driven emission carries one.
`crates/logit-pipeline/src/runtime.rs`'s `run_lua` commits a `scope` write the same way it commits
a `resource` write, on both the per-batch and the `flush()` path. See
[ADR `lua-flush-root-context`](../adr/lua-flush-root-context.md).

`scope:to_table()` is the enumeration escape hatch, as `resource:to_table()` is, but its shape
differs. `resource:to_table()` is the flat attribute map; `scope:to_table()` returns the named
fields with the attributes nested:
`{name=, version=, schema_url=, attributes={...}, dropped_attributes_count=}`, with `schema_url`
present only when set.

## Reading and writing `event.log`

`event.log` is `nil` on an event with no log (`event.has_log == false`). Otherwise it's a proxy
onto the log record:

```lua
function process(event)
  if event.log and event.log.trace_id == nil then
    event.log.trace_id = "4bf92f3577b34da6a3ce929d0e0e4736"
  end
  return event
end
```

| Field | Access |
|---|---|
| `trace_id`, `span_id`, `trace_flags` | read/write |
| `event_name`, `observed_timestamp` | read/write |
| `message`, `severity`, `body_format` | read-only for now |
| `dropped_attributes_count` | read-only |

**Trace context.** `trace_id`/`span_id` are lowercase hex strings, `nil` when absent: 32
characters (16 bytes) and 16 characters (8 bytes), the same shape the `trace` global uses.

- Assigning a valid hex string to `trace_id` replaces the whole trace context. A log with no trace
  context gets a fresh one, and a log that had one gets a fresh one too, with `span_id`/
  `trace_flags` reset, because an old span belongs to the old trace.
- Assigning `nil` to `trace_id` clears the *whole* trace context, `span_id`/`trace_flags`
  included, because OTLP's contract is that a span only means something alongside a trace.
- Set `trace_id` first. Assigning `span_id` or `trace_flags` before it is an error, not a silent
  no-op, because there's nothing for them to attach to.
- `trace_flags` is a plain integer, 0-255 (the low 8 bits OTLP's `LogRecord.flags` carries); bit 0
  is the W3C `SAMPLED` flag.
- An invalid hex string, or a `trace_flags` outside `0..=255`, is a runtime error naming the
  field, the same strictness `event.timestamp`'s parse has.

**`message`/`severity`/`body_format` are read-only for now.** Writing them waits on a concrete
need; it isn't an oversight. Assigning to any of the three raises a "read-only for now" error.

- `message` reads as whatever `Value` the log body holds, most commonly a string.
- `severity` reads as a lowercase name
  (`"trace"`/`"debug"`/`"info"`/`"warn"`/`"error"`/`"fatal"`, matching `stdio_out`'s rendering),
  or `nil` if the record carries none.
- `body_format` reads as `"raw"`/`"json"`/`"structured"`.

**`event_name`, `observed_timestamp`, and `dropped_attributes_count`** are real `LogRecord` fields
([`docs/plans/lossless-transit.md`](../plans/lossless-transit.md)).

- `event_name` is read/write, a plain string or `nil`. `event.log.event_name = "request.completed"`
  interns the string, as a string-valued attribute write does. **Use a name from a fixed, bounded
  vocabulary in the script's own source, never one built from event data.** This is the same
  cardinality caution `telemetry.count`'s metric name carries ("Emitting telemetry from a script"
  above): a name built from a request id or order id leaks one process-wide interner entry per
  distinct value, forever (`docs/known-gaps.md`'s interner entry).
- `observed_timestamp` is read/write, a decimal-digit string, not a Lua number, for the same 2^53
  reason as `event.timestamp`. `0` (OTLP's "unset" convention) reads back as the string `"0"`, not
  `nil`: unlike `event.log` itself, this field has no "unset means absent" convention.
- `dropped_attributes_count` is read-only: OTLP's count of attributes a *producer* dropped before
  the record reached `logit`, which a Lua write can't meaningfully change.
  `event.log.dropped_attributes_count = 5` raises `event.log.dropped_attributes_count is read-only`,
  the same "read-only, name it" rule `provenance` follows.

**Native alternatives.** The `trace_context` native transform
(`logit_config::ComponentKind::TraceContext`) covers the common case without Lua: lifting a trace
id already in an attribute (a JSON log body's `trace.id` field, or a W3C `traceparent`) onto the
log record, the same relationship `set` has to `resource`/`event.attributes`. See
`docs/adr/log-record-trace-context.md`. Its `span:` block mints a `SpanRecord` from an access
line's ids and timing (`docs/adr/trace-context-span-lifting.md`, `docs/design/data-model.md`'s
"Well-known attribute names").

A script can *read* an existing span through `event.span` (read-only in place; see "Reading
`event.span`" below) and *create* one with `Event.new` (the `span` table in "Constructing events"
below), which takes everything `trace_context` lifts and more: `events`, `links`, a
`parent_span_id`, an `ext`. It can't mutate an existing `event.span` field by field; to change
one, call `Event.new(event:to_table())` with the table edited. `docs/known-gaps.md` narrowed this
gap twice: first (in `lossless-transit`) to span writes and minting, then (in
`lua-event-constructor`) to in-place span mutation alone.

## Reading and writing `event.metrics`

`event.metrics` gives `process()` and `flush()` access to the event's metric list: an indexable,
array-like proxy over `logit_core::MetricList` (`crates/logit-script/src/proxy.rs`). It is always
present, even on an event with no metrics: `#event.metrics == 0` is a normal read. Unlike
`event.log`/`event.span`, there's no `nil` gate on the container, only on what indexing into it
returns.

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

**The container's whole surface is `#event.metrics` (`MetaMethod::Len`) and 1-based
`event.metrics[i]` (`MetaMethod::Index`).** Lua can't add, remove, or reorder metrics. Indexing
out of range (including `<= 0`) reads `nil`, like reading past the end of an ordinary Lua array;
only a non-integer key is an error.

Each `event.metrics[i]` access mints a small, fresh `MetricProxy` instead of reusing a cached one
the way `event.attributes`/`event.log` do. A metric list is typically short and read once per
access, so this trades a per-access allocation for not holding a registry slot per index for the
event's lifetime. The allocation is pinned in `crates/logit-bench/tests/allocations.rs`; see
[`memory.md`](memory.md) §2.

**Every access rechecks the index.** Nothing in today's Lua surface can shrink `event.metrics`
mid-script, but every access checks the index against the current list length anyway and raises
`event.metrics[i] no longer exists` rather than trusting a stale handle. This is cheap insurance
against a future surface that *can* shrink the list, such as an `event.metrics:remove(i)`.

`kind` names every metric kind: `"sum"`, `"gauge"`, `"gauge_delta"`, `"samples"`,
`"distribution"`, `"set_members"`, `"set"`, `"histogram"`, `"exponential_histogram"`, `"summary"`
(`crates/logit-core/src/metric.rs`'s `MetricKind`, [`docs/plans/lossless-transit.md`](../plans/lossless-transit.md)'s
target model). Every field is readable on every kind (`nil` when the kind doesn't carry it), but
**only a handful are writable, and only on the kind that makes them meaningful**:

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
| `estimate` | count | read-only | `set` |
| `buckets` | table, array of `{bound=, count=}` (`count` a count) | read-only | `histogram` |
| `sum` | number or `nil` | read-only | `histogram`/`exponential_histogram` (optional), `summary` (always a number) |
| `min` | number or `nil` | read-only | `histogram`/`exponential_histogram` |
| `max` | number or `nil` | read-only | `histogram`/`exponential_histogram` |
| `count` | count | read-only | `distribution` (the sketch's own observation count, `DdSketch::count()`), `exponential_histogram`, `summary` |
| `scale` | integer | read-only | `exponential_histogram` |
| `zero_count` | count | read-only | `exponential_histogram` |
| `zero_threshold` | number | read-only | `exponential_histogram` |
| `positive` | table, `{offset=, counts=[...]}` (`counts` an array of counts) | read-only | `exponential_histogram` |
| `negative` | table, `{offset=, counts=[...]}` (`counts` an array of counts) | read-only | `exponential_histogram` |
| `quantiles` | table, array of `{quantile=, value=}` | read-only | `summary` |
| `:quantile(q)` | method, returns a number or `nil` | -- | meaningful only on `distribution`; `nil` on every other kind |

`count` is a plain field, not a method: unlike `:quantile(q)`, no argument changes its meaning, so
a script writes `m.count`, not `m:count()`.

**A count is an integer up to 2^53 and a decimal string above it**, the rule an `I64`/`U64`
attribute follows ("Timestamps are strings" above): a Lua number would round a larger count, and
one past `i64::MAX` would read negative. `m.count` on a summary of 9007199254740993 observations
is the string `"9007199254740993"`; `tonumber()` it for arithmetic at the precision a Lua number
has. The same encoding applies in `to_table()`, and `Event.new` accepts either form, so a count at
any magnitude round-trips.

**A write the metric's kind doesn't allow names the kind.** Writing an always-read-only field
(`flags`, `kind`, `exemplars`, `values`, `sum`, `count`, ...), or a kind-specific one (`value`,
`temporality`, `monotonic`) on a kind that doesn't support it, raises
`event.metrics[i].<field> is read-only on a <kind> metric`. The kind is in the message because the
same field is writable on a different kind. **Writing `value` on `gauge_delta` is rejected too**,
though it's readable there: `gauge_delta` is explicitly *unresolved* state that must never reach a
sink unresolved (`docs/known-gaps.md`'s relative-gauge-adjustments entry), so there's no
meaningful in-place adjustment to make.

**Only `sum`/`gauge` payloads are mutable in place.** A script can adjust a counter or a gauge, or
rename, retag, or re-time any metric regardless of kind, but can't write a sketch or a
cardinality estimate by hand. This mirrors `AGENTS.md`'s "metric kinds must stay mergeable" rule
on the Rust side: `distribution` (`DdSketch`) and `set` (`HyperLogLog`) carry merge invariants a
naive field write could violate, and `samples`/`set_members` are raw pre-aggregation collections
`aggregate` still has to fold correctly. None of these has a script-safe partial-write surface, so
none gets one. Every kind but the sketches *can* be built whole: `Event.new` constructs a `sum`,
`gauge`, `samples`, `set_members`, `histogram`, `exponential_histogram` or `summary` record
(exemplars included) from the table shape this proxy's `to_table()` emits, and refuses the two
sketches by name. See "Constructing events" below.

`exemplars` is a read-only snapshot table, one entry per `logit_core::Exemplar`: `{timestamp=
<nanos-string>, value=<number>, trace_id=<hex-or-nil>, span_id=<hex-or-nil>,
trace_flags=<integer-or-nil>, attributes=<table>}`. `trace_flags` is `nil` exactly when
`trace_id` is, the same rule `event.log.trace_flags` follows. Lua can't add, remove, or mutate an
individual exemplar in place, only read the whole list or rebuild the record with `Event.new`.

## Reading `event.span`

`event.span` is read-only in place; build a new span with `Event.new`. It is `nil` on an event with
no span (`event.has_span == false`). Otherwise it's a read-only proxy onto the span record
(`crates/logit-script/src/proxy.rs`), however the span was made: by `Event.new` (the `span` table
in "Constructing events" below), by `trace_context`'s `span:` block
([ADR `trace-context-span-lifting`](../adr/trace-context-span-lifting.md)), or by a wire codec.

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

`SpanRecord.ext` (`crates/logit-core/src/span.rs`) is boxed and is `None` in the common case, a
span with no error status message and no W3C tracestate. Then `status_message`/`trace_state` read
`nil` and every `dropped_*_count` reads `0`; the proxy neither errors nor fabricates a `Some`.

- `events[i]` is `{timestamp=<nanos-string>, name=<value>, attributes=<table>,
  dropped_attributes_count=<integer>}`.
- `links[i]` is `{trace_id=<hex>, span_id=<hex>, trace_state=<string-or-nil>, flags=<integer>,
  dropped_attributes_count=<integer>, attributes=<table>}`.

**`event.span` has no `attributes` field.** A `SpanRecord` has no attribute map of its own: a
span-carrying event's attributes are `event.attributes`, the single attribute set every event has,
so there's nothing for `event.span.attributes` to be.

**Any assignment raises `event.span is read-only`.** Unlike every other proxy in this module,
`event.span`'s write path doesn't distinguish an unknown field from a read-only one, because no
span field is writable in place. To change a span, rebuild the event:
`local t = event:to_table(); t.span.status = "error"; return Event.new(t)`. To mint one from
nothing, use `Event.new{timestamp = ..., span = {...}}`; see the `span` table below.

## Constructing events

**`Event.new(t)` is the inverse of `event:to_table()`** ([ADR
`lua-event-constructor`](../adr/lua-event-constructor.md), `crates/logit-script/src/construct.rs`).
The `Event` global, a table with one function, `new`, is installed on every worker's VM beside
`telemetry`/`trace`/`resource`/`scope`/`provenance`, before the script's top-level code runs.
`Event.new` takes one table in exactly the shape `to_table()` returns (same keys, encodings, and
nesting) and returns an ordinary event handle. You can mutate it, `clone()` it, mark it with
`to()`, and return it from `process()` or include it in a `flush()` table, like any event a script
was handed. `Event.new(event:to_table())` round-trips every lossless shape, so "rebuild this event
with one field changed" is
`local t = event:to_table(); t.log.severity = "error"; return Event.new(t)`.

```lua
-- a stateful script minting a log line at each tick, with no incoming event to copy from
local seen = 0

function process(event)
  seen = seen + 1
  return event
end

function flush(now)
  local summary = Event.new{
    timestamp = now,
    attributes = {component = provenance.component},
    log = {message = "processed " .. seen .. " events", severity = "info"},
  }
  seen = 0
  return {summary:to("audit")}
end
```

The top-level table:

| Key | Type / encoding | Required? |
|---|---|---|
| `timestamp` | decimal-nanos string, the same rule as `event.timestamp` (a Lua number is the same error `event.timestamp = 1` is) | **required** |
| `attributes` | table of string keys; every value converts the way an `event.attributes.k = v` write does | optional, default empty |
| `log` | table, below | optional |
| `metrics` | array of metric tables, below, in order | optional, default empty (which is what `to_table()` emits for an event with no metrics) |
| `span` | table, below; the event's `timestamp` is the span's start | optional |
| `has_log`, `has_metrics`, `has_span` | boolean | optional; accepted because `to_table()` emits them, **values ignored** -- the payload keys are the truth |

The `log` table, `to_table().log`'s shape:

| Key | Type / encoding | Required? |
|---|---|---|
| `message` | any value (a string, most commonly), converted like an attribute value | **required** |
| `severity` | `"trace"`/`"debug"`/`"info"`/`"warn"`/`"error"`/`"fatal"` or `nil` | optional, default absent |
| `body_format` | `"raw"`/`"json"`/`"structured"` | optional, default `"raw"` |
| `trace_id` | 32-char hex string, not all-zero | optional, default no trace context |
| `span_id` | 16-char hex string, not all-zero; only with `trace_id` | optional |
| `trace_flags` | integer 0-255; only with `trace_id` | optional, default `0` |
| `event_name` | string (interned -- the same cardinality caution as the `event.log.event_name` write) | optional, default absent |
| `observed_timestamp` | decimal-nanos string | optional, default `0` |
| `dropped_attributes_count` | non-negative integer | optional, default `0` |

### `metrics`

Each entry of `metrics` is a metric table in `to_table().metrics[i]`'s shape: the fields every
kind carries, plus the payload fields of its `kind`. `Event.new` reads `kind` first, and it
decides which payload keys are fields at all: `monotonic` is a field on a `sum`, and on a `gauge`
it raises `Event.new: metrics[1].monotonic is not a field`. So a typo in `kind` is reported as a
bad kind, never as unknown payload keys.

| Key | Type / encoding | Required? |
|---|---|---|
| `name` | string (interned -- the same cardinality caution as the `event.metrics[i].name` write) | **required** |
| `kind` | `"sum"`, `"gauge"`, `"samples"`, `"set_members"`, `"histogram"`, `"exponential_histogram"` or `"summary"` (the others below) | **required** |
| `unit`, `description` | string (interned) or `nil` | optional, default absent |
| `start_timestamp` | decimal-nanos string | optional, default `0` (unknown, OTLP's own convention) |
| `flags` | non-negative integer, the OTLP `DataPointFlags` mask | optional, default `0` |
| `is_no_recorded_value` | boolean -- sugar for the flag bit: `true` ORs `MetricRecord::FLAG_NO_RECORDED_VALUE` onto `flags`, `false` leaves `flags` untouched (so `{flags = 1, is_no_recorded_value = false}` keeps the bit, and a round-trip of a flagged record is exact) | optional |
| `exemplars` | array of exemplar tables, below | optional, default empty |

Per kind (only that kind's keys are accepted):

| `kind` | Key | Type / encoding | Required? |
|---|---|---|---|
| `sum` | `value` | finite number (NaN and the infinities are the same error a `value` write raises) | **required** |
| | `temporality` | `"delta"` or `"cumulative"` | optional, default `"delta"` |
| | `monotonic` | boolean | optional, default `true` |
| `gauge` | `value` | finite number | **required** |
| `samples` | `values` | array of finite numbers | optional, default empty |
| | `sample_rate` | finite number | optional, default `1.0` (`Samples::new`'s) |
| `set_members` | `members` | array of strings (each stored as opaque bytes, UTF-8 or not) | optional, default empty |
| `histogram` | `buckets` | array of `{bound = <number>, count = <non-negative integer or decimal-digit string>}` rows; may be empty. `count` is each bucket's *own* observation count, not a running total -- a Prometheus `le="1"`=3, `le="+Inf"`=5 series is `{bound = 1, count = 3}, {bound = math.huge, count = 2}`. Bounds must be strictly increasing (a duplicate or out-of-order bound is an error). `bound` is the one field anywhere in `Event.new` that may be non-finite, and only as `math.huge`, and only on the *last* row: that is the overflow bucket (Prometheus's `+Inf`, OTLP's implicit last `bucket_counts` entry), which `to_table()` emits with the bound `math.huge`. A non-empty `buckets` whose last bound is finite gets `{bound = math.huge, count = 0}` appended -- the constructor's one normalization; it adds no information and keeps the OTLP shape valid. NaN and `-math.huge` are rejected | **required** |
| | `temporality` | `"delta"` or `"cumulative"` | **required** -- core documents no default for it |
| | `sum`, `min`, `max` | finite number or `nil` (`to_table()` emits `nil` for an absent one) | optional, default absent |
| `exponential_histogram` | `scale` | integer in `[-10, 20]` (OTLP's `ExponentialHistogramDataPoint.scale` range) | **required** |
| | `zero_count`, `count` | non-negative integer, or a string of decimal digits (the form `to_table()` gives a count past 2^53) | **required** |
| | `zero_threshold` | finite number | **required** |
| | `positive`, `negative` | table of exactly `{offset = <integer fitting an i32>, counts = <array of non-negative integers or decimal-digit strings>}`; `counts` may be empty but must be present | **required** |
| | `temporality` | `"delta"` or `"cumulative"` | **required** -- core documents no default for it |
| | `sum`, `min`, `max` | finite number or `nil` | optional, default absent |
| `summary` | `quantiles` | array of `{quantile = <number in [0, 1]>, value = <finite number>}` rows; may be empty and need not be sorted | **required** |
| | `count` | non-negative integer, or a string of decimal digits | **required** |
| | `sum` | finite number | **required** |

A `sum` given only its `value` is `MetricKind::counter` (delta, monotonic), so `{name = "hits",
kind = "sum", value = 1}` is exactly the counter `statsd_in`'s `c` or a `kv_metrics` `counters:`
entry emits. Every other default above is one core documents; `Event.new` invents none. That's
why a `histogram`'s or `exponential_histogram`'s `temporality` is required: a `sum` can fall back
on `MetricKind::counter`'s delta, but these have nothing to fall back on
(`Event.new: metrics[1].temporality is required`).

An exemplar table, `to_table()`'s exemplar snapshot shape (see "Reading and writing
`event.metrics`" above):

| Key | Type / encoding | Required? |
|---|---|---|
| `timestamp` | decimal-nanos string | **required** |
| `value` | finite number | **required** |
| `trace_id` | 32-char hex string, not all-zero | optional, default no trace context |
| `span_id` | 16-char hex string, not all-zero; only with `trace_id` | optional |
| `trace_flags` | integer 0-255; only with `trace_id` | optional, default `0` |
| `attributes` | table of string keys, the exemplar's filtered attributes, converted like `attributes` above | optional, default empty |

Errors carry the full path. For example:

- `Event.new: metrics[2].value must be a finite number, got NaN`
- `Event.new: metrics[1].values[3] must be a number, got string`
- `Event.new: metrics[1].members[1] must be a string, got integer`
- `Event.new: metrics[1].exemplars[1].span_id can't be set without a trace_id`
- `Event.new: metrics[1].exemplars[1].flags is not a field`
- `Event.new: metrics[1].buckets[2].bound must be a finite number or math.huge, got NaN`
- `Event.new: metrics[1].positive.counts[1] must be a non-negative integer, got -2`
- `Event.new: metrics[1].quantiles[1].value is required`
- `Event.new: metrics[1].scale must be an integer between -2147483648 and 2147483647, got 1099511627776`
- A non-table entry: `Event.new: metrics[1] must be a table, got integer`. A bucket or quantile
  row likewise: `Event.new: metrics[1].buckets[1] must be a table, got integer`.
- `metrics`, `exemplars`, `values`, `members`, `buckets`, `quantiles` and a side's `counts` must
  each be a contiguous array: `{[2] = 1}` raises
  `Event.new: metrics[1].values must be a contiguous array-like table`.

**Kinds a script can't build.** An unknown `kind` lists the constructible ones:
`Event.new: metrics[1].kind must be one of sum, gauge, samples, set_members, histogram, exponential_histogram, summary, got "counter"`.
Three kinds are deliberately absent from that list and will never be constructible, and the error
for each says why:

- `distribution` and `set` raise `is not constructible from Lua -- a merged sketch; build a "samples" metric and let aggregate summarize it`
  (`"set_members"` for `set`). `to_table()` emits only a `count` or an `estimate` for them, never
  the DDSketch or HyperLogLog state, so there's no shape to invert, and a sketch rebuilt from a
  count alone would misrepresent its contents.
- `gauge_delta` raises `is not constructible from Lua -- aggregate's private intermediate, never valid at a sink`
  ([`docs/known-gaps.md`](../known-gaps.md)'s relative-gauge-adjustments entry).

### `span`

The `span` table is `to_table().span`'s shape: the fields `event.span` reads ("Reading
`event.span`" above), with the event's `timestamp` as the span's start. A `SpanRecord` has no
start of its own (`crates/logit-core/src/span.rs`). A span needs its two ids and a `name`;
everything else defaults to what core documents.

| Key | Type / encoding | Required? |
|---|---|---|
| `trace_id` | 32-char hex string, not all-zero | **required** |
| `span_id` | 16-char hex string, not all-zero | **required** |
| `parent_span_id` | 16-char hex string, not all-zero, or `nil` | optional, default absent |
| `name` | any value (a string, most commonly), converted like an attribute value | **required** |
| `kind` | `"internal"`/`"server"`/`"client"`/`"producer"`/`"consumer"` | optional, default `"internal"` |
| `status` | `"unset"`/`"ok"`/`"error"` | optional, default `"unset"` |
| `end_timestamp` | decimal-nanos string; must not precede the event's `timestamp` -- the same rule `trace_context`'s `span:` block applies to a lifted span, and a zero-duration span (`end_timestamp == timestamp`) is fine | optional, default the event's `timestamp` |
| `flags` | non-negative integer, OTLP's `Span.flags` (the low 8 bits are the W3C trace flags) | optional, default `0` |
| `status_message` | string or `nil` | optional, default absent |
| `trace_state` | string or `nil`, the W3C `tracestate` | optional, default absent |
| `dropped_attributes_count`, `dropped_events_count`, `dropped_links_count` | non-negative integer | optional, default `0` |
| `events` | array of span-event tables, below, in order | optional, default empty (which is what `to_table()` emits for a span with none) |
| `links` | array of span-link tables, below, in order | optional, default empty |

A span-event table, `to_table().span.events[i]`'s shape:

| Key | Type / encoding | Required? |
|---|---|---|
| `timestamp` | decimal-nanos string | **required** |
| `name` | any value, converted like an attribute value | **required** |
| `attributes` | table of string keys, converted like `attributes` above | optional, default empty |
| `dropped_attributes_count` | non-negative integer | optional, default `0` |

A span-link table, `to_table().span.links[i]`'s shape. Both ids are required, because a link *is*
a reference to another span:

| Key | Type / encoding | Required? |
|---|---|---|
| `trace_id` | 32-char hex string, not all-zero | **required** |
| `span_id` | 16-char hex string, not all-zero | **required** |
| `trace_state` | string or `nil` | optional, default absent |
| `flags` | non-negative integer | optional, default `0` |
| `dropped_attributes_count` | non-negative integer | optional, default `0` |
| `attributes` | table of string keys | optional, default empty |

There is no `span.attributes`, for the reason "Reading `event.span`" gives: a span-carrying
event's attributes are the top-level `attributes`, so `span = {attributes = {}}` raises
`Event.new: span.attributes is not a field`.

`SpanRecord.ext` (`status_message`, `trace_state`, the three `dropped_*` counts) is boxed only
when one of the five is non-default, the rule `crates/logit-proto`'s `ext_from_wire` already
applies to a decoded span. So a minimal constructed span costs what a minimal decoded one does,
and writing `dropped_events_count = 0` explicitly doesn't allocate a box.

**Some decoded spans can't be rebuilt unchanged.** `Event.new` deliberately rejects three shapes a
wire decoder can produce:

- A `Value::Null` span or span-event `name`, the same residual as a `Value::Null` `message`:
  `to_table()` emits it as an absent key, and `Event.new` rejects it as missing.
- An `end_timestamp` before the event's `timestamp`, which raises
  `Event.new: span.end_timestamp precedes timestamp`. `otlp_in` and the native codec carry the
  wire value through unchecked, a missing end decodes as `0`, and `stdio_out` renders such a span
  with a saturating duration.
- An all-zero `trace_id`/`span_id`/`parent_span_id` or link id, which raises the `not all-zero`
  error above. Both decoders check length only, so an exporter that pads a root span's parent
  with eight zero bytes yields `parent_span_id = "0000000000000000"`.

To rebuild such an event through `Event.new(event:to_table())`, fix or drop the offending field
first: `t.span.parent_span_id = nil`, say, or `t.span.end_timestamp = t.timestamp`.

```lua
-- a script minting a span from a line trace_context can't lift (say, a two-timestamp line
-- whose end is in a different field on each variant)
function process(event)
  local a = event.attributes
  local t = event:to_table()
  t.span = {
    trace_id = a["trace.id"], span_id = a["span.id"], name = a["http.route"] or "request",
    kind = "server", status = a["http.status"] >= 500 and "error" or "unset",
    end_timestamp = a["request.end"],
    events = {{timestamp = a["request.end"], name = "response.sent"}},
  }
  return Event.new(t)
end
```

Errors carry the full path. For example:

- `Event.new: span.trace_id is required`
- `Event.new: span.trace_id must be a 32-character hex string, and not all-zero`
- `Event.new: span.parent_span_id must be a 16-character hex string (or nil), and not all-zero`
- `Event.new: span.end_timestamp precedes timestamp`
- `Event.new: span.kind must be one of internal, server, client, producer, consumer (or nil), got "SERVER"`
  (names are exact and lowercase, as everywhere)
- `Event.new: span.events[1].name is required`
- `Event.new: span.links[1].span_id is required`
- `Event.new: span.links[1].bogus is not a field`
- `Event.new: span.status_message must be a string or nil, got integer`
- A non-table row: `Event.new: span.events[1] must be a table, got integer`.
- `events` and `links` must each be a contiguous array:
  `Event.new: span.links must be a contiguous array-like table`.

### Errors, targets, and value conversion

**Every mistake is a runtime error at the call, prefixed with the dotted path down to the
field.** A malformed value inside a nested attribute table reports the shared
attribute-conversion error behind the attribute's own path. Unknown keys are rejected everywhere, at the top level and in
sub-tables, the same strictness the proxies apply to an unknown field on read or write. Examples:

- `Event.new: log.severty is not a field`
- `Event.new: timestamp is required`
- `Event.new: log.severity must be one of trace, debug, info, warn, error, fatal (or nil), got "warning"`
- `Event.new: log.span_id can't be set without a trace_id`
- `Event.new: attributes has a non-string key (integer)`
- `Event.new: attributes.cb can't be a Lua function`
- `Event.new: attributes.loop: can't use a table nested more than 128 levels deep as an event attribute value (does a table contain itself?)`

Table access is raw, so a metatable on the input can't make the key check and the field reads
disagree. Defaults exist only where core already documents one (`BodyFormat::Raw`, the zeros
above, `MetricKind::counter`'s temporality and monotonicity, `Samples::new`'s `sample_rate`, a
span's `internal`/`unset` and its own start as its end); `Event.new` invents nothing else.

**Targets resolve at call time, not at script load.** A constructed event's `to(id)` checks the
worker's `targets:` list as it stands when `Event.new` runs. Inside `process()` or `flush()`,
that's the list the component declared. An `Event.new` at the script's top level runs before the
list is installed and sees an empty one, so `e:to("x")` on such an event raises the same
"declares no targets" error a plain `lua` component gives. A top-level `resource` write has the
same caveat ("Reading and writing `resource`" above). This is documented, not prevented: call
`Event.new` inside `process()`/`flush()`.

**Values convert the way any fresh Lua value does.** A constructed value has no existing `Value`
to compare against, so the no-op-assignment identity rule
([ADR `lua-value-identity-preservation`](../adr/lua-value-identity-preservation.md)) can't apply.
A Lua string becomes `Str` (or `Bytes` only if it isn't valid UTF-8), a Lua integer `I64`, and an
empty table an empty `Map`. So these don't round-trip through `Event.new(e:to_table())`:

- `U64`/`Timestamp`/UTF-8 `Bytes` attribute values: `Value::U64(5)` comes back `Value::I64(5)`.
- An `I64` past ±2^53, which `to_table()` emits as a decimal string (the same string branch
  `Timestamp` takes) and which comes back as `Value::Str`: `Value::I64(9007199254740993)` returns
  as `Value::Str("9007199254740993")`.
- An *integral* `F64` such as `3.0`, which LuaJIT's dual-number mode canonicalizes to a Lua
  integer, so it comes back `Value::I64(3)`. A fractional `F64` is unaffected.
- A `Value::Null` `message`, which `to_table()` emits as an absent key and `Event.new` rejects as
  missing.
- A NaN or infinite `sum`/`gauge`/`samples`/exemplar `value` (or `sample_rate`), which the
  finiteness rule above rejects. `to_table()` emits the raw float, and the pipeline does admit
  such a point: `prometheus_in` carries OpenMetrics `NaN`/`+Inf` through verbatim, and `otlp_in`
  passes an `AsDouble` through unfiltered. A script rebuilding a scraped event must fix or drop
  the offending value.

Each case is that ADR's recorded residual, not an oversight.

## Config shape

A Lua transform is one component in the pipeline's component graph
(`docs/design/pipeline-graph.md`, `docs/adr/component-graph-configuration.md`). It names its own
`sources` and is available as a source to anything downstream; there's no fixed per-pipeline
`transforms:` chain. The script can be inline in YAML (a block scalar) or in a file:

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

**`interval`** is optional and drives the component's `flush()`, the same way `aggregate`'s does
(see `docs/adr/aggregation-window-semantics.md`). If you omit it, the common case, the component
never ticks, the same as a script with no `flush()`. Config validation rejects a zero interval on
either kind of component.

**`targets:`** is optional and lists the `target` components this one may direct events into:
what `event:to(id)` resolves against ("Routing to a target" above). It sits beside `sources:` on
the component, not among the `script:`/`lua_file:` fields. A non-empty `targets:` is legal only on
`lua`/`lua_file`. `route` is the graph's other router kind, but it declares its targets through
`routes:`' values and must leave `targets:` empty, like every non-router kind
(`docs/design/pipeline-graph.md`'s validation rule 47). Without `targets:`, every event the
component emits goes to its own consumers.

**Native transforms handle the common parsing cases without per-event VM overhead:** `json`,
`logfmt`, `kv`, `regex`, `csv`, `keep`/`remove`/`set`, `has_attributes`/`drop_attributes`,
`sample`, `aggregate`, and the rest of the transform kinds `docs/design/pipeline-graph.md` lists.
(`filter`, `rename`, `throttle`, and `dedup` were retired rather than built, per ADR
`routing-by-condition-is-lua`; `sample` came back as a native kind for the keyed, cross-process
consistency Lua can't express, per ADR `consistent-sampling-component`.) Each is a transform-kind
component like `lua` above, and wires into the graph the same way. They're meant to sit in front
of user Lua ("parse the JSON body, then run my logic"), not to replace it: a native transform can
name a Lua component as its source, or the reverse, like any other edge.

## Concurrency

**`mlua::Lua` is neither `Send` nor `Sync`.** This is a hard constraint of the embedded VM, not a
design preference, and it shapes the pipeline's threading model directly:

- Each `lua`/`lua_file` component gets one Lua VM, on its own dedicated OS thread (`logit-{id}`,
  spawned in `crates/logit-pipeline/src/runtime.rs`). Workers don't share VM state.
- A script has **no implicit shared mutable state** across workers: two components running the
  same script see independent Lua globals.
- No host-provided shared store exists. State that must span workers goes through the graph
  instead, for example events fed into a native `aggregate`. Anything added later to share state
  (an enrichment table loaded once and read by every worker, say) must be an **explicit
  host-provided store**: a Rust-side structure the proxy exposes, with its own concurrency
  semantics (for example `dashmap`, or a design sharded so related events reach the same worker).

Reaching for a naively shared `Lua` instance is the easiest way to end up with a design that
can't be parallelized without a rewrite.

## Sandboxing

Each script's VM is built with an explicit `StdLib` allowlist, `TABLE | STRING | MATH`
(`crates/logit-script/src/lib.rs`), instead of trusting the exact composition of
`mlua::Lua::new()`'s "safe" default. That matters for LuaJIT: its `ffi` library is a real sandbox
escape (raw memory access, arbitrary C calls) if enabled, and mlua's docs don't commit to
`Lua::new()` excluding it. There's no `PACKAGE`, so no `require`: scripts transform data and get
no ambient access to the host or to files. Core language functions like `pairs`/`type`/
`tostring` are always available; there's no `BASE` flag to gate them.

**`StdLib` selection alone isn't the whole sandbox, because no `StdLib` flag gates Lua 5.1's base
library.** Review against the real implementation found `loadfile ~= nil` and `dofile ~= nil`
both true in a worker built with only `TABLE | STRING | MATH`, so a script could read and execute
any file this process can read. `remove_unsandboxed_base_globals`
(`crates/logit-script/src/lib.rs`) sets six base globals to `nil` after VM creation:

- `loadfile`/`dofile`: the reproduced file-access issue.
- `load`/`loadstring`: dynamic execution of arbitrary constructed strings. Not file I/O, but it
  undermines "only the configured script source ever runs".
- `getfenv`/`setfenv`: Lua 5.1-specific, and well known as sandbox-escape-adjacent tools for
  tampering with a function's environment.

`crates/logit-script/src/lib.rs`'s tests confirm with real scripts that `os`, `io`, `ffi`,
`require`, `loadfile`, `dofile`, `load`, `loadstring`, `getfenv`, and `setfenv` are all absent:
ten checks, each its own test, so a regression in any one fails on its own.

On top of that base, `logit` adds exactly the globals this document describes: `telemetry`,
`trace`, `provenance`, `resource`, `scope`, and `Event` (the `Event.new` constructor,
"Constructing events" above). Each is a proxy or a table of Rust closures, and none is a route to
the host.

## Costs

| Surface | Where to look |
|---|---|
| `event.attributes`, `event:to_table()` (proxy vs. table conversion) | [`memory.md`](memory.md) §2, §8 |
| `resource`, `scope` (copy-on-write, read vs. write path) | [`memory.md`](memory.md) §2 |
| `event.metrics`, `event.span` (per-access `MetricProxy`, `to_table()` growth) | [`memory.md`](memory.md) §2 |
| `Event.new` (a constructed log event, a constructed gauge event and a constructed span event, each from a literal table; a script that never calls it pays nothing) | [`memory.md`](memory.md) §2 |

`crates/logit-bench/tests/allocations.rs` measures every number for these surfaces. This table
deliberately carries none, so it can't drift when a benchmark changes; see
[`memory.md`](memory.md) §2 for the current figures.
