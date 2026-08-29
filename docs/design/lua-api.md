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
`event.type` (a read-only string: `"log"`/`"metric"`/`"span"`) and `event:clone()` (an independent
deep copy, needed for fan-out — see the script contract below) round out the proxy's surface for
now. Deliberately not exposed yet: typed access to payload fields (a metric's value, a log's
message, ...) and any `Event.new(...)`-style constructor — real API surface that deserves its own
design pass once a concrete consumer (the built-in `aggregate` processor is the obvious one) needs
it, rather than being guessed at ahead of that.

No `__pairs`: it isn't available under LuaJIT. `mlua::MetaMethod::Pairs` requires Lua 5.2+, and
LuaJIT is Lua 5.1 semantics — this was in the original version of this section and is wrong.
`event:to_table()` is the answer instead: a real, disconnected Lua table with `timestamp`,
`attributes`, and `type`, for anything the proxy doesn't expose directly — including full
attribute iteration (`for k, v in pairs(event:to_table().attributes) do ... end`, native `pairs()`
on a real table) and building new structures or logging for debugging. The cost is opt-in and
visible at the call site rather than paid unconditionally.

One more thing the proxy design ran into once actually implemented: **`event.timestamp` is a Lua
*string*, not a Lua number.** Lua's only numeric type is an IEEE-754 double, exact only up to 2^53
(~9e15) — and a unix-nanos timestamp is routinely ~1.7e18, nearly 200x past that. This was
verified empirically, not just reasoned about: an early version exposed it as a Lua integer, and a
script that did nothing but read `event.timestamp` and write it back unchanged already came back
wrong (`tostring` showed `"1.7e+18"`). A decimal-digit string round-trips exactly; a script that
needs real arithmetic on it can `tonumber()` at whatever precision it actually needs (millisecond
granularity, for instance, comfortably fits a Lua number). The same reasoning applies to
`Value::Timestamp` generally (`crates/logit-script/src/value.rs`), not just `Event::timestamp`.

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

## Config shape

Lua can be inline in YAML (block scalar) or referenced from a file:

```yaml
pipelines:
  app_metrics:
    inputs: [statsd_in]
    transforms:
      - builtin: aggregate
        interval: 10s
      - lua: |
          function process(event)
            event.attributes.env = event.attributes.env or "unknown"
            return event
          end
      - lua_file: ./scripts/enrich.lua
    outputs: [influx_out]
```

Built-in native processors (no Lua involved) handle the common structured-parsing cases without
per-event VM overhead: `json`, `logfmt`, `kv`, `regex`/`grok`, `csv`, `rename`/`remove`/`copy`,
`filter`, `sample`, `throttle`, `dedup`, `aggregate`. These are meant to sit in front of user Lua in
a chain — "parse the JSON body, then run my logic" — rather than being an either/or with scripting.

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
flag to include.) Verified with real scripts, not just configured and assumed: `os`, `io`, `ffi`,
and `require` are all confirmed absent (`crates/logit-script/src/lib.rs`'s tests).
