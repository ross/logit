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

Instead, `Event` is exposed as **`mlua` userdata with `__index`/`__newindex`/`__pairs` metamethods**
that read through to the underlying Rust event lazily, and copy-on-write only the fields a script
actually assigns to:

```lua
function process(event)
  event.attributes.env = "prod"     -- __newindex on attributes: writes through, nothing else copied
  local host = event.attributes.host -- __index: reads through, no allocation
  return event
end
```

`event:to_table()` is provided as an explicit escape hatch for scripts that want a real Lua table
(to build a new structure, log for debugging, etc.) — the cost is opt-in and visible at the call
site rather than paid unconditionally.

**Before the API is frozen**, benchmark the proxy against plain table conversion with `criterion`
on realistic scripts (a field rename, a JSON-body-into-attributes flatten, a full-event copy) and
record the numbers here. The design commits to the proxy; the benchmark is to confirm the expected
win rather than to leave the choice open.

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

Each script's VM is created with a restricted standard library (no `io`, `os.execute`, or arbitrary
`require`) — scripts transform data, they don't get ambient access to the host. Exact allowlist
(likely `string`, `table`, `math`, a `json`/`logfmt` helper module, and the event proxy itself) is an
implementation detail to nail down when `logit-script` is built, not a v1-blocking design question.
