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
value.** A plain Lua string or number genuinely can't carry which `Value` variant it came from —
`Str`, `Bytes` (when its content happens to be valid UTF-8), `Timestamp`, or an out-of-range
`I64`/`U64` are all indistinguishable Lua strings; a safe-range `U64` and, thanks to LuaJIT's
dual-number mode canonicalizing an integral `Number` to an `Integer`, an integral `F64`, are both
indistinguishable Lua integers from a same-valued `I64`. Left alone, an identity round-trip through
a script (`event.attributes.x = event.attributes.x`, or the very ordinary "read every attribute via
`to_table()` and copy it back while tagging the event with something else") would silently change a
value's variant even though its content never changed — a real behavioral consequence, since
`logit-outputs::influxdb`'s tag handling treats `Bytes` and `Str` differently.

The fix lives in `AttrsProxy::__newindex` (`crates/logit-script/src/proxy.rs`): before converting
an assignment's Lua value into a new `Value`, it checks whether that Lua value is byte-for-byte (or
number-for-number) what `value_to_lua` would have produced for the attribute's *current* content
(`value.rs`'s `lua_value_matches`). If so, the assignment is a no-op — the stored `Value`, variant
included, is left untouched. Anything that changes content still converts exactly as before.

A tagged/userdata value wrapper (an `mlua::UserData` carrying bytes plus an origin tag) was
considered instead and rejected: probing LuaJIT directly showed a userdata's `__eq` metamethod
never fires when compared against a plain Lua string under Lua 5.1 semantics (both operands must be
userdata sharing the metamethod), so `event.attributes.host == "web1"` would silently become
`false` for a wrapped value — trading this narrow, already-scoped gap for a much more commonly hit
one. See `docs/adr/0007-lua-value-identity-preservation.md` for the full comparison.

This rule is deliberately keyed on content matching at the *same* attribute key, not on tracking a
value's provenance across the whole script call — copying a value to a **different** key
(`event.attributes.y = event.attributes.x`) still produces a `Value::Str`/`Value::I64`, same as
before this fix. A script doing that is constructing what is, from Lua's perspective, a brand new
value; recognizing it as "the same content as some other attribute, elsewhere" would need to track
every string/number a script has read during the call and hope no ordinary new value collides with
one, which costs more than the case is worth.

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

**An event handle is consumed once it's returned from `process()` or included in a `flush()`
table** — don't keep using a Lua variable referencing an event after handing it back that way. A
Lua userdata is a reference type, so a variable a script stashed elsewhere (`pending = event`) can
be the *exact same* underlying object as the one returned, not an independent copy; extracting the
returned one invalidates every other reference to it, and using a stashed alias afterward is a
clear error, not silently wrong data. If a script genuinely needs to both emit an event now and
keep something for later (a stateful `flush()` re-emitting it, say), stash `event:clone()` — an
independent copy — instead of `event` itself.

`return {a, b}` must be a proper array-like table (keys exactly `1..=n`, matching Lua's own
notion of a sequence) — a malformed table (non-contiguous keys) is a clear error, not a silently
incomplete or empty result.

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
