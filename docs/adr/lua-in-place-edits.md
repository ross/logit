---
created: 2026-10-16
updated: 2026-10-16
---

# Lua in-place edits: writable log fields, `= nil` deletes an attribute, and `pairs` enumerates a proxy

## Status
Accepted

## Context
A script that rewrites a log body or removes attributes has to convert the whole event with
`event:to_table()` and rebuild it with `Event.new(t)`, because three things are missing from the
event proxy:

- `event.log.message`, `severity`, and `body_format` are read-only. [ADR
  `lua-event-constructor`](lua-event-constructor.md) kept them read-only to keep its stack small,
  and [ADR `log-record-trace-context`](log-record-trace-context.md) deferred writing them until a
  concrete need showed up. `examples/prusa/prusa.lua` is that need.
- `event.attributes[k] = nil` stores `Value::Null`, so a key can't be removed. `resource` and
  `scope.attributes` follow the same rule.
- No proxy can be enumerated. `mlua::MetaMethod::Pairs` needs Lua 5.2, and the build uses mlua's
  `luajit` feature.

The [plan](../plans/lua-in-place-edits.md) has the workstreams.

## Decision
A script edits an event in place: the three log fields are writable, assigning `nil` to an
attribute deletes it on all three attribute proxies, and a Lua wrapper over the global `pairs`
enumerates them.

- **Log fields.** `event.log.message`, `severity`, and `body_format` accept what `Event.new`'s
  `log` table accepts, through the same parsers, and refuse with the same message under an
  `event.log` prefix. `severity = nil` clears the field. `body_format = nil` errors, because an
  in-place write is explicit, where `Event.new` defaults an absent `body_format` to `raw`.
  `message = nil` errors, but the no-op check runs first, so writing back a `Null` message (the
  OTLP logs decoder's body-less log) is not an error. The three writes are independent: a
  `message` write doesn't touch `body_format`.
- **`= nil` deletes.** On `event.attributes`, `resource`, and `scope.attributes`, `nil` removes
  the key, and the `nil` check runs before the no-op check. A script can't write `Value::Null`.
  Writing back a `Null` attribute's read value (`nil`) deletes it.
- **`pairs`.** The sandbox installs a Lua `pairs` wrapper into `_G` before the script's chunk
  runs. For the three attribute proxies it returns a stateless Rust iterator keyed by the last key
  seen; for a table it calls the original `pairs`; for other userdata it raises a type error at
  the caller's line. `next(proxy)` and `ipairs(proxy)` still raise.

## Alternatives considered
- **A separate delete function, keeping `= nil` as a `Null` write.** Rejected. A `nil` that
  stores a null sinks render is the trap the prusa example hit: `syslog_out` writes `-` and JSON
  formats write `null` for a key the script meant to remove. `Null` already reads as `nil`, and
  `to_table()` drops it, so no script could tell it from an absent key by reading.
- **Switch mlua to `luajit52`.** Rejected. It turns on `__pairs`, but also other Lua 5.2 behavior
  (`__len` on tables, `load` modes, and others) that the sandbox would need re-reviewing. Without
  it, LuaJIT honors `__pairs` only for cdata.
- **A proxy method such as `attributes:keys()` or `:remove(k)`.** Rejected. mlua resolves methods
  before `__index`, so a method shadows an attribute of the same name. `resource:to_table()`
  already has that problem.
- **A Rust `pairs`.** Rejected. A C or Rust function called at loop entry is not yet implemented
  in LuaJIT's recorder (`recff_nyi`), so it would stop plain-table loops compiling. A Lua wrapper
  returns the builtin `next` for tables, which the loop specialization recognizes.
- **A separate global such as `attributes_of(proxy)`.** Not chosen first, because it is one more
  name to learn for a loop every Lua author already writes. It is the fallback if W3's
  measurement shows the wrapper slows plain-table loops.

## Consequences
- **No `Value::Null` from Lua.** `syslog_out` renders a `Null` `syslog.timestamp` as `-`, and
  the JSON formats render `null`; a script can no longer produce either. No shipped config or
  example does. If a need shows up, a separate global function can write `Null` (not a proxy
  method, which would shadow an attribute); whether to add it is decided then.
- **A read-back write deletes a `Null` attribute.** `a.x = a.x`, or a `pairs` loop that writes
  back `f(v)` where `f(nil)` is `nil`, removes an attribute a decoder stored as `Null`, where the
  no-op rule kept it. The iterator yields such an entry as `(key, nil)`, so a prefix loop still
  finds it. [ADR `lua-value-identity-preservation`](lua-value-identity-preservation.md) gets an
  amendment.
- **A call site that sees a proxy loses the `ITERN` specialization** for good, though it still
  records. A helper shared by tables and proxies pays this.
- **`pairs(resource)` can yield a name that is also a fixed field or method** (`schema_url`,
  `to_table`, `dropped_attributes_count`). Writing `nil` to a field name clears the field or
  raises; a method name has no write-side field, so `resource.to_table = nil` deletes the
  attribute. The design doc says so.
- A script that relied on `= nil` keeping a key present changes output. Logit is pre-release, so
  there is no compatibility path.
- The two earlier ADRs get a dated amendment with the code that lands the writes, not with this
  record.
