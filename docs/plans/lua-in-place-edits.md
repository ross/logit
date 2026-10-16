---
created: 2026-10-16
updated: 2026-10-16
---

# Enabling plan: in-place log body, severity, and attribute edits from Lua

## Decisions already settled

| Question | Decision |
|---|---|
| Deletion | `= nil` deletes the key, on `event.attributes`, `resource`, and `scope.attributes`. No separate delete function; one can be added later if a need for writing `Value::Null` shows up. |
| Writing `Value::Null` | Not possible from Lua, a recorded gap (see "`= nil` deletes an attribute"). |
| Identity write of a `Null` attribute | `a.x = a.x` reads `nil` and deletes the key. |
| Enumeration | A Lua `pairs` wrapper installed into `_G` before the script runs, over a stateless Rust iterator. W3 measured it at about 13 ns per `pairs` call on a table and nothing per step, and kept it (ADR's amendment). |

## Goal

A script that rewrites a log body or deletes attributes edits the event in place, without
`event:to_table()` and `Event.new(t)`. The acceptance case is `examples/prusa/prusa.lua`: after this
stream it returns the event it was given, and `crates/logit-cli/tests/prusa_example.rs` passes
unchanged.

Today the script converts every datagram to a table and back (`prusa.lua`, the block ending
`return Event.new(t)`), because:

- `event.log.message`, `severity`, and `body_format` raise `event.log.<key> is read-only for now`
  (`LogProxy::__newindex`, `crates/logit-script/src/proxy.rs`).
- `event.attributes[k] = nil` stores `Value::Null` (`lua_to_value_at`'s `LuaValue::Nil` arm,
  `crates/logit-script/src/value.rs`), so a key can't be removed. `resource` and
  `scope.attributes` follow the same rule.
- No proxy can be enumerated: `mlua::MetaMethod::Pairs` needs Lua 5.2, and the build uses mlua's
  `luajit` feature, not `luajit52`. The script can't find the `syslog.*` keys without
  `to_table()`.

The read-only rule was a deferral, not a design: [ADR `lua-event-constructor`](../adr/lua-event-constructor.md)
says the reason "no longer holds once a constructor can set them", and kept them read-only only
to keep its stack small. [ADR `log-record-trace-context`](../adr/log-record-trace-context.md)
deferred them "once a concrete need for writing them shows up". Prusa is that need.

## Non-goals

- Writing `event.span` fields, adding or removing metrics, or creating a log on an event that has
  none. `Event.new` stays the way to do those; `docs/known-gaps/transforms.md`'s span entry stays.
- Writing `dropped_attributes_count` on any proxy.
- A Lua null sentinel. `Value::Null` stays unrepresentable in Lua, as it is today.
- Switching mlua to `luajit52`. It would give `__pairs`, but also turns on other Lua 5.2 behavior
  (`__len` on tables, `load` modes, and others) that the sandbox would need re-reviewing for.

## Design

### Log field writes

`LogProxy::__newindex` gains three arms, each reusing `crates/logit-script/src/construct.rs`'s
parser for the same field, so a write accepts what `Event.new`'s `log` table accepts and refuses
with the same message (prefixed `event.log` instead of `Event.new: log`), except that
`body_format`'s messages don't offer `nil`:

| Field | Accepts | `nil` | Allocation per write |
|---|---|---|---|
| `message` | any value `value_field` accepts: a string (`Str`, or `Bytes` when not UTF-8), number, boolean, or table | error: a log needs a message, matching `Event.new`'s required `log.message`. The no-op check runs first (below) | one, the `Bytes::copy_from_slice` of a string; zero for a number or boolean |
| `severity` | a `Severity` name (`Severity::from_name`) | clears it (the field is an `Option`) | zero |
| `body_format` | `raw`, `json`, or `structured` (`BodyFormat::from_name`) | error (`optional = false`). This diverges from `Event.new`, where an absent `body_format` defaults to `raw`, because an in-place write is explicit | zero |

A message write doesn't touch `body_format`, and a `body_format` write doesn't reparse the message.
That's the same independence the two fields have in `Event.new`. The doc says so, because a script
that writes a JSON string into a `structured` log gets the string it wrote.

The no-op rule from [ADR `lua-value-identity-preservation`](../adr/lua-value-identity-preservation.md)
applies to `message`: `event.log.message = event.log.message` leaves a `Bytes` message `Bytes` and
allocates nothing. The no-op check runs before the `nil` refusal, because the OTLP logs decoder
stores a missing body as `Value::Null`, which reads as `nil`: reading it back and writing it must
not error on a body-less log. The identity ADR's amendment extends the no-op rule to `message`
explicitly.

W2 refactors `construct.rs`'s `enum_field` and `value_at`, which hardcode the `Event.new: `
prefix in their `format!` calls, to take the prefix (or to split parsing from message building),
so a write refuses with `event.log` in its message.

### `= nil` deletes an attribute

In `AttrsProxy`, `ResourceProxy`, and `ScopeAttrsProxy`'s `__newindex`, a `nil` value calls
`AttrMap::remove(key)` instead of inserting `Value::Null`. The `nil` check comes before the
no-op check in all three handlers, because `value.rs`'s `lua_value_matches` treats `Nil` as
matching `Null`, so `a.x = nil` on a `Null` attribute would otherwise be a no-op and keep the key.
W1 tests that deleting a `Null` attribute removes it. `remove` looks the key up without
interning it and shifts the sorted `SmallVec` down, so it allocates nothing and keeps attribute
order. On `resource` and `scope`, the copy-on-write `ensure_modified` runs only when the key is
present, so deleting an absent key leaves `take_resource`/`take_scope` returning `None` and the
untouched-hook pins don't move.

Why `nil` and not a method:

- `nil` storing `Null` exists for no reason this surface depends on. A `Null` attribute already
  reads as `nil`, `to_table()` already drops it, and `Event.new` can't produce one, so no script can
  tell a `Null` attribute from an absent one except by what a sink renders. No test pins the
  current behavior.
- A method named `remove` (or `keys`) on `event.attributes` would shadow an attribute of that name,
  because mlua resolves methods before `__index`. `resource:to_table()` already has this problem.
- Assigning `nil` to delete is what every Lua programmer expects from a table.

The one documented recipe that relied on the old rule, `docs/http-access-logs.md`'s
"Writing `nil` stores a null, which `http_access` treats as absent and a later `keep` removes",
still works and gets simpler: the key is gone. Its Caddy config has no `keep` stage, so the
recipe only needs the sentence reworded to say the script deletes the key.

What this gives up, recorded in `docs/known-gaps/transforms.md`'s Lua section:

- **A script can't write `Value::Null`.** Two sinks render a `Null` differently from an absent key:
  `syslog_out` writes a `Null` `syslog.timestamp` as RFC 5424's nil `-`, and the JSON formats
  write `null`. A script could produce both today with `= nil`; after this it can't. No shipped
  config or example does. To close: a separate way to write `Null` (a global function, since a
  proxy method would shadow an attribute of its name), if a need shows up.
- **A `Null` attribute doesn't survive a read-back write.** Lua can't tell "write back what I read"
  from "delete", because a `Null` attribute reads as `nil`. So `a.x = a.x`, or
  `for k, v in pairs(a) do a[k] = f(v) end` where `f(nil)` is `nil`, deletes an attribute a decoder
  stored as `Null` (a JSON `null`, a syslog nil timestamp), where today the no-op rule keeps it.
  [ADR `lua-value-identity-preservation`](../adr/lua-value-identity-preservation.md) gets an
  amendment saying so. The `pairs` iterator yields such an entry as `(key, nil)`, so a prefix
  loop like prusa's still finds and deletes it.

### Enumerating a proxy

The sandbox installs a Lua wrapper over `pairs` into `_G` before the script's chunk executes, so
a top-level `local pairs = pairs` in a script captures the wrapper:

```lua
local P = pairs
pairs = function(t)
  if type(t) == "userdata" then
    -- an attribute proxy: return the Rust iterator; any other userdata: error(msg, 2)
  end
  return P(t)
end
```

On an `AttrsProxy`, `ResourceProxy`, or `ScopeAttrsProxy` it returns `(iter, proxy, nil)`; on
anything else it calls the original `pairs`, so a table's loop is unchanged. The wrapper raises its
own type error for any other userdata with `error(msg, 2)`, so the message names the script's line
and not the wrapper; W3 tests that. `next(proxy)` and `ipairs(proxy)` still raise, and the doc says
so. This is Lua 5.2's `__pairs`, done once in the sandbox instead of by the build flag. Scripts
write the ordinary loop:

```lua
for key in pairs(event.attributes) do
  if string.sub(key, 1, 7) == "syslog." then
    event.attributes[key] = nil
  end
end
```

The iterator is stateless: one Rust function created at install, shared by every loop. Each step
receives `(ud, last_key_string)`, resolves the key with `interner::lookup`, and seeks the first
entry with a greater `Symbol`. That makes deleting or overwriting the current key during the loop
safe, as Lua's `next` contract allows for tables, without snapshotting the keys. Adding a key
during the loop is undefined, as it is for a Lua table; the doc says so.

Constraints on the iterator:

- It must not capture the event's `Rc`. `EventProxy::into_inner` does `Rc::try_unwrap` and
  `debug_assert`s that no clone exists, which is why `MetricProxy` holds a `Weak`. Keeping the
  proxy userdata as the loop's state is enough.
- `resource` and `scope` steps re-read modified-else-base on each step, so a write earlier in the
  loop is seen.
- `crates/logit-core/src/attrs.rs` gains a "first entry after `Option<Symbol>`" accessor
  (`partition_point`), which W3 uses for the seek.

Why the wrapper is Lua and not a Rust function: a table's loop keeps its specialized bytecode.
`predict_next` keys on the call-site name `pairs`, and `BC_ISNEXT` checks at run time for the
builtin `next`, a table, and `nil`, which the original `pairs` still returns. With the JIT on, a
Rust `pairs` would also stop plain-table loops compiling, because LuaJIT's recorder treats a C
function called at loop entry as not yet implemented (`recff_c` goes to `recff_nyi` in
`lj_ffrecord.c`). The sandbox runs interpreted, though (W3's measurement below), so today the
specialization is the interpreter's `ITERN` and not a trace.

Caveat, recorded in the doc: a call site that ever sees a proxy despecializes to `ITERC`
permanently for that site, and its plain-table loops go through a generic iterator call. A shared
helper that loops over both tables and proxies pays this.

Fixed-name shadowing: `pairs(resource)` and `pairs(scope.attributes)` can yield an attribute
named like a fixed field or method (`schema_url`, `to_table`, `dropped_attributes_count`), because
`__index` resolves those names first. Reading `resource[k]` for such a key returns the field, and
on `schema_url` the write clears the field, and on `dropped_attributes_count` it raises. A method
name such as `to_table` has no write-side field, so `resource.to_table = nil` deletes an attribute
of that name while reads still return the method. The doc says so.

W3 measured the wrapper (ADR `lua-in-place-edits`'s amendment has the table). The sandbox opens
no `jit` library, so it runs LuaJIT interpreted. There, a plain-table loop's steps cost the same
with the wrapper, each `pairs` call on a table costs about 13 ns more, and a call site that has
seen a proxy runs plain-table loops about 60% slower. The wrapper stays. The fallback, a separate
global such as `attributes_of(proxy)`, wasn't taken: it leaves `pairs` alone but is one more name a
script author has to learn, for a loop every Lua author already writes.

### `prusa.lua` after

```lua
  for key in pairs(event.attributes) do
    if string.sub(key, 1, 7) == "syslog." then
      event.attributes[key] = nil
    end
  end
  event.attributes.mac_address = mac
  event.log.message = table.concat(lines, "\n")
  resource["_logit.influxdb.bucket"] = BUCKET
  return event
```

### Allocation pins

The counting allocator sees Rust allocations only. Lua strings allocate in LuaJIT's heap, so a
key or value pushed to Lua is invisible to it. Every existing `lua:` pin in
`crates/logit-bench/tests/allocations.rs` and `docs/design/memory.md` §2 stays: none of their scripts write a log field or assign `nil`, and the
new arms format strings only on the error path. New rows, additive like the `Event.new` rows:

| Pin | Script | Expected |
|---|---|---|
| writing `event.log.message` | `event.log.message = "rewritten"` | the `LogProxy` creation plus one `Bytes` |
| writing `event.log.severity` | `event.log.severity = "error"` | the `LogProxy` creation, nothing for the write |
| deleting an attribute | `event.attributes.host = nil` | the `AttrsProxy` baseline, nothing for the delete |
| iterating attributes | `for k, v in pairs(event.attributes) do end` | measured at 8, the `AttrsProxy` baseline: no step allocates, with 12 more attributes too |

The counts are measured, not derived; each PR sets them and the `memory.md` row in the same commit.

## Docs

One copy of each rule, in `docs/design/lua-api.md`, which every other mention points at:

- `docs/design/lua-value-type-preservation.md`: its statement about what `to_table().attributes`
  enumerates, which a `pairs` loop now also does.
- `docs/design/lua-api.md`: the "Typed record access" bullet that lists `event.log`'s read-only
  fields, the surface table, the "No `__pairs`" section (rewritten around the
  `pairs` wrapper), the "Reading and writing `resource`" section's "Assigning `nil` stores a null value" and `schema_url`'s
  "exception" wording, the `event.log` table and its "read-only for now" paragraph, and the
  `to_table()`/`Event.new` rewrite example, which becomes the in-place form.
- `docs/http-access-logs.md`: the `nil` recipe, reworded to say the script deletes the key. Its Caddy config has no `keep` stage.
- `docs/known-gaps/transforms.md`: drop the log-fields clause from the span entry, and add the two
  `Null` entries above.
- `docs/adr/lua-event-constructor.md`, `docs/adr/log-record-trace-context.md`, and
  `docs/adr/lua-value-identity-preservation.md`: a dated amendment each, pointing at the new ADR.
- `examples/prusa/prusa.lua`: the comment above the rebuilt block goes with the block.

## Workstreams

Stream key **`lmut`**: branches `lmut/w0`…`lmut/w4`, a linear stack, each PR based on and targeting
its parent's branch, brought up to date with `git merge origin/main`.

| # | PR | Files | Depends |
|---|---|---|---|
| W0 | ADR `lua-in-place-edits` and this plan, with README rows. Docs only | `docs/adr/`, `docs/plans/` | — |
| W1 | `= nil` deletes, on all three attribute proxies; `http-access-logs.md`; identity-ADR amendment; the two `Null` known-gaps entries | `proxy.rs`, `resource.rs`, `scope.rs`, `value.rs` | W0 |
| W2 | `message`, `severity`, `body_format` writes through `construct.rs`'s parsers, which take an error prefix; `lua-api.md` rewrite example; two ADR amendments | `proxy.rs`, `construct.rs` | W1 |
| W3 | the `pairs` wrapper and its iterator; the JIT check | sandbox setup, `proxy.rs`, `resource.rs`, `scope.rs`, `crates/logit-core/src/attrs.rs` | W2 |
| W4 | `prusa.lua` in place; known-gaps; AGENTS.md's `lua` row; plan status | examples, docs | W3 |

Each code PR adds its pins and its `memory.md` rows, its `lua-api.md` section, and tests in
`crates/logit-script` for every arm, including each refusal's exact message. Each regression test
is run with its change reverted to show it fails, and the PR body says so. Each code PR gets
`pr-review --comment` before its fix rounds.

**Done when:** `prusa.lua` has no `to_table()` or `Event.new`, `prusa_example.rs` passes with no
edit, every pre-existing `lua:` pin is unchanged, and `script/cibuild` passes.

### Status (2026-10-16)

All five workstreams are built as a linear stack of PRs, each targeting its parent: W0 #727, W1 #728,
W2 #729, W3 #730, and W4 (this closeout) on `lmut/w4` targeting `lmut/w3`. `examples/prusa/prusa.lua`
edits its event in place and `crates/logit-cli/tests/prusa_example.rs` passes unchanged.

## Open risks

- The `pairs` wrapper's cost, measured in W3: about 13 ns per `pairs` call on a table, and a call
  site that has seen a proxy runs plain-table loops about 60% slower for good. Accepted;
  `docs/design/lua-api.md` tells a script author not to share one hot loop between the two.
- A script that relied on `= nil` keeping a key present, for a sink that renders `Null`, changes
  output. No shipped config or example does; logit is pre-release, so there's no compatibility
  path.
