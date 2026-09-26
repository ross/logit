# Lua value type preservation

How a [`logit_core::Value`](data-model.md) survives a trip through a Lua script, where it doesn't,
and why. [lua-api.md](lua-api.md) covers the proxy design and script contract in general; this
document covers only the value-conversion boundary in `crates/logit-script/src/value.rs`.

In short: an assignment that writes back exactly what the attribute already reads as is a no-op,
so the stored variant survives. Three narrow gaps remain, each deliberate and tested.

## The problem

Lua has far fewer value types than `Value` has variants (`Null | Bool | I64 | U64 | F64 | Bytes |
Str | Timestamp | Array | Map`, see [data-model.md](data-model.md)). Several variants collapse
onto the same Lua representation on the way into a script.

**The string branch.** A Lua string can't record *why* it's a string:

- `Value::Str`: a string.
- `Value::Bytes` whose content is valid UTF-8. `value_to_lua` hands a script the same
  `LuaValue::String` either way. `Value::Bytes` with invalid UTF-8 is unaffected; see "Known
  residual gaps" below.
- `Value::Timestamp`, in practice always. Lua's only numeric type is an IEEE-754 double, exact for
  integers only up to 2^53 (~9.007e15), and a unix-nanos timestamp is routinely ~1.7e18, nearly
  200x past that. An early version of the proxy exposed timestamps as Lua integers, and a script
  that only read `event.timestamp` and wrote it back unchanged corrupted it (`tostring` showed
  `"1.7e+18"`). A decimal-digit string round-trips exactly.
- `Value::I64`/`Value::U64` outside the same exact-integer range. Unlike `Timestamp`, this is
  conditional: an integer attribute is usually small (`retry_count = 3`), and a real Lua number is
  safe there and more useful to a script (natural comparisons and arithmetic) than a string.
  `crates/logit-script/src/value.rs`'s `exact_i64_to_lua`/`exact_u64_to_lua` check each value
  against the boundary and fall back to a string only when it doesn't fit.

**The number branch.** A Lua *number* can't fully record its origin either:

- A safe-range `Value::U64` becomes `LuaValue::Integer`, the same as a same-valued `Value::I64`. A
  Lua integer has no signed/unsigned tag.
- An integral `Value::F64` (for example `42.0`) is also indistinguishable from a same-valued
  `Value::I64`. LuaJIT's dual-number mode stores an integral Lua number as an integer internally,
  however it was pushed: `LuaValue::Number(42.0)` goes in, and `lua_to_value` sees
  `LuaValue::Integer(42)` come back. A *fractional* `F64` (`42.5`) is unaffected, because it has
  no integer representation.

## Why it matters

The variant changes what a sink writes. `crates/logit-outputs/src/influxdb.rs`'s `tag_value`
treats `Value::Str` and `Value::Bytes` attributes differently: a `Str` becomes an InfluxDB tag,
and a `Bytes` is excluded from the write. So an **identity round-trip** through a script could
silently flip an attribute from excluded to included as a tag, with no error and no signal to the
script. Two ordinary patterns do this: `event.attributes.x = event.attributes.x`, and a generic
enrichment stage that reads every attribute through `event:to_table().attributes` and copies it
back while tagging the event.

## The goal

**A script assignment whose Lua-side content is identical to what the attribute already reads as
must be a no-op.** The stored `Value`, and therefore its variant, stays untouched. An assignment
that changes content converts as usual.

## The rule: content identity, not a tagged value

Before converting an assignment's Lua value into a new `Value`, `AttrsProxy::__newindex`
(`crates/logit-script/src/proxy.rs`) checks whether that Lua value is exactly what `value_to_lua`
would produce for the attribute's *current* content (`value.rs`'s `lua_value_matches`). If it is,
the assignment is a no-op. If not, conversion proceeds.

| existing `Value` | incoming `LuaValue` | matches when |
|---|---|---|
| any | `String(s)` | `lua_string_repr(existing) == Some(s.as_bytes())` — covers `Bytes`, `Str`, `Timestamp`, and out-of-range `I64`/`U64` |
| `I64(i)` | `Integer(n)` | `i == n` (already lossless; included so the rule is uniform) |
| `U64(u)` | `Integer(n)` | in the safe range and `u == n as u64` |
| `Timestamp(t)` | `Integer(n)` | in the safe range and `t == n` |
| `F64(f)` | `Integer(n)` | `f == n as f64` — the number-branch case: LuaJIT hands back an integral `Number` as an `Integer` |
| `F64(f)` | `Number(n)` | `f == n` |
| `Bool(b)` | `Boolean(x)` | `b == x` |
| `Null` | `Nil` | always |
| anything else, including `Table` | — | `false` (see "Known residual gaps") |

`lua_value_matches` is deliberately cheap and self-contained: it never constructs a new `Value`
and never calls back into Lua (no `Table` traversal, no metamethods). It has to be, because
`AttrsProxy::__newindex` runs it while holding a short immutable borrow of the event. If the check
read through Lua, a script-supplied metatable on some *other* table could reenter the same proxy
and panic that borrow. This is also why the check can't recurse into `Table` values; see below.

## Rejected alternative: a tagged userdata wrapper

An earlier proposal wrapped each value in a `TypedValue` `mlua::UserData`: bytes plus an
origin-type tag, with `__tostring`/`__concat`/`__eq` so it behaves like a string in scripts,
unwrapped back to the original variant on the way out.

It was rejected after probing the `luajit` binary this project embeds (`newproxy(true)` with a
metatable, not just the Lua 5.1 manual):

| Expression | With a userdata wrapper |
|---|---|
| `attr == "web1"` | **silently `false`** — Lua 5.1 only calls `__eq` when both operands are userdata sharing the metamethod, never against a plain string |
| `string.upper(attr)`, `string.match(attr, …)` | error: `bad argument #1 (string expected, got userdata)` |
| `tonumber(attr)` | `nil` |
| `seen[attr] = true` | a table key distinct from `seen["web1"]` |
| `..`, `tostring`, `#`, `("%s"):format(attr)`, `attr:sub()` via `__index` | fine |

The `__eq` breakage can't be fixed in Lua 5.1, and it breaks comparison, the most common thing a
script does with a string-shaped attribute. A wrapper would trade this document's narrow gaps for
a far more commonly hit one, and fixing the `string.*`/`tonumber` cases would mean reimplementing
much of Lua's `string` library against the wrapper. See
[ADR `lua-value-identity-preservation`](../adr/lua-value-identity-preservation.md) for the
decision record.

Rejecting non-round-trippable assignments outright was also considered, and doesn't work as a
general answer: `lua_to_value` can't tell an unmodified round-trip of a `Bytes` attribute from a
brand-new string a script is constructing, and `event.attributes.new_field = "hello"` must succeed
as `Value::Str`.

## Known residual gaps

These are deliberate, tested contracts. Each is regression-tested in
`crates/logit-script/src/lib.rs`, so a future change either preserves the documented behavior or
changes it on purpose.

- **Cross-key copies.** `lua_value_matches` compares content at the *same* attribute key. Copying
  a value to a **different** key (`event.attributes.y = event.attributes.x`) still produces a
  `Value::Str`/`Value::I64` for `y`, not the original variant: by then it's a plain Lua
  string/number, indistinguishable from one the script built. Recognizing "same content as some
  other attribute" would mean tracking every string and number a script reads during the call and
  hoping no new value collides with one, which is more machinery than the case is worth.
  (`cross_key_copy_of_a_bytes_attribute_is_a_documented_residual_gap`)

- **Nested container elements.** `lua_value_matches` doesn't recurse into `Table`. An
  `Array`/`Map` already round-trips correctly *as a shape* (a real Lua table, not a string),
  independently of this rule. What isn't preserved is a scalar variant *nested inside* one:
  `Value::Array(vec![Value::Bytes(b"web-01")])` assigned back to itself unchanged comes back as
  `Array([Str(b"web-01")])`. The top-level assignment falls through to a full `lua_to_value`
  reconversion of the table, which has no memory of the nested element's variant. Closing this
  would mean walking the incoming table to compare nested elements while the event's `RefCell`
  is borrowed, where the top-level check only compares one value. Not pursued:
  no concrete consequence of a *nested* variant collapse has been reported (unlike the top-level
  case, which changes InfluxDB tag output), so the complexity isn't justified yet. The same
  trade-off ruled out the userdata wrapper above.
  (`nested_bytes_in_an_array_is_a_documented_residual_gap`)

- **Empty container ambiguity.** `Value::Array(vec![])` and `Value::Map(AttrMap::new())` are both
  Lua's empty table `{}`, with no content difference for an identity check to compare.
  `lua_table_to_value` (`value.rs`) decodes an empty table as `Map`, a documented, tested default:
  attributes are map-shaped and are what scripts mostly manipulate.
  (`empty_table_decodes_as_map_not_array`, `empty_map_stays_a_map`) This gap is independent of the
  identity rule; it's listed here because it's the same family of problem, Lua's value model
  losing origin type, for containers instead of scalars.

## Where this lives

- `crates/logit-script/src/value.rs` — `value_to_lua`/`lua_to_value` (the conversion),
  `lua_value_matches`/`lua_string_repr` (the identity check), `validated_sequence_len`/
  `lua_table_to_value` (the `Array`-vs-`Map` decision, including the empty-table default).
- `crates/logit-script/src/proxy.rs` — `AttrsProxy::__newindex`, where the no-op check is applied.
- `crates/logit-script/src/lib.rs` — the round-trip regression tests, including all three residual
  gaps above.
- `crates/logit-outputs/src/influxdb.rs` — `bytes_attribute_stays_excluded_from_tags_after_a_lua_enrichment_stage`,
  the end-to-end regression for the InfluxDB-tag consequence.
- [`docs/adr/lua-value-identity-preservation.md`](../adr/lua-value-identity-preservation.md) —
  the decision record.
