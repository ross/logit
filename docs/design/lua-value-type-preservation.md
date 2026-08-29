# Lua value type preservation

How [`logit_core::Value`](data-model.md) survives (and doesn't quite always survive) a trip through
a Lua script, and why. Companion to [lua-api.md](lua-api.md) — that document covers the proxy
design and script contract generally; this one is specifically about the value-conversion boundary
in `crates/logit-script/src/value.rs`, which turned out to have more nuance than it looks like.

## The problem

Lua has far fewer value types than `Value` has variants (`Null | Bool | I64 | U64 | F64 | Bytes |
Str | Timestamp | Array | Map`, see [data-model.md](data-model.md)). Several distinct variants
collapse onto the same Lua representation on the way into a script:

**The string branch.** A Lua string can't distinguish *why* it's a string:

- `Value::Str` — obviously a string.
- `Value::Bytes`, when its content happens to be valid UTF-8. `value_to_lua` hands a script the
  same `LuaValue::String` either way; `Value::Bytes` with genuinely invalid UTF-8 is unaffected —
  see "Known residual gaps" below.
- `Value::Timestamp` — always, in practice. Lua's only numeric type is an IEEE-754 double, exact
  for integers only up to 2^53 (~9.007e15); a unix-nanos timestamp is routinely ~1.7e18, nearly
  200x past that. An early version of the proxy exposed timestamps as Lua integers, and a script
  that did nothing but read `event.timestamp` and write it back unchanged already came back wrong
  (`tostring` showed `"1.7e+18"`). A decimal-digit string round-trips exactly.
- `Value::I64`/`Value::U64` outside that same exact-integer range. Unlike `Timestamp`, this is
  conditional: an ordinary integer attribute is usually small (`retry_count = 3`), where a real Lua
  number is both safe and far more useful to a script (natural comparisons/arithmetic) than a
  string would be. `crates/logit-script/src/value.rs`'s `exact_i64_to_lua`/`exact_u64_to_lua` check
  each value individually against the boundary and only fall back to a string when it doesn't fit.

**The number branch.** Less obviously, a Lua *number* can't fully distinguish its origin either:

- A safe-range `Value::U64` becomes `LuaValue::Integer` — same as a same-valued `Value::I64` would.
  A Lua integer has no signed/unsigned tag to preserve.
- An integral `Value::F64` (e.g. `42.0`) *also* becomes indistinguishable from a same-valued
  `Value::I64`: LuaJIT's dual-number mode canonicalizes an integral Lua number as an integer
  internally, regardless of how it was originally pushed (`LuaValue::Number(42.0)` in, but
  `lua_to_value` sees `LuaValue::Integer(42)` coming back). A *fractional* `F64` (`42.5`) is
  unaffected — it has no integer representation to be canonicalized into.

## Why it matters

Not a purity concern: `crates/logit-outputs/src/influxdb.rs`'s `value_as_tag_string` treats
`Value::Str` and `Value::Bytes` attributes differently (the former becomes an InfluxDB tag, the
latter is excluded from the write). So an **identity round-trip** through a script —
`event.attributes.x = event.attributes.x`, or the very ordinary "read every attribute via
`event:to_table().attributes` and copy it back while tagging the event with something else" pattern
a generic enrichment stage would use — could silently flip an attribute from "excluded from the
write" to "included as a tag," with no error and no script-visible signal that anything changed.

## The goal

**A script assignment whose Lua-side content is identical to what the attribute already reads as
must be a no-op.** The stored `Value` — and therefore its variant — is left untouched. Anything
that genuinely changes content converts exactly as it always has.

## The rule: content identity, not a tagged value

`AttrsProxy::__newindex` (`crates/logit-script/src/proxy.rs`) checks, before converting an
assignment's Lua value into a new `Value`, whether that Lua value is exactly what `value_to_lua`
would have produced for the attribute's *current* content — `value.rs`'s `lua_value_matches`. If
so, the assignment is a no-op. If not, conversion proceeds as before.

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

`lua_value_matches` is deliberately cheap and self-contained: it never constructs a new `Value` and
never calls back into Lua (no `Table` traversal, no metamethods). `AttrsProxy::__newindex` runs it
while still holding a short immutable borrow of the event — a script-supplied metatable on some
*other* table could reenter the same proxy if the check ever needed to read through Lua, which
would panic that borrow. This is also why the check can't simply recurse into `Table` values; see
below.

## Rejected alternative: a tagged userdata wrapper

An earlier version of this design (the handoff note this document replaces) proposed a `TypedValue`
`mlua::UserData` wrapper — bytes plus an origin-type tag, with `__tostring`/`__concat`/`__eq` so it
still behaves like a string in scripts, unwrapped back to the original variant on the way out.

Rejected after probing the actual `luajit` binary this project embeds (`newproxy(true)` with a
metatable, not just reading the Lua 5.1 manual):

| Expression | With a userdata wrapper |
|---|---|
| `attr == "web1"` | **silently `false`** — Lua 5.1 only calls `__eq` when both operands are userdata sharing the metamethod, never against a plain string |
| `string.upper(attr)`, `string.match(attr, …)` | error: `bad argument #1 (string expected, got userdata)` |
| `tonumber(attr)` | `nil` |
| `seen[attr] = true` | a table key distinct from `seen["web1"]` |
| `..`, `tostring`, `#`, `("%s"):format(attr)`, `attr:sub()` via `__index` | fine |

The `__eq` breakage is unfixable in Lua 5.1 and hits comparison — the single most common thing a
script does with a string-shaped attribute. A wrapper would trade this document's narrow,
already-scoped gaps for a much more commonly hit one, and fixing the `string.*`/`tonumber` cases
would mean reimplementing significant chunks of Lua's `string` library against the wrapper. See
[ADR 0007](../adr/0007-lua-value-identity-preservation.md) for the decision record.

"Reject non-round-trippable assignments" was also considered and is unworkable as a general
answer: `lua_to_value` has no way to distinguish "this string is an unmodified round-trip of a
`Bytes` attribute" from "this is a brand-new string a script is legitimately constructing"
(`event.attributes.new_field = "hello"` must succeed as `Value::Str`, not be rejected).

## Known residual gaps

These are deliberate, tested contracts — not oversights. Each is regression-tested in
`crates/logit-script/src/lib.rs` specifically so a future change either preserves the documented
behavior or updates it on purpose.

- **Cross-key copies.** `lua_value_matches` is keyed on content matching at the *same* attribute
  key. Copying a value to a **different** key (`event.attributes.y = event.attributes.x`) still
  produces a `Value::Str`/`Value::I64` for `y`, not the original variant — by the time it's
  assigned to `y`, it's just a plain Lua string/number like any other, indistinguishable from one a
  script constructed from scratch. Recognizing "this is the same content as some other attribute,
  elsewhere" would mean tracking every string/number a script reads during the call and hoping no
  ordinary new value collides with one — more machinery than the case is worth.
  (`cross_key_copy_of_a_bytes_attribute_is_a_documented_residual_gap`)

- **Nested container elements.** `lua_value_matches` doesn't recurse into `Table`. An `Array`/`Map`
  already round-trips correctly *as a shape* (a real Lua table, not a string) — that part predates
  this fix and isn't affected by it. What isn't preserved is a scalar variant *nested inside* one:
  `Value::Array(vec![Value::Bytes(b"web-01")])`, assigned back to itself unchanged, comes back as
  `Array([Str(b"web-01")])`, because the top-level assignment falls through to a full
  `lua_to_value` reconversion of the whole table, and that reconversion has no memory of what the
  nested element used to be. Closing this would mean walking the incoming table element-by-element
  to compare against the existing nested values — and that walk (`Table::get`/`pairs`) can trigger
  a script-supplied `__index` and reenter this same proxy, so it can't run while the event's
  `RefCell` is still borrowed the way the top-level check does. Not pursued: no concrete reported
  consequence has been found for a *nested* variant collapse (unlike the top-level case, which
  concretely changes InfluxDB tag output), so the added complexity wasn't judged worth it yet — the
  same complexity-vs-value tradeoff that ruled out the userdata wrapper above.
  (`nested_bytes_in_an_array_is_a_documented_residual_gap`)

- **Empty container ambiguity.** `Value::Array(vec![])` and `Value::Map(AttrMap::new())` are both
  just Lua's empty table `{}`, with nothing to distinguish them — there's no content difference for
  an identity check to even compare. `lua_table_to_value` (`value.rs`) picks one documented, tested
  default (an empty table decodes as `Map`) rather than solving the unsolvable: attributes are
  map-shaped and are the primary thing scripts manipulate, so that's the more natural default for
  the common case. (`empty_table_decodes_as_map_not_array`, `empty_map_stays_a_map`) This one
  predates and is independent of the identity-preservation work above — included here because it's
  the same family of "Lua's value model can't carry origin type" problem, just for containers
  instead of scalars.

## Where this lives

- `crates/logit-script/src/value.rs` — `value_to_lua`/`lua_to_value` (the conversion),
  `lua_value_matches`/`lua_string_repr` (the identity check), `validated_sequence_len`/
  `lua_table_to_value` (the `Array`-vs-`Map` decision, including the empty-table default).
- `crates/logit-script/src/proxy.rs` — `AttrsProxy::__newindex`, where the no-op check is applied.
- `crates/logit-script/src/lib.rs` — the round-trip regression tests, including all three residual
  gaps above.
- `crates/logit-outputs/src/influxdb.rs` — `bytes_attribute_stays_excluded_from_tags_after_a_lua_enrichment_stage`,
  the end-to-end regression closing the loop on the concrete InfluxDB-tag consequence.
- [`docs/adr/0007-lua-value-identity-preservation.md`](../adr/0007-lua-value-identity-preservation.md) —
  the decision record: same material, framed as Status/Context/Decision/Alternatives/Consequences.
