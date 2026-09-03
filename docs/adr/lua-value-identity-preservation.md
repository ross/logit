---
created: 2026-08-29
updated: 2026-08-29
---

# Preserving `Value` variant identity across a Lua round-trip

## Status
Accepted

## Context
`crates/logit-script/src/value.rs` maps `logit_core::Value` onto `mlua::Value` for the Lua proxy
(`docs/design/lua-api.md`'s "Exposure: a proxy, not a converted table"). Several distinct `Value`
variants collapse onto the same Lua representation:

- `Value::Bytes` (when its content is valid UTF-8), `Value::Str`, `Value::Timestamp`, and an
  out-of-safe-range `Value::I64`/`Value::U64` are all plain Lua strings — a Lua string has no way
  to carry which one produced it.
- A safe-range `Value::U64` and an integral `Value::F64` (e.g. `42.0`) are both plain Lua
  integers, indistinguishable from a same-valued `Value::I64` — LuaJIT's dual-number mode
  canonicalizes an integral `Number` as an `Integer` before it ever reaches `lua_to_value`.

Left alone, this means an **identity round-trip** — a script reading a value and writing it
straight back unchanged, or the very ordinary "copy every attribute back via
`event:to_table().attributes` while tagging the event with something else" pattern a generic
enrichment stage would use — silently changes the attribute's variant even though its content
never changed. This is a real, already-observed behavioral consequence, not a purity concern:
`logit-outputs::influxdb`'s `value_as_tag_string` includes `Value::Str` attributes as InfluxDB tags
and excludes `Value::Bytes` ones, so a pass-through Lua stage that touches no relevant logic can
silently flip an attribute from "excluded from the write" to "included as a tag."

This was shipped as a documented, deliberate gap in PR #6 (`v0.1-lua-engine`) for the string-branch
half of the problem; a follow-up review comment on that PR
([discussion_r3887008990](https://github.com/ross/logit/pull/6#discussion_r3887008990)) reproduced
the number-branch half (`U64(42)`/`F64(42.0)` both becoming `I64(42)`) and asked that it at least be
documented and regression-tested. This ADR covers fixing both, together. See
[`docs/design/lua-value-type-preservation.md`](../design/lua-value-type-preservation.md) for the
full detailed writeup this decision record summarizes.

## Decision
`AttrsProxy::__newindex` (`crates/logit-script/src/proxy.rs`) treats an assignment as a **no-op**
when the Lua value being assigned is exactly what `value_to_lua` would have produced for that
attribute's *current* content — checked by `value.rs`'s `lua_value_matches`, which compares byte
strings for the string branch and same-value integers/floats for the number branch, without
constructing a new `Value` or calling back into Lua. When it matches, the stored `Value` (and
therefore its variant) is left untouched; when it doesn't, conversion proceeds exactly as before.

This makes the *content* of a value, not its Lua-visible type, the criterion for "did this
assignment actually change anything" — which is the right criterion, since content is the only
thing a script can actually observe or act on through the string/number surface.

## Alternatives considered
- **A tagged/userdata value wrapper** (`TypedValue`, per the original handoff doc) — an
  `mlua::UserData` holding the raw bytes plus an origin-type tag, with `__tostring`/`__concat`/
  `__eq`/`__len` so it still behaves like a string in scripts, unwrapped back to the original
  variant by `lua_to_value`. Rejected after probing the actual `luajit` binary this project embeds
  (`newproxy(true)` with a metatable, not just reasoning about the Lua 5.1 manual): a userdata's
  `__eq` metamethod **never fires when compared against a plain Lua string** — Lua 5.1 only calls
  `__eq` when both operands are tables or both are userdata sharing the same metamethod — so
  `event.attributes.host == "web1"` would silently evaluate to `false` for a wrapped `Bytes`
  attribute compared against a literal string. That's comparison, the single most common thing a
  script does with a string-shaped attribute; the wrapper would trade this ADR's narrow,
  already-scoped gap for a broader, less obvious one. `string.upper`/`string.match`/`string.sub`
  called as free functions (not methods) on the wrapper would also error (`bad argument: string
  expected, got userdata`), and `tonumber()` on it returns `nil` — both fixable only by
  reimplementing significant chunks of Lua's `string` library against the wrapper, which is exactly
  the "large enough addition" the original handoff doc flagged as its main cost, now with a
  correctness regression to show for it rather than just added surface area.
- **Reject assignments that can't round-trip losslessly.** Unworkable as a general answer:
  `lua_to_value` has no way to distinguish "this string is an unmodified round-trip of a `Bytes`
  attribute" from "this is a brand-new string a script is legitimately constructing"
  (`event.attributes.new_field = "hello"` must succeed as `Value::Str`, not be rejected).
- **Track provenance across a whole `process()`/`flush()` call** (remember every string/number a
  script has read, and recognize one reappearing at any key, not just the same one). Would close
  the residual cross-key gap this ADR accepts (see Consequences), at the cost of extra per-call
  state and a fuzzier rule — "matches something read from this event during this call," rather
  than "matches this same attribute's current content." Not pursued: the identified real-world
  failure mode (a generic script iterating and copying attributes back onto themselves) never hits
  the cross-key case, so the added complexity wasn't judged worth it for a gap with no known
  concrete consequence.

## Consequences
- An unmodified `event.attributes.x = event.attributes.x` (or the equivalent read-all/write-back
  pattern via `to_table()`) now preserves `Bytes`, `Timestamp`, out-of-range `I64`/`U64`, safe-range
  `U64`, and `F64` exactly, closing the `logit-outputs::influxdb` tag-inclusion divergence described
  above.
- **Residual, deliberate gap:** copying a value to a *different* key
  (`event.attributes.y = event.attributes.x`) still produces `Value::Str`/`Value::I64` for `y`, not
  the original variant — the rule is keyed on content matching at the same attribute key, not on
  tracking provenance across the whole call (see the rejected alternative above). Regression-tested
  explicitly (`cross_key_copy_of_a_bytes_attribute_is_a_documented_residual_gap`,
  `crates/logit-script/src/lib.rs`) so it stays a documented contract rather than something a future
  change silently breaks or silently "fixes" by accident in a way nobody notices.
- A script that coincidentally assigns byte-identical (or number-identical) content to an
  attribute's existing key keeps that attribute's original variant, even though the script "wrote a
  new value" — defensible because content is what the script actually specified, and the variant is
  the only thing that differs from what was already there.
- **Residual, deliberate gap:** `lua_value_matches` doesn't recurse into `Table`, so a scalar
  variant nested inside an `Array`/`Map` isn't preserved through an identity assignment the way a
  top-level one is — `Array([Bytes(..)])` assigned back to itself becomes `Array([Str(..)])`. The
  container's *shape* round-trips correctly regardless (a real Lua table, not a string; unaffected
  by and predating this decision) — only a nested element's variant is at risk. Closing this would
  mean walking the incoming table to compare nested elements, which can trigger a script-supplied
  `__index` and reenter the proxy while its event is still borrowed — the same
  complexity-vs-value tradeoff that ruled out the userdata wrapper above, and with no concrete
  reported consequence (unlike the top-level case's InfluxDB-tag divergence) to justify it yet.
  Regression-tested (`nested_bytes_in_an_array_is_a_documented_residual_gap`,
  `crates/logit-script/src/lib.rs`) for the same reason as the cross-key gap above.
- No new dependency, no new userdata type, no change to what a script observes when it prints,
  concatenates, or compares an attribute — only to what variant survives an unmodified round-trip.
