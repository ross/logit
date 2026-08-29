//! Conversion between `logit_core::Value` and `mlua::Value`.
//!
//! Shared by [`crate::proxy`] (reading/writing individual attributes, and `to_table()`'s full
//! snapshot) -- one definition of this mapping, not two ad hoc conversions drifting apart.

use bytes::Bytes;
use logit_core::interner::resolve;
use logit_core::{AttrMap, Value};
use mlua::{Lua, Table, Value as LuaValue};
use std::borrow::Cow;

/// Converts an internal `Value` into an `mlua::Value` for handing to a script.
pub fn value_to_lua<'lua>(lua: &'lua Lua, value: &Value) -> mlua::Result<LuaValue<'lua>> {
    Ok(match value {
        Value::Null => LuaValue::Nil,
        Value::Bool(b) => LuaValue::Boolean(*b),
        // A Lua number is an IEEE-754 double, exact only for integers up to 2^53 (~9e15) --
        // confirmed the hard way, not just reasoned about: an earlier version of this function
        // always used LuaValue::Integer for I64/Timestamp, and a review reproduced
        // 9_007_199_254_740_993 (one past 2^53) silently becoming ..._992 after nothing more than
        // an identity assignment (`event.attributes.x = event.attributes.x`) -- and separately,
        // U64(u64::MAX) wrapping negative through the `as mlua::Integer` cast that used to live
        // here. `exact_i64_to_lua`/`exact_u64_to_lua` below check per-value against the actual
        // exact-integer boundary (see [`i64_is_exact_lua_number`]'s doc comment for why that's a
        // magnitude check, not a round-trip cast -- the obvious round-trip check has its own bug
        // near `u64::MAX`) and fall back to a decimal string when a value doesn't fit. This is
        // conditional rather than the blanket string `Timestamp` uses because ordinary I64/U64
        // attributes are usually small (`retry_count = 3`), where a real Lua number is both safe
        // and far more useful to a script (natural comparisons/arithmetic) than a string would
        // be; a `Timestamp` is *always* large enough in practice to take the string branch
        // anyway, so it shares this same exact-round-trip logic rather than a separately
        // maintained rule.
        //
        // A value that takes the string branch here (any I64/U64/Timestamp outside the safe
        // range, every Bytes/Str) would be indistinguishable from an ordinary Value::Str once
        // inside Lua -- a plain Lua string has no way to carry which of those it came from, and
        // the same is true of LuaJIT's dual-number mode collapsing an integral F64/U64 onto
        // LuaValue::Integer (see [`lua_value_matches`]'s F64 arm). `AttrsProxy::__newindex`
        // (proxy.rs) is what actually closes this gap: an assignment whose Lua-side content is
        // byte-for-byte what this function would have produced for the attribute's *current*
        // value is treated as a no-op, so the stored `Value` -- and its variant -- survives an
        // unmodified round-trip even though nothing here can tell the difference between "this
        // string came from a Bytes attribute" and "this is a brand-new string a script just
        // built". See `lua_value_matches` below and `docs/adr/0007-lua-value-identity-preservation.md`
        // for the full reasoning, including why a tagged userdata wrapper was considered and
        // rejected.
        //
        // Not covered by that fix, and not attempted here: two empty containers, `Value::Array
        // (vec![])` and `Value::Map(AttrMap::new())`, both reach Lua as the same empty table --
        // `lua_table_to_value` below picks a documented default (`Map`) rather than solving what's
        // genuinely unsolvable without tagging containers too.
        Value::I64(i) => exact_i64_to_lua(lua, *i)?,
        Value::U64(u) => exact_u64_to_lua(lua, *u)?,
        Value::F64(f) => LuaValue::Number(*f),
        Value::Bytes(b) => LuaValue::String(lua.create_string(b)?),
        Value::Str(s) => LuaValue::String(lua.create_string(s)?),
        Value::Timestamp(t) => exact_i64_to_lua(lua, *t)?,
        Value::Array(items) => {
            let table = lua.create_table()?;
            for (i, item) in items.iter().enumerate() {
                table.set(i + 1, value_to_lua(lua, item)?)?;
            }
            LuaValue::Table(table)
        }
        Value::Map(map) => LuaValue::Table(attrmap_to_lua_table(lua, map)?),
    })
}

/// The largest magnitude `i64` an IEEE-754 double (Lua's only numeric type) can represent
/// exactly.
///
/// A magnitude check against this constant, not a `(v as f64) as $int == v` round-trip check:
/// that was the first thing tried here, and it has a real bug for values near `u64::MAX`. Rust's
/// float-to-int `as` casts saturate rather than wrap (since 1.45), so `(u64::MAX as f64) as u64`
/// rounds up to 2^64 as an f64 and then *saturates back down* to exactly `u64::MAX` on the cast
/// back -- the round trip "succeeds" despite real precision loss in between, because saturation
/// happens to land back on the original value. A direct magnitude comparison against the actual
/// exact-integer boundary has no such edge case.
const MAX_EXACT_F64_INT: i64 = 1 << 53; // 9_007_199_254_740_992

/// Whether `value_to_lua` represents this `i64` as a real `LuaValue::Integer` (`true`) or falls
/// back to a decimal string (`false`). Shared by `exact_i64_to_lua` (which produces that
/// representation) and `lua_value_matches` (which needs to recognize it without producing a new
/// `LuaValue` just to compare), so the boundary is defined in exactly one place.
fn i64_is_exact_lua_number(i: i64) -> bool {
    (-MAX_EXACT_F64_INT..=MAX_EXACT_F64_INT).contains(&i)
}

/// As [`i64_is_exact_lua_number`], for `u64`.
fn u64_is_exact_lua_number(u: u64) -> bool {
    u <= MAX_EXACT_F64_INT as u64
}

fn exact_i64_to_lua(lua: &Lua, i: i64) -> mlua::Result<LuaValue<'_>> {
    if i64_is_exact_lua_number(i) {
        Ok(LuaValue::Integer(i))
    } else {
        Ok(LuaValue::String(lua.create_string(i.to_string())?))
    }
}

/// As [`exact_i64_to_lua`], for `u64`. A `u64` within the safe range is always well within `i64`'s
/// range too (2^53 is far below `i64::MAX`), so the `as i64` cast on the safe branch never
/// truncates.
fn exact_u64_to_lua(lua: &Lua, u: u64) -> mlua::Result<LuaValue<'_>> {
    if u64_is_exact_lua_number(u) {
        Ok(LuaValue::Integer(u as mlua::Integer))
    } else {
        Ok(LuaValue::String(lua.create_string(u.to_string())?))
    }
}

/// The exact bytes [`value_to_lua`] hands a script for a value that takes its string branch, or
/// `None` for a value that reaches Lua as something other than a string (e.g. a small integer,
/// which becomes a real Lua number instead). Shared with `value_to_lua` only in spirit -- kept as
/// a single definition here so the string-branch boundary can't drift between the two -- and used
/// directly by [`lua_value_matches`] to recognize an unmodified round-trip through that branch.
fn lua_string_repr(value: &Value) -> Option<Cow<'_, [u8]>> {
    match value {
        Value::Bytes(b) | Value::Str(b) => Some(Cow::Borrowed(b.as_ref())),
        Value::I64(i) if !i64_is_exact_lua_number(*i) => {
            Some(Cow::Owned(i.to_string().into_bytes()))
        }
        Value::U64(u) if !u64_is_exact_lua_number(*u) => {
            Some(Cow::Owned(u.to_string().into_bytes()))
        }
        Value::Timestamp(t) if !i64_is_exact_lua_number(*t) => {
            Some(Cow::Owned(t.to_string().into_bytes()))
        }
        _ => None,
    }
}

/// Whether `new` is exactly what [`value_to_lua`] would have produced for `existing` -- i.e. an
/// assignment carrying `new` back into the attribute `existing` came from doesn't change its
/// content, only (absent this check) its variant. [`crate::proxy::AttrsProxy`]'s `__newindex`
/// uses this to make such an assignment a no-op, so the original `Value` variant survives an
/// unmodified round-trip through a script -- see the long comment on `value_to_lua`'s match arms
/// for why a plain string/number can't carry that information on its own.
///
/// Deliberately shallow: doesn't recurse into `Table`. An `Array`/`Map` already round-trips
/// correctly *as a shape* (a real Lua table, not a string -- see `lua_to_value`'s doc comment),
/// but a scalar variant *nested inside* one is not preserved through an identity assignment the
/// way a top-level one is -- `Array([Bytes(..)])` assigned back to itself becomes
/// `Array([Str(..)])`, because the top-level `Table` case here always falls through to a full
/// `lua_to_value` reconversion with no memory of what the nested elements used to be. This is a
/// deliberate, documented, and tested scope limit
/// (`docs/design/lua-value-type-preservation.md`'s "Known residual gaps"), not an oversight:
/// closing it would mean walking the incoming table to compare nested elements, and that walk
/// (`Table::get`/`pairs`) can trigger a script-supplied `__index` and reenter this same proxy --
/// it can't run while the event's `RefCell` is still borrowed the way this check is (see
/// `AttrsProxy::__newindex`'s comment), and no concrete reported consequence has justified the
/// restructuring that would take yet, the same complexity-vs-value tradeoff that ruled out a
/// tagged userdata wrapper for the top-level case (ADR 0007).
pub(crate) fn lua_value_matches(existing: &Value, new: &LuaValue) -> bool {
    match new {
        LuaValue::Nil => matches!(existing, Value::Null),
        LuaValue::Boolean(b) => matches!(existing, Value::Bool(e) if e == b),
        LuaValue::Integer(n) => match existing {
            Value::I64(i) => i == n,
            Value::U64(u) => u64_is_exact_lua_number(*u) && *u == *n as u64,
            Value::Timestamp(t) => i64_is_exact_lua_number(*t) && t == n,
            // LuaJIT's dual-number mode canonicalizes an integral Number as an Integer, so an
            // F64 that started exact-integral (e.g. 42.0) comes back through this arm, not
            // LuaValue::Number's -- reproduced via PR #6 review discussion_r3887008990
            // (`Value::F64(42.0)` silently becoming `Value::I64(42)` after an identity
            // assignment).
            Value::F64(f) => *f == *n as f64,
            _ => false,
        },
        LuaValue::Number(n) => matches!(existing, Value::F64(f) if f == n),
        LuaValue::String(s) => lua_string_repr(existing).as_deref() == Some(s.as_bytes()),
        _ => false,
    }
}

/// Converts an [`AttrMap`] into a plain Lua table -- used by `to_table()`, and by the `Value::Map`
/// case above for a nested map inside some other value.
pub fn attrmap_to_lua_table<'lua>(lua: &'lua Lua, map: &AttrMap) -> mlua::Result<Table<'lua>> {
    let table = lua.create_table()?;
    for (key, value) in map.iter() {
        table.set(resolve(key), value_to_lua(lua, value)?)?;
    }
    Ok(table)
}

/// Converts a script-provided `mlua::Value` into an internal `Value`, for `__newindex`
/// assignments (`event.attributes.foo = <lua value>`). A table is treated as an `Array` if its
/// keys are exactly the contiguous sequence `1..=n` (Lua's own notion of a "sequence" table, per
/// the `#` operator), and as a `Map` otherwise -- a reasonable default given Lua doesn't
/// distinguish the two at the language level, and scripts overwhelmingly write one or the other,
/// not a mix.
pub fn lua_to_value(value: LuaValue) -> mlua::Result<Value> {
    Ok(match value {
        LuaValue::Nil => Value::Null,
        LuaValue::Boolean(b) => Value::Bool(b),
        LuaValue::Integer(i) => Value::I64(i),
        LuaValue::Number(n) => Value::F64(n),
        LuaValue::String(s) => {
            let bytes = Bytes::copy_from_slice(s.as_bytes());
            match std::str::from_utf8(&bytes) {
                Ok(_) => Value::Str(bytes),
                Err(_) => Value::Bytes(bytes),
            }
        }
        LuaValue::Table(table) => lua_table_to_value(table)?,
        other => {
            return Err(mlua::Error::RuntimeError(format!(
                "can't use a Lua {} as an event attribute value",
                other.type_name()
            )))
        }
    })
}

/// Checks whether `table`'s keys are exactly the contiguous sequence `1..=n` (Lua's own notion of
/// a "sequence" table, per the `#` operator) -- an empty table counts (vacuously true: it has no
/// keys to be non-contiguous). Returns the validated length if so, `None` otherwise. Shared by
/// [`lua_table_to_value`] (deciding `Array` vs. `Map`) and `ScriptWorker::process`/`flush`
/// (validating a script's returned table of events), rather than duplicating the same check.
///
/// Validates every key directly (each must be a positive Lua integer, and the full set must be
/// exactly `1..=n` once sorted) rather than comparing the total pair count against `raw_len()`.
/// An earlier version used the `raw_len()` comparison, which has a real bug: `raw_len()` (Lua's
/// `#` operator) is *undefined* for a table with holes -- free to return any valid "border," not
/// necessarily the one that would actually reveal a problem. Review reproduced
/// `{[1]="a", [2]="b", [4]="d", extra="c"}`: 4 total pairs, and `raw_len()` happens to also return
/// 4 (LuaJIT's choice of border here), so the count comparison passed despite key `3` being
/// missing and `extra` not belonging to the sequence at all -- silently decoding as
/// `Array(["a", "b", Null, "d"])` and dropping `extra` with no error. Checking each key's actual
/// identity has no such undefined-behavior dependency to exploit.
pub(crate) fn validated_sequence_len(table: &Table) -> mlua::Result<Option<usize>> {
    let mut keys: Vec<i64> = Vec::new();
    for pair in table.clone().pairs::<LuaValue, LuaValue>() {
        let (key, _value) = pair?;
        match key {
            LuaValue::Integer(i) if i >= 1 => keys.push(i),
            _ => return Ok(None),
        }
    }
    keys.sort_unstable();
    let is_contiguous_from_one = keys.iter().enumerate().all(|(idx, &k)| k == idx as i64 + 1);
    Ok(is_contiguous_from_one.then_some(keys.len()))
}

fn lua_table_to_value(table: Table) -> mlua::Result<Value> {
    match validated_sequence_len(&table)? {
        // An empty table is genuinely ambiguous between Value::Array(vec![]) and
        // Value::Map(AttrMap::new()) -- Lua's `{}` carries no origin-type information at all, so
        // there is no correct answer without tagging (the same class of problem this file's
        // `lua_value_matches` fixes for scalars, just for containers instead). An earlier version
        // let this fall through to the Array branch below, which fixed Value::Array(vec![])
        // losing its variant on a round trip by breaking the opposite case
        // (Value::Map(AttrMap::new()) also became Array). Documented, tested default: attributes
        // are the primary thing scripts manipulate and are map-shaped, so an empty table becomes
        // Value::Map. `validated_sequence_len` itself is unchanged and still correctly reports
        // `Some(0)` for an empty table -- `ScriptWorker::process`/`flush`'s use of it (`return {}`
        // meaning zero events) has no such ambiguity and isn't affected by this special case,
        // which lives here rather than in the shared helper.
        Some(0) => Ok(Value::Map(Box::new(AttrMap::new()))),
        Some(seq_len) => {
            let mut items = Vec::with_capacity(seq_len);
            for i in 1..=seq_len {
                items.push(lua_to_value(table.get(i)?)?);
            }
            Ok(Value::Array(items))
        }
        None => {
            let mut map = AttrMap::new();
            for pair in table.pairs::<String, LuaValue>() {
                let (key, value) = pair?;
                map.insert(&key, lua_to_value(value)?);
            }
            Ok(Value::Map(Box::new(map)))
        }
    }
}
