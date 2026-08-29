//! Conversion between `logit_core::Value` and `mlua::Value`.
//!
//! Shared by [`crate::proxy`] (reading/writing individual attributes, and `to_table()`'s full
//! snapshot) -- one definition of this mapping, not two ad hoc conversions drifting apart.

use bytes::Bytes;
use logit_core::interner::resolve;
use logit_core::{AttrMap, Value};
use mlua::{Lua, Table, Value as LuaValue};

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
        // exact-integer boundary (see their doc comments for why that's a magnitude check, not a
        // round-trip cast -- the obvious round-trip check has its own bug near `u64::MAX`) and
        // fall back to a decimal string when a value doesn't fit. This is conditional rather than
        // the blanket string `Timestamp` uses because ordinary I64/U64 attributes are
        // usually small (`retry_count = 3`), where a real Lua number is both safe and far more
        // useful to a script (natural comparisons/arithmetic) than a string would be; a
        // `Timestamp` is *always* large enough in practice to take the string branch anyway, so
        // it shares this same exact-round-trip logic rather than a separately maintained rule.
        //
        // NOTE: this means a value that takes the string branch (any I64/U64 outside the safe
        // range, every Timestamp, every Bytes/Str) is indistinguishable from an ordinary
        // Value::Str once inside Lua -- a plain Lua string has no way to carry which of those it
        // came from. An identity round-trip through a script can therefore turn a Bytes/Timestamp/
        // large-integer attribute into a Str one. Known, not silently shipped: downstream code
        // (e.g. logit-outputs::influxdb's tag handling) is allowed to treat these variants
        // differently, so this is a real, if narrow, gap -- a proper fix needs a tagged/userdata
        // value wrapper preserving origin type through Lua, which is a large enough addition
        // (a new userdata type, its own metamethod surface) to belong in a focused follow-up
        // rather than here.
        Value::I64(i) => exact_i64_to_lua(lua, *i)?,
        Value::U64(u) => exact_u64_to_lua(lua, *u)?,
        Value::F64(f) => LuaValue::Number(*f),
        Value::Bytes(b) => LuaValue::String(lua.create_string(b)?),
        Value::Str(s) => LuaValue::String(lua.create_string(s)?),
        // Converting a Lua value back into a `Value::Timestamp` isn't supported by `lua_to_value`
        // below -- a script-provided string becomes `Value::Str`, per the note above. Nothing
        // produces a `Value::Timestamp` attribute today, so this has no live consequence yet.
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

/// Represents an `i64` as a Lua integer if doing so round-trips exactly through Lua's actual
/// numeric representation (an IEEE-754 double), or as a decimal string otherwise.
///
/// A magnitude check against [`MAX_EXACT_F64_INT`], not a `(v as f64) as $int == v` round-trip
/// check: that was the first thing tried here, and it has a real bug for values near `u64::MAX`.
/// Rust's float-to-int `as` casts saturate rather than wrap (since 1.45), so
/// `(u64::MAX as f64) as u64` rounds up to 2^64 as an f64 and then *saturates back down* to
/// exactly `u64::MAX` on the cast back -- the round trip "succeeds" despite real precision loss
/// in between, because saturation happens to land back on the original value. A direct magnitude
/// comparison against the actual exact-integer boundary has no such edge case.
const MAX_EXACT_F64_INT: i64 = 1 << 53; // 9_007_199_254_740_992

fn exact_i64_to_lua(lua: &Lua, i: i64) -> mlua::Result<LuaValue<'_>> {
    if (-MAX_EXACT_F64_INT..=MAX_EXACT_F64_INT).contains(&i) {
        Ok(LuaValue::Integer(i))
    } else {
        Ok(LuaValue::String(lua.create_string(i.to_string())?))
    }
}

/// As [`exact_i64_to_lua`], for `u64`. A `u64` within the safe range is always well within `i64`'s
/// range too (2^53 is far below `i64::MAX`), so the `as i64` cast on the safe branch never
/// truncates.
fn exact_u64_to_lua(lua: &Lua, u: u64) -> mlua::Result<LuaValue<'_>> {
    if u <= MAX_EXACT_F64_INT as u64 {
        Ok(LuaValue::Integer(u as mlua::Integer))
    } else {
        Ok(LuaValue::String(lua.create_string(u.to_string())?))
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
/// Deliberately checks the *total* pair count against `raw_len()` rather than gating on
/// `raw_len() > 0` first: an earlier version did gate on it, which meant `raw_len() == 0` always
/// short-circuited to "not a sequence" -- silently turning `Value::Array(vec![])` into
/// `Value::Map` on a round trip, since an empty table could never be recognized as an empty
/// sequence. Checking the count unconditionally handles the empty case correctly for free (0
/// pairs == a `raw_len` of 0 is trivially equal) with no special-casing.
pub(crate) fn validated_sequence_len(table: &Table) -> mlua::Result<Option<usize>> {
    let seq_len: usize = table.raw_len();
    let mut count = 0usize;
    for pair in table.clone().pairs::<LuaValue, LuaValue>() {
        pair?;
        count += 1;
    }
    Ok((count == seq_len).then_some(seq_len))
}

fn lua_table_to_value(table: Table) -> mlua::Result<Value> {
    match validated_sequence_len(&table)? {
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
