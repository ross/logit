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
        //
        // The same class of loss also happens on the *number* side, not just strings, for
        // reasons entirely outside this function's control -- a Lua value fundamentally can't
        // carry which Value variant produced it, regardless of which Lua type represents it.
        // Review-confirmed: a safe (in-range) U64 round-trips to I64 (a Lua integer has no
        // signed/unsigned tag to preserve), and a whole-number F64 (e.g. 42.0) *also* round-trips
        // to I64 -- LuaJIT's dual-number mode canonicalizes an integral Lua number as an integer
        // internally, so `lua_to_value` sees `LuaValue::Integer`, not `LuaValue::Number`,
        // regardless of how the value was originally pushed. A fractional F64 (42.5) is
        // unaffected and round-trips correctly, since LuaJIT has no integer representation for
        // it. Same deferred fix, same follow-up.
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
        // there is no correct answer without tagging (the same class of problem as the deferred
        // type-loss work in tmp/lua-value-type-preservation.md, just for containers instead of
        // scalars). An earlier version let this fall through to the Array branch below, which
        // fixed Value::Array(vec![]) losing its variant on a round trip by breaking the opposite
        // case (Value::Map(AttrMap::new()) also became Array). Documented, tested default:
        // attributes are the primary thing scripts manipulate and are map-shaped, so an empty
        // table becomes Value::Map. `validated_sequence_len` itself is unchanged and still
        // correctly reports `Some(0)` for an empty table -- `ScriptWorker::process`/`flush`'s use
        // of it (`return {}` meaning zero events) has no such ambiguity and isn't affected by this
        // special case, which lives here rather than in the shared helper.
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
