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
        Value::I64(i) => LuaValue::Integer(*i),
        // TODO: a U64 above i64::MAX truncates -- Lua (including LuaJIT) has no unsigned 64-bit
        // integer type. Rare in practice (attribute values and metric fields this large are
        // unusual); revisit if a real script hits it rather than solving it speculatively now.
        Value::U64(u) => LuaValue::Integer(*u as mlua::Integer),
        Value::F64(f) => LuaValue::Number(*f),
        Value::Bytes(b) => LuaValue::String(lua.create_string(b)?),
        Value::Str(s) => LuaValue::String(lua.create_string(s)?),
        // A string, not a Lua integer, for the same reason `EventProxy`'s `timestamp` field is
        // (see proxy.rs): a Lua number is an IEEE-754 double, exact only to 2^53 (~9e15), and a
        // unix-nanos timestamp is routinely ~1.7e18. (Converting a Lua value back into a
        // `Value::Timestamp` isn't supported by `lua_to_value` below -- a script-provided string
        // becomes `Value::Str`. Nothing produces a `Value::Timestamp` attribute today, so this
        // round-trip asymmetry has no live consequence yet; worth a real API if that changes.)
        Value::Timestamp(t) => LuaValue::String(lua.create_string(t.to_string())?),
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

fn lua_table_to_value(table: Table) -> mlua::Result<Value> {
    let seq_len: usize = table.raw_len();
    let is_sequence = seq_len > 0 && {
        let mut count = 0usize;
        for pair in table.clone().pairs::<LuaValue, LuaValue>() {
            pair?;
            count += 1;
        }
        count == seq_len
    };

    if is_sequence {
        let mut items = Vec::with_capacity(seq_len);
        for i in 1..=seq_len {
            items.push(lua_to_value(table.get(i)?)?);
        }
        Ok(Value::Array(items))
    } else {
        let mut map = AttrMap::new();
        for pair in table.pairs::<String, LuaValue>() {
            let (key, value) = pair?;
            map.insert(&key, lua_to_value(value)?);
        }
        Ok(Value::Map(Box::new(map)))
    }
}
