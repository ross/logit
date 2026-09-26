//! Conversion between `logit_core::Value` and `mlua::Value`: the one mapping every proxy,
//! `to_table()`, and `Event.new` share. See `docs/design/lua-value-type-preservation.md`.
//!
//! | `Value` | to Lua ([`value_to_lua`]) | from Lua ([`lua_to_value`]) |
//! |---|---|---|
//! | `Null` | `nil` | from `nil` |
//! | `Bool` | boolean | from a boolean |
//! | `I64` | integer within ±2^53, else decimal string | from an integer |
//! | `Timestamp` | as `I64` | never |
//! | `U64` | integer up to 2^53, else decimal string | never |
//! | `F64` | number | from a non-integral number |
//! | `Str` | string | from a UTF-8 string |
//! | `Bytes` | string | from a non-UTF-8 string |
//! | `Array` | 1-based sequence table | from a non-empty table whose keys are `1..=n` |
//! | `Map` | table | from any other table, the empty table included |
//!
//! A Lua string or number can't carry which variant it came from, so an identity assignment
//! would change the variant; [`lua_value_matches`] is how `AttrsProxy::__newindex` keeps it.

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
        // A Lua number is a double, exact only up to 2^53. Past that, an identity assignment
        // (`event.attributes.x = event.attributes.x`) would change the value, and an `as` cast
        // would wrap a large `U64` negative, so such an integer becomes a decimal string. Small
        // integers stay numbers because scripts compare and do arithmetic on them. A `Timestamp`
        // is in practice always past 2^53, so it takes the string branch under the same rule.
        //
        // An empty `Array` and an empty `Map` both become `{}`, which reads back as `Map`
        // (`lua_table_to_value`).
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

/// The largest magnitude integer an IEEE-754 double (Lua's number type) represents exactly.
///
/// Compare magnitudes against this, not `(v as f64) as u64 == v`: `u64::MAX as f64` rounds up to
/// 2^64 and the float-to-int cast saturates back to `u64::MAX`, so that round trip passes despite
/// the precision loss.
const MAX_EXACT_F64_INT: i64 = 1 << 53; // 9_007_199_254_740_992

/// Whether `value_to_lua` gives this `i64` as a Lua integer (`true`) or a decimal string.
///
/// The one definition of the boundary, shared by `exact_i64_to_lua` and `lua_value_matches`.
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

/// As [`exact_i64_to_lua`], for `u64`. The exact range is within `i64`'s, so the cast never
/// truncates.
///
/// Also the one encoding of a metric's `u64` count (`crate::proxy`'s `metric_to_table` and
/// `MetricProxy`), which `construct::count` reads back from either form, so a count at any
/// magnitude survives `Event.new(e:to_table())`.
pub(crate) fn exact_u64_to_lua(lua: &Lua, u: u64) -> mlua::Result<LuaValue<'_>> {
    if u64_is_exact_lua_number(u) {
        Ok(LuaValue::Integer(u as mlua::Integer))
    } else {
        Ok(LuaValue::String(lua.create_string(u.to_string())?))
    }
}

/// The bytes [`value_to_lua`] hands a script for a value that takes its string branch, or `None`
/// for a value that reaches Lua as something else. Must agree with `value_to_lua`'s string arms.
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

/// Whether `new` is what [`value_to_lua`] produces for `existing`, so assigning it back would
/// change only the variant.
///
/// [`crate::proxy::AttrsProxy`]'s `__newindex` makes such an assignment a no-op, so the original
/// variant survives an unmodified round trip (`docs/adr/lua-value-identity-preservation.md`).
///
/// Shallow: a `Table` never matches, so `Array([Bytes(..)])` assigned back to itself becomes
/// `Array([Str(..)])`. Recursing would walk the incoming table while the event's `RefCell` is
/// borrowed, which the attribute write paths release before any table walk. A tested gap:
/// `docs/design/lua-value-type-preservation.md`'s "Known residual gaps".
pub(crate) fn lua_value_matches(existing: &Value, new: &LuaValue) -> bool {
    match new {
        LuaValue::Nil => matches!(existing, Value::Null),
        LuaValue::Boolean(b) => matches!(existing, Value::Bool(e) if e == b),
        LuaValue::Integer(n) => match existing {
            Value::I64(i) => i == n,
            Value::U64(u) => u64_is_exact_lua_number(*u) && *u == *n as u64,
            Value::Timestamp(t) => i64_is_exact_lua_number(*t) && t == n,
            // LuaJIT's dual-number mode hands an integral number (42.0) back as an Integer, so
            // an integral `F64` arrives here, not in the `Number` arm.
            Value::F64(f) => *f == *n as f64,
            _ => false,
        },
        LuaValue::Number(n) => matches!(existing, Value::F64(f) if f == n),
        LuaValue::String(s) => lua_string_repr(existing).as_deref() == Some(s.as_bytes()),
        _ => false,
    }
}

/// Converts an [`AttrMap`] into a plain Lua table.
pub fn attrmap_to_lua_table<'lua>(lua: &'lua Lua, map: &AttrMap) -> mlua::Result<Table<'lua>> {
    let table = lua.create_table()?;
    for (key, value) in map.iter() {
        table.set(resolve(key), value_to_lua(lua, value)?)?;
    }
    Ok(table)
}

/// How many `Map`/`Array` levels a converted value may nest: an attribute value sits at depth 0
/// and each `Map` or `Array` is one level, so a scalar leaf at depth 128 converts and a table at
/// depth 128 is an error. The same accounting as `logit_proto::native`'s `MAX_VALUE_DEPTH`
/// (`crates/logit-proto/src/native/value.rs`), so a value a script builds always decodes on a
/// `logit_in` peer. The cap is what turns a self-referencing table into an error instead of a
/// stack overflow.
pub(crate) const MAX_TABLE_DEPTH: usize = 128;

/// Converts a script's value into a `Value`, per the module doc's table.
///
/// Lua has one table type, so a table is an `Array` if its keys are a non-empty `1..=n` and a
/// `Map` otherwise. Any other Lua type (a function, userdata) is an error, and so is a table
/// nested past [`MAX_TABLE_DEPTH`]. Every table read is raw, so conversion runs no metamethod.
pub fn lua_to_value(value: LuaValue) -> mlua::Result<Value> {
    lua_to_value_at(value, 0)
}

/// Prefixes a [`lua_to_value`] error with the attribute it was converting for, as
/// `<path>.<key>: <message>`, so a depth error or a bad nested key names the write that raised
/// it. Run the resource/scope relabel first, since it matches only `RuntimeError`. Call it only
/// on the error branch: it formats.
pub(crate) fn attribute_error(path: &str, key: &str, err: mlua::Error) -> mlua::Error {
    prefixed_error(&format!("{path}.{key}"), err)
}

/// `err` as a `RuntimeError` reading `<prefix>: <message>`, whatever its variant: a nested key
/// mlua can't read as a string is a `FromLuaConversionError`, and it needs the prefix too.
pub(crate) fn prefixed_error(prefix: &str, err: mlua::Error) -> mlua::Error {
    match err {
        mlua::Error::RuntimeError(msg) => mlua::Error::RuntimeError(format!("{prefix}: {msg}")),
        other => mlua::Error::RuntimeError(format!("{prefix}: {other}")),
    }
}

/// [`lua_to_value`] for a value `depth` tables below the attribute value.
fn lua_to_value_at(value: LuaValue, depth: usize) -> mlua::Result<Value> {
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
        LuaValue::Table(_) if depth >= MAX_TABLE_DEPTH => {
            return Err(mlua::Error::RuntimeError(format!(
                "can't use a table nested more than {MAX_TABLE_DEPTH} levels deep as an event \
                 attribute value (does a table contain itself?)"
            )))
        }
        LuaValue::Table(table) => lua_table_to_value(table, depth)?,
        other => {
            return Err(mlua::Error::RuntimeError(format!(
                "can't use a Lua {} as an event attribute value",
                other.type_name()
            )))
        }
    })
}

/// The length `n` if `table`'s keys are exactly `1..=n` (`Some(0)` for an empty table), else
/// `None`.
///
/// Shared by [`lua_table_to_value`] and `ScriptWorker::process`/`flush`'s check of a returned
/// table of events. Checks every key rather than comparing the pair count with `raw_len()`: `#`
/// is undefined for a table with holes, and LuaJIT returns 4 for
/// `{[1]="a", [2]="b", [4]="d", extra="c"}`, which would pass a count check.
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

/// `table`, found `depth` tables below the attribute value; its children convert at `depth + 1`.
fn lua_table_to_value(table: Table, depth: usize) -> mlua::Result<Value> {
    match validated_sequence_len(&table)? {
        // `{}` is ambiguous between an empty `Array` and an empty `Map`; it becomes `Map` because
        // attributes are map-shaped. The case lives here, not in `validated_sequence_len`, because
        // `return {}` from `process` means zero events.
        Some(0) => Ok(Value::Map(Box::new(AttrMap::new()))),
        Some(seq_len) => {
            let mut items = Vec::with_capacity(seq_len);
            for i in 1..=seq_len {
                items.push(lua_to_value_at(table.raw_get(i)?, depth + 1)?);
            }
            Ok(Value::Array(items))
        }
        None => Ok(Value::Map(Box::new(lua_table_to_attrmap(table, depth)?))),
    }
}

/// Converts a map-shaped Lua table into an [`AttrMap`], every value through [`lua_to_value_at`]
/// one level down. `Table::pairs` walks with `lua_next` in mlua 0.9, so the walk is raw.
///
/// A numeric key is coerced to its decimal string (`{[1] = "a", x = "b"}` gives keys `"1"` and
/// `"x"`), a key must be UTF-8, and any other non-string key is mlua's conversion error.
/// `Event.new` rejects numeric and non-UTF-8 keys instead (`construct::attributes_from_table`).
fn lua_table_to_attrmap(table: Table, depth: usize) -> mlua::Result<AttrMap> {
    let mut map = AttrMap::new();
    for pair in table.pairs::<mlua::String, LuaValue>() {
        let (key, value) = pair?;
        map.insert(key.to_str()?, lua_to_value_at(value, depth + 1)?);
    }
    Ok(map)
}

/// An `Array` attribute (a repeated DogStatsD tag key, `statsd_in`'s `insert_tags`) seen through
/// a real [`crate::ScriptWorker`].
#[cfg(test)]
mod array_attribute_tests {
    use super::*;
    use crate::{ProcessOutcome, ScriptWorker};
    use logit_core::Event;

    fn worker(source: &str) -> ScriptWorker {
        ScriptWorker::new(source).expect("script should load")
    }

    fn emitted(outcome: ProcessOutcome) -> Event {
        match outcome {
            ProcessOutcome::Emit(e, _) => *e,
            _ => panic!("expected Emit"),
        }
    }

    fn event_with_team(team: Value) -> Event {
        let mut attrs = AttrMap::new();
        attrs.insert("team", team);
        Event::empty(0, attrs)
    }

    #[test]
    fn a_multi_valued_array_attribute_is_seen_as_a_two_element_1_based_table() {
        let w = worker(
            r#"
            function process(event)
                assert(#event.attributes.team == 2, "expected 2 elements")
                assert(event.attributes.team[1] == "a", "expected element 1 to be a")
                assert(event.attributes.team[2] == "b", "expected element 2 to be b")
                return event
            end
            "#,
        );
        let event = event_with_team(Value::Array(vec![Value::str("a"), Value::str("b")]));
        // A failed Lua `assert()` is a runtime error, so the `.unwrap()` checks them.
        let out = emitted(w.process(event).unwrap());
        assert_eq!(
            out.attributes.get("team"),
            Some(&Value::Array(vec![Value::str("a"), Value::str("b")]))
        );
    }

    #[test]
    fn an_untouched_read_write_round_trip_keeps_the_array_and_its_elements() {
        let w = worker(
            r#"
            function process(event)
                event.attributes.team = event.attributes.team
                return event
            end
            "#,
        );
        let event = event_with_team(Value::Array(vec![Value::str("a"), Value::str("b")]));
        let out = emitted(w.process(event).unwrap());
        assert_eq!(
            out.attributes.get("team"),
            Some(&Value::Array(vec![Value::str("a"), Value::str("b")]))
        );
    }

    #[test]
    fn a_mixed_bool_and_str_array_keeps_each_elements_own_type_through_a_round_trip() {
        let w = worker(
            r#"
            function process(event)
                assert(event.attributes.team[1] == true, "expected element 1 to be true")
                assert(event.attributes.team[2] == "1", "expected element 2 to be the string 1")
                event.attributes.team = event.attributes.team
                return event
            end
            "#,
        );
        let event = event_with_team(Value::Array(vec![Value::Bool(true), Value::str("1")]));
        let out = emitted(w.process(event).unwrap());
        assert_eq!(
            out.attributes.get("team"),
            Some(&Value::Array(vec![Value::Bool(true), Value::str("1")]))
        );
    }
}

/// [`MAX_TABLE_DEPTH`] and raw reads, driven through a real [`crate::ScriptWorker`].
#[cfg(test)]
mod depth_tests {
    use super::*;
    use crate::{ProcessOutcome, ScriptWorker};
    use logit_core::Event;

    fn worker(source: &str) -> ScriptWorker {
        ScriptWorker::new(source).expect("script should load")
    }

    fn emitted(outcome: ProcessOutcome) -> Event {
        match outcome {
            ProcessOutcome::Emit(e, _) => *e,
            _ => panic!("expected Emit"),
        }
    }

    /// `process()`'s error text, for a script that must fail.
    fn process_err(source: &str) -> String {
        match worker(source).process(Event::empty(0, AttrMap::new())) {
            Err(err) => err.to_string(),
            Ok(_) => panic!("expected process() to fail"),
        }
    }

    /// A script that wraps a scalar leaf in `levels` tables, alternating one-key maps and
    /// one-element arrays, and assigns the result to `target`.
    fn nest_and_assign(levels: usize, target: &str) -> String {
        format!(
            r#"
            function process(event)
                local v = 1
                for i = 1, {levels} do
                    if i % 2 == 0 then v = {{v}} else v = {{k = v}} end
                end
                {target} = v
                return event
            end
            "#
        )
    }

    /// How many `Map`/`Array` levels wrap the scalar leaf of `value`.
    fn table_levels(mut value: &Value) -> usize {
        let mut levels = 0;
        loop {
            value = match value {
                Value::Map(m) => m.get("k").expect("a one-key map level"),
                Value::Array(items) => &items[0],
                _ => return levels,
            };
            levels += 1;
        }
    }

    #[test]
    fn a_table_nested_128_deep_converts() {
        let out = emitted(
            worker(&nest_and_assign(128, "event.attributes.deep"))
                .process(Event::empty(0, AttrMap::new()))
                .unwrap(),
        );
        let deep = out.attributes.get("deep").expect("the attribute was written");
        assert_eq!(table_levels(deep), 128);
    }

    #[test]
    fn a_table_nested_129_deep_is_an_error_naming_the_cap() {
        let err = process_err(&nest_and_assign(129, "event.attributes.deep"));
        assert!(err.contains("event.attributes.deep: "), "{err}");
        assert!(err.contains("nested more than 128 levels deep"), "{err}");
    }

    #[test]
    fn a_self_referencing_table_is_the_depth_error_not_a_stack_overflow() {
        let err = process_err(
            r#"
            function process(event)
                local t = {}
                t.self = t
                event.attributes.loop = t
                return event
            end
            "#,
        );
        assert!(err.contains("event.attributes.loop: "), "{err}");
        assert!(err.contains("does a table contain itself?"), "{err}");
    }

    #[test]
    fn a_self_referencing_array_is_the_depth_error() {
        let err = process_err(
            r#"
            function process(event)
                local t = {}
                t[1] = t
                event.attributes.loop = t
                return event
            end
            "#,
        );
        assert!(err.contains("event.attributes.loop: "), "{err}");
        assert!(err.contains("nested more than 128 levels deep"), "{err}");
    }

    /// Pins the raw-read guarantee rather than reproducing a regression: `Table::get` consults
    /// `__index` only for a nil raw value, and every index here is present, so this passes
    /// whether the array branch reads with `get` or `raw_get`.
    #[test]
    fn an_array_tables_index_metamethod_never_fires_during_conversion() {
        let out = emitted(
            worker(
                r#"
                fired = false
                function process(event)
                    local list = setmetatable({"a", "b"}, {
                        __index = function() fired = true; return "from __index" end,
                        __len = function() fired = true; return 5 end,
                    })
                    event.attributes.list = list
                    assert(not fired, "a metamethod ran during conversion")
                    return event
                end
                "#,
            )
            .process(Event::empty(0, AttrMap::new()))
            .unwrap(),
        );
        assert_eq!(
            out.attributes.get("list"),
            Some(&Value::Array(vec![Value::str("a"), Value::str("b")]))
        );
    }

    /// Pins the guarantee that conversion runs no script code while it builds the value, as
    /// [`an_array_tables_index_metamethod_never_fires_during_conversion`] does; it doesn't
    /// reproduce a regression.
    #[test]
    fn a_metamethod_that_writes_back_into_the_event_never_runs_during_an_attribute_write() {
        let out = emitted(
            worker(
                r#"
                function process(event)
                    local writeback = function()
                        event.attributes.intruder = "wrote during conversion"
                        return nil
                    end
                    local inner = setmetatable({x = 1}, {__index = writeback, __newindex = writeback})
                    event.attributes.value = setmetatable({"a", inner}, {__index = writeback})
                    return event
                end
                "#,
            )
            .process(Event::empty(0, AttrMap::new()))
            .unwrap(),
        );
        assert!(out.attributes.get("intruder").is_none(), "a metamethod wrote into the event");
        let mut inner = AttrMap::new();
        inner.insert("x", 1i64);
        assert_eq!(
            out.attributes.get("value"),
            Some(&Value::Array(vec![Value::str("a"), Value::Map(Box::new(inner))]))
        );
    }

    /// A nested key mlua can't read as a string fails as a `FromLuaConversionError`, which gets
    /// the attribute's prefix like any other conversion error.
    #[test]
    fn a_bad_key_inside_a_nested_attribute_table_names_the_attribute() {
        let err = process_err(
            r#"
            function process(event)
                event.attributes.x = {[true] = 1}
                return event
            end
            "#,
        );
        assert!(err.contains("event.attributes.x: "), "{err}");

        let err = process_err(
            r#"
            function process(event)
                return Event.new{timestamp = "1", attributes = {x = {[true] = 1}}}
            end
            "#,
        );
        assert!(err.contains("Event.new: attributes.x: "), "{err}");
    }

    #[test]
    fn a_resource_and_a_scope_attribute_write_get_the_same_cap() {
        let resource_err = process_err(&nest_and_assign(129, "resource.deep"));
        assert!(resource_err.contains("resource.deep: "), "{resource_err}");
        assert!(resource_err.contains("resource attribute value"), "{resource_err}");
        assert!(resource_err.contains("nested more than 128 levels deep"), "{resource_err}");

        let scope_err = process_err(&nest_and_assign(129, "scope.attributes.deep"));
        assert!(scope_err.contains("scope.attributes.deep: "), "{scope_err}");
        assert!(scope_err.contains("scope attribute value"), "{scope_err}");
        assert!(scope_err.contains("nested more than 128 levels deep"), "{scope_err}");
    }

    /// mlua 0.9.9's LuaJIT number read truncates toward zero (`num_traits::cast`) and keeps the
    /// integer when `(n - i as f64).abs() < f64::EPSILON`, so only `0 < |x| < 2^-52` (and
    /// `-0.0`) collapse to `0`: a tiny nonzero float written back is stored as `I64(0)`. A
    /// recorded residual (`docs/adr/lua-event-constructor.md`'s count amendment); this pins it.
    #[test]
    fn a_float_below_epsilon_written_back_becomes_zero_a_recorded_residual() {
        let mut attrs = AttrMap::new();
        attrs.insert("tiny", 1e-20f64);
        attrs.insert("small", 1e-10f64);
        let out = emitted(
            worker(
                r#"
                function process(event)
                    event.attributes.tiny = event.attributes.tiny
                    event.attributes.small = event.attributes.small
                    return event
                end
                "#,
            )
            .process(Event::empty(0, attrs))
            .unwrap(),
        );
        assert_eq!(out.attributes.get("tiny"), Some(&Value::I64(0)));
        assert_eq!(out.attributes.get("small"), Some(&Value::F64(1e-10)));
    }
}
