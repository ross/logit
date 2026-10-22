//! The `vars` global: a component's `vars:` config as plain Lua tables, built once per VM before
//! the script's source runs. See `docs/design/lua-api.md`'s "Reading configuration from `vars`"
//! section and `docs/adr/lua-script-vars.md`.
//!
//! Plain tables, not a read-only proxy, so `pairs`, `ipairs`, and `#` work and a read costs a
//! table lookup. A script that writes to `vars` changes only its own VM's copy.
//!
//! `logit validate` rejects a `null` and an integer past 2^53 in magnitude (graph rule 95), so
//! neither reaches here from a validated config. Built anyway, a `null` becomes `nil` (a hole in
//! a list) and such an integer the nearest double.

use logit_config::{LuaVar, LuaVars};
use mlua::{Lua, Table, Value as LuaValue};

/// Creates the `vars` global from `vars`, an empty table when it's empty.
pub(crate) fn install(lua: &Lua, vars: &LuaVars) -> mlua::Result<()> {
    let table = map_table(lua, vars)?;
    lua.globals().set("vars", table)
}

fn map_table<'lua>(lua: &'lua Lua, map: &LuaVars) -> mlua::Result<Table<'lua>> {
    let table = lua.create_table_with_capacity(0, map.len())?;
    for (key, value) in map {
        table.raw_set(key.as_str(), lua_value(lua, value)?)?;
    }
    Ok(table)
}

fn lua_value<'lua>(lua: &'lua Lua, value: &LuaVar) -> mlua::Result<LuaValue<'lua>> {
    Ok(match value {
        LuaVar::Null => LuaValue::Nil,
        LuaVar::Bool(b) => LuaValue::Boolean(*b),
        LuaVar::Integer(n) => LuaValue::Number(*n as f64),
        LuaVar::Float(x) => LuaValue::Number(*x),
        LuaVar::String(s) => LuaValue::String(lua.create_string(s)?),
        LuaVar::List(items) => {
            let table = lua.create_table_with_capacity(items.len(), 0)?;
            for (i, item) in items.iter().enumerate() {
                table.raw_set(i + 1, lua_value(lua, item)?)?;
            }
            LuaValue::Table(table)
        }
        LuaVar::Map(map) => LuaValue::Table(map_table(lua, map)?),
    })
}

#[cfg(test)]
mod tests {
    use crate::ScriptWorker;
    use logit_config::{LuaVar, LuaVars};
    use std::collections::BTreeMap;

    fn worker(source: &str, vars: &LuaVars) -> ScriptWorker {
        ScriptWorker::new_with_vars(source, false, vars).expect("script should load")
    }

    fn eval<T: for<'lua> mlua::FromLua<'lua>>(w: &ScriptWorker, expr: &str) -> T {
        w.lua.load(expr).eval().unwrap_or_else(|e| panic!("{expr}: {e}"))
    }

    fn prusa_vars() -> LuaVars {
        LuaVars::from([
            ("bucket".to_string(), LuaVar::String("prusa".to_string())),
            ("port".to_string(), LuaVar::Integer(8086)),
            ("ratio".to_string(), LuaVar::Float(0.25)),
            ("debug".to_string(), LuaVar::Bool(true)),
            (
                "hosts".to_string(),
                LuaVar::List(vec![
                    LuaVar::String("a".to_string()),
                    LuaVar::String("b".to_string()),
                    LuaVar::List(vec![LuaVar::Integer(7)]),
                ]),
            ),
            (
                "names".to_string(),
                LuaVar::Map(BTreeMap::from([
                    ("10:9c:70:2d:d5:c4".to_string(), LuaVar::String("sand".to_string())),
                    (
                        "deep".to_string(),
                        LuaVar::Map(BTreeMap::from([("on".to_string(), LuaVar::Bool(false))])),
                    ),
                ])),
            ),
        ])
    }

    const PASS: &str = "function process(event) return event end";

    #[test]
    fn each_yaml_type_maps_to_its_lua_type() {
        let w = worker(PASS, &prusa_vars());
        assert_eq!(eval::<String>(&w, "return vars.bucket"), "prusa");
        assert_eq!(eval::<String>(&w, "return type(vars.port)"), "number");
        assert_eq!(eval::<f64>(&w, "return vars.port"), 8086.0);
        assert_eq!(eval::<f64>(&w, "return vars.ratio"), 0.25);
        assert_eq!(eval::<String>(&w, "return type(vars.debug)"), "boolean");
        assert!(eval::<bool>(&w, "return vars.debug"));
        assert_eq!(eval::<String>(&w, r#"return vars.names["10:9c:70:2d:d5:c4"]"#), "sand");
        assert!(!eval::<bool>(&w, "return vars.names.deep.on"));
    }

    #[test]
    fn a_list_is_an_array_indexed_from_1() {
        let w = worker(PASS, &prusa_vars());
        assert_eq!(eval::<i64>(&w, "return #vars.hosts"), 3);
        assert!(eval::<bool>(&w, "return vars.hosts[0] == nil"));
        assert_eq!(eval::<String>(&w, "return vars.hosts[1] .. vars.hosts[2]"), "ab");
        assert_eq!(eval::<i64>(&w, "return vars.hosts[3][1]"), 7);
        let joined: String = eval(
            &w,
            "local out = {} for i, v in ipairs(vars.hosts) do \
             if type(v) == 'string' then out[#out + 1] = i .. v end end \
             return table.concat(out, ',')",
        );
        assert_eq!(joined, "1a,2b");
    }

    #[test]
    fn pairs_walks_a_map() {
        let w = worker(PASS, &prusa_vars());
        let keys: String = eval(
            &w,
            "local keys = {} for k in pairs(vars.names) do keys[#keys + 1] = k end \
             table.sort(keys) return table.concat(keys, ',')",
        );
        assert_eq!(keys, "10:9c:70:2d:d5:c4,deep");
    }

    #[test]
    fn absent_vars_are_an_empty_table() {
        for w in [ScriptWorker::new(PASS).unwrap(), worker(PASS, &LuaVars::new())] {
            assert_eq!(eval::<String>(&w, "return type(vars)"), "table");
            assert!(eval::<bool>(&w, "return next(vars) == nil"));
            assert_eq!(eval::<String>(&w, r#"return vars.bucket or "prusa""#), "prusa");
        }
    }

    /// The script's top level runs after `vars` is installed, so a top-level alias captures it.
    #[test]
    fn top_level_code_reads_vars() {
        let w =
            worker(&format!("local BUCKET = vars.bucket\nseen = BUCKET\n{PASS}"), &prusa_vars());
        assert_eq!(eval::<String>(&w, "return seen"), "prusa");
    }

    #[test]
    fn a_write_changes_only_that_vms_copy() {
        let vars = prusa_vars();
        let writer = worker(
            &format!("vars.bucket = 'other'\nvars.names.new = 'x'\nvars.hosts[1] = nil\n{PASS}"),
            &vars,
        );
        assert_eq!(eval::<String>(&writer, "return vars.bucket"), "other");
        let reader = worker(PASS, &vars);
        assert_eq!(eval::<String>(&reader, "return vars.bucket"), "prusa");
        assert!(eval::<bool>(&reader, "return vars.names.new == nil"));
        assert_eq!(eval::<String>(&reader, "return vars.hosts[1]"), "a");
    }

    /// Validation keeps these out of a running config; built anyway, a null is a hole.
    #[test]
    fn an_unvalidated_null_is_nil() {
        let vars = LuaVars::from([(
            "list".to_string(),
            LuaVar::List(vec![LuaVar::Integer(1), LuaVar::Null, LuaVar::Integer(3)]),
        )]);
        let w = worker(PASS, &vars);
        assert!(eval::<bool>(&w, "return vars.list[2] == nil"));
        assert_eq!(eval::<i64>(&w, "return vars.list[3]"), 3);
    }
}
