//! Exposes the incoming batch's OTLP instrumentation scope to Lua as a global `scope` userdata,
//! readable and writable, copy-on-write. It mirrors `crate::resource` (installed before
//! `.exec()`, `Rc<RefCell<..>>` state, identity writes as no-ops); `docs/design/lua-api.md`'s
//! "Reading and writing `resource`" section covers both.
//!
//! **Unlike `resource`, a batch may have no scope** (`logit_core::EventBatch::scope` is an
//! `Option`). Every field then reads as [`logit_core::Scope::default`]'s value (`""` for
//! `name`/`version`, `nil` for `schema_url`, `0` for `dropped_attributes_count`, an empty table
//! for `attributes`), and a write starts `modified` from `Scope::default()` rather than erroring.
//!
//! **`scope.attributes` is its own sub-userdata**, [`ScopeAttrsProxy`], with `AttrsProxy`'s
//! open-map shape over the same `Rc<RefCell<ScopeState>>`. It skips `EventProxy::attrs`'s lazy
//! create-and-cache: that exists because an `EventProxy` is created per event and must be torn
//! down before the event is handed back. `scope` lives for the worker's lifetime, so [`install`]
//! creates the sub-userdata once and [`ScopeProxy`] holds its `RegistryKey`.

use crate::value::{attrmap_to_lua_table, lua_to_value, lua_value_matches, value_to_lua};
use bytes::Bytes;
use logit_core::Scope;
use mlua::{Lua, MetaMethod, RegistryKey, UserData, UserDataMethods, Value as LuaValue};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

/// Per-batch scope state, shared by [`crate::ScriptWorker`], [`ScopeProxy`], and
/// [`ScopeAttrsProxy`], so a script's write is visible to [`take`] without a trip through Lua.
pub(crate) struct ScopeState {
    /// `None` before the first [`set`] and for a batch that carries no scope.
    base: Option<Arc<Scope>>,
    /// A full copy of `base` (or `Scope::default()`), made on the first write since the last
    /// [`set`] and mutated in place after. `None` keeps a script that never writes allocation-free.
    modified: Option<Scope>,
}

/// Creates the `scope` global, reading as defaults until the first [`set`], and returns its
/// shared state.
pub(crate) fn install(lua: &Lua) -> mlua::Result<Rc<RefCell<ScopeState>>> {
    let state = Rc::new(RefCell::new(ScopeState { base: None, modified: None }));
    let attrs_ud = lua.create_userdata(ScopeAttrsProxy(state.clone()))?;
    let attrs = lua.create_registry_value(attrs_ud)?;
    let proxy = lua.create_userdata(ScopeProxy { state: state.clone(), attrs })?;
    lua.globals().set("scope", proxy)?;
    Ok(state)
}

/// Resets `scope` to read `scope` (defaults if `None`) and discards any earlier write.
///
/// Called once per incoming batch before its events reach `process`, and before every `flush()`
/// with `None`, the flush root context (`docs/adr/lua-flush-root-context.md`).
pub(crate) fn set(state: &Rc<RefCell<ScopeState>>, scope: &Option<Arc<Scope>>) {
    let mut state = state.borrow_mut();
    state.base = scope.clone();
    state.modified = None;
}

/// The script's write since the last [`set`], if any.
///
/// Commits the write as the new `base`, so a read before the next [`set`] still sees it and a
/// second call returns `None`. `run_lua` calls [`set`] before every batch and every `flush()`, so
/// no write carries into the next call.
pub(crate) fn take(state: &Rc<RefCell<ScopeState>>) -> Option<Arc<Scope>> {
    let mut state = state.borrow_mut();
    let modified = state.modified.take()?;
    let new = Arc::new(modified);
    state.base = Some(new.clone());
    Some(new)
}

/// Clones `state.base` (or `Scope::default()`) into `state.modified` on the first write since
/// the last [`set`].
///
/// Every write path goes through this, so a write to one field never discards an earlier write
/// to another in the same batch.
fn ensure_modified(state: &mut ScopeState) {
    if state.modified.is_none() {
        state.modified = Some(match &state.base {
            Some(scope) => (**scope).clone(),
            None => Scope::default(),
        });
    }
}

fn scope_name(state: &ScopeState) -> &[u8] {
    match &state.modified {
        Some(s) => s.name.as_ref(),
        None => state.base.as_ref().map(|b| b.name.as_ref()).unwrap_or(b""),
    }
}

fn scope_version(state: &ScopeState) -> &[u8] {
    match &state.modified {
        Some(s) => s.version.as_ref(),
        None => state.base.as_ref().map(|b| b.version.as_ref()).unwrap_or(b""),
    }
}

fn scope_schema_url(state: &ScopeState) -> Option<&Bytes> {
    match &state.modified {
        Some(s) => s.schema_url.as_ref(),
        None => state.base.as_ref().and_then(|b| b.schema_url.as_ref()),
    }
}

fn scope_dropped_attributes_count(state: &ScopeState) -> u32 {
    match &state.modified {
        Some(s) => s.dropped_attributes_count,
        None => state.base.as_ref().map(|b| b.dropped_attributes_count).unwrap_or(0),
    }
}

/// Accepts only a string for `scope.name`/`scope.version`. Unlike `schema_url`, neither has a
/// `nil` meaning: an absent scope reads back `""`.
fn require_string<'a>(value: &'a LuaValue, field: &str) -> mlua::Result<&'a [u8]> {
    match value {
        LuaValue::String(s) => Ok(s.as_bytes()),
        other => Err(mlua::Error::RuntimeError(format!(
            "scope.{field} must be a string, got {}",
            other.type_name()
        ))),
    }
}

/// The `scope` global's userdata.
struct ScopeProxy {
    state: Rc<RefCell<ScopeState>>,
    /// `scope.attributes`'s sub-userdata, created once in [`install`]; see the module doc.
    attrs: RegistryKey,
}

impl UserData for ScopeProxy {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_meta_method(MetaMethod::Index, |lua, this, key: mlua::String| {
            let key = key.to_str()?;
            if key == "attributes" {
                return Ok(LuaValue::UserData(lua.registry_value(&this.attrs)?));
            }
            let state = this.state.borrow();
            Ok(match key {
                "name" => LuaValue::String(lua.create_string(scope_name(&state))?),
                "version" => LuaValue::String(lua.create_string(scope_version(&state))?),
                "schema_url" => match scope_schema_url(&state) {
                    Some(s) => LuaValue::String(lua.create_string(s)?),
                    None => LuaValue::Nil,
                },
                "dropped_attributes_count" => {
                    LuaValue::Integer(scope_dropped_attributes_count(&state) as i64)
                }
                _ => LuaValue::Nil,
            })
        });

        methods.add_meta_method(
            MetaMethod::NewIndex,
            |_, this, (key, value): (mlua::String, LuaValue)| -> mlua::Result<()> {
                let key = key.to_str()?;
                match key {
                    "name" => {
                        let s = require_string(&value, "name")?;
                        let mut state = this.state.borrow_mut();
                        if scope_name(&state) == s {
                            return Ok(());
                        }
                        ensure_modified(&mut state);
                        state.modified.as_mut().expect("just ensured Some above").name =
                            Bytes::copy_from_slice(s);
                        Ok(())
                    }
                    "version" => {
                        let s = require_string(&value, "version")?;
                        let mut state = this.state.borrow_mut();
                        if scope_version(&state) == s {
                            return Ok(());
                        }
                        ensure_modified(&mut state);
                        state.modified.as_mut().expect("just ensured Some above").version =
                            Bytes::copy_from_slice(s);
                        Ok(())
                    }
                    "schema_url" => {
                        // Byte-equality no-op check, as `resource::write_schema_url`: an
                        // `Option<Bytes>` has no `Value` variant for `lua_value_matches` to keep.
                        let new_value: Option<&[u8]> = match &value {
                            LuaValue::Nil => None,
                            LuaValue::String(s) => Some(s.as_bytes()),
                            other => {
                                return Err(mlua::Error::RuntimeError(format!(
                                    "scope.schema_url must be a string or nil, got {}",
                                    other.type_name()
                                )))
                            }
                        };
                        let mut state = this.state.borrow_mut();
                        let current = scope_schema_url(&state).map(|b| b.as_ref());
                        if current == new_value {
                            return Ok(());
                        }
                        ensure_modified(&mut state);
                        state.modified.as_mut().expect("just ensured Some above").schema_url =
                            new_value.map(Bytes::copy_from_slice);
                        Ok(())
                    }
                    "dropped_attributes_count" => Err(mlua::Error::RuntimeError(
                        "scope.dropped_attributes_count is read-only".to_string(),
                    )),
                    "attributes" => {
                        Err(mlua::Error::RuntimeError("scope.attributes is read-only".to_string()))
                    }
                    other => {
                        Err(mlua::Error::RuntimeError(format!("scope has no field '{other}'")))
                    }
                }
            },
        );

        // LuaJIT has no `__pairs`, so `scope:to_table()` is how a script enumerates.
        methods.add_method("to_table", |lua, this, ()| {
            let state = this.state.borrow();
            let table = lua.create_table()?;
            table.set("name", lua.create_string(scope_name(&state))?)?;
            table.set("version", lua.create_string(scope_version(&state))?)?;
            if let Some(url) = scope_schema_url(&state) {
                table.set("schema_url", lua.create_string(url)?)?;
            }
            let attributes = match &state.modified {
                Some(s) => attrmap_to_lua_table(lua, &s.attributes)?,
                None => match &state.base {
                    Some(b) => attrmap_to_lua_table(lua, &b.attributes)?,
                    None => lua.create_table()?,
                },
            };
            table.set("attributes", attributes)?;
            table.set("dropped_attributes_count", scope_dropped_attributes_count(&state) as i64)?;
            Ok(table)
        });
    }
}

/// The `scope.attributes` sub-userdata, over its parent [`ScopeProxy`]'s state, with
/// `crate::proxy::AttrsProxy`'s attribute semantics.
struct ScopeAttrsProxy(Rc<RefCell<ScopeState>>);

impl UserData for ScopeAttrsProxy {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_meta_method(MetaMethod::Index, |lua, this, key: mlua::String| {
            let key = key.to_str()?;
            let state = this.0.borrow();
            let value = match &state.modified {
                Some(s) => s.attributes.get(key),
                None => state.base.as_ref().and_then(|b| b.attributes.get(key)),
            };
            match value {
                Some(v) => value_to_lua(lua, v),
                None => Ok(LuaValue::Nil),
            }
        });

        methods.add_meta_method(
            MetaMethod::NewIndex,
            |_, this, (key, value): (mlua::String, LuaValue)| {
                let key = key.to_str()?;
                // Same no-op check and borrow ordering as `crate::proxy::AttrsProxy::__newindex`:
                // the borrow must be released before `lua_to_value`, which can re-enter Lua.
                let is_noop = {
                    let state = this.0.borrow();
                    let existing = match &state.modified {
                        Some(s) => s.attributes.get(key),
                        None => state.base.as_ref().and_then(|b| b.attributes.get(key)),
                    };
                    existing.is_some_and(|existing| lua_value_matches(existing, &value))
                };
                if is_noop {
                    return Ok(());
                }
                let value = lua_to_scope_value(value)?;
                let mut state = this.0.borrow_mut();
                ensure_modified(&mut state);
                state
                    .modified
                    .as_mut()
                    .expect("just ensured Some above")
                    .attributes
                    .insert(key, value);
                Ok(())
            },
        );

        methods.add_method("to_table", |lua, this, ()| {
            let state = this.0.borrow();
            match &state.modified {
                Some(s) => attrmap_to_lua_table(lua, &s.attributes),
                None => match &state.base {
                    Some(b) => attrmap_to_lua_table(lua, &b.attributes),
                    None => attrmap_to_lua_table(lua, &logit_core::AttrMap::new()),
                },
            }
        });
    }
}

/// `lua_to_value` with its error text relabeled for a `scope` attribute write, as
/// `crate::resource::lua_to_resource_value` does.
fn lua_to_scope_value(value: LuaValue) -> mlua::Result<logit_core::Value> {
    lua_to_value(value).map_err(|err| match err {
        mlua::Error::RuntimeError(msg) => {
            mlua::Error::RuntimeError(msg.replace("event attribute value", "scope attribute value"))
        }
        other => other,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, Value};

    fn scope(name: &str, version: &str, pairs: &[(&str, &str)]) -> Arc<Scope> {
        let mut attrs = AttrMap::new();
        for (k, v) in pairs {
            attrs.insert(k, *v);
        }
        Arc::new(Scope {
            name: Bytes::copy_from_slice(name.as_bytes()),
            version: Bytes::copy_from_slice(version.as_bytes()),
            attributes: attrs,
            ..Default::default()
        })
    }

    #[test]
    fn installs_empty_before_any_set() {
        let lua = Lua::new();
        let state = install(&lua).unwrap();

        lua.load(
            "seen_name = scope.name \
             seen_version = scope.version \
             seen_schema_url = scope.schema_url \
             seen_dropped = scope.dropped_attributes_count",
        )
        .exec()
        .unwrap();
        let name: String = lua.globals().get("seen_name").unwrap();
        let version: String = lua.globals().get("seen_version").unwrap();
        assert_eq!(name, "");
        assert_eq!(version, "");
        assert!(lua.globals().get::<_, LuaValue>("seen_schema_url").unwrap().is_nil());
        let dropped: i64 = lua.globals().get("seen_dropped").unwrap();
        assert_eq!(dropped, 0);

        assert!(take(&state).is_none());
    }

    #[test]
    fn set_then_read_sees_the_batch_scope_and_take_is_none() {
        let lua = Lua::new();
        let state = install(&lua).unwrap();
        set(&state, &Some(scope("nginx-otel-module", "1.0.0", &[])));

        lua.load(
            "seen_name = scope.name \
             seen_version = scope.version",
        )
        .exec()
        .unwrap();
        let name: String = lua.globals().get("seen_name").unwrap();
        let version: String = lua.globals().get("seen_version").unwrap();
        assert_eq!(name, "nginx-otel-module");
        assert_eq!(version, "1.0.0");

        assert!(take(&state).is_none(), "a read-only batch must not report a write");
    }

    #[test]
    fn a_name_write_is_visible_immediately_and_take_commits_it_other_fields_carried_over() {
        let lua = Lua::new();
        let state = install(&lua).unwrap();
        set(&state, &Some(scope("nginx-otel-module", "1.0.0", &[("k", "v")])));

        lua.load(r#"scope.name = "renamed""#).exec().unwrap();

        lua.load("still_there = scope.name").exec().unwrap();
        let still_there: String = lua.globals().get("still_there").unwrap();
        assert_eq!(still_there, "renamed");

        let committed = take(&state).expect("a write must report Some");
        assert_eq!(committed.name.as_ref(), b"renamed");
        assert_eq!(committed.version.as_ref(), b"1.0.0");
        assert_eq!(committed.attributes.get("k"), Some(&Value::str("v")));
    }

    #[test]
    fn an_attribute_write_through_scope_attributes_commits() {
        let lua = Lua::new();
        let state = install(&lua).unwrap();
        set(&state, &Some(scope("nginx-otel-module", "1.0.0", &[])));

        lua.load(r#"scope.attributes["k"] = "v""#).exec().unwrap();

        let committed = take(&state).expect("a write must report Some");
        assert_eq!(committed.attributes.get("k"), Some(&Value::str("v")));
        assert_eq!(committed.name.as_ref(), b"nginx-otel-module");
    }

    #[test]
    fn a_write_on_a_batch_without_a_scope_starts_from_default() {
        let lua = Lua::new();
        let state = install(&lua).unwrap();
        set(&state, &None);

        lua.load(r#"scope.name = "fresh""#).exec().unwrap();

        let committed = take(&state).expect("a write must report Some");
        assert_eq!(committed.name.as_ref(), b"fresh");
        assert_eq!(committed.version.as_ref(), b"");
        assert!(committed.schema_url.is_none());
        assert!(committed.attributes.is_empty());
    }

    #[test]
    fn an_identity_write_is_a_no_op() {
        let lua = Lua::new();
        let state = install(&lua).unwrap();
        set(&state, &Some(scope("nginx-otel-module", "1.0.0", &[("k", "v")])));

        lua.load(
            r#"
            scope.name = scope.name
            scope.version = scope.version
            scope.attributes.k = scope.attributes.k
            "#,
        )
        .exec()
        .unwrap();

        assert!(take(&state).is_none(), "an identity assignment must not count as a write");
    }

    #[test]
    fn dropped_attributes_count_is_read_only() {
        let lua = Lua::new();
        let state = install(&lua).unwrap();
        let mut s = (*scope("nginx-otel-module", "1.0.0", &[])).clone();
        s.dropped_attributes_count = 4;
        set(&state, &Some(Arc::new(s)));

        lua.load("seen = scope.dropped_attributes_count").exec().unwrap();
        let seen: i64 = lua.globals().get("seen").unwrap();
        assert_eq!(seen, 4);

        let err = lua.load("scope.dropped_attributes_count = 5").exec().unwrap_err();
        let message = format!("{err}");
        assert!(message.contains("scope.dropped_attributes_count is read-only"), "got: {message}");
    }

    #[test]
    fn to_table_snapshot() {
        let lua = Lua::new();
        let state = install(&lua).unwrap();
        let mut s = (*scope("nginx-otel-module", "1.0.0", &[("k", "v")])).clone();
        s.schema_url = Some(Bytes::from_static(b"https://example.com/schema"));
        s.dropped_attributes_count = 2;
        set(&state, &Some(Arc::new(s)));

        lua.load(
            r#"
            local t = scope:to_table()
            seen_name = t.name
            seen_version = t.version
            seen_schema_url = t.schema_url
            seen_attr = t.attributes.k
            seen_dropped = t.dropped_attributes_count
            "#,
        )
        .exec()
        .unwrap();
        let name: String = lua.globals().get("seen_name").unwrap();
        let version: String = lua.globals().get("seen_version").unwrap();
        let schema_url: String = lua.globals().get("seen_schema_url").unwrap();
        let attr: String = lua.globals().get("seen_attr").unwrap();
        let dropped: i64 = lua.globals().get("seen_dropped").unwrap();
        assert_eq!(name, "nginx-otel-module");
        assert_eq!(version, "1.0.0");
        assert_eq!(schema_url, "https://example.com/schema");
        assert_eq!(attr, "v");
        assert_eq!(dropped, 2);
    }
}
