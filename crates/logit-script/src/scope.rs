//! Exposes the incoming batch's OTLP instrumentation scope to Lua as a global `scope`
//! userdata -- readable and writable, copy-on-write. Mirrors `crate::resource`; see that module's
//! doc comment for the shared reasoning (install-before-`.exec()`, `Rc<RefCell<..>>` state instead
//! of a `RegistryKey`-held table, identity writes as no-ops) and `docs/design/lua-api.md`'s
//! "Reading and writing `resource`" section, whose shape this follows for `scope` too.
//!
//! **Unlike `resource`, a batch may have no scope at all** (`EventBatch.scope: Option<Arc<Scope>>`
//! -- `crate::event`). Reading any field before a write reports the all-clear value (`""` for
//! `name`/`version`, `nil` for `schema_url`, `0` for `dropped_attributes_count`, an empty table
//! for `attributes`) -- the same values [`logit_core::Scope::default`] itself carries. A write on
//! such a batch starts `modified` from `Scope::default()` rather than erroring.
//!
//! **`scope.attributes` is its own sub-userdata**, mirroring `event.attributes`'s `AttrsProxy`
//! (`crate::proxy`) -- same open-map `__index`/`__newindex`/`to_table` shape, sharing this same
//! `Rc<RefCell<ScopeState>>`. Unlike `EventProxy::attrs`, this doesn't need the
//! lazy-create-and-cache-via-`RegistryKey` dance that module documents at length: that laziness
//! exists there because a fresh `EventProxy` is created once *per event* (worth avoiding the
//! `create_userdata` cost when a script never touches `.attributes`) and the cached handle must
//! later be torn down cleanly (`EventProxy::into_inner`) before the event itself is handed back.
//! `scope` is installed exactly once, for a worker's entire lifetime, and is never "handed back"
//! or destructed the way an event is -- so `scope.attributes`'s userdata is simply created once,
//! here in [`install`], and its `RegistryKey` held directly as a [`ScopeProxy`] field for the
//! worker's whole lifetime. The simplest correct approach that's still right, not a shortcut.

use crate::value::{attrmap_to_lua_table, lua_to_value, lua_value_matches, value_to_lua};
use bytes::Bytes;
use logit_core::Scope;
use mlua::{Lua, MetaMethod, RegistryKey, UserData, UserDataMethods, Value as LuaValue};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

/// Per-batch scope state, shared between [`crate::ScriptWorker`], the installed [`ScopeProxy`]
/// userdata, and its `attributes` sub-userdata ([`ScopeAttrsProxy`]) through one
/// `Rc<RefCell<..>>` -- a script mutating through either handle is immediately visible to `take`
/// below, with no trip back through Lua required.
pub(crate) struct ScopeState {
    /// `None` before the first [`set`], and again for any batch that itself carries no scope --
    /// see the module doc comment.
    base: Option<Arc<Scope>>,
    /// `Some` once a script has written at least one field since the last [`set`] -- a full copy
    /// of `base` (or, if `base` is `None`, of `Scope::default()`), mutated in place from there so
    /// one clone on the first write covers every field. `None` (the common case: a script that
    /// never writes `scope`) is what keeps this allocation-free.
    modified: Option<Scope>,
}

/// Creates the `scope` global (starting with no batch's scope seen yet -- `base: None`, reading
/// as all-clear, like `crate::resource::install`'s empty placeholder) and returns the shared
/// state [`set`]/[`take`] mutate directly.
pub(crate) fn install(lua: &Lua) -> mlua::Result<Rc<RefCell<ScopeState>>> {
    let state = Rc::new(RefCell::new(ScopeState { base: None, modified: None }));
    let attrs_ud = lua.create_userdata(ScopeAttrsProxy(state.clone()))?;
    let attrs = lua.create_registry_value(attrs_ud)?;
    let proxy = lua.create_userdata(ScopeProxy { state: state.clone(), attrs })?;
    lua.globals().set("scope", proxy)?;
    Ok(state)
}

/// Called once per incoming batch, before any of its events reach `process` -- resets `scope` to
/// read the batch's own scope (or the all-clear defaults, if the batch has none) and clears any
/// write left over from a previous batch.
pub(crate) fn set(state: &Rc<RefCell<ScopeState>>, scope: &Option<Arc<Scope>>) {
    let mut state = state.borrow_mut();
    state.base = scope.clone();
    state.modified = None;
}

/// `Some` if a script wrote `scope` since the last [`set`], committing that write as the new
/// `base` so a later read (inside the same batch's remaining `process()` calls, or a `flush()`
/// that runs before the next `set`) sees it too. `None` -- the common case -- costs nothing.
pub(crate) fn take(state: &Rc<RefCell<ScopeState>>) -> Option<Arc<Scope>> {
    let mut state = state.borrow_mut();
    let modified = state.modified.take()?;
    let new = Arc::new(modified);
    state.base = Some(new.clone());
    Some(new)
}

/// `state.base`'s scope, or `Scope::default()` if the batch carried none -- the starting point
/// for `modified` on a write. Shared by every write path below so a write to one field never
/// discards an earlier write to another within the same batch, and so "the batch has no scope
/// yet" and "start a fresh one" share one definition.
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

/// `scope.name`/`scope.version = <string>` -- rejects anything else outright (there is no `nil`
/// meaning for either: an absent scope reads back `""`, not `nil`, so a script never needs to
/// write `nil` here the way it can for `schema_url`).
fn require_string<'a>(value: &'a LuaValue, field: &str) -> mlua::Result<&'a [u8]> {
    match value {
        LuaValue::String(s) => Ok(s.as_bytes()),
        other => Err(mlua::Error::RuntimeError(format!(
            "scope.{field} must be a string, got {}",
            other.type_name()
        ))),
    }
}

/// The `scope` global's userdata. Shares `ScopeState` with [`install`]'s caller and with
/// [`ScopeAttrsProxy`].
struct ScopeProxy {
    state: Rc<RefCell<ScopeState>>,
    /// `scope.attributes`'s sub-userdata, created once in [`install`] and cached for this
    /// worker's whole lifetime -- see the module doc comment for why, unlike `EventProxy::attrs`,
    /// this needs no lazy-create/`RefCell<Option<..>>`/teardown dance.
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
                        // Same string-equality no-op check as `resource::write_schema_url`, and
                        // for the same reason: this is a plain `Option<Bytes>` field, not an
                        // attribute `Value`, so `lua_value_matches` (which compares against a
                        // `Value`'s variant) doesn't apply here.
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

        // No __pairs (unavailable under LuaJIT, same as `AttrsProxy`/`ResourceProxy`):
        // `scope:to_table()` is the enumeration escape hatch.
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

/// The `scope.attributes` sub-object. Shares the same `Rc<RefCell<ScopeState>>` as its parent
/// [`ScopeProxy`] -- reads/writes through this proxy are reads/writes to that same scope. Mirrors
/// `crate::proxy::AttrsProxy` and `crate::resource::ResourceProxy`'s attribute handling exactly.
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
                // Same no-op check, and the same borrow-then-release-before-`lua_to_value`
                // ordering, as `crate::proxy::AttrsProxy::__newindex` -- see that method's
                // comment for why the borrow below must not still be held once `lua_to_value`
                // (which can re-enter Lua for a table value) runs.
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

/// `lua_to_value`, relabeled for a `scope` attribute write -- see
/// `crate::resource::lua_to_resource_value` for why this is a string replace rather than a
/// threaded parameter.
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
