//! Exposes the incoming batch's resource to Lua as a global `resource` userdata -- readable and
//! writable, copy-on-write. See `docs/design/lua-api.md`'s "Reading and writing `resource`"
//! section and `docs/adr/operator-declared-resource-attributes.md`.
//!
//! Installed unconditionally in [`crate::ScriptWorker::new`], before the script's own source
//! runs -- same reasoning as `crate::trace`'s module doc: a top-level alias (`local r = resource`)
//! captures whatever `resource` *is* at that instant, once, forever, and Lua resolves a
//! function-body global lookup at call time but a top-level statement only once, during
//! `Lua::load(source).exec()`.
//!
//! Unlike `trace` (a plain table `set_context` overwrites in place), `resource` needs proxy
//! semantics -- it wraps a real [`Resource`]'s [`AttrMap`], the same shape `crate::proxy`'s
//! `AttrsProxy` gives `event.attributes` -- so this mirrors that module's `__index`/`__newindex`
//! pattern instead. State is a plain `Rc<RefCell<..>>`, not a `RegistryKey`-held table: `Resource`
//! itself must cross back out to [`crate::ScriptWorker::set_resource`]/`take_resource` without a
//! `&Lua` in hand, which a `Rc` clone gives for free and a registry lookup would not.

use crate::value::{attrmap_to_lua_table, lua_to_value, lua_value_matches, value_to_lua};
use logit_core::{AttrMap, Resource};
use mlua::{Lua, MetaMethod, UserData, UserDataMethods, Value as LuaValue};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

/// Per-batch resource state, shared between [`crate::ScriptWorker`] and the installed
/// [`ResourceProxy`] userdata through one `Rc<RefCell<..>>` -- a script mutating through the
/// proxy is immediately visible to `take` below, with no trip back through Lua required.
pub(crate) struct ResourceState {
    base: Arc<Resource>,
    /// `Some` once a script has written at least one key since the last [`set`] -- a copy of
    /// `base`'s attributes, mutated in place from there. `None` (the common case: a script that
    /// never writes `resource`) is what keeps this allocation-free.
    modified: Option<AttrMap>,
}

/// Creates the `resource` global (starting empty, like `crate::trace::install`'s all-zero
/// placeholder -- no batch has been seen yet) and returns the shared state [`set`]/[`take`]
/// mutate directly.
pub(crate) fn install(lua: &Lua) -> mlua::Result<Rc<RefCell<ResourceState>>> {
    let state = Rc::new(RefCell::new(ResourceState {
        base: Arc::new(Resource::default()),
        modified: None,
    }));
    let proxy = lua.create_userdata(ResourceProxy(state.clone()))?;
    lua.globals().set("resource", proxy)?;
    Ok(state)
}

/// Called once per incoming batch, before any of its events reach `process` -- resets `resource`
/// to read `resource`'s attributes and clears any write left over from a previous batch.
pub(crate) fn set(state: &Rc<RefCell<ResourceState>>, resource: &Arc<Resource>) {
    let mut state = state.borrow_mut();
    state.base = resource.clone();
    state.modified = None;
}

/// `Some` if a script wrote `resource` since the last [`set`], committing that write as the new
/// `base` so a later read (inside the same batch's remaining `process()` calls, or a `flush()`
/// that runs before the next `set`) sees it too. `None` -- the common case -- costs nothing.
pub(crate) fn take(state: &Rc<RefCell<ResourceState>>) -> Option<Arc<Resource>> {
    let mut state = state.borrow_mut();
    let modified = state.modified.take()?;
    let new = Arc::new(Resource { attributes: modified });
    state.base = new.clone();
    Some(new)
}

/// The `resource` global's userdata. Shares `ResourceState` with [`install`]'s caller.
struct ResourceProxy(Rc<RefCell<ResourceState>>);

impl UserData for ResourceProxy {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_meta_method(MetaMethod::Index, |lua, this, key: mlua::String| {
            let key = key.to_str()?;
            let state = this.0.borrow();
            let value = match &state.modified {
                Some(attrs) => attrs.get(key),
                None => state.base.attributes.get(key),
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
                // ordering, as `crate::proxy::AttrsProxy::__newindex` -- a table value's `pairs()`
                // walk inside `lua_to_value` can re-enter Lua and hit this same `__index`, so the
                // borrow below must not still be held when that happens.
                let is_noop = {
                    let state = this.0.borrow();
                    let existing = match &state.modified {
                        Some(attrs) => attrs.get(key),
                        None => state.base.attributes.get(key),
                    };
                    existing.is_some_and(|existing| lua_value_matches(existing, &value))
                };
                if is_noop {
                    return Ok(());
                }
                let value = lua_to_resource_value(value)?;
                let mut state = this.0.borrow_mut();
                if state.modified.is_none() {
                    state.modified = Some(state.base.attributes.clone());
                }
                state.modified.as_mut().expect("just set to Some above").insert(key, value);
                Ok(())
            },
        );

        // No __pairs (unavailable under LuaJIT, same as `AttrsProxy`): `resource:to_table()` is
        // the enumeration escape hatch.
        methods.add_method("to_table", |lua, this, ()| {
            let state = this.0.borrow();
            match &state.modified {
                Some(attrs) => attrmap_to_lua_table(lua, attrs),
                None => attrmap_to_lua_table(lua, &state.base.attributes),
            }
        });
    }
}

/// `lua_to_value`, relabeled for a `resource` write: its error text says "as an event attribute
/// value" (written for `AttrsProxy`, its only caller until now), which would be misleading for a
/// value rejected here. A string replace rather than a parameter on the shared function -- the
/// message is the only thing that differs, and `lua_to_value` is also called recursively from
/// nested-table conversion, where threading a context string through adds real complexity for one
/// cosmetic line.
fn lua_to_resource_value(value: LuaValue) -> mlua::Result<logit_core::Value> {
    lua_to_value(value).map_err(|err| match err {
        mlua::Error::RuntimeError(msg) => mlua::Error::RuntimeError(
            msg.replace("event attribute value", "resource attribute value"),
        ),
        other => other,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::Value;

    fn resource(pairs: &[(&str, &str)]) -> Arc<Resource> {
        let mut attrs = AttrMap::new();
        for (k, v) in pairs {
            attrs.insert(k, *v);
        }
        Arc::new(Resource { attributes: attrs })
    }

    #[test]
    fn installs_empty_before_any_set() {
        let lua = Lua::new();
        let state = install(&lua).unwrap();
        assert!(state.borrow().base.attributes.is_empty());
        assert!(state.borrow().modified.is_none());
    }

    #[test]
    fn set_then_read_sees_the_new_base_and_take_is_none() {
        let lua = Lua::new();
        let state = install(&lua).unwrap();
        set(&state, &resource(&[("service.name", "nginx")]));

        lua.load("seen = resource[\"service.name\"]").exec().unwrap();
        let seen: String = lua.globals().get("seen").unwrap();
        assert_eq!(seen, "nginx");
        assert!(take(&state).is_none(), "a read-only batch must not report a write");
    }

    #[test]
    fn a_write_is_visible_immediately_and_take_commits_it() {
        let lua = Lua::new();
        let state = install(&lua).unwrap();
        set(&state, &resource(&[("service.name", "nginx")]));

        lua.load(r#"resource["service.namespace"] = "demo""#).exec().unwrap();

        let committed = take(&state).expect("a write must report Some");
        assert_eq!(committed.attributes.get("service.name"), Some(&Value::str("nginx")));
        assert_eq!(committed.attributes.get("service.namespace"), Some(&Value::str("demo")));

        // Committed as the new base: a later read (no intervening `set`) sees it too.
        lua.load("still_there = resource[\"service.namespace\"]").exec().unwrap();
        let still_there: String = lua.globals().get("still_there").unwrap();
        assert_eq!(still_there, "demo");
    }

    #[test]
    fn an_identity_assignment_is_a_no_op_and_take_stays_none() {
        let lua = Lua::new();
        let state = install(&lua).unwrap();
        set(&state, &resource(&[("service.name", "nginx")]));

        lua.load(r#"resource["service.name"] = resource["service.name"]"#).exec().unwrap();

        assert!(take(&state).is_none(), "an identity assignment must not count as a write");
    }
}
