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
use logit_core::Resource;
use mlua::{Lua, MetaMethod, UserData, UserDataMethods, Value as LuaValue};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

/// Per-batch resource state, shared between [`crate::ScriptWorker`] and the installed
/// [`ResourceProxy`] userdata through one `Rc<RefCell<..>>` -- a script mutating through the
/// proxy is immediately visible to `take` below, with no trip back through Lua required.
pub(crate) struct ResourceState {
    base: Arc<Resource>,
    /// `Some` once a script has written at least one field since the last [`set`] -- a full copy
    /// of `base` (attributes, `schema_url`, and `dropped_attributes_count` alike), mutated in
    /// place from there so one clone on the first write covers every field, not just attributes.
    /// `None` (the common case: a script that never writes `resource`) is what keeps this
    /// allocation-free. Widened from `Option<AttrMap>` (W7): `schema_url`/`dropped_attributes_count`
    /// are now themselves writable (`schema_url`) or need to be read back out of `modified`
    /// (`dropped_attributes_count`, read-only but still stored here so [`take`] doesn't need to
    /// special-case where each field comes from).
    modified: Option<Resource>,
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
    let new = Arc::new(modified);
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
            // Named fields take precedence over the open attribute map for these two keys -- an
            // attribute literally named `schema_url` (or `dropped_attributes_count`) is still in
            // `resource:to_table()`, but unreachable via `resource["schema_url"]`: the named
            // field always wins the lookup. Documented rather than guarded against, the same
            // trade `event`'s fixed fields (`timestamp`, `attributes`, ...) already make against
            // an attribute of the same name.
            match key {
                "schema_url" => {
                    return match &state.modified {
                        Some(r) => Ok(match &r.schema_url {
                            Some(s) => LuaValue::String(lua.create_string(s)?),
                            None => LuaValue::Nil,
                        }),
                        None => Ok(match &state.base.schema_url {
                            Some(s) => LuaValue::String(lua.create_string(s)?),
                            None => LuaValue::Nil,
                        }),
                    };
                }
                "dropped_attributes_count" => {
                    let count = match &state.modified {
                        Some(r) => r.dropped_attributes_count,
                        None => state.base.dropped_attributes_count,
                    };
                    return Ok(LuaValue::Integer(count as i64));
                }
                _ => {}
            }
            let value = match &state.modified {
                Some(r) => r.attributes.get(key),
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
                match key {
                    "schema_url" => return write_schema_url(&this.0, value),
                    "dropped_attributes_count" => {
                        return Err(mlua::Error::RuntimeError(
                            "resource.dropped_attributes_count is read-only".to_string(),
                        ))
                    }
                    _ => {}
                }
                // Same no-op check, and the same borrow-then-release-before-`lua_to_value`
                // ordering, as `crate::proxy::AttrsProxy::__newindex` -- a table value's `pairs()`
                // walk inside `lua_to_value` can re-enter Lua and hit this same `__index`, so the
                // borrow below must not still be held when that happens.
                let is_noop = {
                    let state = this.0.borrow();
                    let existing = match &state.modified {
                        Some(r) => r.attributes.get(key),
                        None => state.base.attributes.get(key),
                    };
                    existing.is_some_and(|existing| lua_value_matches(existing, &value))
                };
                if is_noop {
                    return Ok(());
                }
                let value = lua_to_resource_value(value)?;
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

        // No __pairs (unavailable under LuaJIT, same as `AttrsProxy`): `resource:to_table()` is
        // the enumeration escape hatch. Attributes only, unchanged by the named `schema_url`/
        // `dropped_attributes_count` fields above -- callers that want those read them directly
        // off `resource`.
        methods.add_method("to_table", |lua, this, ()| {
            let state = this.0.borrow();
            match &state.modified {
                Some(r) => attrmap_to_lua_table(lua, &r.attributes),
                None => attrmap_to_lua_table(lua, &state.base.attributes),
            }
        });
    }
}

/// Starts `state.modified` from a clone of `state.base` if this is the first write since the
/// last [`set`] -- shared by every write path (`__newindex`'s attribute arm and
/// [`write_schema_url`]) so a write to one field never discards an earlier write to another
/// within the same batch.
fn ensure_modified(state: &mut ResourceState) {
    if state.modified.is_none() {
        state.modified = Some((*state.base).clone());
    }
}

/// `resource.schema_url = <string|nil>` -- a nil write clears it. String-equality no-op check
/// (not `lua_value_matches`: `schema_url` is a plain `Option<Bytes>` field, not an attribute
/// `Value`, so there's no variant-preservation concern to guard).
fn write_schema_url(state: &Rc<RefCell<ResourceState>>, value: LuaValue) -> mlua::Result<()> {
    // Borrow the Lua string's bytes for the identity check; only copy them into a `Bytes` once
    // it's certain the write isn't a no-op -- the same allocation-free no-op path the attribute
    // write above has.
    let new_value: Option<&[u8]> = match &value {
        LuaValue::Nil => None,
        LuaValue::String(s) => Some(s.as_bytes()),
        other => {
            return Err(mlua::Error::RuntimeError(format!(
                "resource.schema_url must be a string or nil, got {}",
                other.type_name()
            )))
        }
    };
    let mut state = state.borrow_mut();
    let current = match &state.modified {
        Some(r) => r.schema_url.as_ref(),
        None => state.base.schema_url.as_ref(),
    };
    if current.map(|b| b.as_ref()) == new_value {
        return Ok(());
    }
    ensure_modified(&mut state);
    state.modified.as_mut().expect("just ensured Some above").schema_url =
        new_value.map(bytes::Bytes::copy_from_slice);
    Ok(())
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
    use logit_core::{AttrMap, Value};

    fn resource(pairs: &[(&str, &str)]) -> Arc<Resource> {
        let mut attrs = AttrMap::new();
        for (k, v) in pairs {
            attrs.insert(k, *v);
        }
        Arc::new(Resource { attributes: attrs, ..Default::default() })
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

    #[test]
    fn schema_url_read_write_and_nil_clear_commit_through_take_with_attributes_untouched() {
        let lua = Lua::new();
        let state = install(&lua).unwrap();
        let mut r = (*resource(&[("service.name", "nginx")])).clone();
        r.schema_url = Some(bytes::Bytes::from_static(b"https://example.com/schema"));
        set(&state, &Arc::new(r));

        lua.load(
            r#"
            before = resource.schema_url
            resource.schema_url = "https://example.com/new-schema"
            after = resource.schema_url
            resource.schema_url = nil
            cleared = resource.schema_url
            "#,
        )
        .exec()
        .unwrap();
        let before: String = lua.globals().get("before").unwrap();
        assert_eq!(before, "https://example.com/schema");
        let after: String = lua.globals().get("after").unwrap();
        assert_eq!(after, "https://example.com/new-schema");
        assert!(lua.globals().get::<_, LuaValue>("cleared").unwrap().is_nil());

        let committed = take(&state).expect("a write must report Some");
        assert!(committed.schema_url.is_none());
        assert_eq!(committed.attributes.get("service.name"), Some(&Value::str("nginx")));
    }

    #[test]
    fn dropped_attributes_count_reads_and_a_write_errors() {
        let lua = Lua::new();
        let state = install(&lua).unwrap();
        let mut r = (*resource(&[])).clone();
        r.dropped_attributes_count = 3;
        set(&state, &Arc::new(r));

        lua.load("seen = resource.dropped_attributes_count").exec().unwrap();
        let seen: i64 = lua.globals().get("seen").unwrap();
        assert_eq!(seen, 3);

        let err = lua.load("resource.dropped_attributes_count = 5").exec().unwrap_err();
        let message = format!("{err}");
        assert!(
            message.contains("resource.dropped_attributes_count is read-only"),
            "got: {message}"
        );
    }

    #[test]
    fn an_attribute_write_still_carries_schema_url_and_dropped_attributes_count_over_unchanged() {
        let lua = Lua::new();
        let state = install(&lua).unwrap();
        let mut r = (*resource(&[("service.name", "nginx")])).clone();
        r.schema_url = Some(bytes::Bytes::from_static(b"https://example.com/schema"));
        r.dropped_attributes_count = 2;
        set(&state, &Arc::new(r));

        lua.load(r#"resource["service.namespace"] = "demo""#).exec().unwrap();

        let committed = take(&state).expect("a write must report Some");
        assert_eq!(
            committed.schema_url,
            Some(bytes::Bytes::from_static(b"https://example.com/schema"))
        );
        assert_eq!(committed.dropped_attributes_count, 2);
        assert_eq!(committed.attributes.get("service.namespace"), Some(&Value::str("demo")));
    }

    #[test]
    fn an_identity_schema_url_write_is_a_no_op() {
        let lua = Lua::new();
        let state = install(&lua).unwrap();
        let mut r = (*resource(&[])).clone();
        r.schema_url = Some(bytes::Bytes::from_static(b"https://example.com/schema"));
        set(&state, &Arc::new(r));

        lua.load(r#"resource.schema_url = resource.schema_url"#).exec().unwrap();

        assert!(take(&state).is_none(), "an identity assignment must not count as a write");
    }
}
