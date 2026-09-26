//! Exposes the incoming batch's resource to Lua as a global `resource` userdata, readable and
//! writable, copy-on-write. See `docs/design/lua-api.md`'s "Reading and writing `resource`"
//! section and `docs/adr/operator-declared-resource-attributes.md`.
//!
//! Installed in [`crate::ScriptWorker::new`] before the script's source runs, for the reason in
//! `crate::trace`'s module doc.
//!
//! Unlike `trace`, `resource` wraps a real [`Resource`]'s [`AttrMap`], so it follows
//! `crate::proxy`'s `AttrsProxy` `__index`/`__newindex` pattern. State is an `Rc<RefCell<..>>`,
//! not a `RegistryKey`-held table, because [`crate::ScriptWorker::set_resource`]/`take_resource`
//! must reach it without a `&Lua`.

use crate::value::{
    attribute_error, attrmap_to_lua_table, lua_to_value, lua_value_matches, value_to_lua,
};
use logit_core::Resource;
use mlua::{Lua, MetaMethod, UserData, UserDataMethods, Value as LuaValue};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

/// Per-batch resource state, shared by [`crate::ScriptWorker`] and the [`ResourceProxy`]
/// userdata, so a script's write is visible to [`take`] without a trip through Lua.
pub(crate) struct ResourceState {
    base: Arc<Resource>,
    /// A full copy of `base`, every field included, made on the first write since the last
    /// [`set`] and mutated in place after. `None`, a script that never writes `resource`, is
    /// what keeps this allocation-free.
    modified: Option<Resource>,
}

/// Creates the `resource` global, empty until the first [`set`], and returns its shared state.
pub(crate) fn install(lua: &Lua) -> mlua::Result<Rc<RefCell<ResourceState>>> {
    let state = Rc::new(RefCell::new(ResourceState {
        base: Arc::new(Resource::default()),
        modified: None,
    }));
    let proxy = lua.create_userdata(ResourceProxy(state.clone()))?;
    lua.globals().set("resource", proxy)?;
    Ok(state)
}

/// Resets `resource` to read `resource` and discards any earlier write.
///
/// Called once per incoming batch before its events reach `process`, and before every `flush()`
/// with an empty resource, the flush root context (`docs/adr/lua-flush-root-context.md`).
pub(crate) fn set(state: &Rc<RefCell<ResourceState>>, resource: &Arc<Resource>) {
    let mut state = state.borrow_mut();
    state.base = resource.clone();
    state.modified = None;
}

/// The script's write since the last [`set`], if any.
///
/// Commits the write as the new `base`, so a read before the next [`set`] still sees it and a
/// second call returns `None`. `run_lua` calls [`set`] before every batch and every `flush()`, so
/// no write carries into the next call.
pub(crate) fn take(state: &Rc<RefCell<ResourceState>>) -> Option<Arc<Resource>> {
    let mut state = state.borrow_mut();
    let modified = state.modified.take()?;
    let new = Arc::new(modified);
    state.base = new.clone();
    Some(new)
}

/// The `resource` global's userdata.
struct ResourceProxy(Rc<RefCell<ResourceState>>);

impl UserData for ResourceProxy {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_meta_method(MetaMethod::Index, |lua, this, key: mlua::String| {
            let key = key.to_str()?;
            let state = this.0.borrow();
            // A named field wins over an attribute of the same name: an attribute called
            // `schema_url` or `dropped_attributes_count` appears in `resource:to_table()` but not
            // through `resource[...]`, the same trade `event`'s fixed fields make.
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
                // Same no-op check and borrow ordering as `crate::proxy::AttrsProxy::__newindex`:
                // conversion reads raw and runs no metamethod; the borrow is still released first.
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
                let value = lua_to_resource_value(value)
                    .map_err(|err| attribute_error("resource", key, err))?;
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

        // LuaJIT has no `__pairs`, so `resource:to_table()` is how a script enumerates. It
        // returns attributes only; `schema_url`/`dropped_attributes_count` are read by name.
        methods.add_method("to_table", |lua, this, ()| {
            let state = this.0.borrow();
            match &state.modified {
                Some(r) => attrmap_to_lua_table(lua, &r.attributes),
                None => attrmap_to_lua_table(lua, &state.base.attributes),
            }
        });
    }
}

/// Clones `state.base` into `state.modified` on the first write since the last [`set`].
///
/// Every write path goes through this, so a write to one field never discards an earlier write
/// to another in the same batch.
fn ensure_modified(state: &mut ResourceState) {
    if state.modified.is_none() {
        state.modified = Some((*state.base).clone());
    }
}

/// `resource.schema_url = <string|nil>`; `nil` clears it.
///
/// The no-op check is plain byte equality, not `lua_value_matches`: `schema_url` is an
/// `Option<Bytes>`, not a `Value`, so there's no variant to preserve.
fn write_schema_url(state: &Rc<RefCell<ResourceState>>, value: LuaValue) -> mlua::Result<()> {
    // Copy into `Bytes` only once the write is known not to be a no-op.
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

/// `lua_to_value` with its "event attribute value" error text relabeled for a `resource` write.
///
/// A string replace, not a parameter, because `lua_to_value` recurses through nested tables and
/// the message is the only difference.
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
