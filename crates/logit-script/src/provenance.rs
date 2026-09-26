//! Exposes a batch's [`logit_core::Provenance`] (the component that created it, and the one this
//! component received it from) plus this worker's own component id, to Lua as a read-only
//! `provenance` userdata. See `docs/design/lua-api.md`'s "Reading provenance" section and
//! `docs/adr/batch-provenance-on-delivered.md`.
//!
//! Installed in [`crate::ScriptWorker::new`] before the script's source runs, for the reason in
//! `crate::trace`'s module doc. A top-level alias (`local p = provenance`) holds a reference to
//! this userdata, so later [`set`]/[`set_component`] calls are visible through it.
//!
//! **UserData, not a plain table like `trace`**, because a plain table accepts a script's write
//! until the next batch overwrites it. This mirrors `crate::resource`'s `__index`/`__newindex`
//! shape, but `__newindex` rejects every write.

use mlua::{Lua, MetaMethod, UserData, UserDataMethods, Value as LuaValue};
use std::cell::RefCell;
use std::rc::Rc;

/// State shared by [`crate::ScriptWorker`] and the [`ProvenanceProxy`] userdata, so [`set`] and
/// [`set_component`] can mutate it without a `&Lua` (as `crate::resource`'s `ResourceState`).
pub(crate) struct ProvenanceState {
    /// This worker's component id; `None` until [`crate::ScriptWorker::with_component`] sets it.
    component: Option<String>,
    origin: Option<String>,
    previous: Option<String>,
}

/// Creates the `provenance` global, every field `nil` until set, and returns its shared state.
pub(crate) fn install(lua: &Lua) -> mlua::Result<Rc<RefCell<ProvenanceState>>> {
    let state =
        Rc::new(RefCell::new(ProvenanceState { component: None, origin: None, previous: None }));
    let proxy = lua.create_userdata(ProvenanceProxy(state.clone()))?;
    lua.globals().set("provenance", proxy)?;
    Ok(state)
}

/// Overwrites `origin`/`previous`.
///
/// Called once per incoming batch before its events reach `process`, and before every `flush()`
/// with the flushing component as both: the root context a flush runs in
/// (`docs/adr/lua-flush-root-context.md`).
pub(crate) fn set(state: &Rc<RefCell<ProvenanceState>>, provenance: logit_core::Provenance) {
    let mut state = state.borrow_mut();
    state.origin = provenance.origin_str().map(str::to_string);
    state.previous = provenance.previous_str().map(str::to_string);
}

/// Sets `provenance.component`, from [`crate::ScriptWorker::with_component`].
pub(crate) fn set_component(state: &Rc<RefCell<ProvenanceState>>, id: &str) {
    state.borrow_mut().component = Some(id.to_string());
}

/// Runs `f` with this worker's component id, `None` before
/// [`crate::ScriptWorker::with_component`]. `crate::print` tags its self-log line with it.
pub(crate) fn with_component<R>(
    state: &Rc<RefCell<ProvenanceState>>,
    f: impl FnOnce(Option<&str>) -> R,
) -> R {
    f(state.borrow().component.as_deref())
}

/// The `provenance` global's userdata.
struct ProvenanceProxy(Rc<RefCell<ProvenanceState>>);

impl UserData for ProvenanceProxy {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_meta_method(MetaMethod::Index, |lua, this, key: mlua::String| {
            let state = this.0.borrow();
            let value = match key.to_str()? {
                "component" => &state.component,
                "origin" => &state.origin,
                "previous" => &state.previous,
                _ => return Ok(LuaValue::Nil),
            };
            Ok(match value {
                Some(s) => LuaValue::String(lua.create_string(s)?),
                None => LuaValue::Nil,
            })
        });

        // Every write fails. A known field says "read-only" and an unknown one says "no field",
        // as `crate::proxy::EventProxy` does for `event.has_log`, so a script can tell a
        // forbidden write from a typo.
        methods.add_meta_method(
            MetaMethod::NewIndex,
            |_, _this, (key, _value): (mlua::String, LuaValue)| -> mlua::Result<()> {
                let key = key.to_str()?;
                match key {
                    "component" | "origin" | "previous" => {
                        Err(mlua::Error::RuntimeError(format!("provenance.{key} is read-only")))
                    }
                    other => {
                        Err(mlua::Error::RuntimeError(format!("provenance has no field '{other}'")))
                    }
                }
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provenance(origin: &str, previous: &str) -> logit_core::Provenance {
        logit_core::Provenance {
            origin: Some(logit_core::interner::intern(origin)),
            previous: Some(logit_core::interner::intern(previous)),
        }
    }

    #[test]
    fn installs_every_field_nil_before_any_set_or_with_component() {
        let lua = Lua::new();
        install(&lua).unwrap();
        lua.load(
            "seen_component = provenance.component \
             seen_origin = provenance.origin \
             seen_previous = provenance.previous",
        )
        .exec()
        .unwrap();
        assert!(lua.globals().get::<_, LuaValue>("seen_component").unwrap().is_nil());
        assert!(lua.globals().get::<_, LuaValue>("seen_origin").unwrap().is_nil());
        assert!(lua.globals().get::<_, LuaValue>("seen_previous").unwrap().is_nil());
    }

    #[test]
    fn set_then_read_sees_origin_and_previous() {
        let lua = Lua::new();
        let state = install(&lua).unwrap();
        set(&state, provenance("nginx_in", "parse_json"));

        lua.load(
            "seen_origin = provenance.origin \
             seen_previous = provenance.previous",
        )
        .exec()
        .unwrap();
        let origin: String = lua.globals().get("seen_origin").unwrap();
        let previous: String = lua.globals().get("seen_previous").unwrap();
        assert_eq!(origin, "nginx_in");
        assert_eq!(previous, "parse_json");
    }

    #[test]
    fn set_component_then_read_sees_it() {
        let lua = Lua::new();
        let state = install(&lua).unwrap();
        set_component(&state, "enrich");

        lua.load("seen = provenance.component").exec().unwrap();
        let seen: String = lua.globals().get("seen").unwrap();
        assert_eq!(seen, "enrich");
    }

    /// A top-level alias holds the userdata, not a snapshot, so later sets are visible through it.
    #[test]
    fn a_top_level_alias_sees_a_later_set() {
        let lua = Lua::new();
        let state = install(&lua).unwrap();
        lua.load("alias = provenance").exec().unwrap();

        set(&state, provenance("nginx_in", "parse_json"));
        set_component(&state, "enrich");

        lua.load(
            "seen_origin = alias.origin \
             seen_component = alias.component",
        )
        .exec()
        .unwrap();
        let origin: String = lua.globals().get("seen_origin").unwrap();
        let component: String = lua.globals().get("seen_component").unwrap();
        assert_eq!(origin, "nginx_in");
        assert_eq!(component, "enrich");
    }

    #[test]
    fn writing_a_known_field_is_reported_as_read_only_not_a_missing_field() {
        let lua = Lua::new();
        install(&lua).unwrap();
        let err = lua.load(r#"provenance.origin = "forged""#).exec().unwrap_err();
        let message = format!("{err}");
        assert!(message.contains("read-only"), "got: {message}");
        assert!(!message.contains("no field"), "should report read-only, not a nonexistent field");
    }

    #[test]
    fn writing_an_unknown_field_is_reported_as_a_missing_field() {
        let lua = Lua::new();
        install(&lua).unwrap();
        let err = lua.load(r#"provenance.bogus = "x""#).exec().unwrap_err();
        let message = format!("{err}");
        assert!(message.contains("no field"), "got: {message}");
    }
}
