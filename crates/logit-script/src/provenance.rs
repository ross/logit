//! Exposes a batch's [`logit_core::Provenance`] (which component created it, which component this
//! one received it from) plus this worker's own component id, to Lua as a global `provenance`
//! userdata -- **read-only**, unlike `resource`. See `docs/design/lua-api.md`'s "Reading
//! provenance" section and `docs/adr/batch-provenance-on-delivered.md`.
//!
//! Installed unconditionally in [`crate::ScriptWorker::new`], before the script's own source
//! runs -- same reasoning as `crate::trace`/`crate::resource`'s module docs: a top-level alias
//! (`local p = provenance`) captures whatever `provenance` *is* at that instant, once, forever;
//! installing first means that instant is "the placeholder," not "doesn't exist yet," and later
//! mutation (`set`/`ScriptWorker::with_component`) is visible through the alias because the
//! captured value is a reference to the same underlying userdata, not a copy.
//!
//! **UserData, not a plain table like `trace`.** `trace`'s own doc comment concedes a script's
//! write to it is silently accepted and only clobbered on the next batch -- that fails "never
//! modifiable" outright. This mirrors `crate::resource`'s `__index`/`__newindex` proxy shape
//! instead, but rejects every write in `__newindex` rather than accepting one: there is no
//! mutable half to this global the way there is for `resource`.

use mlua::{Lua, MetaMethod, UserData, UserDataMethods, Value as LuaValue};
use std::cell::RefCell;
use std::rc::Rc;

/// Shared between [`crate::ScriptWorker`] and the installed [`ProvenanceProxy`] userdata through
/// one `Rc<RefCell<..>>` -- the same shape `crate::resource`'s `ResourceState` uses, and for the
/// same reason: `set`/`ScriptWorker::with_component` need to mutate this without a `&Lua` in hand.
pub(crate) struct ProvenanceState {
    /// This worker's own component id -- set once, via [`crate::ScriptWorker::with_component`],
    /// and never changed again for the worker's whole lifetime. `None` until that builder is
    /// called (mirrors every other placeholder-before-first-real-value shape in this crate).
    component: Option<String>,
    origin: Option<String>,
    previous: Option<String>,
}

/// Creates the `provenance` global (every field `None`, like `crate::trace::install`'s all-zero
/// placeholder -- no batch has been seen yet, and no component id set yet either) and returns the
/// shared state [`set`]/[`crate::ScriptWorker::with_component`] mutate directly.
pub(crate) fn install(lua: &Lua) -> mlua::Result<Rc<RefCell<ProvenanceState>>> {
    let state =
        Rc::new(RefCell::new(ProvenanceState { component: None, origin: None, previous: None }));
    let proxy = lua.create_userdata(ProvenanceProxy(state.clone()))?;
    lua.globals().set("provenance", proxy)?;
    Ok(state)
}

/// Called once per incoming batch, before any of its events reach `process` -- overwrites
/// `origin`/`previous` in place with the batch's own [`logit_core::Provenance`]. Not called around
/// a `flush()` call, which keeps whatever was last set, exactly like `trace`/`resource`'s own
/// documented flush-time staleness (`docs/known-gaps.md`).
pub(crate) fn set(state: &Rc<RefCell<ProvenanceState>>, provenance: logit_core::Provenance) {
    let mut state = state.borrow_mut();
    state.origin = provenance.origin_str().map(str::to_string);
    state.previous = provenance.previous_str().map(str::to_string);
}

/// Called once, from [`crate::ScriptWorker::with_component`] -- sets `provenance.component` for
/// the rest of this worker's lifetime.
pub(crate) fn set_component(state: &Rc<RefCell<ProvenanceState>>, id: &str) {
    state.borrow_mut().component = Some(id.to_string());
}

/// The `provenance` global's userdata. Shares `ProvenanceState` with [`install`]'s caller.
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

        // Every field is read-only -- unlike `resource`'s `__newindex`, there is no accepted
        // write path here at all, matching `crate::proxy::EventProxy`'s "read-only, name it or
        // say no field" split (`event.has_log` etc): a known field reports itself as read-only,
        // an unknown one reports it has no field -- never the reverse, so a caller can tell "this
        // isn't for you to write" from "you mistyped this."
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

    /// A top-level alias captures the *userdata reference*, not a snapshot -- a later `set`/
    /// `set_component` must still be visible through it, exactly the property `crate::trace`'s
    /// own module doc names as the reason installation must happen before `.exec()`.
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
