//! Replaces Lua's `print` with one that writes a self-log line
//! (`docs/adr/tracing-for-self-logging.md`) instead of process stdout, where it would corrupt a
//! `stdio_out` writing there, fatally under `format: native`
//! (`docs/adr/lua-runaway-script-bounds.md`).
//!
//! Each argument is rendered through the script-visible global `tostring`, looked up per call, so
//! a `__tostring` metamethod or a script's own `tostring` renders as under Lua's `luaB_print`.
//! The line is emitted at `info` under target `logit`, tagged with the component id
//! [`crate::ScriptWorker::with_component`] set, or [`UNSET_COMPONENT`] for top-level code that
//! runs during [`crate::ScriptWorker::new`], before any id is known.

use crate::provenance::{self, ProvenanceState};
use mlua::{Lua, MultiValue};
use std::cell::RefCell;
use std::rc::Rc;

/// The `component` field of a line printed before `with_component` runs.
pub(crate) const UNSET_COMPONENT: &str = "<unset>";

/// Installs the `print` global. `provenance` is the worker's shared state, the one place its
/// component id lives.
pub(crate) fn install(lua: &Lua, provenance: Rc<RefCell<ProvenanceState>>) -> mlua::Result<()> {
    let print = lua.create_function(move |lua, args: MultiValue| {
        let tostring: mlua::Function = lua.globals().get("tostring")?;
        let mut line = String::new();
        for (i, arg) in args.into_iter().enumerate() {
            if i > 0 {
                line.push('\t');
            }
            let rendered = tostring.call(arg)?;
            // `luaB_print`'s own check and wording: a number is coerced, anything else is an error.
            let Some(rendered) = lua.coerce_string(rendered)? else {
                return Err(mlua::Error::RuntimeError(
                    "'tostring' must return a string to 'print'".to_string(),
                ));
            };
            line.push_str(&rendered.to_string_lossy());
        }
        provenance::with_component(&provenance, |id| {
            let id = id.unwrap_or(UNSET_COMPONENT);
            tracing::info!(target: "logit", component = %id, "print: {line}");
        });
        Ok(())
    })?;
    lua.globals().set("print", print)
}
