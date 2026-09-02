//! Exposes the incoming batch's trace context to Lua as a plain global table, mirroring
//! `crate::telemetry`'s install-once-mutate-later shape. See `docs/design/lua-api.md`'s "Reading
//! trace context" section and `docs/adr/0020-trace-context-propagation-on-delivered.md`.
//!
//! Unlike `telemetry`, this is installed unconditionally in [`crate::ScriptWorker::new`], not as
//! an opt-in builder: propagation is a property of the pipeline every Lua node runs in, not
//! something a config turns on, so every script gets `trace.trace_id`/`trace.span_id` whether it
//! reads them or not -- the same "always present, cheap either way" shape `EventProxy`'s
//! `has_span` already has. Two primitive byte arrays cross this boundary, not `TraceContext`
//! itself: `logit-script` doesn't depend on `logit-pipeline` (where that type lives), and there's
//! nothing this module needs from it beyond the two arrays.
//!
//! **Installed *before* the script's source runs, not after like `telemetry`.** `telemetry`
//! getting away with installing late relies on a real but narrow property: a *function body's*
//! global lookup resolves at call time, so `telemetry.count(...)` inside `process()` sees it
//! correctly however late `install` ran, right up until the first call. A script's top-level code
//! (which runs once, during `Lua::load(source).exec()`) doesn't get that -- an ordinary top-level
//! alias like `local ctx = trace` captures whatever `trace` *is at that instant*, once, forever.
//! `ScriptWorker::new` installs this before `.exec()` specifically because of that: installing
//! after would make every such alias permanently `nil`, caught in review by exactly that script
//! shape failing on every event.

use mlua::{Lua, RegistryKey, Table};
use std::fmt::Write;

/// Creates the `trace` global (both fields initialized to the all-zero hex `TraceContext::default()`
/// would render, since no batch has been seen yet) and returns the `RegistryKey` [`set_context`]
/// later mutates in place.
pub fn install(lua: &Lua) -> mlua::Result<RegistryKey> {
    let table = lua.create_table()?;
    table.set("trace_id", "0".repeat(32))?;
    table.set("span_id", "0".repeat(16))?;
    lua.globals().set("trace", table.clone())?;
    lua.create_registry_value(table)
}

/// Overwrites the installed `trace` table's fields in place with `trace_id`/`span_id`, hex-encoded
/// (matching `crates/logit-outputs/src/stdio.rs`'s existing rendering of the same shape of id).
/// Lua resolves a global lookup inside a function body at call time, not at the point the function
/// was defined (the same property `telemetry::install`'s doc comment relies on), so a script's
/// `process()` sees whatever was last set here regardless of exactly when between two calls this
/// runs.
pub fn set_context(
    lua: &Lua,
    table: &RegistryKey,
    trace_id: [u8; 16],
    span_id: [u8; 8],
) -> mlua::Result<()> {
    let table: Table = lua.registry_value(table)?;
    table.set("trace_id", push_hex(&trace_id))?;
    table.set("span_id", push_hex(&span_id))
}

fn push_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn install_sets_the_all_zero_placeholder_before_any_context_is_set() {
        let lua = Lua::new();
        install(&lua).unwrap();
        let table: Table = lua.globals().get("trace").unwrap();
        assert_eq!(table.get::<_, String>("trace_id").unwrap(), "0".repeat(32));
        assert_eq!(table.get::<_, String>("span_id").unwrap(), "0".repeat(16));
    }

    #[test]
    fn set_context_overwrites_both_fields_as_lowercase_hex() {
        let lua = Lua::new();
        let key = install(&lua).unwrap();
        set_context(&lua, &key, [0xab; 16], [0xcd; 8]).unwrap();

        let table: Table = lua.globals().get("trace").unwrap();
        assert_eq!(table.get::<_, String>("trace_id").unwrap(), "ab".repeat(16));
        assert_eq!(table.get::<_, String>("span_id").unwrap(), "cd".repeat(8));
    }
}
