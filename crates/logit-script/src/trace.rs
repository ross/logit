//! Exposes the incoming batch's trace context to Lua as a plain global `trace` table. See
//! `docs/design/lua-api.md`'s "Reading trace context" section and
//! `docs/adr/trace-context-propagation-on-delivered.md`.
//!
//! Installed unconditionally in [`crate::ScriptWorker::new`]: propagation is a property of every
//! pipeline, not something a config turns on. Two byte arrays cross this boundary rather than
//! `TraceContext`, which lives in `logit-pipeline`, a crate `logit-script` doesn't depend on.
//!
//! A plain table is writable: a script's write to `trace.trace_id` is accepted and only
//! overwritten by the next batch's [`set_context`]. `crate::provenance` is userdata for that
//! reason.
//!
//! **Installed before the script's source runs.** A global lookup inside a function body
//! resolves at call time, which is why `telemetry` can install after loading. Top-level code runs
//! once, during `Lua::load(source).exec()`, so an alias like `local ctx = trace` captures whatever
//! `trace` is at that instant; installing after `.exec()` would leave every such alias `nil`.
//! `crate::resource`, `crate::scope`, and `crate::provenance` install early for the same reason.

use mlua::{Lua, RegistryKey, Table};
use std::fmt::Write;

/// Creates the `trace` global, both fields all-zero hex until the first batch, and returns the key
/// [`set_context`] mutates it through.
pub fn install(lua: &Lua) -> mlua::Result<RegistryKey> {
    let table = lua.create_table()?;
    table.set("trace_id", "0".repeat(32))?;
    table.set("span_id", "0".repeat(16))?;
    lua.globals().set("trace", table.clone())?;
    lua.create_registry_value(table)
}

/// Overwrites the `trace` table's fields in place with lowercase hex, matching `stdio_out`'s
/// rendering of the same ids.
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
