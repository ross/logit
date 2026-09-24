//! Exposes `logit_core::telemetry::Telemetry` to Lua, so `process()`/`flush()` can emit metrics
//! only the script knows (an order value, a business counter). See `docs/design/lua-api.md`'s
//! "Emitting telemetry from a script" and `docs/adr/lua-authored-telemetry-cardinality.md`.
//!
//! **Cardinality is convention-enforced here, not type-system-enforced.** Rust-side `Telemetry`
//! calls take `&'static str` names and tags so cardinality is bounded by code, not traffic. A
//! Lua string can't satisfy that, so [`static_str`] round-trips it through the process interner
//! (`logit_core::interner`): `resolve(intern(s))` returns the interner's permanent storage, and
//! re-interning a held string allocates nothing (`docs/known-gaps.md`'s interner section). A
//! script that builds a metric name or tag value from per-event data, rather than a literal in its
//! source, leaks the interner one entry at a time, as a misused `kv_metrics` can. That's the
//! author's responsibility; the ADR has the tradeoff.
//!
//! Three more boundaries, because a script's input is less constrained than a Rust call site's:
//! - [`install`]'s closures take `mlua::String`, not `String`, and check `Telemetry::is_enabled`
//!   before reading it. A disabled handle must cost nothing whatever a script passes, and a
//!   `String` parameter would make `mlua` allocate, copy, and UTF-8-check it during argument
//!   extraction, before the closure body runs.
//! - [`static_metric_name`] rejects the `logit.` prefix, reserved for the runtime's own metrics;
//!   otherwise a script could coalesce into, and corrupt, a runtime point with the same name.
//! - [`read_tags`] rejects a `component`/`kind`/`role` tag key, reserved for a point's identity
//!   (`logit_core::telemetry::is_reserved_tag_key`). The buffer filters these anyway, but a script
//!   that set one meant something by it, so it gets an error instead of a silent no-op.

use logit_core::interner::{intern, resolve};
use logit_core::telemetry::is_reserved_tag_key;
use logit_core::Telemetry;
use mlua::{Lua, String as LuaString, Table, Value as LuaValue};

/// Converts a Lua string to a `&'static str` via intern-then-resolve; see the module doc for the
/// cardinality cost.
fn static_str(s: &str) -> &'static str {
    resolve(intern(s))
}

/// Converts a Lua metric name, rejecting the `logit.` prefix reserved for the runtime's own
/// metrics (`docs/design/internal-telemetry.md`'s naming scheme).
///
/// A `(name, tags)` buffer key doesn't record its writer, so `count` under a runtime gauge's name
/// would silently turn it into a counter, and vice versa (`ComponentBuffer::upsert`'s
/// kind-mismatch fallback).
fn static_metric_name(name: &str) -> mlua::Result<&'static str> {
    if name.starts_with("logit.") {
        return Err(mlua::Error::RuntimeError(format!(
            "metric name '{name}' is reserved -- the 'logit.' prefix is used by logit's own \
             internal metrics; pick a name outside that namespace"
        )));
    }
    Ok(static_str(name))
}

/// Reads an optional `{tag = "value", ...}` table into interned tag pairs.
///
/// Two mistakes are Lua errors rather than a silent skip, as elsewhere in this crate: a
/// non-string value, and a key reserved for a point's identity (`is_reserved_tag_key`), which
/// `logit_core::telemetry`'s `PointKey::new` would otherwise drop.
fn read_tags(table: Option<Table>) -> mlua::Result<Vec<(&'static str, &'static str)>> {
    let Some(table) = table else { return Ok(Vec::new()) };
    let mut tags = Vec::new();
    for pair in table.pairs::<String, LuaValue>() {
        let (key, value) = pair?;
        if is_reserved_tag_key(&key) {
            return Err(mlua::Error::RuntimeError(format!(
                "tag '{key}' is reserved -- 'component'/'kind'/'role' identify which component \
                 emitted a point and cannot be set as a tag"
            )));
        }
        let LuaValue::String(value) = value else {
            return Err(mlua::Error::RuntimeError(format!(
                "tag '{key}' must be a string, got {}",
                value.type_name()
            )));
        };
        let value = value.to_str()?;
        tags.push((static_str(&key), static_str(value)));
    }
    Ok(tags)
}

/// Installs the `telemetry` global with `count(name, n, tags?)` and `gauge(name, v, tags?)`.
///
/// No `timing()`: the sandboxed stdlib (`sandbox_libs`: `TABLE | STRING | MATH`) exposes no clock,
/// so a script has no way to produce a duration.
///
/// Safe to call after the script has loaded, as [`crate::ScriptWorker::with_telemetry`] does,
/// because a global lookup inside a function body resolves at call time. A top-level alias
/// (`local t = telemetry`) captures `nil` instead; `crate::trace`'s module doc has why `trace` is
/// installed first.
pub fn install(lua: &Lua, telemetry: Telemetry) -> mlua::Result<()> {
    let table = lua.create_table()?;

    let count_telemetry = telemetry.clone();
    table.set(
        "count",
        lua.create_function(move |_, (name, n, tags): (LuaString, f64, Option<Table>)| {
            // First, before anything interns or allocates: a disabled handle (no `internal`
            // component) must cost nothing (`docs/design/internal-telemetry.md`), and interning
            // first would grow the process interner permanently with telemetry off. `name` is a
            // `LuaString` so it stays in the VM's buffer until `.to_str()`, only once enabled.
            if !count_telemetry.is_enabled() {
                return Ok(());
            }
            let name = static_metric_name(name.to_str()?)?;
            let tags = read_tags(tags)?;
            count_telemetry.count(name, n, &tags);
            Ok(())
        })?,
    )?;

    table.set(
        "gauge",
        lua.create_function(move |_, (name, v, tags): (LuaString, f64, Option<Table>)| {
            if !telemetry.is_enabled() {
                return Ok(());
            }
            let name = static_metric_name(name.to_str()?)?;
            let tags = read_tags(tags)?;
            telemetry.gauge(name, v, &tags);
            Ok(())
        })?,
    )?;

    lua.globals().set("telemetry", table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner;
    use logit_core::{MetricKind, Registry};
    use mlua::StdLib;

    fn sandboxed_lua() -> Lua {
        Lua::new_with(StdLib::TABLE | StdLib::STRING | StdLib::MATH, mlua::LuaOptions::new())
            .expect("sandboxed Lua should build")
    }

    #[test]
    fn a_lua_count_call_reaches_the_registry() {
        let lua = sandboxed_lua();
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("script", "lua", "transform");
        install(&lua, telemetry).unwrap();

        lua.load(r#"telemetry.count("orders.total", 3)"#).exec().unwrap();

        let events = registry.drain(0);
        assert_eq!(events.len(), 1);
        match &events[0].metrics[0].kind {
            MetricKind::Sum(sum) => assert_eq!(sum.value, 3.0),
            other => panic!("expected Sum, got {other:?}"),
        }
        assert_eq!(interner::resolve(events[0].metrics[0].name), "orders.total");
    }

    #[test]
    fn a_lua_gauge_call_with_tags_reaches_the_registry() {
        let lua = sandboxed_lua();
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("script", "lua", "transform");
        install(&lua, telemetry).unwrap();

        lua.load(r#"telemetry.gauge("queue.depth", 42, {status = "completed"})"#).exec().unwrap();

        let events = registry.drain(0);
        assert_eq!(events.len(), 1);
        match &events[0].metrics[0].kind {
            MetricKind::Gauge(v) => assert_eq!(*v, 42.0),
            other => panic!("expected Gauge, got {other:?}"),
        }
        assert_eq!(events[0].attributes.get("status").and_then(|v| v.as_str()), Some("completed"));
    }

    #[test]
    fn repeated_calls_with_the_same_literal_name_do_not_grow_the_interner() {
        let lua = sandboxed_lua();
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("script", "lua", "transform");
        install(&lua, telemetry).unwrap();

        lua.load(r#"telemetry.count("orders.total", 1)"#).exec().unwrap();
        let before = interner::len();
        for _ in 0..10 {
            lua.load(r#"telemetry.count("orders.total", 1)"#).exec().unwrap();
        }
        assert_eq!(interner::len(), before, "re-interning the same name should not grow the table");
    }

    #[test]
    fn a_non_string_tag_value_is_a_clear_lua_error_not_a_silent_skip() {
        let lua = sandboxed_lua();
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("script", "lua", "transform");
        install(&lua, telemetry).unwrap();

        let err = lua
            .load(r#"telemetry.count("m", 1, {bad = 42})"#)
            .exec()
            .expect_err("a non-string tag value should error");
        assert!(format!("{err}").contains("must be a string"), "got: {err}");
    }

    #[test]
    fn every_reserved_identity_tag_key_is_a_clear_lua_error_not_a_silent_no_op() {
        let lua = sandboxed_lua();
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("script", "lua", "transform");
        install(&lua, telemetry).unwrap();

        for key in ["component", "kind", "role"] {
            let err = lua
                .load(format!(r#"telemetry.count("m", 1, {{{key} = "spoofed"}})"#))
                .exec()
                .expect_err(&format!("'{key}' should be rejected as a tag key, not accepted"));
            assert!(format!("{err}").contains("reserved"), "got: {err}");
        }

        // No point was recorded, not only an error returned.
        assert_eq!(registry.drain(0).len(), 0);
    }

    #[test]
    fn a_disabled_telemetry_handle_records_nothing_and_does_not_error() {
        let lua = sandboxed_lua();
        install(&lua, Telemetry::default()).unwrap();
        lua.load(r#"telemetry.count("m", 1); telemetry.gauge("g", 1)"#).exec().unwrap();
    }

    /// A disabled handle interns nothing; the names are distinct so interning would grow the table.
    #[test]
    fn a_disabled_telemetry_handle_never_touches_the_interner_even_with_dynamic_looking_input() {
        let lua = sandboxed_lua();
        install(&lua, Telemetry::default()).unwrap();

        let before = interner::len();
        for i in 0..10 {
            lua.load(format!(
                r#"telemetry.count("disabled_probe_xyzzy_{i}", 1, {{tag_{i} = "v_{i}"}})"#
            ))
            .exec()
            .unwrap();
        }
        assert_eq!(
            interner::len(),
            before,
            "a disabled handle must never intern a script's input, dynamic or not"
        );
    }

    /// A disabled handle never UTF-8-checks `name`, so a non-UTF-8 string doesn't error.
    #[test]
    fn a_disabled_handle_never_reads_the_lua_argument_as_a_str_either() {
        let lua = sandboxed_lua();
        install(&lua, Telemetry::default()).unwrap();
        // `\255` is one non-UTF-8 byte; `LuaString::to_str()` would reject it.
        lua.load(r#"telemetry.count("\255", 1)"#).exec().unwrap();
    }

    #[test]
    fn the_logit_dot_prefix_is_reserved_and_rejected_with_a_clear_error() {
        let lua = sandboxed_lua();
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("script", "lua", "transform");
        install(&lua, telemetry).unwrap();

        let err = lua
            .load(r#"telemetry.count("logit.component.events.received", 1)"#)
            .exec()
            .expect_err("a script writing into the runtime's own namespace should error");
        assert!(format!("{err}").contains("reserved"), "got: {err}");

        // No point was recorded, not only an error returned.
        assert_eq!(registry.drain(0).len(), 0);
    }

    #[test]
    fn a_reserved_name_rejected_while_disabled_still_costs_nothing() {
        // `is_enabled()` short-circuits before the reserved-name check, so no error here.
        let lua = sandboxed_lua();
        install(&lua, Telemetry::default()).unwrap();

        let before = interner::len();
        lua.load(r#"telemetry.count("logit.component.events.received", 1)"#).exec().unwrap();
        assert_eq!(interner::len(), before);
    }
}
