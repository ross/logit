//! Exposes `logit_core::telemetry::Telemetry` to Lua scripts, so `process()`/`flush()` can emit
//! their own metrics -- domain-specific facts (an order value, a custom business counter) that no
//! amount of Rust-side instrumentation could infer. See `docs/design/lua-api.md`'s "Emitting
//! telemetry from a script" and `docs/adr/0019-lua-authored-telemetry-cardinality.md`.
//!
//! **Cardinality is convention-enforced here, not type-system-enforced.** Every Rust-side
//! `Telemetry` call takes `&'static str` names/tags specifically so cardinality is bounded by
//! *code*, not traffic -- a guarantee the type system enforces. A Lua-provided `String` can't
//! satisfy that directly, so [`static_str`] round-trips it through the process's own attribute
//! interner (`logit_core::interner`): `resolve(intern(s))` genuinely returns a `&'static str` (the
//! interner's own permanent storage), and re-interning a string it already holds allocates nothing
//! (`docs/known-gaps.md`'s interner section). This reuses existing, already-accepted
//! infrastructure rather than inventing a new leak mechanism -- but it does mean a script that
//! constructs a metric name or tag value from per-event data (rather than a fixed literal in its
//! own source) can leak the interner exactly the way a hand-rolled `kv_metrics` misuse already
//! could. Author responsibility, not a type-system guarantee -- see the ADR for the full tradeoff.
//!
//! Two more boundaries this module holds, both because a script's input is less constrained than
//! a Rust call site's: [`install`] checks `Telemetry::is_enabled` *before* touching the interner
//! at all, so a disabled handle costs nothing regardless of what a script passes it (not just once
//! it reaches `Telemetry::count`/`.gauge`); and [`static_metric_name`] rejects the `logit.` prefix,
//! reserved for the runtime's own metrics -- without it, a script could coalesce into (and
//! corrupt) a runtime counter or gauge sharing its exact name.

use logit_core::interner::{intern, resolve};
use logit_core::Telemetry;
use mlua::{Lua, Table, Value as LuaValue};

/// Converts a Lua-provided string into a genuine `&'static str` via intern-then-resolve. See this
/// module's doc comment for what that does and doesn't guarantee.
fn static_str(s: &str) -> &'static str {
    resolve(intern(s))
}

/// Converts and validates a Lua-provided metric name: rejects the `logit.` prefix, reserved for
/// the runtime's own metrics (`docs/design/internal-telemetry.md`'s naming scheme). Without this,
/// a script calling e.g. `telemetry.count("logit.component.events.received", 1)` would coalesce
/// into -- and corrupt -- the exact buffer key the runtime itself writes to: a `(name, tags)` key
/// carries no notion of which caller wrote to it, so `count` under a runtime gauge's name would
/// silently convert it to a counter, and vice versa (`ComponentBuffer::upsert`'s kind-mismatch
/// fallback).
fn static_metric_name(name: &str) -> mlua::Result<&'static str> {
    if name.starts_with("logit.") {
        return Err(mlua::Error::RuntimeError(format!(
            "metric name '{name}' is reserved -- the 'logit.' prefix is used by logit's own \
             internal metrics; pick a name outside that namespace"
        )));
    }
    Ok(static_str(name))
}

/// Reads an optional Lua table of `{tag = "value", ...}` pairs into owned `Tag`s. A non-string
/// value is a clear Lua error (`"tag '<key>' must be a string, got <type>"`), not a silent skip --
/// matching this crate's existing stance that a script's mistake should fail loudly
/// (`ScriptError`'s doc comments) rather than quietly produce a different result than intended.
fn read_tags(table: Option<Table>) -> mlua::Result<Vec<(&'static str, &'static str)>> {
    let Some(table) = table else { return Ok(Vec::new()) };
    let mut tags = Vec::new();
    for pair in table.pairs::<String, LuaValue>() {
        let (key, value) = pair?;
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

/// Installs the `telemetry` global table into `lua`, with `count(name, n, tags?)` and
/// `gauge(name, v, tags?)` -- see this module's doc comment. No `timing()`: scripts have no clock
/// exposed in the sandboxed stdlib (`sandbox_libs`, `TABLE | STRING | MATH` only), so there's no
/// sensible way for a script to produce a duration; exposing one is a separate, bigger scope
/// decision this doesn't make.
///
/// Safe to call at any point after `Lua::new_with` -- Lua resolves a global lookup inside a
/// function body at call time, not at the point the function was defined, so installing this
/// after a script has already loaded (as [`crate::ScriptWorker::with_telemetry`] does, to avoid
/// touching this crate's constructor) works identically to installing it before.
pub fn install(lua: &Lua, telemetry: Telemetry) -> mlua::Result<()> {
    let table = lua.create_table()?;

    let count_telemetry = telemetry.clone();
    table.set(
        "count",
        lua.create_function(move |_, (name, n, tags): (String, f64, Option<Table>)| {
            // Checked before anything else touches the interner or allocates -- a disabled
            // handle (no `internal` component configured) must cost nothing, the same guarantee
            // every other `Telemetry` call site gives (`docs/design/internal-telemetry.md`).
            // Interning/validating first, as the original version of this did, would mean a
            // pipeline with telemetry entirely turned off still permanently grows the process
            // interner for every distinct Lua-provided string it happens to see.
            if !count_telemetry.is_enabled() {
                return Ok(());
            }
            let name = static_metric_name(&name)?;
            let tags = read_tags(tags)?;
            count_telemetry.count(name, n, &tags);
            Ok(())
        })?,
    )?;

    table.set(
        "gauge",
        lua.create_function(move |_, (name, v, tags): (String, f64, Option<Table>)| {
            if !telemetry.is_enabled() {
                return Ok(());
            }
            let name = static_metric_name(&name)?;
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
            MetricKind::Counter(v) => assert_eq!(*v, 3.0),
            other => panic!("expected Counter, got {other:?}"),
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
        // Mirrors `crates/logit-core/src/attrs.rs`'s
        // `getting_an_absent_key_does_not_grow_the_interner` pattern: re-interning a string the
        // table already holds allocates nothing, so a script calling with the same literal name
        // repeatedly (the intended, common case) reaches a steady state rather than leaking.
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
    fn a_disabled_telemetry_handle_records_nothing_and_does_not_error() {
        let lua = sandboxed_lua();
        install(&lua, Telemetry::default()).unwrap();
        lua.load(r#"telemetry.count("m", 1); telemetry.gauge("g", 1)"#).exec().unwrap();
        // Nothing to assert beyond "doesn't panic and doesn't error" -- there is no `Registry` to
        // drain, matching `Telemetry::default()`'s disabled, no-op contract elsewhere.
    }

    /// The bug a `Telemetry::is_enabled()` check has to prevent: without it, a disabled handle
    /// (no `internal` component configured) would still intern every distinct name/tag a script
    /// passes -- a real, permanent leak the moment a script builds one out of per-event data,
    /// exactly contradicting "no config with an `internal` component pays nothing"
    /// (`docs/design/internal-telemetry.md`). Uses genuinely distinct, never-interned-elsewhere
    /// strings (not a fixed literal, which re-interning wouldn't grow the table for anyway) so a
    /// regression back to interning-before-checking would actually be caught here.
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

        // Confirms this isn't just an error message -- the runtime's own point is genuinely
        // untouched, not merged with a rejected script value.
        assert_eq!(registry.drain(0).len(), 0);
    }

    #[test]
    fn a_reserved_name_rejected_while_disabled_still_costs_nothing() {
        // A disabled handle short-circuits before the reserved-name check even runs (the whole
        // point of checking `is_enabled()` first) -- so this must NOT error, unlike the enabled
        // case above, and must not touch the interner either.
        let lua = sandboxed_lua();
        install(&lua, Telemetry::default()).unwrap();

        let before = interner::len();
        lua.load(r#"telemetry.count("logit.component.events.received", 1)"#).exec().unwrap();
        assert_eq!(interner::len(), before);
    }
}
