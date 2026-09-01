//! Embeds LuaJIT (via `mlua`, vendored) and runs user `process`/`flush` scripts against
//! [`logit_core::Event`]. See `docs/design/lua-api.md` for the full design and, in particular,
//! the concurrency rules below.
//!
//! `mlua::Lua` is neither `Send` nor `Sync` -- that is a hard constraint from the embedded VM, not
//! a preference. [`ScriptWorker`] is the enforcement point: it owns one `Lua` instance and is
//! itself `!Send`, so the type system stops a `Lua` from being shared across pipeline workers
//! rather than relying on a convention nobody checks. The pipeline runs one [`ScriptWorker`] per
//! worker thread.

use logit_core::{Event, Telemetry};
use mlua::{Lua, LuaOptions, RegistryKey, StdLib, Value as LuaValue};
use std::marker::PhantomData;

mod proxy;
mod telemetry;
mod trace;
mod value;

pub use proxy::EventProxy;

#[derive(Debug, thiserror::Error)]
pub enum ScriptError {
    #[error("lua error: {0}")]
    Lua(#[from] mlua::Error),
    #[error("script has no `process` function")]
    MissingProcess,
}

/// The standard library surface scripts get: enough to write real transform logic (`table`,
/// `string`, `math` -- core language functions like `pairs`/`type`/`tostring` are always
/// available and aren't gated behind a `StdLib` flag at all), nothing that reaches the host.
/// Deliberately explicit rather than trusting `Lua::new()`'s "safe" default's exact composition --
/// LuaJIT's `ffi` library in particular is a genuine sandbox escape (raw memory access, arbitrary
/// C calls) if left enabled, and mlua's docs don't commit to `Lua::new()` excluding it. No
/// `PACKAGE`, so no `require`, either.
fn sandbox_libs() -> StdLib {
    StdLib::TABLE | StdLib::STRING | StdLib::MATH
}

/// Lua 5.1's base library isn't gated by any `StdLib` flag at all -- the same non-gating that
/// keeps `pairs`/`type`/`tostring` always available also keeps a handful of genuinely dangerous
/// functions available regardless of `sandbox_libs()`. Confirmed the hard way: a review reproduced
/// both `loadfile ~= nil` and `dofile ~= nil` in a worker built with only `TABLE | STRING | MATH`
/// selected, meaning a script could read and execute arbitrary files readable by this process
/// despite the documented "nothing that reaches the host" sandbox.
///
/// Removed here, each for its own reason:
/// - `loadfile`, `dofile` -- read and execute a file from the process's filesystem. The
///   concretely reproduced issue.
/// - `load`, `loadstring` -- compile and execute an arbitrary *constructed* string as Lua code.
///   Not filesystem access, but it undermines the property that only the one script source this
///   worker was built from ever runs.
/// - `getfenv`, `setfenv` -- Lua 5.1-specific, well documented in the wider Lua community as
///   sandbox-escape-adjacent: they let code inspect/replace a function's environment table, which
///   is exactly the kind of tampering a "restricted stdlib" sandbox is meant to prevent.
///
/// `lua.globals()` *is* `_G` (not a copy), so removing a key here removes it from `_G` too.
fn remove_unsandboxed_base_globals(lua: &Lua) -> mlua::Result<()> {
    let globals = lua.globals();
    for name in ["loadfile", "dofile", "load", "loadstring", "getfenv", "setfenv"] {
        globals.set(name, LuaValue::Nil)?;
    }
    Ok(())
}

/// Owns one Lua VM and runs the `process`/`flush` globals it defines, for one pipeline stage, on
/// one worker. Not `Send`/`Sync` (via the `PhantomData<*const ()>` marker) -- see the module docs.
pub struct ScriptWorker {
    lua: Lua,
    /// `RegistryKey`s for this script's `process`/`flush` functions, resolved once here rather
    /// than looked up from `_G` on every `process`/`flush` call (`docs/design/memory.md` §8).
    /// `mlua::Function<'lua>` is tied to a borrow of `self.lua`, so it can't be stored directly
    /// as a field of a struct that outlives any one call -- a `RegistryKey` is `mlua`'s `'static`
    /// handle for exactly this "hold a Lua value across calls" case, redeemed via
    /// `Lua::registry_value` whenever a call needs the real `Function` back. `flush` is
    /// `Option`al because a script may not define one at all (the stateless-processor case).
    process: RegistryKey,
    flush: Option<RegistryKey>,
    /// The installed `trace` global's table, held so [`ScriptWorker::set_trace_context`] can
    /// mutate it in place -- see `crate::trace`'s module doc for why this is unconditional,
    /// unlike `telemetry`'s opt-in builder.
    trace_table: RegistryKey,
    _not_send_sync: PhantomData<*const ()>,
}

/// What running a script's `process` returned, per the contract in `docs/design/lua-api.md`.
///
/// `Emit` is boxed: `Event`'s inline attribute storage (`docs/design/data-model.md`'s small-map
/// layout) makes it large enough that clippy flags the size gap against `Drop` otherwise.
pub enum ProcessOutcome {
    /// Pass the (possibly mutated) event through.
    Emit(Box<Event>),
    /// The script returned multiple events (fan-out).
    EmitMany(Vec<Event>),
    /// The script returned `nil`: drop the event.
    Drop,
}

impl ScriptWorker {
    /// Loads a script's source and sandboxes it (see [`sandbox_libs`]). Fails at load time if the
    /// source doesn't parse/execute, doesn't define a `process` function, or defines a `flush`
    /// global that isn't a function (or `nil`) -- a config error should surface immediately, not
    /// at the first event, or first flush tick, that happens to flow through.
    ///
    /// `process`/`flush` are resolved to `RegistryKey`s exactly once, here, rather than looked up
    /// from `_G` on every call (see the field doc comments) -- one consequence worth being
    /// explicit about (and written down in `docs/design/lua-api.md`): a script that reassigns
    /// `_G.process`/`_G.flush` after this point has no effect on what actually runs. Reassigning
    /// either mid-run isn't a documented or tested pattern to begin with, so this is very unlikely
    /// to be a real behavior change for anything that exists, but it is a real narrowing of what
    /// this boundary guarantees.
    pub fn new(source: &str) -> Result<Self, ScriptError> {
        let lua = Lua::new_with(sandbox_libs(), LuaOptions::new())?;
        remove_unsandboxed_base_globals(&lua)?;
        lua.load(source).exec()?;
        let process_fn = match lua.globals().get::<_, LuaValue>("process")? {
            LuaValue::Function(f) => f,
            _ => return Err(ScriptError::MissingProcess),
        };
        let process = lua.create_registry_value(process_fn)?;
        // `Option<mlua::Function>::from_lua` maps a `nil` global to `None` (no `flush` at all --
        // the stateless-processor case) and, for anything else, delegates to
        // `Function::from_lua` -- which errors clearly (`error converting Lua <type> to
        // function`) rather than silently treating a mistyped global (e.g. `flush = 5`, plausible
        // from a copy-paste or a renamed variable) the same as "absent." That distinction matters
        // more here than it would for a plain lookup: a script that thinks it flushes but doesn't
        // would silently lose events at every flush tick with the eager-resolution approach below
        // instead of erroring at load time the way the equivalent mistake on `process` already
        // does via `MissingProcess`.
        let flush_fn: Option<mlua::Function> = lua.globals().get("flush")?;
        let flush = flush_fn.map(|f| lua.create_registry_value(f)).transpose()?;
        let trace_table = trace::install(&lua)?;
        Ok(Self { lua, process, flush, trace_table, _not_send_sync: PhantomData })
    }

    /// Overwrites the `trace` global's `trace_id`/`span_id` (hex-encoded) so this worker's next
    /// `process()` call reads the given batch's context. Called once per incoming batch, before
    /// its events reach `process` (`crates/logit-pipeline/src/runtime.rs`'s `run_lua`) -- not
    /// called at all around a `flush()` call, which keeps whatever was last set, exactly like
    /// `run_lua`'s own `last_resource` staleness (see `docs/known-gaps.md`'s entry for both).
    pub fn set_trace_context(
        &self,
        trace_id: [u8; 16],
        span_id: [u8; 8],
    ) -> Result<(), ScriptError> {
        trace::set_context(&self.lua, &self.trace_table, trace_id, span_id).map_err(Into::into)
    }

    /// Installs a `telemetry` global so `process()`/`flush()` can emit their own metrics -- a
    /// builder rather than a `new()` parameter, mirroring `with_diagnostics`/`with_timeout`/
    /// `with_retry` everywhere else in this framework, specifically so this doesn't touch any of
    /// this crate's existing `ScriptWorker::new(script)` call sites. Safe to call after `new`
    /// returns (rather than needing to happen before the script's own top-level code runs): Lua
    /// resolves a global lookup inside a function body at call time, not at the point the
    /// function was defined, so a script's `process`/`flush` sees `telemetry` correctly regardless
    /// of exactly when between `new` and the first call this was installed. See
    /// `crate::telemetry` and `docs/design/lua-api.md`.
    pub fn with_telemetry(self, telemetry: Telemetry) -> Result<Self, ScriptError> {
        telemetry::install(&self.lua, telemetry)?;
        Ok(self)
    }

    /// Bytes currently in use by this worker's Lua VM -- the strongest single signal a stateful
    /// script is leaking state (e.g. accumulating something across `flush()` calls) has, since
    /// nothing else in the process can see inside the VM. Wraps `mlua::Lua::used_memory`.
    pub fn used_memory(&self) -> usize {
        self.lua.used_memory()
    }

    /// Runs this worker's `process(event)` once. See `docs/design/lua-api.md` for the
    /// proxy-vs-table-conversion tradeoff [`EventProxy`] exists to avoid.
    pub fn process(&self, event: Event) -> Result<ProcessOutcome, ScriptError> {
        let process: mlua::Function = self.lua.registry_value(&self.process)?;
        let result: LuaValue =
            process.call(EventProxy::new(event)).map_err(proxy::clarify_destructed_handle_use)?;
        Ok(match result {
            LuaValue::Nil => ProcessOutcome::Drop,
            LuaValue::UserData(ud) => {
                ProcessOutcome::Emit(Box::new(proxy::take_event(&self.lua, ud)?))
            }
            LuaValue::Table(table) => {
                ProcessOutcome::EmitMany(events_from_table(&self.lua, table, "process")?)
            }
            other => {
                return Err(ScriptError::Lua(mlua::Error::RuntimeError(format!(
                    "process() must return nil, an event, or a table of events, got {}",
                    other.type_name()
                ))))
            }
        })
    }

    /// Runs this worker's `flush()`, if the script defines one (the stateful-processor contract,
    /// e.g. the built-in `aggregate` transform). Returns an empty `Vec` if the script has none.
    pub fn flush(&self) -> Result<Vec<Event>, ScriptError> {
        let Some(flush_key) = self.flush.as_ref() else {
            return Ok(Vec::new());
        };
        let flush: mlua::Function = self.lua.registry_value(flush_key)?;
        let result: LuaValue = flush.call(()).map_err(proxy::clarify_destructed_handle_use)?;
        Ok(match result {
            LuaValue::Nil => Vec::new(),
            LuaValue::Table(table) => events_from_table(&self.lua, table, "flush")?,
            other => {
                return Err(ScriptError::Lua(mlua::Error::RuntimeError(format!(
                    "flush() must return nil or a table of events, got {}",
                    other.type_name()
                ))))
            }
        })
    }
}

/// Extracts a `Vec<Event>` from a table a script returned from `process()` or `flush()`.
/// `caller` names which, for the error message.
///
/// Validates the table is a proper contiguous `1..=n` sequence first, rather than reaching
/// straight for `Table::sequence_values`, which stops at the first gap and never notices
/// non-sequence keys -- a review reproduced `return {[2] = event}` silently succeeding as
/// `EmitMany([])` (`sequence_values` finds index 1 missing and stops immediately), dropping the
/// event with no indication anything was wrong instead of reporting the malformed return value.
/// An empty table is a valid (empty) sequence -- equivalent to returning nothing.
fn events_from_table(
    lua: &Lua,
    table: mlua::Table,
    caller: &str,
) -> Result<Vec<Event>, ScriptError> {
    let Some(len) = value::validated_sequence_len(&table)? else {
        return Err(ScriptError::Lua(mlua::Error::RuntimeError(format!(
            "{caller}() must return a contiguous array-like table of events (found non-sequence keys)"
        ))));
    };
    let mut events = Vec::with_capacity(len);
    for i in 1..=len {
        events.push(proxy::take_event(lua, table.get(i)?)?);
    }
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{interner::intern, AttrMap, LogRecord, MetricKind, MetricRecord};

    fn counter_event(name: &str, value: f64) -> Event {
        Event::metric(
            1_700_000_000_000_000_000,
            AttrMap::new(),
            MetricRecord { name: intern(name), kind: MetricKind::Counter(value), unit: None },
        )
    }

    /// An event carrying both a log and a metric -- the shape `kv_metrics` (workstream E)
    /// produces, and the case the `has_*` accessors exist to make legible without a lossy
    /// summary label.
    fn log_and_counter_event(name: &str, value: f64) -> Event {
        let mut event = counter_event(name, value);
        event.log = Some(LogRecord {
            message: logit_core::Value::str("GET /"),
            severity: None,
            body_format: logit_core::BodyFormat::Raw,
        });
        event
    }

    fn worker(source: &str) -> ScriptWorker {
        ScriptWorker::new(source).expect("script should load")
    }

    fn emitted(outcome: ProcessOutcome) -> Event {
        match outcome {
            ProcessOutcome::Emit(e) => *e,
            _ => panic!("expected Emit"),
        }
    }

    #[test]
    fn process_can_read_and_write_attributes() {
        let w = worker(
            r#"
            function process(event)
                event.attributes.env = event.attributes.env or "unknown"
                return event
            end
            "#,
        );
        let out = emitted(w.process(counter_event("hits", 1.0)).unwrap());
        assert_eq!(out.attributes.get("env").and_then(|v| v.as_str()), Some("unknown"));
    }

    #[test]
    fn process_sees_attributes_already_set() {
        let w = worker(
            r#"
            function process(event)
                event.attributes.env = event.attributes.env or "unknown"
                return event
            end
            "#,
        );
        let mut event = counter_event("hits", 1.0);
        event.attributes.insert("env", "prod");
        let out = emitted(w.process(event).unwrap());
        assert_eq!(out.attributes.get("env").and_then(|v| v.as_str()), Some("prod"));
    }

    #[test]
    fn timestamp_round_trips_exactly_as_a_string() {
        // Lua's only numeric type is an IEEE-754 double (exact only to 2^53, ~9e15); a
        // unix-nanos timestamp is routinely ~1.7e18. `event.timestamp` is a string specifically
        // so this round-trips exactly instead of silently losing precision -- see the comment at
        // its definition in proxy.rs. This test would have caught the original, wrong,
        // Lua-number-based design (it read back as "1.7e+18" instead of the real value).
        let w = worker(
            r#"
            function process(event)
                local ts = event.timestamp
                event.attributes.captured = ts
                event.timestamp = ts
                return event
            end
            "#,
        );
        let event = counter_event("hits", 1.0);
        let original_ts = event.timestamp;
        let out = emitted(w.process(event).unwrap());
        assert_eq!(out.timestamp, original_ts);
        assert_eq!(
            out.attributes.get("captured").and_then(|v| v.as_str()),
            Some(original_ts.to_string().as_str())
        );
    }

    #[test]
    fn large_i64_attribute_round_trips_exactly() {
        // 9_007_199_254_740_993 is exactly 2^53 + 1 -- one past the largest integer an IEEE-754
        // double can represent exactly, and the review's own repro value: a prior version of
        // value_to_lua always used LuaValue::Integer for I64, and this became ..._992 after
        // nothing more than an identity assignment.
        let w = worker(
            r#"
            function process(event)
                event.attributes.x = event.attributes.x
                return event
            end
            "#,
        );
        let mut event = counter_event("hits", 1.0);
        event.attributes.insert("x", 9_007_199_254_740_993i64);
        let out = emitted(w.process(event).unwrap());
        // This value takes value_to_lua's string branch (it's outside the exact-integer range),
        // and the identity assignment above hands that exact same string straight back --
        // AttrsProxy::__newindex recognizes that as a no-op (lua_value_matches) and leaves the
        // stored Value untouched, so the variant stays I64, not (as an earlier version of this
        // test asserted, matching the then-real bug) Str. The precision fix itself -- no silent
        // truncation to ..._992 -- is unaffected either way.
        assert_eq!(out.attributes.get("x"), Some(&logit_core::Value::I64(9_007_199_254_740_993)));
    }

    #[test]
    fn small_i64_attribute_stays_a_real_lua_number() {
        // The fix for the above is conditional (a string only when a value doesn't survive an
        // exact f64 round-trip), not a blanket string like `timestamp` -- ordinary small integer
        // attributes should still arrive as genuine Lua numbers so scripts can do arithmetic on
        // them directly. This is a regression test for that ergonomic, not just the precision fix.
        let w = worker(
            r#"
            function process(event)
                event.attributes.doubled = event.attributes.x * 2
                return event
            end
            "#,
        );
        let mut event = counter_event("hits", 1.0);
        event.attributes.insert("x", 21i64);
        let out = emitted(w.process(event).unwrap());
        // LuaJIT's dual-number mode keeps small-integer arithmetic as an integer, not a float --
        // an even better outcome than originally assumed here (this test's first version expected
        // F64, which was itself a wrong assumption caught by actually running it).
        assert_eq!(out.attributes.get("doubled"), Some(&logit_core::Value::I64(42)));
    }

    #[test]
    fn u64_max_attribute_round_trips_exactly_instead_of_wrapping_negative() {
        let w = worker(
            r#"
            function process(event)
                event.attributes.x = event.attributes.x
                return event
            end
            "#,
        );
        let mut event = counter_event("hits", 1.0);
        event.attributes.insert("x", logit_core::Value::U64(u64::MAX));
        let out = emitted(w.process(event).unwrap());
        // u64::MAX takes value_to_lua's string branch, and the identity assignment hands that
        // exact string straight back -- recognized as a no-op (lua_value_matches), so the
        // variant stays U64 rather than (as an earlier version of this test asserted, matching
        // the then-real bug) collapsing to Str. The precision fix -- no wrapping negative -- is
        // unaffected either way.
        assert_eq!(out.attributes.get("x"), Some(&logit_core::Value::U64(u64::MAX)));
    }

    // The tests below cover `AttrsProxy::__newindex`'s no-op-assignment rule (value.rs's
    // `lua_value_matches`) -- the fix for the variant-collapse gap described in
    // `docs/design/lua-value-type-preservation.md` and PR #6 review discussion_r3887008990. Before
    // that fix, every one of these "stays X" assertions failed: an identity assignment
    // (`event.attributes.x = event.attributes.x`) always turned the attribute into `Value::Str`
    // (or, for the two number-branch repros the review added, `Value::I64`), regardless of what
    // it started as.

    #[test]
    fn bytes_attribute_with_valid_utf8_stays_bytes_through_identity_round_trip() {
        // The headline regression: Value::Bytes whose content happens to be valid UTF-8 takes
        // value_to_lua's plain-string branch (same as Value::Str) with nothing to mark which one
        // it was -- this is also the concrete case logit-outputs::influxdb's tag handling treats
        // differently (Str becomes a tag, Bytes doesn't).
        let w = worker(
            r#"
            function process(event)
                event.attributes.x = event.attributes.x
                return event
            end
            "#,
        );
        let mut event = counter_event("hits", 1.0);
        event
            .attributes
            .insert("x", logit_core::Value::Bytes(bytes::Bytes::from_static(b"web-01")));
        let out = emitted(w.process(event).unwrap());
        assert_eq!(
            out.attributes.get("x"),
            Some(&logit_core::Value::Bytes(bytes::Bytes::from_static(b"web-01")))
        );
    }

    #[test]
    fn bytes_attribute_with_invalid_utf8_still_round_trips_correctly() {
        // Unchanged behavior, guarded: invalid-UTF-8 Bytes already round-trips correctly without
        // this fix, because it fails lua_to_value's UTF-8 check on the way back regardless of the
        // no-op-assignment rule. Not a case this fix needed to touch, but worth pinning down.
        let w = worker(
            r#"
            function process(event)
                event.attributes.x = event.attributes.x
                return event
            end
            "#,
        );
        let mut event = counter_event("hits", 1.0);
        let invalid = bytes::Bytes::from_static(&[0xff, 0xfe, 0x00]);
        event.attributes.insert("x", logit_core::Value::Bytes(invalid.clone()));
        let out = emitted(w.process(event).unwrap());
        assert_eq!(out.attributes.get("x"), Some(&logit_core::Value::Bytes(invalid)));
    }

    #[test]
    fn large_timestamp_attribute_stays_timestamp_through_identity_round_trip() {
        // A Timestamp value large enough to take value_to_lua's string branch (unix-nanos
        // timestamps always are, in practice).
        let w = worker(
            r#"
            function process(event)
                event.attributes.x = event.attributes.x
                return event
            end
            "#,
        );
        let mut event = counter_event("hits", 1.0);
        event.attributes.insert("x", logit_core::Value::Timestamp(1_700_000_000_000_000_000));
        let out = emitted(w.process(event).unwrap());
        assert_eq!(
            out.attributes.get("x"),
            Some(&logit_core::Value::Timestamp(1_700_000_000_000_000_000))
        );
    }

    #[test]
    fn small_timestamp_attribute_stays_timestamp_through_identity_round_trip() {
        // A Timestamp small enough to fit the exact-integer range takes value_to_lua's
        // LuaValue::Integer branch instead (exact_i64_to_lua treats Timestamp exactly like I64) --
        // covered separately from the large case above since it exercises `lua_value_matches`'s
        // Integer arm, not its String arm.
        let w = worker(
            r#"
            function process(event)
                event.attributes.x = event.attributes.x
                return event
            end
            "#,
        );
        let mut event = counter_event("hits", 1.0);
        event.attributes.insert("x", logit_core::Value::Timestamp(42));
        let out = emitted(w.process(event).unwrap());
        assert_eq!(out.attributes.get("x"), Some(&logit_core::Value::Timestamp(42)));
    }

    #[test]
    fn small_u64_attribute_stays_u64_through_identity_round_trip() {
        let w = worker(
            r#"
            function process(event)
                event.attributes.x = event.attributes.x
                return event
            end
            "#,
        );
        let mut event = counter_event("hits", 1.0);
        event.attributes.insert("x", logit_core::Value::U64(42));
        let out = emitted(w.process(event).unwrap());
        assert_eq!(out.attributes.get("x"), Some(&logit_core::Value::U64(42)));
    }

    #[test]
    fn f64_attribute_stays_f64_through_identity_round_trip() {
        // The PR #6 review's other repro: LuaJIT's dual-number mode canonicalizes an integral
        // Number (42.0) as an Integer, so a naive `lua_to_value` sees LuaValue::Integer(42) and
        // silently produces Value::I64(42) instead of the original Value::F64(42.0).
        let w = worker(
            r#"
            function process(event)
                event.attributes.x = event.attributes.x
                return event
            end
            "#,
        );
        let mut event = counter_event("hits", 1.0);
        event.attributes.insert("x", logit_core::Value::F64(42.0));
        let out = emitted(w.process(event).unwrap());
        assert_eq!(out.attributes.get("x"), Some(&logit_core::Value::F64(42.0)));
    }

    #[test]
    fn fractional_f64_round_trips_correctly_the_contrast_case() {
        // Unlike a whole-number float, a fractional one has no integer representation in LuaJIT
        // to be canonicalized into, so it was never affected by the number-branch collapse the
        // test above covers -- confirms the loss (before this fix) and the fix itself are both
        // specific to integral floats, not floats in general.
        let w = worker(
            r#"
            function process(event)
                event.attributes.x = event.attributes.x
                return event
            end
            "#,
        );
        let mut event = counter_event("hits", 1.0);
        event.attributes.insert("x", logit_core::Value::F64(42.5));
        let out = emitted(w.process(event).unwrap());
        assert_eq!(out.attributes.get("x"), Some(&logit_core::Value::F64(42.5)));
    }

    #[test]
    fn modifying_a_bytes_attribute_still_converts_it_to_str() {
        // The no-op rule is content-gated, not blanket: a script that genuinely builds a new
        // string from an old value should still get Value::Str, same as before this fix.
        let w = worker(
            r#"
            function process(event)
                event.attributes.x = tostring(event.attributes.x) .. "-suffix"
                return event
            end
            "#,
        );
        let mut event = counter_event("hits", 1.0);
        event
            .attributes
            .insert("x", logit_core::Value::Bytes(bytes::Bytes::from_static(b"web-01")));
        let out = emitted(w.process(event).unwrap());
        assert_eq!(out.attributes.get("x").and_then(|v| v.as_str()), Some("web-01-suffix"));
    }

    #[test]
    fn assigning_a_brand_new_string_key_produces_str() {
        // A script constructing a genuinely new attribute must not be rejected or coerced to
        // some other variant just because the no-op rule exists elsewhere.
        let w = worker(
            r#"
            function process(event)
                event.attributes.greeting = "hello"
                return event
            end
            "#,
        );
        let out = emitted(w.process(counter_event("hits", 1.0)).unwrap());
        assert_eq!(out.attributes.get("greeting").and_then(|v| v.as_str()), Some("hello"));
    }

    #[test]
    fn assigning_different_content_over_a_bytes_attribute_produces_str() {
        // The rule compares content, not key: overwriting an existing key with genuinely
        // different content is exactly as much a real change as writing a new key, and should
        // convert the same way.
        let w = worker(
            r#"
            function process(event)
                event.attributes.x = "replaced"
                return event
            end
            "#,
        );
        let mut event = counter_event("hits", 1.0);
        event
            .attributes
            .insert("x", logit_core::Value::Bytes(bytes::Bytes::from_static(b"web-01")));
        let out = emitted(w.process(event).unwrap());
        assert_eq!(out.attributes.get("x").and_then(|v| v.as_str()), Some("replaced"));
    }

    #[test]
    fn generic_copy_all_attributes_script_preserves_every_variant() {
        // The motivating real-world scenario from docs/design/lua-value-type-preservation.md: a
        // script with no intention of touching a particular attribute -- here, one that just tags
        // every event with `env` and otherwise copies attributes through via to_table(), a very
        // ordinary pattern for a generic enrichment stage -- must not silently change that
        // attribute's variant.
        let w = worker(
            r#"
            function process(event)
                local attrs = event:to_table().attributes
                for k, v in pairs(attrs) do
                    event.attributes[k] = v
                end
                event.attributes.env = "prod"
                return event
            end
            "#,
        );
        let mut event = counter_event("hits", 1.0);
        event
            .attributes
            .insert("host", logit_core::Value::Bytes(bytes::Bytes::from_static(b"web-01")));
        event.attributes.insert("retries", logit_core::Value::U64(42));
        let out = emitted(w.process(event).unwrap());
        assert_eq!(
            out.attributes.get("host"),
            Some(&logit_core::Value::Bytes(bytes::Bytes::from_static(b"web-01")))
        );
        assert_eq!(out.attributes.get("retries"), Some(&logit_core::Value::U64(42)));
        assert_eq!(out.attributes.get("env").and_then(|v| v.as_str()), Some("prod"));
    }

    #[test]
    fn cross_key_copy_of_a_bytes_attribute_is_a_documented_residual_gap() {
        // The rule this fix implements is keyed on an assignment's Lua-side content matching an
        // *existing* attribute at the *same key* -- it can't (and isn't meant to) recognize that
        // a value copied to a different key came from somewhere that remembers its variant, since
        // by that point it's just a plain Lua string like any other. Asserted deliberately, as a
        // documented contract rather than an accident -- see
        // docs/adr/0007-lua-value-identity-preservation.md's Consequences section.
        let w = worker(
            r#"
            function process(event)
                event.attributes.y = event.attributes.x
                return event
            end
            "#,
        );
        let mut event = counter_event("hits", 1.0);
        event
            .attributes
            .insert("x", logit_core::Value::Bytes(bytes::Bytes::from_static(b"web-01")));
        let out = emitted(w.process(event).unwrap());
        assert_eq!(
            out.attributes.get("x"),
            Some(&logit_core::Value::Bytes(bytes::Bytes::from_static(b"web-01")))
        );
        assert_eq!(out.attributes.get("y").and_then(|v| v.as_str()), Some("web-01"));
    }

    #[test]
    fn nested_bytes_in_an_array_is_a_documented_residual_gap() {
        // lua_value_matches doesn't recurse into Table: an Array/Map already round-trips
        // correctly as a *shape* (a real Lua table, not a string), but the top-level identity
        // check has no way to tell that a Table assignment's *contents* are unchanged without
        // walking it -- and that walk can trigger a script-supplied __index and reenter this
        // proxy, so it isn't attempted. The whole array is reconverted via lua_to_value, which
        // has no memory of what the nested element used to be. Asserted deliberately, as a
        // documented contract rather than an accident -- see
        // docs/design/lua-value-type-preservation.md's "Known residual gaps".
        let w = worker(
            r#"
            function process(event)
                event.attributes.x = event.attributes.x
                return event
            end
            "#,
        );
        let mut event = counter_event("hits", 1.0);
        event.attributes.insert(
            "x",
            logit_core::Value::Array(vec![logit_core::Value::Bytes(bytes::Bytes::from_static(
                b"web-01",
            ))]),
        );
        let out = emitted(w.process(event).unwrap());
        assert_eq!(
            out.attributes.get("x"),
            Some(&logit_core::Value::Array(vec![logit_core::Value::Str(
                bytes::Bytes::from_static(b"web-01")
            )]))
        );
    }

    #[test]
    fn empty_table_decodes_as_map_not_array() {
        // An empty Lua table can't carry which of Value::Array(vec![])/Value::Map(AttrMap::new())
        // it came from -- there's nothing to inspect. An earlier version always decoded it as
        // Array (fixing Array losing its variant by breaking Map the other way); this documents
        // and tests the chosen default instead: an empty table becomes Map, since attributes
        // (map-shaped) are the primary thing scripts manipulate. See value.rs's
        // lua_table_to_value for the full reasoning.
        let w = worker(
            r#"
            function process(event)
                event.attributes.x = event.attributes.x
                return event
            end
            "#,
        );
        let mut event = counter_event("hits", 1.0);
        event.attributes.insert("x", logit_core::Value::Array(Vec::new()));
        let out = emitted(w.process(event).unwrap());
        assert_eq!(
            out.attributes.get("x"),
            Some(&logit_core::Value::Map(Box::new(AttrMap::new())))
        );
    }

    #[test]
    fn empty_map_stays_a_map() {
        // The regression this round's review caught in the previous round's empty-array fix:
        // Value::Map(AttrMap::new()) used to also become Array once the seq_len > 0 guard was
        // removed. Must stay Map now that the empty case is handled explicitly.
        let w = worker(
            r#"
            function process(event)
                event.attributes.x = event.attributes.x
                return event
            end
            "#,
        );
        let mut event = counter_event("hits", 1.0);
        event.attributes.insert("x", logit_core::Value::Map(Box::new(AttrMap::new())));
        let out = emitted(w.process(event).unwrap());
        assert_eq!(
            out.attributes.get("x"),
            Some(&logit_core::Value::Map(Box::new(AttrMap::new())))
        );
    }

    #[test]
    fn table_with_a_hole_and_an_extra_key_becomes_a_map_not_a_silently_truncated_array() {
        // raw_len() (Lua's `#` operator) is undefined for a table with holes: {[1]="a", [2]="b",
        // [4]="d", extra="c"} has 4 total pairs, and raw_len() happens to also return 4 here
        // (LuaJIT's choice of border), so a count-based check couldn't tell this apart from a
        // real 4-element sequence -- it used to silently decode as Array(["a","b",Null,"d"]),
        // dropping "extra" with no error. validated_sequence_len now checks every key's actual
        // identity, correctly recognizing "extra" makes this a non-sequence -- so it falls back
        // to the Map branch instead, preserving all 4 entries rather than silently dropping one.
        let w = worker(
            r#"
            function process(event)
                event.attributes.x = {[1] = "a", [2] = "b", [4] = "d", extra = "c"}
                return event
            end
            "#,
        );
        let out = emitted(w.process(counter_event("hits", 1.0)).unwrap());
        match out.attributes.get("x") {
            Some(logit_core::Value::Map(map)) => {
                assert_eq!(map.len(), 4, "expected all 4 entries preserved, got: {map:?}");
            }
            other => panic!("expected a Map preserving all entries, got: {other:?}"),
        }
    }

    #[test]
    fn has_accessors_read_true_and_false_per_payload() {
        let w = worker(
            r#"
            function process(event)
                event.attributes.has_metrics = event.has_metrics
                event.attributes.has_log = event.has_log
                event.attributes.has_span = event.has_span
                return event
            end
            "#,
        );
        let out = emitted(w.process(counter_event("hits", 1.0)).unwrap());
        assert!(matches!(out.attributes.get("has_metrics"), Some(logit_core::Value::Bool(true))));
        assert!(matches!(out.attributes.get("has_log"), Some(logit_core::Value::Bool(false))));
        assert!(matches!(out.attributes.get("has_span"), Some(logit_core::Value::Bool(false))));
    }

    /// The headline test for the multi-payload model (docs/adr/0012-multi-payload-events.md): an
    /// event carrying both a log and a metric reports both as present simultaneously, with no
    /// lossy single "type" to check instead.
    #[test]
    fn has_metrics_and_has_log_are_both_true_on_a_mixed_event() {
        let w = worker(
            r#"
            function process(event)
                event.attributes.both = event.has_log and event.has_metrics
                return event
            end
            "#,
        );
        let out = emitted(w.process(log_and_counter_event("hits", 1.0)).unwrap());
        assert!(matches!(out.attributes.get("both"), Some(logit_core::Value::Bool(true))));
    }

    #[test]
    fn assigning_to_a_has_accessor_reports_it_as_read_only() {
        let w = worker(
            r#"
            function process(event)
                event.has_log = true
                return event
            end
            "#,
        );
        let err = match w.process(counter_event("hits", 1.0)) {
            Err(err) => err,
            Ok(_) => panic!("expected assigning to event.has_log to be rejected"),
        };
        let message = format!("{err}");
        assert!(message.contains("read-only"), "got: {message}");
        assert!(
            !message.contains("no field"),
            "should report read-only, not a nonexistent field: {message}"
        );
    }

    /// `event.type` no longer exists (docs/adr/0012-multi-payload-events.md) -- `__index`'s
    /// existing catch-all still returns `nil` for any unrecognized key, so a script written
    /// against the old field silently reads `nil` (falsy) rather than erroring. Worth pinning
    /// down explicitly: this is a silent behavior change for any pre-existing script, not a hard
    /// error that would surface it.
    #[test]
    fn reading_event_dot_type_is_nil_not_an_error() {
        let w = worker(
            r#"
            function process(event)
                event.attributes.was_nil = (event.type == nil)
                return event
            end
            "#,
        );
        let out = emitted(w.process(counter_event("hits", 1.0)).unwrap());
        assert!(matches!(out.attributes.get("was_nil"), Some(logit_core::Value::Bool(true))));
    }

    #[test]
    fn returning_nil_drops_the_event() {
        let w = worker("function process(event) return nil end");
        let outcome = w.process(counter_event("hits", 1.0)).unwrap();
        assert!(matches!(outcome, ProcessOutcome::Drop));
    }

    #[test]
    fn returning_the_same_event_passes_through_unchanged() {
        let w = worker("function process(event) return event end");
        let event = counter_event("hits", 1.0);
        let ts = event.timestamp;
        let out = emitted(w.process(event).unwrap());
        assert_eq!(out.timestamp, ts);
    }

    #[test]
    fn fan_out_via_clone() {
        let w = worker(
            r#"
            function process(event)
                local copy = event:clone()
                copy.attributes.variant = "b"
                event.attributes.variant = "a"
                return {event, copy}
            end
            "#,
        );
        match w.process(counter_event("hits", 1.0)).unwrap() {
            ProcessOutcome::EmitMany(events) => {
                assert_eq!(events.len(), 2);
                assert_eq!(events[0].attributes.get("variant").and_then(|v| v.as_str()), Some("a"));
                assert_eq!(events[1].attributes.get("variant").and_then(|v| v.as_str()), Some("b"));
            }
            _ => panic!("expected EmitMany"),
        }
    }

    #[test]
    fn non_sequence_table_return_is_a_clear_error_not_a_silent_empty_emit() {
        // Table::sequence_values stops at the first gap: {[2] = event} has no key 1, so it used
        // to find nothing and silently succeed as EmitMany([]), dropping the event with no
        // indication anything was wrong. The review's exact repro.
        let w = worker("function process(event) return {[2] = event} end");
        let err = match w.process(counter_event("hits", 1.0)) {
            Ok(_) => panic!("expected process() to reject the malformed table"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("contiguous array-like table"),
            "expected a clear malformed-return error, got: {err}"
        );
    }

    #[test]
    fn non_sequence_table_return_from_flush_is_also_a_clear_error() {
        let w = worker(
            r#"
            function process(event) return event end
            function flush() return {[2] = "not even an event"} end
            "#,
        );
        w.process(counter_event("hits", 1.0)).unwrap();
        let err = match w.flush() {
            Ok(_) => panic!("expected flush() to reject the malformed table"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("contiguous array-like table"),
            "expected a clear malformed-return error, got: {err}"
        );
    }

    #[test]
    fn stashing_and_returning_the_same_event_alias_fails_clearly_on_later_use() {
        // AnyUserData::take empties the *shared Lua box*, not just the extracted Rust handle: a
        // Lua userdata is a reference type, so `pending = event` doesn't clone anything at the
        // Rust level -- `pending` and the returned value are the exact same underlying box. The
        // review's exact repro: process() stashes the event *and* returns it in the same call;
        // by the time flush() tries to use the stashed alias, it's already been taken/destructed.
        // This must fail with this crate's own clear error, not mlua's internal
        // "UserDataDestructed" terminology.
        let w = worker(
            r#"
            local pending = nil
            function process(event)
                pending = event
                return event
            end
            function flush()
                return {pending}
            end
            "#,
        );
        emitted(w.process(counter_event("hits", 1.0)).unwrap());
        let err = match w.flush() {
            Ok(_) => panic!("expected flush() to fail: pending should already be destructed"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("already been returned") || err.contains("consumed"),
            "expected this crate's own clear error, not mlua's raw one: {err}"
        );
    }

    #[test]
    fn stashing_and_returning_event_attributes_fails_clearly_on_later_use() {
        // The same failure class as the test above, discovered from a different place: caching
        // `event.attributes` means the `AttrsProxy` handle stashed here is destructed too, once
        // `into_inner` releases it as part of returning `event` from process() -- not just the
        // `EventProxy` handle `take_event` catches directly. The review's exact repro: stash
        // `event.attributes` (not `event` itself) in a Lua local, return the event, then read the
        // stash from flush(). Before this fix, this failed with mlua's raw
        // "a destructed callback or destructed userdata method was called" instead of this
        // crate's own wording.
        let w = worker(
            r#"
            local pending_attrs = nil
            function process(event)
                pending_attrs = event.attributes
                return event
            end
            function flush()
                pending_attrs.env = "prod"
                return {}
            end
            "#,
        );
        emitted(w.process(counter_event("hits", 1.0)).unwrap());
        let err = match w.flush() {
            Ok(_) => panic!("expected flush() to fail: pending_attrs should already be destructed"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("already returned") || err.contains("consumed"),
            "expected this crate's own clear error, not mlua's raw one: {err}"
        );
        assert!(
            err.contains("attributes"),
            "expected the error to call out event.attributes specifically: {err}"
        );
        assert!(
            !err.contains("destructed"),
            "expected this crate's own wording, not mlua's \"destructed\" terminology: {err}"
        );
    }

    #[test]
    fn cloning_before_stashing_avoids_the_alias_problem() {
        // The documented workaround for the pattern above: stash an independent clone, not the
        // live alias, so returning the original doesn't invalidate the stashed copy.
        let w = worker(
            r#"
            local pending = nil
            function process(event)
                pending = event:clone()
                return event
            end
            function flush()
                if pending then
                    local e = pending
                    pending = nil
                    return {e}
                end
                return {}
            end
            "#,
        );
        emitted(w.process(counter_event("hits", 1.0)).unwrap());
        assert_eq!(w.flush().unwrap().len(), 1);
    }

    #[test]
    fn to_table_exposes_timestamp_attributes_and_has_flags() {
        let w = worker(
            r#"
            function process(event)
                local t = event:to_table()
                event.attributes.snapshot_has_metrics = t.has_metrics
                event.attributes.snapshot_has_log = t.has_log
                event.attributes.snapshot_has_span = t.has_span
                event.attributes.snapshot_ts = t.timestamp -- already a string, see proxy.rs
                event.attributes.snapshot_attr = t.attributes.existing
                return event
            end
            "#,
        );
        let mut event = counter_event("hits", 1.0);
        event.attributes.insert("existing", "value");
        let ts = event.timestamp;
        let out = emitted(w.process(event).unwrap());
        assert!(matches!(
            out.attributes.get("snapshot_has_metrics"),
            Some(logit_core::Value::Bool(true))
        ));
        assert!(matches!(
            out.attributes.get("snapshot_has_log"),
            Some(logit_core::Value::Bool(false))
        ));
        assert!(matches!(
            out.attributes.get("snapshot_has_span"),
            Some(logit_core::Value::Bool(false))
        ));
        assert_eq!(
            out.attributes.get("snapshot_ts").and_then(|v| v.as_str()),
            Some(ts.to_string().as_str())
        );
        assert_eq!(out.attributes.get("snapshot_attr").and_then(|v| v.as_str()), Some("value"));
    }

    /// `ScriptWorker` isn't `Debug` (it wraps a `Lua` VM), so `Result::unwrap_err` -- which needs
    /// `Debug` on the `Ok` side to format its panic message -- doesn't work here.
    fn expect_err(source: &str) -> ScriptError {
        match ScriptWorker::new(source) {
            Ok(_) => panic!("expected this script to fail to load"),
            Err(e) => e,
        }
    }

    #[test]
    fn missing_process_function_is_rejected_at_load_time() {
        assert!(matches!(
            expect_err("function not_process(event) return event end"),
            ScriptError::MissingProcess
        ));
    }

    #[test]
    fn syntax_error_is_rejected_at_load_time() {
        assert!(matches!(expect_err("this is not lua ("), ScriptError::Lua(_)));
    }

    /// `process`/`flush` are now resolved once at load time (a cached `RegistryKey`, not a fresh
    /// `_G` lookup per call -- see `ScriptWorker::new`), which means a `flush` global that exists
    /// but isn't a function (a plausible copy-paste or renamed-variable mistake, e.g. `flush = 5`)
    /// must be rejected right here, the same way a missing `process` already is -- not resolved as
    /// "no flush" and then silently emit nothing at every flush tick forever. The eager-resolution
    /// review's exact repro.
    #[test]
    fn flush_bound_to_a_non_function_value_is_rejected_at_load_time() {
        assert!(matches!(
            expect_err("function process(event) return event end\nflush = 5"),
            ScriptError::Lua(_)
        ));
    }

    #[test]
    fn os_library_is_not_available() {
        let w = worker(
            "function process(event) event.attributes.has_os = (os == nil) return event end",
        );
        let out = emitted(w.process(counter_event("hits", 1.0)).unwrap());
        assert!(matches!(out.attributes.get("has_os"), Some(logit_core::Value::Bool(true))));
    }

    #[test]
    fn io_library_is_not_available() {
        let w = worker(
            "function process(event) event.attributes.has_io = (io == nil) return event end",
        );
        let out = emitted(w.process(counter_event("hits", 1.0)).unwrap());
        assert!(matches!(out.attributes.get("has_io"), Some(logit_core::Value::Bool(true))));
    }

    #[test]
    fn ffi_library_is_not_available() {
        let w = worker(
            "function process(event) event.attributes.has_ffi = (ffi == nil) return event end",
        );
        let out = emitted(w.process(counter_event("hits", 1.0)).unwrap());
        assert!(matches!(out.attributes.get("has_ffi"), Some(logit_core::Value::Bool(true))));
    }

    #[test]
    fn require_is_not_available() {
        let w = worker(
            "function process(event) event.attributes.has_require = (require == nil) return event end",
        );
        let out = emitted(w.process(counter_event("hits", 1.0)).unwrap());
        assert!(matches!(out.attributes.get("has_require"), Some(logit_core::Value::Bool(true))));
    }

    /// `StdLib` selection doesn't gate Lua 5.1's base library at all -- `loadfile`/`dofile`/
    /// `load`/`loadstring`/`getfenv`/`setfenv` load unconditionally regardless of which `StdLib`
    /// flags are set, unless explicitly removed (see `remove_unsandboxed_base_globals`). One test
    /// per global, matching the os/io/ffi/require style above, not a combined assertion -- so a
    /// regression in any single one fails on its own.
    fn assert_global_is_nil(global: &str) {
        let source = format!(
            "function process(event) event.attributes.present = ({global} ~= nil) return event end"
        );
        let w = worker(&source);
        let out = emitted(w.process(counter_event("hits", 1.0)).unwrap());
        assert!(
            matches!(out.attributes.get("present"), Some(logit_core::Value::Bool(false))),
            "expected global '{global}' to be nil"
        );
    }

    #[test]
    fn loadfile_is_not_available() {
        assert_global_is_nil("loadfile");
    }

    #[test]
    fn dofile_is_not_available() {
        assert_global_is_nil("dofile");
    }

    #[test]
    fn load_is_not_available() {
        assert_global_is_nil("load");
    }

    #[test]
    fn loadstring_is_not_available() {
        assert_global_is_nil("loadstring");
    }

    #[test]
    fn getfenv_is_not_available() {
        assert_global_is_nil("getfenv");
    }

    #[test]
    fn setfenv_is_not_available() {
        assert_global_is_nil("setfenv");
    }

    #[test]
    fn flush_returns_events_a_script_stashed_from_process() {
        let w = worker(
            r#"
            local pending = nil
            function process(event)
                pending = event
                return nil
            end
            function flush()
                if pending then
                    local e = pending
                    pending = nil
                    return {e}
                end
                return {}
            end
            "#,
        );
        let outcome = w.process(counter_event("hits", 1.0)).unwrap();
        assert!(matches!(outcome, ProcessOutcome::Drop));

        let flushed = w.flush().unwrap();
        assert_eq!(flushed.len(), 1);

        // Second flush with nothing pending returns nothing.
        assert_eq!(w.flush().unwrap().len(), 0);
    }

    #[test]
    fn flush_is_a_noop_when_the_script_defines_none() {
        let w = worker("function process(event) return event end");
        assert_eq!(w.flush().unwrap().len(), 0);
    }

    #[test]
    fn with_telemetry_lets_process_emit_its_own_metric() {
        use logit_core::Registry;

        let registry = Registry::new();
        let telemetry = registry.telemetry_for("script", "lua", "transform");
        let w = ScriptWorker::new(
            r#"
            function process(event)
                telemetry.count("orders.total", 1)
                return event
            end
            "#,
        )
        .expect("script should load")
        .with_telemetry(telemetry)
        .expect("installing telemetry should not fail");

        w.process(counter_event("hits", 1.0)).unwrap();

        let events = registry.drain(0);
        assert_eq!(events.len(), 1);
        assert_eq!(logit_core::interner::resolve(events[0].metrics[0].name), "orders.total");
    }

    #[test]
    fn used_memory_reports_a_positive_byte_count() {
        let w = worker("function process(event) return event end");
        assert!(w.used_memory() > 0, "a loaded Lua VM should already have some memory in use");
    }

    /// `trace.trace_id`/`trace.span_id` are readable inside `process()` -- installed for every
    /// worker unconditionally (`crate::trace`'s module doc, unlike `telemetry`'s opt-in builder),
    /// starting at the all-zero placeholder and changing once `set_trace_context` is called, the
    /// same way `run_lua` calls it once per incoming batch
    /// (`crates/logit-pipeline/src/runtime.rs`).
    #[test]
    fn trace_context_is_readable_in_process_and_changes_after_set_trace_context() {
        let w = worker(
            r#"
            function process(event)
                event.attributes.trace_id = trace.trace_id
                event.attributes.span_id = trace.span_id
                return event
            end
            "#,
        );

        let before = emitted(w.process(counter_event("hits", 1.0)).unwrap());
        assert_eq!(
            before.attributes.get("trace_id").and_then(|v| v.as_str()),
            Some(&"0".repeat(32)[..])
        );
        assert_eq!(
            before.attributes.get("span_id").and_then(|v| v.as_str()),
            Some(&"0".repeat(16)[..])
        );

        w.set_trace_context([0xab; 16], [0xcd; 8])
            .expect("setting the trace context should not fail");

        let after = emitted(w.process(counter_event("hits", 1.0)).unwrap());
        assert_eq!(
            after.attributes.get("trace_id").and_then(|v| v.as_str()),
            Some(&"ab".repeat(16)[..])
        );
        assert_eq!(
            after.attributes.get("span_id").and_then(|v| v.as_str()),
            Some(&"cd".repeat(8)[..])
        );
    }
}
