//! Embeds LuaJIT (via `mlua`, vendored) and runs user `process`/`flush` scripts against
//! [`logit_core::Event`]. See `docs/design/lua-api.md` for the full design and, in particular,
//! the concurrency rules below.
//!
//! `mlua::Lua` is neither `Send` nor `Sync` -- that is a hard constraint from the embedded VM, not
//! a preference. [`ScriptWorker`] is the enforcement point: it owns one `Lua` instance and is
//! itself `!Send`, so the type system stops a `Lua` from being shared across pipeline workers
//! rather than relying on a convention nobody checks. The pipeline runs one [`ScriptWorker`] per
//! worker thread.

use logit_core::Event;
use mlua::{Lua, LuaOptions, StdLib, Value as LuaValue};
use std::marker::PhantomData;

mod proxy;
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
    /// source doesn't parse/execute, or doesn't define a `process` function -- a config error
    /// should surface immediately, not at the first event that happens to flow through.
    pub fn new(source: &str) -> Result<Self, ScriptError> {
        let lua = Lua::new_with(sandbox_libs(), LuaOptions::new())?;
        remove_unsandboxed_base_globals(&lua)?;
        lua.load(source).exec()?;
        let has_process =
            matches!(lua.globals().get::<_, LuaValue>("process")?, LuaValue::Function(_));
        if !has_process {
            return Err(ScriptError::MissingProcess);
        }
        Ok(Self { lua, _not_send_sync: PhantomData })
    }

    /// Runs this worker's `process(event)` once. See `docs/design/lua-api.md` for the
    /// proxy-vs-table-conversion tradeoff [`EventProxy`] exists to avoid.
    pub fn process(&self, event: Event) -> Result<ProcessOutcome, ScriptError> {
        let process: mlua::Function = self.lua.globals().get("process")?;
        let result: LuaValue = process.call(EventProxy::new(event))?;
        Ok(match result {
            LuaValue::Nil => ProcessOutcome::Drop,
            LuaValue::UserData(ud) => ProcessOutcome::Emit(Box::new(proxy::take_event(ud)?)),
            LuaValue::Table(table) => {
                ProcessOutcome::EmitMany(events_from_table(table, "process")?)
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
        let flush: Option<mlua::Function> = self.lua.globals().get("flush")?;
        let Some(flush) = flush else {
            return Ok(Vec::new());
        };
        let result: LuaValue = flush.call(())?;
        Ok(match result {
            LuaValue::Nil => Vec::new(),
            LuaValue::Table(table) => events_from_table(table, "flush")?,
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
fn events_from_table(table: mlua::Table, caller: &str) -> Result<Vec<Event>, ScriptError> {
    let Some(len) = value::validated_sequence_len(&table)? else {
        return Err(ScriptError::Lua(mlua::Error::RuntimeError(format!(
            "{caller}() must return a contiguous array-like table of events (found non-sequence keys)"
        ))));
    };
    let mut events = Vec::with_capacity(len);
    for i in 1..=len {
        events.push(proxy::take_event(table.get(i)?)?);
    }
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{interner::intern, AttrMap, MetricKind, MetricRecord, Payload};

    fn counter_event(name: &str, value: f64) -> Event {
        Event {
            timestamp: 1_700_000_000_000_000_000,
            attributes: AttrMap::new(),
            payload: Payload::Metric(MetricRecord {
                name: intern(name),
                kind: MetricKind::Counter(value),
                unit: None,
            }),
        }
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
        // The fix represents this as an exact decimal string, not (as the review's repro showed)
        // a silently-wrong Lua number -- so the expectation here is Str, not I64.
        assert_eq!(
            out.attributes.get("x"),
            Some(&logit_core::Value::Str(bytes::Bytes::from(9_007_199_254_740_993i64.to_string())))
        );
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
        assert_eq!(
            out.attributes.get("x"),
            Some(&logit_core::Value::Str(bytes::Bytes::from(u64::MAX.to_string())))
        );
    }

    #[test]
    fn empty_table_decodes_as_map_not_array() {
        // An empty Lua table can't carry which of Value::Array(vec![])/Value::Map(AttrMap::new())
        // it came from -- there's nothing to inspect. An earlier version always decoded it as
        // Array (fixing Array losing its variant by breaking Map the other way); this documents
        // and tests the chosen default instead: an empty table becomes Map, since attributes
        // (map-shaped) are the primary thing scripts manipulate. See value.rs's
        // lua_table_to_value for the full reasoning, and tmp/lua-value-type-preservation.md for
        // why a real fix (tagging) is deferred rather than attempted here.
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
    fn safe_u64_round_trips_to_i64_documented_number_branch_loss() {
        // Part of the same deferred type-loss limitation as the string-branch cases above, just
        // via Lua's number representation instead: a Lua integer has no signed/unsigned tag, so a
        // safe (in-range) U64 comes back as I64 even though the numeric value survives exactly.
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
        assert_eq!(out.attributes.get("x"), Some(&logit_core::Value::I64(42)));
    }

    #[test]
    fn whole_number_f64_round_trips_to_i64_documented_number_branch_loss() {
        // LuaJIT's dual-number mode canonicalizes an integral Lua number as an integer
        // internally, so lua_to_value sees LuaValue::Integer regardless of how the value was
        // originally pushed (LuaValue::Number(42.0)) -- outside this crate's control, not a bug
        // in the conversion logic here.
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
        assert_eq!(out.attributes.get("x"), Some(&logit_core::Value::I64(42)));
    }

    #[test]
    fn fractional_f64_round_trips_correctly_the_contrast_case() {
        // Unlike a whole-number float, a fractional one has no integer representation in LuaJIT
        // to be canonicalized into -- showing the loss above is specific to integral floats, not
        // floats in general.
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
    fn event_type_is_readable() {
        let w = worker(
            r#"
            function process(event)
                event.attributes.kind = event.type
                return event
            end
            "#,
        );
        let out = emitted(w.process(counter_event("hits", 1.0)).unwrap());
        assert_eq!(out.attributes.get("kind").and_then(|v| v.as_str()), Some("metric"));
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
    fn to_table_exposes_timestamp_attributes_and_type() {
        let w = worker(
            r#"
            function process(event)
                local t = event:to_table()
                event.attributes.snapshot_type = t.type
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
        assert_eq!(out.attributes.get("snapshot_type").and_then(|v| v.as_str()), Some("metric"));
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
}
