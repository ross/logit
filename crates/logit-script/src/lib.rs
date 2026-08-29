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
                let mut events = Vec::new();
                for item in table.sequence_values::<mlua::AnyUserData>() {
                    events.push(proxy::take_event(item?)?);
                }
                ProcessOutcome::EmitMany(events)
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
            LuaValue::Table(table) => {
                let mut events = Vec::new();
                for item in table.sequence_values::<mlua::AnyUserData>() {
                    events.push(proxy::take_event(item?)?);
                }
                events
            }
            other => {
                return Err(ScriptError::Lua(mlua::Error::RuntimeError(format!(
                    "flush() must return nil or a table of events, got {}",
                    other.type_name()
                ))))
            }
        })
    }
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
