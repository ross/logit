//! Embeds LuaJIT (via `mlua`, vendored) and runs user `process`/`flush` scripts against
//! [`logit_core::Event`]. See `docs/design/lua-api.md`, including its concurrency rules.
//!
//! `mlua::Lua` is neither `Send` nor `Sync`, a hard constraint of the embedded VM. [`ScriptWorker`]
//! owns one `Lua` and is itself `!Send`/`!Sync` through a `PhantomData<*const ()>` marker, so the
//! type system, not a convention, stops a VM being shared across pipeline workers. The pipeline
//! runs one [`ScriptWorker`] per worker thread. Don't remove the marker to make something compile.

use logit_core::{Event, Resource, Scope, Telemetry};
use mlua::{Lua, LuaOptions, RegistryKey, StdLib, Value as LuaValue};
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

mod construct;
mod provenance;
mod proxy;
mod resource;
mod scope;
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

/// The standard libraries scripts get: `table`, `string`, and `math`, nothing that reaches the
/// host. The base library (`pairs`, `type`, `tostring`) isn't gated by a `StdLib` flag.
///
/// Listed explicitly rather than trusting `Lua::new()`'s default: LuaJIT's `ffi` is a sandbox
/// escape (raw memory, arbitrary C calls), and mlua doesn't promise `Lua::new()` excludes it. No
/// `PACKAGE`, so no `require`.
fn sandbox_libs() -> StdLib {
    StdLib::TABLE | StdLib::STRING | StdLib::MATH
}

/// Removes the base-library globals that escape the sandbox; `sandbox_libs` can't, because no
/// `StdLib` flag gates the base library.
///
/// - `loadfile`, `dofile` read and execute a file from the process's filesystem.
/// - `load`, `loadstring` run a constructed string as code, so more than the one script source
///   this worker was built from could run.
/// - `getfenv`, `setfenv` inspect and replace a function's environment table, the tampering a
///   restricted stdlib exists to prevent.
///
/// `lua.globals()` is `_G` itself, not a copy.
fn remove_unsandboxed_base_globals(lua: &Lua) -> mlua::Result<()> {
    let globals = lua.globals();
    for name in ["loadfile", "dofile", "load", "loadstring", "getfenv", "setfenv"] {
        globals.set(name, LuaValue::Nil)?;
    }
    Ok(())
}

/// Owns one Lua VM and runs its `process`/`flush` globals, for one pipeline stage on one worker.
/// Not `Send`/`Sync`, via the `PhantomData<*const ()>` marker; see the module doc.
pub struct ScriptWorker {
    lua: Lua,
    /// The script's `process`/`flush`, resolved once at load rather than looked up in `_G` per
    /// call (`docs/design/memory.md` §8). A `RegistryKey`, because `mlua::Function<'lua>` borrows
    /// `self.lua` and can't be a field. `flush` is `None` for a stateless script.
    process: RegistryKey,
    flush: Option<RegistryKey>,
    /// The `trace` global's table, which [`ScriptWorker::set_trace_context`] mutates in place.
    trace_table: RegistryKey,
    /// Shared with the `resource` global's userdata. An `Rc`, not a `RegistryKey`, so
    /// `set_resource`/`take_resource` need no `&Lua`.
    resource_state: Rc<RefCell<resource::ResourceState>>,
    /// Shared with the `scope` global's userdata, as `resource_state`.
    scope_state: Rc<RefCell<scope::ScopeState>>,
    /// Shared with the `provenance` global's userdata, as `resource_state`.
    provenance_state: Rc<RefCell<provenance::ProvenanceState>>,
    /// This component's `targets:` as the name-to-slot table `event:to(id)` resolves against
    /// (`docs/adr/target-components.md`). Built once by [`ScriptWorker::with_targets`] and shared
    /// by `Rc` with every [`EventProxy`]; the shared empty table (`proxy::no_targets`) until then.
    ///
    /// Behind a `RefCell` because the `Event.new` global (`crate::construct`) is installed in
    /// `new`, before `with_targets` runs, and reads the table through this cell on each call.
    /// `process` only borrows and bumps the `Rc`, so the `lua: process 1 event` allocation pin
    /// (`crates/logit-bench/tests/allocations.rs`) doesn't move.
    targets: Rc<RefCell<Rc<proxy::TargetTable>>>,
    _not_send_sync: PhantomData<*const ()>,
}

/// What running a script's `process` returned, per the contract in `docs/design/lua-api.md`.
///
/// `Emit` is boxed: `Event`'s inline attribute storage (`docs/design/data-model.md`'s small-map
/// layout) makes it large enough that clippy flags the size gap against `Drop`.
///
/// Every emitted event carries its own routing mark: the target slot `event:to(id)` set on that
/// event's handle, in `graph::targets_of` order, or `None`. Per event, not per call, because
/// `return {a:to("x"), b}` is one `EmitMany` whose events go two ways
/// (`docs/adr/target-components.md`).
pub enum ProcessOutcome {
    /// Pass the (possibly mutated) event through, to the slot it was marked for.
    Emit(Box<Event>, Option<u16>),
    /// The script returned multiple events (fan-out), each with its own mark.
    EmitMany(Vec<(Event, Option<u16>)>),
    /// The script returned `nil`: drop the event.
    Drop,
}

impl ScriptWorker {
    /// Loads and sandboxes a script (see [`sandbox_libs`]).
    ///
    /// Fails at load, not at the first event or flush tick, if the source doesn't parse or run,
    /// defines no `process` function, or binds `flush` to something other than a function or
    /// `nil`.
    ///
    /// `process`/`flush` are resolved once, here, so a script that reassigns `_G.process` or
    /// `_G.flush` later changes nothing that runs (`docs/design/lua-api.md` says so).
    pub fn new(source: &str) -> Result<Self, ScriptError> {
        let lua = Lua::new_with(sandbox_libs(), LuaOptions::new())?;
        remove_unsandboxed_base_globals(&lua)?;
        // Every unconditional global is installed before `.exec()`: top-level code runs once,
        // there, and a top-level alias (`local ctx = trace`) would otherwise capture `nil` for
        // good (`crate::trace`'s module doc). Only `telemetry`, reached from function bodies at
        // call time, installs later.
        let trace_table = trace::install(&lua)?;
        let resource_state = resource::install(&lua)?;
        let scope_state = scope::install(&lua)?;
        let provenance_state = provenance::install(&lua)?;
        // `Event.new` reads the targets cell at call time, so a later `with_targets` still takes
        // effect; an `Event.new` at top level, during `.exec()`, sees the empty table
        // (`docs/design/lua-api.md` says so).
        let targets = Rc::new(RefCell::new(proxy::TargetTable::empty()));
        construct::install(&lua, targets.clone())?;
        lua.load(source).exec()?;
        let process_fn = match lua.globals().get::<_, LuaValue>("process")? {
            LuaValue::Function(f) => f,
            _ => return Err(ScriptError::MissingProcess),
        };
        let process = lua.create_registry_value(process_fn)?;
        // `nil` becomes `None`; any other non-function (`flush = 5`) is a load error, not a
        // silent "no flush" that loses events at every tick.
        let flush_fn: Option<mlua::Function> = lua.globals().get("flush")?;
        let flush = flush_fn.map(|f| lua.create_registry_value(f)).transpose()?;
        Ok(Self {
            lua,
            process,
            flush,
            trace_table,
            resource_state,
            scope_state,
            provenance_state,
            targets,
            _not_send_sync: PhantomData,
        })
    }

    /// Sets the `trace` global's `trace_id`/`span_id` for the next `process()`/`flush()` call.
    ///
    /// `run_lua` calls this once per incoming batch, before its events reach `process`, and before
    /// every `flush()` with the fresh root its emission is sent under
    /// (`docs/adr/lua-flush-root-context.md`).
    pub fn set_trace_context(
        &self,
        trace_id: [u8; 16],
        span_id: [u8; 8],
    ) -> Result<(), ScriptError> {
        trace::set_context(&self.lua, &self.trace_table, trace_id, span_id).map_err(Into::into)
    }

    /// Resets the `resource` global to `resource` for the next `process()`/`flush()` call,
    /// discarding any earlier write.
    ///
    /// `run_lua` calls this once per incoming batch, and before every `flush()` with an empty
    /// resource: a flush runs in a root context, not the last batch's
    /// (`docs/adr/lua-flush-root-context.md`).
    pub fn set_resource(&self, resource: &Arc<Resource>) {
        resource::set(&self.resource_state, resource);
    }

    /// The script's write to `resource` since the last [`ScriptWorker::set_resource`], if any.
    /// `run_lua` calls this after a batch's `process()` calls and after `flush()`.
    pub fn take_resource(&self) -> Option<Arc<Resource>> {
        resource::take(&self.resource_state)
    }

    /// Resets the `scope` global to `scope` (defaults for `None`, per `crate::scope`) for the next
    /// `process()`/`flush()` call, discarding any earlier write.
    ///
    /// Called as `set_resource` is, with `None` before every `flush()`.
    pub fn set_scope(&self, scope: &Option<Arc<Scope>>) {
        scope::set(&self.scope_state, scope);
    }

    /// The script's write to `scope` since the last [`ScriptWorker::set_scope`], if any. Called
    /// where `take_resource` is.
    pub fn take_scope(&self) -> Option<Arc<Scope>> {
        scope::take(&self.scope_state)
    }

    /// Sets the read-only `provenance` global's `origin`/`previous` for the next
    /// `process()`/`flush()` call.
    ///
    /// `run_lua` calls this once per incoming batch, and before every `flush()` with this worker's
    /// own component as both, which is what the flushed batch is stamped with
    /// (`docs/adr/lua-flush-root-context.md`).
    pub fn set_provenance(&self, provenance: logit_core::Provenance) {
        provenance::set(&self.provenance_state, provenance)
    }

    /// Installs a `telemetry` global so `process()`/`flush()` can emit their own metrics.
    ///
    /// Safe after `new`, since a function body resolves `telemetry` at call time; a top-level
    /// alias of it captures `nil`. See `crate::telemetry` and `docs/design/lua-api.md`.
    pub fn with_telemetry(self, telemetry: Telemetry) -> Result<Self, ScriptError> {
        telemetry::install(&self.lua, telemetry)?;
        Ok(self)
    }

    /// Sets `provenance.component` to this worker's component id for the rest of its lifetime.
    ///
    /// Safe any time after `new`: a top-level alias of `provenance` holds the userdata, not a
    /// snapshot (`crate::provenance`'s module doc).
    pub fn with_component(self, id: &str) -> Self {
        provenance::set_component(&self.provenance_state, id);
        self
    }

    /// Declares the `target` ids this component may direct events into, in
    /// `logit_pipeline::graph::targets_of` slot order -- what `event:to(id)` resolves against, and
    /// the list an unknown id's error message names (`docs/adr/target-components.md`).
    ///
    /// Safe any time after `new`: the table is reached only through the `targets` cell, which
    /// `process` and `Event.new` (`crate::construct`) read per call. The one exception is an
    /// `Event.new` in top-level code, which runs during `new` and sees the empty table. The table
    /// is built once, here, and shared by `Rc` with every proxy.
    ///
    /// An empty slice is the same as never calling this: `event:to(..)` is then a script error
    /// for any id, never a silent forward.
    pub fn with_targets(self, targets: &[String]) -> Self {
        *self.targets.borrow_mut() = match targets.is_empty() {
            true => proxy::TargetTable::empty(),
            false => Rc::new(proxy::TargetTable::new(targets)),
        };
        self
    }

    /// Bytes in use by this worker's Lua VM, the only view into a stateful script leaking state
    /// across `flush()` calls.
    pub fn used_memory(&self) -> usize {
        self.lua.used_memory()
    }

    /// Runs this worker's `process(event)` once.
    ///
    /// The event reaches the script as an [`EventProxy`], not a converted table
    /// (`docs/design/lua-api.md`). The proxy carries this worker's target table, and an
    /// `event:to(id)` mark comes back on the outcome, never on the `Event` itself
    /// (`docs/adr/target-components.md`).
    pub fn process(&self, event: Event) -> Result<ProcessOutcome, ScriptError> {
        let process: mlua::Function = self.lua.registry_value(&self.process)?;
        let result: LuaValue = process
            .call(EventProxy::with_targets(event, self.targets.borrow().clone()))
            .map_err(proxy::clarify_destructed_handle_use)?;
        Ok(match result {
            LuaValue::Nil => ProcessOutcome::Drop,
            LuaValue::UserData(ud) => {
                let (event, target) = proxy::take_event(&self.lua, ud)?;
                ProcessOutcome::Emit(Box::new(event), target)
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

    /// Runs this worker's `flush()`, or returns nothing if the script defines none.
    ///
    /// Each flushed event carries its own routing mark, as a `process()`-emitted one does
    /// (`docs/adr/target-components.md`). `event:clone()` copies the mark and the target table, so
    /// a clone stashed in `process()` can be routed here, as can `Event.new(..):to("x")`.
    ///
    /// `now` is the runtime's tick time, the `now_unix_nanos()` a native `aggregate` gets, passed
    /// as `flush(now)` in decimal nanos: a string, as `event.timestamp` is, so
    /// `Event.new{timestamp = now, ..}` works with no clock in the sandbox
    /// (`docs/adr/lua-event-constructor.md`). A `function flush()` ignores it.
    pub fn flush(&self, now: i64) -> Result<Vec<(Event, Option<u16>)>, ScriptError> {
        let Some(flush_key) = self.flush.as_ref() else {
            return Ok(Vec::new());
        };
        let flush: mlua::Function = self.lua.registry_value(flush_key)?;
        let result: LuaValue =
            flush.call(now.to_string()).map_err(proxy::clarify_destructed_handle_use)?;
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

/// Extracts the events, each with its routing mark, from a table `process()` or `flush()`
/// returned. `caller` names which, for the error message.
///
/// Validates a contiguous `1..=n` sequence rather than using `Table::sequence_values`, which
/// stops at the first gap: `return {[2] = event}` would silently emit nothing. An empty table is
/// valid and emits nothing. Every read is raw, as in `crate::value`'s conversion, so a returned
/// table's metatable runs no code here.
fn events_from_table(
    lua: &Lua,
    table: mlua::Table,
    caller: &str,
) -> Result<Vec<(Event, Option<u16>)>, ScriptError> {
    let Some(len) = value::validated_sequence_len(&table)? else {
        return Err(ScriptError::Lua(mlua::Error::RuntimeError(format!(
            "{caller}() must return a contiguous array-like table of events (found non-sequence keys)"
        ))));
    };
    let mut events = Vec::with_capacity(len);
    for i in 1..=len {
        events.push(proxy::take_event(lua, table.raw_get(i)?)?);
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
            MetricRecord::new(intern(name), MetricKind::counter(value)),
        )
    }

    /// An event carrying both a log and a metric, the shape `kv_metrics` produces.
    fn log_and_counter_event(name: &str, value: f64) -> Event {
        let mut event = counter_event(name, value);
        event.log = Some(LogRecord {
            message: logit_core::Value::str("GET /"),
            severity: None,
            body_format: logit_core::BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        });
        event
    }

    fn worker(source: &str) -> ScriptWorker {
        ScriptWorker::new(source).expect("script should load")
    }

    fn emitted(outcome: ProcessOutcome) -> Event {
        match outcome {
            ProcessOutcome::Emit(e, _) => *e,
            _ => panic!("expected Emit"),
        }
    }

    /// `unwrap_err` for `process`, whose `ProcessOutcome` isn't `Debug`.
    fn process_err(w: &ScriptWorker, event: Event) -> String {
        match w.process(event) {
            Err(err) => err.to_string(),
            Ok(_) => panic!("expected process() to reject this script"),
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
        // A unix-nanos timestamp (~1.7e18) is past a double's exact range, hence the string.
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
        // 2^53 + 1, one past a double's exact range.
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
        // The string branch's identity assignment is a no-op, so the variant stays I64.
        assert_eq!(out.attributes.get("x"), Some(&logit_core::Value::I64(9_007_199_254_740_993)));
    }

    #[test]
    fn small_i64_attribute_stays_a_real_lua_number() {
        // A small integer arrives as a Lua number, so a script can do arithmetic on it.
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
        // LuaJIT's dual-number mode keeps small-integer arithmetic an integer, not a float.
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
        // The string branch's identity assignment is a no-op, so the variant stays U64.
        assert_eq!(out.attributes.get("x"), Some(&logit_core::Value::U64(u64::MAX)));
    }

    // `AttrsProxy::__newindex`'s no-op-assignment rule (`lua_value_matches`), which keeps a
    // variant through an identity assignment (`docs/design/lua-value-type-preservation.md`).

    #[test]
    fn bytes_attribute_with_valid_utf8_stays_bytes_through_identity_round_trip() {
        // UTF-8 `Bytes` reaches Lua as a plain string, like `Str`; `influxdb_out` makes a `Str`
        // a tag and a `Bytes` not, so the variant matters.
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
        // Non-UTF-8 `Bytes` would round-trip even without the no-op rule, via `lua_to_value`.
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
        // Large enough to take `value_to_lua`'s string branch, as real timestamps are.
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
        // Exercises `lua_value_matches`'s Integer arm, not its String arm.
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
        // LuaJIT hands 42.0 back as an Integer; without the no-op rule this would become I64.
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
        // A fractional float stays a Lua number, so it never hit the integral-float collapse.
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
        // The no-op rule compares content: a new string built from an old value becomes `Str`.
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
        // The design doc's scenario: an enrichment stage copying every attribute back through
        // `to_table()` must not change any attribute's variant.
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
        // A documented gap: the no-op rule matches content at the same key only, so a copy to a
        // new key becomes `Str` (`docs/adr/lua-value-identity-preservation.md`'s Consequences).
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
        // A documented gap: `lua_value_matches` doesn't recurse into a table, so a nested
        // element's variant is lost (`docs/design/lua-value-type-preservation.md`'s "Known
        // residual gaps").
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
        // `{}` can't say which it came from; the chosen default is `Map` (`lua_table_to_value`).
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
        // LuaJIT's `#` returns 4 here, so a pair-count check would decode a truncated array and
        // drop `extra` (`validated_sequence_len`).
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

    /// A log-and-metric event reports both present (`docs/adr/multi-payload-events.md`).
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

    #[test]
    fn event_dot_log_is_nil_when_the_event_has_no_log() {
        let w = worker(
            r#"
            function process(event)
                event.attributes.log_is_nil = (event.log == nil)
                return event
            end
            "#,
        );
        let out = emitted(w.process(counter_event("hits", 1.0)).unwrap());
        assert!(matches!(out.attributes.get("log_is_nil"), Some(logit_core::Value::Bool(true))));
    }

    #[test]
    fn event_dot_log_reads_message_severity_and_body_format() {
        let w = worker(
            r#"
            function process(event)
                event.attributes.message = event.log.message
                event.attributes.body_format = event.log.body_format
                event.attributes.severity_is_nil = (event.log.severity == nil)
                event.attributes.trace_id_is_nil = (event.log.trace_id == nil)
                event.attributes.trace_flags_is_nil = (event.log.trace_flags == nil)
                return event
            end
            "#,
        );
        let out = emitted(w.process(log_and_counter_event("hits", 1.0)).unwrap());
        assert_eq!(out.attributes.get("message").and_then(|v| v.as_str()), Some("GET /"));
        assert_eq!(out.attributes.get("body_format").and_then(|v| v.as_str()), Some("raw"));
        assert!(matches!(
            out.attributes.get("severity_is_nil"),
            Some(logit_core::Value::Bool(true))
        ));
        assert!(matches!(
            out.attributes.get("trace_id_is_nil"),
            Some(logit_core::Value::Bool(true))
        ));
        assert!(matches!(
            out.attributes.get("trace_flags_is_nil"),
            Some(logit_core::Value::Bool(true))
        ));
    }

    #[test]
    fn writing_trace_id_then_reading_it_back_round_trips_as_lowercase_hex() {
        let w = worker(
            r#"
            function process(event)
                event.log.trace_id = "ab000000000000000000000000000000"
                event.attributes.trace_id = event.log.trace_id
                event.attributes.span_id_is_nil = (event.log.span_id == nil)
                event.attributes.flags = event.log.trace_flags
                return event
            end
            "#,
        );
        let trace_id_hex = "ab000000000000000000000000000000";
        assert_eq!(trace_id_hex.len(), 32, "fixture bug: not a valid 16-byte trace id");
        let out = emitted(w.process(log_and_counter_event("hits", 1.0)).unwrap());
        assert_eq!(out.attributes.get("trace_id").and_then(|v| v.as_str()), Some(trace_id_hex));
        assert!(matches!(
            out.attributes.get("span_id_is_nil"),
            Some(logit_core::Value::Bool(true))
        ));
        assert!(matches!(out.attributes.get("flags"), Some(logit_core::Value::I64(0))));
    }

    #[test]
    fn writing_span_id_and_trace_flags_after_trace_id_works() {
        let w = worker(
            r#"
            function process(event)
                event.log.trace_id = "ab000000000000000000000000000000"
                event.log.span_id = "cd00000000000000"
                event.log.trace_flags = 1
                event.attributes.span_id = event.log.span_id
                event.attributes.flags = event.log.trace_flags
                return event
            end
            "#,
        );
        let out = emitted(w.process(log_and_counter_event("hits", 1.0)).unwrap());
        assert_eq!(
            out.attributes.get("span_id").and_then(|v| v.as_str()),
            Some("cd00000000000000")
        );
        assert!(matches!(out.attributes.get("flags"), Some(logit_core::Value::I64(1))));
    }

    #[test]
    fn writing_trace_id_to_nil_clears_span_id_and_flags_too() {
        let w = worker(
            r#"
            function process(event)
                event.log.trace_id = "ab000000000000000000000000000000"
                event.log.span_id = "cd00000000000000"
                event.log.trace_id = nil
                event.attributes.trace_id_is_nil = (event.log.trace_id == nil)
                event.attributes.span_id_is_nil = (event.log.span_id == nil)
                return event
            end
            "#,
        );
        let out = emitted(w.process(log_and_counter_event("hits", 1.0)).unwrap());
        assert!(matches!(
            out.attributes.get("trace_id_is_nil"),
            Some(logit_core::Value::Bool(true))
        ));
        assert!(matches!(
            out.attributes.get("span_id_is_nil"),
            Some(logit_core::Value::Bool(true))
        ));
    }

    #[test]
    fn reassigning_trace_id_drops_the_old_spans_id_and_flags() {
        let w = worker(
            r#"
            function process(event)
                event.log.trace_id = "ab000000000000000000000000000000"
                event.log.span_id = "cd00000000000000"
                event.log.trace_flags = 1
                event.log.trace_id = "ef000000000000000000000000000000"
                event.attributes.trace_id = event.log.trace_id
                event.attributes.span_id_is_nil = (event.log.span_id == nil)
                event.attributes.flags = event.log.trace_flags
                return event
            end
            "#,
        );
        let out = emitted(w.process(log_and_counter_event("hits", 1.0)).unwrap());
        assert_eq!(
            out.attributes.get("trace_id").and_then(|v| v.as_str()),
            Some("ef000000000000000000000000000000")
        );
        assert!(matches!(
            out.attributes.get("span_id_is_nil"),
            Some(logit_core::Value::Bool(true))
        ));
        assert!(matches!(out.attributes.get("flags"), Some(logit_core::Value::I64(0))));
    }

    #[test]
    fn writing_span_id_without_a_trace_id_first_is_a_clear_error() {
        let w = worker(
            r#"
            function process(event)
                event.log.span_id = "cd00000000000000"
                return event
            end
            "#,
        );
        let err = process_err(&w, log_and_counter_event("hits", 1.0));
        assert!(err.contains("trace_id"), "got: {err}");
    }

    #[test]
    fn writing_trace_flags_without_a_trace_id_first_is_a_clear_error() {
        let w = worker(
            r#"
            function process(event)
                event.log.trace_flags = 1
                return event
            end
            "#,
        );
        let err = process_err(&w, log_and_counter_event("hits", 1.0));
        assert!(err.contains("trace_id"), "got: {err}");
    }

    #[test]
    fn writing_an_invalid_hex_trace_id_is_a_clear_error() {
        let w = worker(
            r#"
            function process(event)
                event.log.trace_id = "not-hex"
                return event
            end
            "#,
        );
        let err = process_err(&w, log_and_counter_event("hits", 1.0));
        assert!(err.contains("trace_id"), "got: {err}");
    }

    #[test]
    fn writing_an_out_of_range_trace_flags_is_a_clear_error() {
        let w = worker(
            r#"
            function process(event)
                event.log.trace_id = "ab000000000000000000000000000000"
                event.log.trace_flags = 256
                return event
            end
            "#,
        );
        let err = process_err(&w, log_and_counter_event("hits", 1.0));
        assert!(err.contains("trace_flags"), "got: {err}");
    }

    #[test]
    fn assigning_to_message_severity_or_body_format_reports_read_only() {
        for field in ["message", "severity", "body_format"] {
            let w = worker(&format!(
                r#"
                function process(event)
                    event.log.{field} = "x"
                    return event
                end
                "#
            ));
            let err = process_err(&w, log_and_counter_event("hits", 1.0));
            assert!(err.contains("read-only"), "field {field}, got: {err}");
        }
    }

    #[test]
    fn an_unknown_field_on_event_dot_log_is_a_clear_error() {
        let w = worker(
            r#"
            function process(event)
                local _ = event.log.nonexistent
                event.log.nonexistent = 1
                return event
            end
            "#,
        );
        let err = process_err(&w, log_and_counter_event("hits", 1.0));
        assert!(err.contains("no field"), "got: {err}");
    }

    #[test]
    fn assigning_to_event_dot_log_itself_reports_it_as_read_only() {
        let w = worker(
            r#"
            function process(event)
                event.log = nil
                return event
            end
            "#,
        );
        let err = process_err(&w, log_and_counter_event("hits", 1.0));
        assert!(err.contains("read-only"), "got: {err}");
    }

    #[test]
    fn to_table_includes_a_log_sub_table_matching_the_proxy() {
        let w = worker(
            r#"
            function process(event)
                event.log.trace_id = "ab000000000000000000000000000000"
                local t = event:to_table()
                event.attributes.message = t.log.message
                event.attributes.trace_id = t.log.trace_id
                return event
            end
            "#,
        );
        let out = emitted(w.process(log_and_counter_event("hits", 1.0)).unwrap());
        assert_eq!(out.attributes.get("message").and_then(|v| v.as_str()), Some("GET /"));
        assert_eq!(
            out.attributes.get("trace_id").and_then(|v| v.as_str()),
            Some("ab000000000000000000000000000000")
        );
    }

    #[test]
    fn to_table_dot_log_is_nil_when_the_event_has_no_log() {
        let w = worker(
            r#"
            function process(event)
                local t = event:to_table()
                event.attributes.log_is_nil = (t.log == nil)
                return event
            end
            "#,
        );
        let out = emitted(w.process(counter_event("hits", 1.0)).unwrap());
        assert!(matches!(out.attributes.get("log_is_nil"), Some(logit_core::Value::Bool(true))));
    }

    /// `event.type` doesn't exist (`docs/adr/multi-payload-events.md`), and reads as `nil`, not
    /// an error, like any unknown key.
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
                assert_eq!(
                    events[0].0.attributes.get("variant").and_then(|v| v.as_str()),
                    Some("a")
                );
                assert_eq!(
                    events[1].0.attributes.get("variant").and_then(|v| v.as_str()),
                    Some("b")
                );
            }
            _ => panic!("expected EmitMany"),
        }
    }

    #[test]
    fn non_sequence_table_return_is_a_clear_error_not_a_silent_empty_emit() {
        // `Table::sequence_values` would stop at the missing key 1 and emit nothing.
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
        let err = match w.flush(0) {
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
        // `pending = event` aliases the userdata, and returning `event` takes the shared box, so
        // using `pending` in `flush()` must fail with this crate's error, not mlua's
        // "UserDataDestructed".
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
        let err = match w.flush(0) {
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
        // As above for a stashed `event.attributes`: `into_inner` destructs the `AttrsProxy`
        // when `event` is returned, and the later read must get this crate's wording, not mlua's
        // "a destructed callback or destructed userdata method was called".
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
        let err = match w.flush(0) {
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
        // The documented workaround: stash a clone, not the live alias.
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
        assert_eq!(w.flush(0).unwrap().len(), 1);
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

    /// `unwrap_err` for `ScriptWorker::new`, since `ScriptWorker` isn't `Debug`.
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

    /// `flush = 5` is a load error, not a silent "no flush".
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

    /// Asserts a base-library global `remove_unsandboxed_base_globals` removes is `nil`; one test
    /// per global, so each fails on its own.
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

    /// The `Event` global, with its `new` constructor (`crate::construct`), is visible in
    /// `process()`.
    #[test]
    fn event_global_is_installed() {
        let w = worker(
            r#"
            function process(event)
                event.attributes.present = (Event ~= nil)
                event.attributes.callable = (type(Event.new) == "function")
                return event
            end
            "#,
        );
        let out = emitted(w.process(counter_event("hits", 1.0)).unwrap());
        assert!(matches!(out.attributes.get("present"), Some(logit_core::Value::Bool(true))));
        assert!(matches!(out.attributes.get("callable"), Some(logit_core::Value::Bool(true))));
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

        let flushed = w.flush(0).unwrap();
        assert_eq!(flushed.len(), 1);

        assert_eq!(w.flush(0).unwrap().len(), 0);
    }

    #[test]
    fn flush_is_a_noop_when_the_script_defines_none() {
        let w = worker("function process(event) return event end");
        assert_eq!(w.flush(0).unwrap().len(), 0);
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

    /// `trace` reads all-zero until `set_trace_context`, then the given ids.
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

    /// `trace` is installed before top-level code runs, so a top-level alias isn't `nil`
    /// (`crate::trace`'s module doc).
    #[test]
    fn a_top_level_alias_of_trace_sees_a_real_table_not_nil() {
        let w = worker(
            r#"
            local incoming_trace = trace

            function process(event)
                event.attributes.trace_id = incoming_trace.trace_id
                return event
            end
            "#,
        );

        let out = emitted(w.process(counter_event("hits", 1.0)).unwrap());
        assert_eq!(
            out.attributes.get("trace_id").and_then(|v| v.as_str()),
            Some(&"0".repeat(32)[..]),
            "a top-level alias of `trace` should see the installed table, not nil"
        );
    }

    /// `scope` is installed for every worker and reads `""` for `name` before any `set_scope`.
    #[test]
    fn scope_name_is_readable_in_process() {
        let w = worker(
            r#"
            function process(event)
                event.attributes.scope_name = scope.name
                return event
            end
            "#,
        );
        let out = emitted(w.process(counter_event("hits", 1.0)).unwrap());
        assert_eq!(out.attributes.get("scope_name").and_then(|v| v.as_str()), Some(""));
    }

    /// Writes to `scope.version` and `resource.schema_url` in one `process()` both come back
    /// through `take_scope`/`take_resource`.
    #[test]
    fn writing_scope_version_and_resource_schema_url_commits_through_take() {
        let w = worker(
            r#"
            function process(event)
                scope.version = "2.0.0"
                resource.schema_url = "https://example.com/schema"
                return event
            end
            "#,
        );
        w.process(counter_event("hits", 1.0)).unwrap();

        let scope = w.take_scope().expect("a scope write must report Some");
        assert_eq!(scope.version.as_ref(), b"2.0.0");

        let resource = w.take_resource().expect("a resource write must report Some");
        assert_eq!(
            resource.schema_url,
            Some(bytes::Bytes::from_static(b"https://example.com/schema"))
        );
    }

    // ---------------------------------------------------------------------------------------
    // Routing to a target (`docs/adr/target-components.md`, `docs/design/lua-api.md`)
    // ---------------------------------------------------------------------------------------

    /// A worker with `targets:`, slot order being declaration order, as `run_lua` builds one.
    fn routing_worker(source: &str, targets: &[&str]) -> ScriptWorker {
        let targets: Vec<String> = targets.iter().map(|t| (*t).to_string()).collect();
        worker(source).with_targets(&targets)
    }

    /// As `emitted`, keeping the routing mark.
    fn emitted_with_mark(outcome: ProcessOutcome) -> (Event, Option<u16>) {
        match outcome {
            ProcessOutcome::Emit(e, target) => (*e, target),
            _ => panic!("expected Emit"),
        }
    }

    #[test]
    fn to_marks_the_returned_event() {
        let w = routing_worker(
            r#"
            function process(event)
                return event:to("b")
            end
            "#,
            &["a", "b"],
        );
        let (event, mark) = emitted_with_mark(w.process(counter_event("hits", 1.0)).unwrap());
        // A slot, not the id: nothing on this path compares a string.
        assert_eq!(mark, Some(1));
        assert_eq!(event.metrics.len(), 1);
    }

    #[test]
    fn to_nil_clears_the_mark() {
        let w = routing_worker(
            r#"
            function process(event)
                event:to("a")
                event:to(nil)
                return event
            end
            "#,
            &["a"],
        );
        let (_, mark) = emitted_with_mark(w.process(counter_event("hits", 1.0)).unwrap());
        assert_eq!(mark, None, "to(nil) must send the event back to the ordinary consumers");
    }

    /// An unknown id is a script error naming the configured list, since the likeliest cause is a
    /// typo (`docs/adr/target-components.md`).
    #[test]
    fn to_an_unknown_target_is_a_script_error_naming_the_configured_targets() {
        let w = routing_worker(
            r#"
            function process(event)
                return event:to("nope")
            end
            "#,
            &["host_stream", "app_stream"],
        );
        let err = process_err(&w, counter_event("hits", 1.0));
        assert!(err.contains(r#"no target named "nope""#), "got: {err}");
        assert!(
            err.contains("this component's targets are [host_stream, app_stream]"),
            "the error must name the configured targets, got: {err}"
        );
    }

    /// With no `targets:`, the error says so rather than printing an empty list.
    #[test]
    fn to_on_a_component_with_no_targets_says_so() {
        let w = worker(
            r#"
            function process(event)
                return event:to("a")
            end
            "#,
        );
        let err = process_err(&w, counter_event("hits", 1.0));
        assert!(err.contains("this component declares no targets"), "got: {err}");
    }

    /// `to` returns the same handle, not a copy, so `return event:to("x")` works.
    #[test]
    fn to_returns_the_handle_for_chaining() {
        let w = routing_worker(
            r#"
            function process(event)
                local same = event:to("a")
                same.attributes.marked = "yes"
                return same
            end
            "#,
            &["a"],
        );
        let (event, mark) = emitted_with_mark(w.process(counter_event("hits", 1.0)).unwrap());
        assert_eq!(mark, Some(0));
        assert_eq!(event.attributes.get("marked").and_then(|v| v.as_str()), Some("yes"));
    }

    /// `clone` copies the mark and shares the target table, so the copy can be re-routed alone
    /// (`docs/adr/target-components.md`).
    #[test]
    fn clone_copies_the_mark() {
        let w = routing_worker(
            r#"
            function process(event)
                event:to("b")
                local copy = event:clone()
                return {event, copy}
            end
            "#,
            &["a", "b"],
        );
        match w.process(counter_event("hits", 1.0)).unwrap() {
            ProcessOutcome::EmitMany(events) => {
                assert_eq!(events.len(), 2);
                assert_eq!(events[0].1, Some(1));
                assert_eq!(events[1].1, Some(1), "the clone should inherit its source's mark");
            }
            _ => panic!("expected EmitMany"),
        }
    }

    /// The mark rides on the handle, so one `return {a, b}` can fork two ways.
    #[test]
    fn a_table_return_carries_independent_marks() {
        let w = routing_worker(
            r#"
            function process(event)
                local b = event:clone():to(nil)
                return {event:to("x"), b}
            end
            "#,
            &["x", "y"],
        );
        match w.process(counter_event("hits", 1.0)).unwrap() {
            ProcessOutcome::EmitMany(events) => {
                assert_eq!(events.len(), 2);
                assert_eq!(events[0].1, Some(0), "the first event was marked for x");
                assert_eq!(events[1].1, None, "the second was cleared and stays unrouted");
            }
            _ => panic!("expected EmitMany"),
        }
    }

    /// A clone stashed in `process()` and routed in `flush()` keeps its mark, since it shares
    /// its source's target table.
    #[test]
    fn flush_output_carries_marks() {
        let w = routing_worker(
            r#"
            local pending = nil
            function process(event)
                pending = event:clone()
                return nil
            end
            function flush()
                if pending then
                    local e = pending
                    pending = nil
                    return {e:to("a")}
                end
                return {}
            end
            "#,
            &["a", "b"],
        );
        assert!(matches!(w.process(counter_event("hits", 1.0)).unwrap(), ProcessOutcome::Drop));

        let flushed = w.flush(0).unwrap();
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].1, Some(0), "a flushed event carries the mark it was given");
    }
}
