//! The `Event` <-> Lua boundary. Events reach a script as userdata proxies, not converted tables,
//! so a stage pays only for the fields its script touches; `Event.new(t)` (`crate::construct`)
//! and `event:to_table()` are the opt-in full-table paths (`docs/design/memory.md` §2). See
//! `docs/design/lua-api.md` for the script-visible contract.
//!
//! - [`EventProxy`]: the whole event.
//! - `AttrsProxy`: `event.attributes`, an open map.
//! - `LogProxy`: `event.log`, a fixed field set, mostly read-write.
//! - `MetricsProxy`/`MetricProxy`: `event.metrics`, a 1-based array of records. Writable fields
//!   are `name`, `unit`, `description`, `start_timestamp`, `value` on a `sum`/`gauge`, and a
//!   `sum`'s `temporality`/`monotonic`.
//! - `SpanProxy`: `event.span`, read-only. A script makes a span with `Event.new`.
//!
//! Every sub-proxy shares its parent's `Rc<RefCell<Event>>`, so a write through one is visible
//! through all, matching Lua's reference semantics (`local e2 = event` aliases the event).

use crate::value::{
    attribute_error, attrmap_to_lua_table, exact_u64_to_lua, lua_to_value, lua_value_matches,
    value_to_lua,
};
use logit_core::interner::{intern, resolve};
use logit_core::trace::{parse_span_id, parse_trace_id, to_hex};
use logit_core::{
    Event, Exemplar, LogRecord, MetricKind, MetricRecord, SpanEvent, SpanLink, SpanRecord,
    Temporality, TraceRef,
};
use mlua::{
    AnyUserData, Lua, MetaMethod, RegistryKey, Table, UserData, UserDataMethods, Value as LuaValue,
};
use std::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};

/// The `target` ids one `lua`/`lua_file` component may direct events into, as a name-to-slot
/// lookup, built once per worker ([`crate::ScriptWorker::with_targets`]) and shared by every
/// [`EventProxy`] it creates.
///
/// A linear scan, not a `HashMap`: a `targets:` list is a handful of ids, and neither allocates
/// on lookup (`crates/logit-bench/tests/allocations.rs`). The slot is the index in
/// `logit_pipeline::graph::targets_of`'s order (`docs/adr/target-components.md`), so `Some(n)`
/// here and `logit_pipeline::Destination::To(n)` name the same target.
#[derive(Default)]
pub(crate) struct TargetTable {
    names: Vec<Box<str>>,
}

impl TargetTable {
    pub(crate) fn new(names: &[String]) -> Self {
        Self { names: names.iter().map(|name| name.as_str().into()).collect() }
    }

    /// The shared empty table, for a component with no `targets:`; a refcount bump, not an
    /// allocation. See [`NO_TARGETS`].
    pub(crate) fn empty() -> Rc<Self> {
        no_targets()
    }

    /// The slot `name` occupies, or `None`, which `event:to(..)` turns into a script error, never
    /// a silent forward (`docs/adr/target-components.md`).
    pub(crate) fn slot(&self, name: &str) -> Option<u16> {
        self.names.iter().position(|candidate| &**candidate == name).map(|slot| slot as u16)
    }

    /// The configured ids, in slot order, for `event:to(..)`'s error message.
    pub(crate) fn names(&self) -> &[Box<str>] {
        &self.names
    }

    /// `event:to("x")`'s error text for an id this component doesn't declare.
    fn unknown_target_message(&self, name: &str) -> String {
        match self.names().is_empty() {
            true => format!(
                "event:to({name:?}): no target named {name:?} -- this component declares no targets"
            ),
            false => format!(
                "event:to({name:?}): no target named {name:?} -- this component's targets are [{}]",
                self.names().join(", ")
            ),
        }
    }
}

thread_local! {
    /// The empty [`TargetTable`] every untargeted proxy shares. A component with no `targets:` is
    /// the common case, and a fresh `Rc` per event would add an allocation to every `process()`
    /// call (`crates/logit-bench/tests/allocations.rs`). A `thread_local` because `Rc` is
    /// `!Send`, as is every `ScriptWorker` that reaches it.
    static NO_TARGETS: Rc<TargetTable> = Rc::new(TargetTable::default());
}

/// A shared handle to the empty [`TargetTable`].
fn no_targets() -> Rc<TargetTable> {
    NO_TARGETS.with(Rc::clone)
}

/// Wraps one [`Event`] for a `process()`/`flush()` call, or longer if a script stashes it.
///
/// **An event handle, and every sub-handle from it, is consumed once the event is returned from
/// `process()` or included in a `flush()` table.** A stashed alias is the same Lua box as the
/// returned value, so extracting one invalidates both ([`take_event`]); the cached sub-proxies
/// are torn down with it ([`EventProxy::into_inner`]). Later use fails with this crate's wording
/// ([`clarify_destructed_handle_use`]). A script that needs to emit now and keep the event for
/// `flush()` stashes `event:clone()`.
pub struct EventProxy {
    event: Rc<RefCell<Event>>,
    /// The `event.attributes` sub-proxy, created on first access and cached, so repeated access
    /// costs no `create_userdata` allocation (`docs/design/memory.md` §8).
    ///
    /// A `RegistryKey` because `AnyUserData<'lua>` borrows the `Lua` and a `UserData` type must be
    /// `'static`. The registry is a GC root, so a cached entry would outlive this proxy unless
    /// removed: [`into_inner`](EventProxy::into_inner) removes it when the event is returned,
    /// which also keeps its `Rc::try_unwrap` fast path working. An event dropped without being
    /// returned leaves the entry to `RegistryKey::drop`, which queues the slot for reuse; forcing
    /// cleanup there could invalidate an event a script stashed for `flush()`.
    attrs: RefCell<Option<RegistryKey>>,
    /// `event.log`'s sub-proxy, cached as `attrs` is. Only populated when `event.log.is_some()`;
    /// no script can remove a log, so a cached proxy always has one.
    log: RefCell<Option<RegistryKey>>,
    /// `event.metrics`'s sub-proxy ([`MetricsProxy`]), cached as `attrs` is, even for an empty
    /// list (`#event.metrics == 0`).
    metrics: RefCell<Option<RegistryKey>>,
    /// `event.span`'s sub-proxy ([`SpanProxy`]), cached and gated as `log` is.
    span: RefCell<Option<RegistryKey>>,
    /// Where `event:to(id)` said this event goes, a slot in [`TargetTable`] order; `None` if
    /// unrouted.
    ///
    /// **The mark rides on the handle, not on the `Event`.** `size_of::<Event>()` is pinned
    /// (`crates/logit-core/tests/type_sizes.rs`), and a routing decision belongs to this call's
    /// answer, not to the event (`docs/adr/target-components.md`'s "Lua" consequence). It leaves
    /// with the event through [`take_event`]. A `Cell` because `event:to(..)` only gets `&self`.
    target: Cell<Option<u16>>,
    /// This component's `targets:`, shared with every proxy the worker creates; [`no_targets`]
    /// for a component that declares none.
    targets: Rc<TargetTable>,
}

impl EventProxy {
    /// A proxy with no `targets:`, on which `event:to(..)` is always a script error.
    pub fn new(event: Event) -> Self {
        Self::with_targets(event, no_targets())
    }

    /// A proxy whose `event:to(id)` resolves `id` against `targets`.
    pub(crate) fn with_targets(event: Event, targets: Rc<TargetTable>) -> Self {
        Self {
            event: Rc::new(RefCell::new(event)),
            attrs: RefCell::new(None),
            log: RefCell::new(None),
            metrics: RefCell::new(None),
            span: RefCell::new(None),
            target: Cell::new(None),
            targets,
        }
    }

    /// `event:clone()`: a fresh `Event` with the same routing table and a copy of the mark, so a
    /// fanned-out copy starts headed the same way (`docs/adr/target-components.md`).
    fn cloned_from(source: &Self) -> Self {
        let clone = Self::with_targets(source.event.borrow().clone(), source.targets.clone());
        clone.target.set(source.target.get());
        clone
    }

    /// This event's `AttrsProxy` userdata, created and cached on first call.
    fn attrs_userdata<'lua>(&self, lua: &'lua Lua) -> mlua::Result<AnyUserData<'lua>> {
        if let Some(key) = self.attrs.borrow().as_ref() {
            return lua.registry_value(key);
        }
        let ud = lua.create_userdata(AttrsProxy(self.event.clone()))?;
        *self.attrs.borrow_mut() = Some(lua.create_registry_value(ud.clone())?);
        Ok(ud)
    }

    /// As [`Self::attrs_userdata`], for `event.log`. Only call when `event.log.is_some()`.
    fn log_userdata<'lua>(&self, lua: &'lua Lua) -> mlua::Result<AnyUserData<'lua>> {
        if let Some(key) = self.log.borrow().as_ref() {
            return lua.registry_value(key);
        }
        let ud = lua.create_userdata(LogProxy(self.event.clone()))?;
        *self.log.borrow_mut() = Some(lua.create_registry_value(ud.clone())?);
        Ok(ud)
    }

    /// As [`Self::attrs_userdata`], for `event.metrics`, including an empty list.
    fn metrics_userdata<'lua>(&self, lua: &'lua Lua) -> mlua::Result<AnyUserData<'lua>> {
        if let Some(key) = self.metrics.borrow().as_ref() {
            return lua.registry_value(key);
        }
        let ud = lua.create_userdata(MetricsProxy(self.event.clone()))?;
        *self.metrics.borrow_mut() = Some(lua.create_registry_value(ud.clone())?);
        Ok(ud)
    }

    /// As [`Self::attrs_userdata`], for `event.span`. Only call when `event.span.is_some()`.
    fn span_userdata<'lua>(&self, lua: &'lua Lua) -> mlua::Result<AnyUserData<'lua>> {
        if let Some(key) = self.span.borrow().as_ref() {
            return lua.registry_value(key);
        }
        let ud = lua.create_userdata(SpanProxy(self.event.clone()))?;
        *self.span.borrow_mut() = Some(lua.create_registry_value(ud.clone())?);
        Ok(ud)
    }

    /// Unwraps back to the owned `Event` and its routing mark.
    ///
    /// No clone in the ordinary case, because [`take_event`]'s [`AnyUserData::take`] leaves this
    /// the only reference. It falls back to cloning if something else still holds one, and never
    /// panics.
    ///
    /// The cached sub-proxies are emptied with `take` and removed from the registry first, before
    /// `Rc::try_unwrap`: each holds an `Rc` to the event, and waiting for the GC would make nearly
    /// every script pay the clone.
    pub fn into_inner(self, lua: &Lua) -> (Event, Option<u16>) {
        if let Some(key) = self.attrs.into_inner() {
            if let Ok(ud) = lua.registry_value::<AnyUserData>(&key) {
                let _ = ud.take::<AttrsProxy>();
            }
            let _ = lua.remove_registry_value(key);
        }
        if let Some(key) = self.log.into_inner() {
            if let Ok(ud) = lua.registry_value::<AnyUserData>(&key) {
                let _ = ud.take::<LogProxy>();
            }
            let _ = lua.remove_registry_value(key);
        }
        if let Some(key) = self.metrics.into_inner() {
            if let Ok(ud) = lua.registry_value::<AnyUserData>(&key) {
                let _ = ud.take::<MetricsProxy>();
            }
            let _ = lua.remove_registry_value(key);
        }
        if let Some(key) = self.span.into_inner() {
            if let Ok(ud) = lua.registry_value::<AnyUserData>(&key) {
                let _ = ud.take::<SpanProxy>();
            }
            let _ = lua.remove_registry_value(key);
        }
        let event = match Rc::try_unwrap(self.event) {
            Ok(cell) => cell.into_inner(),
            Err(rc) => rc.borrow().clone(),
        };
        (event, self.target.get())
    }

    /// The event's strong count, which `into_inner`'s `Rc::try_unwrap` needs to be 1; an
    /// uncollected `MetricProxy` must not raise it.
    #[cfg(test)]
    fn strong_count(&self) -> usize {
        Rc::strong_count(&self.event)
    }
}

impl UserData for EventProxy {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_meta_method(MetaMethod::Index, |lua, this, key: mlua::String| {
            match key.to_str()? {
                // A decimal-digit string, not a Lua number: a double is exact only to 2^53, and a
                // unix-nanos timestamp (~1.7e18) read as a number loses precision even through an
                // unmodified read-then-write. A script can `tonumber()` it if it needs arithmetic.
                "timestamp" => Ok(LuaValue::String(
                    lua.create_string(this.event.borrow().timestamp.to_string())?,
                )),
                "attributes" => Ok(LuaValue::UserData(this.attrs_userdata(lua)?)),
                // `nil` when the event has no log, agreeing with `has_log`.
                "log" => match this.event.borrow().log.is_some() {
                    true => Ok(LuaValue::UserData(this.log_userdata(lua)?)),
                    false => Ok(LuaValue::Nil),
                },
                // Present even for an empty metric list.
                "metrics" => Ok(LuaValue::UserData(this.metrics_userdata(lua)?)),
                "span" => match this.event.borrow().span.is_some() {
                    true => Ok(LuaValue::UserData(this.span_userdata(lua)?)),
                    false => Ok(LuaValue::Nil),
                },
                // Presence flags, not an `event.type` label: an event can carry a log, metrics,
                // and a span at once (`docs/adr/multi-payload-events.md`), and a script branching
                // on one label would skip the metrics on a log event `kv_metrics` produced.
                "has_log" => Ok(LuaValue::Boolean(this.event.borrow().log.is_some())),
                "has_metrics" => Ok(LuaValue::Boolean(!this.event.borrow().metrics.is_empty())),
                "has_span" => Ok(LuaValue::Boolean(this.event.borrow().span.is_some())),
                _ => Ok(LuaValue::Nil),
            }
        });

        methods.add_meta_method(
            MetaMethod::NewIndex,
            |_, this, (key, value): (mlua::String, LuaValue)| {
                let key = key.to_str()?;
                match key {
                    "timestamp" => {
                        // A string only, as `__index` returns: a Lua number may already have lost
                        // precision.
                        let LuaValue::String(s) = value else {
                            return Err(mlua::Error::RuntimeError(format!(
                                "event.timestamp must be a string of decimal digits (a Lua number \
                                 can't represent full nanosecond precision), got {}",
                                value.type_name()
                            )));
                        };
                        let ts: i64 = s.to_str()?.parse().map_err(|_| {
                            mlua::Error::RuntimeError(
                                "event.timestamp must be a string of decimal digits".to_string(),
                            )
                        })?;
                        this.event.borrow_mut().timestamp = ts;
                        Ok(())
                    }
                    "attributes" | "log" | "metrics" | "span" | "has_log" | "has_metrics"
                    | "has_span" => {
                        Err(mlua::Error::RuntimeError(format!("event.{key} is read-only")))
                    }
                    other => {
                        Err(mlua::Error::RuntimeError(format!("event has no field '{other}'")))
                    }
                }
            },
        );

        // An independent deep copy, for fan-out (`return {a, b}`); see `cloned_from` for the
        // mark.
        methods.add_method("clone", |_, this, ()| Ok(EventProxy::cloned_from(this)));

        // `event:to(id)` marks this event for one of the component's `targets:` and returns the
        // same handle, so `return event:to("host_stream")` chains
        // (`docs/adr/target-components.md`, `docs/design/lua-api.md`'s "Routing to a target").
        // It doesn't emit: the event must still be returned.
        //
        // `add_function`, not `add_method`, so the same userdata can be returned; `add_method`
        // only sees `&EventProxy`. A colon call passes the handle first either way, so scripts
        // see no difference. A destructed handle never reaches this closure: looking up
        // `to` on it raises, and `clarify_destructed_handle_use` rewords that.
        methods.add_function("to", |_, (this, id): (AnyUserData, LuaValue)| {
            {
                let proxy = this.borrow::<EventProxy>()?;
                match id {
                    // Clears the mark: the event goes to the component's ordinary consumers.
                    LuaValue::Nil => proxy.target.set(None),
                    LuaValue::String(name) => match proxy.targets.slot(name.to_str()?) {
                        Some(slot) => proxy.target.set(Some(slot)),
                        // A script error, never a silent forward, naming the configured list.
                        None => {
                            return Err(mlua::Error::RuntimeError(
                                proxy.targets.unknown_target_message(name.to_str()?),
                            ))
                        }
                    },
                    other => {
                        return Err(mlua::Error::RuntimeError(format!(
                            "event:to(id) takes a target id string, or nil to clear the mark, got \
                             {}",
                            other.type_name()
                        )))
                    }
                }
            }
            Ok(this)
        });

        // A plain Lua table, detached from the live event: how a script iterates attributes,
        // since LuaJIT has no `__pairs`. Covers every payload field, and `Event.new` is its
        // inverse (`crate::construct`).
        methods.add_method("to_table", |lua, this, ()| {
            let event = this.event.borrow();
            let table = lua.create_table()?;
            table.set("timestamp", event.timestamp.to_string())?; // string -- see __index above
            table.set("attributes", attrmap_to_lua_table(lua, &event.attributes)?)?;
            table.set(
                "log",
                match &event.log {
                    Some(log) => LuaValue::Table(log_to_table(lua, log)?),
                    None => LuaValue::Nil,
                },
            )?;
            table.set("has_log", event.log.is_some())?;
            table.set("has_metrics", !event.metrics.is_empty())?;
            table.set("has_span", event.span.is_some())?;
            let metrics_table = lua.create_table()?;
            for (i, record) in event.metrics.iter().enumerate() {
                metrics_table.set(i + 1, metric_to_table(lua, record)?)?;
            }
            table.set("metrics", metrics_table)?;
            table.set(
                "span",
                match &event.span {
                    Some(span) => LuaValue::Table(span_to_table(lua, span)?),
                    None => LuaValue::Nil,
                },
            )?;
            Ok(table)
        });
    }
}

/// The `event.attributes` sub-proxy, an open map over its parent's event.
struct AttrsProxy(Rc<RefCell<Event>>);

impl UserData for AttrsProxy {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_meta_method(MetaMethod::Index, |lua, this, key: mlua::String| {
            match this.0.borrow().attributes.get(key.to_str()?) {
                Some(value) => value_to_lua(lua, value),
                None => Ok(LuaValue::Nil),
            }
        });

        methods.add_meta_method(
            MetaMethod::NewIndex,
            |_, this, (key, value): (mlua::String, LuaValue)| {
                let key = key.to_str()?;
                // If `value` is what `value_to_lua` gave for the current content, keep the stored
                // `Value`, so its variant (`Bytes` vs. `Str`, `U64` vs. `I64`) survives
                // `event.attributes.x = event.attributes.x` (`lua_value_matches`). Conversion
                // reads the table raw and runs no metamethod, so nothing re-enters this proxy
                // during it; the borrow is still released first, the cheap ordering.
                let is_noop = this
                    .0
                    .borrow()
                    .attributes
                    .get(key)
                    .is_some_and(|existing| lua_value_matches(existing, &value));
                if is_noop {
                    return Ok(());
                }
                let value = lua_to_value(value)
                    .map_err(|err| attribute_error("event.attributes", key, err))?;
                this.0.borrow_mut().attributes.insert(key, value);
                Ok(())
            },
        );

        // No `__pairs`: mlua's `MetaMethod::Pairs` needs Lua 5.2+, not LuaJIT. A script
        // enumerates `event:to_table().attributes` instead.
    }
}

/// The `event.log` sub-proxy: a fixed, typed field set over its parent's event.
///
/// `trace_id`/`span_id`/`trace_flags` (`docs/adr/log-record-trace-context.md`), `event_name`, and
/// `observed_timestamp` are writable; `message`/`severity`/`body_format` are read-only pending a
/// design (`docs/design/lua-api.md`), and `dropped_attributes_count` is read-only. Construct only
/// when `event.log.is_some()`: every accessor `.expect`s it.
struct LogProxy(Rc<RefCell<Event>>);

impl LogProxy {
    fn with_log<R>(&self, f: impl FnOnce(&LogRecord) -> R) -> R {
        let event = self.0.borrow();
        f(event.log.as_ref().expect("LogProxy is only ever created when event.log.is_some()"))
    }

    fn with_log_mut<R>(&self, f: impl FnOnce(&mut LogRecord) -> R) -> R {
        let mut event = self.0.borrow_mut();
        f(event.log.as_mut().expect("LogProxy is only ever created when event.log.is_some()"))
    }
}

impl UserData for LogProxy {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_meta_method(MetaMethod::Index, |lua, this, key: mlua::String| {
            this.with_log(|log| match key.to_str()? {
                "trace_id" => match log.trace {
                    Some(t) => Ok(LuaValue::String(lua.create_string(to_hex(&t.trace_id))?)),
                    None => Ok(LuaValue::Nil),
                },
                "span_id" => match log.trace.and_then(|t| t.span_id) {
                    Some(id) => Ok(LuaValue::String(lua.create_string(to_hex(&id))?)),
                    None => Ok(LuaValue::Nil),
                },
                // `nil`, not `0`, without a trace, so it agrees with `trace_id == nil`.
                "trace_flags" => match log.trace {
                    Some(t) => Ok(LuaValue::Integer(t.flags as i64)),
                    None => Ok(LuaValue::Nil),
                },
                "message" => value_to_lua(lua, &log.message),
                "severity" => match log.severity {
                    Some(s) => Ok(LuaValue::String(lua.create_string(s.as_str())?)),
                    None => Ok(LuaValue::Nil),
                },
                "body_format" => Ok(LuaValue::String(lua.create_string(log.body_format.as_str())?)),
                // OTLP's `LogRecord.event_name`, an interned `Symbol`.
                "event_name" => match log.event_name {
                    Some(sym) => Ok(LuaValue::String(lua.create_string(resolve(sym))?)),
                    None => Ok(LuaValue::Nil),
                },
                // A decimal-digit string, as `event.timestamp` is. Unset reads as `"0"`, not
                // `nil`.
                "observed_timestamp" => {
                    Ok(LuaValue::String(lua.create_string(log.observed_timestamp.to_string())?))
                }
                "dropped_attributes_count" => {
                    Ok(LuaValue::Integer(log.dropped_attributes_count as i64))
                }
                _ => Ok(LuaValue::Nil),
            })
        });

        methods.add_meta_method(
            MetaMethod::NewIndex,
            |_, this, (key, value): (mlua::String, LuaValue)| {
                let key = key.to_str()?;
                match key {
                    "trace_id" => this.with_log_mut(|log| match value {
                        LuaValue::Nil => {
                            log.trace = None; // clears the whole TraceRef, span/flags included
                            Ok(())
                        }
                        LuaValue::String(s) => {
                            let trace_id = parse_trace_id(s.to_str()?).ok_or_else(|| {
                                mlua::Error::RuntimeError(
                                    "event.log.trace_id must be a 32-character hex string \
                                     (or nil), and not all-zero"
                                        .to_string(),
                                )
                            })?;
                            // A new trace_id replaces the whole TraceRef: the old span_id and
                            // flags belong to the old trace.
                            log.trace = Some(TraceRef { trace_id, span_id: None, flags: 0 });
                            Ok(())
                        }
                        other => Err(mlua::Error::RuntimeError(format!(
                            "event.log.trace_id must be a hex string or nil, got {}",
                            other.type_name()
                        ))),
                    }),
                    "span_id" => this.with_log_mut(|log| {
                        let Some(trace) = &mut log.trace else {
                            return Err(mlua::Error::RuntimeError(
                                "event.log.span_id can't be set without a trace_id -- set \
                                 event.log.trace_id first"
                                    .to_string(),
                            ));
                        };
                        match value {
                            LuaValue::Nil => {
                                trace.span_id = None;
                                Ok(())
                            }
                            LuaValue::String(s) => {
                                trace.span_id =
                                    Some(parse_span_id(s.to_str()?).ok_or_else(|| {
                                        mlua::Error::RuntimeError(
                                            "event.log.span_id must be a 16-character hex string \
                                         (or nil), and not all-zero"
                                                .to_string(),
                                        )
                                    })?);
                                Ok(())
                            }
                            other => Err(mlua::Error::RuntimeError(format!(
                                "event.log.span_id must be a hex string or nil, got {}",
                                other.type_name()
                            ))),
                        }
                    }),
                    "trace_flags" => this.with_log_mut(|log| {
                        let Some(trace) = &mut log.trace else {
                            return Err(mlua::Error::RuntimeError(
                                "event.log.trace_flags can't be set without a trace_id -- set \
                                 event.log.trace_id first"
                                    .to_string(),
                            ));
                        };
                        let LuaValue::Integer(n) = value else {
                            return Err(mlua::Error::RuntimeError(format!(
                                "event.log.trace_flags must be an integer 0-255, got {}",
                                value.type_name()
                            )));
                        };
                        let flags = u8::try_from(n).map_err(|_| {
                            mlua::Error::RuntimeError(format!(
                                "event.log.trace_flags must be an integer 0-255, got {n}"
                            ))
                        })?;
                        trace.flags = flags;
                        Ok(())
                    }),
                    "message" | "severity" | "body_format" => Err(mlua::Error::RuntimeError(
                        format!("event.log.{key} is read-only for now"),
                    )),
                    "event_name" => this.with_log_mut(|log| match value {
                        LuaValue::Nil => {
                            log.event_name = None;
                            Ok(())
                        }
                        LuaValue::String(s) => {
                            log.event_name = Some(intern(s.to_str()?));
                            Ok(())
                        }
                        other => Err(mlua::Error::RuntimeError(format!(
                            "event.log.event_name must be a string or nil, got {}",
                            other.type_name()
                        ))),
                    }),
                    "observed_timestamp" => this.with_log_mut(|log| {
                        let LuaValue::String(s) = value else {
                            return Err(mlua::Error::RuntimeError(format!(
                                "event.log.observed_timestamp must be a string of decimal digits \
                                 (a Lua number can't represent full nanosecond precision), got {}",
                                value.type_name()
                            )));
                        };
                        let ts: i64 = s.to_str()?.parse().map_err(|_| {
                            mlua::Error::RuntimeError(
                                "event.log.observed_timestamp must be a string of decimal digits"
                                    .to_string(),
                            )
                        })?;
                        log.observed_timestamp = ts;
                        Ok(())
                    }),
                    "dropped_attributes_count" => Err(mlua::Error::RuntimeError(
                        "event.log.dropped_attributes_count is read-only".to_string(),
                    )),
                    other => {
                        Err(mlua::Error::RuntimeError(format!("event.log has no field '{other}'")))
                    }
                }
            },
        );

        methods.add_method("to_table", |lua, this, ()| this.with_log(|log| log_to_table(lua, log)));
    }
}

/// A log record as a plain table, for `LogProxy::to_table` and `EventProxy::to_table`'s `log`.
fn log_to_table<'lua>(lua: &'lua Lua, log: &LogRecord) -> mlua::Result<Table<'lua>> {
    let table = lua.create_table()?;
    table.set(
        "trace_id",
        match log.trace {
            Some(t) => LuaValue::String(lua.create_string(to_hex(&t.trace_id))?),
            None => LuaValue::Nil,
        },
    )?;
    table.set(
        "span_id",
        match log.trace.and_then(|t| t.span_id) {
            Some(id) => LuaValue::String(lua.create_string(to_hex(&id))?),
            None => LuaValue::Nil,
        },
    )?;
    table.set(
        "trace_flags",
        match log.trace {
            Some(t) => LuaValue::Integer(t.flags as i64),
            None => LuaValue::Nil,
        },
    )?;
    table.set("message", value_to_lua(lua, &log.message)?)?;
    table.set(
        "severity",
        match log.severity {
            Some(s) => LuaValue::String(lua.create_string(s.as_str())?),
            None => LuaValue::Nil,
        },
    )?;
    table.set("body_format", log.body_format.as_str())?;
    table.set(
        "event_name",
        match log.event_name {
            Some(sym) => LuaValue::String(lua.create_string(resolve(sym))?),
            None => LuaValue::Nil,
        },
    )?;
    table.set("observed_timestamp", log.observed_timestamp.to_string())?;
    table.set("dropped_attributes_count", log.dropped_attributes_count as i64)?;
    Ok(table)
}

// -- `event.metrics` -----------------------------------------------------------------------

/// The `event.metrics` sub-proxy: a 1-based array view over the event's
/// [`logit_core::MetricList`].
///
/// `#event.metrics` and `event.metrics[i]` are its whole surface: no `__newindex`, so a script
/// can't add, remove, or reorder metrics. Each `event.metrics[i]` creates a fresh [`MetricProxy`]
/// (3 allocations, measured: part of `crates/logit-bench/tests/allocations.rs`'s pinned 11 in
/// `lua_process_one_event_reading_metric_value`) rather than holding a registry slot per index
/// for the event's lifetime. A cache would only win for a script that re-indexes the same
/// metric; `local m = event.metrics[1]` is the documented idiom (`docs/design/memory.md` §8).
/// Not caching is safe only because a `MetricProxy` holds a [`Weak`]; see its doc.
struct MetricsProxy(Rc<RefCell<Event>>);

impl UserData for MetricsProxy {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_meta_method(MetaMethod::Len, |_, this, ()| {
            Ok(this.0.borrow().metrics.len() as i64)
        });

        methods.add_meta_method(MetaMethod::Index, |lua, this, key: LuaValue| {
            let index = match key {
                LuaValue::Integer(i) => i,
                // A computed index (`i + 0.0`) can arrive as an integral Number.
                LuaValue::Number(n) if n.fract() == 0.0 => n as i64,
                other => {
                    return Err(mlua::Error::RuntimeError(format!(
                        "event.metrics must be indexed by an integer, got {}",
                        other.type_name()
                    )))
                }
            };
            // Out of range, `<= 0` included, is `nil`, as for a Lua array; only a non-integer key
            // is an error.
            if index < 1 {
                return Ok(LuaValue::Nil);
            }
            let zero_based = (index - 1) as usize;
            if zero_based >= this.0.borrow().metrics.len() {
                return Ok(LuaValue::Nil);
            }
            Ok(LuaValue::UserData(lua.create_userdata(MetricProxy {
                event: Rc::downgrade(&this.0),
                index: zero_based,
            })?))
        });
    }
}

/// One `event.metrics[i]` entry, not cached (see [`MetricsProxy`]). `index` is 0-based; every
/// message adds 1 back to match the script's Lua index.
///
/// **Holds a [`Weak`], not an [`Rc`].** The cached sub-proxies are torn down in
/// [`EventProxy::into_inner`] before its `Rc::try_unwrap`; this one isn't, and the GC may not
/// have collected it by then. An `Rc` would make every script that reads a metric field pay a
/// full `Event` clone, and would let a stashed `local m = event.metrics[1]` silently mutate a
/// returned event's clone from `flush()`. A `Weak` doesn't count toward `try_unwrap`, and its
/// failed upgrade after the event is gone gives the same "already returned" error the other
/// handles give.
///
/// **Checks the index on every access**, though no script can shrink `event.metrics` today: a
/// future surface might, and tests construct one with a bad index directly.
struct MetricProxy {
    event: Weak<RefCell<Event>>,
    index: usize,
}

impl MetricProxy {
    /// Runs `f` on this proxy's metric. A failed upgrade means the event was returned; a missing
    /// index means the event lives but the metric doesn't. Each has its own error.
    fn with_metric<R>(&self, f: impl FnOnce(&MetricRecord) -> mlua::Result<R>) -> mlua::Result<R> {
        let event = self.event.upgrade().ok_or_else(|| metric_handle_consumed_error(self.index))?;
        let event = event.borrow();
        match event.metrics.get(self.index) {
            Some(record) => f(record),
            None => Err(stale_metric_error(self.index)),
        }
    }

    fn with_metric_mut<R>(
        &self,
        f: impl FnOnce(&mut MetricRecord) -> mlua::Result<R>,
    ) -> mlua::Result<R> {
        let event = self.event.upgrade().ok_or_else(|| metric_handle_consumed_error(self.index))?;
        let mut event = event.borrow_mut();
        match event.metrics.get_mut(self.index) {
            Some(record) => f(record),
            None => Err(stale_metric_error(self.index)),
        }
    }
}

/// The event behind this handle was returned: the "consumed handle" error
/// [`clarify_destructed_handle_use`] gives other stashed handles, found here by a failed
/// `Weak::upgrade` rather than mlua's destructed-userdata marker.
fn metric_handle_consumed_error(index: usize) -> mlua::Error {
    mlua::Error::RuntimeError(format!(
        "event.metrics[{}] belongs to an event that has already been returned from process() or \
         included in a flush() table -- stash event:clone() instead if you need to keep using it",
        index + 1
    ))
}

fn stale_metric_error(index: usize) -> mlua::Error {
    mlua::Error::RuntimeError(format!("event.metrics[{}] no longer exists", index + 1))
}

/// A write to a field the metric's current kind doesn't allow. The message names the kind,
/// because the same field (`value`, `temporality`) is writable on another kind
/// (`docs/design/lua-api.md`).
fn metric_ro_error(index: usize, field: &str, kind: &MetricKind) -> mlua::Error {
    mlua::Error::RuntimeError(format!(
        "event.metrics[{}].{field} is read-only on a {} metric",
        index + 1,
        kind.name()
    ))
}

/// An optional `f64` as a Lua number or `nil`.
fn opt_number<'lua>(v: Option<f64>) -> LuaValue<'lua> {
    match v {
        Some(n) => LuaValue::Number(n),
        None => LuaValue::Nil,
    }
}

/// A finite number for a `value` write. NaN and the infinities are errors rather than values a
/// sink or `aggregate` must defend against.
fn require_finite_number(value: LuaValue, field: &str) -> mlua::Result<f64> {
    let v = match value {
        LuaValue::Integer(i) => i as f64,
        LuaValue::Number(n) => n,
        other => {
            return Err(mlua::Error::RuntimeError(format!(
                "{field} must be a number, got {}",
                other.type_name()
            )))
        }
    };
    if !v.is_finite() {
        return Err(mlua::Error::RuntimeError(format!("{field} must be a finite number, got {v}")));
    }
    Ok(v)
}

fn parse_temporality(value: &LuaValue, index: usize) -> mlua::Result<Temporality> {
    match value {
        LuaValue::String(s) => {
            let name = s.to_str()?;
            Temporality::from_name(name).ok_or_else(|| {
                mlua::Error::RuntimeError(format!(
                    "event.metrics[{}].temporality must be \"delta\" or \"cumulative\", got \"{name}\"",
                    index + 1
                ))
            })
        }
        other => Err(mlua::Error::RuntimeError(format!(
            "event.metrics[{}].temporality must be a string, got {}",
            index + 1,
            other.type_name()
        ))),
    }
}

fn exemplar_to_table<'lua>(lua: &'lua Lua, exemplar: &Exemplar) -> mlua::Result<Table<'lua>> {
    let table = lua.create_table()?;
    table.set("timestamp", exemplar.timestamp.to_string())?;
    table.set("value", exemplar.value)?;
    table.set(
        "trace_id",
        match exemplar.trace {
            Some(t) => LuaValue::String(lua.create_string(to_hex(&t.trace_id))?),
            None => LuaValue::Nil,
        },
    )?;
    table.set(
        "span_id",
        match exemplar.trace.and_then(|t| t.span_id) {
            Some(id) => LuaValue::String(lua.create_string(to_hex(&id))?),
            None => LuaValue::Nil,
        },
    )?;
    // `nil` without a trace, as `event.log.trace_flags` is; present so `Event.new` rebuilds the
    // flags rather than zeroing them.
    table.set(
        "trace_flags",
        match exemplar.trace {
            Some(t) => LuaValue::Integer(i64::from(t.flags)),
            None => LuaValue::Nil,
        },
    )?;
    table.set("attributes", attrmap_to_lua_table(lua, &exemplar.filtered_attributes)?)?;
    Ok(table)
}

/// `ExpHistogram::positive`/`negative`'s `(offset, counts)` as `{offset=, counts=[...]}`.
fn exp_buckets_table<'lua>(lua: &'lua Lua, bucket: &(i32, Vec<u64>)) -> mlua::Result<Table<'lua>> {
    let (offset, counts) = bucket;
    let table = lua.create_table()?;
    table.set("offset", *offset as i64)?;
    let counts_table = lua.create_table()?;
    for (i, count) in counts.iter().enumerate() {
        counts_table.set(i + 1, exact_u64_to_lua(lua, *count)?)?;
    }
    table.set("counts", counts_table)?;
    Ok(table)
}

impl UserData for MetricProxy {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        // The one method on this proxy. mlua checks `add_method` methods before the `Index`
        // metamethod, so `m:quantile(q)` resolves here and the `Index` closure needs no
        // `"quantile"` arm. Only a `distribution` answers (`DdSketch::quantile`); any other kind
        // returns `nil`.
        methods.add_method("quantile", |_, this, q: f64| {
            this.with_metric(|m| {
                Ok(match &m.kind {
                    MetricKind::Distribution(sketch) => sketch.quantile(q),
                    _ => None,
                })
            })
        });

        methods.add_meta_method(MetaMethod::Index, |lua, this, key: mlua::String| {
            this.with_metric(|m| match key.to_str()? {
                "name" => Ok(LuaValue::String(lua.create_string(resolve(m.name))?)),
                "unit" => match m.unit {
                    Some(s) => Ok(LuaValue::String(lua.create_string(resolve(s))?)),
                    None => Ok(LuaValue::Nil),
                },
                "description" => match m.description {
                    Some(s) => Ok(LuaValue::String(lua.create_string(resolve(s))?)),
                    None => Ok(LuaValue::Nil),
                },
                "start_timestamp" => {
                    Ok(LuaValue::String(lua.create_string(m.start_timestamp.to_string())?))
                }
                "flags" => Ok(LuaValue::Integer(m.flags as i64)),
                "is_no_recorded_value" => Ok(LuaValue::Boolean(m.is_no_recorded_value())),
                "kind" => Ok(LuaValue::String(lua.create_string(m.kind.name())?)),
                "exemplars" => {
                    let t = lua.create_table()?;
                    for (i, e) in m.exemplars.iter().enumerate() {
                        t.set(i + 1, exemplar_to_table(lua, e)?)?;
                    }
                    Ok(LuaValue::Table(t))
                }
                "value" => match &m.kind {
                    MetricKind::Sum(s) => Ok(LuaValue::Number(s.value)),
                    MetricKind::Gauge(v) | MetricKind::GaugeDelta(v) => Ok(LuaValue::Number(*v)),
                    _ => Ok(LuaValue::Nil),
                },
                "temporality" => match &m.kind {
                    MetricKind::Sum(s) => {
                        Ok(LuaValue::String(lua.create_string(s.temporality.as_str())?))
                    }
                    MetricKind::Histogram(h) => {
                        Ok(LuaValue::String(lua.create_string(h.temporality.as_str())?))
                    }
                    MetricKind::ExponentialHistogram(e) => {
                        Ok(LuaValue::String(lua.create_string(e.temporality.as_str())?))
                    }
                    _ => Ok(LuaValue::Nil),
                },
                "monotonic" => match &m.kind {
                    MetricKind::Sum(s) => Ok(LuaValue::Boolean(s.monotonic)),
                    _ => Ok(LuaValue::Nil),
                },
                "values" => match &m.kind {
                    MetricKind::Samples(s) => {
                        let t = lua.create_table()?;
                        for (i, v) in s.values.iter().enumerate() {
                            t.set(i + 1, *v)?;
                        }
                        Ok(LuaValue::Table(t))
                    }
                    _ => Ok(LuaValue::Nil),
                },
                "sample_rate" => match &m.kind {
                    MetricKind::Samples(s) => Ok(LuaValue::Number(s.sample_rate)),
                    _ => Ok(LuaValue::Nil),
                },
                "members" => match &m.kind {
                    MetricKind::SetMembers(members) => {
                        let t = lua.create_table()?;
                        for (i, member) in members.iter().enumerate() {
                            t.set(i + 1, lua.create_string(member)?)?;
                        }
                        Ok(LuaValue::Table(t))
                    }
                    _ => Ok(LuaValue::Nil),
                },
                "estimate" => match &m.kind {
                    MetricKind::Set(hll) => exact_u64_to_lua(lua, hll.estimate()),
                    _ => Ok(LuaValue::Nil),
                },
                "buckets" => match &m.kind {
                    MetricKind::Histogram(h) => {
                        let t = lua.create_table()?;
                        for (i, (bound, count)) in h.buckets.iter().enumerate() {
                            let row = lua.create_table()?;
                            row.set("bound", *bound)?;
                            row.set("count", exact_u64_to_lua(lua, *count)?)?;
                            t.set(i + 1, row)?;
                        }
                        Ok(LuaValue::Table(t))
                    }
                    _ => Ok(LuaValue::Nil),
                },
                "sum" => match &m.kind {
                    MetricKind::Histogram(h) => Ok(opt_number(h.sum)),
                    MetricKind::ExponentialHistogram(e) => Ok(opt_number(e.sum)),
                    MetricKind::Summary(s) => Ok(LuaValue::Number(s.sum)),
                    _ => Ok(LuaValue::Nil),
                },
                "min" => match &m.kind {
                    MetricKind::Histogram(h) => Ok(opt_number(h.min)),
                    MetricKind::ExponentialHistogram(e) => Ok(opt_number(e.min)),
                    _ => Ok(LuaValue::Nil),
                },
                "max" => match &m.kind {
                    MetricKind::Histogram(h) => Ok(opt_number(h.max)),
                    MetricKind::ExponentialHistogram(e) => Ok(opt_number(e.max)),
                    _ => Ok(LuaValue::Nil),
                },
                // A field, not a method like `quantile`, since it takes no argument. A
                // `distribution`'s is `DdSketch::count()`.
                "count" => match &m.kind {
                    MetricKind::Distribution(sketch) => {
                        exact_u64_to_lua(lua, sketch.count() as u64)
                    }
                    MetricKind::ExponentialHistogram(e) => exact_u64_to_lua(lua, e.count),
                    MetricKind::Summary(s) => exact_u64_to_lua(lua, s.count),
                    _ => Ok(LuaValue::Nil),
                },
                "scale" => match &m.kind {
                    MetricKind::ExponentialHistogram(e) => Ok(LuaValue::Integer(e.scale as i64)),
                    _ => Ok(LuaValue::Nil),
                },
                "zero_count" => match &m.kind {
                    MetricKind::ExponentialHistogram(e) => exact_u64_to_lua(lua, e.zero_count),
                    _ => Ok(LuaValue::Nil),
                },
                "zero_threshold" => match &m.kind {
                    MetricKind::ExponentialHistogram(e) => Ok(LuaValue::Number(e.zero_threshold)),
                    _ => Ok(LuaValue::Nil),
                },
                "positive" => match &m.kind {
                    MetricKind::ExponentialHistogram(e) => {
                        Ok(LuaValue::Table(exp_buckets_table(lua, &e.positive)?))
                    }
                    _ => Ok(LuaValue::Nil),
                },
                "negative" => match &m.kind {
                    MetricKind::ExponentialHistogram(e) => {
                        Ok(LuaValue::Table(exp_buckets_table(lua, &e.negative)?))
                    }
                    _ => Ok(LuaValue::Nil),
                },
                "quantiles" => match &m.kind {
                    MetricKind::Summary(s) => {
                        let t = lua.create_table()?;
                        for (i, (q, v)) in s.quantiles.iter().enumerate() {
                            let row = lua.create_table()?;
                            row.set("quantile", *q)?;
                            row.set("value", *v)?;
                            t.set(i + 1, row)?;
                        }
                        Ok(LuaValue::Table(t))
                    }
                    _ => Ok(LuaValue::Nil),
                },
                // `quantile` is a method, resolved before this fallback.
                _ => Ok(LuaValue::Nil),
            })
        });

        methods.add_meta_method(
            MetaMethod::NewIndex,
            |_, this, (key, value): (mlua::String, LuaValue)| {
                let index = this.index;
                let key = key.to_str()?;
                this.with_metric_mut(|m| match key {
                    "name" => {
                        let LuaValue::String(s) = value else {
                            return Err(mlua::Error::RuntimeError(format!(
                                "event.metrics[{}].name must be a string, got {}",
                                index + 1,
                                value.type_name()
                            )));
                        };
                        m.name = intern(s.to_str()?);
                        Ok(())
                    }
                    "unit" => match value {
                        LuaValue::Nil => {
                            m.unit = None;
                            Ok(())
                        }
                        LuaValue::String(s) => {
                            m.unit = Some(intern(s.to_str()?));
                            Ok(())
                        }
                        other => Err(mlua::Error::RuntimeError(format!(
                            "event.metrics[{}].unit must be a string or nil, got {}",
                            index + 1,
                            other.type_name()
                        ))),
                    },
                    "description" => match value {
                        LuaValue::Nil => {
                            m.description = None;
                            Ok(())
                        }
                        LuaValue::String(s) => {
                            m.description = Some(intern(s.to_str()?));
                            Ok(())
                        }
                        other => Err(mlua::Error::RuntimeError(format!(
                            "event.metrics[{}].description must be a string or nil, got {}",
                            index + 1,
                            other.type_name()
                        ))),
                    },
                    "start_timestamp" => {
                        let LuaValue::String(s) = value else {
                            return Err(mlua::Error::RuntimeError(format!(
                                "event.metrics[{}].start_timestamp must be a string of decimal \
                                 digits, got {}",
                                index + 1,
                                value.type_name()
                            )));
                        };
                        let ts: i64 = s.to_str()?.parse().map_err(|_| {
                            mlua::Error::RuntimeError(format!(
                                "event.metrics[{}].start_timestamp must be a string of decimal \
                                 digits",
                                index + 1
                            ))
                        })?;
                        m.start_timestamp = ts;
                        Ok(())
                    }
                    "value" => match &mut m.kind {
                        MetricKind::Sum(s) => {
                            s.value = require_finite_number(
                                value,
                                &format!("event.metrics[{}].value", index + 1),
                            )?;
                            Ok(())
                        }
                        MetricKind::Gauge(v) => {
                            *v = require_finite_number(
                                value,
                                &format!("event.metrics[{}].value", index + 1),
                            )?;
                            Ok(())
                        }
                        other => Err(metric_ro_error(index, "value", other)),
                    },
                    "temporality" => match &mut m.kind {
                        MetricKind::Sum(s) => {
                            s.temporality = parse_temporality(&value, index)?;
                            Ok(())
                        }
                        other => Err(metric_ro_error(index, "temporality", other)),
                    },
                    "monotonic" => match &mut m.kind {
                        MetricKind::Sum(s) => {
                            let LuaValue::Boolean(b) = value else {
                                return Err(mlua::Error::RuntimeError(format!(
                                    "event.metrics[{}].monotonic must be a boolean, got {}",
                                    index + 1,
                                    value.type_name()
                                )));
                            };
                            s.monotonic = b;
                            Ok(())
                        }
                        other => Err(metric_ro_error(index, "monotonic", other)),
                    },
                    "flags"
                    | "is_no_recorded_value"
                    | "kind"
                    | "exemplars"
                    | "values"
                    | "sample_rate"
                    | "members"
                    | "estimate"
                    | "buckets"
                    | "sum"
                    | "min"
                    | "max"
                    | "count"
                    | "scale"
                    | "zero_count"
                    | "zero_threshold"
                    | "positive"
                    | "negative"
                    | "quantiles"
                    | "quantile" => Err(metric_ro_error(index, key, &m.kind)),
                    other => Err(mlua::Error::RuntimeError(format!(
                        "event.metrics[{}] has no field '{other}'",
                        index + 1
                    ))),
                })
            },
        );
    }
}

/// A metric record as a plain table, for `EventProxy::to_table`'s `metrics`: every
/// kind-specific field, each present only on the kind that carries it.
fn metric_to_table<'lua>(lua: &'lua Lua, record: &MetricRecord) -> mlua::Result<Table<'lua>> {
    let table = lua.create_table()?;
    table.set("name", resolve(record.name))?;
    table.set(
        "unit",
        match record.unit {
            Some(s) => LuaValue::String(lua.create_string(resolve(s))?),
            None => LuaValue::Nil,
        },
    )?;
    table.set(
        "description",
        match record.description {
            Some(s) => LuaValue::String(lua.create_string(resolve(s))?),
            None => LuaValue::Nil,
        },
    )?;
    table.set("start_timestamp", record.start_timestamp.to_string())?;
    table.set("flags", record.flags as i64)?;
    table.set("is_no_recorded_value", record.is_no_recorded_value())?;
    table.set("kind", record.kind.name())?;
    let exemplars = lua.create_table()?;
    for (i, e) in record.exemplars.iter().enumerate() {
        exemplars.set(i + 1, exemplar_to_table(lua, e)?)?;
    }
    table.set("exemplars", exemplars)?;

    match &record.kind {
        MetricKind::Sum(s) => {
            table.set("value", s.value)?;
            table.set("temporality", s.temporality.as_str())?;
            table.set("monotonic", s.monotonic)?;
        }
        MetricKind::Gauge(v) => table.set("value", *v)?,
        MetricKind::GaugeDelta(v) => table.set("value", *v)?,
        MetricKind::Samples(s) => {
            let values = lua.create_table()?;
            for (i, v) in s.values.iter().enumerate() {
                values.set(i + 1, *v)?;
            }
            table.set("values", values)?;
            table.set("sample_rate", s.sample_rate)?;
        }
        MetricKind::Distribution(sketch) => {
            table.set("count", exact_u64_to_lua(lua, sketch.count() as u64)?)?;
        }
        MetricKind::SetMembers(members) => {
            let t = lua.create_table()?;
            for (i, m) in members.iter().enumerate() {
                t.set(i + 1, lua.create_string(m)?)?;
            }
            table.set("members", t)?;
        }
        MetricKind::Set(hll) => table.set("estimate", exact_u64_to_lua(lua, hll.estimate())?)?,
        MetricKind::Histogram(h) => {
            let buckets = lua.create_table()?;
            for (i, (bound, count)) in h.buckets.iter().enumerate() {
                let row = lua.create_table()?;
                row.set("bound", *bound)?;
                row.set("count", exact_u64_to_lua(lua, *count)?)?;
                buckets.set(i + 1, row)?;
            }
            table.set("buckets", buckets)?;
            table.set("temporality", h.temporality.as_str())?;
            table.set("sum", opt_number(h.sum))?;
            table.set("min", opt_number(h.min))?;
            table.set("max", opt_number(h.max))?;
        }
        MetricKind::ExponentialHistogram(e) => {
            table.set("scale", e.scale as i64)?;
            table.set("zero_count", exact_u64_to_lua(lua, e.zero_count)?)?;
            table.set("zero_threshold", e.zero_threshold)?;
            table.set("positive", exp_buckets_table(lua, &e.positive)?)?;
            table.set("negative", exp_buckets_table(lua, &e.negative)?)?;
            table.set("temporality", e.temporality.as_str())?;
            table.set("count", exact_u64_to_lua(lua, e.count)?)?;
            table.set("sum", opt_number(e.sum))?;
            table.set("min", opt_number(e.min))?;
            table.set("max", opt_number(e.max))?;
        }
        MetricKind::Summary(s) => {
            let quantiles = lua.create_table()?;
            for (i, (q, v)) in s.quantiles.iter().enumerate() {
                let row = lua.create_table()?;
                row.set("quantile", *q)?;
                row.set("value", *v)?;
                quantiles.set(i + 1, row)?;
            }
            table.set("quantiles", quantiles)?;
            table.set("count", exact_u64_to_lua(lua, s.count)?)?;
            table.set("sum", s.sum)?;
        }
    }
    Ok(table)
}

// -- `event.span` ---------------------------------------------------------------------------

/// The `event.span` sub-proxy, read-only. A script changes a span by rebuilding the event with
/// `Event.new(event:to_table())`, edited (`docs/design/lua-api.md`'s "Constructing events");
/// `crate::construct`'s `span_from_table` is [`span_to_table`]'s inverse. Construct only when
/// `event.span.is_some()`, as for [`LogProxy`].
///
/// There is no `event.span.attributes`: a `SpanRecord` has no attributes of its own, and a span
/// event's are `event.attributes`.
struct SpanProxy(Rc<RefCell<Event>>);

impl SpanProxy {
    fn with_span<R>(&self, f: impl FnOnce(&SpanRecord) -> R) -> R {
        let event = self.0.borrow();
        f(event.span.as_ref().expect("SpanProxy is only ever created when event.span.is_some()"))
    }
}

impl UserData for SpanProxy {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_meta_method(MetaMethod::Index, |lua, this, key: mlua::String| {
            this.with_span(|span| match key.to_str()? {
                "trace_id" => Ok(LuaValue::String(lua.create_string(to_hex(&span.trace_id))?)),
                "span_id" => Ok(LuaValue::String(lua.create_string(to_hex(&span.span_id))?)),
                "parent_span_id" => match span.parent_span_id {
                    Some(id) => Ok(LuaValue::String(lua.create_string(to_hex(&id))?)),
                    None => Ok(LuaValue::Nil),
                },
                "name" => value_to_lua(lua, &span.name),
                "kind" => Ok(LuaValue::String(lua.create_string(span.kind.as_str())?)),
                "status" => Ok(LuaValue::String(lua.create_string(span.status.as_str())?)),
                "status_message" => match span.ext.as_ref().and_then(|e| e.status_message.as_ref())
                {
                    Some(m) => Ok(LuaValue::String(lua.create_string(m)?)),
                    None => Ok(LuaValue::Nil),
                },
                "trace_state" => match span.ext.as_ref().and_then(|e| e.trace_state.as_ref()) {
                    Some(s) => Ok(LuaValue::String(lua.create_string(s)?)),
                    None => Ok(LuaValue::Nil),
                },
                "end_timestamp" => {
                    Ok(LuaValue::String(lua.create_string(span.end_timestamp.to_string())?))
                }
                "flags" => Ok(LuaValue::Integer(span.flags as i64)),
                "dropped_attributes_count" => Ok(LuaValue::Integer(
                    span.ext.as_ref().map(|e| e.dropped_attributes_count).unwrap_or(0) as i64,
                )),
                "dropped_events_count" => Ok(LuaValue::Integer(
                    span.ext.as_ref().map(|e| e.dropped_events_count).unwrap_or(0) as i64,
                )),
                "dropped_links_count" => Ok(LuaValue::Integer(
                    span.ext.as_ref().map(|e| e.dropped_links_count).unwrap_or(0) as i64,
                )),
                "events" => {
                    let t = lua.create_table()?;
                    for (i, ev) in span.events.iter().enumerate() {
                        t.set(i + 1, span_event_to_table(lua, ev)?)?;
                    }
                    Ok(LuaValue::Table(t))
                }
                "links" => {
                    let t = lua.create_table()?;
                    for (i, link) in span.links.iter().enumerate() {
                        t.set(i + 1, span_link_to_table(lua, link)?)?;
                    }
                    Ok(LuaValue::Table(t))
                }
                _ => Ok(LuaValue::Nil),
            })
        });

        // Every field is read-only, so there's no per-field split.
        methods.add_meta_method(
            MetaMethod::NewIndex,
            |_, _this, (_key, _value): (mlua::String, LuaValue)| {
                Err::<(), _>(mlua::Error::RuntimeError("event.span is read-only".to_string()))
            },
        );
    }
}

fn span_event_to_table<'lua>(lua: &'lua Lua, ev: &SpanEvent) -> mlua::Result<Table<'lua>> {
    let table = lua.create_table()?;
    table.set("timestamp", ev.timestamp.to_string())?;
    table.set("name", value_to_lua(lua, &ev.name)?)?;
    table.set("attributes", attrmap_to_lua_table(lua, &ev.attributes)?)?;
    table.set("dropped_attributes_count", ev.dropped_attributes_count as i64)?;
    Ok(table)
}

fn span_link_to_table<'lua>(lua: &'lua Lua, link: &SpanLink) -> mlua::Result<Table<'lua>> {
    let table = lua.create_table()?;
    table.set("trace_id", to_hex(&link.trace_id))?;
    table.set("span_id", to_hex(&link.span_id))?;
    table.set(
        "trace_state",
        match &link.trace_state {
            Some(s) => LuaValue::String(lua.create_string(s)?),
            None => LuaValue::Nil,
        },
    )?;
    table.set("flags", link.flags as i64)?;
    table.set("dropped_attributes_count", link.dropped_attributes_count as i64)?;
    table.set("attributes", attrmap_to_lua_table(lua, &link.attributes)?)?;
    Ok(table)
}

/// A span record as a plain table, for `EventProxy::to_table`'s `span`; mirrors [`SpanProxy`]'s
/// fields.
fn span_to_table<'lua>(lua: &'lua Lua, span: &SpanRecord) -> mlua::Result<Table<'lua>> {
    let table = lua.create_table()?;
    table.set("trace_id", to_hex(&span.trace_id))?;
    table.set("span_id", to_hex(&span.span_id))?;
    table.set(
        "parent_span_id",
        match span.parent_span_id {
            Some(id) => LuaValue::String(lua.create_string(to_hex(&id))?),
            None => LuaValue::Nil,
        },
    )?;
    table.set("name", value_to_lua(lua, &span.name)?)?;
    table.set("kind", span.kind.as_str())?;
    table.set("status", span.status.as_str())?;
    table.set(
        "status_message",
        match span.ext.as_ref().and_then(|e| e.status_message.as_ref()) {
            Some(m) => LuaValue::String(lua.create_string(m)?),
            None => LuaValue::Nil,
        },
    )?;
    table.set(
        "trace_state",
        match span.ext.as_ref().and_then(|e| e.trace_state.as_ref()) {
            Some(s) => LuaValue::String(lua.create_string(s)?),
            None => LuaValue::Nil,
        },
    )?;
    table.set("end_timestamp", span.end_timestamp.to_string())?;
    table.set("flags", span.flags as i64)?;
    table.set(
        "dropped_attributes_count",
        span.ext.as_ref().map(|e| e.dropped_attributes_count).unwrap_or(0) as i64,
    )?;
    table.set(
        "dropped_events_count",
        span.ext.as_ref().map(|e| e.dropped_events_count).unwrap_or(0) as i64,
    )?;
    table.set(
        "dropped_links_count",
        span.ext.as_ref().map(|e| e.dropped_links_count).unwrap_or(0) as i64,
    )?;
    let events = lua.create_table()?;
    for (i, ev) in span.events.iter().enumerate() {
        events.set(i + 1, span_event_to_table(lua, ev)?)?;
    }
    table.set("events", events)?;
    let links = lua.create_table()?;
    for (i, link) in span.links.iter().enumerate() {
        links.set(i + 1, span_link_to_table(lua, link)?)?;
    }
    table.set("links", links)?;
    Ok(table)
}

/// Extracts the owned `Event` and its routing mark from a returned [`EventProxy`] userdata, for
/// `process()`'s and `flush()`'s return values.
///
/// `AnyUserData::take`, not `borrow().clone()`: `take` empties the Lua box, so
/// [`EventProxy::into_inner`]'s `Rc::try_unwrap` succeeds; with a clone, the box's reference
/// would live until GC and every call would pay a full `Event` clone.
///
/// The cost: `pending = event` makes a second Lua reference to the same box, invisible from Rust
/// (`Rc::strong_count` stays 1), and `take` invalidates every alias. A script that stashes an
/// event for `flush()` and also returns it finds the stash destructed; it should stash
/// `event:clone()`.
pub(crate) fn take_event(lua: &Lua, ud: AnyUserData) -> mlua::Result<(Event, Option<u16>)> {
    match ud.take::<EventProxy>() {
        Ok(proxy) => Ok(proxy.into_inner(lua)),
        Err(mlua::Error::UserDataDestructed) => Err(mlua::Error::RuntimeError(
            "this event was already returned/emitted elsewhere and can no longer be used -- an \
             event handle is consumed once it's returned from process() or included in a flush() \
             table; use event:clone() to keep an independent copy if you need to both return an \
             event now and hold onto it for later"
                .to_string(),
        )),
        Err(other) => Err(other),
    }
}

/// Rewrites a script's use of a destructed handle into this crate's "consumed once returned"
/// wording; any other error passes through.
///
/// A stashed `event`, or a cached sub-proxy (`event.attributes`, `event.log`, `event.span`) torn
/// down by [`EventProxy::into_inner`], is destructed once its event is returned. Using it inside
/// a running script hits mlua's replacement metatable, which raises
/// `mlua::Error::CallbackDestructed` without reaching this module's closures, and that arrives
/// wrapped in `CallbackError` layers. So this wraps the whole `process`/`flush` call rather than
/// any metamethod. `MetricProxy` reports the same mistake itself (`metric_handle_consumed_error`),
/// but the message names it too, since the rule is the same for every handle.
pub(crate) fn clarify_destructed_handle_use(err: mlua::Error) -> mlua::Error {
    fn is_destructed_handle_use(err: &mlua::Error) -> bool {
        match err {
            mlua::Error::CallbackDestructed => true,
            mlua::Error::CallbackError { cause, .. } => is_destructed_handle_use(cause),
            _ => false,
        }
    }
    if is_destructed_handle_use(&err) {
        return mlua::Error::RuntimeError(
            "this event, or a handle obtained from it (event.attributes, event.log, \
             event.metrics[i], or event.span), was already returned/emitted elsewhere and can no \
             longer be used -- an event handle, and any sub-handle obtained from it, are all \
             consumed once the event is returned from process() or included in a flush() table; \
             use event:clone() before returning if you need to keep using any of them afterward"
                .to_string(),
        );
    }
    err
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ProcessOutcome, ScriptWorker};
    use bytes::Bytes;
    use logit_core::{
        AttrMap, BodyFormat, DdSketch, ExpHistogram, Histogram, HyperLogLog, Samples, Severity,
        SpanExt, SpanKind, SpanStatus, Sum, Summary, Value,
    };

    fn worker(source: &str) -> ScriptWorker {
        ScriptWorker::new(source).expect("script should load")
    }

    fn emitted(outcome: ProcessOutcome) -> Event {
        match outcome {
            ProcessOutcome::Emit(e, _) => *e,
            _ => panic!("expected Emit"),
        }
    }

    /// As `lib.rs`'s own `process_err` helper -- `ProcessOutcome` isn't `Debug`, so
    /// `Result::unwrap_err` doesn't work directly on `ScriptWorker::process`'s return value.
    fn process_err(w: &ScriptWorker, event: Event) -> String {
        match w.process(event) {
            Err(err) => err.to_string(),
            Ok(_) => panic!("expected process() to reject this script"),
        }
    }

    // -- fixtures -----------------------------------------------------------------------------

    fn log_record_with_everything() -> LogRecord {
        LogRecord {
            message: Value::str("GET /widgets"),
            severity: Some(Severity::Warn),
            body_format: BodyFormat::Json,
            trace: Some(TraceRef { trace_id: [0x11; 16], span_id: Some([0x22; 8]), flags: 1 }),
            event_name: Some(intern("request.completed")),
            observed_timestamp: 9_007_199_254_740_993, // one past 2^53
            dropped_attributes_count: 3,
        }
    }

    fn log_event() -> Event {
        Event::log(1_700_000_000_000_000_000, AttrMap::new(), log_record_with_everything())
    }

    /// A metric record carrying non-default values on every kind-independent field (unit,
    /// description, start_timestamp, flags, one exemplar) -- callers fill in `kind`.
    fn metric_record(kind: MetricKind) -> MetricRecord {
        let mut record = MetricRecord::new(intern("test.metric"), kind);
        record.unit = Some(intern("ms"));
        record.description = Some(intern("a test metric"));
        record.start_timestamp = 1_700_000_000_000_000_000;
        record.flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
        record.exemplars = vec![Exemplar {
            timestamp: 1_700_000_000_500_000_000,
            value: 42.0,
            trace: Some(TraceRef { trace_id: [0x33; 16], span_id: Some([0x44; 8]), flags: 1 }),
            filtered_attributes: {
                let mut m = AttrMap::new();
                m.insert("dropped", "yes");
                m
            },
        }];
        record
    }

    fn metric_event(kind: MetricKind) -> Event {
        Event::metric(1_700_000_000_000_000_000, AttrMap::new(), metric_record(kind))
    }

    fn sum_kind() -> MetricKind {
        MetricKind::Sum(Sum { value: 12.5, temporality: Temporality::Cumulative, monotonic: false })
    }

    fn samples_kind() -> MetricKind {
        let mut samples = Samples::new([1.0, 2.0, 3.0]);
        samples.sample_rate = 0.5;
        MetricKind::Samples(samples)
    }

    fn distribution_kind() -> MetricKind {
        let mut sketch = DdSketch::new();
        sketch.add(1.0);
        sketch.add(2.0);
        sketch.add(3.0);
        MetricKind::Distribution(sketch)
    }

    fn set_members_kind() -> MetricKind {
        MetricKind::SetMembers(vec![Bytes::from_static(b"alice"), Bytes::from_static(b"bob")])
    }

    fn set_kind() -> MetricKind {
        let mut hll = HyperLogLog::new();
        hll.insert(b"alice");
        hll.insert(b"bob");
        MetricKind::Set(hll)
    }

    fn histogram_kind() -> MetricKind {
        MetricKind::Histogram(Histogram {
            buckets: vec![(1.0, 3), (5.0, 7)],
            temporality: Temporality::Cumulative,
            sum: Some(15.0),
            min: Some(0.5),
            max: Some(9.0),
        })
    }

    fn exp_histogram_kind() -> MetricKind {
        MetricKind::ExponentialHistogram(ExpHistogram {
            scale: 3,
            zero_count: 2,
            zero_threshold: 0.001,
            positive: (1, vec![1, 2, 3]),
            negative: (0, vec![4, 5]),
            temporality: Temporality::Delta,
            count: 11,
            sum: Some(20.0),
            min: Some(-1.0),
            max: Some(9.5),
        })
    }

    fn summary_kind() -> MetricKind {
        MetricKind::Summary(Summary {
            quantiles: vec![(0.5, 10.0), (0.99, 99.0)],
            count: 42,
            sum: 500.0,
        })
    }

    fn span_record_with_everything() -> SpanRecord {
        SpanRecord {
            trace_id: [1; 16],
            span_id: [2; 8],
            parent_span_id: Some([3; 8]),
            name: Value::str("GET /"),
            kind: SpanKind::Server,
            status: SpanStatus::Error,
            events: vec![SpanEvent {
                timestamp: 1_700_000_000_100_000_000,
                name: Value::str("exception"),
                attributes: {
                    let mut m = AttrMap::new();
                    m.insert("type", "Timeout");
                    m
                },
                dropped_attributes_count: 2,
            }],
            links: vec![SpanLink {
                trace_id: [4; 16],
                span_id: [5; 8],
                attributes: {
                    let mut m = AttrMap::new();
                    m.insert("linked", true);
                    m
                },
                flags: 1,
                trace_state: Some(Bytes::from_static(b"vendor=value")),
                dropped_attributes_count: 1,
            }],
            end_timestamp: 1_700_000_000_900_000_000,
            flags: 1,
            ext: Some(Box::new(SpanExt {
                status_message: Some(Bytes::from_static(b"boom")),
                trace_state: Some(Bytes::from_static(b"vendor=xyz")),
                dropped_attributes_count: 3,
                dropped_events_count: 4,
                dropped_links_count: 5,
            })),
        }
    }

    fn span_event_full() -> Event {
        Event::span(1_700_000_000_000_000_000, AttrMap::new(), span_record_with_everything())
    }

    fn minimal_span_record() -> SpanRecord {
        SpanRecord {
            trace_id: [9; 16],
            span_id: [8; 8],
            parent_span_id: None,
            name: Value::str("minimal"),
            kind: SpanKind::Internal,
            status: SpanStatus::Unset,
            events: vec![],
            links: vec![],
            end_timestamp: 0,
            flags: 0,
            ext: None,
        }
    }

    fn minimal_span_event() -> Event {
        Event::span(0, AttrMap::new(), minimal_span_record())
    }

    // -- event.log: new fields -----------------------------------------------------------------

    #[test]
    fn log_event_name_reads_and_write_interns_and_reads_back() {
        let w = worker(
            r#"
            function process(event)
                event.attributes.original = event.log.event_name
                event.log.event_name = "renamed"
                return event
            end
            "#,
        );
        let out = emitted(w.process(log_event()).unwrap());
        assert_eq!(
            out.attributes.get("original").and_then(|v| v.as_str()),
            Some("request.completed")
        );
        assert_eq!(resolve(out.log.unwrap().event_name.unwrap()), "renamed");
    }

    #[test]
    fn log_event_name_nil_clears() {
        let w = worker(
            r#"
            function process(event)
                event.log.event_name = nil
                return event
            end
            "#,
        );
        let out = emitted(w.process(log_event()).unwrap());
        assert_eq!(out.log.unwrap().event_name, None);
    }

    #[test]
    fn log_observed_timestamp_round_trips_exactly_beyond_2_53() {
        let w = worker(
            r#"
            function process(event)
                local ts = event.log.observed_timestamp
                event.attributes.captured = ts
                event.log.observed_timestamp = ts
                return event
            end
            "#,
        );
        let event = log_event();
        let original = event.log.as_ref().unwrap().observed_timestamp;
        let out = emitted(w.process(event).unwrap());
        assert_eq!(out.log.as_ref().unwrap().observed_timestamp, original);
        assert_eq!(
            out.attributes.get("captured").and_then(|v| v.as_str()),
            Some(original.to_string().as_str())
        );
    }

    #[test]
    fn log_observed_timestamp_zero_reads_as_the_string_zero() {
        let w = worker(
            r#"
            function process(event)
                event.attributes.ts = event.log.observed_timestamp
                return event
            end
            "#,
        );
        let mut record = log_record_with_everything();
        record.observed_timestamp = 0;
        let out = emitted(w.process(Event::log(0, AttrMap::new(), record)).unwrap());
        assert_eq!(out.attributes.get("ts").and_then(|v| v.as_str()), Some("0"));
    }

    #[test]
    fn log_dropped_attributes_count_reads_and_is_read_only() {
        let w = worker(
            r#"
            function process(event)
                event.attributes.dac = event.log.dropped_attributes_count
                return event
            end
            "#,
        );
        let out = emitted(w.process(log_event()).unwrap());
        assert_eq!(out.attributes.get("dac"), Some(&Value::I64(3)));

        let w = worker(
            r#"
            function process(event)
                event.log.dropped_attributes_count = 5
                return event
            end
            "#,
        );
        let err = process_err(&w, log_event());
        assert!(err.contains("event.log.dropped_attributes_count is read-only"), "{err}");
    }

    // -- event.metrics: shape -------------------------------------------------------------------

    #[test]
    fn metrics_len_and_out_of_range_index_is_nil() {
        let w = worker(
            r#"
            function process(event)
                event.attributes.len = #event.metrics
                event.attributes.oob_is_nil = event.metrics[99] == nil
                event.attributes.zero_is_nil = event.metrics[0] == nil
                return event
            end
            "#,
        );
        let out = emitted(w.process(metric_event(sum_kind())).unwrap());
        assert_eq!(out.attributes.get("len"), Some(&Value::I64(1)));
        assert_eq!(out.attributes.get("oob_is_nil"), Some(&Value::Bool(true)));
        assert_eq!(out.attributes.get("zero_is_nil"), Some(&Value::Bool(true)));
    }

    #[test]
    fn metrics_is_present_and_empty_on_a_metric_less_event() {
        let w = worker(
            r#"
            function process(event)
                event.attributes.len = #event.metrics
                return event
            end
            "#,
        );
        let out = emitted(w.process(Event::empty(0, AttrMap::new())).unwrap());
        assert_eq!(out.attributes.get("len"), Some(&Value::I64(0)));
    }

    #[test]
    fn metrics_non_integer_index_errors() {
        let w = worker(
            r#"
            function process(event)
                local m = event.metrics["x"]
                return event
            end
            "#,
        );
        let err = process_err(&w, metric_event(sum_kind()));
        assert!(err.contains("indexed by an integer"), "{err}");
    }

    #[test]
    fn metric_index_on_emptied_list_errors_cleanly() {
        let lua = Lua::new();
        let event = Rc::new(RefCell::new(metric_event(sum_kind())));
        // The event stays alive, so this exercises the index check, not the failed upgrade.
        event.borrow_mut().metrics.clear(); // simulate a handle outliving its metric
        let proxy = MetricProxy { event: Rc::downgrade(&event), index: 0 };
        let ud = lua.create_userdata(proxy).unwrap();
        lua.globals().set("m", ud).unwrap();
        let err = lua.load("return m.value").eval::<LuaValue>().unwrap_err();
        assert!(err.to_string().contains("event.metrics[1] no longer exists"), "{err}");
    }

    #[test]
    fn metric_handle_errors_once_its_event_is_gone() {
        let lua = Lua::new();
        let weak = {
            let event = Rc::new(RefCell::new(metric_event(sum_kind())));
            Rc::downgrade(&event)
            // `event`'s last strong reference drops here.
        };
        let proxy = MetricProxy { event: weak, index: 0 };
        let ud = lua.create_userdata(proxy).unwrap();
        lua.globals().set("m", ud).unwrap();
        let err = lua.load("return m.value").eval::<LuaValue>().unwrap_err();
        assert!(
            err.to_string().contains(
                "event.metrics[1] belongs to an event that has already been returned from \
                 process() or included in a flush() table -- stash event:clone() instead"
            ),
            "{err}"
        );
    }

    // -- event.metrics: per-kind field reads -----------------------------------------------------

    #[test]
    fn sum_metric_fields_read_correctly() {
        let w = worker(
            r#"
            function process(event)
                local m = event.metrics[1]
                assert(m.kind == "sum", m.kind)
                assert(m.value == 12.5, tostring(m.value))
                assert(m.temporality == "cumulative", m.temporality)
                assert(m.monotonic == false)
                assert(m.name == "test.metric")
                assert(m.unit == "ms")
                assert(m.description == "a test metric")
                assert(m.start_timestamp == "1700000000000000000")
                assert(m.flags == 1)
                assert(m.is_no_recorded_value == true)
                assert(#m.exemplars == 1)
                local e = m.exemplars[1]
                assert(e.timestamp == "1700000000500000000")
                assert(e.value == 42.0)
                assert(e.trace_id == string.rep("33", 16))
                assert(e.span_id == string.rep("44", 8))
                assert(e.trace_flags == 1)
                assert(e.attributes.dropped == "yes")
                assert(m.values == nil)
                assert(m.buckets == nil)
                assert(m.totally_bogus_field == nil)
                return event
            end
            "#,
        );
        w.process(metric_event(sum_kind())).unwrap();
    }

    #[test]
    fn gauge_metric_fields_read_correctly() {
        let w = worker(
            r#"
            function process(event)
                local m = event.metrics[1]
                assert(m.kind == "gauge")
                assert(m.value == 3.25)
                assert(m.temporality == nil)
                assert(m.monotonic == nil)
                return event
            end
            "#,
        );
        w.process(metric_event(MetricKind::Gauge(3.25))).unwrap();
    }

    #[test]
    fn gauge_delta_metric_value_reads_and_write_is_read_only() {
        let w = worker(
            r#"
            function process(event)
                local m = event.metrics[1]
                assert(m.kind == "gauge_delta")
                assert(m.value == -1.5)
                return event
            end
            "#,
        );
        w.process(metric_event(MetricKind::GaugeDelta(-1.5))).unwrap();

        let w = worker(
            r#"
            function process(event)
                event.metrics[1].value = 1.0
                return event
            end
            "#,
        );
        let err = process_err(&w, metric_event(MetricKind::GaugeDelta(-1.5)));
        assert!(err.contains("read-only on a gauge_delta metric"), "{err}");
    }

    #[test]
    fn samples_metric_fields_read_correctly() {
        let w = worker(
            r#"
            function process(event)
                local m = event.metrics[1]
                assert(m.kind == "samples")
                assert(#m.values == 3)
                assert(m.values[1] == 1.0 and m.values[2] == 2.0 and m.values[3] == 3.0)
                assert(m.sample_rate == 0.5)
                assert(m.value == nil)
                return event
            end
            "#,
        );
        w.process(metric_event(samples_kind())).unwrap();
    }

    #[test]
    fn distribution_metric_count_field_and_quantile_method() {
        let w = worker(
            r#"
            function process(event)
                local m = event.metrics[1]
                assert(m.kind == "distribution")
                assert(m.count == 3)
                local q = m:quantile(0.5)
                assert(type(q) == "number", tostring(q))
                assert(m.value == nil)
                return event
            end
            "#,
        );
        w.process(metric_event(distribution_kind())).unwrap();
    }

    /// `:quantile` answers `nil` on a non-distribution kind, and `count` is a field, not a method.
    #[test]
    fn quantile_method_returns_nil_for_a_non_distribution_kind() {
        let w = worker(
            r#"
            function process(event)
                local m = event.metrics[1]
                assert(m:quantile(0.5) == nil)
                assert(m.count == nil)
                return event
            end
            "#,
        );
        w.process(metric_event(sum_kind())).unwrap();
    }

    /// A count reads the way a `U64` attribute does: an integer up to 2^53, a decimal string past
    /// it, never a rounded number. Covers the `MetricProxy` fields and `to_table()` alike.
    #[test]
    fn a_metric_count_above_two_to_the_53_reads_as_a_decimal_string() {
        let w = worker(
            r#"
            function process(event)
                local m = event.metrics[1]
                assert(m.count == "9007199254740993", tostring(m.count))
                assert(m.zero_count == 9007199254740992, tostring(m.zero_count))
                assert(m.positive.counts[1] == "18446744073709551615")
                assert(m.positive.counts[2] == 1)
                local t = event:to_table().metrics[1]
                assert(t.count == "9007199254740993", tostring(t.count))
                assert(t.zero_count == 9007199254740992, tostring(t.zero_count))
                assert(t.positive.counts[1] == "18446744073709551615")
                return event
            end
            "#,
        );
        let kind = MetricKind::ExponentialHistogram(ExpHistogram {
            scale: 0,
            zero_count: 1 << 53,
            zero_threshold: 0.0,
            positive: (0, vec![u64::MAX, 1]),
            negative: (0, vec![]),
            temporality: Temporality::Delta,
            count: (1 << 53) + 1,
            sum: None,
            min: None,
            max: None,
        });
        w.process(metric_event(kind)).unwrap();

        let histogram = worker(
            r#"
            function process(event)
                assert(event.metrics[1].buckets[1].count == "9223372036854775808")
                assert(event:to_table().metrics[1].buckets[1].count == "9223372036854775808")
                return event
            end
            "#,
        );
        let kind = MetricKind::Histogram(Histogram {
            buckets: vec![(f64::INFINITY, 1 << 63)],
            temporality: Temporality::Cumulative,
            sum: None,
            min: None,
            max: None,
        });
        histogram.process(metric_event(kind)).unwrap();
    }

    #[test]
    fn set_members_metric_fields_read_correctly() {
        let w = worker(
            r#"
            function process(event)
                local m = event.metrics[1]
                assert(m.kind == "set_members")
                assert(#m.members == 2)
                assert(m.members[1] == "alice")
                assert(m.members[2] == "bob")
                return event
            end
            "#,
        );
        w.process(metric_event(set_members_kind())).unwrap();
    }

    #[test]
    fn set_metric_estimate_reads_correctly() {
        let w = worker(
            r#"
            function process(event)
                local m = event.metrics[1]
                assert(m.kind == "set")
                assert(m.estimate == 2)
                return event
            end
            "#,
        );
        w.process(metric_event(set_kind())).unwrap();
    }

    #[test]
    fn histogram_metric_fields_read_correctly() {
        let w = worker(
            r#"
            function process(event)
                local m = event.metrics[1]
                assert(m.kind == "histogram")
                assert(#m.buckets == 2)
                assert(m.buckets[1].bound == 1.0 and m.buckets[1].count == 3)
                assert(m.buckets[2].bound == 5.0 and m.buckets[2].count == 7)
                assert(m.temporality == "cumulative")
                assert(m.sum == 15.0)
                assert(m.min == 0.5)
                assert(m.max == 9.0)
                assert(m.count == nil)
                return event
            end
            "#,
        );
        w.process(metric_event(histogram_kind())).unwrap();
    }

    #[test]
    fn exponential_histogram_metric_fields_read_correctly() {
        let w = worker(
            r#"
            function process(event)
                local m = event.metrics[1]
                assert(m.kind == "exponential_histogram")
                assert(m.scale == 3)
                assert(m.zero_count == 2)
                assert(m.zero_threshold == 0.001)
                assert(m.positive.offset == 1)
                assert(#m.positive.counts == 3 and m.positive.counts[1] == 1)
                assert(m.negative.offset == 0)
                assert(#m.negative.counts == 2 and m.negative.counts[2] == 5)
                assert(m.temporality == "delta")
                assert(m.count == 11)
                assert(m.sum == 20.0)
                assert(m.min == -1.0)
                assert(m.max == 9.5)
                return event
            end
            "#,
        );
        w.process(metric_event(exp_histogram_kind())).unwrap();
    }

    #[test]
    fn summary_metric_fields_read_correctly() {
        let w = worker(
            r#"
            function process(event)
                local m = event.metrics[1]
                assert(m.kind == "summary")
                assert(#m.quantiles == 2)
                assert(m.quantiles[1].quantile == 0.5 and m.quantiles[1].value == 10.0)
                assert(m.quantiles[2].quantile == 0.99 and m.quantiles[2].value == 99.0)
                assert(m.count == 42)
                assert(m.sum == 500.0)
                return event
            end
            "#,
        );
        w.process(metric_event(summary_kind())).unwrap();
    }

    // -- event.metrics: writes -------------------------------------------------------------------

    #[test]
    fn sum_value_temporality_and_monotonic_write_round_trip() {
        let w = worker(
            r#"
            function process(event)
                local m = event.metrics[1]
                m.value = 99.0
                m.temporality = "delta"
                m.monotonic = true
                return event
            end
            "#,
        );
        let out = emitted(w.process(metric_event(sum_kind())).unwrap());
        match &out.metrics[0].kind {
            MetricKind::Sum(s) => {
                assert_eq!(s.value, 99.0);
                assert_eq!(s.temporality, Temporality::Delta);
                assert!(s.monotonic);
            }
            other => panic!("expected Sum, got {other:?}"),
        }
    }

    #[test]
    fn gauge_value_write_round_trips() {
        let w = worker(
            r#"
            function process(event)
                event.metrics[1].value = 7.5
                return event
            end
            "#,
        );
        let out = emitted(w.process(metric_event(MetricKind::Gauge(1.0))).unwrap());
        assert_eq!(out.metrics[0].kind, MetricKind::Gauge(7.5));
    }

    #[test]
    fn value_write_on_a_non_sum_gauge_kind_errors_naming_the_kind() {
        let w = worker(
            r#"
            function process(event)
                event.metrics[1].value = 1.0
                return event
            end
            "#,
        );
        let err = process_err(&w, metric_event(histogram_kind()));
        assert!(err.contains("read-only on a histogram metric"), "{err}");
    }

    #[test]
    fn value_write_rejects_a_non_finite_number() {
        let w = worker(
            r#"
            function process(event)
                event.metrics[1].value = 0/0
                return event
            end
            "#,
        );
        let err = process_err(&w, metric_event(sum_kind()));
        assert!(err.contains("finite"), "{err}");
    }

    #[test]
    fn ro_field_write_errors_naming_the_kind() {
        let w = worker(
            r#"
            function process(event)
                event.metrics[1].buckets = {}
                return event
            end
            "#,
        );
        let err = process_err(&w, metric_event(sum_kind()));
        assert!(err.contains("event.metrics[1].buckets is read-only on a sum metric"), "{err}");
    }

    #[test]
    fn kind_write_errors() {
        let w = worker(
            r#"
            function process(event)
                event.metrics[1].kind = "gauge"
                return event
            end
            "#,
        );
        let err = process_err(&w, metric_event(sum_kind()));
        assert!(err.contains("event.metrics[1].kind is read-only on a sum metric"), "{err}");
    }

    #[test]
    fn unknown_metric_field_write_errors() {
        let w = worker(
            r#"
            function process(event)
                event.metrics[1].bogus = 1
                return event
            end
            "#,
        );
        let err = process_err(&w, metric_event(sum_kind()));
        assert!(err.contains("event.metrics[1] has no field 'bogus'"), "{err}");
    }

    #[test]
    fn name_unit_description_start_timestamp_write_round_trip() {
        let w = worker(
            r#"
            function process(event)
                local m = event.metrics[1]
                m.name = "renamed.metric"
                m.unit = "s"
                m.description = nil
                m.start_timestamp = "42"
                return event
            end
            "#,
        );
        let out = emitted(w.process(metric_event(sum_kind())).unwrap());
        let record = &out.metrics[0];
        assert_eq!(resolve(record.name), "renamed.metric");
        assert_eq!(record.unit.map(resolve), Some("s"));
        assert_eq!(record.description, None);
        assert_eq!(record.start_timestamp, 42);
    }

    // -- event.span -------------------------------------------------------------------------------

    #[test]
    fn span_fields_read_correctly() {
        let w = worker(
            r#"
            function process(event)
                local s = event.span
                assert(s.trace_id == string.rep("01", 16))
                assert(s.span_id == string.rep("02", 8))
                assert(s.parent_span_id == string.rep("03", 8))
                assert(s.name == "GET /")
                assert(s.kind == "server")
                assert(s.status == "error")
                assert(s.status_message == "boom")
                assert(s.trace_state == "vendor=xyz")
                assert(s.end_timestamp == "1700000000900000000")
                assert(s.flags == 1)
                assert(s.dropped_attributes_count == 3)
                assert(s.dropped_events_count == 4)
                assert(s.dropped_links_count == 5)
                assert(#s.events == 1)
                assert(s.events[1].name == "exception")
                assert(s.events[1].timestamp == "1700000000100000000")
                assert(s.events[1].attributes.type == "Timeout")
                assert(s.events[1].dropped_attributes_count == 2)
                assert(#s.links == 1)
                assert(s.links[1].trace_id == string.rep("04", 16))
                assert(s.links[1].span_id == string.rep("05", 8))
                assert(s.links[1].trace_state == "vendor=value")
                assert(s.links[1].flags == 1)
                assert(s.links[1].dropped_attributes_count == 1)
                assert(s.links[1].attributes.linked == true)
                return event
            end
            "#,
        );
        w.process(span_event_full()).unwrap();
    }

    #[test]
    fn span_ext_none_defaults() {
        let w = worker(
            r#"
            function process(event)
                local s = event.span
                assert(s.parent_span_id == nil)
                assert(s.status_message == nil)
                assert(s.trace_state == nil)
                assert(s.dropped_attributes_count == 0)
                assert(s.dropped_events_count == 0)
                assert(s.dropped_links_count == 0)
                assert(#s.events == 0)
                assert(#s.links == 0)
                assert(s.kind == "internal")
                assert(s.status == "unset")
                return event
            end
            "#,
        );
        w.process(minimal_span_event()).unwrap();
    }

    #[test]
    fn span_is_nil_and_has_span_is_false_when_absent() {
        let w = worker(
            r#"
            function process(event)
                event.attributes.has_span = event.has_span
                event.attributes.span_is_nil = event.span == nil
                return event
            end
            "#,
        );
        let out = emitted(w.process(Event::empty(0, AttrMap::new())).unwrap());
        assert_eq!(out.attributes.get("has_span"), Some(&Value::Bool(false)));
        assert_eq!(out.attributes.get("span_is_nil"), Some(&Value::Bool(true)));
    }

    #[test]
    fn span_write_errors_read_only() {
        let w = worker(
            r#"
            function process(event)
                event.span.name = "renamed"
                return event
            end
            "#,
        );
        let err = process_err(&w, span_event_full());
        assert!(err.contains("event.span is read-only"), "{err}");
    }

    // -- to_table() and teardown ----------------------------------------------------------------

    #[test]
    fn to_table_includes_metrics_and_span_sections() {
        let w = worker(
            r#"
            function process(event)
                local t = event:to_table()
                event.attributes.log_event_name = t.log.event_name
                event.attributes.metrics_len = #t.metrics
                event.attributes.metric_kind = t.metrics[1].kind
                event.attributes.metric_value = t.metrics[1].value
                event.attributes.has_span = t.span ~= nil
                event.attributes.span_name = t.span.name
                event.attributes.span_events_len = #t.span.events
                event.attributes.span_links_len = #t.span.links
                return event
            end
            "#,
        );
        let mut event = log_event();
        event.metrics.push(metric_record(sum_kind()));
        event.span = Some(span_record_with_everything());
        let out = emitted(w.process(event).unwrap());
        assert_eq!(
            out.attributes.get("log_event_name").and_then(|v| v.as_str()),
            Some("request.completed")
        );
        assert_eq!(out.attributes.get("metrics_len"), Some(&Value::I64(1)));
        assert_eq!(out.attributes.get("metric_kind").and_then(|v| v.as_str()), Some("sum"));
        assert_eq!(out.attributes.get("metric_value"), Some(&Value::F64(12.5)));
        assert_eq!(out.attributes.get("has_span"), Some(&Value::Bool(true)));
        assert_eq!(out.attributes.get("span_name").and_then(|v| v.as_str()), Some("GET /"));
        assert_eq!(out.attributes.get("span_events_len"), Some(&Value::I64(1)));
        assert_eq!(out.attributes.get("span_links_len"), Some(&Value::I64(1)));
    }

    /// Reading through `event.metrics[1]` and `event.span` leaves the returned event identical,
    /// through `into_inner`'s cache teardown.
    #[test]
    fn reading_metric_and_span_fields_leaves_the_event_unchanged() {
        let w = worker(
            r#"
            function process(event)
                local _ = event.metrics[1].value
                local _ = event.span.name
                return event
            end
            "#,
        );
        let mut input = log_event();
        input.metrics.push(metric_record(sum_kind()));
        input.span = Some(span_record_with_everything());
        let expected = input.clone();
        let out = emitted(w.process(input).unwrap());
        assert_eq!(out, expected);
    }

    // -- MetricProxy holds a Weak, not an Rc ------------------------------------------------------

    /// A live `MetricProxy` doesn't raise the event's strong count. Built directly, bypassing
    /// `MetricsProxy`, whose own cached `Rc` would confound the count before `into_inner`.
    #[test]
    fn reading_a_metric_field_keeps_into_inner_on_the_no_clone_path() {
        let lua = Lua::new();
        let event_proxy = EventProxy::new(metric_event(sum_kind()));
        let metric_ud = lua
            .create_userdata(MetricProxy { event: Rc::downgrade(&event_proxy.event), index: 0 })
            .unwrap();
        lua.globals().set("m", metric_ud).unwrap();
        let value: f64 = lua.load("return m.value").eval().unwrap();
        assert_eq!(value, 12.5);

        assert_eq!(
            event_proxy.strong_count(),
            1,
            "a MetricProxy minted for a metric read must never hold a strong Rc onto the event"
        );
        let _ = event_proxy.into_inner(&lua); // must not panic
    }

    /// A stashed `event.metrics[i]` used after its event is returned gets the "already returned"
    /// error, not a resurrected copy.
    #[test]
    fn a_stashed_metric_handle_errors_after_the_event_is_returned() {
        let w = worker(
            r#"
            function process(event)
                stashed = event.metrics[1]
                return event
            end

            function flush()
                stashed.value = 99
                return {}
            end
            "#,
        );
        emitted(w.process(metric_event(sum_kind())).unwrap());
        let err = match w.flush(0) {
            Err(err) => err.to_string(),
            Ok(_) => panic!("expected flush() to reject the stashed metric handle"),
        };
        assert!(
            err.contains(
                "event.metrics[1] belongs to an event that has already been returned from \
                 process() or included in a flush() table -- stash event:clone() instead"
            ),
            "{err}"
        );
    }
}
