//! The `Event` <-> Lua boundary: [`EventProxy`] (the whole event) and its typed sub-proxies --
//! `AttrsProxy` (`event.attributes`, an open map), `LogProxy` (`event.log`, a fixed field set,
//! mostly read-write), `MetricsProxy`/`MetricProxy` (`event.metrics`, an indexable array of
//! per-kind records, read-write only on the handful of fields a script can legitimately mutate
//! in place -- a metric's `value` on `sum`/`gauge`, plus `sum`'s own `temporality`/`monotonic`),
//! and `SpanProxy` (`event.span`, entirely read-only -- there is no script-visible way to build or
//! mutate a span, only to construct one via `logit_config::ComponentKind::TraceContext`'s `span:`
//! block or read one that already exists). All of these share the same `Rc<RefCell<Event>>` as
//! their parent [`EventProxy`] -- mutating through any of them is visible through every other,
//! matching Lua's own reference semantics (`local e2 = event` aliases the same event, exactly as
//! it would for a table).
//!
//! See `docs/design/lua-api.md` for why this exists instead of full table conversion, and for the
//! script-visible contract these types implement.

use crate::value::{attrmap_to_lua_table, lua_to_value, lua_value_matches, value_to_lua};
use logit_core::interner::{intern, resolve};
use logit_core::trace::{parse_span_id, parse_trace_id, to_hex};
use logit_core::{
    BodyFormat, Event, Exemplar, LogRecord, MetricKind, MetricRecord, Severity, SpanEvent,
    SpanKind, SpanLink, SpanRecord, SpanStatus, Temporality, TraceRef,
};
use mlua::{
    AnyUserData, Lua, MetaMethod, RegistryKey, Table, UserData, UserDataMethods, Value as LuaValue,
};
use std::cell::RefCell;
use std::rc::{Rc, Weak};

/// Wraps one [`Event`] for the duration of a `process()`/`flush()` call -- and possibly longer, if
/// a script stashes it in a global or upvalue.
///
/// **Contract: an event handle -- and its `event.attributes` handle -- is consumed once the event
/// is returned from `process()` or included in a `flush()` table.** Don't keep using a Lua
/// variable referencing either after handing the event back that way -- both stop working (see
/// [`take_event`]'s doc comment for exactly why: a Lua userdata is a reference type, so a stashed
/// alias and the returned value can be the *same* underlying box, and extracting one invalidates
/// the other; `event.attributes` is cached per event -- see the `attrs` field below -- so the same
/// is true of a `local a = event.attributes` stashed alongside it). Touching either past that
/// point fails clearly, via [`clarify_destructed_handle_use`], rather than with mlua's generic
/// destructed-userdata wording. If a script genuinely needs to both emit an event now and keep
/// something for later (e.g. a stateful `flush()` re-emitting it), stash `event:clone()` -- an
/// independent copy -- rather than `event` (or `event.attributes`) itself.
pub struct EventProxy {
    event: Rc<RefCell<Event>>,
    /// The `event.attributes` sub-proxy, created lazily on the first access and cached rather
    /// than rebuilt on every later one (`docs/design/memory.md` §8's "cache the `AttrsProxy`
    /// userdata" recommendation) -- a script that reads and writes attributes on the same event
    /// used to pay a fresh `create_userdata` call (a real allocation) per access.
    ///
    /// Stored as a `RegistryKey`, not the `AnyUserData` handle itself: `AnyUserData<'lua>`
    /// carries a `'lua` lifetime tied to a specific borrow of the `Lua` instance, and `EventProxy`
    /// -- like every `UserData` type -- must be `'static` to be storable as userdata at all, so
    /// there is no field type that could hold the handle directly. A `RegistryKey` is `mlua`'s
    /// `'static` answer to exactly this: redeemable via `Lua::registry_value` whenever a `&Lua`
    /// is back in scope (the same reason `ScriptWorker` caches `process`/`flush` this way -- see
    /// lib.rs).
    ///
    /// mlua's own docs warn that a `RegistryKey` stored inside a `UserData` type is an easy way
    /// to leak: the registry is a GC root, so the referenced `AttrsProxy` would stay alive forever
    /// once cached, independent of whether this `EventProxy` itself is still reachable, unless
    /// something removes it explicitly. [`into_inner`](EventProxy::into_inner) is that explicit
    /// removal, for the path that matters: a script that returns (or emits) its event is the
    /// overwhelmingly common case, and that's exactly when this cache must be torn down anyway,
    /// both to avoid the leak and -- more importantly here -- to keep `into_inner`'s
    /// `Rc::try_unwrap` fast path working (see that method's doc comment). A script that drops an
    /// event after touching its attributes without ever returning it (no `into_inner` call at
    /// all) leaves the registry entry for `Lua`'s own reclaiming -- `RegistryKey::drop` queues its
    /// slot for reuse, and this worker creates new registry entries constantly, so the slot doesn't
    /// sit unreclaimed for long. A deliberate, bounded trade against the complexity of covering
    /// that path too: forcing cleanup there risks invalidating a script that legitimately stashed
    /// the event (or its attributes) for `flush()` to use later, the same pattern this module's
    /// docs already call out as supported for `event` itself.
    attrs: RefCell<Option<RegistryKey>>,
    /// `event.log`'s sub-proxy, cached the same way and for the same reason as `attrs` above.
    /// Only ever populated when `event.log.is_some()` -- nothing in this crate's Lua surface can
    /// clear a log once an event has one (only fields *within* it, via [`LogProxy`]), so "cached
    /// once" and "log is present" stay equivalent for this event's whole lifetime.
    log: RefCell<Option<RegistryKey>>,
    /// `event.metrics`'s sub-proxy ([`MetricsProxy`]), cached the same way as `attrs` -- and, like
    /// `attrs`, created lazily on first access regardless of whether the metric list is empty
    /// (unlike `log`/`span`, there's no `is_some()` gate: an empty `MetricsProxy` is still a valid
    /// handle, `#event.metrics == 0`).
    metrics: RefCell<Option<RegistryKey>>,
    /// `event.span`'s sub-proxy ([`SpanProxy`]), cached and gated on `event.span.is_some()` the
    /// same way `log` is above.
    span: RefCell<Option<RegistryKey>>,
}

impl EventProxy {
    pub fn new(event: Event) -> Self {
        Self {
            event: Rc::new(RefCell::new(event)),
            attrs: RefCell::new(None),
            log: RefCell::new(None),
            metrics: RefCell::new(None),
            span: RefCell::new(None),
        }
    }

    /// Returns this event's `AttrsProxy` userdata, creating and caching it on the first call and
    /// simply handing back the same handle on every later one -- see the field doc comment above.
    fn attrs_userdata<'lua>(&self, lua: &'lua Lua) -> mlua::Result<AnyUserData<'lua>> {
        if let Some(key) = self.attrs.borrow().as_ref() {
            return lua.registry_value(key);
        }
        let ud = lua.create_userdata(AttrsProxy(self.event.clone()))?;
        *self.attrs.borrow_mut() = Some(lua.create_registry_value(ud.clone())?);
        Ok(ud)
    }

    /// As [`Self::attrs_userdata`], for `event.log` -- caller must only invoke this when
    /// `event.log.is_some()` (the `"log"` `__index` arm checks first).
    fn log_userdata<'lua>(&self, lua: &'lua Lua) -> mlua::Result<AnyUserData<'lua>> {
        if let Some(key) = self.log.borrow().as_ref() {
            return lua.registry_value(key);
        }
        let ud = lua.create_userdata(LogProxy(self.event.clone()))?;
        *self.log.borrow_mut() = Some(lua.create_registry_value(ud.clone())?);
        Ok(ud)
    }

    /// As [`Self::attrs_userdata`], for `event.metrics` -- unlike `log_userdata`, has no
    /// `is_some()` precondition: an empty metric list still gets a (cheap, empty-backed)
    /// `MetricsProxy`.
    fn metrics_userdata<'lua>(&self, lua: &'lua Lua) -> mlua::Result<AnyUserData<'lua>> {
        if let Some(key) = self.metrics.borrow().as_ref() {
            return lua.registry_value(key);
        }
        let ud = lua.create_userdata(MetricsProxy(self.event.clone()))?;
        *self.metrics.borrow_mut() = Some(lua.create_registry_value(ud.clone())?);
        Ok(ud)
    }

    /// As [`Self::log_userdata`], for `event.span` -- caller must only invoke this when
    /// `event.span.is_some()` (the `"span"` `__index` arm checks first).
    fn span_userdata<'lua>(&self, lua: &'lua Lua) -> mlua::Result<AnyUserData<'lua>> {
        if let Some(key) = self.span.borrow().as_ref() {
            return lua.registry_value(key);
        }
        let ud = lua.create_userdata(SpanProxy(self.event.clone()))?;
        *self.span.borrow_mut() = Some(lua.create_registry_value(ud.clone())?);
        Ok(ud)
    }

    /// Unwraps back to an owned `Event`. Cheap (no clone) in the ordinary case -- see
    /// `ScriptWorker::process`'s use of [`AnyUserData::take`], which is what makes this the
    /// *only* remaining reference by the time a script returns its event unchanged. Falls back to
    /// cloning the inner event if something else still holds a reference; correctness over
    /// performance in what should be a rare case, and this must never panic either way.
    ///
    /// Needs `&Lua` (unlike the version of this method before `attrs` existed) to release the
    /// cached `AttrsProxy` first: that release must happen, and must happen *before* the
    /// `Rc::try_unwrap` below, or every script that ever reads `event.attributes` -- which is
    /// nearly all of them -- would permanently defeat this fast path, paying a full `Event` clone
    /// on every call instead of the rare fallback this was always meant to be. Releasing it means
    /// synchronously emptying the cached userdata's box via `take`, the same tool [`take_event`]
    /// uses on the `EventProxy` itself -- Lua's GC would get there eventually, but "eventually"
    /// isn't deterministic enough to depend on here.
    pub fn into_inner(self, lua: &Lua) -> Event {
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
        match Rc::try_unwrap(self.event) {
            Ok(cell) => cell.into_inner(),
            Err(rc) => rc.borrow().clone(),
        }
    }

    /// Test-only window onto the strong-count `into_inner`'s `Rc::try_unwrap` above lives and
    /// dies by: a `MetricProxy` minted for a script's `event.metrics[i]` read must never nudge
    /// this above 1 (it holds a `Weak`, not an `Rc` -- see `MetricProxy`'s own doc comment for
    /// why that's load-bearing), regardless of whether LuaJIT has gotten around to collecting the
    /// userdata that read produced by the time this is checked.
    #[cfg(test)]
    fn strong_count(&self) -> usize {
        Rc::strong_count(&self.event)
    }
}

impl UserData for EventProxy {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_meta_method(MetaMethod::Index, |lua, this, key: mlua::String| {
            match key.to_str()? {
                // A string, not a Lua number: Lua's only numeric type is an IEEE-754 double,
                // safely exact only up to 2^53 (~9e15). A unix-nanos timestamp is routinely
                // ~1.7e18 -- empirically confirmed to silently round-trip wrong as a Lua number
                // (verified with a real script: reads back as "1.7e+18", and even an unmodified
                // read-then-write loses precision). A decimal-digit string is exact and
                // unambiguous; a script that wants to do real arithmetic on it can `tonumber()`
                // at whatever precision it actually needs.
                "timestamp" => Ok(LuaValue::String(
                    lua.create_string(this.event.borrow().timestamp.to_string())?,
                )),
                "attributes" => Ok(LuaValue::UserData(this.attrs_userdata(lua)?)),
                // Typed access to the log record -- trace_id/span_id/trace_flags read+write,
                // message/severity/body_format read-only for now (`docs/design/lua-api.md`).
                // `nil` when the event has no log, exactly like `has_log` says it should.
                "log" => match this.event.borrow().log.is_some() {
                    true => Ok(LuaValue::UserData(this.log_userdata(lua)?)),
                    false => Ok(LuaValue::Nil),
                },
                // Always present, even for an empty metric list -- see `metrics_userdata`'s doc
                // comment.
                "metrics" => Ok(LuaValue::UserData(this.metrics_userdata(lua)?)),
                "span" => match this.event.borrow().span.is_some() {
                    true => Ok(LuaValue::UserData(this.span_userdata(lua)?)),
                    false => Ok(LuaValue::Nil),
                },
                // Presence flags, not a classification string: an event can carry a log, several
                // metrics, and a span all at once now, so "what type is this event" has no single
                // right answer (docs/adr/multi-payload-events.md) -- a script or native
                // component checks the specific thing it cares about instead. There is
                // deliberately no `event.type` any more: a single summary label would be lossy at
                // best and a silent footgun at worst (a script branching on `event.type ==
                // "metric"` would skip the metrics on a log-carrying event, exactly the shape
                // `kv_metrics` produces).
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
                        // Must be a string, for the same precision reason __index returns one --
                        // accepting a Lua number here would silently accept an already-corrupted
                        // value rather than catching the mistake.
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

        // An independent deep copy, for fan-out: `return {a, b}` needs a second event distinct
        // from the first, and there's no `Event.new(...)` constructor yet (docs/adr and the
        // v0.1-lua-engine PR both call this out as a deliberate follow-up, not an oversight).
        methods.add_method("clone", |_, this, ()| Ok(EventProxy::new(this.event.borrow().clone())));

        // The escape hatch: a real Lua table, disconnected from the live event, for anything the
        // proxy doesn't expose directly -- including iterating all attributes, since `__pairs`
        // isn't available under LuaJIT (see docs/design/lua-api.md). Deliberately not exhaustive
        // over payload fields; see the v0.1-lua-engine PR description for why.
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

/// The `event.attributes` sub-object. Shares the same `Rc<RefCell<Event>>` as its parent
/// [`EventProxy`] -- reads/writes through this proxy are reads/writes to that same event.
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
                // A no-op assignment must stay a no-op: if `value` is byte-for-byte what
                // value_to_lua would have handed the script for this attribute's current
                // content, leave the stored Value untouched -- so its variant (e.g. Bytes vs.
                // Str, U64 vs. I64) survives an unmodified `event.attributes.x =
                // event.attributes.x` even though a plain Lua string/number can't itself carry
                // that information. See value.rs's `lua_value_matches` for the full reasoning.
                // Must run before `lua_to_value` re-borrows mutably below, and must not call
                // back into Lua while `this.0` is borrowed here -- see that function's doc
                // comment for why.
                let is_noop = this
                    .0
                    .borrow()
                    .attributes
                    .get(key)
                    .is_some_and(|existing| lua_value_matches(existing, &value));
                if is_noop {
                    return Ok(());
                }
                let value = lua_to_value(value)?;
                this.0.borrow_mut().attributes.insert(key, value);
                Ok(())
            },
        );

        // No __pairs: not available under LuaJIT/Lua 5.1 (mlua's MetaMethod::Pairs requires Lua
        // 5.2+). A script that needs to enumerate every attribute uses
        // `event:to_table().attributes` and native `pairs()` on that real table instead.
    }
}

/// The `event.log` sub-object. Shares the same `Rc<RefCell<Event>>` as its parent [`EventProxy`],
/// like [`AttrsProxy`] -- but unlike that one, exposes a fixed, typed field set rather than an
/// open map, closer in shape to `EventProxy` itself: `trace_id`/`span_id`/`trace_flags` are
/// read+write (`docs/adr/log-record-trace-context.md`); `message`/`severity`/`body_format` are
/// read-only for now (`docs/design/lua-api.md` -- a later design pass, not an oversight). Callers
/// must only construct this when `event.log.is_some()`; every method below panics via `.expect`
/// on a borrow it assumes is upheld by that precondition, which `EventProxy::log_userdata`'s only
/// call site enforces.
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
                // `nil`, not `0`, when there's no trace at all -- so a script checking
                // `event.log.trace_flags == nil` agrees with `event.log.trace_id == nil` instead
                // of a flags read silently implying a trace that isn't there.
                "trace_flags" => match log.trace {
                    Some(t) => Ok(LuaValue::Integer(t.flags as i64)),
                    None => Ok(LuaValue::Nil),
                },
                "message" => value_to_lua(lua, &log.message),
                "severity" => match log.severity {
                    Some(s) => Ok(LuaValue::String(lua.create_string(severity_name(s))?)),
                    None => Ok(LuaValue::Nil),
                },
                "body_format" => {
                    Ok(LuaValue::String(lua.create_string(body_format_name(log.body_format))?))
                }
                // OTLP's `LogRecord.event_name` -- an interned `Symbol`, like every other
                // string-shaped attribute-ish field crossing the Lua boundary.
                "event_name" => match log.event_name {
                    Some(sym) => Ok(LuaValue::String(lua.create_string(resolve(sym))?)),
                    None => Ok(LuaValue::Nil),
                },
                // A decimal-digit string, not a Lua number, for the same reason
                // `event.timestamp` is -- see that field's comment above. `0` (unset) still reads
                // as the string `"0"`, not `nil`: unlike `event.log` itself, there's no
                // "unset means absent" convention for this field.
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
                            // A changed trace_id replaces the whole TraceRef, not just the
                            // id field -- an old span_id/flags belongs to the old trace and
                            // must not be carried over onto the new one.
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

/// Shared by `LogProxy::to_table` and `EventProxy::to_table` (the latter's `log` key) -- one
/// definition of what a log record looks like as a plain table.
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
            Some(s) => LuaValue::String(lua.create_string(severity_name(s))?),
            None => LuaValue::Nil,
        },
    )?;
    table.set("body_format", body_format_name(log.body_format))?;
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

/// Lowercase severity names for Lua -- matches `crates/logit-outputs/src/stdio.rs`'s own
/// rendering of the same values, so `event.log.severity == "info"` reads the way the `stdio_out`
/// line a script is looking at already does.
fn severity_name(severity: Severity) -> &'static str {
    match severity {
        Severity::Trace => "trace",
        Severity::Debug => "debug",
        Severity::Info => "info",
        Severity::Warn => "warn",
        Severity::Error => "error",
        Severity::Fatal => "fatal",
    }
}

fn body_format_name(format: BodyFormat) -> &'static str {
    match format {
        BodyFormat::Raw => "raw",
        BodyFormat::Json => "json",
        BodyFormat::Structured => "structured",
    }
}

// -- `event.metrics` -----------------------------------------------------------------------

/// The `event.metrics` sub-object: an indexable, array-like view over the event's
/// [`logit_core::MetricList`], shared with its parent [`EventProxy`] the same way [`AttrsProxy`]
/// is. Always present once accessed, even for an empty list (`#event.metrics == 0` is a normal,
/// valid read) -- see [`EventProxy::metrics_userdata`]'s doc comment.
///
/// `#event.metrics` (`MetaMethod::Len`) and `event.metrics[i]` (`MetaMethod::Index`, 1-based) are
/// its whole surface: no `__newindex` (there is no script-visible way to add, remove, or reorder
/// metrics), and no cached per-index handle -- each `event.metrics[i]` access mints a fresh, tiny
/// [`MetricProxy`] rather than reusing one, unlike `attrs`/`log`/`span` above, which are each
/// worth caching because there's exactly one per event. A metric list is typically short (`kv_
/// metrics`' multi-metric shape is the extreme case), so a script indexing into it pays the
/// userdata mint per access -- **3 allocations, measured** (`crates/logit-bench/tests/
/// allocations.rs`'s `lua_process_one_event_reading_metric_value`, the same cost a cached proxy's
/// first access pays) -- rather than a registry slot per index held for the event's whole
/// lifetime. A per-index cache would only win for a script that re-indexes the *same* metric
/// repeatedly; `local m = event.metrics[1]` once is the idiom `docs/design/lua-api.md` shows, and
/// `docs/design/memory.md` §8 records this as the known trade.
///
/// Not caching a `MetricProxy` is only safe because it holds a [`Weak`], not an [`Rc`], onto the
/// event -- see [`MetricProxy`]'s own doc comment for why that's load-bearing, not incidental.
struct MetricsProxy(Rc<RefCell<Event>>);

impl UserData for MetricsProxy {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_meta_method(MetaMethod::Len, |_, this, ()| {
            Ok(this.0.borrow().metrics.len() as i64)
        });

        methods.add_meta_method(MetaMethod::Index, |lua, this, key: LuaValue| {
            let index = match key {
                LuaValue::Integer(i) => i,
                // LuaJIT's dual-number mode keeps small-integer arithmetic as an Integer, but a
                // computed index (`event.metrics[i + 0.0]`, say) could in principle still arrive
                // as an integral Number -- accepted the same way, anything with a fractional part
                // is not a valid index either way.
                LuaValue::Number(n) if n.fract() == 0.0 => n as i64,
                other => {
                    return Err(mlua::Error::RuntimeError(format!(
                        "event.metrics must be indexed by an integer, got {}",
                        other.type_name()
                    )))
                }
            };
            // Out of range (including `<= 0`, Lua has no negative indexing here) is `nil`, same
            // as an ordinary Lua array read past its end -- only a genuinely non-integer key is a
            // hard error (checked above).
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

/// One `event.metrics[i]` entry -- unlike every other proxy in this module, not cached: see
/// [`MetricsProxy`]'s doc comment for why. `index` is 0-based (a plain `Vec`/`SmallVec` index);
/// every user-facing message adds 1 back, to match the 1-based Lua index a script actually wrote.
///
/// **Holds a [`Weak`], not an [`Rc`], onto the event -- load-bearing, not a style choice.** Every
/// other proxy in this module (`AttrsProxy`, `LogProxy`, `SpanProxy`) is cached as a
/// [`RegistryKey`] on its parent [`EventProxy`] and explicitly `take`n in
/// [`EventProxy::into_inner`] before that method's own `Rc::try_unwrap` -- so by the time
/// `into_inner` runs, none of them still hold a strong reference. `MetricProxy` is deliberately
/// *not* cached that way (see [`MetricsProxy`]'s doc comment), so nothing tears one down on the
/// same schedule; a script reading so much as `event.metrics[1].value` mints one, and LuaJIT's GC
/// gives no guarantee it's been collected -- or even that its underlying `MetricProxy` has been
/// dropped -- by the time `process()` returns and `take_event` calls `into_inner`. An `Rc` field
/// here would silently defeat `into_inner`'s no-clone fast path on *every* script that ever reads
/// a metric field (`Rc::try_unwrap` fails whenever any other strong reference is still alive,
/// which a not-yet-collected `MetricProxy` always would be) -- paying a full `Event` clone on
/// what should be the overwhelmingly common case, exactly the cost that field exists to avoid.
/// Worse, a *stashed* `local m = event.metrics[1]` used from `flush()` after its event was
/// returned would keep that returned event's clone alive and silently mutable through `m` --
/// wrong data accepted quietly, rather than the "consumed handle" error every other stashed
/// sub-proxy in this module already gives (see [`take_event`]'s and
/// [`clarify_destructed_handle_use`]'s doc comments). A [`Weak`] fixes both: it costs nothing
/// towards `Rc::try_unwrap`'s strong-count check regardless of GC timing, so the no-clone path
/// keeps working; and `Weak::upgrade` on a `MetricProxy` outliving its event fails deterministically
/// the moment that event is torn down, giving `with_metric`/`with_metric_mut` below a clear signal
/// to raise the same "already returned" error the rest of this module's stashed-handle story
/// already tells scripts, instead of resurrecting stale data.
///
/// **Robust to a stale index**, independent of the above: nothing in today's Lua surface can
/// shrink `event.metrics` *while its event is still alive*, so `index >= event.metrics.len()`
/// can't actually happen through a script alone -- but this proxy checks for it on every access
/// anyway (`with_metric`/`with_metric_mut` below), both as cheap insurance against a future
/// surface that *can* (an eventual `event.metrics:remove(i)`, say) and because nothing about
/// `MetricProxy`'s own type forbids constructing one with a bad index by hand (as this module's
/// own tests do, directly, to exercise exactly this path).
struct MetricProxy {
    event: Weak<RefCell<Event>>,
    index: usize,
}

impl MetricProxy {
    /// Upgrades the held [`Weak`] and, if that succeeds, looks up this proxy's metric by index --
    /// the two ways a `MetricProxy` access can fail, kept distinct: an upgrade failure means the
    /// *event* this handle pointed at is gone (already returned/emitted elsewhere -- see this
    /// struct's own doc comment), while a successful upgrade with a missing index means the event
    /// is still alive but this particular metric no longer is (today, unreachable through Lua
    /// alone, but checked anyway -- same doc comment).
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

/// The event behind this handle is gone -- it was returned from `process()` or included in a
/// `flush()` table (and every other strong reference, per `MetricProxy`'s own doc comment, was
/// already gone by then too), the same "consumed handle" failure
/// [`clarify_destructed_handle_use`] gives a script for a stashed `event`/`event.attributes`/
/// `event.log`, just discovered here via a failed `Weak::upgrade` instead of mlua's destructed-
/// userdata marker (`MetricProxy` isn't registry-cached, so it was never a candidate for that
/// mechanism in the first place -- see this module's doc comment on the difference).
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

/// A write to a field a metric's *current kind* doesn't allow -- either a field that's read-only
/// for every kind (`flags`, `kind`, `exemplars`, ...), or a kind-specific one (`value`, `sum`,
/// `temporality`, ...) being written on a kind that doesn't support writing it (or doesn't carry
/// it at all). Names the kind either way, per `docs/design/lua-api.md`'s contract for this proxy
/// -- "read-only" alone wouldn't tell a script *why* (unlike an unconditionally read-only field
/// elsewhere in this module), since the same field name is legitimately writable on a different
/// kind.
fn metric_ro_error(index: usize, field: &str, kind: &MetricKind) -> mlua::Error {
    mlua::Error::RuntimeError(format!(
        "event.metrics[{}].{field} is read-only on a {} metric",
        index + 1,
        metric_kind_name(kind)
    ))
}

fn metric_kind_name(kind: &MetricKind) -> &'static str {
    match kind {
        MetricKind::Sum(_) => "sum",
        MetricKind::Gauge(_) => "gauge",
        MetricKind::GaugeDelta(_) => "gauge_delta",
        MetricKind::Samples(_) => "samples",
        MetricKind::Distribution(_) => "distribution",
        MetricKind::SetMembers(_) => "set_members",
        MetricKind::Set(_) => "set",
        MetricKind::Histogram(_) => "histogram",
        MetricKind::ExponentialHistogram(_) => "exponential_histogram",
        MetricKind::Summary(_) => "summary",
    }
}

fn temporality_name(t: Temporality) -> &'static str {
    match t {
        Temporality::Delta => "delta",
        Temporality::Cumulative => "cumulative",
    }
}

/// As [`lua_to_value`]'s string branch reasoning, but simpler: a nil-able `f64` reaches Lua as a
/// real number when present, `nil` when not -- there is no variant-identity concern here the way
/// `value.rs` has for attributes, since a metric field's shape is fixed by its kind, not
/// reconstructed from an arbitrary Lua value.
fn opt_number<'lua>(v: Option<f64>) -> LuaValue<'lua> {
    match v {
        Some(n) => LuaValue::Number(n),
        None => LuaValue::Nil,
    }
}

/// A non-finite (or non-numeric) `value` write is a clear error rather than silently storing NaN/
/// infinity into a metric a downstream sink or `aggregate` would then have to defend against.
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
        LuaValue::String(s) => match s.to_str()? {
            "delta" => Ok(Temporality::Delta),
            "cumulative" => Ok(Temporality::Cumulative),
            other => Err(mlua::Error::RuntimeError(format!(
                "event.metrics[{}].temporality must be \"delta\" or \"cumulative\", got \"{other}\"",
                index + 1
            ))),
        },
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
    table.set("attributes", attrmap_to_lua_table(lua, &exemplar.filtered_attributes)?)?;
    Ok(table)
}

/// `(offset, counts)` -- `ExpHistogram::positive`/`.negative`'s shape -- as `{offset=, counts=
/// [...]}`.
fn exp_buckets_table<'lua>(lua: &'lua Lua, bucket: &(i32, Vec<u64>)) -> mlua::Result<Table<'lua>> {
    let (offset, counts) = bucket;
    let table = lua.create_table()?;
    table.set("offset", *offset as i64)?;
    let counts_table = lua.create_table()?;
    for (i, count) in counts.iter().enumerate() {
        counts_table.set(i + 1, *count as i64)?;
    }
    table.set("counts", counts_table)?;
    Ok(table)
}

impl UserData for MetricProxy {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        // Registered via `add_method`, not handled inside the `MetaMethod::Index` closure below,
        // and deliberately the *only* thing on this proxy that is: mlua consults a type's
        // `add_method`-registered methods table before ever falling back to a custom `Index`
        // meta method, so `m.quantile` (plain field read) and `m:quantile(q)` (sugar for
        // `m.quantile(m, q)`) both resolve here regardless of what key names the `Index` closure
        // handles -- no risk of the two definitions drifting or shadowing each other the way a
        // second `"quantile"` arm down there would. `add_method` also hands the callback `&Self`
        // directly and strips the implicit receiver argument a colon call passes, so unlike the
        // hand-rolled closure this replaced, there's no `_self` parameter to thread through by
        // hand. Only meaningful for `distribution` (`DdSketch::quantile`); every other kind's
        // call returns `nil`, matching the field table's own wording for this method.
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
                "kind" => Ok(LuaValue::String(lua.create_string(metric_kind_name(&m.kind))?)),
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
                        Ok(LuaValue::String(lua.create_string(temporality_name(s.temporality))?))
                    }
                    MetricKind::Histogram(h) => {
                        Ok(LuaValue::String(lua.create_string(temporality_name(h.temporality))?))
                    }
                    MetricKind::ExponentialHistogram(e) => {
                        Ok(LuaValue::String(lua.create_string(temporality_name(e.temporality))?))
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
                    MetricKind::Set(hll) => Ok(LuaValue::Integer(hll.estimate() as i64)),
                    _ => Ok(LuaValue::Nil),
                },
                "buckets" => match &m.kind {
                    MetricKind::Histogram(h) => {
                        let t = lua.create_table()?;
                        for (i, (bound, count)) in h.buckets.iter().enumerate() {
                            let row = lua.create_table()?;
                            row.set("bound", *bound)?;
                            row.set("count", *count as i64)?;
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
                // A plain read-only integer field on every kind that carries one --
                // `exponential_histogram`, `summary`, and `distribution` (`DdSketch::count()`,
                // the sketch's own observation count) -- and `nil` everywhere else, the ordinary
                // "nil when the kind doesn't carry it" rule every other kind-specific field here
                // follows. No method role: unlike `quantile` (meaningful only via a `q` argument
                // a plain field can't carry), a count is just data, so there's no reason to make
                // scripts write `m:count()` instead of `m.count`.
                "count" => match &m.kind {
                    MetricKind::Distribution(sketch) => {
                        Ok(LuaValue::Integer(sketch.count() as i64))
                    }
                    MetricKind::ExponentialHistogram(e) => Ok(LuaValue::Integer(e.count as i64)),
                    MetricKind::Summary(s) => Ok(LuaValue::Integer(s.count as i64)),
                    _ => Ok(LuaValue::Nil),
                },
                "scale" => match &m.kind {
                    MetricKind::ExponentialHistogram(e) => Ok(LuaValue::Integer(e.scale as i64)),
                    _ => Ok(LuaValue::Nil),
                },
                "zero_count" => match &m.kind {
                    MetricKind::ExponentialHistogram(e) => {
                        Ok(LuaValue::Integer(e.zero_count as i64))
                    }
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
                // No `"quantile"` arm here -- it's registered via `add_method` above, which mlua
                // resolves before ever reaching this `Index` fallback (see that registration's
                // comment).
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

/// Shared by `EventProxy::to_table`'s `metrics` array -- one definition of what a metric record
/// looks like as a plain table, exhaustive over every kind-specific field (present only for the
/// kind that actually carries it, same "only when present" rule the field table above documents).
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
    table.set("kind", metric_kind_name(&record.kind))?;
    let exemplars = lua.create_table()?;
    for (i, e) in record.exemplars.iter().enumerate() {
        exemplars.set(i + 1, exemplar_to_table(lua, e)?)?;
    }
    table.set("exemplars", exemplars)?;

    match &record.kind {
        MetricKind::Sum(s) => {
            table.set("value", s.value)?;
            table.set("temporality", temporality_name(s.temporality))?;
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
            table.set("count", sketch.count() as i64)?;
        }
        MetricKind::SetMembers(members) => {
            let t = lua.create_table()?;
            for (i, m) in members.iter().enumerate() {
                t.set(i + 1, lua.create_string(m)?)?;
            }
            table.set("members", t)?;
        }
        MetricKind::Set(hll) => table.set("estimate", hll.estimate() as i64)?,
        MetricKind::Histogram(h) => {
            let buckets = lua.create_table()?;
            for (i, (bound, count)) in h.buckets.iter().enumerate() {
                let row = lua.create_table()?;
                row.set("bound", *bound)?;
                row.set("count", *count as i64)?;
                buckets.set(i + 1, row)?;
            }
            table.set("buckets", buckets)?;
            table.set("temporality", temporality_name(h.temporality))?;
            table.set("sum", opt_number(h.sum))?;
            table.set("min", opt_number(h.min))?;
            table.set("max", opt_number(h.max))?;
        }
        MetricKind::ExponentialHistogram(e) => {
            table.set("scale", e.scale as i64)?;
            table.set("zero_count", e.zero_count as i64)?;
            table.set("zero_threshold", e.zero_threshold)?;
            table.set("positive", exp_buckets_table(lua, &e.positive)?)?;
            table.set("negative", exp_buckets_table(lua, &e.negative)?)?;
            table.set("temporality", temporality_name(e.temporality))?;
            table.set("count", e.count as i64)?;
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
            table.set("count", s.count as i64)?;
            table.set("sum", s.sum)?;
        }
    }
    Ok(table)
}

// -- `event.span` ---------------------------------------------------------------------------

/// The `event.span` sub-object -- entirely read-only, unlike every other proxy in this module:
/// there is no script-visible way to construct or mutate a span (`docs/design/lua-api.md`'s
/// "Reading and writing `event.log`" section notes the same gap for span *construction*; this
/// proxy is the read side once one already exists, minted by `ComponentKind::TraceContext`'s
/// `span:` block or a codec that decoded one off the wire). Shares the same `Rc<RefCell<Event>>`
/// as its parent [`EventProxy`], cached and gated on `event.span.is_some()` the same way
/// [`LogProxy`] is -- see [`EventProxy::span_userdata`].
///
/// Note what's *not* here: a `SpanRecord` has no `attributes` field of its own (checked against
/// `crates/logit-core/src/span.rs` directly) -- a span-carrying event's attributes are
/// `event.attributes`, the same single attribute set every event has, not a second span-specific
/// map. So there is no `event.span.attributes` -- a script already has that data through
/// `event.attributes`.
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
                "kind" => Ok(LuaValue::String(lua.create_string(span_kind_name(span.kind))?)),
                "status" => Ok(LuaValue::String(lua.create_string(span_status_name(span.status))?)),
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

        // Unconditional, unlike every other proxy's `__newindex` -- there's no per-field split to
        // make: nothing on a span is writable from Lua.
        methods.add_meta_method(
            MetaMethod::NewIndex,
            |_, _this, (_key, _value): (mlua::String, LuaValue)| {
                Err::<(), _>(mlua::Error::RuntimeError("event.span is read-only".to_string()))
            },
        );
    }
}

fn span_kind_name(kind: SpanKind) -> &'static str {
    match kind {
        SpanKind::Internal => "internal",
        SpanKind::Server => "server",
        SpanKind::Client => "client",
        SpanKind::Producer => "producer",
        SpanKind::Consumer => "consumer",
    }
}

fn span_status_name(status: SpanStatus) -> &'static str {
    match status {
        SpanStatus::Unset => "unset",
        SpanStatus::Ok => "ok",
        SpanStatus::Error => "error",
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

/// Shared by `EventProxy::to_table`'s `span` key -- one definition of what a span record looks
/// like as a plain table, mirroring [`SpanProxy`]'s own `__index` field-for-field.
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
    table.set("kind", span_kind_name(span.kind))?;
    table.set("status", span_status_name(span.status))?;
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

/// Extracts the owned `Event` from a Lua value that should be an [`EventProxy`] userdata --
/// shared by `ScriptWorker::process`'s single-event and table-of-events return-value cases, and
/// by `flush()`'s table case.
///
/// Uses `AnyUserData::take`, not `borrow().clone()`: `take` empties the value out of the Lua
/// userdata box itself (leaving a "destructed" marker `mlua` returns a clear error for on any
/// further use), which is what makes `EventProxy::into_inner`'s `Rc::try_unwrap` fast path
/// actually fire in the ordinary case -- with `borrow().clone()`, the original argument's Lua-side
/// box would still hold its own reference for as long as Lua's GC keeps it alive, so
/// `try_unwrap` would essentially never succeed and every call would pay a full `Event` clone.
///
/// The real cost of `take`: a Lua userdata is a *reference* type, so `pending = event` doesn't
/// clone anything at the Rust level (`Rc::strong_count` stays 1 the whole time -- there is no way
/// to detect this aliasing from Rust at all) -- it makes `pending` a second Lua variable pointing
/// at the exact same underlying box. `take` empties that box, so it invalidates every alias, not
/// just the one being extracted here. A script that stashes an event in `process()` (for `flush()`
/// to pick up later) and *also* returns that same event from `process()` in the same call will
/// find the stashed alias destructed by the time `flush()` tries to use it -- see the "handles are
/// consumed once returned" note on [`EventProxy`], and use `event:clone()` for the stash if both
/// are genuinely needed.
///
/// Takes `&Lua` to hand to [`EventProxy::into_inner`], which needs it to release the cached
/// `AttrsProxy` registry entry before its own `Rc::try_unwrap` fast path.
pub(crate) fn take_event(lua: &Lua, ud: AnyUserData) -> mlua::Result<Event> {
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

/// Rewrites an error caused by a *script* touching an already-destructed `EventProxy`/`AttrsProxy`
/// handle into this crate's own clear wording -- the same "handle consumed once returned" message
/// [`take_event`] already gives for the one case it can see directly (a destructed `EventProxy`
/// it's the one taking). Passed through unchanged if it isn't that.
///
/// This exists because caching `event.attributes` (see [`EventProxy::attrs_userdata`] and
/// [`EventProxy::into_inner`]) opens the same failure class on an `AttrsProxy` handle that already
/// existed for `EventProxy` itself: a script that does `local a = event.attributes`, returns its
/// event (destructing the cached `AttrsProxy` as part of that), and then touches `a` again from
/// `flush()` now hits a destructed userdata -- exactly the stash-then-reuse mistake
/// `take_event`'s message already explains for the event handle itself, just discovered from a
/// different place.
///
/// `take_event` catches its case by matching `Err(mlua::Error::UserDataDestructed)` returned
/// directly from its own `AnyUserData::take` call -- a Rust-side operation. This case is
/// different: the destructed access happens *inside a running script* (`flush()`'s own body reads
/// or writes through `a`), so it's mlua's metamethod dispatch, not our code, that discovers the
/// problem -- Lua swaps a destructed userdata's metatable out for one whose every metamethod
/// raises `mlua::Error::CallbackDestructed` unconditionally, without ever reaching `AttrsProxy`'s
/// own `__index`/`__newindex` closures above. That error then crosses back into Rust wrapped in
/// one or more layers of `mlua::Error::CallbackError` (mlua's mechanism for propagating a Lua-side
/// error, plus a traceback, back through a `Function::call`) by the time `ScriptWorker::process`/
/// `flush` see it -- so the only place this can be caught and clarified is here, wrapping the
/// whole `process.call(...)`/`flush.call(())`, not inside any one metamethod.
///
/// `SpanProxy` fails exactly this same way for exactly this same reason: it's cached and `take`n
/// in `into_inner` just like `AttrsProxy`/`LogProxy` are, so a stashed `local s = event.span`
/// used after its event is returned hits the identical destructed-userdata path. `MetricProxy` is
/// the one handle in this module that reaches an "already returned" error *without* going through
/// this function at all -- it isn't registry-cached (see its own doc comment), so there's no
/// destructed-userdata marker for it to trip; `metric_handle_consumed_error` raises a plain
/// `RuntimeError` directly from a failed `Weak::upgrade` instead. The message below still names it
/// alongside `event.attributes`/`event.log`/`event.span`, though, since a script doesn't need to
/// know or care which internal mechanism caught the mistake -- only that stashing any handle this
/// module hands out has the same rule.
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
        AttrMap, DdSketch, ExpHistogram, Histogram, HyperLogLog, Samples, SpanExt, Sum, Summary,
        Value,
    };

    fn worker(source: &str) -> ScriptWorker {
        ScriptWorker::new(source).expect("script should load")
    }

    fn emitted(outcome: ProcessOutcome) -> Event {
        match outcome {
            ProcessOutcome::Emit(e) => *e,
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
        event.borrow_mut().metrics.clear(); // simulate a handle outliving its metric
                                            // The event itself is still alive (`event` stays in scope for the whole test), so
                                            // `Weak::upgrade` succeeds and this exercises the *other* staleness check --
                                            // `with_metric`'s index lookup -- not `metric_handle_consumed_error`.
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

    /// `:quantile` is a bound function on every kind (per its own field-table entry, "only
    /// meaningful for distribution, returns nil for other kinds"), so calling it on a `sum`
    /// doesn't error, it just answers `nil`. `count` has no method role at all -- it's a plain
    /// (and, off `distribution`/`exponential_histogram`/`summary`, absent) *field*, matching the
    /// field table's "count (ro integer; distribution, exponential_histogram, summary)" entry
    /// exactly, not a method the way `quantile` is.
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

    /// Reading `event.metrics[1].value` and `event.span.name` mints (and drops) a `MetricProxy`
    /// and reads through the cached `SpanProxy`, but touches nothing -- the event returned from
    /// `process()` must come back byte-for-byte identical, and `into_inner`'s cache teardown
    /// (`attrs`/`log`/`metrics`/`span` registry entries all released before `Rc::try_unwrap`) must
    /// not panic or corrupt anything along the way.
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

    // -- MetricProxy holds a Weak, not an Rc (PR #134 review finding 1) --------------------------

    /// Direct regression coverage for the bug the `Weak` fix closes: with `MetricProxy` holding a
    /// strong `Rc`, a script reading so much as `event.metrics[1].value` -- even without stashing
    /// anything anywhere -- would leave that temporary `MetricProxy` userdata's `Rc` clone alive
    /// on Lua's stack for as long as LuaJIT's GC hadn't gotten around to collecting it, which
    /// `EventProxy::into_inner`'s `Rc::try_unwrap` has no way to wait for.
    ///
    /// Mints its `MetricProxy` directly against `EventProxy`'s own `event` field, bypassing
    /// `event.metrics` (`MetricsProxy`) entirely -- going through the real `event.metrics[i]`
    /// surface would also populate `EventProxy`'s *own* `metrics` registry cache (correctly
    /// released inside `into_inner`, before its `Rc::try_unwrap`, same as `attrs`/`log`/`span`;
    /// unrelated to this fix), which would confound a strong-count check taken *before*
    /// `into_inner` runs. Isolating `MetricProxy` this way targets exactly the regression: does
    /// *this* proxy type hold a strong `Rc`, independent of anything else `EventProxy` caches.
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

    /// The other half of the same fix: a script that stashes `event.metrics[i]` in a global and
    /// uses it later (the same `flush()`-reuses-state idiom `docs/design/lua-api.md` documents for
    /// `event`/`event.attributes` themselves) must get the same "already returned" error those
    /// handles give, not silently read or write a resurrected copy of the metric.
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
        let err = match w.flush() {
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
