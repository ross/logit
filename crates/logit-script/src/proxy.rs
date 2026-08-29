//! The `Event` <-> Lua boundary: [`EventProxy`] (the whole event) and `AttrsProxy` (its
//! `attributes` sub-object). Both wrap the same `Rc<RefCell<Event>>`, so mutating through either
//! handle is visible through the other -- matching Lua's own reference semantics (`local e2 =
//! event` aliases the same event, exactly as it would for a table).
//!
//! See `docs/design/lua-api.md` for why this exists instead of full table conversion, and for the
//! script-visible contract these two types implement.

use crate::value::{attrmap_to_lua_table, lua_to_value, value_to_lua};
use logit_core::{Event, Payload};
use mlua::{AnyUserData, MetaMethod, UserData, UserDataMethods, Value as LuaValue};
use std::cell::RefCell;
use std::rc::Rc;

/// Wraps one [`Event`] for the duration of a `process()`/`flush()` call -- and possibly longer, if
/// a script stashes it in a global or upvalue.
///
/// **Contract: an event handle is consumed once it's returned from `process()` or included in a
/// `flush()` table.** Don't keep using a Lua variable referencing an event after handing it back
/// that way -- it stops working (see [`take_event`]'s doc comment for exactly why: a Lua userdata
/// is a reference type, so a stashed alias and the returned value can be the *same* underlying
/// box, and extracting one invalidates the other). If a script genuinely needs to both emit an
/// event now and keep something for later (e.g. a stateful `flush()` re-emitting it), stash
/// `event:clone()` -- an independent copy -- rather than `event` itself.
pub struct EventProxy(Rc<RefCell<Event>>);

impl EventProxy {
    pub fn new(event: Event) -> Self {
        Self(Rc::new(RefCell::new(event)))
    }

    /// Unwraps back to an owned `Event`. Cheap (no clone) in the ordinary case -- see
    /// `ScriptWorker::process`'s use of [`AnyUserData::take`], which is what makes this the
    /// *only* remaining reference by the time a script returns its event unchanged. Falls back to
    /// cloning the inner event if something else still holds a reference; correctness over
    /// performance in what should be a rare case, and this must never panic either way.
    pub fn into_inner(self) -> Event {
        match Rc::try_unwrap(self.0) {
            Ok(cell) => cell.into_inner(),
            Err(rc) => rc.borrow().clone(),
        }
    }

    fn type_name(&self) -> &'static str {
        match self.0.borrow().payload {
            Payload::Log(_) => "log",
            Payload::Metric(_) => "metric",
            Payload::Span(_) => "span",
        }
    }
}

impl UserData for EventProxy {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_meta_method(MetaMethod::Index, |lua, this, key: String| match key.as_str() {
            // A string, not a Lua number: Lua's only numeric type is an IEEE-754 double, safely
            // exact only up to 2^53 (~9e15). A unix-nanos timestamp is routinely ~1.7e18 --
            // empirically confirmed to silently round-trip wrong as a Lua number (verified with a
            // real script: reads back as "1.7e+18", and even an unmodified read-then-write loses
            // precision). A decimal-digit string is exact and unambiguous; a script that wants to
            // do real arithmetic on it can `tonumber()` at whatever precision it actually needs.
            "timestamp" => {
                Ok(LuaValue::String(lua.create_string(this.0.borrow().timestamp.to_string())?))
            }
            "attributes" => {
                Ok(LuaValue::UserData(lua.create_userdata(AttrsProxy(this.0.clone()))?))
            }
            "type" => Ok(LuaValue::String(lua.create_string(this.type_name())?)),
            _ => Ok(LuaValue::Nil),
        });

        methods.add_meta_method(
            MetaMethod::NewIndex,
            |_, this, (key, value): (String, LuaValue)| match key.as_str() {
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
                    this.0.borrow_mut().timestamp = ts;
                    Ok(())
                }
                "attributes" | "type" => {
                    Err(mlua::Error::RuntimeError(format!("event.{key} is read-only")))
                }
                other => Err(mlua::Error::RuntimeError(format!("event has no field '{other}'"))),
            },
        );

        // An independent deep copy, for fan-out: `return {a, b}` needs a second event distinct
        // from the first, and there's no `Event.new(...)` constructor yet (docs/adr and the
        // v0.1-lua-engine PR both call this out as a deliberate follow-up, not an oversight).
        methods.add_method("clone", |_, this, ()| Ok(EventProxy::new(this.0.borrow().clone())));

        // The escape hatch: a real Lua table, disconnected from the live event, for anything the
        // proxy doesn't expose directly -- including iterating all attributes, since `__pairs`
        // isn't available under LuaJIT (see docs/design/lua-api.md). Deliberately not exhaustive
        // over payload fields; see the v0.1-lua-engine PR description for why.
        methods.add_method("to_table", |lua, this, ()| {
            let event = this.0.borrow();
            let table = lua.create_table()?;
            table.set("timestamp", event.timestamp.to_string())?; // string -- see __index above
            table.set("attributes", attrmap_to_lua_table(lua, &event.attributes)?)?;
            table.set("type", this.type_name())?;
            Ok(table)
        });
    }
}

/// The `event.attributes` sub-object. Shares the same `Rc<RefCell<Event>>` as its parent
/// [`EventProxy`] -- reads/writes through this proxy are reads/writes to that same event.
struct AttrsProxy(Rc<RefCell<Event>>);

impl UserData for AttrsProxy {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_meta_method(MetaMethod::Index, |lua, this, key: String| {
            match this.0.borrow().attributes.get(&key) {
                Some(value) => value_to_lua(lua, value),
                None => Ok(LuaValue::Nil),
            }
        });

        methods.add_meta_method(
            MetaMethod::NewIndex,
            |_, this, (key, value): (String, LuaValue)| {
                let value = lua_to_value(value)?;
                this.0.borrow_mut().attributes.insert(&key, value);
                Ok(())
            },
        );

        // No __pairs: not available under LuaJIT/Lua 5.1 (mlua's MetaMethod::Pairs requires Lua
        // 5.2+). A script that needs to enumerate every attribute uses
        // `event:to_table().attributes` and native `pairs()` on that real table instead.
    }
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
pub(crate) fn take_event(ud: AnyUserData) -> mlua::Result<Event> {
    match ud.take::<EventProxy>() {
        Ok(proxy) => Ok(proxy.into_inner()),
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
