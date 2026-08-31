//! The `Event` <-> Lua boundary: [`EventProxy`] (the whole event) and `AttrsProxy` (its
//! `attributes` sub-object). Both wrap the same `Rc<RefCell<Event>>`, so mutating through either
//! handle is visible through the other -- matching Lua's own reference semantics (`local e2 =
//! event` aliases the same event, exactly as it would for a table).
//!
//! See `docs/design/lua-api.md` for why this exists instead of full table conversion, and for the
//! script-visible contract these two types implement.

use crate::value::{attrmap_to_lua_table, lua_to_value, lua_value_matches, value_to_lua};
use logit_core::Event;
use mlua::{
    AnyUserData, Lua, MetaMethod, RegistryKey, UserData, UserDataMethods, Value as LuaValue,
};
use std::cell::RefCell;
use std::rc::Rc;

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
}

impl EventProxy {
    pub fn new(event: Event) -> Self {
        Self { event: Rc::new(RefCell::new(event)), attrs: RefCell::new(None) }
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
        match Rc::try_unwrap(self.event) {
            Ok(cell) => cell.into_inner(),
            Err(rc) => rc.borrow().clone(),
        }
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
                // Presence flags, not a classification string: an event can carry a log, several
                // metrics, and a span all at once now, so "what type is this event" has no single
                // right answer (docs/adr/0012-multi-payload-events.md) -- a script or native
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
                    "attributes" | "has_log" | "has_metrics" | "has_span" => {
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
            table.set("has_log", event.log.is_some())?;
            table.set("has_metrics", !event.metrics.is_empty())?;
            table.set("has_span", event.span.is_some())?;
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
            "this event, or its attributes (event.attributes), was already returned/emitted \
             elsewhere and can no longer be used -- an event handle, and the attributes handle \
             obtained from event.attributes, are both consumed once the event is returned from \
             process() or included in a flush() table; use event:clone() before returning if you \
             need to keep using either afterward"
                .to_string(),
        );
    }
    err
}
