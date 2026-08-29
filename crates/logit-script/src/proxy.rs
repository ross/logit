//! The `Event` <-> Lua boundary.
//!
//! `EventProxy` is `mlua` userdata implementing `__index`/`__newindex`/`__pairs`, reading through
//! to the underlying [`Event`] lazily and copying only fields a script actually assigns to. This
//! exists specifically to avoid converting every event to a plain Lua table on every pipeline
//! stage -- see `docs/design/lua-api.md` for why that costs more than it looks like and why the
//! choice is designed-in now rather than left for later.

use logit_core::Event;

/// Wraps one [`Event`] for the duration of a single `process()` call.
pub struct EventProxy {
    event: Event,
}

impl EventProxy {
    pub fn new(event: Event) -> Self {
        Self { event }
    }

    pub fn into_inner(self) -> Event {
        self.event
    }
}

// TODO: `impl mlua::UserData for EventProxy` with `__index`/`__newindex`/`__pairs` over
// `attributes`, plus a `to_table()` method as the explicit full-copy escape hatch
// (`docs/design/lua-api.md`). Left unimplemented in this skeleton pass: it's the first thing to
// build and benchmark against plain table conversion before the API is frozen.
