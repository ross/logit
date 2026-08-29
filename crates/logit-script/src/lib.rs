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
use mlua::Lua;
use std::marker::PhantomData;

mod proxy;

pub use proxy::EventProxy;

#[derive(Debug, thiserror::Error)]
pub enum ScriptError {
    #[error("lua error: {0}")]
    Lua(#[from] mlua::Error),
    #[error("script has no `process` function")]
    MissingProcess,
}

/// Owns one Lua VM and the compiled `process`/`flush` functions for one pipeline stage, on one
/// worker. Not `Send`/`Sync` (via the `PhantomData<*const ()>` marker) -- see the module docs.
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
    /// Load a script's source, sandboxed per `docs/design/lua-api.md` (no `io`, `os.execute`, or
    /// arbitrary `require` -- exact stdlib allowlist is an implementation-time decision, not a
    /// design one).
    pub fn new(_source: &str) -> Result<Self, ScriptError> {
        let lua = Lua::new();
        // TODO: restrict the standard library surface before this leaves stub form.
        Ok(Self { lua, _not_send_sync: PhantomData })
    }

    /// Run this worker's `process(event)` once. See `docs/design/lua-api.md` for the
    /// proxy-vs-table-conversion tradeoff `EventProxy` exists to avoid.
    pub fn process(&self, _event: Event) -> Result<ProcessOutcome, ScriptError> {
        let _ = &self.lua;
        todo!("wire EventProxy through mlua's UserData and call the script's `process`")
    }

    /// Run this worker's `flush()`, if the script defines one (the stateful-processor contract,
    /// e.g. the built-in `aggregate` transform).
    pub fn flush(&self) -> Result<Vec<Event>, ScriptError> {
        todo!("invoke `flush()` if present; return an empty Vec if the script has none")
    }
}
