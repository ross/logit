//! Process-wide interning for attribute/metric keys.
//!
//! Keys repeat enormously across telemetry (`host`, `env`, `service.name`, ...). Interning them
//! once means `AttrMap` compares and hashes `u32`s instead of repeated string allocations, and the
//! same table backs the wire format's dictionary encoding (see `docs/design/wire-protocol.md`).

use lasso::{Spur, ThreadedRodeo};
use std::sync::OnceLock;

/// An interned string. Cheap to copy, compare, and hash.
pub type Symbol = Spur;

static INTERNER: OnceLock<ThreadedRodeo> = OnceLock::new();

fn interner() -> &'static ThreadedRodeo {
    INTERNER.get_or_init(ThreadedRodeo::new)
}

/// Intern a key/value string, returning its `Symbol`. Safe to call concurrently from any worker.
pub fn intern(s: &str) -> Symbol {
    interner().get_or_intern(s)
}

/// Resolve a `Symbol` back to its string. Panics if the symbol was not produced by [`intern`] --
/// symbols are only ever created by this module, so this indicates a bug, not bad input.
pub fn resolve(sym: Symbol) -> &'static str {
    interner().resolve(&sym)
}

/// Look up a string's `Symbol` *without* interning it. Returns `None` if the string was never
/// interned, and -- unlike [`intern`] -- a miss never adds it to the table. Since interning is
/// monotonic and process-global, `None` here means the string cannot possibly be the key of
/// anything already built from a `Symbol` (an `AttrMap` entry, a `SeriesKey`, ...), so a caller
/// that only wants to test membership can skip `intern` entirely. Use this instead of `intern`
/// whenever the string might not exist and creating it on a miss would be wasted work -- e.g.
/// `AttrMap::get`/`remove` on a key the map turns out not to have.
pub fn lookup(s: &str) -> Option<Symbol> {
    interner().get(s)
}

/// Count of distinct strings interned so far, process-wide. Never decreases -- `ThreadedRodeo`
/// never evicts (see `docs/design/memory.md` §4) -- so this is an observability hook for that
/// growth, not a live size. Cheap enough to expose now even with nothing wired up to read it yet.
pub fn len() -> usize {
    interner().len()
}
