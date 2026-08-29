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
