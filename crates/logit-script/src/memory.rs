//! `max_memory` for one Lua VM: the post-call collection that decides a verdict, and the in-call
//! check `Event.new` makes (`docs/adr/lua-runaway-script-bounds.md`, decision 3).
//!
//! The runtime owns the verdict: after each batch and each `flush()`, a VM over the cap runs
//! [`crate::ScriptWorker::collect_until_under`], and one still over it after that fails the node.
//! One full collection is not enough on LuaJIT: each cycle halves the string table and frees a
//! finalized userdata only on the cycle after, so a VM full of transient strings needs several
//! passes to reach its live size.
//!
//! The in-call half exists because one call can run unbounded: a `flush()` or `process()` that
//! keeps building and retaining events never returns to the post-call check. The invariant it
//! holds: **once a collection inside a call leaves the VM over the cap, every later `Event.new` in
//! that call raises**, until the runtime calls [`crate::ScriptWorker::reset_memory_trip`] after the
//! call returns. The trip is sticky so a script's own `pcall(Event.new, ..)` can't swallow one
//! error and carry on constructing. The check reads a field (`lua_gc(LUA_GCCOUNT)`) on every
//! call and allocates nothing, so the `Event.new` allocation pins don't move; a collection runs
//! at most once per [`CALLS_PER_COLLECTION`] over-cap calls.
//!
//! The cap bounds the VM heap only. An event proxy costs the VM about 150 bytes while its
//! payload stays in the Rust heap, which `used_memory()` does not see.

use mlua::Lua;
use std::cell::Cell;

/// How many over-cap `Event.new` calls may pass between two in-call collections.
pub(crate) const CALLS_PER_COLLECTION: u32 = 1024;

/// The cap and the in-call trip state, shared by `Rc` between a `ScriptWorker` and its
/// `Event.new` closure, the way the heartbeat cell is.
#[derive(Debug, Default)]
pub(crate) struct MemoryCap {
    cap: Cell<Option<usize>>,
    tripped: Cell<bool>,
    calls_since_collection: Cell<u32>,
}

impl MemoryCap {
    pub(crate) fn set(&self, cap: Option<usize>) {
        self.cap.set(cap);
    }

    pub(crate) fn reset_trip(&self) {
        self.tripped.set(false);
    }

    /// `Event.new`'s check. `Ok` with no cap set, or under it.
    pub(crate) fn check(&self, lua: &Lua) -> mlua::Result<()> {
        let Some(cap) = self.cap.get() else {
            return Ok(());
        };
        if self.tripped.get() {
            return Err(over_cap(lua.used_memory(), cap));
        }
        let calls = self.calls_since_collection.get().saturating_add(1);
        if lua.used_memory() <= cap || calls < CALLS_PER_COLLECTION {
            self.calls_since_collection.set(calls);
            return Ok(());
        }
        self.calls_since_collection.set(0);
        lua.expire_registry_values();
        lua.gc_collect()?;
        let used = lua.used_memory();
        if used > cap {
            self.tripped.set(true);
            return Err(over_cap(used, cap));
        }
        Ok(())
    }
}

fn over_cap(used: usize, cap: usize) -> mlua::Error {
    mlua::Error::RuntimeError(format!("Event.new: over max_memory ({used} > {cap})"))
}

/// What [`crate::ScriptWorker::collect_until_under`] ended at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcVerdict {
    /// VM bytes in use after the last pass.
    pub used: usize,
    /// Full collections run; `0` when the VM was already at or under the cap.
    pub passes: usize,
}

#[cfg(test)]
mod tests {
    use crate::ScriptWorker;

    const MIB: usize = 1024 * 1024;

    #[test]
    fn collect_until_under_keeps_collecting_while_a_pass_frees_enough() {
        // ~100 KiB live, then a million transient strings with the collector stopped so they're
        // all still there when the verdict runs.
        let w = ScriptWorker::new(
            r#"
            collectgarbage("stop")
            live = {}
            for i = 1, 100 do live[i] = string.rep("x", 1000) .. i end
            for i = 1, 1000000 do local s = "transient" .. i end
            function process(event) return event end
            "#,
        )
        .unwrap();
        let before = w.used_memory();
        assert!(before > 16 * MIB, "the transient strings should still be counted: {before}");
        let verdict = w.collect_until_under(MIB, 8).unwrap();
        assert!(verdict.used <= MIB, "live data is ~100 KiB, got {verdict:?} from {before}");
        assert!(verdict.passes > 1, "one pass should not reach the live size: {verdict:?}");
        assert!(verdict.passes <= 8, "{verdict:?}");

        // Already under: no pass runs.
        assert_eq!(w.collect_until_under(usize::MAX, 8).unwrap().passes, 0);
    }

    #[test]
    fn collect_until_under_stops_once_a_pass_frees_little() {
        // ~8 MiB live against a 1 MiB cap: the first pass frees almost nothing, so no second runs.
        let w = ScriptWorker::new(
            r#"
            live = {}
            for i = 1, 8192 do live[i] = string.rep("y", 1000) .. i end
            function process(event) return event end
            "#,
        )
        .unwrap();
        let verdict = w.collect_until_under(MIB, 8).unwrap();
        assert!(verdict.used > MIB, "{verdict:?}");
        assert!(verdict.passes <= 2, "{verdict:?}");
    }

    /// A `flush()` that builds and keeps 200k events.
    const RETAINING_FLUSH: &str = r#"
        kept = {}
        function process(event) return event end
        function flush(now)
            for i = 1, 200000 do kept[#kept + 1] = Event.new{timestamp = "1"} end
        end
    "#;

    /// Whether one `Event.new` succeeds right now, outside any `process()`/`flush()`.
    fn event_new_succeeds(w: &ScriptWorker) -> bool {
        w.lua.load(r#"return (pcall(Event.new, {timestamp = "1"}))"#).eval().unwrap()
    }

    #[test]
    fn a_retaining_event_new_loop_trips_the_in_call_check() {
        let w = ScriptWorker::new(RETAINING_FLUSH).unwrap();
        w.set_memory_cap(Some(4 * MIB));
        let err = match w.flush(0) {
            Ok(_) => panic!("a loop retaining 200k events should trip a 4 MiB cap"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains("Event.new: over max_memory ("), "got: {err}");
        assert!(err.contains(&format!("> {})", 4 * MIB)), "got: {err}");
        // The events it kept are still live: the post-call verdict sees the VM over the cap.
        w.reset_memory_trip();
        let verdict = w.collect_until_under(4 * MIB, 8).unwrap();
        assert!(verdict.used > 4 * MIB, "{verdict:?}");
    }

    #[test]
    fn a_pcall_wrapped_event_new_loop_stays_tripped() {
        let w = ScriptWorker::new(
            r#"
            kept = {}
            function process(event) return event end
            function flush(now)
                for i = 1, 200000 do
                    local ok, e = pcall(Event.new, {timestamp = "1"})
                    if ok then kept[#kept + 1] = e end
                end
            end
            "#,
        )
        .unwrap();
        w.set_memory_cap(Some(4 * MIB));
        w.flush(0).expect("pcall swallows every error, so flush() itself succeeds");
        let kept: usize = w.lua.load("return #kept").eval().unwrap();
        assert!(kept < 200_000, "the trip should have refused most constructions, kept {kept}");
        // Without the sticky trip, 200k pcall'd constructions reach several times the cap.
        let verdict = w.collect_until_under(4 * MIB, 8).unwrap();
        assert!(verdict.used < 6 * MIB, "the VM should stay near the cap: {verdict:?}");

        // The trip outlives the call until the runtime resets it.
        assert!(!event_new_succeeds(&w), "still tripped after the call returned");
        w.reset_memory_trip();
        assert!(event_new_succeeds(&w), "a reset trip lets the next call construct again");
    }

    #[test]
    fn a_discarding_event_new_loop_never_trips() {
        // The collector is stopped so the garbage does cross the cap; only the in-call collection
        // brings it back under.
        let w = ScriptWorker::new(
            r#"
            function process(event) return event end
            function flush(now)
                collectgarbage("stop")
                for i = 1, 200000 do local e = Event.new{timestamp = "1"} end
            end
            "#,
        )
        .unwrap();
        w.set_memory_cap(Some(4 * MIB));
        w.flush(0).expect("garbage the collector frees never trips the cap");
        assert!(event_new_succeeds(&w));
    }

    #[test]
    fn with_no_cap_event_new_never_checks_memory() {
        let w = ScriptWorker::new(RETAINING_FLUSH).unwrap();
        w.flush(0).expect("with no cap set, Event.new never raises over memory");
    }
}
