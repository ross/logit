//! A progress counter a Lua node's thread writes and its watcher task reads, so the watcher can
//! tell a script that is working from one that has stopped making progress
//! (`docs/adr/lua-runaway-script-bounds.md`).
//!
//! One `u64`: bit 0 is set while the thread is inside a script call, and the upper bits count
//! progress (each call entered, each `Event.new`, each element of a returned table). The watcher
//! compares successive reads: busy and unchanged for long enough is a stall.
//!
//! There is one writer, the Lua thread, so every update is a `Relaxed` load then store, never a
//! read-modify-write. The watcher needs no ordering against other memory: it only asks whether
//! the value moved.

use std::sync::atomic::{AtomicU64, Ordering};

/// The busy bit plus a progress count. See the module doc.
#[derive(Debug, Default)]
pub struct Heartbeat(AtomicU64);

impl Heartbeat {
    const BUSY: u64 = 1;
    /// One step of the progress count, which lives above the busy bit.
    const STEP: u64 = 2;

    pub fn new() -> Self {
        Self::default()
    }

    /// Marks the thread busy and counts one step. Calling it while already busy still advances.
    pub fn enter(&self) {
        let v = self.0.load(Ordering::Relaxed);
        self.0.store((v | Self::BUSY).wrapping_add(Self::STEP), Ordering::Relaxed);
    }

    /// Counts one step without changing the busy bit.
    pub fn tick(&self) {
        let v = self.0.load(Ordering::Relaxed);
        self.0.store(v.wrapping_add(Self::STEP), Ordering::Relaxed);
    }

    /// Clears the busy bit. The value still changes (when it was busy), which the watcher reads
    /// as progress.
    pub fn leave(&self) {
        let v = self.0.load(Ordering::Relaxed);
        self.0.store(v & !Self::BUSY, Ordering::Relaxed);
    }

    /// The current value, for the watcher.
    pub fn read(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    /// Whether `value` (a [`Heartbeat::read`]) has the busy bit set.
    pub fn is_busy(value: u64) -> bool {
        value & Self::BUSY != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enter_tick_leave_keep_the_busy_bit_and_advance() {
        let hb = Heartbeat::new();
        assert_eq!(hb.read(), 0);
        assert!(!Heartbeat::is_busy(hb.read()));

        hb.enter();
        let entered = hb.read();
        assert!(Heartbeat::is_busy(entered), "enter() sets the busy bit");
        assert_eq!(entered, 3);

        hb.tick();
        let ticked = hb.read();
        assert!(Heartbeat::is_busy(ticked), "tick() keeps the busy bit");
        assert!(ticked > entered, "tick() advances the count");

        hb.enter();
        let reentered = hb.read();
        assert!(Heartbeat::is_busy(reentered));
        assert!(reentered > ticked, "enter() while busy still advances");

        hb.leave();
        let left = hb.read();
        assert!(!Heartbeat::is_busy(left), "leave() clears the busy bit");
        assert_ne!(left, reentered, "leaving is itself a change the watcher sees");
        assert_eq!(left >> 1, reentered >> 1, "leave() leaves the count alone");

        hb.tick();
        assert!(!Heartbeat::is_busy(hb.read()), "tick() while idle stays idle");
    }
}
