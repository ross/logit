//! Once-per-batch accounting for a sink's encode-side counters (ADR
//! `sink-send-path-and-attempt-accounting`, decisions 1 and 2).
//!
//! The runtime calls `Output::send` once per attempt, and every sink re-encodes on each one. A
//! sink holds a [`BatchAccounting`], hands its encoder handles built with
//! `Telemetry::gated`/`Diagnostics::gated` over [`BatchAccounting::gate`], and runs each encode
//! through [`BatchAccounting::encode`]. The gate is muted only during an encode that repeats one an
//! earlier attempt at the same batch already counted, and only once `Output::observe_batch` armed
//! it, so a caller that never calls `observe_batch` sees every encode counted. The encode is a
//! synchronous closure, so nothing awaits while the gate is muted.
//!
//! A sink skips the encode-side counts it emits itself when `encode` reports a repeat, and keeps
//! ungated handles for its transport counters and for drops a kernel or peer verdict decides.

use logit_core::CountGate;

/// The per-batch state behind a sink's [`CountGate`]. A unit is a bit index below 32: `0` for a
/// sink that encodes a batch in one piece.
#[derive(Debug, Default)]
pub(crate) struct BatchAccounting {
    gate: CountGate,
    /// Set by [`BatchAccounting::observe`], cleared by [`BatchAccounting::delivered`]. Unarmed,
    /// the gate never mutes.
    armed: bool,
    /// Units already encoded for the armed batch.
    counted: u32,
}

impl BatchAccounting {
    /// The gate a sink builds its encoder's handles over.
    pub(crate) fn gate(&self) -> &CountGate {
        &self.gate
    }

    /// Arms the gate for a new batch, with no unit counted: `Output::observe_batch`.
    pub(crate) fn observe(&mut self) {
        self.armed = true;
        self.counted = 0;
    }

    /// Runs one encode of `unit` with the gate muted when it repeats, and returns whether it was
    /// the batch's first encode of `unit` with the encode's result.
    pub(crate) fn encode<R>(&mut self, unit: u32, encode: impl FnOnce() -> R) -> (bool, R) {
        let first = self.begin(unit);
        let result = encode();
        self.end(unit);
        (first, result)
    }

    /// Mutes the gate when `unit` was already encoded for the armed batch; returns whether this is
    /// its first encode.
    fn begin(&mut self, unit: u32) -> bool {
        debug_assert!(unit < u32::BITS, "unit {unit} is past the {}-unit bitset", u32::BITS);
        let first = !self.armed || self.counted & (1 << unit) == 0;
        self.gate.set_muted(!first);
        first
    }

    /// Marks `unit` encoded when armed, and unmutes.
    fn end(&mut self, unit: u32) {
        if self.armed {
            self.counted |= 1 << unit;
        }
        self.gate.set_muted(false);
    }

    /// Disarms after an `Ok` send, so a later `send` with no `observe_batch` counts.
    fn delivered(&mut self) {
        self.armed = false;
    }

    /// Passes `result` through, disarming on `Ok`. Each sink's `send` returns through this at its
    /// one exit, so no early `Ok` leaves the accounting armed.
    pub(crate) fn finish<T>(&mut self, result: anyhow::Result<T>) -> anyhow::Result<T> {
        if result.is_ok() {
            self.delivered();
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs one encode of `unit` and returns whether it was first and whether the gate was muted
    /// during it.
    fn encode(accounting: &mut BatchAccounting, unit: u32) -> (bool, bool) {
        let gate = accounting.gate().clone();
        let (first, muted) = accounting.encode(unit, || gate.is_muted());
        assert!(!accounting.gate().is_muted(), "the gate is open again after every encode");
        (first, muted)
    }

    #[test]
    fn an_unarmed_gate_never_mutes() {
        let mut accounting = BatchAccounting::default();
        for _ in 0..3 {
            assert_eq!(encode(&mut accounting, 0), (true, false));
        }
    }

    #[test]
    fn armed_the_first_encode_of_a_unit_is_live_and_a_repeat_is_muted() {
        let mut accounting = BatchAccounting::default();
        accounting.observe();
        assert_eq!(encode(&mut accounting, 0), (true, false));
        assert_eq!(encode(&mut accounting, 0), (false, true));
        assert_eq!(encode(&mut accounting, 0), (false, true));
    }

    #[test]
    fn observe_starts_a_new_batch_with_nothing_counted() {
        let mut accounting = BatchAccounting::default();
        accounting.observe();
        encode(&mut accounting, 0);
        encode(&mut accounting, 3);
        accounting.observe();
        assert_eq!(encode(&mut accounting, 0), (true, false));
        assert_eq!(encode(&mut accounting, 3), (true, false));
    }

    #[test]
    fn delivered_disarms_so_a_later_encode_counts() {
        let mut accounting = BatchAccounting::default();
        accounting.observe();
        encode(&mut accounting, 0);
        accounting.delivered();
        assert_eq!(encode(&mut accounting, 0), (true, false));
        assert_eq!(encode(&mut accounting, 0), (true, false), "unarmed, nothing is remembered");
    }

    #[test]
    fn distinct_units_are_independent() {
        let mut accounting = BatchAccounting::default();
        accounting.observe();
        assert_eq!(encode(&mut accounting, 1), (true, false));
        assert_eq!(encode(&mut accounting, 2), (true, false));
        assert_eq!(encode(&mut accounting, 1), (false, true));
        assert_eq!(encode(&mut accounting, 31), (true, false));
        assert_eq!(encode(&mut accounting, 2), (false, true));
    }
}
