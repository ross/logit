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
//!
//! A sink that sends one batch as several requests can also remember, per armed batch, which
//! requests the destination settled ([`BatchAccounting::settle`]), so a retry resends only the
//! rest. The memory has the gate's lifetime: `observe` clears it, a final send disarms it, and
//! unarmed nothing is settled, so a caller that never calls `observe_batch` resends every
//! request. `otlp_out` keys it by signal (`crate::otlp`, "One `send`, several requests").
//!
//! A send is final when it returns `Ok` or a fault `write_loop` won't retry under the sink's
//! posture ([`logit_pipeline::is_retryable`]). Only a final send disarms: a retryable failure
//! leaves the batch armed for the retry, which is what lets it skip what was settled. The posture
//! is `at_least_once` until the sink forwards `Output::observe_posture` through
//! [`BatchAccounting::observe_posture`], so a sink that doesn't stays armed after an `Ambiguous`
//! failure, and a direct `send` of another batch after it, with no `observe_batch`, runs muted.

use logit_core::CountGate;
use logit_pipeline::DeliveryPosture;

/// The per-batch state behind a sink's [`CountGate`]. An encode unit is a bit index below 32: `0`
/// for a sink that encodes a batch in one piece. A request index has no bound.
#[derive(Debug)]
pub(crate) struct BatchAccounting {
    gate: CountGate,
    /// `write_loop`'s posture, which decides whether a failed send is final.
    posture: DeliveryPosture,
    /// Set by [`BatchAccounting::observe`], cleared by [`BatchAccounting::delivered`]. Unarmed,
    /// the gate never mutes.
    armed: bool,
    /// Units already encoded for the armed batch.
    counted: u32,
    /// Requests of the armed batch the destination accepted; a request's index is the sink's own
    /// numbering, separate from the encode units.
    accepted: RequestBits,
    /// Requests of the armed batch the destination rejected; their records are counted dropped,
    /// and a resend would be rejected again.
    rejected: RequestBits,
}

impl Default for BatchAccounting {
    fn default() -> Self {
        Self {
            gate: CountGate::default(),
            posture: DeliveryPosture::AtLeastOnce,
            armed: false,
            counted: 0,
            accepted: RequestBits::default(),
            rejected: RequestBits::default(),
        }
    }
}

/// A growable set of request indexes. A `datadog_out` route cut by body size has no bound on its
/// request count, so a fixed-width word can't hold them.
#[derive(Debug, Default)]
struct RequestBits(Vec<u64>);

impl RequestBits {
    fn set(&mut self, index: u32) {
        let (word, bit) = Self::locate(index);
        if self.0.len() <= word {
            self.0.resize(word + 1, 0);
        }
        self.0[word] |= bit;
    }

    fn get(&self, index: u32) -> bool {
        let (word, bit) = Self::locate(index);
        self.0.get(word).is_some_and(|w| w & bit != 0)
    }

    fn any(&self) -> bool {
        self.0.iter().any(|&w| w != 0)
    }

    /// Empties the set, keeping its allocation for the next batch.
    fn clear(&mut self) {
        self.0.clear();
    }

    fn locate(index: u32) -> (usize, u64) {
        ((index / u64::BITS) as usize, 1 << (index % u64::BITS))
    }
}

impl BatchAccounting {
    /// The gate a sink builds its encoder's handles over.
    pub(crate) fn gate(&self) -> &CountGate {
        &self.gate
    }

    /// Records `write_loop`'s posture: `Output::observe_posture`.
    pub(crate) fn observe_posture(&mut self, posture: DeliveryPosture) {
        self.posture = posture;
    }

    /// Arms the gate for a new batch, with no unit counted: `Output::observe_batch`.
    pub(crate) fn observe(&mut self) {
        self.armed = true;
        self.counted = 0;
        self.accepted.clear();
        self.rejected.clear();
    }

    /// Records the destination's verdict on `request` of the armed batch: accepted, or rejected.
    /// Unarmed, it records nothing.
    pub(crate) fn settle(&mut self, request: u32, accepted: bool) {
        if self.armed {
            let bits = if accepted { &mut self.accepted } else { &mut self.rejected };
            bits.set(request);
        }
    }

    /// Whether the destination already settled `request` of the armed batch, so a retry skips
    /// it.
    pub(crate) fn settled(&self, request: u32) -> bool {
        self.armed && (self.accepted.get(request) || self.rejected.get(request))
    }

    /// Whether the destination accepted any request of the armed batch on an earlier attempt.
    pub(crate) fn any_accepted(&self) -> bool {
        self.armed && self.accepted.any()
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

    /// Disarms after a final send, so a later `send` with no `observe_batch` counts.
    fn delivered(&mut self) {
        self.armed = false;
    }

    /// Whether `result` ends the batch: `Ok`, or a fault `write_loop` won't retry under the
    /// recorded posture.
    pub(crate) fn is_final<T>(&self, result: &anyhow::Result<T>) -> bool {
        match result {
            Ok(_) => true,
            Err(err) => !logit_pipeline::is_retryable(logit_pipeline::classify(err), self.posture),
        }
    }

    /// Passes `result` through, disarming when it's final ([`BatchAccounting::is_final`]). Each
    /// sink's `send` returns through this at its one exit, so no early return leaves the
    /// accounting armed.
    pub(crate) fn finish<T>(&mut self, result: anyhow::Result<T>) -> anyhow::Result<T> {
        if self.is_final(&result) {
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
    fn settled_requests_are_remembered_until_the_next_batch() {
        let mut accounting = BatchAccounting::default();
        accounting.observe();
        accounting.settle(0, true);
        accounting.settle(2, false);
        assert!(accounting.settled(0) && accounting.settled(2) && !accounting.settled(1));
        assert!(accounting.any_accepted());
        accounting.observe();
        assert!(!accounting.settled(0) && !accounting.settled(2), "observe clears the verdicts");
        assert!(!accounting.any_accepted());
    }

    /// A request index past one 64-bit word is remembered, and `observe` clears it.
    #[test]
    fn a_request_past_the_first_word_is_settled_and_cleared() {
        let mut accounting = BatchAccounting::default();
        accounting.observe();
        accounting.settle(70, false);
        assert!(accounting.settled(70));
        assert!(!accounting.settled(6) && !accounting.settled(69) && !accounting.settled(200));
        assert!(!accounting.any_accepted(), "a rejection is not an acceptance");
        accounting.settle(130, true);
        assert!(accounting.settled(130) && accounting.any_accepted());
        accounting.observe();
        assert!(!accounting.settled(70) && !accounting.settled(130));
        assert!(!accounting.any_accepted());
    }

    #[test]
    fn unarmed_or_after_delivery_nothing_is_settled() {
        let mut accounting = BatchAccounting::default();
        accounting.settle(0, true);
        assert!(!accounting.settled(0) && !accounting.any_accepted(), "unarmed records nothing");
        accounting.observe();
        accounting.settle(1, true);
        accounting.delivered();
        assert!(!accounting.settled(1) && !accounting.any_accepted(), "an Ok send disarms");
    }

    fn failed(fault: logit_pipeline::Fault) -> anyhow::Result<()> {
        Err(anyhow::anyhow!("send failed").context(fault))
    }

    /// `finish` disarms on `Ok` and on a fault `write_loop` won't retry under the posture, and
    /// leaves the batch armed for a retry otherwise.
    #[test]
    fn finish_disarms_on_a_final_verdict_only() {
        use logit_pipeline::Fault::*;
        use DeliveryPosture::*;
        let cases = [
            (AtLeastOnce, Clean, false),
            (AtLeastOnce, Refused, false),
            (AtLeastOnce, Ambiguous, false),
            (AtLeastOnce, Rejected, true),
            (AtMostOnce, Clean, false),
            (AtMostOnce, Refused, false),
            (AtMostOnce, Ambiguous, true),
            (AtMostOnce, Rejected, true),
        ];
        for (posture, fault, disarms) in cases {
            let mut accounting = BatchAccounting::default();
            accounting.observe_posture(posture);
            accounting.observe();
            accounting.settle(0, true);
            assert!(accounting.finish(failed(fault)).is_err());
            assert_eq!(accounting.settled(0), !disarms, "{posture:?}, {fault}");
        }
        let mut accounting = BatchAccounting::default();
        accounting.observe();
        accounting.settle(0, true);
        assert!(accounting.finish(Ok(())).is_ok());
        assert!(!accounting.settled(0), "an Ok disarms");
    }

    /// With no posture observed, `Ambiguous` is retryable, so the batch stays armed.
    #[test]
    fn the_posture_is_at_least_once_until_observed() {
        let mut accounting = BatchAccounting::default();
        accounting.observe();
        accounting.settle(0, true);
        let _ = accounting.finish(failed(logit_pipeline::Fault::Ambiguous));
        assert!(accounting.settled(0));
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
