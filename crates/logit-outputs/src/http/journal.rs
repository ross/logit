//! [`Journal`]: what a batch's requests did to a multi-request sink's telemetry, diagnostics, and
//! settled-request memory, recorded where the requests ran and applied on the loop task.
//!
//! `otlp_out`, `datadog_out`, `datadog_trace_out`, and `splunk_hec_out` send one batch as several
//! requests, and count and diagnose each answer as it arrives. Above a concurrency of 1 those
//! requests run in a spawned task ([`super::Slots`]), which can't reach the sink's `&mut`
//! diagnostics or the slot's accounting, so the task records each effect here in order and
//! `await_ack` replays them ([`Journal::replay`]). A sink's `send` runs the same task code inline
//! and replays its journal before returning, so both paths count the same points in the same
//! order.
//!
//! A journal dropped before it's replayed applies its counts and timings to the telemetry handle
//! it holds. `write_loop` drops an attempt still running at the shutdown grace's end, the `send`
//! future or the slot's task, and the requests already answered still count. Its warnings and
//! settled requests need the loop task and are skipped: the cut batch is a shutdown drop.

use crate::accounting::BatchAccounting;
use logit_core::{Diagnostics, Tag, Telemetry};
use std::fmt::Display;
use std::time::Duration;

/// At most this many tags per point; every multi-request sink's points carry two or fewer.
const MAX_TAGS: usize = 2;

#[derive(Debug, Clone, Copy)]
struct Tags {
    tags: [Tag; MAX_TAGS],
    len: usize,
}

impl Tags {
    fn new(tags: &[Tag]) -> Self {
        assert!(tags.len() <= MAX_TAGS, "a journaled point carries at most {MAX_TAGS} tags");
        let mut out = Self { tags: [("", ""); MAX_TAGS], len: tags.len() };
        out.tags[..tags.len()].copy_from_slice(tags);
        out
    }

    fn as_slice(&self) -> &[Tag] {
        &self.tags[..self.len]
    }
}

#[derive(Debug)]
enum Effect {
    Count { name: &'static str, n: f64, tags: Tags },
    Timing { name: &'static str, took: Duration, tags: Tags },
    Warn { key: &'static str, message: String },
    Settle { request: u32, accepted: bool },
}

/// The effects of one attempt's requests, in the order they happened, and the sink's ungated
/// telemetry handle they count on (the module doc).
#[derive(Debug)]
pub(crate) struct Journal {
    effects: Vec<Effect>,
    telemetry: Telemetry,
}

impl Journal {
    /// An empty journal whose counts and timings land on `telemetry`.
    pub(crate) fn new(telemetry: Telemetry) -> Self {
        Self { effects: Vec::new(), telemetry }
    }

    /// `Telemetry::count`, deferred.
    pub(crate) fn count(&mut self, name: &'static str, n: f64, tags: &[Tag]) {
        self.effects.push(Effect::Count { name, n, tags: Tags::new(tags) });
    }

    /// `Telemetry::timing`, deferred.
    pub(crate) fn timing(&mut self, name: &'static str, took: Duration, tags: &[Tag]) {
        self.effects.push(Effect::Timing { name, took, tags: Tags::new(tags) });
    }

    /// `Diagnostics::warn_throttled`, deferred. The message is formatted now, whether or not the
    /// throttle reports it.
    pub(crate) fn warn(&mut self, key: &'static str, message: impl Display) {
        self.effects.push(Effect::Warn { key, message: message.to_string() });
    }

    /// `BatchAccounting::settle`, deferred.
    pub(crate) fn settle(&mut self, request: u32, accepted: bool) {
        self.effects.push(Effect::Settle { request, accepted });
    }

    /// Applies every effect in order: counts and timings to the journal's telemetry, warnings to
    /// `diag`, and the destination's verdicts to `accounting`.
    pub(crate) fn replay(mut self, diag: &mut Diagnostics, accounting: &mut BatchAccounting) {
        for effect in std::mem::take(&mut self.effects) {
            match effect {
                Effect::Warn { key, message } => {
                    diag.warn_throttled(key, message);
                }
                Effect::Settle { request, accepted } => accounting.settle(request, accepted),
                point => self.apply(point),
            }
        }
    }

    fn apply(&self, effect: Effect) {
        match effect {
            Effect::Count { name, n, tags } => self.telemetry.count(name, n, tags.as_slice()),
            Effect::Timing { name, took, tags } => {
                self.telemetry.timing(name, took, tags.as_slice());
            }
            Effect::Warn { .. } | Effect::Settle { .. } => {}
        }
    }
}

/// A journal never replayed, its attempt cut mid-flight: its counts and timings still land (the
/// module doc).
impl Drop for Journal {
    fn drop(&mut self) {
        for effect in std::mem::take(&mut self.effects) {
            self.apply(effect);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_pipeline::test_util::TelemetryProbe;

    #[test]
    fn replay_applies_every_effect_in_order() {
        let mut probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("out", "test", "sink");
        let mut diag = Diagnostics::new("out").with_telemetry(telemetry.clone());
        let mut accounting = BatchAccounting::default();
        accounting.observe();
        let mut journal = Journal::new(telemetry.clone());
        journal.count("a", 1.0, &[("route", "x")]);
        journal.count("a", 2.0, &[("route", "x")]);
        journal.timing("t", Duration::from_millis(5), &[]);
        journal.warn("key", "message");
        journal.settle(3, true);
        journal.replay(&mut diag, &mut accounting);
        let totals = probe.poll();
        assert_eq!(totals.sum("a", &[("route", "x")]), 3.0, "counted once, not again on drop");
        assert_eq!(totals.sum("logit.component.diagnostics", &[("key", "key")]), 1.0);
        assert!(accounting.settled(3) && accounting.any_accepted());
    }

    /// A journal dropped unreplayed lands its counts, and nothing else.
    #[test]
    fn a_dropped_journal_lands_its_counts() {
        let mut probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("out", "test", "sink");
        let mut journal = Journal::new(telemetry);
        journal.count("a", 2.0, &[]);
        journal.warn("key", "message");
        drop(journal);
        let totals = probe.poll();
        assert_eq!(totals.sum("a", &[]), 2.0);
        assert_eq!(totals.sum("logit.component.diagnostics", &[("key", "key")]), 0.0);
    }
}
