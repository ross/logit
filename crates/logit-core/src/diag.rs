//! A deliberately small diagnostics helper -- not a `tracing` migration (that stays separate,
//! out-of-scope work; see `docs/known-gaps.md`). Gives every stderr diagnostic a consistent
//! `component 'id': msg` prefix, and a way to bound how many times a high-volume, low-value
//! message repeats. See `docs/adr/0013-service-lifecycle-and-output-retry.md`.

use std::collections::HashMap;
use std::fmt::Display;

/// Attached to a component (an input, transform, or output) via that component's
/// `with_diagnostics` builder -- mirrors the existing `with_timeout`/`with_retry` idiom rather
/// than changing any constructor. A component that never gets one keeps the placeholder id from
/// [`Diagnostics::default`], which is still safe to print -- just not attributable to a specific
/// running instance.
#[derive(Debug, Clone)]
pub struct Diagnostics {
    component_id: String,
    /// Per-key occurrence counts for [`Diagnostics::warn_throttled`]. Independent keys never
    /// interfere with each other's throttling -- a component with two distinct failure modes
    /// (e.g. `json`'s "no brace found" and "parse failed") reports each on its own cadence.
    counts: HashMap<&'static str, u64>,
}

impl Default for Diagnostics {
    fn default() -> Self {
        Self::new("<unnamed>")
    }
}

impl Diagnostics {
    pub fn new(component_id: impl Into<String>) -> Self {
        Self { component_id: component_id.into(), counts: HashMap::new() }
    }

    /// Reports unconditionally, prefixed with this component's id. For a diagnostic that can fire
    /// once per event under normal operation (a malformed line, a parse failure), prefer
    /// [`Diagnostics::warn_throttled`] instead -- this is for genuinely rare events, like a single
    /// retry attempt failing.
    pub fn warn(&self, msg: impl Display) {
        eprintln!("component '{}': {msg}", self.component_id);
    }

    /// Reports the 1st, 2nd, 4th, 8th, ... occurrence of `key` (each naming the running total) and
    /// suppresses the rest -- bounded stderr volume under a flood of the same complaint (e.g. one
    /// malformed line per request) without needing a clock. A time-window limiter was the more
    /// obvious shape, but no component here has a clock injected, and threading one through purely
    /// for diagnostics would make an otherwise-deterministic interface non-deterministic to test
    /// (see ADR 0013's Alternatives). Returns whether this call actually reported, so a test can
    /// assert the throttling directly rather than capturing stderr.
    pub fn warn_throttled(&mut self, key: &'static str, msg: impl Display) -> bool {
        let count = self.counts.entry(key).or_insert(0);
        *count += 1;
        let count = *count; // copied out so `self.counts`'s borrow ends before `self.warn` below
        let should_report = count.is_power_of_two();
        if should_report {
            self.warn(format_args!("{msg} (x{count}, further occurrences suppressed)"));
        }
        should_report
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warn_throttled_reports_at_powers_of_two_and_suppresses_the_rest() {
        let mut diag = Diagnostics::new("test");
        let fired: Vec<bool> = (1..=10).map(|_| diag.warn_throttled("k", "x")).collect();
        assert_eq!(
            fired,
            vec![true, true, false, true, false, false, false, true, false, false],
            "should report on occurrences 1, 2, 4, 8 and suppress every other one"
        );
    }

    #[test]
    fn distinct_keys_throttle_independently() {
        let mut diag = Diagnostics::new("test");
        assert!(diag.warn_throttled("a", "x"), "a's 1st occurrence reports");
        assert!(diag.warn_throttled("a", "x"), "a's 2nd occurrence reports (a power of two)");
        assert!(!diag.warn_throttled("a", "x"), "a's 3rd occurrence is suppressed");
        // b's own count starts fresh at 1, unaffected by a already being at 3 -- if the two keys
        // shared one counter, this would be b's 4th-overall call and would also report, hiding
        // the bug this test exists to catch.
        assert!(
            diag.warn_throttled("b", "y"),
            "b's 1st occurrence reports regardless of a's count"
        );
    }
}
