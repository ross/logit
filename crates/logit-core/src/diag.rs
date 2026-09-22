//! A deliberately small diagnostics helper, now backed by `tracing` rather than a bare
//! `eprintln!` (docs/plans/operator-surface.md, ADR `tracing-for-self-logging.md`) -- the
//! `tracing` migration `docs/known-gaps.md` filed as separate, later work. Gives every
//! self-diagnostic a consistent `component`/`key` field pair, a severity, and a way to bound how
//! many times a high-volume, low-value message repeats. See
//! `docs/adr/service-lifecycle-and-output-retry.md`.

use crate::telemetry::Telemetry;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{Arc, Mutex};

/// Attached to a component (an input, transform, or output) via that component's
/// `with_diagnostics` builder -- mirrors the existing `with_timeout`/`with_retry` idiom rather
/// than changing any constructor. A component that never gets one keeps the placeholder id from
/// [`Diagnostics::default`], which is still safe to log -- just not attributable to a specific
/// running instance.
///
/// **A throttle's scope is the component, not the clone.** [`Diagnostics::warn_throttled`]'s
/// per-key counts live behind an [`Arc`], so every clone of one component's `Diagnostics` --
/// the clone a stream listener hands each connection task, the one a listener pushes into its
/// per-connection decoder, the one a transform hands a helper -- throttles against the same
/// running totals. That is what "throttled" has to mean for a component whose work is spread
/// over short-lived tasks: a peer looping connect / send-something-bad / close would otherwise
/// report its 1st occurrence forever, one warning per connection, which is precisely the flood
/// the throttle exists to bound (`docs/adr/service-lifecycle-and-output-retry.md`'s
/// "Diagnostics" section and its 2026-09-14 amendment).
///
/// A genuinely independent throttle -- one piece of a component that must count on its own
/// cadence -- is spelled by building a second value rather than by cloning:
/// `Diagnostics::new(id).with_telemetry(telemetry)`. Nothing in the tree needs one today.
#[derive(Debug, Clone)]
pub struct Diagnostics {
    /// A [`Cow`] rather than a `String` so [`Diagnostics::default`]'s placeholder id -- a
    /// `&'static str` -- costs no allocation: `Default` is what every component that never got a
    /// `with_diagnostics` call carries, including the throwaway no-op encoders/decoders built
    /// inside hot paths (`logit_proto::prometheus::text::write`'s own, which
    /// `crates/logit-bench/tests/allocations.rs` measures). A real id comes from config as an
    /// owned `String` and is stored as one. `Debug` renders both variants identically.
    component_id: Cow<'static, str>,
    /// Per-key occurrence counts for [`Diagnostics::warn_throttled`]. Independent keys never
    /// interfere with each other's throttling -- a component with two distinct failure modes
    /// (e.g. `json`'s "no brace found" and "parse failed") reports each on its own cadence.
    ///
    /// Shared across clones (see the type's own doc comment). A `std::sync::Mutex`, not tokio's:
    /// it is locked only on a diagnostic's error branch, never on a happy path, and the guard
    /// never outlives the statement that takes it, so it is never held across an `.await`.
    counts: Arc<Mutex<HashMap<&'static str, u64>>>,
    /// Mirrors every [`Diagnostics::warn_throttled`] occurrence into a
    /// `logit.component.diagnostics{key=...}` counter -- every occurrence, not just the throttled
    /// log subset, since that's exactly the volume a metric (unlike a log stream) is good at
    /// carrying. [`Telemetry::default`] (this field's default) makes that a no-op, so attaching
    /// telemetry is purely additive to every existing `Diagnostics` user.
    telemetry: Telemetry,
}

impl Default for Diagnostics {
    fn default() -> Self {
        // The placeholder id is borrowed, not copied onto the heap, so a default `Diagnostics`
        // costs exactly the one allocation its shared counts need -- see the `component_id`
        // field's doc comment.
        Self::with_id(Cow::Borrowed("<unnamed>"))
    }
}

impl Diagnostics {
    pub fn new(component_id: impl Into<String>) -> Self {
        Self::with_id(Cow::Owned(component_id.into()))
    }

    fn with_id(component_id: Cow<'static, str>) -> Self {
        Self {
            component_id,
            // A fresh `Arc` per construction: two components never share a throttle, only clones
            // of one component's value do.
            counts: Arc::new(Mutex::new(HashMap::new())),
            telemetry: Telemetry::default(),
        }
    }

    /// Attaches a telemetry handle -- see the `telemetry` field's doc comment. Mirrors the
    /// existing `with_diagnostics`/`with_timeout`/`with_retry` builder idiom.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Reports unconditionally, at `warn`, carrying `component` but no `key`. For a diagnostic
    /// that can fire once per event under normal operation (a malformed line, a parse failure),
    /// prefer [`Diagnostics::warn_throttled`] instead -- this is for genuinely rare events, like a
    /// single retry attempt failing.
    pub fn warn(&self, msg: impl Display) {
        tracing::warn!(target: "logit", component = %self.component_id, "{msg}");
    }

    /// Reports unconditionally, at `debug`, carrying `component` but no `key` -- the level for
    /// something the operator can do precisely nothing about, and which therefore should not
    /// reach either the terminal or (via `internal`'s `logs:` setting, `warn` by default) the
    /// pipeline in the ordinary course of running. The per-socket kernel counters being absent on
    /// a non-Linux build is the case it exists for: one line per listener, at startup, every
    /// startup, about a platform property.
    pub fn debug(&self, msg: impl Display) {
        tracing::debug!(target: "logit", component = %self.component_id, "{msg}");
    }

    /// Reports unconditionally, at `info`, carrying `component` and `key` -- for a lifecycle
    /// message a component itself owns (a listener's bound address, a file rotated), which is
    /// rare enough to need no throttling but structured enough to want a stable `key`.
    pub fn info(&self, key: &'static str, msg: impl Display) {
        tracing::info!(target: "logit", component = %self.component_id, key = key, "{msg}");
    }

    /// Reports unconditionally, at `error`, carrying `component` and `key`. For a failure a
    /// component considers fatal to its own operation, not merely a single dropped event.
    pub fn error(&self, key: &'static str, msg: impl Display) {
        tracing::error!(target: "logit", component = %self.component_id, key = key, "{msg}");
    }

    /// Mainly for tests: lets a caller confirm which id a `Diagnostics` value actually carries,
    /// e.g. after a builder chain is supposed to have propagated one into a nested component (a
    /// decoder wrapped by a listener, say) that has no other way to expose it. Not `#[cfg(test)]`
    /// -- a cross-crate test (`logit-inputs`' own test module, checking a value built in
    /// `logit-core`) needs this compiled into `logit-core`'s normal build, since `#[cfg(test)]`
    /// items are never visible outside the crate that defines them, even to a dependent crate's
    /// own tests.
    pub fn component_id(&self) -> &str {
        &self.component_id
    }

    /// How many times `key` has been reported through [`Diagnostics::warn_throttled`] on this
    /// component -- the running total the throttle itself keys on, including the occurrences it
    /// suppressed. Read through the shared counts, so any clone answers for all of them (see the
    /// type's own doc comment).
    ///
    /// Mainly for tests, and not `#[cfg(test)]` for the same reason [`Diagnostics::component_id`]
    /// isn't: the tests that need it live in dependent crates. It is the one observable that
    /// distinguishes "throttled component-wide" from "each task counted 1 in its own copy" from
    /// outside -- `warn_throttled`'s return value can't, since those reports happen on spawned
    /// tasks.
    pub fn occurrences(&self, key: &str) -> u64 {
        self.lock_counts().get(key).copied().unwrap_or(0)
    }

    /// The shared counts, tolerating a poisoned lock. A poisoned lock means some other holder of
    /// this component's `Diagnostics` panicked mid-report; the counts themselves are still
    /// perfectly usable, and losing the throttle entirely would be the worse outcome.
    fn lock_counts(&self) -> std::sync::MutexGuard<'_, HashMap<&'static str, u64>> {
        self.counts.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Reports the 1st, 2nd, 4th, 8th, ... occurrence of `key` (each naming the running total) and
    /// suppresses the rest -- bounded log volume under a flood of the same complaint (e.g. one
    /// malformed line per request) without needing a clock. A time-window limiter was the more
    /// obvious shape, but no component here has a clock injected, and threading one through purely
    /// for diagnostics would make an otherwise-deterministic interface non-deterministic to test
    /// (see ADR `service-lifecycle-and-output-retry`'s Alternatives). Returns whether this call actually reported, so a test can
    /// assert the throttling directly rather than capturing the log stream.
    ///
    /// Every occurrence -- not just the ones that actually report -- increments
    /// `logit.component.diagnostics{key}` via `self.telemetry`, so a flood that's invisible on a
    /// throttled log is still visible as a real rate once telemetry is live.
    ///
    /// The suppressed path never touches `tracing`, reads a clock, or allocates beyond the
    /// `HashMap::entry` below -- `crates/logit-bench/tests/allocations.rs`'s live-telemetry rows
    /// depend on that. The counts are shared across clones, so the entry is reached through a
    /// lock; it is taken on this call only and dropped before the report below.
    ///
    /// Takes `&mut self` even though the shared counts no longer require it. The exclusive borrow
    /// is what all ~190 call sites already hold, and it says the honest thing about the call: it
    /// mutates the component's diagnostic state. What the borrow does *not* scope is the throttle
    /// -- that is the component's, shared by every clone of this value (see the type's doc
    /// comment) -- so holding `&mut` here buys no isolation and none is intended.
    pub fn warn_throttled(&mut self, key: &'static str, msg: impl Display) -> bool {
        self.telemetry.count("logit.component.diagnostics", 1.0, &[("key", key)]);
        let mut counts = self.lock_counts();
        let count = counts.entry(key).or_insert(0);
        *count += 1;
        let count = *count; // copied out so the guard can be dropped before the report below
        drop(counts);
        let should_report = count.is_power_of_two();
        if should_report {
            tracing::warn!(
                target: "logit",
                component = %self.component_id,
                key = key,
                "{msg} (x{count}, further occurrences suppressed)"
            );
        }
        should_report
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::Registry;
    use crate::MetricKind;
    use tracing_subscriber::layer::SubscriberExt;

    /// A capturing `tracing` `Layer` recording every event's level, `component`/`key` fields
    /// (via a `Visit` impl -- `tracing`'s field values arrive through callbacks, not a map), and
    /// rendered message, so these tests can assert on `Diagnostics`' actual `tracing` output
    /// rather than only on its return values.
    #[derive(Clone, Default)]
    struct Captured(std::sync::Arc<std::sync::Mutex<Vec<CapturedEvent>>>);

    #[derive(Debug, Clone)]
    struct CapturedEvent {
        level: tracing::Level,
        component: Option<String>,
        key: Option<String>,
        message: String,
    }

    impl<S> tracing_subscriber::Layer<S> for Captured
    where
        S: tracing::Subscriber,
    {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct Visitor {
                component: Option<String>,
                key: Option<String>,
                message: String,
            }
            impl tracing::field::Visit for Visitor {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    self.record_str(field, &format!("{value:?}"));
                }
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    match field.name() {
                        "component" => self.component = Some(value.trim_matches('"').to_string()),
                        "key" => self.key = Some(value.trim_matches('"').to_string()),
                        "message" => self.message = value.trim_matches('"').to_string(),
                        _ => {}
                    }
                }
            }
            let mut visitor = Visitor { component: None, key: None, message: String::new() };
            event.record(&mut visitor);
            self.0.lock().unwrap().push(CapturedEvent {
                level: *event.metadata().level(),
                component: visitor.component,
                key: visitor.key,
                message: visitor.message,
            });
        }
    }

    /// Runs `body` with `Captured` installed as the default subscriber for the current thread
    /// only (`tracing::subscriber::with_default`), then returns everything it captured -- no
    /// global state, so these tests can run alongside every other test in the crate.
    fn capture(body: impl FnOnce()) -> Vec<CapturedEvent> {
        let captured = Captured::default();
        let subscriber = tracing_subscriber::registry().with(captured.clone());
        tracing::subscriber::with_default(subscriber, body);
        let events = captured.0.lock().unwrap().clone();
        events
    }

    /// The metric side is not throttled even though the log is: 10 occurrences of the same key
    /// report only 4 times (powers of two) but should still sum to 10 on the counter, since a
    /// flood invisible on a throttled log is exactly what a metric is for.
    #[test]
    fn every_warn_throttled_occurrence_increments_the_metric_even_when_the_log_is_suppressed() {
        let registry = Registry::new();
        let mut diag = Diagnostics::new("test").with_telemetry(registry.telemetry_for(
            "test",
            "json",
            "transform",
        ));
        for _ in 0..10 {
            diag.warn_throttled("parse_failure", "x");
        }

        let events = registry.drain(0);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].attributes.get("key").and_then(|v| v.as_str()), Some("parse_failure"));
        match &events[0].metrics[0].kind {
            MetricKind::Sum(s) => assert_eq!(s.value, 10.0),
            other => panic!("expected Sum, got {other:?}"),
        }
    }

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

    /// The scope of a throttle is the component, not the value that happens to hold it: a clone
    /// (what a stream listener hands each connection task, what a listener pushes into a
    /// per-connection decoder) counts against the same running totals as its original. Without
    /// this, a component whose work is spread over short-lived tasks reports every occurrence's
    /// "1st" forever -- see the type's doc comment and
    /// `docs/adr/service-lifecycle-and-output-retry.md`'s 2026-09-14 amendment.
    #[test]
    fn a_clone_shares_the_throttle_with_its_original() {
        let diag = Diagnostics::new("syslog_in");
        let mut clone = diag.clone();
        assert!(clone.warn_throttled("bad_line", "x"), "the 1st occurrence reports");
        assert!(
            clone.warn_throttled("bad_line", "x"),
            "the 2nd reports too -- 2 is a power of two"
        );
        assert!(
            !clone.warn_throttled("bad_line", "x"),
            "the 3rd is suppressed, which it could not be if the clone counted on its own"
        );
        assert_eq!(
            diag.occurrences("bad_line"),
            3,
            "the original reads the clone's occurrences: one shared count, not two"
        );
    }

    /// The other half of the same property: sharing is per component, so two `Diagnostics::new`
    /// values never interfere -- `Diagnostics::new` builds a fresh count map every time.
    #[test]
    fn two_components_do_not_share() {
        let mut first = Diagnostics::new("syslog_in");
        let mut second = Diagnostics::new("statsd_in");
        for _ in 0..3 {
            first.warn_throttled("bad_line", "x");
        }
        assert!(
            second.warn_throttled("bad_line", "x"),
            "a second component's 1st occurrence reports regardless of the first's count"
        );
        assert_eq!(first.occurrences("bad_line"), 3);
        assert_eq!(second.occurrences("bad_line"), 1);
    }

    #[test]
    fn warn_throttled_emits_only_on_powers_of_two_carrying_component_and_key() {
        let mut diag = Diagnostics::new("otlp_in");
        let events = capture(|| {
            for _ in 0..3 {
                diag.warn_throttled("bad_frame", "malformed frame");
            }
        });
        assert_eq!(events.len(), 2, "occurrences 1 and 2 report, 3 is suppressed");
        for event in &events {
            assert_eq!(event.level, tracing::Level::WARN);
            assert_eq!(event.component.as_deref(), Some("otlp_in"));
            assert_eq!(event.key.as_deref(), Some("bad_frame"));
        }
        assert!(events[1].message.contains("x2"));
    }

    #[test]
    fn warn_carries_component_but_no_key() {
        let diag = Diagnostics::new("influx_out");
        let events = capture(|| diag.warn("retry failed"));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].level, tracing::Level::WARN);
        assert_eq!(events[0].component.as_deref(), Some("influx_out"));
        assert_eq!(events[0].key, None);
    }

    #[test]
    fn info_and_error_carry_component_and_key_unthrottled() {
        let diag = Diagnostics::new("otlp_in");
        let events = capture(|| {
            diag.info("bound", "listening on 0.0.0.0:4317");
            diag.error("bind_failed", "address already in use");
        });
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].level, tracing::Level::INFO);
        assert_eq!(events[0].key.as_deref(), Some("bound"));
        assert_eq!(events[1].level, tracing::Level::ERROR);
        assert_eq!(events[1].key.as_deref(), Some("bind_failed"));
    }
}
