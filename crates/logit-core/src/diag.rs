//! A small diagnostics helper over `tracing`: every self-diagnostic gets a consistent
//! `component`/`key` field pair, a severity, and a way to bound how often a high-volume message
//! repeats. See `docs/adr/service-lifecycle-and-output-retry.md` and
//! `docs/adr/tracing-for-self-logging.md`.

use crate::telemetry::Telemetry;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{Arc, Mutex};

/// A component's diagnostics handle, attached through its `with_diagnostics` builder. Without
/// one, a component logs under [`Diagnostics::default`]'s placeholder id.
///
/// **A throttle's scope is the component, not the clone.** [`Diagnostics::warn_throttled`]'s
/// per-key counts live behind an [`Arc`], so every clone (one per connection task, per decoder,
/// per helper) counts against the same totals. Otherwise a peer looping connect, send something
/// bad, close would report its 1st occurrence forever, which is the flood the throttle exists to
/// bound (`docs/adr/service-lifecycle-and-output-retry.md`'s "Diagnostics" section).
///
/// For an independent throttle, build a second value with `Diagnostics::new` instead of cloning.
#[derive(Debug, Clone)]
pub struct Diagnostics {
    /// A [`Cow`] so the default's `&'static str` placeholder doesn't allocate: throwaway
    /// encoders built on hot paths carry the default, and
    /// `crates/logit-bench/tests/allocations.rs` counts them.
    component_id: Cow<'static, str>,
    /// Per-key counts for [`Diagnostics::warn_throttled`]; each key throttles independently.
    ///
    /// Shared across clones. A `std::sync::Mutex`: locked only on an error branch and never held
    /// across an `.await`.
    counts: Arc<Mutex<HashMap<&'static str, u64>>>,
    /// Counts every [`Diagnostics::warn_throttled`] occurrence, suppressed ones included, as
    /// `logit.component.diagnostics{key}`. A no-op by default.
    telemetry: Telemetry,
}

impl Default for Diagnostics {
    fn default() -> Self {
        // Borrowed: a default costs only the counts' allocation.
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
            // Fresh per construction: only clones share a throttle.
            counts: Arc::new(Mutex::new(HashMap::new())),
            telemetry: Telemetry::default(),
        }
    }

    /// Attaches a telemetry handle that counts [`Diagnostics::warn_throttled`] occurrences.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Reports at `warn`, unthrottled, with `component` but no `key`. For rare events like one
    /// retry failing; anything that can fire per event should use
    /// [`Diagnostics::warn_throttled`].
    pub fn warn(&self, msg: impl Display) {
        tracing::warn!(target: "logit", component = %self.component_id, "{msg}");
    }

    /// Reports at `debug`, unthrottled, with `component` but no `key`. For something the operator
    /// can't act on, such as per-socket kernel counters being unavailable off Linux.
    pub fn debug(&self, msg: impl Display) {
        tracing::debug!(target: "logit", component = %self.component_id, "{msg}");
    }

    /// Reports at `info`, unthrottled, with `component` and `key`: a rare lifecycle message such
    /// as a listener's bound address or a file rotation.
    pub fn info(&self, key: &'static str, msg: impl Display) {
        tracing::info!(target: "logit", component = %self.component_id, key = key, "{msg}");
    }

    /// Reports at `error`, unthrottled, with `component` and `key`: a failure fatal to the
    /// component's operation, not one dropped event.
    pub fn error(&self, key: &'static str, msg: impl Display) {
        tracing::error!(target: "logit", component = %self.component_id, key = key, "{msg}");
    }

    /// The id this value carries, so a test can check it propagated into a nested component.
    /// Not `#[cfg(test)]`: the tests that use it live in dependent crates.
    pub fn component_id(&self) -> &str {
        &self.component_id
    }

    /// The running total of `key` through [`Diagnostics::warn_throttled`], suppressed occurrences
    /// included, across every clone.
    ///
    /// For tests in dependent crates (hence not `#[cfg(test)]`): it's the only way to tell a
    /// component-wide throttle from per-task copies when the reports happen on spawned tasks.
    pub fn occurrences(&self, key: &str) -> u64 {
        self.lock_counts().get(key).copied().unwrap_or(0)
    }

    /// The shared counts, tolerating poison: a clone panicking mid-report leaves them usable, and
    /// losing the throttle would be worse.
    fn lock_counts(&self) -> std::sync::MutexGuard<'_, HashMap<&'static str, u64>> {
        self.counts.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Reports the 1st, 2nd, 4th, 8th, ... occurrence of `key` (powers of two, each naming the
    /// running total) and suppresses the rest. Returns whether this call reported.
    ///
    /// Bounds a flood without a clock: no component has one injected, and threading one through
    /// would make the interface nondeterministic to test (ADR
    /// `service-lifecycle-and-output-retry`'s Alternatives).
    ///
    /// Every occurrence, suppressed or not, increments `logit.component.diagnostics{key}`, so a
    /// flood hidden by the throttle still shows as a rate. The suppressed path never touches
    /// `tracing`, reads a clock, or allocates beyond the `HashMap::entry`;
    /// `crates/logit-bench/tests/allocations.rs` depends on that.
    ///
    /// `&mut self` isn't needed by the shared counts and scopes nothing: the throttle belongs to
    /// the component, across clones.
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

    /// A `tracing` layer recording each event's level, `component`/`key` fields, and message.
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

    /// Runs `body` under a thread-local `Captured` subscriber and returns what it captured.
    fn capture(body: impl FnOnce()) -> Vec<CapturedEvent> {
        let captured = Captured::default();
        let subscriber = tracing_subscriber::registry().with(captured.clone());
        tracing::subscriber::with_default(subscriber, body);
        let events = captured.0.lock().unwrap().clone();
        events
    }

    /// The counter isn't throttled: 10 occurrences log 4 times but count 10.
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
        assert!(
            diag.warn_throttled("b", "y"),
            "b's 1st occurrence reports regardless of a's count"
        );
        assert_eq!(
            diag.occurrences("a"),
            3,
            "a's count stays 3, distinct from b's -- a shared counter would read 4 here too"
        );
        assert_eq!(
            diag.occurrences("b"),
            1,
            "b's count is 1, not 4 -- a shared counter would read 4 for b"
        );
    }

    /// A clone counts against its original's totals.
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

    /// Two `Diagnostics::new` values never share counts.
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
