//! `has_signal`/`keep_signals`/`drop_signals`: signal-type-aware transforms. Exist so a sink that
//! only wants one signal (a traces-only backend like Tempo, a logs-only backend like Loki) can be
//! fed correctly without teaching every `_out` component its own filtering config -- see ADR
//! `signal-filtering-components` for why this is a component, not a sink field.
//!
//! Two *kinds* of operation, not one, because of `docs/adr/multi-payload-events.md`: a single
//! `Event` can carry a log, metrics, and a span at once, so "filter by signal" is ambiguous
//! between "should this event be here at all" and "which of this event's payloads belong here."
//! `HasSignal` answers the first without ever mutating an event; `KeepSignals`/`DropSignals`
//! answer the second by clearing disallowed payload slots, mirroring `crate::keep`'s
//! allowlist/denylist split for attributes.
//!
//! All three drop an event that ends up carrying nothing -- for `HasSignal` because it never
//! matched a listed signal to begin with, for `KeepSignals`/`DropSignals` because stripping left
//! no payload behind. `Transform::process`'s `None` already means "don't forward"
//! (`crates/logit-pipeline/src/transform.rs`), and an all-dropped batch simply sends nothing
//! downstream (`crates/logit-pipeline/src/runtime.rs`'s `process_batch`) -- no runtime change was
//! needed to support this.

use logit_core::{Event, Resource, Telemetry};
use logit_pipeline::Transform;
use std::sync::Arc;

/// Which payload slots a signal-aware transform acts on. Named for OTLP's signals
/// (`logit_proto::Signal`), not `Event`'s field names -- `traces` corresponds to `event.span`.
/// `logit-transforms` depends on neither `logit-config` nor `logit-proto`
/// (`crates/logit-transforms/Cargo.toml`), so this is its own type, built from config in
/// `logit-cli::pipeline::build_spec` the same way `to_metric_specs` builds `MetricSpec`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SignalSet {
    pub logs: bool,
    pub metrics: bool,
    pub traces: bool,
}

impl SignalSet {
    fn contains(self, has_log: bool, has_metrics: bool, has_span: bool) -> bool {
        (self.logs && has_log) || (self.metrics && has_metrics) || (self.traces && has_span)
    }
}

/// `HasSignal`'s matching rule. `AnyOf` forwards an event carrying at least one listed signal;
/// `Only` additionally requires the event carry nothing outside the listed set. Both require at
/// least one listed signal be present, so an empty event is always dropped under either mode, and
/// `Only` can never be satisfied vacuously by an event with no payload at all.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MatchMode {
    #[default]
    AnyOf,
    Only,
}

/// Drops an event that doesn't carry a wanted signal. Never mutates a forwarded event -- an event
/// matched under `MatchMode::AnyOf` keeps every payload it arrived with, including ones not
/// listed in `signals`. Use `KeepSignals` instead when disallowed payloads must actually be
/// removed, not just tolerated.
pub struct HasSignal {
    signals: SignalSet,
    mode: MatchMode,
    telemetry: Telemetry,
}

impl HasSignal {
    pub fn new(signals: SignalSet, mode: MatchMode) -> Self {
        Self { signals, mode, telemetry: Telemetry::default() }
    }

    /// See [`crate::keep::Keep::with_telemetry`] -- same reasoning, no `Diagnostics` here either:
    /// nothing about matching a fixed signal set can fail.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

impl Transform for HasSignal {
    fn process(&mut self, _resource: &Arc<Resource>, event: Event) -> Option<Event> {
        let has_log = event.log.is_some();
        let has_metrics = !event.metrics.is_empty();
        let has_span = event.span.is_some();

        let matched = match self.mode {
            MatchMode::AnyOf => self.signals.contains(has_log, has_metrics, has_span),
            MatchMode::Only => {
                let within = (has_log <= self.signals.logs)
                    && (has_metrics <= self.signals.metrics)
                    && (has_span <= self.signals.traces);
                within && self.signals.contains(has_log, has_metrics, has_span)
            }
        };

        if matched {
            self.telemetry.count("logit.transform.events.filtered", 0.0, &[]);
            Some(event)
        } else {
            self.telemetry.count("logit.transform.events.filtered", 1.0, &[]);
            None
        }
    }
}

/// Retains only the listed signals' payloads on every event, clearing the rest -- an allowlist,
/// the same relationship to `DropSignals` that `crate::keep::Keep` has to `crate::keep::Remove`.
/// Drops an event whose payload is entirely stripped away.
pub struct KeepSignals {
    signals: SignalSet,
    telemetry: Telemetry,
}

impl KeepSignals {
    pub fn new(signals: SignalSet) -> Self {
        Self { signals, telemetry: Telemetry::default() }
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

impl Transform for KeepSignals {
    fn process(&mut self, _resource: &Arc<Resource>, event: Event) -> Option<Event> {
        strip(event, self.signals, &self.telemetry)
    }
}

/// Clears the listed signals' payloads on every event, keeping the rest -- a denylist. Drops an
/// event whose payload is entirely stripped away.
pub struct DropSignals {
    signals: SignalSet,
    telemetry: Telemetry,
}

impl DropSignals {
    pub fn new(signals: SignalSet) -> Self {
        Self { signals, telemetry: Telemetry::default() }
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

impl Transform for DropSignals {
    fn process(&mut self, _resource: &Arc<Resource>, event: Event) -> Option<Event> {
        // `DropSignals` clears exactly what `KeepSignals` would discard -- the complement of its
        // own `signals` set is what survives.
        let complement = SignalSet {
            logs: !self.signals.logs,
            metrics: !self.signals.metrics,
            traces: !self.signals.traces,
        };
        strip(event, complement, &self.telemetry)
    }
}

/// Shared by [`KeepSignals`] and [`DropSignals`]: clears every payload slot not named in `keep`,
/// records `logit.transform.payloads.stripped{signal}` for each slot actually cleared, and drops
/// the event (returns `None`) if nothing survives.
fn strip(mut event: Event, keep: SignalSet, telemetry: &Telemetry) -> Option<Event> {
    if event.log.is_some() && !keep.logs {
        event.log = None;
        telemetry.count("logit.transform.payloads.stripped", 1.0, &[("signal", "logs")]);
    }
    if !event.metrics.is_empty() && !keep.metrics {
        event.metrics.clear();
        telemetry.count("logit.transform.payloads.stripped", 1.0, &[("signal", "metrics")]);
    }
    if event.span.is_some() && !keep.traces {
        event.span = None;
        telemetry.count("logit.transform.payloads.stripped", 1.0, &[("signal", "traces")]);
    }

    if event.log.is_none() && event.metrics.is_empty() && event.span.is_none() {
        telemetry.count("logit.transform.events.filtered", 1.0, &[]);
        None
    } else {
        Some(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner::intern;
    use logit_core::{AttrMap, BodyFormat, LogRecord, MetricKind, MetricRecord, Registry, Value};

    fn default_resource() -> Arc<Resource> {
        Arc::new(Resource::default())
    }

    fn log_event() -> Event {
        Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::str("msg"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
            },
        )
    }

    fn metric_event() -> Event {
        Event::metric(
            0,
            AttrMap::new(),
            MetricRecord { name: intern("m"), kind: MetricKind::Counter(1.0), unit: None },
        )
    }

    fn add_metric(mut event: Event) -> Event {
        event.metrics.push(MetricRecord {
            name: intern("m"),
            kind: MetricKind::Counter(1.0),
            unit: None,
        });
        event
    }

    fn span_event() -> Event {
        use logit_core::{SpanKind, SpanRecord, SpanStatus};
        Event::span(
            0,
            AttrMap::new(),
            SpanRecord {
                trace_id: [0; 16],
                span_id: [0; 8],
                parent_span_id: None,
                name: Value::str("op"),
                kind: SpanKind::Internal,
                status: SpanStatus::Unset,
                events: Vec::new(),
                links: Vec::new(),
                end_timestamp: 0,
            },
        )
    }

    fn traces() -> SignalSet {
        SignalSet { traces: true, ..SignalSet::default() }
    }

    fn logs() -> SignalSet {
        SignalSet { logs: true, ..SignalSet::default() }
    }

    fn metrics() -> SignalSet {
        SignalSet { metrics: true, ..SignalSet::default() }
    }

    // -- HasSignal --------------------------------------------------------------------------

    #[test]
    fn has_signal_any_of_forwards_a_mixed_event_untouched() {
        let mut has_signal = HasSignal::new(traces(), MatchMode::AnyOf);
        let resource = default_resource();
        let event = add_metric(span_event());
        let event = has_signal.process(&resource, event).expect("carries a span");
        assert!(event.span.is_some());
        assert_eq!(event.metrics.len(), 1, "any_of must not strip the metric it didn't ask for");
    }

    #[test]
    fn has_signal_any_of_drops_an_event_missing_every_listed_signal() {
        let mut has_signal = HasSignal::new(traces(), MatchMode::AnyOf);
        let resource = default_resource();
        assert!(has_signal.process(&resource, metric_event()).is_none());
    }

    #[test]
    fn has_signal_only_drops_a_mixed_event() {
        let mut has_signal = HasSignal::new(traces(), MatchMode::Only);
        let resource = default_resource();
        let event = add_metric(span_event());
        assert!(
            has_signal.process(&resource, event).is_none(),
            "carries metrics too, not span-only"
        );
    }

    #[test]
    fn has_signal_only_forwards_a_pure_event() {
        let mut has_signal = HasSignal::new(traces(), MatchMode::Only);
        let resource = default_resource();
        assert!(has_signal.process(&resource, span_event()).is_some());
    }

    #[test]
    fn has_signal_drops_an_empty_event_under_either_mode() {
        let resource = default_resource();
        let empty = Event::empty(0, AttrMap::new());
        assert!(HasSignal::new(traces(), MatchMode::AnyOf)
            .process(&resource, empty.clone())
            .is_none());
        assert!(HasSignal::new(traces(), MatchMode::Only).process(&resource, empty).is_none());
    }

    // -- KeepSignals / DropSignals ------------------------------------------------------------

    #[test]
    fn keep_signals_strips_disallowed_payloads_and_keeps_the_rest() {
        let mut keep = KeepSignals::new(traces());
        let resource = default_resource();
        let event = add_metric(span_event());
        let event = keep.process(&resource, event).expect("span survives");
        assert!(event.span.is_some());
        assert!(event.metrics.is_empty(), "metrics not in the keep set must be stripped");
    }

    #[test]
    fn keep_signals_drops_an_event_left_with_nothing() {
        let mut keep = KeepSignals::new(traces());
        let resource = default_resource();
        assert!(keep.process(&resource, metric_event()).is_none());
    }

    #[test]
    fn drop_signals_clears_the_named_signal_and_keeps_the_rest() {
        let mut drop = DropSignals::new(metrics());
        let resource = default_resource();
        let event = add_metric(span_event());
        let event = drop.process(&resource, event).expect("span survives");
        assert!(event.span.is_some());
        assert!(event.metrics.is_empty());
    }

    #[test]
    fn drop_signals_drops_an_event_left_with_nothing() {
        let mut drop = DropSignals::new(metrics());
        let resource = default_resource();
        assert!(drop.process(&resource, metric_event()).is_none());
    }

    #[test]
    fn keep_signals_is_a_no_op_when_the_event_already_matches() {
        let mut keep = KeepSignals::new(logs());
        let resource = default_resource();
        let event = keep.process(&resource, log_event()).expect("log survives");
        assert!(event.log.is_some());
    }

    // -- telemetry ----------------------------------------------------------------------------

    fn counter_value(events: &[Event], name: &str) -> Option<f64> {
        events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Counter(v) if logit_core::interner::resolve(m.name) == name => Some(*v),
                _ => None,
            })
        })
    }

    #[test]
    fn has_signal_records_filtered_events() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("trace_only", "has_signal", "transform");
        let mut has_signal = HasSignal::new(traces(), MatchMode::AnyOf).with_telemetry(telemetry);
        let resource = default_resource();

        has_signal.process(&resource, span_event());
        has_signal.process(&resource, metric_event());

        let events = registry.drain(0);
        assert_eq!(counter_value(&events, "logit.transform.events.filtered"), Some(1.0));
    }

    #[test]
    fn keep_signals_records_stripped_payloads() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("log_only", "keep_signals", "transform");
        let mut keep = KeepSignals::new(logs()).with_telemetry(telemetry);
        let resource = default_resource();

        keep.process(&resource, add_metric(log_event()));

        let events = registry.drain(0);
        assert_eq!(counter_value(&events, "logit.transform.payloads.stripped"), Some(1.0));
    }

    #[test]
    fn a_disabled_telemetry_handle_is_the_default() {
        let mut has_signal = HasSignal::new(traces(), MatchMode::AnyOf);
        let resource = default_resource();
        assert!(has_signal.process(&resource, span_event()).is_some());
    }
}
