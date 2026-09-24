//! `has_signal`/`keep_signals`/`drop_signals`: feed a single-signal sink (Tempo for traces, Loki
//! for logs) without a filtering field on every `_out`. See
//! `docs/adr/signal-filtering-components.md`.
//!
//! One `Event` can carry a log, metrics, and a span at once (`docs/adr/multi-payload-events.md`),
//! so "filter by signal" is two questions. `HasSignal` answers "should this event be here" and
//! never mutates it; `KeepSignals`/`DropSignals` answer "which payloads belong here" by clearing
//! payload slots, as `keep`/`remove` do for attributes.
//!
//! All three drop an event that ends up carrying nothing: for `HasSignal` because no listed signal
//! matched, for the other two because stripping left no payload.

use logit_core::{Event, Resource, Telemetry};
use logit_pipeline::Transform;
use std::sync::Arc;

/// Which payload slots a signal-aware transform acts on.
///
/// Named for OTLP's signals, not `Event`'s fields: `traces` is `event.span`. `logit-cli`'s
/// `build_spec` builds it from config.
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

/// `HasSignal`'s matching rule.
///
/// `AnyOf` forwards an event carrying at least one listed signal; `Only` also requires nothing
/// outside the listed set. Both require a listed signal to be present, so an event with no payload
/// is dropped under either mode rather than satisfying `Only` vacuously.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MatchMode {
    #[default]
    AnyOf,
    Only,
}

/// Drops an event that doesn't carry a wanted signal.
///
/// Never mutates a forwarded event: under `MatchMode::AnyOf` it keeps unlisted payloads too. Use
/// `KeepSignals` to remove them.
pub struct HasSignal {
    signals: SignalSet,
    mode: MatchMode,
    telemetry: Telemetry,
}

impl HasSignal {
    pub fn new(signals: SignalSet, mode: MatchMode) -> Self {
        Self { signals, mode, telemetry: Telemetry::default() }
    }

    /// Attaches a telemetry handle; matching a fixed signal set can't fail, so no `Diagnostics`.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

impl Transform for HasSignal {
    fn process(&mut self, _resource: &Arc<Resource>, event: &mut Event) -> bool {
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
            true
        } else {
            self.telemetry.count("logit.transform.events.filtered", 1.0, &[]);
            false
        }
    }
}

/// Retains only the listed signals' payloads on every event, clearing the rest (an allowlist).
///
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
    fn process(&mut self, _resource: &Arc<Resource>, event: &mut Event) -> bool {
        strip(event, self.signals, &self.telemetry)
    }
}

/// Clears the listed signals' payloads on every event, keeping the rest (a denylist).
///
/// Drops an event whose payload is entirely stripped away.
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
    fn process(&mut self, _resource: &Arc<Resource>, event: &mut Event) -> bool {
        let complement = SignalSet {
            logs: !self.signals.logs,
            metrics: !self.signals.metrics,
            traces: !self.signals.traces,
        };
        strip(event, complement, &self.telemetry)
    }
}

/// Clears every payload slot not named in `keep`; `false` (drop) if nothing survives.
///
/// Records `logit.transform.payloads.stripped{signal}` per slot cleared, and
/// `logit.transform.events.filtered` on a drop.
fn strip(event: &mut Event, keep: SignalSet, telemetry: &Telemetry) -> bool {
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
        false
    } else {
        true
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
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        )
    }

    fn metric_event() -> Event {
        Event::metric(0, AttrMap::new(), MetricRecord::new(intern("m"), MetricKind::counter(1.0)))
    }

    fn add_metric(mut event: Event) -> Event {
        event.metrics.push(MetricRecord::new(intern("m"), MetricKind::counter(1.0)));
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
                flags: 0,
                ext: None,
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
        let mut event = add_metric(span_event());
        assert!(has_signal.process(&resource, &mut event), "carries a span");
        assert!(event.span.is_some());
        assert_eq!(event.metrics.len(), 1, "any_of must not strip the metric it didn't ask for");
    }

    #[test]
    fn has_signal_any_of_drops_an_event_missing_every_listed_signal() {
        let mut has_signal = HasSignal::new(traces(), MatchMode::AnyOf);
        let resource = default_resource();
        assert!(!has_signal.process(&resource, &mut metric_event()));
    }

    #[test]
    fn has_signal_only_drops_a_mixed_event() {
        let mut has_signal = HasSignal::new(traces(), MatchMode::Only);
        let resource = default_resource();
        let mut event = add_metric(span_event());
        assert!(!has_signal.process(&resource, &mut event), "carries metrics too, not span-only");
    }

    #[test]
    fn has_signal_only_forwards_a_pure_event() {
        let mut has_signal = HasSignal::new(traces(), MatchMode::Only);
        let resource = default_resource();
        assert!(has_signal.process(&resource, &mut span_event()));
    }

    #[test]
    fn has_signal_drops_an_empty_event_under_either_mode() {
        let resource = default_resource();
        let mut empty = Event::empty(0, AttrMap::new());
        assert!(!HasSignal::new(traces(), MatchMode::AnyOf).process(&resource, &mut empty.clone()));
        assert!(!HasSignal::new(traces(), MatchMode::Only).process(&resource, &mut empty));
    }

    // -- KeepSignals / DropSignals ------------------------------------------------------------

    #[test]
    fn keep_signals_strips_disallowed_payloads_and_keeps_the_rest() {
        let mut keep = KeepSignals::new(traces());
        let resource = default_resource();
        let mut event = add_metric(span_event());
        assert!(keep.process(&resource, &mut event), "span survives");
        assert!(event.span.is_some());
        assert!(event.metrics.is_empty(), "metrics not in the keep set must be stripped");
    }

    #[test]
    fn keep_signals_drops_an_event_left_with_nothing() {
        let mut keep = KeepSignals::new(traces());
        let resource = default_resource();
        assert!(!keep.process(&resource, &mut metric_event()));
    }

    #[test]
    fn drop_signals_clears_the_named_signal_and_keeps_the_rest() {
        let mut drop = DropSignals::new(metrics());
        let resource = default_resource();
        let mut event = add_metric(span_event());
        assert!(drop.process(&resource, &mut event), "span survives");
        assert!(event.span.is_some());
        assert!(event.metrics.is_empty());
    }

    #[test]
    fn drop_signals_drops_an_event_left_with_nothing() {
        let mut drop = DropSignals::new(metrics());
        let resource = default_resource();
        assert!(!drop.process(&resource, &mut metric_event()));
    }

    #[test]
    fn keep_signals_is_a_no_op_when_the_event_already_matches() {
        let mut keep = KeepSignals::new(logs());
        let resource = default_resource();
        let mut event = log_event();
        assert!(keep.process(&resource, &mut event), "log survives");
        assert!(event.log.is_some());
    }

    // -- telemetry ----------------------------------------------------------------------------

    fn counter_value(events: &[Event], name: &str) -> Option<f64> {
        events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(sum) if logit_core::interner::resolve(m.name) == name => {
                    Some(sum.value)
                }
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

        has_signal.process(&resource, &mut span_event());
        has_signal.process(&resource, &mut metric_event());

        let events = registry.drain(0);
        assert_eq!(counter_value(&events, "logit.transform.events.filtered"), Some(1.0));
    }

    #[test]
    fn keep_signals_records_stripped_payloads() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("log_only", "keep_signals", "transform");
        let mut keep = KeepSignals::new(logs()).with_telemetry(telemetry);
        let resource = default_resource();

        keep.process(&resource, &mut add_metric(log_event()));

        let events = registry.drain(0);
        assert_eq!(counter_value(&events, "logit.transform.payloads.stripped"), Some(1.0));
    }

    #[test]
    fn a_disabled_telemetry_handle_is_the_default() {
        let mut has_signal = HasSignal::new(traces(), MatchMode::AnyOf);
        let resource = default_resource();
        assert!(has_signal.process(&resource, &mut span_event()));
    }
}
