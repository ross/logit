//! What the two Prometheus targets share: a decoder whose drop, skip, and degradation counters can
//! be read back.

use logit_core::interner::resolve;
use logit_core::{MetricKind, Registry};
use logit_proto::prometheus::PrometheusDecoder;
use std::collections::BTreeMap;
use std::sync::Arc;

/// A decoder counting into a registry of its own, so [`reasons`] reads only this call's counts.
pub fn counted_decoder() -> (Arc<Registry>, PrometheusDecoder) {
    let registry = Registry::new();
    let telemetry = registry.telemetry_for("prometheus", "prometheus_in", "source");
    (registry, PrometheusDecoder::new().with_telemetry(telemetry))
}

/// Each `logit.input.metrics.*{reason}` counter the decoder writes, by reason, with its count.
#[derive(Default)]
pub struct Reasons {
    pub dropped: BTreeMap<String, u64>,
    /// The decoder counts nothing here; a target asserts it stays empty.
    pub skipped: BTreeMap<String, u64>,
    pub degraded: BTreeMap<String, u64>,
}

/// Reads every counter in one pass: [`Registry::drain`] empties the registry, so a second read
/// would see nothing.
pub fn reasons(registry: &Registry) -> Reasons {
    let mut out = Reasons::default();
    for event in registry.drain(0) {
        let reason = event.attributes.get("reason").and_then(|value| value.as_str());
        for metric in &event.metrics {
            let map = match resolve(metric.name) {
                "logit.input.metrics.dropped" => &mut out.dropped,
                "logit.input.metrics.skipped" => &mut out.skipped,
                "logit.input.metrics.degraded" => &mut out.degraded,
                _ => continue,
            };
            let count = match &metric.kind {
                MetricKind::Sum(sum) => sum.value.as_f64(),
                _ => 0.0,
            };
            *map.entry(reason.unwrap_or("").to_string()).or_insert(0) += count as u64;
        }
    }
    out
}
