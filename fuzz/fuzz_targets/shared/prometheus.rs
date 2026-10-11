//! What the two Prometheus targets share: a decoder whose skip and degradation counters can be
//! read back.

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

/// Every `logit.input.metrics.<counter>{reason}` the registry holds, with its count.
pub fn reasons(registry: &Registry, counter: &str) -> BTreeMap<String, u64> {
    let name = format!("logit.input.metrics.{counter}");
    let mut out = BTreeMap::new();
    for event in registry.drain(0) {
        let Some(count) = event.metrics.iter().find_map(|metric| {
            (resolve(metric.name) == name).then_some(match &metric.kind {
                MetricKind::Sum(sum) => sum.value,
                _ => 0.0,
            })
        }) else {
            continue;
        };
        let reason = event.attributes.get("reason").and_then(|value| value.as_str());
        *out.entry(reason.unwrap_or("").to_string()).or_insert(0) += count as u64;
    }
    out
}
