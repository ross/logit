//! The exit summary of telemetry no `internal` drain exported: after every node has exited,
//! `run_pipelines` drains the registry once more and writes each leftover metric point to the
//! self-log as `telemetry left undrained at exit` (decision 16 of
//! `docs/adr/shutdown-accounting-and-cancellation-safety.md`'s "Amendment: internal drains last").
//!
//! A counter or gauge logs its number as `value`. A timing, which drains as a
//! `MetricKind::Distribution`, logs `count` and `sum` in place of `value`. Spans and captured logs
//! are skipped: the logs already reached stderr.

use logit_core::{Event, MetricKind, Registry, Value};

/// One leftover point, as the summary line renders it.
#[derive(Debug, PartialEq)]
pub struct Leftover {
    pub component: String,
    pub metric: &'static str,
    /// The point's tags as `k=v` pairs sorted by key and joined by `,`, without the identity
    /// attributes (`component`, `kind`, `role`) every drained point carries.
    pub tags: String,
    pub reading: Reading,
}

#[derive(Debug, PartialEq)]
pub enum Reading {
    /// A counter's or gauge's number.
    Value(f64),
    /// A timing's observation count and sum.
    Distribution { count: u64, sum: f64 },
}

impl Leftover {
    /// A nonzero `*.dropped` count whose `reason` the config didn't ask for is data the run lost;
    /// everything else is routine. The intended reasons are an exclusion list, so a reason added
    /// later warns until someone decides it is intended.
    fn is_loss(&self) -> bool {
        /// Drops a transform or router makes because the config says to: an aggregate or filter
        /// absorbing an event, a route with no matching target, and a script returning no event.
        const INTENDED: [&str; 3] = ["absorbed", "unrouted", "script_drop"];
        let reason = self.tags.split(',').find_map(|pair| pair.strip_prefix("reason="));
        matches!(self.reading, Reading::Value(v) if v != 0.0)
            && self.metric.ends_with(".dropped")
            && !reason.is_some_and(|r| INTENDED.contains(&r))
    }
}

/// Drains `registry` and writes one self-log line per leftover metric point. The drain happens
/// before the first line is written, so a line `TelemetryLayer` captures back into the registry
/// stays there: nothing drains it again.
pub fn log_leftovers(registry: &Registry) {
    for leftover in leftovers(registry.drain(0)) {
        let Leftover { component, metric, tags, reading } = &leftover;
        match (reading, leftover.is_loss()) {
            (Reading::Value(value), true) => tracing::warn!(
                target: "logit", component, metric, tags, value, "telemetry left undrained at exit"
            ),
            (Reading::Value(value), false) => tracing::debug!(
                target: "logit", component, metric, tags, value, "telemetry left undrained at exit"
            ),
            (Reading::Distribution { count, sum }, _) => tracing::debug!(
                target: "logit", component, metric, tags, count, sum,
                "telemetry left undrained at exit"
            ),
        }
    }
}

/// Every metric point in `events` as a [`Leftover`], sorted by component, metric, and tags.
pub fn leftovers(events: Vec<Event>) -> Vec<Leftover> {
    let mut out = Vec::new();
    for event in events {
        if event.span.is_some() || event.log.is_some() {
            continue;
        }
        let component = event.attributes.get("component").map(render).unwrap_or_default();
        let mut tags: Vec<(&str, String)> = event
            .attributes
            .iter()
            .map(|(k, v)| (logit_core::interner::resolve(k), render(v)))
            .filter(|(k, _)| !matches!(*k, "component" | "kind" | "role"))
            .collect();
        tags.sort();
        let tags = tags.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join(",");
        for record in &event.metrics {
            // `Registry::drain` emits only these three kinds.
            let reading = match &record.kind {
                MetricKind::Sum(sum) => Reading::Value(sum.value),
                MetricKind::Gauge(v) => Reading::Value(*v),
                MetricKind::Distribution(sketch) => {
                    Reading::Distribution { count: sketch.count() as u64, sum: sketch.sum() }
                }
                _ => continue,
            };
            out.push(Leftover {
                component: component.clone(),
                metric: logit_core::interner::resolve(record.name),
                tags: tags.clone(),
                reading,
            });
        }
    }
    out.sort_by(|a, b| (&a.component, a.metric, &a.tags).cmp(&(&b.component, b.metric, &b.tags)));
    out
}

fn render(value: &Value) -> String {
    value.as_str().map_or_else(|| format!("{value:?}"), str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn reading_of<'a>(all: &'a [Leftover], metric: &str) -> &'a Leftover {
        all.iter().find(|l| l.metric == metric).unwrap_or_else(|| panic!("no {metric} in {all:?}"))
    }

    #[test]
    fn renders_each_drained_kind_with_sorted_tags_and_no_identity_attributes() {
        let registry = Registry::new();
        let sink = registry.telemetry_for("out", "influxdb_out", "output");
        sink.count("logit.component.batches.dropped", 2.0, &[("reason", "shutdown")]);
        sink.gauge("logit.component.queue.depth", 3.0, &[("zone", "b"), ("area", "a")]);
        sink.timing("logit.component.send.duration", Duration::from_millis(10), &[]);
        sink.timing("logit.component.send.duration", Duration::from_millis(30), &[]);

        let all = leftovers(registry.drain(0));
        assert_eq!(all.len(), 3, "{all:?}");
        assert_eq!(
            reading_of(&all, "logit.component.batches.dropped"),
            &Leftover {
                component: "out".into(),
                metric: "logit.component.batches.dropped",
                tags: "reason=shutdown".into(),
                reading: Reading::Value(2.0),
            }
        );
        let gauge = reading_of(&all, "logit.component.queue.depth");
        assert_eq!(gauge.tags, "area=a,zone=b");
        assert_eq!(gauge.reading, Reading::Value(3.0));
        let timing = reading_of(&all, "logit.component.send.duration");
        assert_eq!(timing.tags, "");
        let Reading::Distribution { count, sum } = timing.reading else {
            panic!("{timing:?}");
        };
        assert_eq!(count, 2);
        assert!((sum - 0.04).abs() < 1e-9, "{sum}");
    }

    #[test]
    fn only_a_nonzero_dropped_value_is_a_loss() {
        let leftover = |metric, reading| Leftover {
            component: "c".into(),
            metric,
            tags: String::new(),
            reading,
        };
        assert!(leftover("logit.component.batches.dropped", Reading::Value(1.0)).is_loss());
        assert!(!leftover("logit.component.batches.dropped", Reading::Value(0.0)).is_loss());
        assert!(!leftover("logit.component.batches.sent", Reading::Value(1.0)).is_loss());
        assert!(!leftover("x.dropped", Reading::Distribution { count: 1, sum: 1.0 }).is_loss());
    }

    #[test]
    fn a_drop_the_config_asked_for_is_not_a_loss() {
        let dropped = |reason: &str| Leftover {
            component: "c".into(),
            metric: "logit.component.events.dropped",
            tags: format!("reason={reason}"),
            reading: Reading::Value(3.0),
        };
        for intended in ["absorbed", "unrouted", "script_drop"] {
            assert!(!dropped(intended).is_loss(), "{intended}");
        }
        for lost in ["shutdown", "rejected", "ambiguous_at_most_once", "a_new_reason"] {
            assert!(dropped(lost).is_loss(), "{lost}");
        }
    }

    #[test]
    fn skips_spans_and_captured_logs() {
        let event = Event::log(
            0,
            logit_core::AttrMap::new(),
            logit_core::LogRecord {
                message: Value::str("m"),
                severity: None,
                body_format: logit_core::BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        assert!(leftovers(vec![event]).is_empty());
    }
}
