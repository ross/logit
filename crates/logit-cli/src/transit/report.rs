//! A run's result as one JSON document:
//!
//! ```text
//! {"events": [<one format: json object per event>, ...],
//!  "counters": {"<name>{<tag>=<value>,...}": <total>, ...}}
//! ```
//!
//! Each event is the line `crates/logit-outputs/src/ndjson.rs` writes for it, spliced in as
//! written, never parsed and re-serialized, so no number's spelling changes. Events sort by
//! timestamp, then first metric or span name, then line.
//!
//! `counters` holds every series with a nonzero total whose name ends in one of [`OUTCOMES`],
//! keyed by name and its sorted tags (`component`, `kind`, and `role` included), in key order. A
//! series that isn't there didn't move.

use logit_core::interner::resolve;
use logit_core::{Event, EventBatch};
use logit_outputs::ndjson::render_ndjson;
use logit_pipeline::test_util::Totals;
use std::collections::BTreeMap;
use std::fmt::Write;

/// The name suffixes `counters` keeps: the loss and normalization vocabulary
/// (`docs/design/internal-telemetry.md`'s "Loss vocabulary"), plus `omitted`, a field or tag
/// left off a record that was still written.
const OUTCOMES: [&str; 7] =
    ["dropped", "skipped", "degraded", "normalized", "discarded", "passed_through", "omitted"];

/// What one run produced.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Outcome {
    /// One `format: json` line per re-decoded event, without its newline, in sort order.
    pub events: Vec<String>,
    /// `name{tag=value,...}` to total.
    pub counters: BTreeMap<String, f64>,
}

impl Outcome {
    pub(super) fn new(decoded: &[EventBatch], totals: &Totals) -> Self {
        let mut keyed: Vec<((i64, String), String)> = Vec::new();
        for batch in decoded {
            let mut text = String::new();
            render_ndjson(&mut text, batch);
            // One line per event: the grammar escapes every newline inside a value.
            for (event, line) in batch.events.iter().zip(text.lines()) {
                keyed.push(((event.timestamp, first_name(event)), line.to_string()));
            }
        }
        keyed.sort();
        let events = keyed.into_iter().map(|(_, line)| line).collect();

        let mut counters = BTreeMap::new();
        for (name, tags, total) in totals.sums() {
            let suffix = name.rsplit('.').next().unwrap_or(name);
            if total == 0.0 || !OUTCOMES.contains(&suffix) {
                continue;
            }
            let tags: Vec<String> = tags.iter().map(|(k, v)| format!("{k}={v}")).collect();
            *counters.entry(format!("{name}{{{}}}", tags.join(","))).or_default() += total;
        }
        Self { events, counters }
    }

    /// The JSON document (module doc), ending in a newline.
    pub(crate) fn document(&self) -> String {
        let mut out = String::from("{\"events\": [");
        for (i, line) in self.events.iter().enumerate() {
            out.push_str(if i == 0 { "\n  " } else { ",\n  " });
            out.push_str(line);
        }
        if !self.events.is_empty() {
            out.push('\n');
        }
        out.push_str("], \"counters\": {");
        for (i, (key, total)) in self.counters.iter().enumerate() {
            out.push_str(if i == 0 { "\n  " } else { ",\n  " });
            out.push_str(&serde_json::Value::from(key.as_str()).to_string());
            // Counts are whole; a whole total prints as an integer.
            if total.fract() == 0.0 && total.abs() < 9_007_199_254_740_992.0 {
                let _ = write!(out, ": {}", *total as i64);
            } else {
                let _ = write!(out, ": {}", serde_json::Value::from(*total));
            }
        }
        if !self.counters.is_empty() {
            out.push('\n');
        }
        out.push_str("}}\n");
        out
    }
}

/// The first metric's name, else the span's, else empty: the sort's second key.
fn first_name(event: &Event) -> String {
    if let Some(metric) = event.metrics.iter().next() {
        return resolve(metric.name).to_string();
    }
    event.span.as_ref().and_then(|span| span.name.as_str().map(str::to_string)).unwrap_or_default()
}
