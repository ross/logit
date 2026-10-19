//! A run's result as one JSON document:
//!
//! ```text
//! {"events": [<one format: json object per event>, ...],
//!  "counters": {"<name>{<tag>=<value>,...}": <total>, ...}}
//! ```
//!
//! Each event is the line `crates/logit-outputs/src/ndjson.rs` writes for it, with every map
//! under an `attributes` key, and the `types` table, sorted by key ([`canonical_line`]); no value
//! is re-serialized, so no number's spelling changes. Events sort by timestamp, then first metric
//! or span name, then line.
//!
//! `counters` holds every series with a nonzero total whose name ends in one of [`OUTCOMES`],
//! keyed by name and its sorted tags (`component`, `kind`, and `role` included), in key order. A
//! series that isn't there didn't move.

use logit_core::interner::resolve;
use logit_core::{Event, EventBatch};
use logit_outputs::ndjson::render_ndjson;
use logit_pipeline::test_util::Totals;
use serde::de::{Deserialize, Deserializer, MapAccess, Visitor};
use serde_json::value::RawValue;
use std::collections::BTreeMap;
use std::fmt::Write;

/// The name suffixes `counters` keeps: the loss and normalization vocabulary
/// (`docs/design/internal-telemetry.md`'s "Loss vocabulary"), plus `omitted`, a field or tag
/// left off a record that was still written, and `evicted`, a series a cap removed.
const OUTCOMES: [&str; 8] = [
    "dropped",
    "skipped",
    "degraded",
    "normalized",
    "discarded",
    "passed_through",
    "omitted",
    "evicted",
];

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
                keyed.push(((event.timestamp, first_name(event)), canonical_line(line)));
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

/// `line`, a `format: json` object, with each map under an `attributes` key sorted by key,
/// recursively, and the `types` table sorted by pointer. `format: json` writes an attribute map in
/// `AttrMap` order, which is the process interner's, so the same event writes its keys in another
/// order once another key set has been interned first; sorted, a line depends on the event alone.
/// Every other object keeps the grammar's order, and every value keeps its spelling.
pub(super) fn canonical_line(line: &str) -> String {
    let raw: Box<RawValue> = serde_json::from_str(line).expect("a format: json line is JSON");
    let mut out = String::with_capacity(line.len());
    walk(&raw, false, &mut out);
    out
}

/// One JSON object's fields in their written order.
struct Fields(Vec<(String, Box<RawValue>)>);

impl<'de> Deserialize<'de> for Fields {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct FieldsVisitor;
        impl<'de> Visitor<'de> for FieldsVisitor {
            type Value = Fields;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON object")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Fields, A::Error> {
                let mut fields = Vec::new();
                while let Some(entry) = map.next_entry::<String, Box<RawValue>>()? {
                    fields.push(entry);
                }
                Ok(Fields(fields))
            }
        }
        deserializer.deserialize_map(FieldsVisitor)
    }
}

/// Writes `raw` to `out`, sorting an object's keys when `sort` is set or the object sits under
/// `attributes` or `types` (module doc).
fn walk(raw: &RawValue, sort: bool, out: &mut String) {
    let text = raw.get();
    match text.as_bytes().first() {
        Some(b'{') => {
            let Fields(mut fields) = serde_json::from_str(text).expect("an object parses");
            if sort {
                fields.sort_by(|a, b| a.0.cmp(&b.0));
            }
            out.push('{');
            for (i, (key, value)) in fields.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::Value::from(key.as_str()).to_string());
                out.push(':');
                walk(value, sort || key == "attributes" || key == "types", out);
            }
            out.push('}');
        }
        Some(b'[') => {
            let items: Vec<Box<RawValue>> = serde_json::from_str(text).expect("an array parses");
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                walk(item, sort, out);
            }
            out.push(']');
        }
        _ => out.push_str(text),
    }
}

/// The first metric's name, else the span's, else empty: the sort's second key.
fn first_name(event: &Event) -> String {
    if let Some(metric) = event.metrics.iter().next() {
        return resolve(metric.name).to_string();
    }
    event.span.as_ref().and_then(|span| span.name.as_str().map(str::to_string)).unwrap_or_default()
}
