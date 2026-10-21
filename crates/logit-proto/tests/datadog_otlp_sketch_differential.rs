//! `datadog_out`'s sketches route for OTLP histograms against the Datadog exporter's translator,
//! from `testdata/differential/datadog-sketch/cases/` (its README has the corpus).
//!
//! Each case's point goes through [`DatadogEncoder::encode_sketches`] under the default
//! `histograms.mode: distributions`. Where the translator sent a sketch, the `Dogsketch` must carry
//! its `k` and `n` in the same order, a count past 65535 split remainder first as the Agent's
//! `appendSafe` splits it, and its `cnt`, `sum`, `min`, `max`, and `avg` bit for bit. Where it
//! sent none, the encoder sends none and counts the outcome under its reason. Where `trimLeft`
//! folded bins, the per-key totals match but not Go's order (`fold_order` below).

use logit_core::interner::{intern, resolve};
use logit_core::{
    AttrMap, Event, EventBatch, ExpHistogram, Histogram, MetricKind, MetricRecord, Registry,
    Resource, Temporality, Value,
};
use logit_proto::datadog::generated::agentpayload::SketchPayload;
use logit_proto::datadog::DatadogEncoder;
use prost::Message;
use serde_json::Value as Json;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

fn corpus() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../testdata/differential/datadog-sketch/cases")
}

fn num(v: &Json) -> f64 {
    match v.as_str().expect("a float is a string") {
        "+Inf" => f64::INFINITY,
        "-Inf" => f64::NEG_INFINITY,
        "NaN" => f64::NAN,
        s => s.parse().unwrap_or_else(|e| panic!("{s}: {e}")),
    }
}

fn opt_num(v: &Json) -> Option<f64> {
    (!v.is_null()).then(|| num(v))
}

fn u64s(v: &Json) -> Vec<u64> {
    v.as_array().unwrap().iter().map(|n| n.as_u64().unwrap()).collect()
}

fn kind(case: &Json) -> MetricKind {
    let input = &case["input"];
    if case["kind"] == "explicit" {
        let bounds: Vec<f64> = input["bounds"].as_array().unwrap().iter().map(num).collect();
        let counts = u64s(&input["counts"]);
        return MetricKind::Histogram(Histogram {
            buckets: counts
                .iter()
                .enumerate()
                .map(|(i, c)| (bounds.get(i).copied().unwrap_or(f64::INFINITY), *c))
                .collect(),
            temporality: Temporality::Delta,
            sum: opt_num(&input["sum"]),
            min: opt_num(&input["min"]),
            max: opt_num(&input["max"]),
        });
    }
    let side = |v: &Json| (v["offset"].as_i64().unwrap() as i32, u64s(&v["counts"]));
    MetricKind::ExponentialHistogram(ExpHistogram {
        scale: input["scale"].as_i64().unwrap() as i32,
        zero_count: input["zero_count"].as_u64().unwrap(),
        zero_threshold: 0.0,
        positive: side(&input["positive"]),
        negative: side(&input["negative"]),
        temporality: Temporality::Delta,
        count: input["count"].as_u64().unwrap(),
        sum: opt_num(&input["sum"]),
        min: opt_num(&input["min"]),
        max: opt_num(&input["max"]),
    })
}

fn same(a: f64, b: f64) -> bool {
    (a.is_nan() && b.is_nan()) || a.to_bits() == b.to_bits()
}

/// Per-key totals, for the cases whose order `trimLeft` sets.
fn totals(k: &[i32], n: &[u32]) -> BTreeMap<i32, u64> {
    let mut out = BTreeMap::new();
    for (k, n) in k.iter().zip(n) {
        *out.entry(*k).or_insert(0) += u64::from(*n);
    }
    out
}

/// Whether Go's `k`/`n` put a full bin ahead of a partial one of the same key: `trimLeft` keeps an
/// overflowing fold that way, the reverse of `appendSafe`'s order, which the encoder writes.
fn fold_order(k: &[i32], n: &[u32]) -> bool {
    (1..k.len()).any(|i| k[i] == k[i - 1] && n[i - 1] == 65535 && n[i] != 65535)
}

/// The counter each no-sketch outcome moves.
fn counter(outcome: &str) -> (&'static str, &'static str) {
    match outcome {
        "empty" => ("logit.output.metrics.skipped", "empty_sketch"),
        "too_many_observations" => ("logit.output.metrics.dropped", "oversized_sketch"),
        "non_monotonic_bounds" => ("logit.output.metrics.dropped", "non_monotonic_bounds"),
        "unconvertible" => ("logit.output.metrics.dropped", "sketch_range"),
        other => panic!("unknown outcome {other}"),
    }
}

#[test]
fn every_sketch_matches_the_translator() {
    let mut paths: Vec<_> = std::fs::read_dir(corpus())
        .expect("the corpus is committed")
        .map(|e| e.unwrap().path())
        .collect();
    paths.sort();
    let mut diffs = Vec::new();
    for path in &paths {
        let case: Json = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        let name = path.file_stem().unwrap().to_string_lossy().into_owned();
        let registry = Registry::new();
        let mut encoder = DatadogEncoder::new().with_telemetry(registry.telemetry_for(
            "dd",
            "datadog_out",
            "output",
        ));
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![Event::metric(
                1_700_000_000_000_000_000,
                AttrMap::new(),
                MetricRecord::new(intern("case"), kind(&case)),
            )],
        };
        let body = encoder.encode_sketches(&batch);
        let outcome = case["outcome"].as_str().unwrap();
        if outcome != "sketch" {
            let (metric, reason) = counter(outcome);
            let counted = registry.drain(0).iter().any(|e| {
                e.attributes.get("reason").and_then(Value::as_str) == Some(reason)
                    && e.metrics.iter().any(|m| resolve(m.name) == metric)
            });
            if body.is_some() || !counted {
                diffs.push(format!("{name}: want no sketch and {metric}{{{reason}}}"));
            }
            continue;
        }
        let Some(body) = body else {
            diffs.push(format!("{name}: no sketch"));
            continue;
        };
        let payload = SketchPayload::decode(body).unwrap();
        let dog = &payload.sketches[0].dogsketches[0];
        let want = &case["sketch"];
        let k: Vec<i32> =
            want["k"].as_array().unwrap().iter().map(|v| v.as_i64().unwrap() as i32).collect();
        let n: Vec<u32> =
            want["n"].as_array().unwrap().iter().map(|v| v.as_u64().unwrap() as u32).collect();
        let bins_match = if fold_order(&k, &n) {
            totals(&dog.k, &dog.n) == totals(&k, &n)
        } else {
            dog.k == k && dog.n == n
        };
        if !bins_match {
            diffs.push(format!("{name}: k/n {:?}/{:?}, want {k:?}/{n:?}", dog.k, dog.n));
        }
        if dog.cnt != want["cnt"].as_i64().unwrap() {
            diffs.push(format!("{name}: cnt {}, want {}", dog.cnt, want["cnt"]));
        }
        for (field, got) in [("sum", dog.sum), ("min", dog.min), ("max", dog.max), ("avg", dog.avg)]
        {
            if !same(got, num(&want[field])) {
                diffs.push(format!("{name}: {field} {got:e}, want {}", want[field]));
            }
        }
    }
    assert!(paths.len() >= 200, "{} cases", paths.len());
    assert!(diffs.is_empty(), "{} differences:\n{}", diffs.len(), diffs.join("\n"));
}
