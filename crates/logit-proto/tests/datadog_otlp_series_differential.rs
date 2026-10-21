//! `datadog_out`'s series for OTLP histograms and summaries against the Datadog exporter's
//! translator, from `testdata/differential/datadog-sketch/series/` (its README has the corpus).
//!
//! Each reading is one case put through the translator under `histograms.mode: counters` with
//! `send_aggregation_metrics` and quantiles on. The encoder runs under the same settings, and every
//! series it writes on the v2 JSON route must match the translator's, in order: name, tags, type,
//! and value bit for bit. A unit is compared as present or absent: the exporter maps a UCUM unit to
//! a Datadog name (`ms` to `millisecond`), and `datadog_out` writes the record's unit as it is
//! (`docs/known-gaps/datadog.md`).

use logit_core::interner::intern;
use logit_core::{
    AttrMap, Event, EventBatch, ExpHistogram, Histogram, MetricKind, MetricRecord, Resource,
    Summary, Temporality,
};
use logit_proto::datadog::{DatadogEncoder, HistogramMode, HistogramOptions};
use serde_json::Value;
use std::path::PathBuf;
use std::sync::Arc;

fn corpus() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../testdata/differential/datadog-sketch/series")
}

/// A float the generator wrote as Go's shortest form.
fn num(v: &Value) -> f64 {
    match v.as_str().expect("a float is a string") {
        "+Inf" => f64::INFINITY,
        "-Inf" => f64::NEG_INFINITY,
        "NaN" => f64::NAN,
        s => s.parse().unwrap_or_else(|e| panic!("{s}: {e}")),
    }
}

fn opt_num(v: &Value) -> Option<f64> {
    (!v.is_null()).then(|| num(v))
}

fn u64s(v: &Value) -> Vec<u64> {
    v.as_array().unwrap().iter().map(|n| n.as_u64().unwrap()).collect()
}

/// The case's point as `otlp_in` decodes it.
fn kind(case: &Value) -> MetricKind {
    let input = &case["input"];
    match case["kind"].as_str().unwrap() {
        "explicit" => {
            let bounds: Vec<f64> = input["bounds"].as_array().unwrap().iter().map(num).collect();
            let counts = u64s(&input["counts"]);
            MetricKind::Histogram(Histogram {
                buckets: counts
                    .iter()
                    .enumerate()
                    .map(|(i, c)| (bounds.get(i).copied().unwrap_or(f64::INFINITY), *c))
                    .collect(),
                temporality: Temporality::Delta,
                sum: opt_num(&input["sum"]),
                min: opt_num(&input["min"]),
                max: opt_num(&input["max"]),
            })
        }
        "exponential" => {
            let side = |v: &Value| (v["offset"].as_i64().unwrap() as i32, u64s(&v["counts"]));
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
        "summary" => MetricKind::Summary(Summary {
            quantiles: input["quantiles"]
                .as_array()
                .unwrap()
                .iter()
                .map(|q| (num(&q[0]), num(&q[1])))
                .collect(),
            count: input["count"].as_u64().unwrap(),
            sum: num(&input["sum"]),
            temporality: Temporality::Cumulative,
        }),
        other => panic!("unknown kind {other}"),
    }
}

/// Bit-identical, with every `NaN` equal to every other.
fn same(a: f64, b: f64) -> bool {
    (a.is_nan() && b.is_nan()) || a.to_bits() == b.to_bits()
}

#[test]
fn every_series_matches_the_translator() {
    let mut paths: Vec<_> = std::fs::read_dir(corpus())
        .expect("the corpus is committed")
        .map(|e| e.unwrap().path())
        .collect();
    paths.sort();
    let mut diffs = Vec::new();
    let mut series_seen = 0;
    for path in &paths {
        let case: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        let name = path.file_stem().unwrap().to_string_lossy().into_owned();
        let mut record = MetricRecord::new(intern("case"), kind(&case));
        record.unit = Some(intern("ms"));
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![Event::metric(1_700_000_000_000_000_000, AttrMap::new(), record)],
        };
        let mut encoder = DatadogEncoder::new().with_histograms(HistogramOptions {
            mode: HistogramMode::Counters,
            send_aggregation_metrics: true,
        });
        let got: Vec<Value> = match encoder.encode_series_v2_json(&batch) {
            Some(body) => {
                let body: Value = serde_json::from_slice(&body).unwrap();
                body["series"].as_array().unwrap().clone()
            }
            None => Vec::new(),
        };
        let want = case["series"].as_array().unwrap();
        series_seen += want.len();
        if got.len() != want.len() {
            diffs.push(format!("{name}: {} series, want {}", got.len(), want.len()));
            continue;
        }
        for (i, (g, w)) in got.iter().zip(want).enumerate() {
            let wire_type = match g["type"].as_i64().unwrap() {
                1 => "count",
                3 => "gauge",
                t => panic!("{name}: type {t}"),
            };
            let tags: Vec<&str> =
                g["tags"].as_array().unwrap().iter().map(|t| t.as_str().unwrap()).collect();
            let want_tags: Vec<&str> =
                w["tags"].as_array().unwrap().iter().map(|t| t.as_str().unwrap()).collect();
            let value = g["points"][0]["value"].as_f64().unwrap();
            let checks = [
                (g["metric"] == w["name"], format!("name {} vs {}", g["metric"], w["name"])),
                (tags == want_tags, format!("tags {tags:?} vs {want_tags:?}")),
                (wire_type == w["type"], format!("type {wire_type} vs {}", w["type"])),
                (same(value, num(&w["value"])), format!("value {value} vs {}", w["value"])),
                (
                    g.get("unit").is_some() == !w["unit"].as_str().unwrap().is_empty(),
                    format!("unit {:?} vs {}", g.get("unit"), w["unit"]),
                ),
            ];
            for (ok, what) in checks {
                if !ok {
                    diffs.push(format!("{name} series {i}: {what}"));
                }
            }
        }
    }
    assert!(
        paths.len() >= 100 && series_seen >= 500,
        "{} cases, {series_seen} series",
        paths.len()
    );
    assert!(diffs.is_empty(), "{} differences:\n{}", diffs.len(), diffs.join("\n"));
}
