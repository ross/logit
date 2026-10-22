//! The Datadog sketch differential corpus (`testdata/differential/datadog-sketch/README.md`): every
//! case's conversion against the translator's own sketch, and every Go `math` result the
//! conversion is built from against [`gomath`].

use super::*;
use crate::Temporality;
use serde_json::Value;
use std::path::PathBuf;

fn corpus() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../testdata/differential/datadog-sketch")
}

fn read(path: &std::path::Path) -> Value {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
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

/// Bit-identical, with every `NaN` equal to every other.
fn same(a: f64, b: f64) -> bool {
    (a.is_nan() && b.is_nan()) || a.to_bits() == b.to_bits()
}

/// An OTLP point's bounds and counts as `otlp_in` decodes them into a [`Histogram`].
fn histogram(input: &Value) -> Histogram {
    let bounds: Vec<f64> = input["bounds"].as_array().unwrap().iter().map(num).collect();
    // A bucketless point with a nonzero `count` is one `+Inf` bucket of it, as `otlp_in` reads it.
    let mut counts = u64s(&input["counts"]);
    let count = input["count"].as_u64().unwrap();
    if counts.is_empty() && count > 0 {
        counts.push(count);
    }
    Histogram {
        buckets: counts
            .iter()
            .enumerate()
            .map(|(i, c)| (bounds.get(i).copied().unwrap_or(f64::INFINITY), *c))
            .collect(),
        temporality: Temporality::Delta,
        sum: opt_num(&input["sum"]),
        min: opt_num(&input["min"]),
        max: opt_num(&input["max"]),
    }
}

fn exp_histogram(input: &Value) -> ExpHistogram {
    let side = |v: &Value| (v["offset"].as_i64().unwrap() as i32, u64s(&v["counts"]));
    ExpHistogram {
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
    }
}

fn outcome(result: &Result<AgentSketch, ConversionError>) -> &'static str {
    match result {
        Ok(_) => "sketch",
        Err(ConversionError::Empty) => "empty",
        Err(ConversionError::TooManyObservations) => "too_many_observations",
        Err(ConversionError::NonMonotonicBounds) => "non_monotonic_bounds",
        Err(ConversionError::Unconvertible) => "unconvertible",
    }
}

/// Every difference between one case's conversion and the translator's sketch.
fn compare(name: &str, got: &AgentSketch, want: &Value) -> Vec<String> {
    let mut diffs = Vec::new();
    let keys: Vec<i32> =
        want["k"].as_array().unwrap().iter().map(|k| k.as_i64().unwrap() as i32).collect();
    let counts = u64s(&want["n"]);
    let store = AgentStore { bins: keys.into_iter().zip(counts).collect() };
    let (positive, negative, zero) = store.fold();
    let s = &got.sketch;
    if s.positive != positive {
        diffs.push(format!("{name}: positive bins {:?}, want {:?}", s.positive, positive));
    }
    if s.negative != negative {
        diffs.push(format!("{name}: negative bins {:?}, want {:?}", s.negative, negative));
    }
    if !same(s.zero_count, zero) {
        diffs.push(format!("{name}: zero count {}, want {zero}", s.zero_count));
    }
    let cnt = want["cnt"].as_i64().unwrap() as f64;
    if !same(s.count, cnt) {
        diffs.push(format!("{name}: cnt {}, want {cnt}", s.count));
    }
    for (field, got) in [("sum", s.sum), ("min", s.min), ("max", s.max), ("avg", got.avg)] {
        let want = num(&want[field]);
        if !same(got, want) {
            diffs.push(format!("{name}: {field} {got:e}, want {want:e}"));
        }
    }
    diffs
}

/// Cases `logit` can't match: a point with no buckets but a `count`, a `min`, and a `max`. `otlp_in`
/// reads it as one `+Inf` bucket, the model shape of `explicit-single-bucket-min-max`, which the
/// translator sketches at one key, where for the bucketless point it spreads the count between
/// `min` and `max` (`docs/known-gaps/datadog.md`).
const COLLAPSED: [&str; 1] = ["explicit-no-buckets-count-min-max"];

#[test]
fn every_case_converts_as_the_translator_does() {
    let mut paths: Vec<_> = std::fs::read_dir(corpus().join("cases"))
        .expect("the corpus is committed")
        .map(|e| e.unwrap().path())
        .collect();
    paths.sort();
    let mut diffs = Vec::new();
    let mut outcomes = std::collections::BTreeMap::new();
    let mut collapsed = 0;
    for path in &paths {
        let case = read(path);
        let name = case["name"].as_str().unwrap();
        let name = format!("{}-{name}", case["kind"].as_str().unwrap());
        if COLLAPSED.contains(&name.as_str()) {
            collapsed += 1;
            continue;
        }
        let result = match case["kind"].as_str().unwrap() {
            "explicit" => histogram_to_agent(&histogram(&case["input"])),
            "exponential" => exp_histogram_to_agent(&exp_histogram(&case["input"])),
            kind => panic!("{name}: unknown kind {kind}"),
        };
        let want = case["outcome"].as_str().unwrap();
        *outcomes.entry(want.to_string()).or_insert(0) += 1;
        if outcome(&result) != want {
            diffs.push(format!("{name}: {}, want {want}", outcome(&result)));
        } else if let Ok(got) = &result {
            diffs.extend(compare(&name, got, &case["sketch"]));
        }
    }
    assert_eq!(collapsed, COLLAPSED.len(), "every collapsed case is in the corpus");
    // A corpus that lost its cases, or every case of one outcome, passes nothing.
    assert!(paths.len() >= 200, "{} cases", paths.len());
    for o in ["sketch", "empty", "too_many_observations", "non_monotonic_bounds", "unconvertible"] {
        assert!(outcomes.contains_key(o), "no case with outcome {o}: {outcomes:?}");
    }
    assert!(diffs.is_empty(), "{} differences:\n{}", diffs.len(), diffs.join("\n"));
}

#[test]
fn every_go_math_result_matches() {
    let mut diffs = Vec::new();
    let primitives = read(&corpus().join("primitives.json"));
    for p in primitives.as_array().unwrap() {
        let args: Vec<f64> = p["args"].as_array().unwrap().iter().map(num).collect();
        let want = num(&p["out"]);
        let (fun, got) = match p["fn"].as_str().unwrap() {
            // The Agent's `gamma.ln`, which `Mapping::agent` computes with Rust's `ln_1p`.
            "log1p" => ("log1p", Mapping::agent().gamma_ln),
            "log" => ("log", gomath::log(args[0])),
            "exp" => ("exp", gomath::exp(args[0])),
            "pow" => ("pow", gomath::pow(args[0], args[1])),
            f => panic!("unknown function {f}"),
        };
        if !same(got, want) {
            diffs.push(format!(
                "{fun}{args:?} = {got:e} ({:#x}), want {want:e} ({:#x})",
                got.to_bits(),
                want.to_bits()
            ));
        }
    }
    assert!(primitives.as_array().unwrap().len() > 1000);
    assert!(diffs.is_empty(), "{} differences:\n{}", diffs.len(), diffs.join("\n"));
}

/// The Agent's bias, from Go's `Log`, is the one `Mapping::agent` keys with.
#[test]
fn agent_config_matches_mapping_agent() {
    let config = AgentConfig::new();
    assert_eq!(config.bias as f64, Mapping::agent().offset);
    assert_eq!(config.key(1.0), config.bias as i32);
    assert_eq!(config.key(0.0), 0);
    assert_eq!(config.key(f64::MAX), UV_INF);
}

/// A last bucket with a finite bound converts as if an empty open-ended bucket followed it.
#[test]
fn a_finite_last_bound_has_an_empty_bucket_above_it() {
    let h = |buckets: Vec<(f64, u64)>| Histogram {
        buckets,
        temporality: Temporality::Delta,
        sum: Some(20.0),
        min: None,
        max: None,
    };
    let finite = histogram_to_agent(&h(vec![(1.0, 2), (10.0, 5)])).unwrap();
    let open = histogram_to_agent(&h(vec![(1.0, 2), (10.0, 5), (f64::INFINITY, 0)])).unwrap();
    assert_eq!(finite, open);
}

/// `appendSafe` and `trimLeft`: a count past `uint16` takes several bins of one key, remainder
/// first, and folding keeps the lowest kept bin's key.
#[test]
fn the_store_splits_and_folds_bins_as_the_agent_does() {
    let mut bins = Vec::new();
    append_safe(&mut bins, 7, 2 * MAX_BIN + 3);
    assert_eq!(bins, vec![(7, 3), (7, MAX_BIN), (7, MAX_BIN)]);

    let many: Vec<(i32, u64)> = (1..=AGENT_BIN_LIMIT as i32 + 2).map(|k| (k, 1)).collect();
    let trimmed = trim_left(many);
    assert_eq!(trimmed.len(), AGENT_BIN_LIMIT);
    assert_eq!(trimmed[0], (3, 3));
}
