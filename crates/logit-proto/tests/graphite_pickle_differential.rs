//! The carbon pickle reader against CPython's own reading:
//! `testdata/differential/graphite-pickle/`, a corpus of payloads real CPython 3.12 and Python 2.7
//! wrote, each beside a JSON file holding `pickle.loads`'s reading, carbon's receiver's reading,
//! and the verdict `PickleReader` must give. `testdata/differential/README.md` says how it's
//! generated; no Python runs here.
//!
//! Per case:
//! - `read_datapoints` gives the declared verdict: a `malformed` case fails naming its opcode or
//!   message, and an `ok` case yields its datapoints bit for bit, skips the declared count, and
//!   accounts for every item of the batch CPython read (`datapoints + skipped == items`);
//! - `divergence` is present if and only if that verdict differs from carbon's reading: a frame
//!   carbon drops, or the datapoints carbon's `stringReceived` hands on;
//! - an `ok` case through `GraphiteDecoder` in pickle mode yields the module doc's event for each
//!   datapoint, or the skip its decode table names, and the skip counts equal `decoder_skips`;
//!   where carbon's `metricReceived` treats a datapoint otherwise, `decoder_divergence` says why.
//!
//! The `interop-*.json` readings cover the recorded captures under `testdata/interop/graphite/`,
//! read in place with their 4-byte length prefix stripped.

use bytes::Bytes;
use logit_core::{Event, MetricKind, Registry, Resource, Value};
use logit_proto::graphite::pickle::PickleReader;
use logit_proto::graphite::{GraphiteDecoder, Protocol};
use logit_proto::Decoder;
use serde_json::Value as Json;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const RECEIVED_AT: i64 = 1_699_000_000_000_000_000;

/// Every skip reason the decoder can count on the pickle path besides `bad_shape`, which is the
/// reader's own skip count.
const DECODER_SKIPS: [&str; 4] = ["bad_timestamp", "non_finite_value", "bad_tag", "bad_line"];

fn testdata() -> PathBuf {
    // `crates/logit-proto/` -> repository root.
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata")
}

/// `(path, timestamp bits, value bits)`: `None` for a path the reading holds as a `str` with a
/// lone surrogate, which no `&str` equals.
type Point = (Option<String>, u64, u64);

struct Case {
    name: String,
    data: Vec<u8>,
    json: Json,
}

impl Case {
    fn field(&self, key: &str) -> &Json {
        self.json.get(key).unwrap_or_else(|| panic!("{}: no {key:?}", self.name))
    }
}

fn corpus() -> Vec<Case> {
    let dir = testdata().join("differential/graphite-pickle");
    let mut names: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|entry| entry.unwrap().path())
        .collect();
    names.sort();
    let mut cases = Vec::new();
    for path in &names {
        match path.extension().and_then(|e| e.to_str()) {
            Some("json") => {}
            Some("pkl") => {
                assert!(path.with_extension("json").exists(), "{}: no reading", path.display());
                continue;
            }
            _ => continue,
        }
        let name = path.file_stem().unwrap().to_str().unwrap().to_string();
        let json: Json = serde_json::from_slice(&std::fs::read(path).unwrap())
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        let data = match json.get("source").and_then(Json::as_str) {
            Some(source) => {
                let stream = std::fs::read(testdata().join(source)).unwrap();
                let (prefix, payload) = stream.split_first_chunk::<4>().unwrap();
                let declared = json["length_prefix"].as_u64().unwrap();
                assert_eq!(u64::from(u32::from_be_bytes(*prefix)), declared, "{name}: prefix");
                assert_eq!(payload.len() as u64, declared, "{name}: one frame, nothing after");
                payload.to_vec()
            }
            None => std::fs::read(path.with_extension("pkl")).unwrap(),
        };
        cases.push(Case { name, data, json });
    }
    assert!(
        cases.len() >= 60,
        "the corpus holds {} readings; is the directory there?",
        cases.len()
    );
    cases
}

/// A run-length-encoded array (`gen_cases.py`'s `rle`), expanded.
fn expand(items: &Json) -> Vec<&Json> {
    let mut out = Vec::new();
    for item in items.as_array().expect("an array") {
        match (item.get("repeat"), item.get("of")) {
            (Some(n), Some(of)) => {
                out.extend(std::iter::repeat_n(of, n.as_u64().unwrap() as usize))
            }
            _ => out.push(item),
        }
    }
    out
}

fn bits(json: &Json) -> u64 {
    let hex = json["f64"].as_str().unwrap_or_else(|| panic!("not a tagged f64: {json}"));
    u64::from_str_radix(hex.trim_start_matches("0x"), 16).unwrap()
}

fn path(json: &Json) -> Option<String> {
    match json {
        Json::String(s) => Some(s.clone()),
        _ if json.get("surrogates").is_some() => None,
        _ => panic!("not a tagged path: {json}"),
    }
}

fn point(json: &Json) -> Point {
    (path(&json[0]), bits(&json[1]), bits(&json[2]))
}

/// What the reader makes of `data`.
fn read(data: &[u8]) -> Result<(Vec<Point>, usize), String> {
    let mut points = Vec::new();
    let skipped = PickleReader::new()
        .read_datapoints(data, |p, ts, v| {
            points.push((Some(p.to_string()), ts.to_bits(), v.to_bits()))
        })
        .map_err(|e| e.to_string())?;
    Ok((points, skipped))
}

/// How many items the batch holds, as CPython read it or, where CPython's ASCII default failed
/// on a Python 2 `str`, as carbon's UTF-8 unpickler did.
fn items(case: &Case) -> usize {
    if let Some(list) = case.field("cpython").get("list") {
        return expand(list).len();
    }
    case.field("carbon")["items"]
        .as_u64()
        .unwrap_or_else(|| panic!("{}: neither reading has the batch's length", case.name))
        as usize
}

/// The datapoints carbon's `stringReceived` handed to `metricReceived`, and each one's fate there.
fn carbon_received(case: &Case) -> Vec<(Point, &Json)> {
    expand(&case.field("carbon")["received"]).into_iter().map(|r| (point(r), &r[3])).collect()
}

/// Runs `check` on every case and fails once, naming every case that failed and why, so one run
/// shows the whole corpus's state.
fn each_case(check: fn(&Case)) {
    let cases = corpus();
    let failures: Vec<String> = cases
        .iter()
        .filter_map(|case| {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| check(case)));
            outcome.err().map(|panic| {
                let message = panic
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_default();
                message
            })
        })
        .collect();
    assert!(
        failures.is_empty(),
        "{} of {} cases failed:\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n")
    );
}

#[test]
fn the_reader_gives_each_cases_declared_verdict_and_divergences_are_named() {
    each_case(check_verdict);
}

fn check_verdict(case: &Case) {
    {
        let name = &case.name;
        let logit = case.field("logit");
        let carbon = case.field("carbon");
        let carbon_ok = carbon["outcome"] == "ok";
        let received = carbon_received(case);
        let result = read(&case.data);

        let agrees = match logit["verdict"].as_str().unwrap() {
            "malformed" => {
                let err = result.as_ref().expect_err(&format!("{name}: must fail the frame"));
                let wanted = match (logit.get("opcode"), logit.get("message")) {
                    (Some(op), _) => {
                        format!("pickle opcode {} is not permitted", op.as_str().unwrap())
                    }
                    (_, Some(message)) => message.as_str().unwrap().to_string(),
                    _ => panic!("{name}: a malformed verdict names an opcode or a message"),
                };
                assert!(err.contains(&wanted), "{name}: {err:?} doesn't contain {wanted:?}");
                !carbon_ok && received.is_empty()
            }
            "ok" => {
                let (points, skipped) =
                    result.unwrap_or_else(|e| panic!("{name}: must read, failed: {e}"));
                let expected: Vec<Point> = match &logit["datapoints"] {
                    Json::String(s) if s == "carbon" => {
                        assert!(carbon_ok, "{name}: reads as carbon's, but carbon's frame failed");
                        received.iter().map(|(p, _)| p.clone()).collect()
                    }
                    list => expand(list).into_iter().map(point).collect(),
                };
                assert_eq!(points, expected, "{name}: datapoints");
                assert_eq!(skipped as u64, logit["skipped"].as_u64().unwrap(), "{name}: skipped");
                assert_eq!(points.len() + skipped, items(case), "{name}: every item accounted for");
                let carbon_points: Vec<Point> = received.iter().map(|(p, _)| p.clone()).collect();
                carbon_ok && carbon_points == points
            }
            other => panic!("{name}: unknown verdict {other:?}"),
        };
        let named = case.json.get("divergence").is_some();
        assert_eq!(
            named,
            !agrees,
            "{name}: the reader {} carbon, so `divergence` must be {}",
            if agrees { "agrees with" } else { "differs from" },
            if agrees { "absent" } else { "present" },
        );
    }
}

/// What the module doc's decode table makes of one datapoint the reader yields.
#[derive(Debug, PartialEq)]
enum Fate {
    Event { name: String, tags: Vec<(String, String)>, timestamp: i64, value: u64 },
    Skip(&'static str),
}

fn fate(path: &str, ts: f64, value: f64) -> Fate {
    let timestamp = if ts == -1.0 {
        RECEIVED_AT
    } else if !ts.is_finite() || ts <= 0.0 {
        return Fate::Skip("bad_timestamp");
    } else {
        let whole = ts.trunc();
        let sub = ((ts - whole) * 1e9).round() as i64;
        (whole as i64).saturating_mul(1_000_000_000).saturating_add(sub)
    };
    if !value.is_finite() {
        return Fate::Skip("non_finite_value");
    }
    let mut segments = path.split(';');
    let name = segments.next().unwrap().to_string();
    let mut tags = BTreeMap::new();
    for segment in segments {
        match segment.split_once('=') {
            Some((k, v)) if !k.is_empty() && !v.is_empty() => {
                tags.insert(k.to_string(), v.to_string());
            }
            _ => return Fate::Skip("bad_tag"),
        }
    }
    if name.is_empty() {
        return Fate::Skip("bad_line");
    }
    Fate::Event { name, tags: tags.into_iter().collect(), timestamp, value: value.to_bits() }
}

fn event_fate(event: &Event) -> Fate {
    assert_eq!(event.metrics.len(), 1);
    let MetricKind::Gauge(value) = event.metrics[0].kind else { panic!("not a Gauge") };
    let mut tags: Vec<_> = event
        .attributes
        .iter()
        .map(|(k, v)| match v {
            Value::Str(s) => (
                logit_core::interner::resolve(k).to_string(),
                std::str::from_utf8(s).unwrap().to_string(),
            ),
            other => panic!("a tag that isn't a string: {other:?}"),
        })
        .collect();
    tags.sort();
    Fate::Event {
        name: logit_core::interner::resolve(event.metrics[0].name).to_string(),
        tags,
        timestamp: event.timestamp,
        value: value.to_bits(),
    }
}

/// `logit.input.metrics.skipped` by reason.
fn skipped(registry: &Registry) -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    for event in registry.drain(0) {
        let Some(reason) = event.attributes.get("reason").and_then(|v| v.as_str()) else {
            continue;
        };
        for m in &event.metrics {
            if logit_core::interner::resolve(m.name) == "logit.input.metrics.skipped" {
                let MetricKind::Sum(sum) = &m.kind else { panic!("not a counter") };
                *out.entry(reason.to_string()).or_default() += sum.value as u64;
            }
        }
    }
    out
}

#[test]
fn the_pickle_decoder_applies_the_decode_table_to_every_case_that_reads() {
    each_case(check_decoder);
}

fn check_decoder(case: &Case) {
    {
        let name = &case.name;
        let Ok((points, reader_skipped)) = read(&case.data) else { return };
        let registry = Registry::new();
        let mut decoder = GraphiteDecoder::new(Arc::new(Resource::default()))
            .with_protocol(Protocol::Pickle)
            .with_telemetry(registry.telemetry_for("graphite_in", "graphite_in", "listener"));
        let mut events = Vec::new();
        decoder
            .decode_into(Bytes::from(case.data.clone()), RECEIVED_AT, &mut events)
            .unwrap_or_else(|e| panic!("{name}: the reader read it, the decoder failed: {e}"));

        let fates: Vec<Fate> = points
            .iter()
            .map(|(p, ts, v)| fate(p.as_deref().unwrap(), f64::from_bits(*ts), f64::from_bits(*v)))
            .collect();
        let wanted_events: Vec<&Fate> =
            fates.iter().filter(|f| matches!(f, Fate::Event { .. })).collect();
        let got: Vec<Fate> = events.iter().map(event_fate).collect();
        assert_eq!(got.iter().collect::<Vec<_>>(), wanted_events, "{name}: events");

        let counted = skipped(&registry);
        let declared = case.json.get("decoder_skips").cloned().unwrap_or_default();
        for reason in DECODER_SKIPS {
            let by_table = fates.iter().filter(|f| **f == Fate::Skip(reason)).count() as u64;
            let wanted = declared.get(reason).and_then(Json::as_u64).unwrap_or(0);
            assert_eq!(by_table, wanted, "{name}: decoder_skips[{reason:?}] against the table");
            assert_eq!(counted.get(reason).copied().unwrap_or(0), wanted, "{name}: {reason}");
        }
        assert_eq!(counted.get("bad_shape").copied().unwrap_or(0), reader_skipped as u64, "{name}");

        // Where the reader agrees with carbon's `stringReceived`, compare each datapoint's fate
        // with carbon's `metricReceived`: `store` an event, `now` the receipt time, `nan` a skip.
        let received = carbon_received(case);
        let carbon_points: Vec<Point> = received.iter().map(|(p, _)| p.clone()).collect();
        if case.json.get("divergence").is_some() || carbon_points != points {
            assert!(
                case.json.get("decoder_divergence").is_none(),
                "{name}: compared only when the reader agrees"
            );
            return;
        }
        let differs = fates.iter().zip(&received).any(|(f, (_, carbon))| {
            match (carbon.as_str().unwrap(), f) {
                ("store", Fate::Event { timestamp, .. }) => *timestamp == RECEIVED_AT,
                ("now", Fate::Event { timestamp, .. }) => *timestamp != RECEIVED_AT,
                ("nan", Fate::Skip(_)) => false,
                _ => true,
            }
        });
        assert_eq!(
            case.json.get("decoder_divergence").is_some(),
            differs,
            "{name}: `decoder_divergence` must be present if and only if a datapoint's fate \
             differs from carbon's metricReceived"
        );
    }
}
