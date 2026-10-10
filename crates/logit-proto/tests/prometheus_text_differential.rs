//! The exposition decoder against Prometheus's own parser: `testdata/differential/prometheus-text/`,
//! hand-built bodies (`cases/`) and real exporters' scrape bodies
//! (`testdata/interop/prometheus-scrape/`), each beside the reading Prometheus 3.14.0's
//! `model/textparse` gave it (`reference/`). That directory's README says how the readings are made;
//! no Go runs here.
//!
//! Per body:
//! 1. the reading's `input` names the bytes this test reads, by length and CRC-32C, so a body
//!    re-recorded without regenerating its reading fails;
//! 2. [`Dialect::from_content_type`] picks the parser Prometheus picked: `textparse.New`'s, or the
//!    `fallback_scrape_protocol: PrometheusText0.0.4` one where Prometheus would fail the scrape;
//! 3. `text::parse_with` and then `families_to_events_with` count the declared skips and
//!    degradations (none for a recorded body), and give the declared `result`;
//! 4. where Prometheus read the whole body, logit's families, flattened back into exposition
//!    lines, equal Prometheus's series modulo the list below, and each family's metadata matches
//!    the first `# TYPE`/`# HELP`/`# UNIT` Prometheus read for it;
//! 5. where Prometheus stopped at an error (a scrape fails whole on one bad line, where logit skips
//!    the line), every series Prometheus read before it is in logit's output.
//!
//! A case's `divergent` lists the Prometheus series logit drops or reads differently, and
//! `divergence` says why and names the `docs/known-gaps/mappings.md` row; the test checks the list
//! is exact. A recorded body has neither: a real exporter's body must parse.
//!
//! Normalizations the comparison applies, each with its source:
//! - family, series, and label order: `prometheus/mod.rs`'s "Permitted normalizations";
//! - values bit for bit, except that any NaN equals any NaN (Prometheus rewrites a parsed NaN to its
//!   own `NormalNaN` bits, Rust's parse gives `f64::NAN`) and Prometheus's stale-marker NaN
//!   ([`logit_proto::prometheus::STALE_NAN_BITS`]) equals only itself;
//! - an `le` or `quantile` label compared as the `f64` it parses to: Prometheus rewrites a
//!   histogram's `le` and a summary's `quantile` to its own float spelling (`1` as `1.0`), and
//!   logit holds the bound as a number (`prometheus/mod.rs`'s `Point`);
//! - a series Prometheus reads twice counts once, the first: logit skips the repeat as
//!   `duplicate_series` (`text.rs`'s skip table), and Prometheus's appender keeps the first sample
//!   for a series and timestamp;
//! - a `+Inf` bucket only logit has, where Prometheus read none, and a `_count` only logit has, the
//!   `+Inf` total restated: `text.rs`'s "Leniencies";
//! - a histogram `_count` that differs from Prometheus's, for a series whose own `_count` line
//!   Prometheus read as disagreeing with its `+Inf` bucket or, with none, as a count below the
//!   highest bucket, which is the series `histogram_count_mismatch` counts: the bucket wins
//!   (`text.rs`'s skip table);
//! - a bucket, `_count`, or `_gcount` value Prometheus reads as a fraction equals logit's `u64`
//!   count when it rounds to it (`assemble.rs`'s `count_value`);
//! - `untyped` equals `unknown`: Prometheus's text parser reads `# TYPE x untyped` as
//!   `MetricTypeUnknown`;
//! - a counter's `# TYPE` name with or without `_total`, whichever Prometheus read: the model keeps
//!   the sample name (`prometheus/mod.rs`'s "Family naming");
//! - a text 0.0.4 `# UNIT` isn't compared: Prometheus's text parser reads it as a comment, logit as
//!   the family's unit (`text.rs`'s "Leniencies");
//! - an empty `# HELP` equals no `# HELP` (`MetricFamily::help`);
//! - a timestamp in milliseconds: text 0.0.4's integer milliseconds equal logit's nanoseconds over
//!   10^6 with no remainder; an OpenMetrics timestamp is Prometheus's
//!   `int64(ParseFloat(token) * 1000)`, truncated toward zero, which this test reproduces from logit's nanoseconds by writing them as
//!   exact decimal seconds, parsing that with Rust's correctly rounded `f64` parse (the same value
//!   `ParseFloat` gives the token, for a token of nine or fewer fractional digits), multiplying by
//!   1000 and truncating; an exemplar timestamp the same way, with logit's `0` read as absent;
//! - a `_created` line's value from logit's nanoseconds the same way, without the factor of 1000;
//! - an exemplar's `trace_id`/`span_id` labels rebuilt as lowercase hex from logit's `TraceRef`,
//!   which holds the id as bytes, and compared with Prometheus's case-insensitively;
//! - each histogram exemplar on the bucket its value falls in (`text.rs`'s "Writing is
//!   deterministic").

use logit_core::interner::resolve;
use logit_core::trace::to_hex;
use logit_core::{Exemplar, MetricKind, Registry};
use logit_proto::prometheus::text::{parse_with, Dialect};
use logit_proto::prometheus::{
    families_to_events_with, FamilyType, MetricFamily, Point, PrometheusDecoder, STALE_NAN_BITS,
};
use serde_json::Value as Json;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

const RECEIVED_AT: i64 = 1_700_000_000_000_000_000;

fn testdata() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata")
}

fn corpus_dir() -> PathBuf {
    testdata().join("differential/prometheus-text")
}

struct Body {
    name: String,
    bytes: Vec<u8>,
    content_type: Option<String>,
    reading: Json,
    /// `None` for a recorded body.
    expect: Option<Json>,
}

fn load_json(path: &Path) -> Json {
    serde_json::from_slice(
        &std::fs::read(path).unwrap_or_else(|e| panic!("{}: {e}", path.display())),
    )
    .unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn sorted_files(dir: &Path) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|entry| entry.unwrap().path())
        .collect();
    out.sort();
    out
}

/// The value of a sidecar's first `content-type:` line, as the reference program reads it.
fn content_type(headers: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(headers).expect("a UTF-8 sidecar");
    text.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("content-type")
            .then(|| value.trim_matches(|c| c == ' ' || c == '\t').to_string())
    })
}

fn corpus() -> Vec<Body> {
    let mut bodies = Vec::new();
    let cases = corpus_dir().join("cases");
    let mut case_bodies = 0;
    for path in sorted_files(&cases) {
        if matches!(path.extension().and_then(|e| e.to_str()), Some("txt" | "om")) {
            case_bodies += 1;
        }
    }
    for source in ["cases", "prometheus-scrape"] {
        for path in sorted_files(&corpus_dir().join("reference").join(source)) {
            let reading = load_json(&path);
            let name = format!("{source}/{}", path.file_stem().unwrap().to_str().unwrap());
            let input = &reading["input"];
            let body_path = testdata().join(input["path"].as_str().unwrap());
            let bytes = std::fs::read(&body_path)
                .unwrap_or_else(|e| panic!("{name}: {}: {e}", body_path.display()));
            assert_eq!(bytes.len() as u64, input["len"].as_u64().unwrap(), "{name}: input.len");
            assert_eq!(
                format!("0x{:08x}", crc32c::crc32c(&bytes)),
                input["crc32c"].as_str().unwrap(),
                "{name}: input.crc32c; regenerate with script/differential prom-text"
            );
            // Not `with_extension`: a case name can hold a dot (`ct-version-0.0.1`).
            let stem = body_path.to_str().unwrap();
            let stem = &stem[..stem.rfind('.').unwrap()];
            let sidecar = |suffix: &str| PathBuf::from(format!("{stem}.{suffix}"));
            let headers = std::fs::read(sidecar("headers")).unwrap();
            let content_type = content_type(&headers);
            assert_eq!(
                content_type.as_deref(),
                reading["content_type"].as_str(),
                "{name}: content_type"
            );
            let expect = (source == "cases").then(|| load_json(&sidecar("expect.json")));
            bodies.push(Body { name, bytes, content_type, reading, expect });
        }
    }
    let readings = bodies.iter().filter(|b| b.expect.is_some()).count();
    assert_eq!(readings, case_bodies, "every case has a reading and every reading a case");
    assert!(case_bodies >= 100, "{case_bodies} cases; is the corpus there?");
    let recorded = sorted_files(&testdata().join("interop/prometheus-scrape"))
        .iter()
        .filter(|path| path.extension().is_some_and(|ext| ext == "body"))
        .count();
    assert!(recorded > 0, "no recorded bodies");
    assert_eq!(bodies.len() - case_bodies, recorded, "every recorded body has a reading");
    bodies
}

/// Runs `check` on every body and fails once, naming every body that failed and why.
fn each_body(check: fn(&Body)) {
    let bodies = corpus();
    let failures: Vec<String> = bodies
        .iter()
        .filter_map(|body| {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| check(body))).err().map(
                |panic| {
                    panic
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                        .unwrap_or_default()
                },
            )
        })
        .collect();
    assert!(
        failures.is_empty(),
        "{} of {} bodies failed:\n{}",
        failures.len(),
        bodies.len(),
        failures.join("\n")
    );
}

// ---- what logit reads ------------------------------------------------------------------------

struct Decoded {
    result: Result<Vec<MetricFamily>, String>,
    skips: BTreeMap<String, u64>,
    degraded: BTreeMap<String, u64>,
}

fn decode(bytes: &[u8], dialect: Dialect) -> Decoded {
    let registry = Registry::new();
    let mut decoder = PrometheusDecoder::new().with_telemetry(registry.telemetry_for(
        "prometheus",
        "prometheus_in",
        "source",
    ));
    let result = parse_with(bytes, dialect, &mut decoder).map_err(|e| e.to_string());
    if let Ok(families) = &result {
        families_to_events_with(families, RECEIVED_AT, &mut decoder, &mut |_, _| {});
    }
    let mut skips = BTreeMap::new();
    let mut degraded = BTreeMap::new();
    for event in registry.drain(0) {
        let Some(reason) = event.attributes.get("reason").and_then(|v| v.as_str()) else {
            continue;
        };
        for metric in &event.metrics {
            let map = match resolve(metric.name) {
                "logit.input.metrics.skipped" => &mut skips,
                "logit.input.metrics.degraded" => &mut degraded,
                _ => continue,
            };
            let MetricKind::Sum(sum) = &metric.kind else { panic!("not a counter") };
            *map.entry(reason.to_string()).or_default() += sum.value as u64;
        }
    }
    Decoded { result, skips, degraded }
}

// ---- exposition lines --------------------------------------------------------------------------

/// A series' identity: its sample name and its labels, sorted, with an `le` or `quantile` value
/// replaced by the bits of the `f64` it parses to.
type Identity = (String, Vec<(String, String)>);

fn identity(name: &str, labels: &[(String, String)]) -> Identity {
    let mut labels: Vec<(String, String)> = labels
        .iter()
        .map(|(k, v)| {
            let v = match (k.as_str(), v.parse::<f64>()) {
                ("le" | "quantile", Ok(f)) => format!("f64:{:016x}", f.to_bits()),
                _ => v.clone(),
            };
            (k.clone(), v)
        })
        .collect();
    labels.sort();
    (name.to_string(), labels)
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Role {
    Value,
    /// A count the model holds as a `u64`.
    Count,
    /// A histogram's `_count`/`_gcount`, which is its `+Inf` total restated.
    Total,
    /// A `+Inf` bucket.
    Inf,
    Created,
}

#[derive(Debug, Clone)]
struct Line {
    role: Role,
    value: f64,
    /// For [`Role::Created`]: the instant, in nanoseconds.
    created: Option<i64>,
    ts_ns: Option<i64>,
    exemplars: Vec<Exemplar>,
}

/// One logit family as the exposition lines it stands for.
fn flatten(family: &MetricFamily, out: &mut Vec<(Identity, Line)>) {
    let name = family.name.as_str();
    for series in &family.series {
        let line = |role, value| Line {
            role,
            value,
            created: None,
            ts_ns: series.timestamp,
            exemplars: Vec::new(),
        };
        let mut push = |sample: String, extra: Option<(&str, f64)>, line: Line| {
            let mut labels = series.labels.clone();
            if let Some((key, bound)) = extra {
                labels.push((key.to_string(), bound.to_string()));
            }
            out.push((identity(&sample, &labels), line));
        };
        let created_name = match family.kind {
            FamilyType::Counter => name.strip_suffix("_total").unwrap_or(name),
            _ => name,
        };
        let (sum_suffix, count_suffix) = match family.kind {
            FamilyType::GaugeHistogram => ("_gsum", "_gcount"),
            _ => ("_sum", "_count"),
        };
        match &series.point {
            Point::Counter(v) | Point::Gauge(v) | Point::Unknown(v) => {
                let mut l = line(Role::Value, *v);
                l.exemplars = series.exemplars.clone();
                push(name.to_string(), None, l);
            }
            Point::StateSet(on) => {
                push(name.to_string(), None, line(Role::Value, if *on { 1.0 } else { 0.0 }))
            }
            Point::Info => push(format!("{name}_info"), None, line(Role::Value, 1.0)),
            Point::Histogram { buckets, sum, count } => {
                let mut exemplars = series.exemplars.clone();
                for (bound, cumulative) in buckets {
                    let role = if bound.is_infinite() { Role::Inf } else { Role::Count };
                    let mut l = line(role, *cumulative as f64);
                    let (here, rest): (Vec<_>, Vec<_>) =
                        exemplars.into_iter().partition(|e| e.value <= *bound);
                    l.exemplars = here;
                    exemplars = rest;
                    push(format!("{name}_bucket"), Some(("le", *bound)), l);
                }
                if let Some(sum) = sum {
                    push(format!("{name}{sum_suffix}"), None, line(Role::Value, *sum));
                }
                push(format!("{name}{count_suffix}"), None, line(Role::Total, *count as f64));
            }
            Point::Summary { quantiles, sum, count } => {
                for (q, v) in quantiles {
                    push(name.to_string(), Some(("quantile", *q)), line(Role::Value, *v));
                }
                if let Some(sum) = sum {
                    push(format!("{name}_sum"), None, line(Role::Value, *sum));
                }
                if let Some(count) = count {
                    push(format!("{name}_count"), None, line(Role::Count, *count as f64));
                }
            }
            Point::Stale => panic!("{name}: a stale marker has no text spelling"),
        }
        if let Some(created) = series.created {
            let mut l = line(Role::Created, f64::NAN);
            l.created = Some(created);
            l.ts_ns = None;
            push(format!("{created_name}_created"), None, l);
        }
    }
}

/// `nanos` as exact decimal seconds, parsed: the `f64` Prometheus's `ParseFloat` gives a token
/// with that decimal value.
fn seconds_f64(nanos: i64) -> f64 {
    let sign = if nanos < 0 { "-" } else { "" };
    let abs = nanos.unsigned_abs();
    format!("{sign}{}.{:09}", abs / 1_000_000_000, abs % 1_000_000_000).parse().unwrap()
}

/// Prometheus's millisecond reading of a timestamp logit holds as `nanos`, per dialect.
fn prometheus_ms(nanos: i64, dialect: Dialect) -> Option<i64> {
    match dialect {
        Dialect::Text0_0_4 => (nanos % 1_000_000 == 0).then_some(nanos / 1_000_000),
        Dialect::OpenMetrics1_0 => Some((seconds_f64(nanos) * 1000.0) as i64),
    }
}

fn bits(json: &Json) -> f64 {
    let hex = json.as_str().unwrap_or_else(|| panic!("not a bits string: {json}"));
    f64::from_bits(u64::from_str_radix(hex.trim_start_matches("0x"), 16).unwrap())
}

fn same_float(go: f64, logit: f64) -> bool {
    if go.to_bits() == STALE_NAN_BITS || logit.to_bits() == STALE_NAN_BITS {
        return go.to_bits() == logit.to_bits();
    }
    (go.is_nan() && logit.is_nan()) || go.to_bits() == logit.to_bits()
}

fn pairs(json: &Json) -> Vec<(String, String)> {
    json.as_array()
        .unwrap()
        .iter()
        .map(|p| (p[0].as_str().unwrap().to_string(), p[1].as_str().unwrap().to_string()))
        .collect()
}

fn parse_hex_id(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|b| b.is_ascii_hexdigit())
}

/// An exemplar's labels as the wire spells them, the trace reference rebuilt.
fn exemplar_labels(exemplar: &Exemplar) -> Vec<(String, String)> {
    let mut labels = Vec::new();
    if let Some(trace) = &exemplar.trace {
        labels.push(("trace_id".to_string(), to_hex(&trace.trace_id)));
        if let Some(span) = trace.span_id {
            labels.push(("span_id".to_string(), to_hex(&span)));
        }
    }
    for (key, value) in exemplar.filtered_attributes.iter() {
        labels.push((resolve(key).to_string(), value.as_str().unwrap().to_string()));
    }
    labels.sort();
    labels
}

/// Whether Prometheus's own `_count`/`_gcount` line `id` disagrees with that series' own bucket
/// lines the way `finish_series` counts `histogram_count_mismatch`, every count read as
/// `assemble.rs`'s `count_value` rounds it: a count other than the `+Inf` bucket's, or, with no
/// `+Inf` bucket, a count below the highest bucket. With none, `finish_series` takes the larger
/// of the two as the total, so a count above the highest bucket is the total and isn't excused.
fn count_disagrees(go: &[(Identity, &Json)], id: &Identity, entry: &Json) -> bool {
    let base = id.0.strip_suffix("_gcount").or_else(|| id.0.strip_suffix("_count"));
    let Some(base) = base else { return false };
    let bucket = format!("{base}_bucket");
    let mut totals: Vec<(f64, f64)> = go
        .iter()
        .filter(|((name, labels), _)| {
            *name == bucket
                && labels.iter().filter(|(k, _)| k != "le").eq(id.1.iter())
                && labels.iter().any(|(k, _)| k == "le")
        })
        .filter_map(|((_, labels), e)| {
            let (_, le) = labels.iter().find(|(k, _)| k == "le")?;
            let bound = f64::from_bits(u64::from_str_radix(le.strip_prefix("f64:")?, 16).ok()?);
            Some((bound, counted(bits(&e["value"]))?))
        })
        .collect();
    totals.sort_by(|a, b| a.0.total_cmp(&b.0));
    let Some(&(bound, highest)) = totals.last() else { return false };
    counted(bits(&entry["value"])).is_some_and(|count| {
        if bound == f64::INFINITY {
            count != highest
        } else {
            count < highest
        }
    })
}

/// A count line's value as `assemble.rs`'s `count_value` reads it, or `None` for one it skips.
fn counted(value: f64) -> Option<f64> {
    (value.is_finite() && value >= 0.0).then(|| value.round())
}

/// Whether logit's line reads as Prometheus's series entry, or why not.
fn compare(go: &Json, logit: &Line, dialect: Dialect, count_mismatch: bool) -> Result<(), String> {
    let value = bits(&go["value"]);
    let value_ok = match logit.role {
        Role::Value => same_float(value, logit.value),
        Role::Count | Role::Inf => value.is_finite() && value.round() == logit.value,
        Role::Total => count_mismatch || (value.is_finite() && value.round() == logit.value),
        Role::Created => same_float(value, seconds_f64(logit.created.unwrap())),
    };
    if !value_ok {
        let logit_value = match logit.role {
            Role::Created => seconds_f64(logit.created.unwrap()),
            _ => logit.value,
        };
        return Err(format!("value: Prometheus {value:?}, logit {logit_value:?}"));
    }
    let go_ts = go.get("ts_ms").map(|t| t.as_i64().unwrap());
    let logit_ts = logit.ts_ns.map(|ns| prometheus_ms(ns, dialect));
    if go_ts.map(Some) != logit_ts {
        return Err(format!(
            "ts_ms: Prometheus {go_ts:?}, logit {logit_ts:?} from {:?}",
            logit.ts_ns
        ));
    }
    let go_exemplar = go.get("exemplar");
    match (go_exemplar, logit.exemplars.as_slice()) {
        (None, []) => {}
        (Some(e), [ours]) => {
            let ts = e.get("ts_ms").map(|t| t.as_i64().unwrap());
            let ours_ts =
                (ours.timestamp != 0).then(|| prometheus_ms(ours.timestamp, dialect).unwrap());
            let theirs: Vec<(String, String)> = pairs(&e["labels"])
                .into_iter()
                .map(|(k, v)| match k.as_str() {
                    "trace_id" | "span_id" if parse_hex_id(&v) => (k, v.to_ascii_lowercase()),
                    _ => (k, v),
                })
                .collect();
            if theirs != exemplar_labels(ours)
                || !same_float(bits(&e["value"]), ours.value)
                || ts != ours_ts
            {
                return Err(format!("exemplar: Prometheus {e}, logit {ours:?}"));
            }
        }
        (e, ours) => return Err(format!("exemplars: Prometheus {e:?}, logit {ours:?}")),
    }
    Ok(())
}

// ---- what Prometheus read ----------------------------------------------------------------------

fn go_dialect(reading: &Json) -> Dialect {
    let parser = &reading["parser"];
    let parser = parser.as_str().or_else(|| parser["fallback"].as_str()).unwrap();
    match parser {
        "prom" => Dialect::Text0_0_4,
        "openmetrics" => Dialect::OpenMetrics1_0,
        other => panic!("parser {other:?}"),
    }
}

/// Prometheus's series entries, a repeat of an identity dropped (the first wins).
fn go_series(reading: &Json) -> Vec<(Identity, &Json)> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for entry in reading["entries"].as_array().unwrap() {
        if entry["kind"] != "series" {
            continue;
        }
        let id = identity(entry["name"].as_str().unwrap(), &pairs(&entry["labels"]));
        if seen.insert(id.clone()) {
            out.push((id, entry));
        }
    }
    out
}

/// The first value Prometheus read for each `(kind, name)` metadata pair.
fn go_metadata(reading: &Json) -> BTreeMap<(String, String), String> {
    let mut out = BTreeMap::new();
    for entry in reading["entries"].as_array().unwrap() {
        let kind = entry["kind"].as_str().unwrap();
        if kind == "series" {
            continue;
        }
        out.entry((kind.to_string(), entry["name"].as_str().unwrap().to_string()))
            .or_insert_with(|| entry["value"].as_str().unwrap().to_string());
    }
    out
}

fn counts(json: Option<&Json>) -> BTreeMap<String, u64> {
    json.and_then(Json::as_object)
        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.as_u64().unwrap())).collect())
        .unwrap_or_default()
}

fn divergent(expect: Option<&Json>) -> Vec<Identity> {
    expect
        .and_then(|e| e.get("divergent"))
        .and_then(Json::as_array)
        .map(|list| list.iter().map(|d| identity(d[0].as_str().unwrap(), &pairs(&d[1]))).collect())
        .unwrap_or_default()
}

fn check(body: &Body) {
    let name = &body.name;
    let expect = body.expect.as_ref();
    let divergence = expect.and_then(|e| e.get("divergence")).is_some();
    let dialect = Dialect::from_content_type(body.content_type.as_deref().unwrap_or(""));
    let dialect_agrees = dialect == go_dialect(&body.reading);

    let decoded = decode(&body.bytes, dialect);
    let wanted_result = expect.map_or("ok", |e| e["result"].as_str().unwrap());
    let got_result = if decoded.result.is_ok() { "ok" } else { "malformed" };
    assert_eq!(got_result, wanted_result, "{name}: result {:?}", decoded.result.as_ref().err());
    assert_eq!(decoded.skips, counts(expect.and_then(|e| e.get("skips"))), "{name}: skips");
    assert_eq!(
        decoded.degraded,
        counts(expect.and_then(|e| e.get("degraded"))),
        "{name}: degraded"
    );
    let go_ok = body.reading["error"].is_null();
    if decoded.result.is_err() || !dialect_agrees {
        assert!(
            divergent(expect).is_empty(),
            "{name}: no series is compared here, so `divergent` must be empty"
        );
    }
    let Ok(families) = decoded.result else {
        assert_eq!(go_ok, divergence, "{name}: `divergence` iff Prometheus read what logit failed");
        return;
    };
    if !dialect_agrees {
        assert!(
            divergence,
            "{name}: logit reads {:?} as {dialect:?}, Prometheus as {:?}",
            body.content_type,
            go_dialect(&body.reading)
        );
        return;
    }

    let mut lines = Vec::new();
    for family in &families {
        flatten(family, &mut lines);
    }
    let mut ours: BTreeMap<Identity, Line> = BTreeMap::new();
    for (id, line) in lines {
        assert!(ours.insert(id.clone(), line).is_none(), "{name}: logit has {id:?} twice");
    }
    let go = go_series(&body.reading);
    let mut differs = Vec::new();
    let mut matched = std::collections::HashSet::new();
    for (id, entry) in &go {
        match ours.get(id) {
            Some(line) => {
                let count_mismatch = line.role == Role::Total && count_disagrees(&go, id, entry);
                if let Err(why) = compare(entry, line, dialect, count_mismatch) {
                    differs.push((id.clone(), why));
                }
                matched.insert(id.clone());
            }
            None => differs.push((id.clone(), "absent from logit".to_string())),
        }
    }
    let differing: Vec<Identity> = differs.iter().map(|(id, _)| id.clone()).collect();
    assert_eq!(
        differing,
        divergent(expect),
        "{name}: the series Prometheus and logit read differently ({differs:?}) aren't the \
         declared `divergent` list"
    );
    assert_eq!(
        !differing.is_empty(),
        divergence,
        "{name}: `divergence` iff some series reads differently"
    );
    if !go_ok {
        return;
    }
    // A divergent case's series can come back under another identity (Prometheus keeps leading
    // spaces in a name, this decoder trims them), so only an agreeing case is checked this way.
    for (id, line) in ours.iter().filter(|_| !divergence) {
        let restated = matches!(line.role, Role::Inf | Role::Total);
        assert!(
            matched.contains(id) || restated,
            "{name}: logit has {id:?}, which Prometheus didn't read"
        );
    }
    check_metadata(name, &families, &body.reading, dialect);
}

/// Each family's `# TYPE`, `# HELP`, and (OpenMetrics only) `# UNIT` against the first of each
/// Prometheus read for the name its `# TYPE` line carries.
fn check_metadata(name: &str, families: &[MetricFamily], reading: &Json, dialect: Dialect) {
    let metadata = go_metadata(reading);
    for family in families {
        let mut candidates = vec![family.name.as_str()];
        if family.kind == FamilyType::Counter {
            candidates.extend(family.name.strip_suffix("_total"));
        }
        let type_name = candidates
            .iter()
            .find(|c| metadata.contains_key(&("type".to_string(), c.to_string())))
            .copied()
            .unwrap_or(candidates[candidates.len() - 1]);
        let get = |kind: &str| metadata.get(&(kind.to_string(), type_name.to_string()));
        let ours = match family.kind {
            FamilyType::Untyped => "unknown",
            kind => kind.as_str(),
        };
        match get("type") {
            Some(theirs) => assert_eq!(theirs, ours, "{name}: {type_name}'s type"),
            None => assert!(
                matches!(family.kind, FamilyType::Unknown | FamilyType::Untyped),
                "{name}: {type_name} is {ours} with no # TYPE Prometheus read"
            ),
        }
        let help = get("help").filter(|h| !h.is_empty());
        assert_eq!(help, family.help.as_ref(), "{name}: {type_name}'s help");
        if dialect == Dialect::OpenMetrics1_0 {
            let unit = get("unit").filter(|u| !u.is_empty());
            assert_eq!(unit, family.unit.as_ref(), "{name}: {type_name}'s unit");
        }
    }
}

#[test]
fn every_body_reads_as_prometheus_reads_it_modulo_the_named_normalizations() {
    each_body(check);
}

/// A `divergence` ends `(docs/known-gaps/mappings.md: <phrase>)`, and the phrase opens a
/// `decode (Prometheus)` row there.
#[test]
fn every_divergence_names_a_known_gaps_row() {
    let gaps = std::fs::read_to_string(testdata().join("../docs/known-gaps/mappings.md")).unwrap();
    let rows: Vec<&str> = gaps
        .lines()
        .filter(|line| line.trim_start().starts_with("| decode (Prometheus) |"))
        .collect();
    let mut named = 0;
    for body in corpus() {
        let Some(divergence) = body.expect.as_ref().and_then(|e| e.get("divergence")) else {
            continue;
        };
        let text = divergence.as_str().unwrap();
        let phrase = text
            .rsplit_once("(docs/known-gaps/mappings.md: ")
            .and_then(|(_, rest)| rest.strip_suffix(')'))
            .unwrap_or_else(|| panic!("{}: no mappings.md row named in {text:?}", body.name));
        assert!(
            rows.iter().any(|row| row.contains(&format!("| decode (Prometheus) | {phrase}"))),
            "{}: no `decode (Prometheus)` row opens with {phrase:?}",
            body.name
        );
        named += 1;
    }
    assert!(named > 0, "no divergent case; is the corpus there?");
}
