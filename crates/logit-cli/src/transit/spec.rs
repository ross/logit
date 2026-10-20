//! The transit spec's generator: runs every case of `spec/transit/` through every flow that reads
//! its source system, compares each run's document with its committed expectation, and checks the
//! run against `mapping.yaml` (`super::classes`). `spec/transit/README.md` is the reader's account.
//!
//! **Tests.** The generator is three kinds of test, so nextest spreads the flows across workers:
//!
//! - `cases` ([`check_cases`]): the table loads, each case's `.json` is current, nothing under
//!   `expected/` or `ledger/` belongs to no flow, and the `flow_tests!` list names each
//!   `flows/*.yaml` once, under its stem with `-` as `_`.
//! - `flow::<flow>`, one per entry of `flow_tests!` ([`check_flow`]): the flow's cases, the class
//!   check, `expected/<flow>.json`, and `ledger/<flow>.json`, the [`classes::Fragment`] of what
//!   the flow's runs fed the class check's [`Ledger`].
//! - `ledger` ([`check_ledger`]): every flow's committed ledger file merged into one `Ledger`, its
//!   checks across flows (`super::classes`'s "Across every flow"), and `coverage.md`. It runs no
//!   flow. Since a flow's test fails when its ledger file is stale, every test passing means the
//!   merged files hold what one run of every flow would have built.
//!
//! Under `TRANSIT_UPDATE=1` ([`UPDATE`]) each rewrites what it generates: `cases` a wire case's
//! `.json` from its wire file, removing a file under `expected/` or `ledger/` no flow produces; a
//! flow test its expectation and ledger files; `ledger` `coverage.md`. A flow reads the case files
//! and `ledger` reads the ledger files, so `script/transit gen` runs `cases`, the flows, and
//! `ledger` in that order. Unset, each compares, and a stale or missing file or case fails with a
//! diff and the command that regenerates it. A case `.json` with no `.meta.yaml` (and named by no
//! `wire:`) fails.
//!
//! **Expectations.** `expected/<flow>.json` is one JSON object mapping each case's name to its
//! driver document (`super::report`), keys sorted, each document's lines indented under its key so
//! a diff stays inside the case that changed ([`render_flow`]). One file per flow grows with the
//! flows alone, where a file per case would grow with flows times cases. A missing file fails
//! naming the flow and its path; a missing or stale case and a key no case has each fail naming
//! the flow and the case. `gen` writes a flow's expectation and ledger files once every case in
//! the flow has run, and only when none of them failed the class check: a class failure leaves
//! both untouched.
//!
//! **Cases.** A case is `cases/<system>/<case>.meta.yaml` beside `<case>.json`. The meta names
//! the features from `features/<system>.yaml` the case `exercises`, an optional `wire:` file
//! (relative to the case's directory) the `.json` is decoded from by `<system>_in`'s decoder at
//! [`RECEIVED_AT`] (with the request's `name: value` headers from a `.headers` sidecar beside
//! the wire file, as [`decode_wire`] reads them), and optional `tolerance:` entries (`{path, abs}`: an RFC 6901 pointer into
//! each event of the document, and the most the number there may differ by). A hand-written case
//! has no `wire:`, and its `.json` must read back to itself through `logit_proto::event_json`
//! and `format: json`, so each case has one spelling.
//!
//! **Attribute order.** `format: json` writes an attribute map in the process interner's order, so
//! a case or a document written after another source's cases interned their keys would order the
//! same attributes differently. Every case `.json` the generator writes or compares and every
//! document the driver renders sorts each map under an `attributes` key by key, recursively, and
//! the `types` table by pointer (`super::report::canonical_line`); the rest keeps the grammar's
//! order.
//!
//! **Flows.** A flow is `flows/<flow>.yaml` whose one `replay_in`'s `path:` names a case; the
//! directory holding that case is the flow's source, and every case in it runs through the flow
//! with [`Options::default`]. The flow driver's module doc (`super`) lists what a flow may hold.

use super::classes::{self, At, Fragment, Ledger};
use super::mapping::{self, Mapping};
use super::report::canonical_line;
use super::{run_traced, Options};
use anyhow::{bail, ensure, Context};
use bytes::Bytes;
use logit_config::ComponentKind;
use logit_core::{Diagnostics, EventBatch, Resource};
use logit_outputs::ndjson::render_ndjson;
use logit_pipeline::test_util::TelemetryProbe;
use logit_proto::event_json::EventJsonDecoder;
use logit_proto::influxdb::{write_resource, InfluxDecoder, Precision};
use logit_proto::prometheus::{
    families_to_events, text, Dialect, PrometheusDecoder, ATTR_TARGET, LABEL_INSTANCE,
};
use logit_proto::statsd::StatsdDecoder;
use logit_proto::Decoder;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The variable that turns comparing into rewriting.
const UPDATE: &str = "TRANSIT_UPDATE";

/// The command that regenerates every generated file.
const REGENERATE: &str = "script/transit gen";

/// The receipt time a wire case decodes at: 2027-01-15T08:00:00Z, in Unix nanoseconds.
const RECEIVED_AT: i64 = 1_800_000_000_000_000_000;

/// `spec/transit/`.
pub(super) fn spec_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../spec/transit")
}

pub(super) fn updating() -> bool {
    std::env::var(UPDATE).is_ok_and(|value| value == "1")
}

/// A case's `.meta.yaml` (module doc).
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Meta {
    #[serde(default)]
    exercises: Vec<String>,
    #[serde(default)]
    wire: Option<String>,
    #[serde(default)]
    tolerance: Vec<Tolerance>,
}

/// How far one number in a document may move (module doc).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Tolerance {
    path: String,
    abs: f64,
}

/// One entry of `features/<system>.yaml`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Feature {
    name: String,
    text: String,
}

/// One case.
#[derive(Debug)]
struct Case {
    system: String,
    name: String,
    dir: PathBuf,
    meta: Meta,
}

impl Case {
    fn json(&self) -> PathBuf {
        self.dir.join(format!("{}.json", self.name))
    }
}

/// One flow.
struct Flow {
    name: String,
    path: PathBuf,
    system: String,
}

/// A system's features.
fn features(spec: &Path, system: &str) -> anyhow::Result<Vec<Feature>> {
    let path = spec.join("features").join(format!("{system}.yaml"));
    let text =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    let features: Vec<Feature> =
        serde_norway::from_str(&text).with_context(|| format!("in {}", path.display()))?;
    for (i, feature) in features.iter().enumerate() {
        ensure!(
            !feature.text.trim().is_empty(),
            "{}: {} has no text",
            path.display(),
            feature.name
        );
        ensure!(
            !features[..i].iter().any(|f| f.name == feature.name),
            "{}: {} is listed twice",
            path.display(),
            feature.name
        );
    }
    Ok(features)
}

/// The source systems: each directory under `cases/`.
fn systems(spec: &Path) -> anyhow::Result<Vec<String>> {
    let mut systems = Vec::new();
    for entry in std::fs::read_dir(spec.join("cases"))? {
        let path = entry?.path();
        if path.is_dir() {
            systems.push(path.file_name().and_then(|n| n.to_str()).unwrap_or("").to_string());
        }
    }
    systems.sort();
    Ok(systems)
}

/// A system's cases, by name, each `exercises:` checked against its features.
fn system_cases(spec: &Path, system: &str) -> anyhow::Result<Vec<Case>> {
    let dir = spec.join("cases").join(system);
    let known = features(spec, system)?;
    let mut cases = Vec::new();
    for entry in std::fs::read_dir(&dir).with_context(|| format!("reading {}", dir.display()))? {
        let path = entry?.path();
        let Some(name) =
            path.file_name().and_then(|n| n.to_str()).and_then(|n| n.strip_suffix(".meta.yaml"))
        else {
            continue;
        };
        let text = std::fs::read_to_string(&path)?;
        let meta: Meta =
            serde_norway::from_str(&text).with_context(|| format!("in {}", path.display()))?;
        for feature in &meta.exercises {
            ensure!(
                known.iter().any(|f| &f.name == feature),
                "{}: exercises {feature}, which features/{system}.yaml doesn't list",
                path.display()
            );
        }
        cases.push(Case {
            system: system.to_string(),
            name: name.to_string(),
            dir: dir.clone(),
            meta,
        });
    }
    cases.sort_by(|a, b| a.name.cmp(&b.name));
    for entry in std::fs::read_dir(&dir)? {
        let path = entry?.path();
        let Some(file) = path.file_name().and_then(|n| n.to_str()) else { continue };
        let Some(stem) = file.strip_suffix(".json") else { continue };
        let wire = cases.iter().any(|c| c.meta.wire.as_deref() == Some(file));
        ensure!(
            wire || cases.iter().any(|c| c.name == stem),
            "{}: no {stem}.meta.yaml beside it, so no flow runs it",
            path.display()
        );
    }
    Ok(cases)
}

/// `batches` as `format: json` lines, each in [`canonical_line`]'s key order.
fn render(batches: &[EventBatch]) -> String {
    let mut text = String::new();
    for batch in batches {
        render_ndjson(&mut text, batch);
    }
    text.lines().map(|line| canonical_line(line) + "\n").collect()
}

/// What a case's `.json` should hold: decoded from its wire file, or its own text read back.
fn canonical(case: &Case) -> anyhow::Result<String> {
    let Some(wire) = &case.meta.wire else {
        let bytes = std::fs::read(case.json())
            .with_context(|| format!("reading {}", case.json().display()))?;
        let batches = EventJsonDecoder::new().decode_batches(&bytes)?;
        return Ok(render(&batches));
    };
    let path = case.dir.join(wire);
    let bytes = std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
    let sidecar = path.with_extension("headers");
    let headers = match std::fs::read_to_string(&sidecar) {
        Ok(text) => Headers::parse(&text),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Headers::default(),
        Err(err) => return Err(err).with_context(|| format!("reading {}", sidecar.display())),
    };
    decode_wire(&case.system, bytes, &headers)
        .with_context(|| format!("decoding {}", path.display()))
}

/// A wire file's `.headers` sidecar: one `name: value` per line, as `testdata/interop/` records
/// a request, with `path:` holding the request target and its query string.
#[derive(Debug, Default)]
struct Headers(Vec<(String, String)>);

impl Headers {
    fn parse(text: &str) -> Self {
        let pairs = text
            .lines()
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_string()))
            .collect();
        Self(pairs)
    }

    /// The first value of header `name`.
    fn get(&self, name: &str) -> Option<&str> {
        self.0.iter().find(|(n, _)| n == name).map(|(_, v)| v.as_str())
    }

    /// The first value of query parameter `key` in `path:`, `None` when absent or empty, as
    /// `influxdb_in` reads it. The cases' query strings hold no percent-escapes, so none is
    /// decoded.
    fn query(&self, key: &str) -> Option<&str> {
        let (_, query) = self.get("path")?.split_once('?')?;
        let value = query.split('&').find_map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (k == key).then_some(v)
        })?;
        (!value.is_empty()).then_some(value)
    }
}

/// The scrape URL a `prometheus` case reads as coming from, and the `instance` label
/// `prometheus_in` derives from it.
const SCRAPE_URL: &str = "http://transit:9100/metrics";
const SCRAPE_INSTANCE: &str = "transit:9100";

/// `system`'s wire bytes as its listener's decoder reads them, at [`RECEIVED_AT`], under the
/// resource the listener would stamp. A source system joins with an arm here:
///
/// - `statsd`: one datagram through `StatsdDecoder`, under an empty resource.
/// - `influxdb`: one write body through `InfluxDecoder::decode_body`, under the resource
///   `influxdb_in` builds from `path:`'s `org`, `orgID`, and `bucket`, at its `precision` (`ns`
///   when absent), gunzipped first under `content-encoding: gzip`. A bucket and an org or org ID
///   are required, as `influxdb_in` requires them.
/// - `prometheus`: one scrape body through `text::parse_with` and `families_to_events`, in the
///   dialect `content-type:` selects (text 0.0.4 when absent), under the resource
///   `prometheus_in` stamps for the target [`SCRAPE_URL`]: `instance` and
///   `_logit.prometheus.target`.
fn decode_wire(system: &str, bytes: Vec<u8>, headers: &Headers) -> anyhow::Result<String> {
    let mut probe = TelemetryProbe::new();
    let id = "transit.case";
    let batches = match system {
        "statsd" => {
            let telemetry = probe.telemetry(id, "statsd_in", "listener");
            let diag = Diagnostics::new(id).with_telemetry(telemetry.clone());
            let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()))
                .with_telemetry(telemetry)
                .with_diagnostics(diag);
            let mut events = Vec::new();
            let (resource, scope) =
                decoder.decode_into(Bytes::from(bytes), RECEIVED_AT, &mut events)?;
            vec![EventBatch { resource, scope, events }]
        }
        "influxdb" => {
            let telemetry = probe.telemetry(id, "influxdb_in", "listener");
            let precision = match headers.query("precision") {
                None => Precision::Ns,
                Some(unit) => Precision::from_query(unit)
                    .with_context(|| format!("precision {unit:?} isn't ns, us, ms, or s"))?,
            };
            let (org, org_id) = (headers.query("org"), headers.query("orgID"));
            ensure!(org.is_some() || org_id.is_some(), "the .headers path: names no org or orgID");
            let bucket = headers.query("bucket").context("the .headers path: names no bucket")?;
            let resource = Arc::new(write_resource(org, org_id, bucket));
            let bytes = match headers.get("content-encoding") {
                None | Some("identity") => bytes,
                Some("gzip") => {
                    let mut body = Vec::new();
                    std::io::Read::read_to_end(
                        &mut flate2::read::GzDecoder::new(&bytes[..]),
                        &mut body,
                    )
                    .context("the gzip body doesn't decompress")?;
                    body
                }
                Some(other) => bail!("influxdb_in refuses content-encoding {other:?}"),
            };
            let batch = InfluxDecoder::new()
                .with_telemetry(telemetry)
                .decode_body(Bytes::from(bytes), precision, RECEIVED_AT, resource)
                .map_err(|err| anyhow::anyhow!("influxdb_in refused the body: {err}"))?;
            vec![batch]
        }
        "prometheus" => {
            let telemetry = probe.telemetry(id, "prometheus_in", "listener");
            let diag = Diagnostics::new(id).with_telemetry(telemetry.clone());
            let dialect =
                headers.get("content-type").map_or(Dialect::Text0_0_4, Dialect::from_content_type);
            let mut decoder =
                PrometheusDecoder::new().with_telemetry(telemetry).with_diagnostics(diag);
            let families = text::parse_with(&bytes, dialect, &mut decoder)?;
            let events = families_to_events(&families, RECEIVED_AT, &mut decoder);
            let mut resource = Resource::default();
            resource.attributes.insert(LABEL_INSTANCE, logit_core::Value::str(SCRAPE_INSTANCE));
            resource.attributes.insert(ATTR_TARGET, logit_core::Value::str(SCRAPE_URL));
            vec![EventBatch { resource: Arc::new(resource), scope: None, events }]
        }
        other => bail!("no wire decoder for a {other} case yet"),
    };
    let refused: f64 = probe
        .poll()
        .sums()
        .filter(|(name, _, _)| name.ends_with(".dropped"))
        .map(|(_, _, total)| total)
        .sum();
    ensure!(refused == 0.0, "the decoder refused {refused} unit(s) of the wire file");
    ensure!(batches.iter().any(|b| !b.events.is_empty()), "the wire file decodes to no events");
    Ok(render(&batches))
}

/// Up to five lines where `actual` and `expected` differ.
pub(super) fn diff(actual: &str, expected: &str) -> String {
    let (a, e): (Vec<&str>, Vec<&str>) = (actual.lines().collect(), expected.lines().collect());
    let mut out = Vec::new();
    for i in 0..a.len().max(e.len()) {
        let (have, want) = (a.get(i), e.get(i));
        if have != want {
            out.push(format!(
                "  line {}:\n    committed: {}\n    generated: {}",
                i + 1,
                want.unwrap_or(&"<none>"),
                have.unwrap_or(&"<none>")
            ));
        }
        if out.len() == 5 {
            out.push("  ...".to_string());
            break;
        }
    }
    out.join("\n")
}

/// Whether `actual` reads as `expected`: byte-equal, or equal once each tolerance's numbers are
/// within its bound.
fn agrees(actual: &str, expected: &str, tolerances: &[Tolerance]) -> bool {
    if actual == expected {
        return true;
    }
    if tolerances.is_empty() {
        return false;
    }
    let (Ok(mut actual), Ok(expected)) =
        (serde_json::from_str::<Value>(actual), serde_json::from_str::<Value>(expected))
    else {
        return false;
    };
    let (Some(have), Some(want)) = (actual["events"].as_array_mut(), expected["events"].as_array())
    else {
        return false;
    };
    if have.len() != want.len() {
        return false;
    }
    for (have, want) in have.iter_mut().zip(want) {
        for tolerance in tolerances {
            let Some(target) = want.pointer(&tolerance.path) else { continue };
            let Some(value) = have.pointer_mut(&tolerance.path) else { continue };
            if let (Some(a), Some(b)) = (value.as_f64(), target.as_f64()) {
                if (a - b).abs() <= tolerance.abs {
                    *value = target.clone();
                }
            }
        }
    }
    actual == expected
}

/// A flow's expectation file: `{`, then each case's `"<name>": <document>` with the document's
/// later lines indented two spaces, joined by `,`, then `}`.
fn render_flow(docs: &BTreeMap<String, String>) -> String {
    let mut out = String::from("{\n");
    for (i, (name, doc)) in docs.iter().enumerate() {
        if i > 0 {
            out.push_str(",\n");
        }
        out.push_str(&format!("  {}: ", Value::from(name.as_str())));
        for (j, line) in doc.trim_end().lines().enumerate() {
            if j > 0 {
                out.push_str("\n  ");
            }
            out.push_str(line);
        }
    }
    if !docs.is_empty() {
        out.push('\n');
    }
    out.push_str("}\n");
    out
}

/// [`render_flow`]'s inverse: each case's document, ending in a newline as the driver writes it.
/// A case's key is the one line indented two spaces that opens with a `"`; a document's own lines
/// sit at four spaces or open with `]`.
fn split_flow(text: &str) -> BTreeMap<String, String> {
    let mut out: BTreeMap<String, String> = BTreeMap::new();
    let mut current: Option<String> = None;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("  \"") {
            if let Some((key, doc)) = rest.split_once("\": ") {
                let name: String =
                    serde_json::from_str(&format!("\"{key}\"")).unwrap_or_else(|_| key.to_string());
                out.insert(name.clone(), format!("{doc}\n"));
                current = Some(name);
                continue;
            }
        }
        let Some(name) = &current else { continue };
        if line == "}" {
            current = None;
            continue;
        }
        let doc = out.get_mut(name).expect("the case was inserted");
        doc.push_str(line.strip_prefix("  ").unwrap_or(line));
        doc.push('\n');
    }
    // A document but the last is followed by the `,` that joins it to the next.
    for doc in out.values_mut() {
        if doc.ends_with("}},\n") {
            doc.truncate(doc.len() - 2);
            doc.push('\n');
        }
    }
    out
}

/// Compares, or under `update` writes, one flow's expectation file (module doc); one failure
/// per missing, stale, or unknown case.
fn sync_flow(
    path: &Path,
    flow: &str,
    docs: &BTreeMap<String, (String, Vec<Tolerance>)>,
    update: bool,
) -> Vec<String> {
    let texts: BTreeMap<String, String> =
        docs.iter().map(|(name, (doc, _))| (name.clone(), doc.clone())).collect();
    let generated = render_flow(&texts);
    let Ok(committed) = std::fs::read_to_string(path) else {
        if update {
            std::fs::write(path, &generated).expect("write a flow's expectations");
            return Vec::new();
        }
        return vec![format!("{flow}: {} is missing; run `{REGENERATE}`", path.display())];
    };
    if committed == generated {
        return Vec::new();
    }
    let have = split_flow(&committed);
    let mut failures = Vec::new();
    for (name, (doc, tolerances)) in docs {
        match have.get(name) {
            None => failures.push(format!(
                "{flow} × {name}: {} has no expectation for the case; run `{REGENERATE}`",
                path.display()
            )),
            Some(committed) if !agrees(doc, committed, tolerances) => failures.push(format!(
                "{flow} × {name}: the expectation in {} is stale; run `{REGENERATE}` and review \
                 the diff:\n{}",
                path.display(),
                diff(doc, committed)
            )),
            Some(_) => {}
        }
    }
    for name in have.keys().filter(|name| !docs.contains_key(*name)) {
        failures.push(format!(
            "{flow} × {name}: {} holds an expectation for no case; run `{REGENERATE}` to remove it",
            path.display()
        ));
    }
    if update {
        std::fs::write(path, &generated).expect("write a flow's expectations");
        return Vec::new();
    }
    failures
}

/// Compares, or under `update` writes, one generated file; `None` when it's current.
fn sync(path: &Path, generated: &str, tolerances: &[Tolerance], update: bool) -> Option<String> {
    let committed = std::fs::read_to_string(path).ok();
    if committed.as_deref().is_some_and(|c| agrees(generated, c, tolerances)) {
        return None;
    }
    if update {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).expect("create the expectation's directory");
        }
        std::fs::write(path, generated).expect("write a generated file");
        return None;
    }
    Some(match committed {
        Some(committed) => format!(
            "{} is stale; run `{REGENERATE}` and review the diff:\n{}",
            path.display(),
            diff(generated, &committed)
        ),
        None => format!("{} is missing; run `{REGENERATE}`", path.display()),
    })
}

/// A flow's source system, from its `replay_in`'s `path:`.
fn flow(spec: &Path, path: &Path) -> anyhow::Result<Flow> {
    let config = crate::config::load(path)?;
    let paths: Vec<&String> = config
        .components
        .values()
        .filter_map(|c| match &c.kind {
            ComponentKind::ReplayIn { path, .. } => Some(path),
            _ => None,
        })
        .collect();
    let [case] = paths[..] else {
        bail!("{}: a flow has one replay_in, not {}", path.display(), paths.len());
    };
    let base = path.parent().unwrap_or(Path::new("."));
    let dir = base.join(case);
    let dir = dir.parent().context("the replay_in path names no directory")?;
    let dir = dir.canonicalize().with_context(|| format!("{}: {case}", path.display()))?;
    let cases = spec.join("cases").canonicalize()?;
    ensure!(
        dir.parent() == Some(cases.as_path()),
        "{}: replay_in's path names a case under spec/transit/cases/<system>/, not {case}",
        path.display()
    );
    let system = dir.file_name().and_then(|n| n.to_str()).unwrap_or("").to_string();
    let name = path.file_stem().and_then(|n| n.to_str()).unwrap_or("").to_string();
    Ok(Flow { name, path: path.to_path_buf(), system })
}

/// Every flow, by name.
fn flows(spec: &Path) -> anyhow::Result<Vec<Flow>> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(spec.join("flows"))?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<Result<_, _>>()?;
    paths.retain(|p| p.extension().is_some_and(|e| e == "yaml"));
    paths.sort();
    paths.iter().map(|p| flow(spec, p)).collect()
}

/// A system's cases by system, and a failure per system whose cases don't load.
fn all_cases(spec: &Path) -> (BTreeMap<String, Vec<Case>>, Vec<String>) {
    let mut failures = Vec::new();
    let mut by_system = BTreeMap::new();
    match systems(spec) {
        Ok(systems) => {
            for system in systems {
                match system_cases(spec, &system) {
                    Ok(cases) => {
                        by_system.insert(system, cases);
                    }
                    Err(err) => failures.push(format!("{err:#}")),
                }
            }
        }
        Err(err) => failures.push(format!("reading spec/transit/cases: {err:#}")),
    }
    (by_system, failures)
}

/// A flow's expectation file.
fn expected_path(spec: &Path, flow: &str) -> PathBuf {
    spec.join("expected").join(format!("{flow}.json"))
}

/// A flow's ledger file.
fn ledger_path(spec: &Path, flow: &str) -> PathBuf {
    spec.join("ledger").join(format!("{flow}.json"))
}

/// Each failure once: a check that fails once per run of a flow, such as a misspelled `when:`
/// key, reports once.
fn dedupe(mut failures: Vec<String>) -> Vec<String> {
    let mut seen = std::collections::BTreeSet::new();
    failures.retain(|failure| seen.insert(failure.clone()));
    failures
}

/// The `cases` test's work under `spec` (module doc): the table loads, each case's `.json` is
/// current, nothing under `expected/` or `ledger/` belongs to no flow, and `tested`, the
/// `(test name, flow)` pairs of the per-flow tests, names each flow under `flows/` once.
fn check_cases(spec: &Path, tested: &[(&str, &str)], update: bool) -> Vec<String> {
    let mut failures = Vec::new();
    if let Err(err) = mapping::load(&spec.join("mapping.yaml")) {
        failures.push(format!("{err:#}"));
    }
    let (by_system, load_failures) = all_cases(spec);
    failures.extend(load_failures);
    for case in by_system.values().flatten() {
        match canonical(case) {
            Err(err) => failures.push(format!("case {}/{}: {err:#}", case.system, case.name)),
            Ok(want) if case.meta.wire.is_some() => {
                failures.extend(sync(&case.json(), &want, &[], update));
            }
            Ok(want) => {
                let have = std::fs::read_to_string(case.json()).unwrap_or_default();
                if have != want {
                    failures.push(format!(
                        "{} is hand-written and reads back differently; write it as:\n{want}",
                        case.json().display()
                    ));
                }
            }
        }
    }
    let flows = match flows(spec) {
        Ok(flows) => flows,
        Err(err) => {
            failures.push(format!("{err:#}"));
            return dedupe(failures);
        }
    };
    let names: Vec<&str> = flows.iter().map(|f| f.name.as_str()).collect();
    for name in names.iter().filter(|name| !tested.iter().any(|(_, flow)| flow == *name)) {
        failures.push(format!(
            "flows/{name}.yaml has no test: add `{} => \"{name}\"` to spec.rs's flow_tests! list",
            name.replace('-', "_")
        ));
    }
    for (i, (test, flow)) in tested.iter().enumerate() {
        if !names.contains(flow) {
            failures.push(format!(
                "spec.rs's flow_tests! list names {flow}, which has no flows/{flow}.yaml; remove it"
            ));
        }
        if tested[..i].iter().any(|(_, other)| other == flow) {
            failures.push(format!("spec.rs's flow_tests! list names {flow} twice"));
        }
        if *test != flow.replace('-', "_") {
            failures.push(format!(
                "spec.rs's flow_tests! list runs {flow} as {test}; name it {}",
                flow.replace('-', "_")
            ));
        }
    }
    for (dir, path) in
        [("expected", expected_path as fn(&Path, &str) -> PathBuf), ("ledger", ledger_path)]
    {
        let produced: Vec<PathBuf> = names.iter().map(|name| path(spec, name)).collect();
        failures.extend(stale_outputs(spec, dir, &produced, update));
    }
    dedupe(failures)
}

/// One flow's test under `spec` (module doc): every case of its source through it, the class
/// check, and its expectation and ledger files compared, or under `update` rewritten when no
/// case failed the class check.
fn check_flow(spec: &Path, name: &str, update: bool) -> Vec<String> {
    let mapping = match mapping::load(&spec.join("mapping.yaml")) {
        Ok(mapping) => mapping,
        Err(err) => return vec![format!("{err:#}")],
    };
    let flow = match flow(spec, &spec.join("flows").join(format!("{name}.yaml"))) {
        Ok(flow) => flow,
        Err(err) => return vec![format!("{err:#}")],
    };
    let cases = match system_cases(spec, &flow.system) {
        Ok(cases) => cases,
        Err(err) => return vec![format!("{err:#}")],
    };
    let mut failures = Vec::new();
    let mut ledger = Ledger::default();
    let mut docs = BTreeMap::new();
    let mut class_failed = false;
    for case in &cases {
        let (doc, class_failures) = run_one(&mapping, &flow, case, &mut ledger);
        class_failed |= !class_failures.is_empty();
        failures.extend(class_failures);
        if let Some(doc) = doc {
            docs.insert(case.name.clone(), (doc, case.meta.tolerance.clone()));
        }
    }
    // A class failure is a finding, so neither file is rewritten over it.
    let write = update && !class_failed;
    failures.extend(sync_flow(&expected_path(spec, name), name, &docs, write));
    let fragment = ledger.fragment().render();
    failures.extend(sync(&ledger_path(spec, name), &fragment, &[], write));
    dedupe(failures)
}

/// The `ledger` test's work under `spec` (module doc): every flow's committed ledger file merged,
/// the checks across flows, and `coverage.md`.
fn check_ledger(spec: &Path, update: bool) -> Vec<String> {
    let mapping = match mapping::load(&spec.join("mapping.yaml")) {
        Ok(mapping) => mapping,
        Err(err) => return vec![format!("{err:#}")],
    };
    let flows = match flows(spec) {
        Ok(flows) => flows,
        Err(err) => return vec![format!("{err:#}")],
    };
    let mut failures = Vec::new();
    let mut ledger = Ledger::default();
    for flow in &flows {
        let path = ledger_path(spec, &flow.name);
        let fragment = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))
            .and_then(|text| {
                serde_json::from_str::<Fragment>(&text)
                    .with_context(|| format!("in {}", path.display()))
            });
        match fragment {
            Ok(fragment) => ledger.merge(fragment),
            Err(err) => failures.push(format!("{}: {err:#}; run `{REGENERATE}`", flow.name)),
        }
    }
    // A merge short of a flow would misreport the table and the coverage.
    if !failures.is_empty() {
        return failures;
    }
    failures.extend(ledger.finish(&mapping));
    for line in ledger.unspecified() {
        eprintln!("transit: {line}");
    }
    match coverage(spec, &mapping, &ledger) {
        Ok(text) => failures.extend(sync(&spec.join("coverage.md"), &text, &[], update)),
        Err(err) => failures.push(format!("coverage.md: {err:#}")),
    }
    dedupe(failures)
}

/// One flow and one case: the run's document, or `None` when the driver refused the run, and
/// every failure, the class check's included.
fn run_one(
    mapping: &Mapping,
    flow: &Flow,
    case: &Case,
    ledger: &mut Ledger,
) -> (Option<String>, Vec<String>) {
    let here = format!("{} × {}", flow.name, case.name);
    let traced = match run_traced(&flow.path, &case.json(), Options::default()) {
        Ok(traced) => traced,
        Err(err) => return (None, vec![format!("{here}: {err:#}")]),
    };
    let at = At { flow: &flow.name, case: &case.name };
    let failures = classes::check(mapping, &at, &traced, ledger);
    (Some(traced.outcome.document()), failures)
}

/// Entries under `spec/<dir>/` that aren't in `produced`: removed under `update`, else failures.
fn stale_outputs(spec: &Path, dir: &str, produced: &[PathBuf], update: bool) -> Vec<String> {
    let mut failures = Vec::new();
    let Ok(entries) = std::fs::read_dir(spec.join(dir)) else { return failures };
    for path in entries.flatten().map(|e| e.path()) {
        if produced.contains(&path) {
            continue;
        }
        if update {
            if path.is_dir() {
                std::fs::remove_dir_all(&path).expect("remove a stale directory");
            } else {
                std::fs::remove_file(&path).expect("remove a stale generated file");
            }
        } else {
            failures.push(format!(
                "{} matches no flow; run `{REGENERATE}` to remove it",
                path.display()
            ));
        }
    }
    failures
}

/// `coverage.md`: each system's features and the cases that exercise them, then each sink's cells
/// no flow and case reached.
fn coverage(spec: &Path, mapping: &Mapping, ledger: &Ledger) -> anyhow::Result<String> {
    let mut out = String::from(
        "# Transit spec coverage\n\nGenerated by `script/transit gen` from each system's \
         `features/<system>.yaml`, its cases' `exercises:` lists, and the cells the class check \
         compared a record against; don't edit it by hand. A feature no case exercises shows \
         `0`, and a cell no flow reaches is listed under its sink: nothing checks it.\n",
    );
    for system in systems(spec)? {
        let features = features(spec, &system)?;
        let cases = system_cases(spec, &system)?;
        out.push_str(&format!("\n## {system}\n\n| Feature | Cases | Count |\n|---|---|---|\n"));
        for feature in &features {
            let names: Vec<String> = cases
                .iter()
                .filter(|c| c.meta.exercises.contains(&feature.name))
                .map(|c| format!("`{}`", c.name))
                .collect();
            let listed = if names.is_empty() { "none".to_string() } else { names.join(", ") };
            out.push_str(&format!("| `{}` | {listed} | {} |\n", feature.name, names.len()));
        }
    }
    let unreached = ledger.unreached(mapping);
    for sink in &mapping.sinks {
        out.push_str(&format!("\n## Cells no flow reaches: {sink}\n\n"));
        match unreached.get(sink) {
            None => out.push_str("None.\n"),
            Some(cells) => {
                out.push_str("| Row | Cell | When |\n|---|---|---|\n");
                for (row, index, when) in cells {
                    out.push_str(&format!("| `{row}` | {index} | `{when}` |\n"));
                }
            }
        }
    }
    Ok(out)
}

/// Fails listing every failure, when there are any.
fn assert_none(failures: Vec<String>) {
    assert!(failures.is_empty(), "{} failure(s):\n\n{}", failures.len(), failures.join("\n\n"));
}

/// The case files, stale outputs, and the flow tests' list (module doc).
#[test]
fn cases() {
    assert_none(check_cases(&spec_dir(), FLOWS, updating()));
}

/// The checks across every flow, from the committed ledger files, and `coverage.md` (module doc).
#[test]
fn ledger() {
    assert_none(check_ledger(&spec_dir(), updating()));
}

/// One test per flow, `flow::<flow>` with each `-` of the file's stem as `_`, and [`FLOWS`]
/// listing them for the `cases` test's comparison with `flows/`.
macro_rules! flow_tests {
    ($($test:ident => $flow:literal),* $(,)?) => {
        /// Each per-flow test's name and the flow it runs.
        const FLOWS: &[(&str, &str)] = &[$((stringify!($test), $flow)),*];

        /// A test per flow under `spec/transit/flows/` (module doc).
        mod flow {
            $(
                #[test]
                fn $test() {
                    super::assert_none(super::check_flow(
                        &super::spec_dir(),
                        $flow,
                        super::updating(),
                    ));
                }
            )*
        }
    };
}

flow_tests! {
    influxdb_aggregate_to_prometheus => "influxdb-aggregate-to-prometheus",
    influxdb_relay => "influxdb-relay",
    influxdb_to_prometheus_remote_write => "influxdb-to-prometheus-remote-write",
    influxdb_to_prometheus => "influxdb-to-prometheus",
    influxdb_to_otlp => "influxdb-to-otlp",
    influxdb_to_statsd => "influxdb-to-statsd",
    prometheus_to_influxdb => "prometheus-to-influxdb",
    prometheus_to_statsd => "prometheus-to-statsd",
    statsd_aggregate_cumulative => "statsd-aggregate-cumulative",
    statsd_aggregate_samples => "statsd-aggregate-samples",
    statsd_aggregate_sketch => "statsd-aggregate-sketch",
    statsd_aggregate_skip => "statsd-aggregate-skip",
    statsd_relay_plain => "statsd-relay-plain",
    statsd_relay_relative => "statsd-relay-relative",
    statsd_relay_skip => "statsd-relay-skip",
    statsd_relay => "statsd-relay",
    statsd_to_influxdb => "statsd-to-influxdb",
    statsd_to_otlp => "statsd-to-otlp",
    statsd_to_prometheus_remote_write => "statsd-to-prometheus-remote-write",
    statsd_to_prometheus => "statsd-to-prometheus",
}

#[cfg(test)]
mod tests {
    use super::*;

    fn relay() -> (Flow, Case) {
        let spec = spec_dir();
        let flow = flow(&spec, &spec.join("flows/statsd-relay.yaml")).expect("the relay flow");
        let case = system_cases(&spec, "statsd")
            .expect("statsd cases")
            .into_iter()
            .find(|c| c.name == "counter")
            .expect("the counter case");
        (flow, case)
    }

    /// A record the sink writes only in a lesser form has its field rows left unlooked-up, so a
    /// field row with no cell for that sink (`record/min_max` at `statsd_out`) isn't reached.
    #[test]
    fn a_degraded_records_field_rows_arent_looked_up() {
        let spec = spec_dir();
        let flow = flow(&spec, &spec.join("flows/statsd-relay.yaml")).expect("the relay flow");
        let case = system_cases(&spec, "statsd")
            .expect("statsd cases")
            .into_iter()
            .find(|c| c.name == "cross-kinds")
            .expect("the cross-kinds case");
        let traced = run_traced(&flow.path, &case.json(), Options::default()).expect("runs");
        let table = mapping::load(&spec.join("mapping.yaml")).expect("the table");
        let at = At { flow: &flow.name, case: &case.name };
        let mut ledger = Ledger::default();
        assert_eq!(classes::check(&table, &at, &traced, &mut ledger), Vec::<String>::new());
        let unspecified = ledger.unspecified();
        assert!(
            !unspecified.iter().any(|line| line.contains("record/min_max")),
            "{unspecified:#?}"
        );
    }

    /// A cell that disagrees with what the flow does fails the class check, naming the case, the
    /// row, the table's class, and the flow's.
    #[test]
    fn a_wrong_cell_fails_the_class_check() {
        let text = std::fs::read_to_string(spec_dir().join("mapping.yaml")).expect("the table");
        let right =
            "  - kind: sum\n    feature: delta_monotonic\n    cells:\n      statsd_out:\n        \
                     class: exact\n";
        assert!(text.contains(right), "the table's delta_monotonic row moved");
        let wrong = text.replace(right, &right.replace("class: exact", "class: degraded"));
        let (flow, case) = relay();
        let traced = run_traced(&flow.path, &case.json(), Options::default()).expect("runs");
        let at = At { flow: &flow.name, case: &case.name };

        let right = mapping::parse(&text).expect("the table parses");
        assert_eq!(
            classes::check(&right, &at, &traced, &mut Ledger::default()),
            Vec::<String>::new()
        );

        let wrong = mapping::parse(&wrong).expect("the edited table parses");
        let failures = classes::check(&wrong, &at, &traced, &mut Ledger::default());
        let want = "statsd-relay × counter: sum \"requests\": the table expects degraded \
                    (sum/delta_monotonic: degraded, sum/f64: exact), the flow reads exact";
        assert!(failures.iter().any(|f| f.contains(want)), "{failures:#?}");
    }

    /// A record nothing reads back fails when the cell names a counter that didn't move and no
    /// other sink counter says why: `prometheus_out` skips a log uncounted, so a cell claiming a
    /// counter for it is wrong. It pins that path only: the flagged-record rule in `classes.rs`'s
    /// "Counters" is pinned by the `flagged-kinds` case across the statsd flows.
    #[test]
    fn a_loss_no_counter_explains_fails_the_class_check() {
        let spec = spec_dir();
        let text = std::fs::read_to_string(spec.join("mapping.yaml")).expect("the table");
        let right =
            "      prometheus_out:\n        class: skipped\n        reason: uncounted\n        \
                     text: A log has no Prometheus form and isn't written, uncounted.\n";
        assert_eq!(text.matches(right).count(), 2, "the table's two log rows moved");
        let wrong = text.replacen(
            right,
            &right.replace("reason: uncounted", "counter: logit.output.events.skipped"),
            2,
        );
        let flow = flow(&spec, &spec.join("flows/statsd-to-prometheus.yaml")).expect("the flow");
        let case = system_cases(&spec, "statsd")
            .expect("statsd cases")
            .into_iter()
            .find(|c| c.name == "log-only")
            .expect("the log-only case");
        let traced = run_traced(&flow.path, &case.json(), Options::default()).expect("runs");
        let at = At { flow: &flow.name, case: &case.name };

        let right = mapping::parse(&text).expect("the table parses");
        assert_eq!(
            classes::check(&right, &at, &traced, &mut Ledger::default()),
            Vec::<String>::new()
        );

        let wrong = mapping::parse(&wrong).expect("the edited table parses");
        let failures = classes::check(&wrong, &at, &traced, &mut Ledger::default());
        assert!(
            failures.iter().any(|f| f.contains("isn't read back and no sink counter says why")),
            "{failures:#?}"
        );
    }

    /// The counter case's document through the relay flow, keyed for [`sync_flow`].
    fn counter_docs() -> BTreeMap<String, (String, Vec<Tolerance>)> {
        let (flow, case) = relay();
        let document = run_traced(&flow.path, &case.json(), Options::default())
            .expect("runs")
            .outcome
            .document();
        BTreeMap::from([("counter".to_string(), (document, Vec::new()))])
    }

    /// The committed relay expectations hold the counter case's document as the driver writes it.
    #[test]
    fn a_flow_file_reads_back_each_case() {
        let committed = std::fs::read_to_string(spec_dir().join("expected/statsd-relay.json"))
            .expect("the committed expectations");
        let docs = counter_docs();
        assert_eq!(split_flow(&committed).get("counter"), Some(&docs["counter"].0));
        let texts = docs.iter().map(|(k, (d, _))| (k.clone(), d.clone())).collect();
        let rendered = render_flow(&texts);
        assert_eq!(split_flow(&rendered).get("counter"), Some(&docs["counter"].0));
        assert!(serde_json::from_str::<Value>(&rendered).is_ok(), "{rendered}");
    }

    /// An expectation edited by hand fails the comparison, naming the flow and the case, with the
    /// command that regenerates it.
    #[test]
    fn an_edited_expectation_fails_the_comparison() {
        let docs = counter_docs();
        let texts = docs.iter().map(|(k, (d, _))| (k.clone(), d.clone())).collect();
        let current = render_flow(&texts);
        let path = logit_pipeline::test_util::scratch_dir("transit-edited").join("relay.json");
        std::fs::write(&path, &current).expect("write the current file");
        assert_eq!(sync_flow(&path, "statsd-relay", &docs, false), Vec::<String>::new());

        let edited = current.replacen("\"value\":3.0", "\"value\":4.0", 1);
        assert_ne!(edited, current, "the expectation holds the counter's 3.0");
        std::fs::write(&path, edited).expect("write the edited copy");
        let failures = sync_flow(&path, "statsd-relay", &docs, false);
        assert!(
            failures.iter().any(|f| f.starts_with("statsd-relay × counter: the expectation")
                && f.contains(REGENERATE)
                && f.contains("\"value\":4.0")),
            "{failures:#?}"
        );
    }

    /// A case with no key in its flow's file, a key with no case, and a missing file each fail.
    #[test]
    fn a_missing_or_unknown_case_fails() {
        let docs = counter_docs();
        let path = logit_pipeline::test_util::scratch_dir("transit-keys").join("relay.json");
        let missing = sync_flow(&path, "statsd-relay", &docs, false);
        assert!(
            missing.iter().any(|f| f.starts_with("statsd-relay: ")
                && f.contains("is missing")
                && f.contains(REGENERATE)),
            "{missing:#?}"
        );

        let other = BTreeMap::from([("gone".to_string(), docs["counter"].0.clone())]);
        std::fs::write(&path, render_flow(&other)).expect("write a file without the case");
        let failures = sync_flow(&path, "statsd-relay", &docs, false);
        assert!(
            failures.iter().any(|f| f.starts_with("statsd-relay × counter:")
                && f.contains("has no expectation for the case")),
            "{failures:#?}"
        );
        assert!(
            failures.iter().any(|f| f.starts_with("statsd-relay × gone:")
                && f.contains("an expectation for no case")),
            "{failures:#?}"
        );
    }

    /// A scratch copy of the spec holding only `flows/statsd-relay.yaml` and the `counter` case,
    /// with `mapping.yaml` passed through `edit`.
    fn small_spec(label: &str, edit: impl Fn(String) -> String) -> PathBuf {
        let from = spec_dir();
        let to = logit_pipeline::test_util::scratch_dir(label).join("transit");
        for dir in ["features", "flows", "cases/statsd", "expected", "ledger"] {
            std::fs::create_dir_all(to.join(dir)).expect("create a scratch directory");
        }
        let copy = |path: &str| {
            std::fs::copy(from.join(path), to.join(path)).expect("copy a spec file");
        };
        for path in [
            "features/statsd.yaml",
            "flows/statsd-relay.yaml",
            "cases/statsd/counter.meta.yaml",
            "cases/statsd/counter.statsd",
            "cases/statsd/counter.json",
            "expected/statsd-relay.json",
        ] {
            copy(path);
        }
        // The flow names this case, which only has to exist.
        copy("cases/statsd/mixed.json");
        std::fs::write(to.join("cases/statsd/mixed.meta.yaml"), "wire: mixed.statsd\n")
            .expect("write a meta");
        copy("cases/statsd/mixed.statsd");
        let mapping = std::fs::read_to_string(from.join("mapping.yaml")).expect("the table");
        std::fs::write(to.join("mapping.yaml"), edit(mapping)).expect("write the table");
        to
    }

    /// `gen` leaves a flow's expectations and ledger file alone when any of its runs fails the
    /// class check, and writes both once none does.
    #[test]
    fn gen_never_rewrites_over_a_class_failure() {
        let delta = "    feature: delta_monotonic\n    cells:\n      statsd_out:\n        class: ";
        let spec = small_spec("transit-gen-class", |table| {
            table.replace(&format!("{delta}exact\n"), &format!("{delta}degraded\n"))
        });
        let expected = spec.join("expected/statsd-relay.json");
        let ledger = spec.join("ledger/statsd-relay.json");
        for path in [&expected, &ledger] {
            std::fs::write(path, "stale\n").expect("write a stale file");
        }
        let failures = check_flow(&spec, "statsd-relay", true);
        assert!(failures.iter().any(|f| f.contains("the table expects degraded")), "{failures:#?}");
        for path in [&expected, &ledger] {
            assert_eq!(std::fs::read_to_string(path).expect("read it"), "stale\n");
        }

        let table = std::fs::read_to_string(spec_dir().join("mapping.yaml")).expect("the table");
        std::fs::write(spec.join("mapping.yaml"), table).expect("restore the table");
        assert_eq!(check_flow(&spec, "statsd-relay", true), Vec::<String>::new());
        for path in [&expected, &ledger] {
            assert_ne!(std::fs::read_to_string(path).expect("read it"), "stale\n");
        }
    }

    /// The flow tests' list must name each flow under `flows/` once, under its own name.
    #[test]
    fn the_flow_test_list_matches_the_flows() {
        let spec = small_spec("transit-flow-list", |table| table);
        let right = [("statsd_relay", "statsd-relay")];
        assert_eq!(check_cases(&spec, &right, false), Vec::<String>::new());

        let failures = check_cases(&spec, &[], false);
        assert!(
            failures.iter().any(|f| f.contains("flows/statsd-relay.yaml has no test")),
            "{failures:#?}"
        );
        let failures =
            check_cases(&spec, &[("statsd_relay", "statsd-relay"), ("gone", "gone")], false);
        assert!(
            failures.iter().any(|f| f.contains("names gone, which has no flows/gone.yaml")),
            "{failures:#?}"
        );
        let failures = check_cases(&spec, &[("relay", "statsd-relay")], false);
        assert!(
            failures.iter().any(|f| f.contains("as relay; name it statsd_relay")),
            "{failures:#?}"
        );
    }

    /// A ledger file reads back as the ledger that wrote it, and a merged ledger whose
    /// `normalized` cell never reads `normalized` fails as a stale table.
    #[test]
    fn a_ledger_file_reads_back_and_merges() {
        let spec = spec_dir();
        let table = mapping::load(&spec.join("mapping.yaml")).expect("the table");
        let (flow, case) = relay();
        let traced = run_traced(&flow.path, &case.json(), Options::default()).expect("runs");
        let at = At { flow: &flow.name, case: &case.name };
        let mut ledger = Ledger::default();
        assert_eq!(classes::check(&table, &at, &traced, &mut ledger), Vec::<String>::new());
        let text = ledger.fragment().render();
        let read: Fragment = serde_json::from_str(&text).expect("the file parses");
        assert_eq!(read, ledger.fragment());
        let mut merged = Ledger::default();
        merged.merge(read);
        assert_eq!(merged.fragment().render(), text);
        // The relay's `mixed` case writes no tag `statsd_out` sanitizes, so another flow's
        // observation of that `normalized` cell is merged in, as the `ledger` test would.
        let tags = r#"{"cells":[{"sink":"statsd_out","row":"attributes/string_value","cell":1,"observed":["normalized"]}],"unspecified":[]}"#;
        merged.merge(serde_json::from_str(tags).expect("the entry parses"));
        assert_eq!(merged.finish(&table), Vec::<String>::new());

        let (sink, row, index, _) = table
            .cells()
            .into_iter()
            .find(|(_, _, _, cell)| cell.class == mapping::Class::Normalized)
            .expect("the table has a normalized cell");
        let entry = format!(
            "{{\"cells\":[{{\"sink\":\"{sink}\",\"row\":\"{row}\",\"cell\":{index},\
             \"observed\":[\"exact\"]}}],\"unspecified\":[]}}"
        );
        let mut stale = Ledger::default();
        stale.merge(serde_json::from_str(&entry).expect("the entry parses"));
        let failures = stale.finish(&table);
        assert!(
            failures.iter().any(|f| f.contains(&format!("mapping row {row}, {sink} cell {index}"))
                && f.contains("the table may be stale")),
            "{failures:#?}"
        );
    }

    /// A case `.json` with no `.meta.yaml` fails rather than never running.
    #[test]
    fn a_case_json_without_a_meta_fails() {
        let spec = small_spec("transit-orphan", |table| table);
        std::fs::write(spec.join("cases/statsd/orphan.json"), "").expect("write an orphan");
        let failures = check_cases(&spec, &[("statsd_relay", "statsd-relay")], false);
        assert!(
            failures.iter().any(|f| f.contains("orphan.json: no orphan.meta.yaml beside it")),
            "{failures:#?}"
        );
    }

    /// A `when:` key that names no field of the sink's config fails, naming the row, the cell,
    /// and the key.
    #[test]
    fn a_misspelled_when_key_fails() {
        let text = std::fs::read_to_string(spec_dir().join("mapping.yaml")).expect("the table");
        let typo = text.replacen("- when: {multi_value: skip}", "- when: {multivalue: skip}", 1);
        assert_ne!(typo, text, "the table has a multi_value cell");
        let (flow, case) = relay();
        let traced = run_traced(&flow.path, &case.json(), Options::default()).expect("runs");
        let at = At { flow: &flow.name, case: &case.name };
        let table = mapping::parse(&typo).expect("the edited table parses");
        let failures = classes::check(&table, &at, &traced, &mut Ledger::default());
        let want = "mapping row sum/delta_nonmonotonic, statsd_out cell 0: when: names \
                    \"multivalue\", which isn't a statsd_out config field";
        assert!(failures.iter().any(|f| f.contains(want)), "{failures:#?}");
    }

    /// A case written after another interner history reads the same: an event's attributes,
    /// nested map, resource attributes, and type table come out in key order however the
    /// process interned the keys.
    #[test]
    fn attribute_order_doesnt_follow_the_interner() {
        let line = |p: &str| {
            format!(
                "{{\"timestamp\":\"2027-01-15T08:00:00.000000000Z\",\"metrics\":[{{\"name\":\"g\",\
                 \"kind\":\"gauge\",\"value\":1.0}}],\"attributes\":{{\"{p}1\":\"a\",\"{p}2\":5,\
                 \"{p}3\":{{\"{p}5\":\"x\",\"{p}4\":\"y\"}}}},\"resource\":{{\"attributes\":\
                 {{\"{p}6\":\"r\",\"{p}7\":\"s\"}}}},\"types\":{{\"/attributes/{p}2\":\"u64\"}}}}\n"
            )
        };
        let decode = |text: &str| {
            EventJsonDecoder::new().decode_batches(text.as_bytes()).expect("the line decodes")
        };
        // The first set is interned in written order; the second, in reverse, before decoding.
        let forward = decode(&line("transit.order.a"));
        for i in (1..=7).rev() {
            logit_core::interner::intern(&format!("transit.order.b{i}"));
        }
        let reverse = decode(&line("transit.order.b"));

        let rename = |text: String| text.replace("transit.order.b", "transit.order.a");
        assert_eq!(render(&forward), rename(render(&reverse)));
        let mut probe = logit_pipeline::test_util::TelemetryProbe::new();
        let (a, b) = (
            super::super::Outcome::new(&forward, probe.poll()).document(),
            super::super::Outcome::new(&reverse, probe.poll()).document(),
        );
        assert_eq!(a, rename(b));
        assert!(render(&forward).contains("\"transit.order.a4\":\"y\",\"transit.order.a5\""));
    }

    /// A tolerance lets a number at its pointer move by up to its bound, and no further.
    #[test]
    fn a_tolerance_bounds_one_number() {
        let doc = |estimate: f64| {
            format!("{{\"events\": [\n  {{\"metrics\":[{{\"estimate\":{estimate:?}}}]}}\n], \"counters\": {{}}}}\n")
        };
        let tolerance = [Tolerance { path: "/metrics/0/estimate".to_string(), abs: 1.0 }];
        assert!(agrees(&doc(3.0), &doc(3.0), &[]));
        assert!(!agrees(&doc(4.0), &doc(3.0), &[]));
        assert!(agrees(&doc(4.0), &doc(3.0), &tolerance));
        assert!(!agrees(&doc(5.0), &doc(3.0), &tolerance));
    }

    /// The table's schema rejects each kind of mistake, naming the row.
    #[test]
    fn the_table_rejects_a_malformed_row() {
        let table = |rows: &str| {
            format!(
                "classes: {{exact: a, normalized: b, degraded: c, lost: d, skipped: e, dropped: f}}\n\
                 sinks: [statsd_out]\nrows:\n{rows}"
            )
        };
        let cases = [
            ("  - {kind: gauge, cells: {statsd_out: {class: exact, text: t}}}\n", "names one of"),
            ("  - {kind: bogus, cells: {}}\n", "no kind"),
            (
                "  - {kind: samples, cells: {statsd_out: {class: normalized, text: t}}}\n",
                "names its reason",
            ),
            ("  - {kind: samples, cells: {statsd_out: {class: skipped, text: t}}}\n", "names its counter"),
            ("  - {kind: samples, cells: {statsd_out: {class: lost, text: t}}}\n", "can't be lost"),
            (
                "  - {kind: attributes, feature: string_value, cells: {statsd_out: {class: dropped, \
                 counter: x, text: t}}}\n",
                "a field row can't be",
            ),
            ("  - {kind: samples, cells: {otlp_out: {class: exact, text: t}}}\n", "isn't in sinks"),
            (
                "  - {kind: samples, cells: {statsd_out: [{class: exact, text: t}, {class: exact, \
                 text: u}]}}\n",
                "only the last cell has no when",
            ),
        ];
        for (rows, want) in cases {
            let err = mapping::parse(&table(rows)).expect_err(rows);
            assert!(format!("{err:#}").contains(want), "{rows}: {err:#}");
        }
        assert!(mapping::parse(&table(
            "  - {kind: samples, cells: {statsd_out: {class: exact, text: t}}}\n"
        ))
        .is_ok());
    }
}
