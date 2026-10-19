//! The transit spec's generator: runs every case of `spec/transit/` through every flow that reads
//! its source system, compares each run's document with its committed expectation, and checks the
//! run against `mapping.yaml` (`super::classes`). `spec/transit/README.md` is the reader's account.
//!
//! Under `TRANSIT_UPDATE=1` ([`UPDATE`]) it rewrites what's generated: a wire case's `.json`
//! from its wire file, each `expected/<flow>/<case>.json`, and `coverage.md`, and removes an
//! expectation no flow and case produce. Unset, it compares, and a stale or missing file fails
//! with a diff and the command that regenerates it. A class check failure is never rewritten.
//!
//! **Cases.** A case is `cases/<system>/<case>.meta.yaml` beside `<case>.json`. The meta names
//! the features from `features/<system>.yaml` the case `exercises`, an optional `wire:` file
//! (relative to the case's directory) the `.json` is decoded from by `<system>_in`'s decoder at
//! [`RECEIVED_AT`], and optional `tolerance:` entries (`{path, abs}`: an RFC 6901 pointer into
//! each event of the document, and the most the number there may differ by). A hand-written case
//! has no `wire:`, and its `.json` must read back to itself through `logit_proto::event_json`
//! and `format: json`, so each case has one spelling.
//!
//! **Flows.** A flow is `flows/<flow>.yaml` whose one `replay_in`'s `path:` names a case; the
//! directory holding that case is the flow's source, and every case in it runs through the flow
//! with [`Options::default`]. The flow driver's module doc (`super`) lists what a flow may hold.

use super::classes::{self, At, Ledger};
use super::mapping::{self, Mapping};
use super::{run_traced, Options};
use anyhow::{bail, ensure, Context};
use bytes::Bytes;
use logit_config::ComponentKind;
use logit_core::{Diagnostics, EventBatch, Resource};
use logit_outputs::ndjson::render_ndjson;
use logit_pipeline::test_util::TelemetryProbe;
use logit_proto::event_json::EventJsonDecoder;
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
fn spec_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../spec/transit")
}

fn updating() -> bool {
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
fn cases(spec: &Path, system: &str) -> anyhow::Result<Vec<Case>> {
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
    Ok(cases)
}

fn render(batches: &[EventBatch]) -> String {
    let mut text = String::new();
    for batch in batches {
        render_ndjson(&mut text, batch);
    }
    text
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
    decode_wire(&case.system, bytes).with_context(|| format!("decoding {}", path.display()))
}

/// `system`'s wire bytes as its listener's decoder reads them, at [`RECEIVED_AT`]. A source
/// system joins with an arm here.
fn decode_wire(system: &str, bytes: Vec<u8>) -> anyhow::Result<String> {
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
fn diff(actual: &str, expected: &str) -> String {
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

/// Runs the whole spec under `spec`, returning every failure.
fn generate(spec: &Path, update: bool) -> Vec<String> {
    let mut failures = Vec::new();
    let mapping = match mapping::load(&spec.join("mapping.yaml")) {
        Ok(mapping) => mapping,
        Err(err) => return vec![format!("{err:#}")],
    };
    let mut by_system: BTreeMap<String, Vec<Case>> = BTreeMap::new();
    match systems(spec) {
        Ok(systems) => {
            for system in systems {
                match cases(spec, &system) {
                    Ok(cases) => {
                        by_system.insert(system, cases);
                    }
                    Err(err) => failures.push(format!("{err:#}")),
                }
            }
        }
        Err(err) => failures.push(format!("reading spec/transit/cases: {err:#}")),
    }
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
            return failures;
        }
    };
    let mut ledger = Ledger::default();
    let mut produced: Vec<PathBuf> = Vec::new();
    for flow in &flows {
        for case in by_system.get(&flow.system).into_iter().flatten() {
            failures.extend(run_one(
                spec,
                &mapping,
                flow,
                case,
                update,
                &mut ledger,
                &mut produced,
            ));
        }
    }
    failures.extend(ledger.finish());
    for line in ledger.unspecified() {
        eprintln!("transit: {line}");
    }
    failures.extend(stale_expectations(spec, &produced, update));
    failures
}

/// One flow and one case: the document against its expectation, then the class check.
fn run_one(
    spec: &Path,
    mapping: &Mapping,
    flow: &Flow,
    case: &Case,
    update: bool,
    ledger: &mut Ledger,
    produced: &mut Vec<PathBuf>,
) -> Vec<String> {
    let here = format!("{} × {}", flow.name, case.name);
    let traced = match run_traced(&flow.path, &case.json(), Options::default()) {
        Ok(traced) => traced,
        Err(err) => return vec![format!("{here}: {err:#}")],
    };
    let expected = spec.join("expected").join(&flow.name).join(format!("{}.json", case.name));
    produced.push(expected.clone());
    let mut failures: Vec<String> =
        sync(&expected, &traced.outcome.document(), &case.meta.tolerance, update)
            .into_iter()
            .collect();
    failures.extend(classes::check(
        mapping,
        &At { flow: &flow.name, case: &case.name },
        &traced,
        ledger,
    ));
    failures
}

/// Expectations no flow and case produced: removed under `update`, else failures.
fn stale_expectations(spec: &Path, produced: &[PathBuf], update: bool) -> Vec<String> {
    let mut failures = Vec::new();
    let root = spec.join("expected");
    let Ok(dirs) = std::fs::read_dir(&root) else { return failures };
    for dir in dirs.flatten().map(|e| e.path()) {
        let files: Vec<PathBuf> = match std::fs::read_dir(&dir) {
            Ok(files) => files.flatten().map(|e| e.path()).collect(),
            Err(_) => vec![dir.clone()],
        };
        for file in files {
            if produced.contains(&file) {
                continue;
            }
            if update {
                std::fs::remove_file(&file).expect("remove a stale expectation");
            } else {
                failures.push(format!(
                    "{} matches no flow and case; run `{REGENERATE}` to remove it",
                    file.display()
                ));
            }
        }
        if update && std::fs::read_dir(&dir).is_ok_and(|mut d| d.next().is_none()) {
            std::fs::remove_dir(&dir).expect("remove an empty expectation directory");
        }
    }
    failures
}

/// `coverage.md`: each system's features and the cases that exercise them.
fn coverage(spec: &Path) -> anyhow::Result<String> {
    let mut out = String::from(
        "# Transit spec coverage\n\nGenerated by `script/transit coverage` from each system's \
         `features/<system>.yaml` and its cases' `exercises:` lists; don't edit it by hand. A \
         feature no case exercises shows `0`.\n",
    );
    for system in systems(spec)? {
        let features = features(spec, &system)?;
        let cases = cases(spec, &system)?;
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
    Ok(out)
}

/// The spec under `spec/transit/` (module doc).
#[test]
fn transit_spec() {
    let failures = generate(&spec_dir(), updating());
    assert!(failures.is_empty(), "{} failure(s):\n\n{}", failures.len(), failures.join("\n\n"));
}

/// `spec/transit/coverage.md` is current.
#[test]
fn transit_coverage() {
    let spec = spec_dir();
    let text = coverage(&spec).expect("read the features and cases");
    if let Some(failure) = sync(&spec.join("coverage.md"), &text, &[], updating()) {
        panic!("{failure}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn relay() -> (Flow, Case) {
        let spec = spec_dir();
        let flow = flow(&spec, &spec.join("flows/statsd-relay.yaml")).expect("the relay flow");
        let case = cases(&spec, "statsd")
            .expect("statsd cases")
            .into_iter()
            .find(|c| c.name == "counter")
            .expect("the counter case");
        (flow, case)
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

    /// An expectation edited by hand fails the comparison, with the command that regenerates it.
    #[test]
    fn an_edited_expectation_fails_the_comparison() {
        let (flow, case) = relay();
        let path = spec_dir().join("expected").join(&flow.name).join("counter.json");
        let committed = std::fs::read_to_string(&path).expect("the committed expectation");
        let document = run_traced(&flow.path, &case.json(), Options::default())
            .expect("runs")
            .outcome
            .document();
        assert_eq!(sync(&path, &document, &[], false), None, "the committed file is current");

        let edited = committed.replacen("\"value\":3.0", "\"value\":4.0", 1);
        assert_ne!(edited, committed, "the expectation holds the counter's 3.0");
        let scratch = logit_pipeline::test_util::scratch_dir("transit-edited").join("counter.json");
        std::fs::write(&scratch, edited).expect("write the edited copy");
        let failure = sync(&scratch, &document, &[], false).expect("the edit is caught");
        assert!(failure.contains(REGENERATE) && failure.contains("\"value\":4.0"), "{failure}");
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
