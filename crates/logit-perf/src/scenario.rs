//! Discovers `perf/scenarios/*.yaml` and reads the few fields the harness needs from each.
//!
//! Parses a bare [`serde_norway::Value`], not `logit_config::Config`: the crate doesn't depend on
//! `logit-config` for a handful of map lookups. `!env` goes unresolved here; no scenario uses it,
//! and `logit run` resolves it in the spawned binary.
//!
//! ## Two kinds of scenario
//!
//! A `Generated` scenario makes its own load in-process from a `generate_in` with a finite
//! `count`. A `Driven` one tests the socket path, so its load must arrive over a real socket from
//! outside the process: it has no generator, and a sidecar spec under `perf/load/` tells
//! `crate::load` what to send (docs/adr/udp-intake-batching-and-socket-visibility.md).
//!
//! **The sidecar lives in its own directory, not beside the scenario**, because both
//! `script/validate` and `crates/logit-cli/src/config.rs`'s
//! `every_shipped_config_loads_and_validates` glob `perf/scenarios/*.yaml`: everything there must
//! be a valid `logit` config, and a load spec isn't one.

use crate::load::{self, LoadSpec};
use anyhow::{bail, Context};
use serde_norway::Value;
use std::fs;
use std::path::{Path, PathBuf};

/// Where a driven scenario's sidecar load spec lives, relative to the scenarios directory:
/// `perf/scenarios/x.yaml` -> `perf/load/x.yaml`.
pub const LOAD_DIR: &str = "load";

/// How a scenario's events come into being.
#[derive(Debug, Clone, PartialEq)]
pub enum Workload {
    /// A `generate_in` component with a finite `count`, generating in-process.
    ///
    /// The count is the denominator for events/s and CPU-per-event, so an unbounded `generate_in`
    /// (valid for `logit`) is rejected here rather than left to hang.
    Generated { count: u64 },
    /// No generator: a real listener, fed over a real socket by `crate::load` from the sidecar
    /// spec at `perf/load/<name>.yaml`.
    Driven(LoadSpec),
}

impl Workload {
    /// The one-line description `run`/`list` print beside a scenario's name.
    ///
    /// Labels the number, since one kind counts events generated and the other datagrams sent.
    pub fn describe(&self) -> String {
        match self {
            Workload::Generated { count } => format!("count={count}"),
            Workload::Driven(spec) => format!("datagrams={}", spec.datagrams),
        }
    }

    pub fn is_driven(&self) -> bool {
        matches!(self, Workload::Driven(_))
    }
}

/// What the harness needs to know about one `perf/scenarios/*.yaml` file to run it.
#[derive(Debug, Clone, PartialEq)]
pub struct Scenario {
    /// The file stem (`passthrough` for `perf/scenarios/passthrough.yaml`); what `--scenario`
    /// matches and what a results file's `scenarios` map keys on.
    pub name: String,
    pub path: PathBuf,
    pub workload: Workload,
    /// Whether the process won't self-exit once `generate_in` stops sending.
    ///
    /// True when the graph has another listener (any `type` ending in `_in`) or `internal` (a
    /// ticker that runs until shutdown), and always for a `Driven` scenario. `run.rs` then waits
    /// `--settle` past the completion line and sends SIGTERM instead of waiting for an exit.
    pub needs_sigterm: bool,
    /// Every `buffer.disk.path` this scenario declares, one per disk-backed sink, as written in the
    /// YAML: relative to the scenario file and unresolved. `crate::spool::resolve_spool_dirs`
    /// resolves them and checks they stay inside `perf/results/`.
    pub disk_spool_paths: Vec<PathBuf>,
}

impl Scenario {
    /// This scenario's sidecar load spec path, whether or not it exists: `perf/scenarios/x.yaml`
    /// maps to `perf/load/x.yaml`.
    ///
    /// Discovery and every later re-read share this one derivation, so they can't disagree.
    pub fn load_spec_path(&self) -> anyhow::Result<PathBuf> {
        load_spec_path(&self.path, &self.name)
    }
}

fn load_spec_path(scenario_path: &Path, name: &str) -> anyhow::Result<PathBuf> {
    let scenarios_dir = scenario_path
        .parent()
        .with_context(|| format!("{} has no parent directory", scenario_path.display()))?;
    let perf_dir = scenarios_dir
        .parent()
        .with_context(|| format!("{} has no grandparent directory", scenario_path.display()))?;
    Ok(perf_dir.join(LOAD_DIR).join(format!("{name}.yaml")))
}

/// Discovers every `*.yaml` in `dir`, sorted by name.
///
/// One bad scenario file fails the whole discovery, naming the file, rather than being skipped.
pub fn discover(dir: &Path) -> anyhow::Result<Vec<Scenario>> {
    let mut scenarios = Vec::new();
    let entries = fs::read_dir(dir)
        .with_context(|| format!("reading scenario directory {}", dir.display()))?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if !path.extension().is_some_and(|extension| extension == "yaml") {
            continue;
        }
        // Dotfiles are not scenarios: `attribute` and `run` write rewritten copies here as
        // `.<name>.<purpose>.<pid>.yaml` so their relative paths still resolve
        // (`telemetry_leg::rewritten_config_path`), and a killed process can leave one behind.
        // Shell globs skip dotfiles; `read_dir` doesn't.
        if path.file_name().is_some_and(|name| name.to_string_lossy().starts_with('.')) {
            continue;
        }
        let name = path
            .file_stem()
            .map(|stem| stem.to_string_lossy().into_owned())
            .with_context(|| format!("{}: no file stem", path.display()))?;
        let yaml =
            fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
        let parsed = parse(&yaml).with_context(|| format!("{}", path.display()))?;
        let spec_path = load_spec_path(&path, &name)?;
        let workload =
            workload_for(&parsed, &spec_path).with_context(|| format!("{}", path.display()))?;
        scenarios.push(Scenario {
            name,
            path,
            needs_sigterm: parsed.needs_sigterm || workload.is_driven(),
            workload,
            disk_spool_paths: parsed.disk_spool_paths,
        });
    }
    scenarios.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(scenarios)
}

/// The one scenario named `name`, or an error listing every scenario there is.
///
/// `attribute` and `flamegraph` resolve `--scenario` through this, so a typo lists the names.
pub fn find(dir: &Path, name: &str) -> anyhow::Result<Scenario> {
    let scenarios = discover(dir)?;
    let known = scenarios.iter().map(|s| s.name.as_str()).collect::<Vec<_>>().join(", ");
    scenarios.into_iter().find(|scenario| scenario.name == name).with_context(|| {
        format!("no such scenario `{name}` under {} -- have: {known}", dir.display())
    })
}

/// What [`parse`] reads out of one scenario's YAML, before the sidecar has been looked for.
#[derive(Debug, Clone, PartialEq)]
struct Parsed {
    /// `Some(count)` when the scenario has one `generate_in` with a usable `count`, `None` when it
    /// has none. An unusable `generate_in` (no `count`, `count: 0`, or a second one) is an error
    /// from [`parse`], not a `None`.
    generated: Option<u64>,
    needs_sigterm: bool,
    disk_spool_paths: Vec<PathBuf>,
}

/// Decides a scenario's [`Workload`] from its config and whether a sidecar load spec exists.
/// Having both or neither is an error.
fn workload_for(parsed: &Parsed, spec_path: &Path) -> anyhow::Result<Workload> {
    let sidecar = spec_path.exists();
    match (parsed.generated, sidecar) {
        (Some(count), false) => Ok(Workload::Generated { count }),
        (None, true) => Ok(Workload::Driven(load::read_spec(spec_path)?)),
        (Some(_), true) => bail!(
            "this scenario has both a `generate_in` component and a load spec at {} -- a scenario \
             is driven one way or the other, never both. Remove the `generate_in` to make it a \
             real-socket scenario, or delete the load spec to keep it generator-driven",
            spec_path.display()
        ),
        (None, false) => bail!(
            "no `generate_in` component -- every perf scenario needs exactly one, or a sidecar \
             load spec at {} to drive it over a real socket instead \
             (see perf/load/README.md)",
            spec_path.display()
        ),
    }
}

/// Parses one scenario's YAML; split from [`discover`] so tests need no filesystem.
fn parse(yaml: &str) -> anyhow::Result<Parsed> {
    let value: Value = serde_norway::from_str(yaml).context("parsing YAML")?;
    let components = value
        .get("components")
        .and_then(Value::as_mapping)
        .context("no top-level `components` mapping")?;

    let mut count: Option<u64> = None;
    let mut found_generate_in = false;
    let mut needs_sigterm = false;
    let mut disk_spool_paths = Vec::new();

    for (id, component) in components {
        let kind = component
            .get("type")
            .and_then(Value::as_str)
            .with_context(|| format!("component `{}` has no `type`", describe_key(id)))?;

        if kind == "generate_in" {
            if found_generate_in {
                bail!("more than one `generate_in` component; the harness expects exactly one");
            }
            found_generate_in = true;
            count = component.get("count").and_then(Value::as_u64);
        } else if kind == "internal" || kind.ends_with("_in") {
            needs_sigterm = true;
        }
        if let Some(path) = disk_spool_path(component) {
            disk_spool_paths.push(path);
        }
    }

    let generated = if found_generate_in {
        let count = count.context(
            "`generate_in` has no `count` (or it isn't a plain integer) -- an unbounded generator \
             can't be turned into events/s or CPU-per-event, so the harness requires a finite count",
        )?;
        if count == 0 {
            bail!("`generate_in.count` is 0 -- graph rule 42 already rejects this at `logit validate` time");
        }
        Some(count)
    } else {
        None
    };

    Ok(Parsed { generated, needs_sigterm, disk_spool_paths })
}

/// `component.buffer.disk.path`, if present, unresolved.
///
/// Matches the shape on any component kind: a disk-backed sink of any kind spools the same way,
/// and graph validation (not run here) is what rejects a `buffer:` on a non-sink. `crate::spool`
/// resolves and checks the path before anything is removed.
fn disk_spool_path(component: &Value) -> Option<PathBuf> {
    component.get("buffer")?.get("disk")?.get("path")?.as_str().map(PathBuf::from)
}

/// Renders a YAML mapping key for an error message: a string key as typed, not `serde_norway`'s
/// `Debug` form (`String("gen")`); any other key as `Debug`.
fn describe_key(key: &Value) -> String {
    key.as_str().map(str::to_string).unwrap_or_else(|| format!("{key:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn yaml(components: &str) -> String {
        format!("components:\n{components}")
    }

    /// A [`Parsed`] with no generator, for the sidecar-combination tests.
    fn driven_parse() -> Parsed {
        Parsed { generated: None, needs_sigterm: true, disk_spool_paths: Vec::new() }
    }

    fn generated_parse() -> Parsed {
        Parsed { generated: Some(100), needs_sigterm: false, disk_spool_paths: Vec::new() }
    }

    /// A real spec file in a temp directory, so the `exists()` check in `workload_for` has
    /// something to find. Returns the path; the directory is left behind (a few hundred bytes,
    /// and easier to inspect than to clean up on a failing test).
    fn temp_spec(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("logit-perf-scenario-{}-{name}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let model = dir.join("m.yaml");
        fs::write(&model, "lines:\n  - { weight: 1, template: \"a.{seq%3}:1|c\" }\n").unwrap();
        let path = dir.join(format!("{name}.yaml"));
        fs::write(
            &path,
            "target: statsd\ndatagrams: 100\nmodel: m.yaml\ndatagram_mix:\n  - { weight: 1, single: true }\n",
        )
        .unwrap();
        path
    }

    #[test]
    fn extracts_count_from_a_bare_generate_in_to_null_out() {
        let parsed = parse(&yaml(
            "  gen:\n    type: generate_in\n    count: 5000000\n  out:\n    type: null_out\n    sources: [gen]\n",
        ))
        .unwrap();
        assert_eq!(parsed.generated, Some(5_000_000));
        assert!(!parsed.needs_sigterm);
        assert!(parsed.disk_spool_paths.is_empty());
    }

    #[test]
    fn a_socket_listener_alongside_generate_in_needs_sigterm() {
        let parsed = parse(&yaml(
            "  gen:\n    type: generate_in\n    count: 100\n  relay_in:\n    type: logit_in\n    bind: \"127.0.0.1:0\"\n  out:\n    type: null_out\n    sources: [gen, relay_in]\n",
        ))
        .unwrap();
        assert!(parsed.needs_sigterm);
    }

    #[test]
    fn an_internal_component_alongside_generate_in_needs_sigterm() {
        let parsed = parse(&yaml(
            "  gen:\n    type: generate_in\n    count: 100\n  self:\n    type: internal\n    interval: 1s\n  out:\n    type: null_out\n    sources: [gen, self]\n",
        ))
        .unwrap();
        assert!(parsed.needs_sigterm);
    }

    #[test]
    fn a_plain_transform_does_not_need_sigterm() {
        let parsed = parse(&yaml(
            "  gen:\n    type: generate_in\n    count: 100\n  j:\n    type: json\n    sources: [gen]\n  out:\n    type: null_out\n    sources: [j]\n",
        ))
        .unwrap();
        assert!(!parsed.needs_sigterm);
    }

    #[test]
    fn extracts_a_disk_spool_path_from_a_sinks_buffer() {
        let parsed = parse(&yaml(
            "  gen:\n    type: generate_in\n    count: 100\n  out:\n    type: null_out\n    sources: [gen]\n    buffer:\n      disk:\n        path: ../results/spool\n",
        ))
        .unwrap();
        assert_eq!(parsed.disk_spool_paths, vec![PathBuf::from("../results/spool")]);
    }

    #[test]
    fn a_memory_buffer_with_no_disk_block_has_no_spool_path() {
        let parsed = parse(&yaml(
            "  gen:\n    type: generate_in\n    count: 100\n  out:\n    type: null_out\n    sources: [gen]\n    buffer:\n      max_batches: 10\n",
        ))
        .unwrap();
        assert!(parsed.disk_spool_paths.is_empty());
    }

    #[test]
    fn collects_a_disk_spool_path_from_every_disk_backed_sink() {
        let parsed = parse(&yaml(
            "  gen:\n    type: generate_in\n    count: 100\n  a:\n    type: null_out\n    sources: [gen]\n    buffer:\n      disk:\n        path: ../results/a-spool\n  b:\n    type: null_out\n    sources: [gen]\n    buffer:\n      disk:\n        path: ../results/b-spool\n",
        ))
        .unwrap();
        assert_eq!(
            parsed.disk_spool_paths,
            vec![PathBuf::from("../results/a-spool"), PathBuf::from("../results/b-spool")]
        );
    }

    #[test]
    fn a_scenario_with_no_generator_parses_to_no_workload_of_its_own() {
        let parsed = parse(&yaml(
            "  statsd:\n    type: statsd_in\n    bind: 127.0.0.1:18125\n  out:\n    type: null_out\n    sources: [statsd]\n",
        ))
        .unwrap();
        assert_eq!(parsed.generated, None);
        assert!(parsed.needs_sigterm, "a socket listener never self-exits");
    }

    #[test]
    fn unbounded_generate_in_is_an_error() {
        let err = parse(&yaml(
            "  gen:\n    type: generate_in\n  out:\n    type: null_out\n    sources: [gen]\n",
        ))
        .expect_err("should reject a scenario with no count");
        assert!(format!("{err:#}").contains("no `count`"), "{err:#}");
    }

    #[test]
    fn zero_count_is_an_error() {
        let err = parse(&yaml(
            "  gen:\n    type: generate_in\n    count: 0\n  out:\n    type: null_out\n    sources: [gen]\n",
        ))
        .expect_err("should reject a zero count");
        assert!(format!("{err:#}").contains("count` is 0"), "{err:#}");
    }

    #[test]
    fn two_generate_in_components_is_an_error() {
        let err = parse(&yaml(
            "  a:\n    type: generate_in\n    count: 1\n  b:\n    type: generate_in\n    count: 1\n  out:\n    type: null_out\n    sources: [a, b]\n",
        ))
        .expect_err("should reject two generate_in components");
        assert!(format!("{err:#}").contains("more than one"), "{err:#}");
    }

    // The four scenario/sidecar combinations.

    #[test]
    fn a_generator_with_no_sidecar_is_a_generated_workload() {
        let missing = std::env::temp_dir().join("logit-perf-no-such-spec.yaml");
        assert_eq!(
            workload_for(&generated_parse(), &missing).unwrap(),
            Workload::Generated { count: 100 }
        );
    }

    #[test]
    fn a_sidecar_with_no_generator_is_a_driven_workload() {
        let spec_path = temp_spec("driven");
        let workload = workload_for(&driven_parse(), &spec_path).unwrap();
        match workload {
            Workload::Driven(spec) => {
                assert_eq!(spec.target, "statsd");
                assert_eq!(spec.datagrams, 100);
            }
            other => panic!("expected a driven workload, got {other:?}"),
        }
    }

    #[test]
    fn both_a_generator_and_a_sidecar_is_an_error() {
        let spec_path = temp_spec("both");
        let err = workload_for(&generated_parse(), &spec_path)
            .expect_err("a scenario is driven one way or the other");
        assert!(format!("{err:#}").contains("never both"), "{err:#}");
    }

    #[test]
    fn neither_a_generator_nor_a_sidecar_is_an_error_pointing_at_both() {
        let missing = std::env::temp_dir().join("logit-perf-no-such-spec.yaml");
        let err = workload_for(&driven_parse(), &missing)
            .expect_err("a scenario needs one source of load or the other");
        let err = format!("{err:#}");
        assert!(err.contains("no `generate_in` component"), "{err}");
        assert!(err.contains("perf/load/README.md"), "{err}");
    }

    #[test]
    fn a_sidecar_path_is_derived_from_the_scenario_path() {
        assert_eq!(
            load_spec_path(Path::new("/repo/perf/scenarios/udp-statsd.yaml"), "udp-statsd")
                .unwrap(),
            Path::new("/repo/perf/load/udp-statsd.yaml")
        );
    }

    #[test]
    fn describe_labels_the_number_it_is_reporting() {
        assert_eq!(Workload::Generated { count: 7 }.describe(), "count=7");
    }
}
