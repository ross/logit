//! Discovers `perf/scenarios/*.yaml` and reads just enough out of each to drive the harness --
//! never the whole config (docs/plans/load-test-harness.md's "Harness" section).
//!
//! Deliberately parses as a bare [`serde_norway::Value`], not [`logit_config::Config`]: this
//! crate isn't a `logit-config`/`logit-pipeline` dependent (docs/plans/load-test-harness.md's W5
//! row), and doesn't need to be -- it only ever reads a handful of things out of a YAML map, and
//! never resolves `!env` (a scenario file never uses it; `logit run` resolves it for real when the
//! harness spawns the binary). Reaching for the real config types here would mean a dependency
//! this tool doesn't otherwise need, just to read a few fields.
//!
//! ## Two kinds of scenario
//!
//! Every scenario up to [ADR `udp-intake-batching-and-socket-visibility`](../../../docs/adr/udp-intake-batching-and-socket-visibility.md)
//! generated its own load in-process, from a `generate_in` component with a finite `count`. A UDP
//! scenario can't: the thing under test *is* the socket path, so the load has to arrive over a real
//! socket, from outside the process. [`Workload`] is that fork -- `Generated` is exactly what came
//! before, `Driven` is a scenario with no generator at all plus a sidecar load spec under
//! `perf/load/` telling `crate::load` what to send it.
//!
//! **The sidecar lives in its own directory, not beside the scenario**, because both
//! `script/validate` and `crates/logit-cli/src/config.rs`'s `every_shipped_config_loads_and_validates`
//! glob `perf/scenarios/*.yaml` unconditionally: anything dropped in there has to be a valid
//! `logit` config, and a load spec isn't one.

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
    /// A `generate_in` component with a finite `count`, generating in-process. Required to be
    /// finite: a scenario the harness runs must have a denominator for events/s and
    /// CPU-per-event -- an unbounded `generate_in` (soak / profiler-attach) is a real, valid config
    /// the runtime supports, just not one this harness can turn into a measurement, so it's
    /// rejected rather than left to hang.
    Generated { count: u64 },
    /// No generator: a real listener, fed over a real socket by `crate::load` from the sidecar
    /// spec at `perf/load/<name>.yaml`.
    Driven(LoadSpec),
}

impl Workload {
    /// The one-line description `run`/`list` print beside a scenario's name. The two kinds have
    /// no comparable "count" (one is events generated, the other datagrams sent), so this labels
    /// the number rather than pretending they're the same quantity.
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
    /// The file stem (`passthrough` for `perf/scenarios/passthrough.yaml`) -- also what
    /// `--scenario` filters against and what a results file's `scenarios` map keys on.
    pub name: String,
    pub path: PathBuf,
    pub workload: Workload,
    /// Whether some other listener in the graph won't self-exit once `generate_in` stops sending
    /// -- a socket listener (any other kind whose `type` ends in `_in`) or `internal` (a ticker
    /// that runs until shutdown, docs/plans/load-test-harness.md's `internal` final-drain note).
    /// When true, `run.rs` waits `--settle` after the completion line, then sends SIGTERM, rather
    /// than waiting for the process to exit on its own. Always true for a `Driven` scenario, which
    /// is a socket listener by construction.
    pub needs_sigterm: bool,
    /// Every `buffer.disk.path` this scenario declares, one per disk-backed sink, **as written in
    /// the YAML** -- relative to this scenario's own file, exactly like every other path a
    /// component config carries, and not yet resolved against it. `crate::spool::resolve_spool_dirs`
    /// does that resolution the same way `logit` itself does
    /// (`crates/logit-cli/src/pipeline.rs`'s `queue_config`), since it also needs to check the
    /// result stays inside `perf/results/` before anything touches the filesystem.
    pub disk_spool_paths: Vec<PathBuf>,
}

impl Scenario {
    /// This scenario's sidecar load spec path, whether or not it exists: `perf/scenarios/x.yaml`
    /// -> `perf/load/x.yaml`. One derivation, used by discovery and by every consumer that needs
    /// to re-read the spec, so the two can never disagree about where it is.
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

/// Discovers every `*.yaml` in `dir`, sorted by name for deterministic output. A single bad
/// scenario file fails the whole discovery with its own name in the error, rather than silently
/// skipping it -- an unreadable or malformed scenario is a bug to fix, not a scenario to drop.
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
        // Dotfiles are not scenarios: `attribute` writes its rewritten copy here as
        // `.<name>.attribute.<pid>.yaml` so relative paths inside it still resolve against this
        // directory (crates/logit-perf/src/attribute.rs's `rewritten_config_path`). It removes it
        // on every exit path, but a killed process could leave one, and discovering it as a
        // scenario in its own right would be a confusing way to find that out. Shell globs
        // (`script/validate`'s `perf/scenarios/*.yaml`) skip these for free; `read_dir` doesn't.
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

/// The one scenario named `name`, or an error naming every scenario there is -- what the
/// single-scenario subcommands (`attribute`, `flamegraph`) resolve `--scenario` through, so a typo
/// reports the available names instead of a bare "not found".
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
    /// `Some(count)` when the scenario has exactly one `generate_in` carrying a usable `count`.
    /// `None` when it has no `generate_in` at all -- a `generate_in` that *is* present but
    /// unusable (no `count`, `count: 0`, or a second one) is an error from [`parse`] itself, not
    /// a `None` here, so those messages stay exactly where they were.
    generated: Option<u64>,
    needs_sigterm: bool,
    disk_spool_paths: Vec<PathBuf>,
}

/// Decides which [`Workload`] a scenario has, from what's in the config and whether a sidecar load
/// spec exists beside it. All four combinations are accounted for; two of them are errors.
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

/// The parsing logic proper, split out from [`discover`] so it's testable against inline YAML
/// strings with no filesystem involved.
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

/// `component.buffer.disk.path`, if present -- the raw string as written in the YAML, unresolved.
/// Any component can carry a `buffer:` block (graph validation rejects one on a non-sink kind, but
/// this reads the bare `Value` before that check ever runs), so this simply looks for the shape
/// and ignores anything that doesn't have it, rather than restricting itself to `type: null_out`
/// or any other specific kind -- a disk-backed sink under any implemented kind spools the same
/// way. See `crate::spool` for what resolves and validates this path before it's ever removed.
fn disk_spool_path(component: &Value) -> Option<PathBuf> {
    component.get("buffer")?.get("disk")?.get("path")?.as_str().map(PathBuf::from)
}

/// Renders a YAML mapping key for an error message -- the key is almost always a plain string
/// (a component id), so this prints it the way a reader typed it rather than `serde_norway`'s
/// `Debug` form (`String("gen")`); only a non-string key (a YAML oddity no shipped scenario
/// produces) falls back to `Debug`.
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
