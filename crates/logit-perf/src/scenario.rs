//! Discovers `perf/scenarios/*.yaml` and reads just enough out of each to drive the harness --
//! never the whole config (docs/plans/load-test-harness.md's "Harness" section).
//!
//! Deliberately parses as a bare [`serde_norway::Value`], not [`logit_config::Config`]: this
//! crate isn't a `logit-config`/`logit-pipeline` dependent (docs/plans/load-test-harness.md's W5
//! row), and doesn't need to be -- it only ever reads two things, the single `generate_in`
//! component's `count` and whether any other listener is present, and never resolves `!env`
//! (a scenario file never uses it; `logit run` resolves it for real when the harness spawns the
//! binary). Reaching for the real config types here would mean a dependency this tool doesn't
//! otherwise need, just to read two fields out of a YAML map.

use anyhow::{bail, Context};
use serde_norway::Value;
use std::fs;
use std::path::{Path, PathBuf};

/// What the harness needs to know about one `perf/scenarios/*.yaml` file to run it.
#[derive(Debug, Clone, PartialEq)]
pub struct Scenario {
    /// The file stem (`passthrough` for `perf/scenarios/passthrough.yaml`) -- also what
    /// `--scenario` filters against and what a results file's `scenarios` map keys on.
    pub name: String,
    pub path: PathBuf,
    /// The `generate_in` component's `count`. Required: a scenario the harness runs must be
    /// finite, so events/s and CPU-per-event have a denominator -- an unbounded `generate_in`
    /// (soak / profiler-attach) is a real, valid config the runtime supports, just not one this
    /// harness can turn into a measurement, so it's rejected here rather than left to hang.
    pub count: u64,
    /// Whether some other listener in the graph won't self-exit once `generate_in` stops sending
    /// -- a socket listener (any other kind whose `type` ends in `_in`) or `internal` (a ticker
    /// that runs until shutdown, docs/plans/load-test-harness.md's `internal` final-drain note).
    /// When true, `run.rs` waits `--settle` after the completion line, then sends SIGTERM, rather
    /// than waiting for the process to exit on its own.
    pub needs_sigterm: bool,
    /// Every `buffer.disk.path` this scenario declares, one per disk-backed sink, **as written in
    /// the YAML** -- relative to this scenario's own file, exactly like every other path a
    /// component config carries, and not yet resolved against it. `crate::spool::resolve_spool_dirs`
    /// does that resolution the same way `logit` itself does
    /// (`crates/logit-cli/src/pipeline.rs`'s `queue_config`), since it also needs to check the
    /// result stays inside `perf/results/` before anything touches the filesystem.
    pub disk_spool_paths: Vec<PathBuf>,
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
        let (count, needs_sigterm, disk_spool_paths) =
            parse(&yaml).with_context(|| format!("{}", path.display()))?;
        scenarios.push(Scenario { name, path, count, needs_sigterm, disk_spool_paths });
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

/// The parsing logic proper, split out from [`discover`] so it's testable against inline YAML
/// strings with no filesystem involved.
fn parse(yaml: &str) -> anyhow::Result<(u64, bool, Vec<PathBuf>)> {
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

    if !found_generate_in {
        bail!("no `generate_in` component -- every perf scenario needs exactly one");
    }
    let count = count.context(
        "`generate_in` has no `count` (or it isn't a plain integer) -- an unbounded generator \
         can't be turned into events/s or CPU-per-event, so the harness requires a finite count",
    )?;
    if count == 0 {
        bail!("`generate_in.count` is 0 -- graph rule 42 already rejects this at `logit validate` time");
    }

    Ok((count, needs_sigterm, disk_spool_paths))
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

    #[test]
    fn extracts_count_from_a_bare_generate_in_to_null_out() {
        let (count, needs_sigterm, disk_spool_paths) = parse(&yaml(
            "  gen:\n    type: generate_in\n    count: 5000000\n  out:\n    type: null_out\n    sources: [gen]\n",
        ))
        .unwrap();
        assert_eq!(count, 5_000_000);
        assert!(!needs_sigterm);
        assert!(disk_spool_paths.is_empty());
    }

    #[test]
    fn a_socket_listener_alongside_generate_in_needs_sigterm() {
        let (_, needs_sigterm, _) = parse(&yaml(
            "  gen:\n    type: generate_in\n    count: 100\n  relay_in:\n    type: logit_in\n    bind: \"127.0.0.1:0\"\n  out:\n    type: null_out\n    sources: [gen, relay_in]\n",
        ))
        .unwrap();
        assert!(needs_sigterm);
    }

    #[test]
    fn an_internal_component_alongside_generate_in_needs_sigterm() {
        let (_, needs_sigterm, _) = parse(&yaml(
            "  gen:\n    type: generate_in\n    count: 100\n  self:\n    type: internal\n    interval: 1s\n  out:\n    type: null_out\n    sources: [gen, self]\n",
        ))
        .unwrap();
        assert!(needs_sigterm);
    }

    #[test]
    fn a_plain_transform_does_not_need_sigterm() {
        let (_, needs_sigterm, _) = parse(&yaml(
            "  gen:\n    type: generate_in\n    count: 100\n  j:\n    type: json\n    sources: [gen]\n  out:\n    type: null_out\n    sources: [j]\n",
        ))
        .unwrap();
        assert!(!needs_sigterm);
    }

    #[test]
    fn extracts_a_disk_spool_path_from_a_sinks_buffer() {
        let (_, _, disk_spool_paths) = parse(&yaml(
            "  gen:\n    type: generate_in\n    count: 100\n  out:\n    type: null_out\n    sources: [gen]\n    buffer:\n      disk:\n        path: ../results/spool\n",
        ))
        .unwrap();
        assert_eq!(disk_spool_paths, vec![PathBuf::from("../results/spool")]);
    }

    #[test]
    fn a_memory_buffer_with_no_disk_block_has_no_spool_path() {
        let (_, _, disk_spool_paths) = parse(&yaml(
            "  gen:\n    type: generate_in\n    count: 100\n  out:\n    type: null_out\n    sources: [gen]\n    buffer:\n      max_batches: 10\n",
        ))
        .unwrap();
        assert!(disk_spool_paths.is_empty());
    }

    #[test]
    fn collects_a_disk_spool_path_from_every_disk_backed_sink() {
        let (_, _, disk_spool_paths) = parse(&yaml(
            "  gen:\n    type: generate_in\n    count: 100\n  a:\n    type: null_out\n    sources: [gen]\n    buffer:\n      disk:\n        path: ../results/a-spool\n  b:\n    type: null_out\n    sources: [gen]\n    buffer:\n      disk:\n        path: ../results/b-spool\n",
        ))
        .unwrap();
        assert_eq!(
            disk_spool_paths,
            vec![PathBuf::from("../results/a-spool"), PathBuf::from("../results/b-spool")]
        );
    }

    #[test]
    fn missing_generate_in_is_an_error() {
        let err = parse(&yaml("  out:\n    type: null_out\n"))
            .expect_err("should reject a scenario with no generate_in");
        assert!(format!("{err:#}").contains("no `generate_in` component"), "{err:#}");
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
}
