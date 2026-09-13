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
        let name = path
            .file_stem()
            .map(|stem| stem.to_string_lossy().into_owned())
            .with_context(|| format!("{}: no file stem", path.display()))?;
        let yaml = fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?;
        let (count, needs_sigterm) =
            parse(&yaml).with_context(|| format!("{}", path.display()))?;
        scenarios.push(Scenario { name, path, count, needs_sigterm });
    }
    scenarios.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(scenarios)
}

/// The parsing logic proper, split out from [`discover`] so it's testable against inline YAML
/// strings with no filesystem involved.
fn parse(yaml: &str) -> anyhow::Result<(u64, bool)> {
    let value: Value = serde_norway::from_str(yaml).context("parsing YAML")?;
    let components = value
        .get("components")
        .and_then(Value::as_mapping)
        .context("no top-level `components` mapping")?;

    let mut count: Option<u64> = None;
    let mut found_generate_in = false;
    let mut needs_sigterm = false;

    for (id, component) in components {
        let kind = component
            .get("type")
            .and_then(Value::as_str)
            .with_context(|| format!("component `{id:?}` has no `type`"))?;

        if kind == "generate_in" {
            if found_generate_in {
                bail!("more than one `generate_in` component; the harness expects exactly one");
            }
            found_generate_in = true;
            count = component.get("count").and_then(Value::as_u64);
        } else if kind == "internal" || kind.ends_with("_in") {
            needs_sigterm = true;
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

    Ok((count, needs_sigterm))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn yaml(components: &str) -> String {
        format!("components:\n{components}")
    }

    #[test]
    fn extracts_count_from_a_bare_generate_in_to_null_out() {
        let (count, needs_sigterm) = parse(&yaml(
            "  gen:\n    type: generate_in\n    count: 5000000\n  out:\n    type: null_out\n    sources: [gen]\n",
        ))
        .unwrap();
        assert_eq!(count, 5_000_000);
        assert!(!needs_sigterm);
    }

    #[test]
    fn a_socket_listener_alongside_generate_in_needs_sigterm() {
        let (_, needs_sigterm) = parse(&yaml(
            "  gen:\n    type: generate_in\n    count: 100\n  relay_in:\n    type: logit_in\n    bind: \"127.0.0.1:0\"\n  out:\n    type: null_out\n    sources: [gen, relay_in]\n",
        ))
        .unwrap();
        assert!(needs_sigterm);
    }

    #[test]
    fn an_internal_component_alongside_generate_in_needs_sigterm() {
        let (_, needs_sigterm) = parse(&yaml(
            "  gen:\n    type: generate_in\n    count: 100\n  self:\n    type: internal\n    interval: 1s\n  out:\n    type: null_out\n    sources: [gen, self]\n",
        ))
        .unwrap();
        assert!(needs_sigterm);
    }

    #[test]
    fn a_plain_transform_does_not_need_sigterm() {
        let (_, needs_sigterm) = parse(&yaml(
            "  gen:\n    type: generate_in\n    count: 100\n  j:\n    type: json\n    sources: [gen]\n  out:\n    type: null_out\n    sources: [j]\n",
        ))
        .unwrap();
        assert!(!needs_sigterm);
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
