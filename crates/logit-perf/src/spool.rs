//! Clears a disk-backed scenario's spool directory before it's spawned.
//!
//! `buffered.yaml`'s spool is a crash-recoverable disk queue
//! (`docs/adr/disk-backed-sink-buffer.md`), and `DiskQueue::open` reads and CRC-walks the whole
//! active segment on every startup, whether or not anything is left to replay. Left alone across
//! repeats or `script/perf` invocations, the spool only grows, and each run re-validates a larger
//! file than the last: a monotonic throughput collapse (`docs/known-gaps.md`'s `buffered` entry,
//! `docs/design/performance.md`). Clearing it before every spawn removes the accumulation; the
//! per-startup scan itself is still `DiskQueue::open`'s cost and open work.
//!
//! **Never removes anything outside `<repo root>/perf/results/`.** A scenario's `buffer.disk.path`
//! resolves against the scenario file's directory, as `logit` resolves it
//! (`crates/logit-cli/src/pipeline.rs`'s `queue_config`). This harness resolves it the same way
//! and refuses the result unless it lands inside `perf/results/`, where every shipped scenario
//! keeps its spool. A spool path resolving anywhere else is a scenario bug to surface, not a
//! directory to `rm -rf`.

use crate::scenario::Scenario;
use anyhow::{bail, Context};
use std::fs;
use std::path::{Component, Path, PathBuf};

/// Resolves every `buffer.disk.path` `scenario` declares against its own file's directory.
///
/// Resolution matches `logit`'s own (`crates/logit-cli/src/pipeline.rs`'s `queue_config`), then
/// normalizes lexically rather than with [`std::fs::canonicalize`]: the spool may not exist yet,
/// and [`clear`]'s containment check has to work before it does.
pub fn resolve_spool_dirs(scenario: &Scenario) -> Vec<PathBuf> {
    let base_dir = scenario.path.parent().unwrap_or_else(|| Path::new(""));
    scenario.disk_spool_paths.iter().map(|raw| normalize(&base_dir.join(raw))).collect()
}

/// Resolves `.`/`..` components as text, with no symlink resolution and no need for the path to
/// exist. A `..` past the root is dropped, as `PathBuf::pop` does; scenario paths are always
/// absolute (`scenario::discover` lists an absolute `dir`).
fn normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// Refuses `resolved` unless it lands strictly inside `<root>/perf/results/` (see the module doc).
/// `perf/results/` itself is refused, although `Path::starts_with` accepts it: it holds every
/// scenario's JSON output and every other scenario's spool.
fn require_within_results_dir(root: &Path, resolved: &Path) -> anyhow::Result<()> {
    let results_dir = normalize(&root.join("perf/results"));
    if resolved == results_dir || !resolved.starts_with(&results_dir) {
        bail!(
            "refusing to clear spool directory {} -- it does not resolve inside {} (a scenario's \
             `buffer.disk.path` must keep its spool under perf/results/, the one directory this \
             harness is allowed to clear)",
            resolved.display(),
            results_dir.display(),
        );
    }
    Ok(())
}

/// Clears every disk-backed spool `scenario` declares.
///
/// Call it before every spawn (each repeat in `run`, and once each in `attribute`/`flamegraph`),
/// so `DiskQueue::open`'s startup scan never sees a spool from an earlier repeat or invocation.
/// Prints one line per directory removed; silent when there is none.
pub fn clear(root: &Path, scenario: &Scenario) -> anyhow::Result<()> {
    for resolved in resolve_spool_dirs(scenario) {
        require_within_results_dir(root, &resolved)?;
        if resolved.exists() {
            fs::remove_dir_all(&resolved)
                .with_context(|| format!("clearing spool directory {}", resolved.display()))?;
            println!("-- cleared spool {}", resolved.display());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scenario::Workload;

    fn scenario(path: &str, disk_spool_paths: &[&str]) -> Scenario {
        Scenario {
            name: "buffered".to_string(),
            path: PathBuf::from(path),
            workload: Workload::Generated { count: 100 },
            needs_sigterm: false,
            disk_spool_paths: disk_spool_paths.iter().map(PathBuf::from).collect(),
        }
    }

    #[test]
    fn resolve_spool_dirs_joins_against_the_scenarios_own_directory_like_logit_does() {
        let s = scenario("/repo/perf/scenarios/buffered.yaml", &["../results/spool"]);
        assert_eq!(resolve_spool_dirs(&s), vec![PathBuf::from("/repo/perf/results/spool")]);
    }

    #[test]
    fn resolve_spool_dirs_handles_more_than_one_disk_backed_sink() {
        let s = scenario("/repo/perf/scenarios/x.yaml", &["../results/a", "../results/b"]);
        assert_eq!(
            resolve_spool_dirs(&s),
            vec![PathBuf::from("/repo/perf/results/a"), PathBuf::from("/repo/perf/results/b")]
        );
    }

    #[test]
    fn resolve_spool_dirs_is_empty_for_a_scenario_with_no_disk_buffer() {
        let s = scenario("/repo/perf/scenarios/passthrough.yaml", &[]);
        assert!(resolve_spool_dirs(&s).is_empty());
    }

    #[test]
    fn normalize_resolves_parent_and_current_dir_components_with_no_filesystem_access() {
        assert_eq!(
            normalize(Path::new("/repo/perf/scenarios/../results/spool")),
            PathBuf::from("/repo/perf/results/spool")
        );
        assert_eq!(normalize(Path::new("/a/./b/../c")), PathBuf::from("/a/c"));
        // A path with nothing to resolve round-trips unchanged.
        assert_eq!(
            normalize(Path::new("/repo/perf/results/spool")),
            PathBuf::from("/repo/perf/results/spool")
        );
    }

    #[test]
    fn normalize_cannot_escape_above_an_absolute_root() {
        // Mirrors what `PathBuf::pop` already does at the root -- a `..` with nowhere left to go
        // is dropped rather than producing a path outside the filesystem root.
        assert_eq!(normalize(Path::new("/../../etc")), PathBuf::from("/etc"));
    }

    #[test]
    fn a_spool_inside_perf_results_is_allowed() {
        require_within_results_dir(Path::new("/repo"), Path::new("/repo/perf/results/spool"))
            .expect("perf/results/spool is inside perf/results/");
    }

    #[test]
    fn a_spool_outside_perf_results_is_refused() {
        let err =
            require_within_results_dir(Path::new("/repo"), Path::new("/repo/perf/scenarios/spool"))
                .expect_err("perf/scenarios/ is not perf/results/");
        assert!(format!("{err:#}").contains("perf/results"), "{err:#}");
    }

    #[test]
    fn a_spool_that_escapes_via_dot_dot_is_refused() {
        let s = scenario("/repo/perf/scenarios/evil.yaml", &["../../etc/spool"]);
        let resolved = &resolve_spool_dirs(&s)[0];
        assert_eq!(resolved, &PathBuf::from("/repo/etc/spool"));
        let err = require_within_results_dir(Path::new("/repo"), resolved)
            .expect_err("escapes perf/results/ entirely, not just perf/scenarios/");
        assert!(format!("{err:#}").contains("perf/results"), "{err:#}");
    }

    #[test]
    fn a_spool_path_equal_to_perf_results_itself_is_refused() {
        // `perf/results/` is not itself a scenario's spool -- it's the harness's own results
        // directory, holding every scenario's JSON and every other scenario's spool. `starts_with`
        // alone would accept a scenario declaring exactly `perf/results` and this harness would
        // then `rm -rf` all of it.
        let err = require_within_results_dir(Path::new("/repo"), Path::new("/repo/perf/results"))
            .expect_err("perf/results itself, not a spool under it, must be refused");
        assert!(format!("{err:#}").contains("perf/results"), "{err:#}");
    }

    #[test]
    fn clear_removes_an_existing_spool_directory_and_leaves_nothing_else() {
        let root =
            std::env::temp_dir().join(format!("logit-perf-spool-test-{}", std::process::id()));
        let scenario_dir = root.join("perf/scenarios");
        let spool_dir = root.join("perf/results/spool");
        fs::create_dir_all(&scenario_dir).unwrap();
        fs::create_dir_all(spool_dir.join("segments")).unwrap();
        fs::write(spool_dir.join("segments/0000.seg"), b"not real segment bytes").unwrap();

        let s = Scenario {
            name: "buffered".to_string(),
            path: scenario_dir.join("buffered.yaml"),
            workload: Workload::Generated { count: 1 },
            needs_sigterm: false,
            disk_spool_paths: vec![PathBuf::from("../results/spool")],
        };
        clear(&root, &s).unwrap();
        assert!(!spool_dir.exists(), "the spool directory itself should be gone");
        assert!(root.join("perf/results").exists(), "only the spool subdirectory, not its parent");
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn clear_is_a_silent_no_op_when_the_spool_does_not_exist_yet() {
        let root =
            std::env::temp_dir().join(format!("logit-perf-spool-test-noop-{}", std::process::id()));
        fs::create_dir_all(root.join("perf/scenarios")).unwrap();
        let s = Scenario {
            name: "buffered".to_string(),
            path: root.join("perf/scenarios/buffered.yaml"),
            workload: Workload::Generated { count: 1 },
            needs_sigterm: false,
            disk_spool_paths: vec![PathBuf::from("../results/spool")],
        };
        clear(&root, &s).expect("no spool yet is not an error, just nothing to do");
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn clear_bails_rather_than_remove_a_spool_outside_perf_results() {
        let root = std::env::temp_dir()
            .join(format!("logit-perf-spool-test-escape-{}", std::process::id()));
        let scenario_dir = root.join("perf/scenarios");
        fs::create_dir_all(&scenario_dir).unwrap();
        // A spool declared right next to the scenario file itself, not under perf/results/.
        fs::create_dir_all(scenario_dir.join("spool")).unwrap();

        let s = Scenario {
            name: "evil".to_string(),
            path: scenario_dir.join("evil.yaml"),
            workload: Workload::Generated { count: 1 },
            needs_sigterm: false,
            disk_spool_paths: vec![PathBuf::from("spool")],
        };
        let err = clear(&root, &s).expect_err("outside perf/results/ must be refused");
        assert!(format!("{err:#}").contains("perf/results"), "{err:#}");
        assert!(scenario_dir.join("spool").exists(), "must not have been removed");
        fs::remove_dir_all(&root).unwrap();
    }
}
