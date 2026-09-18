//! Clears a disk-backed scenario's spool directory before it's spawned.
//!
//! `buffered.yaml`'s spool is a real crash-recoverable disk queue
//! (`docs/adr/disk-backed-sink-buffer.md`), and `DiskQueue::open` unconditionally reads and
//! CRC-walks the *entire* active segment file on every startup, whether or not there's anything
//! left to replay (`crates/logit-pipeline/src/disk_queue.rs`, `docs/known-gaps.md`'s `buffered`
//! entry). Left alone across repeats -- or across separate `script/perf` invocations -- that spool
//! only grows, so each later run re-validates a larger file than the one before it: exactly the
//! monotonic throughput collapse `docs/design/performance.md`'s `buffered` investigation found.
//! Clearing the spool before every spawn removes the accumulation, not the per-startup scan cost
//! itself -- that's still `DiskQueue::open`'s, and stays open work
//! (`docs/known-gaps.md`).
//!
//! **Never removes anything outside `<repo root>/perf/results/`.** A scenario's `buffer.disk.path`
//! is an ordinary relative path resolved against that scenario's own file
//! (`crates/logit-cli/src/pipeline.rs`'s `queue_config`: `base_dir.join(&disk.path)`, `base_dir`
//! being `path.parent()`) -- this harness resolves it exactly the same way, then refuses to touch
//! the result unless it lands inside `perf/results/`, the one directory every shipped scenario's
//! spool convention keeps it under (`perf/scenarios/buffered.yaml`'s own comment). A scenario
//! declaring a spool path that resolves somewhere else is a scenario bug worth surfacing loudly,
//! not a directory this tool should ever `rm -rf`.

use crate::scenario::Scenario;
use anyhow::{bail, Context};
use std::fs;
use std::path::{Component, Path, PathBuf};

/// Resolves every `buffer.disk.path` `scenario` declares against its own file's directory --
/// exactly what `logit` itself does at startup (`crates/logit-cli/src/pipeline.rs`'s
/// `queue_config`) -- then normalizes the result lexically. Normalizing (rather than
/// [`std::fs::canonicalize`]) is deliberate: the spool directory may not exist yet (the very first
/// repeat, or a scenario nobody has run before), and [`clear`]'s containment check has to work
/// before anything on disk does.
pub fn resolve_spool_dirs(scenario: &Scenario) -> Vec<PathBuf> {
    let base_dir = scenario.path.parent().unwrap_or_else(|| Path::new(""));
    scenario.disk_spool_paths.iter().map(|raw| normalize(&base_dir.join(raw))).collect()
}

/// Resolves `.`/`..` components the way a filesystem would, but purely as text -- no
/// [`std::fs::canonicalize`], no symlink resolution, nothing that requires the path to exist. A
/// leading `..` past the root simply has nowhere to go and is dropped, the same behavior
/// `PathBuf::pop` already gives an absolute path (this harness only ever resolves scenario paths,
/// which are always absolute -- `scenario::discover`'s `entry.path()` off an absolute `dir`).
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

/// Refuses `resolved` unless it lands strictly inside `<root>/perf/results/` -- see the module doc
/// for why. `perf/results/` itself is refused too, not just accepted as trivially "inside itself"
/// (`Path::starts_with`'s own definition): it's the harness's shared results directory, holding
/// every scenario's JSON output and every other scenario's spool, not one scenario's own spool to
/// remove.
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

/// Clears every disk-backed spool `scenario` declares. Called once before every spawn -- each
/// repeat in `run`, and once each in `attribute`/`flamegraph` -- so `DiskQueue::open`'s startup
/// scan never sees a spool left over from an earlier repeat or invocation. Prints one line per
/// directory actually removed; a scenario with no `buffer.disk:` at all, or whose spool doesn't
/// exist yet, is silent.
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
