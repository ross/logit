//! The one durable replace-a-small-file path every checkpoint shares: the disk spool's
//! `cursor.json` and the tail checkpoint
//! (`docs/adr/durable-checkpoint-writes-and-fault-injection.md`).
//!
//! [`write_file_durably`] writes a sibling tmp file, `fsync`s it, renames it over the target, then
//! `fsync`s the directory. A crash at any step leaves either the old document or the new one, and
//! after a power loss the rename can't land ahead of the bytes it names. Synchronous `std::fs`:
//! the spool calls it on its persist worker thread, the tail through `spawn_blocking`.

use std::ffi::OsString;
use std::fmt;
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::fault::{Op, Point};

/// A step of [`write_file_durably`], in the order they run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Creating (or truncating) the tmp file and writing the document into it.
    Write,
    /// `fsync`ing the tmp file.
    SyncFile,
    /// Renaming the tmp file over the target.
    Rename,
    /// `fsync`ing the parent directory, which makes the rename durable.
    SyncDir,
}

impl fmt::Display for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Step::Write => "writing the tmp file",
            Step::SyncFile => "syncing the tmp file",
            Step::Rename => "renaming the tmp file into place",
            Step::SyncDir => "syncing the directory",
        })
    }
}

/// A failed [`write_file_durably`]: the step that failed and its error.
#[derive(Debug, thiserror::Error)]
#[error("{step}: {source}")]
pub struct AtomicWriteError {
    pub step: Step,
    #[source]
    pub source: io::Error,
}

impl AtomicWriteError {
    /// Whether the new document already replaced the old one. True only when the directory
    /// `fsync` failed: the rename has happened, but a power loss may still undo it.
    pub fn replaced(&self) -> bool {
        self.step == Step::SyncDir
    }
}

/// `path` with `.tmp` appended to its full file name: `cursor.json` → `cursor.json.tmp`.
/// Never `with_extension`, which maps `state.json` and `state.yaml` to the same `state.tmp`.
pub fn tmp_path(path: &Path) -> PathBuf {
    let mut name = OsString::from(path.as_os_str());
    name.push(".tmp");
    PathBuf::from(name)
}

/// Replaces `path` with `bytes`: write [`tmp_path`], `fsync` it, rename it over `path`, `fsync`
/// the parent directory (`.` when `path` has none). Each step is preceded by a
/// [`crate::fault::check`] at `site`. On error, the target holds the old document unless
/// [`AtomicWriteError::replaced`].
pub fn write_file_durably(
    path: &Path,
    bytes: &[u8],
    site: &'static str,
) -> Result<(), AtomicWriteError> {
    let tmp = tmp_path(path);
    let dir = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    let step = |step: Step| move |source: io::Error| AtomicWriteError { step, source };

    let mut file = crate::fault_io!(Point::new(site, Op::Write), &tmp, 0, File::create(&tmp))
        .map_err(step(Step::Write))?;
    file.write_all(bytes).map_err(step(Step::Write))?;
    crate::fault_io!(Point::new(site, Op::SyncFile), &tmp, 0, file.sync_all())
        .map_err(step(Step::SyncFile))?;
    drop(file);
    crate::fault_io!(Point::new(site, Op::Rename), path, 0, std::fs::rename(&tmp, path))
        .map_err(step(Step::Rename))?;
    crate::fault_io!(
        Point::new(site, Op::SyncDir),
        dir,
        0,
        File::open(dir).and_then(|d| d.sync_all())
    )
    .map_err(step(Step::SyncDir))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk_queue::test_support::scratch_dir;
    use crate::fault::{self, errno, sites};

    const SITE: &str = sites::SPOOL_CURSOR;
    const STEPS: [(Step, Op); 4] = [
        (Step::Write, Op::Write),
        (Step::SyncFile, Op::SyncFile),
        (Step::Rename, Op::Rename),
        (Step::SyncDir, Op::SyncDir),
    ];

    #[test]
    fn a_durable_write_runs_write_sync_rename_then_directory_sync_in_that_order() {
        let dir = scratch_dir("atomic-order");
        let path = dir.join("cursor.json");
        let scope = fault::scope(&dir);
        scope.record();

        write_file_durably(&path, b"new", SITE).unwrap();

        let hits: Vec<_> = scope.hits().into_iter().map(|h| (h.point, h.path)).collect();
        assert_eq!(
            hits,
            vec![
                (Point::new(SITE, Op::Write), tmp_path(&path)),
                (Point::new(SITE, Op::SyncFile), tmp_path(&path)),
                (Point::new(SITE, Op::Rename), path.clone()),
                (Point::new(SITE, Op::SyncDir), dir.clone()),
            ]
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"new");
        assert!(!tmp_path(&path).exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_tmp_name_appends_to_the_full_file_name() {
        assert_eq!(tmp_path(Path::new("a/cursor.json")), Path::new("a/cursor.json.tmp"));
        assert_eq!(tmp_path(Path::new("checkpoint")), Path::new("checkpoint.tmp"));
        assert_eq!(tmp_path(Path::new("/x/y.tar.gz")), Path::new("/x/y.tar.gz.tmp"));
        assert_ne!(
            tmp_path(Path::new("state.json")),
            tmp_path(Path::new("state.yaml")),
            "files differing only in extension must not share a tmp"
        );
    }

    #[test]
    fn a_failure_at_any_step_leaves_the_previous_document_readable() {
        for (step, op) in STEPS {
            let dir = scratch_dir("atomic-fail");
            let path = dir.join("cursor.json");
            write_file_durably(&path, b"old", SITE).unwrap();

            let scope = fault::scope(&dir);
            scope.fail(Point::new(SITE, op), errno::EIO);
            let err = write_file_durably(&path, b"new", SITE).unwrap_err();
            drop(scope);

            assert_eq!(err.step, step);
            assert_eq!(err.source.raw_os_error(), Some(errno::EIO));
            let expected: &[u8] = if err.replaced() { b"new" } else { b"old" };
            assert_eq!(std::fs::read(&path).unwrap(), expected, "failed at {step:?}");
            assert_eq!(err.replaced(), step == Step::SyncDir);
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn a_crash_at_any_step_leaves_either_the_old_or_the_new_document() {
        for (step, op) in STEPS {
            let dir = scratch_dir("atomic-crash");
            let path = dir.join("cursor.json");
            write_file_durably(&path, b"old", SITE).unwrap();

            let scope = fault::scope(&dir);
            scope.crash_at(Point::new(SITE, op), 1);
            assert!(write_file_durably(&path, b"new", SITE).is_err());
            assert!(scope.crashed());
            assert!(
                write_file_durably(&path, b"newer", SITE).is_err(),
                "nothing under a frozen scope runs"
            );
            scope.revive();
            drop(scope);

            // The crash stops the operation at `step`, so only a crash at the directory sync
            // comes after the rename.
            let expected: &[u8] = if step == Step::SyncDir { b"new" } else { b"old" };
            assert_eq!(std::fs::read(&path).unwrap(), expected, "crashed at {step:?}");
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn a_stray_longer_tmp_is_overwritten_not_appended() {
        let dir = scratch_dir("atomic-stray-tmp");
        let path = dir.join("cursor.json");
        std::fs::write(tmp_path(&path), vec![b'x'; 4096]).unwrap();

        write_file_durably(&path, b"short", SITE).unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), b"short");
        assert!(!tmp_path(&path).exists());
        std::fs::remove_dir_all(&dir).ok();
    }
}
